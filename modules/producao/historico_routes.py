# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\producao\historico_routes.py
# LAST_RECODE: 2026-02-26 09:30 America/Bahia
# MOTIVO: Mostrar produzido por hora no detalhe-dia sem carregar valor da hora anterior; na virada de hora, a nova hora deve iniciar zerada (até haver produção).


from __future__ import annotations

import os
import json
import sqlite3
import traceback
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Blueprint, jsonify, render_template, request

try:
    from modules.db_indflow import init_db, get_db
except Exception:
    init_db = None
    get_db = None

try:
    from modules.machine_state import get_machine
except Exception:
    get_machine = None

TZ_BAHIA = ZoneInfo("America/Bahia")

def _to_sql_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _parse_ts_any(value: str | None) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                dt = None
        if dt is None:
            return None
    if getattr(dt, "tzinfo", None) is not None:
        try:
            dt = dt.astimezone(TZ_BAHIA).replace(tzinfo=None)
        except Exception:
            dt = dt.replace(tzinfo=None)
    return dt

historico_bp = Blueprint(
    "historico_bp",
    __name__,
    template_folder="templates",
)

def _sqlite_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _get_conn() -> sqlite3.Connection:
    if callable(get_db):
        return get_db()

    db_path = os.environ.get("INDFLOW_DB_PATH") or os.environ.get("DB_PATH") or "/data/indflow.db"
    return _sqlite_connect(db_path)


def _hhmmss_to_sec(s: str) -> int:
    try:
        parts = (s or "").split(":")
        if len(parts) != 3:
            return 0
        h = int(parts[0])
        m = int(parts[1])
        sec = int(parts[2])
        return h * 3600 + m * 60 + sec
    except Exception:
        return 0


def _sec_to_hhmmss(total_sec: int) -> str:
    try:
        total_sec = int(total_sec)
    except Exception:
        total_sec = 0
    if total_sec < 0:
        total_sec = 0
    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    s = total_sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def _dt_naive_to_day_sec(dt: datetime) -> int:
    try:
        return (dt.hour * 3600) + (dt.minute * 60) + int(dt.second)
    except Exception:
        return 0

def _ms_to_naive_bahia(ms: int) -> datetime | None:
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=TZ_BAHIA).replace(tzinfo=None)
    except Exception:
        return None


def _naive_bahia_to_ms(dt_naive: datetime) -> int:
    """Converte datetime naive (assumido TZ_BAHIA) para epoch ms."""
    try:
        dt = dt_naive.replace(tzinfo=TZ_BAHIA)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0

def _count_pulses_producao_evento(
    conn: sqlite3.Connection,
    machine_id: str,
    effective_machine_id: str,
    start_ms: int,
    end_ms: int,
) -> int:
    """Conta pulsos (linhas) em producao_evento no intervalo [start_ms, end_ms).
    Usa effective_machine_id primeiro e faz fallback para machine_id original quando necessario.
    """
    if start_ms <= 0 or end_ms <= 0 or end_ms <= start_ms:
        return 0
    if not _table_exists(conn, "producao_evento"):
        return 0
    sql = "SELECT COUNT(1) AS c FROM producao_evento WHERE machine_id = ? AND ts_ms >= ? AND ts_ms < ?"
    def _count(mid: str) -> int:
        try:
            row = conn.execute(sql, (mid, int(start_ms), int(end_ms))).fetchone()
            if not row:
                return 0
            return _safe_int(row[0] if not isinstance(row, sqlite3.Row) else row["c"], 0)
        except Exception:
            return 0
    # prefer effective (scoped) quando existe
    c = _count(effective_machine_id) if effective_machine_id else 0
    if c == 0 and machine_id and machine_id != effective_machine_id:
        c = _count(machine_id)
    return int(c) if c and c > 0 else 0

def _extract_esp_counter(machine_state: dict | None) -> int | None:
    """Extrai o contador absoluto do ESP a partir do estado da maquina (best-effort)."""
    if not isinstance(machine_state, dict):
        return None
    keys = (
        "esp_abs",
        "esp_last",
        "esp",
        "contador",
        "counter",
        "count_abs",
    )
    for k in keys:
        if k in machine_state and machine_state.get(k) is not None:
            try:
                v = int(float(machine_state.get(k)))
                if v >= 0:
                    return v
            except Exception:
                continue
    return None


def _apply_current_stop_to_segments(
    segs: list[dict],
    stop_start_naive: datetime,
    hour_start: datetime,
    hour_end_calc: datetime,
) -> list[dict]:
    """
    Forca STOP no intervalo [max(stop_start, hour_start), hour_end_calc] na hora atual.

    - Preserva partes anteriores a stop_start.
    - Trunca o segmento que cruza stop_start.
    - Substitui todo o restante por um unico STOP ate hour_end_calc.
    """
    if not segs:
        return segs
    if hour_end_calc <= hour_start:
        return segs

    stop_sec = _dt_naive_to_day_sec(stop_start_naive)
    hs_sec = _dt_naive_to_day_sec(hour_start)
    he_sec = _dt_naive_to_day_sec(hour_end_calc)

    if stop_sec < hs_sec:
        stop_sec = hs_sec
    if stop_sec > he_sec:
        stop_sec = he_sec

    new_segs: list[dict] = []
    for s in segs:
        st = s.get("state")
        a = _hhmmss_to_sec(s.get("start", "00:00:00"))
        b = _hhmmss_to_sec(s.get("end", "00:00:00"))
        if b <= stop_sec:
            new_segs.append({"start": _sec_to_hhmmss(a), "end": _sec_to_hhmmss(b), "state": st})
            continue
        if a < stop_sec:
            # Mantem parte ate stop_sec
            new_segs.append({"start": _sec_to_hhmmss(a), "end": _sec_to_hhmmss(stop_sec), "state": st})
        # descarta partes depois de stop_sec (serao substituidas por STOP)

    if he_sec > stop_sec:
        # Evita duplicar STOP se ultimo ja for STOP e encostar
        if new_segs and new_segs[-1].get("state") == "STOP" and _hhmmss_to_sec(new_segs[-1].get("end", "00:00:00")) == stop_sec:
            new_segs[-1]["end"] = _sec_to_hhmmss(he_sec)
        else:
            new_segs.append({"start": _sec_to_hhmmss(stop_sec), "end": _sec_to_hhmmss(he_sec), "state": "STOP"})

    return new_segs


def _calc_seg_metrics(segs: list[dict]) -> tuple[int, int, int]:
    # Regra unica (mesma da barra):
    # - RUN soma em tempo_produzindo_sec
    # - STOP soma em tempo_parado_sec
    # - NP nao soma
    #
    # Paradas (cumulativas na hora):
    # - Conta 1 parada a cada inicio de segmento STOP com duracao > 0,
    #   inclusive se a hora ja comecar em STOP (sem exigir RUN->STOP).
    tempo_produzindo_sec = 0
    tempo_parado_sec = 0
    qtd_paradas = 0

    last_state = None
    for s in segs or []:
        st = s.get("state")
        a = _hhmmss_to_sec(s.get("start", "00:00:00"))
        b = _hhmmss_to_sec(s.get("end", "00:00:00"))
        dur = max(0, b - a)

        if st == "RUN":
            tempo_produzindo_sec += dur

        elif st == "STOP":
            tempo_parado_sec += dur
            # Parada = inicio de STOP com duracao
            if dur > 0 and last_state != "STOP":
                qtd_paradas += 1

        last_state = st

    return tempo_produzindo_sec, tempo_parado_sec, qtd_paradas

def _safe_int(v, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default

def _fetch_one(conn: sqlite3.Connection, sql: str, params: tuple):
    cur = conn.execute(sql, params)
    return cur.fetchone()

def _fetch_scalar(conn: sqlite3.Connection, sql: str, params: tuple, default=0):
    row = _fetch_one(conn, sql, params)
    if not row:
        return default
    try:
        val = row[0]
    except Exception:
        return default
    return default if val is None else val

def _has_coluna(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        for r in rows:
            if str(r[1]).lower() == col.lower():
                return True
    except Exception:
        return False
    return False

def _resolve_data_col(conn: sqlite3.Connection, table: str) -> str:
    if _has_coluna(conn, table, "data_ref"):
        return "data_ref"
    if _has_coluna(conn, table, "data"):
        return "data"
    return "data_ref"

def _resolve_effective_machine_id(conn: sqlite3.Connection, machine_id: str, data_ref: str) -> str:
    """
    Regra: usar UMA unica fonte por dia.

    - Se ja vier scoped (tem '::'), usa direto.
    - Se vier legacy (sem '::'), tentamos achar um machine_id "efetivo" (scoped) na producao_diaria
      para o mesmo dia, pegando o mais relevante por 'produzido'.

    Compatibilidade:
    - Padrao 1 (antigo): <cliente>::<maquina>
    - Padrao 2 (novo): <maquina>::<op/ctx>

    Se nao encontrar nada, cai no legacy.
    """
    mid = (machine_id or "").strip()
    if not mid:
        return mid
    if "::" in mid:
        return mid

    col = _resolve_data_col(conn, "producao_diaria")

    sql = f"""
        SELECT machine_id
          FROM producao_diaria
         WHERE {col} = ?
           AND (
                machine_id LIKE ?
             OR machine_id LIKE ?
           )
         ORDER BY produzido DESC
         LIMIT 1
    """

    # Tenta casar pelos 2 formatos:
    # 1) <cliente>::<maquina> -> termina com ::mid
    # 2) <maquina>::<op/ctx>  -> comeca com mid::
    like_suffix = f"%::{mid}"
    like_prefix = f"{mid}::%"

    try:
        row = _fetch_one(conn, sql, (data_ref, like_suffix, like_prefix))
        if row and row["machine_id"]:
            return str(row["machine_id"])
    except Exception:
        pass

    return mid

def _parse_date_any(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = _fetch_one(
            conn,
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return bool(row)
    except Exception:
        return False

def _get_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        cols = set()
        for r in cur.fetchall():
            try:
                cols.add(str(r[1]))
            except Exception:
                pass
        return cols
    except Exception:
        return set()

def _resolve_ts_col(conn: sqlite3.Connection, table_name: str) -> str | None:
    """Descobre coluna de timestamp (schema pode variar)."""
    cols = _get_columns(conn, table_name)
    preferred = (
        "timestamp",
        "ts",
        "created_at",
        "data_hora",
        "datahora",
        "datetime",
        "dt",
        "data_ref",
        "data",
    )
    for c in preferred:
        if c in cols:
            return c
    for c in cols:
        lc = c.lower()
        if "time" in lc or "data" in lc or "date" in lc:
            return c
    return None
def _load_machine_config_json(conn: sqlite3.Connection, machine_id: str) -> dict:
    if not _table_exists(conn, "machine_config"):
        return {}
    try:
        row = _fetch_one(
            conn,
            "SELECT config_json FROM machine_config WHERE machine_id=?",
            (machine_id,),
        )
        if not row:
            return {}
        raw = row["config_json"] if isinstance(row, sqlite3.Row) else row[0]
        if not raw:
            return {}
        return json.loads(raw)
    except Exception:
        return {}



def _build_meta_24_from_machine_state(machine_state: dict | None) -> list[int] | None:
    """Monta meta[24] a partir do estado/config da maquina.

    Esperado no machine_state:
    - turno_inicio: "HH:MM"
    - meta_por_hora: lista (tipicamente do tamanho do turno, ex. 8 itens)

    Regra:
    - meta_por_hora[i] aplica na hora do relogio (turno_inicio + i) modulo 24
    - demais horas ficam 0
    """
    if not isinstance(machine_state, dict):
        return None

    mph = machine_state.get("meta_por_hora")
    if not isinstance(mph, list) or not mph:
        return None

    turno_inicio = str(machine_state.get("turno_inicio") or "").strip()
    if not turno_inicio:
        return None

    try:
        hh = int(turno_inicio.split(":")[0])
    except Exception:
        return None

    meta24 = [0] * 24
    for i, v in enumerate(mph):
        h = (hh + i) % 24
        meta24[h] = _safe_int(v, 0)

    return meta24


def _merge_intervals(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged: list[tuple[datetime, datetime]] = []
    cur_s, cur_e = intervals[0]
    for s, e in intervals[1:]:
        if s <= cur_e:
            if e > cur_e:
                cur_e = e
        else:
            merged.append((cur_s, cur_e))
            cur_s, cur_e = s, e
    merged.append((cur_s, cur_e))
    return merged

def _compute_run_intervals(event_times: list[datetime], stop_sec: int) -> list[tuple[datetime, datetime]]:
    intervals: list[tuple[datetime, datetime]] = []
    if stop_sec <= 0:
        stop_sec = 120
    delta = timedelta(seconds=stop_sec)
    for t in event_times:
        intervals.append((t, t + delta))
    return _merge_intervals(intervals)

def _intersect(a_s: datetime, a_e: datetime, b_s: datetime, b_e: datetime) -> tuple[datetime, datetime] | None:
    s = max(a_s, b_s)
    e = min(a_e, b_e)
    if e <= s:
        return None
    return (s, e)

def _build_segments_for_hour(
    hour_start: datetime,
    hour_end: datetime,
    is_np: bool,
    run_intervals: list[tuple[datetime, datetime]],
) -> list[dict]:
    if is_np:
        return [
            {
                "start": hour_start.strftime("%H:%M:%S"),
                "end": hour_end.strftime("%H:%M:%S"),
                "state": "NP",
            }
        ]

    intersections: list[tuple[datetime, datetime]] = []
    for rs, re_ in run_intervals:
        inter = _intersect(hour_start, hour_end, rs, re_)
        if inter:
            intersections.append(inter)
    intersections = _merge_intervals(intersections)

    segs: list[dict] = []
    cursor = hour_start
    for rs, re_ in intersections:
        if rs > cursor:
            segs.append(
                {
                    "start": cursor.strftime("%H:%M:%S"),
                    "end": rs.strftime("%H:%M:%S"),
                    "state": "STOP",
                }
            )
        segs.append(
            {
                "start": rs.strftime("%H:%M:%S"),
                "end": re_.strftime("%H:%M:%S"),
                "state": "RUN",
            }
        )
        cursor = re_
    if cursor < hour_end:
        segs.append(
            {
                "start": cursor.strftime("%H:%M:%S"),
                "end": hour_end.strftime("%H:%M:%S"),
                "state": "STOP",
            }
        )
    return segs



def _fetch_run_intervals_from_state_events(
    conn: sqlite3.Connection,
    effective_machine_id: str,
    data_ref: date,
) -> list[tuple[datetime, datetime]]:
    """
    Converte machine_state_event (transicoes RUN/STOP/NP) em intervalos RUN para o dia.

    - Usa o ultimo estado antes de 00:00 para definir estado inicial do dia.
    - Para dia atual, corta intervalo aberto em "agora" para nao preencher futuro.
    - Se a tabela nao existir, retorna [].
    """
    try:
        if not _table_exists(conn, "machine_state_event"):
            return []
    except Exception:
        return []

    # IMPORTANTE: o restante do Historico usa datetimes naive (sem tzinfo).
    # Para evitar erro de comparacao naive vs aware, aqui tambem usamos naive.
    day_start = datetime(data_ref.year, data_ref.month, data_ref.day, 0, 0, 0)
    day_end = day_start + timedelta(days=1)

    # "agora" somente se for o dia atual, para nao inventar futuro
    now_dt = datetime.now(TZ_BAHIA).replace(tzinfo=None)
    if day_start.date() == now_dt.date():
        hard_end = min(day_end, now_dt)
    else:
        hard_end = day_end

    # day_start_ms deve respeitar o fuso America/Bahia; calcula via aware apenas para timestamp.
    day_start_ms = int(day_start.replace(tzinfo=TZ_BAHIA).timestamp() * 1000)

    # Estado inicial: ultimo evento antes do dia
    state0 = None
    try:
        r0 = conn.execute(
            "SELECT state, ts_ms FROM machine_state_event "
            "WHERE effective_machine_id=? AND ts_ms < ? "
            "ORDER BY ts_ms DESC LIMIT 1",
            (effective_machine_id, day_start_ms),
        ).fetchone()
        if r0:
            state0 = str(r0[0] or "").upper()
    except Exception:
        state0 = None

    # Eventos do dia
    evs: list[tuple[int, str]] = []
    try:
        for r in conn.execute(
            "SELECT ts_ms, state FROM machine_state_event "
            "WHERE effective_machine_id=? AND data_ref=? "
            "ORDER BY ts_ms ASC",
            (effective_machine_id, data_ref.isoformat()),
        ).fetchall():
            try:
                ts_ms = int(r[0])
            except Exception:
                continue
            st = str(r[1] or "").upper()
            if st not in ("RUN", "STOP", "NP"):
                continue
            evs.append((ts_ms, st))
    except Exception:
        return []

    # Monta intervalos RUN
    intervals: list[tuple[datetime, datetime]] = []
    cur_state = state0 if state0 in ("RUN", "STOP", "NP") else "STOP"
    cur_t = day_start

    def _push_run(a: datetime, b: datetime) -> None:
        if b <= a:
            return
        # corta em hard_end
        aa = max(a, day_start)
        bb = min(b, hard_end)
        if bb > aa:
            intervals.append((aa, bb))

    for ts_ms, st in evs:
        try:
            t = datetime.fromtimestamp(ts_ms / 1000.0, tz=TZ_BAHIA).replace(tzinfo=None)
        except Exception:
            continue
        if t < day_start:
            continue
        if t > hard_end:
            break

        if cur_state == "RUN":
            _push_run(cur_t, t)

        cur_state = st
        cur_t = t

    if cur_state == "RUN":
        _push_run(cur_t, hard_end)

    return _merge_intervals(intervals)


def _fetch_horaria(conn: sqlite3.Connection, machine_id: str, data_ref: date) -> dict[int, dict]:
    out: dict[int, dict] = {h: {"meta": 0, "produzido": 0, "refugo": 0, "baseline_esp": 0, "esp_last": 0} for h in range(24)}

    # producao_horaria: tenta meta + produzido
    if _table_exists(conn, "producao_horaria"):
        cols = _get_columns(conn, "producao_horaria")
        data_col = _resolve_data_col(conn, "producao_horaria")
        hora_col = (
            "hora_idx" if "hora_idx" in cols else (
                "hora" if "hora" in cols else ("hora_int" if "hora_int" in cols else None)
            )
        )
        prod_col = None
        for c in ("produzido", "producao", "count", "qtd"):
            if c in cols:
                prod_col = c
                break
        meta_col = None
        for c in ("meta_hora", "meta", "meta_pcs"):
            if c in cols:
                meta_col = c
                break

        esp_col = None
        for c in ("esp_last", "esp_abs", "esp", "contador", "counter"):
            if c in cols:
                esp_col = c
                break
        base_col = "baseline_esp" if "baseline_esp" in cols else None

        if data_col and hora_col:
            sel_cols = [hora_col]
            if prod_col:
                sel_cols.append(prod_col)
            if meta_col:
                sel_cols.append(meta_col)
            if base_col:
                sel_cols.append(base_col)
            if esp_col:
                sel_cols.append(esp_col)
            sql = f"SELECT {', '.join(sel_cols)} FROM producao_horaria WHERE machine_id=? AND {data_col}=?"
            try:
                for r in conn.execute(sql, (machine_id, data_ref.isoformat())).fetchall():
                    try:
                        h = int(r[hora_col]) if isinstance(r, sqlite3.Row) else int(r[0])
                    except Exception:
                        continue
                    if h < 0 or h > 23:
                        continue
                    if prod_col:
                        try:
                            out[h]["produzido"] = _safe_int(r[prod_col] if isinstance(r, sqlite3.Row) else r[sel_cols.index(prod_col)], 0)
                        except Exception:
                            pass
                    if meta_col:
                        try:
                            out[h]["meta"] = _safe_int(r[meta_col] if isinstance(r, sqlite3.Row) else r[sel_cols.index(meta_col)], 0)
                        except Exception:
                            pass
                    if base_col:
                        try:
                            out[h]["baseline_esp"] = _safe_int(r[base_col] if isinstance(r, sqlite3.Row) else r[sel_cols.index(base_col)], 0)
                        except Exception:
                            pass
                    if esp_col:
                        try:
                            out[h]["esp_last"] = _safe_int(r[esp_col] if isinstance(r, sqlite3.Row) else r[sel_cols.index(esp_col)], 0)
                        except Exception:
                            pass
            except Exception:
                pass

    # refugo_horaria
    if _table_exists(conn, "refugo_horaria"):
        cols = _get_columns(conn, "refugo_horaria")
        data_col = _resolve_data_col(conn, "refugo_horaria")
        hora_col = (
            "hora_idx" if "hora_idx" in cols else (
                "hora" if "hora" in cols else ("hora_int" if "hora_int" in cols else None)
            )
        )
        ref_col = None
        for c in ("refugo", "qtd", "valor"):
            if c in cols:
                ref_col = c
                break
        if data_col and hora_col and ref_col:
            sql = f"SELECT {hora_col} as hora, {ref_col} as refugo FROM refugo_horaria WHERE machine_id=? AND {data_col}=?"
            try:
                for r in conn.execute(sql, (machine_id, data_ref.isoformat())).fetchall():
                    try:
                        h = int(r["hora"])
                    except Exception:
                        continue
                    if h < 0 or h > 23:
                        continue
                    out[h]["refugo"] = _safe_int(r["refugo"], 0)
            except Exception:
                pass

    return out

def _refugo_do_dia(conn: sqlite3.Connection, machine_id: str, data_ref: str) -> int:
    col = _resolve_data_col(conn, "refugo_horaria")
    tentativas = [
        (f"SELECT COALESCE(SUM(refugo), 0) FROM refugo_horaria WHERE machine_id = ? AND {col} = ?", (machine_id, data_ref)),
        (f"SELECT COALESCE(SUM(qtd), 0) FROM refugo_horaria WHERE machine_id = ? AND {col} = ?", (machine_id, data_ref)),
        (f"SELECT COALESCE(SUM(quantidade), 0) FROM refugo_horaria WHERE machine_id = ? AND {col} = ?", (machine_id, data_ref)),
    ]
    for sql, params in tentativas:
        try:
            return _safe_int(_fetch_scalar(conn, sql, params, default=0), default=0)
        except Exception:
            continue
    return 0

def _diaria_do_dia(conn: sqlite3.Connection, machine_id: str, data_ref: str) -> dict:
    col = _resolve_data_col(conn, "producao_diaria")
    eff_mid = _resolve_effective_machine_id(conn, machine_id, data_ref)

    # Coleta candidatos do dia para evitar dobrar quando existem registros duplicados
    # (ex.: legado + scoped, ou gravacao repetida).
    mid_raw = (machine_id or "").strip()
    mid_uns = mid_raw.split("::", 1)[1] if "::" in mid_raw else mid_raw

    mids = set()
    if eff_mid:
        mids.add(str(eff_mid))
    if mid_raw:
        mids.add(str(mid_raw))

    # Quando o request vem sem scope, considere tambem possiveis variacoes no banco.
    like_scoped = None
    like_legacy = None
    if "::" not in mid_raw and mid_uns:
        like_scoped = f"%::{mid_uns.lower()}"
        like_legacy = f"%:{mid_uns.lower()}"

    where = [f"{col} = ?"]
    params = [data_ref]

    if mids:
        where.append("machine_id IN ({})".format(",".join(["?"] * len(mids))))
        params.extend(list(mids))

    if like_scoped:
        where.append("machine_id LIKE ?")
        params.append(like_scoped)
    if like_legacy:
        where.append("machine_id LIKE ?")
        params.append(like_legacy)

    sql = (
        "SELECT machine_id, produzido, meta, percentual "
        "FROM producao_diaria "
        "WHERE " + " AND ".join(where)
    )

    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
    except Exception:
        rows = []

    if not rows:
        return {"produzido": 0, "meta": None, "percentual": None, "_mid": eff_mid}

    vals = []
    for r in rows:
        try:
            vals.append(int(r["produzido"] or 0))
        except Exception:
            vals.append(0)

    chosen_row = None

    uniq_all = sorted(set([v for v in vals if v is not None]))
    uniq_pos = [v for v in uniq_all if v > 0]

    # Heuristica anti-dobro:
    # - Se existir valor positivo maximo "X" e tambem existir "X/2" no dataset, assume que X foi duplicado (ex.: por join/OPs) e usa X/2.
    # - Caso contrario, usa o maior valor positivo disponivel.
    chosen = max(uniq_all) if uniq_all else 0
    if uniq_pos:
        max_pos = max(uniq_pos)
        if (max_pos % 2 == 0) and ((max_pos // 2) in uniq_pos):
            chosen = max_pos // 2
        else:
            chosen = max_pos

    # Preferir linha "maquina::op" quando existir
    for r in rows:
        try:
            if int(r["produzido"] or 0) == chosen and "::" in str(r["machine_id"] or ""):
                chosen_row = r
                break
        except Exception:
            continue

    if not chosen_row:
        for r in rows:
            try:
                if int(r["produzido"] or 0) == chosen:
                    chosen_row = r
                    break
            except Exception:
                continue

    if not chosen_row:
        chosen_row = rows[0]

    return {
        "produzido": _safe_int(chosen_row["produzido"], 0),
        "meta": _safe_int(chosen_row["meta"], 0) if chosen_row["meta"] is not None else None,
        "percentual": _safe_int(chosen_row["percentual"], 0) if chosen_row["percentual"] is not None else None,
        "_mid": str(chosen_row["machine_id"] or eff_mid),
    }

def _op_contexto(conn: sqlite3.Connection, machine_id: str, data_ref: str) -> list[dict]:
    """
    Regra oficial:
    - A OP pertence ao dia operacional da ABERTURA (inicio_iso).
    - Atravessar a virada do dia (ou encerrar em outro dia) NAO cria segunda ocorrencia no historico.

    Implementacao:
    - Filtra por janela [data_ref 00:01, proximo_dia 00:01) usando APENAS inicio_iso.
    - Comparacao feita via datetime() do SQLite para evitar erro de comparacao textual e formatos ISO diferentes.
    - Sem fallback por data_ref. Se nao bater por inicio_iso, nao exibe.
    - Deduplica registros repetidos do banco para nao exibir a mesma OP duas vezes no mesmo dia.
    """
    eff_mid = _resolve_effective_machine_id(conn, machine_id, data_ref)

    # Janela do dia operacional: vira as 00:01 (inclusive). 00:00 ainda pertence ao dia anterior.
    try:
        d0 = date.fromisoformat(str(data_ref))
    except Exception:
        return []

    d1 = d0 + timedelta(days=1)
    start_dt = f"{d0.isoformat()} 00:01:00"
    end_dt = f"{d1.isoformat()} 00:01:00"

    # datetime(replace(inicio_iso,'T',' ')) cobre:
    # - "YYYY-MM-DDTHH:MM:SS"
    # - "YYYY-MM-DD HH:MM:SS"
    # - com ou sem offset, conforme parser do SQLite.
    sql = """
        SELECT op, lote, operador, inicio_iso, fim_iso, status
          FROM ordens_producao
         WHERE machine_id = ?
           AND datetime(replace(inicio_iso, 'T', ' ')) >= datetime(?)
           AND datetime(replace(inicio_iso, 'T', ' ')) < datetime(?)
         ORDER BY datetime(replace(inicio_iso, 'T', ' ')) ASC
    """

    try:
        rows = conn.execute(sql, (eff_mid, start_dt, end_dt)).fetchall()
    except Exception:
        try:
            rows = conn.execute(sql, (machine_id, start_dt, end_dt)).fetchall()
        except Exception:
            return []

    itens = []
    seen = set()

    for r in rows:
        opv = r["op"]
        lote = r["lote"]
        operador = r["operador"]
        inicio_iso = r["inicio_iso"]
        fim_iso = r["fim_iso"]
        status = r["status"]

        # Dedup defensivo: mesma OP/lote/inicio nao deve aparecer duas vezes
        key = (str(opv or ""), str(lote or ""), str(operador or ""), str(inicio_iso or ""))
        if key in seen:
            continue
        seen.add(key)

        itens.append(
            {
                "op": opv,
                "lote": lote,
                "operador": operador,
                "inicio_iso": inicio_iso,
                "fim_iso": fim_iso,
                "status": status,
            }
        )

    return itens

if callable(init_db):
    try:
        init_db()
    except Exception:
        pass

@historico_bp.route("/api/producao/historico", methods=["GET"])
def api_producao_historico():
    machine_id = (request.args.get("machine_id") or "").strip()
    days = _safe_int(request.args.get("days"), 10)
    days = max(1, min(days, 60))

    if not machine_id:
        return jsonify({"ok": False, "error": "machine_id obrigatorio"}), 400

    hoje = datetime.now(TZ_BAHIA).date()
    inicio = hoje - timedelta(days=days - 1)

    conn = _get_conn()
    try:
        dados = []

        for i in range(days):
            dia: date = inicio + timedelta(days=i)
            data_ref = dia.isoformat()

            diaria = _diaria_do_dia(conn, machine_id, data_ref)
            refugo = _refugo_do_dia(conn, machine_id, data_ref)
            produzido = _safe_int(diaria.get("produzido"), 0)
            pecas_boas = max(produzido - refugo, 0)

            item = {
                "data": data_ref,
                "produzido": produzido,
                "pecas_boas": pecas_boas,
                "refugo": refugo,
                "meta": diaria.get("meta"),
                "percentual": diaria.get("percentual"),
                "ops": _op_contexto(conn, machine_id, data_ref),
            }
            dados.append(item)

        if (request.args.get("wrap") or "").strip() == "1":
            return jsonify({"ok": True, "machine_id": machine_id, "dados": dados})
        return jsonify(dados)
    finally:
        if not callable(get_db):
            try:
                conn.close()
            except Exception:
                pass

@historico_bp.route("/api/producao/detalhe-dia", methods=["GET"])
def api_producao_detalhe_dia():
    machine_id = (request.args.get("machine_id") or "").strip()
    date_str = (request.args.get("date") or request.args.get("data") or "").strip()

    if not machine_id:
        return jsonify({"ok": False, "error": "machine_id obrigatorio"}), 400

    data_ref = _parse_date_any(date_str) or datetime.now(TZ_BAHIA).date()


    # Hora atual (naive, TZ_BAHIA) para cortar a hora em andamento e nao preencher futuro
    now_naive = None
    try:
        now_dt = datetime.now(TZ_BAHIA).replace(tzinfo=None)
        if data_ref == now_dt.date():
            now_naive = now_dt.replace(tzinfo=None)
    except Exception:
        now_naive = None
    conn = _get_conn()
    try:
        try:
            # Resolve machine_id efetivo (scoped) para evitar "horas zeradas" quando o dia foi gravado como <cliente>::<maquina>.
            eff_mid = _resolve_effective_machine_id(conn, machine_id, data_ref.isoformat())

            # Carrega config (tenta primeiro pelo machine_id recebido; se nao existir, tenta pelo efetivo).
            cfg = _load_machine_config_json(conn, machine_id)
            if (not cfg) and eff_mid and eff_mid != machine_id:
                cfg = _load_machine_config_json(conn, eff_mid)

            # stop_sec e dias ativos (se existir)
            stop_sec = _safe_int(
                ((cfg.get("oee") or {}).get("no_count_stop_sec") if isinstance(cfg.get("oee"), dict) else None),
                120,
            )

            machine_state = None
            stop_start_naive = None
            if now_naive is not None and callable(get_machine):
                try:
                    machine_state = get_machine(eff_mid) or get_machine(machine_id)
                    if isinstance(machine_state, dict):
                        stopped_ms = machine_state.get("stopped_since_ms") or machine_state.get("stopped_since")
                        status_ui = str(machine_state.get("status_ui") or "").strip().upper()
                        run_flag = machine_state.get("run")
                        is_stopped = False
                        if status_ui in ("PARADA", "PARADO", "STOP", "STOPPED"):
                            is_stopped = True
                        else:
                            try:
                                if run_flag is not None and int(run_flag) == 0:
                                    is_stopped = True
                            except Exception:
                                is_stopped = False
                        if is_stopped and stopped_ms is not None:
                            try:
                                stop_start_naive = _ms_to_naive_bahia(int(stopped_ms))
                            except Exception:
                                stop_start_naive = None
                except Exception:
                    stop_start_naive = None


            # Segmentos RUN/STOP agora vem do rastro persistido em machine_state_event.
            # Se nao houver eventos (ou tabela), cai para lista vazia (tudo STOP dentro de hora programada).
            run_intervals = _fetch_run_intervals_from_state_events(conn, eff_mid, data_ref)
            # Tabela horaria (meta/produzido/refugo)
            hor = _fetch_horaria(conn, eff_mid, data_ref)

            # ============================================================
            # Meta por Hora (fonte da verdade: estado/config da maquina)
            # Regra:
            # - Usa meta_por_hora + turno_inicio do estado retornado por get_machine()
            # - Converte para vetor de 24h do relogio (00..23)
            # - Aplica no JSON hours[].meta (sem recalcular em banco)
            #
            # Observacao:
            # - Mantemos produzido/refugo do banco sem recalcular
            # - Se nao houver meta_por_hora/turno_inicio, mantem meta do banco (compatibilidade)
            # ============================================================
            try:
                meta24 = _build_meta_24_from_machine_state(machine_state)
                if meta24 is not None:
                    for hh in range(24):
                        hor[hh]["meta"] = _safe_int(meta24[hh], 0)
            except Exception:
                pass



            # ============================================================
            # Normalizacao (dia atual): evitar valores acumulados em producao_horaria
            #
            # Caso producao_horaria tenha armazenado ESP acumulado em 'produzido',
            # usamos esp_last/baseline_esp (se existirem) para converter em delta por hora.
            # Para a hora corrente, alinhamos com o card (ESP_atual - baseline_da_hora).
            # ============================================================
            try:
                if now_naive is not None:
                    now_hour = int(now_naive.hour)
                    esp_now = _extract_esp_counter(machine_state)

                    # Baseline por hora: preferir baseline_esp do banco; se faltar, usar esp_last da hora anterior
                    last_esp = 0
                    for hh in range(24):
                        try:
                            last_esp = max(_safe_int(hor.get(hh, {}).get("esp_last", 0), 0), last_esp)
                        except Exception:
                            pass

                    prev_esp = 0
                    for hh in range(24):
                        base = _safe_int(hor.get(hh, {}).get("baseline_esp", 0), 0)
                        esp_h = _safe_int(hor.get(hh, {}).get("esp_last", 0), 0)
                        if base <= 0 and hh > 0:
                            prev_esp_val = _safe_int(hor.get(hh - 1, {}).get("esp_last", 0), 0)
                            if prev_esp_val > 0:
                                base = prev_esp_val
                        if base <= 0:
                            base = prev_esp

                        # Atualiza prev_esp para permitir fallback mesmo sem esp_last
                        if esp_h > 0:
                            prev_esp = esp_h

                        hor[hh]["_baseline_calc"] = base

                    # Ajusta produzido por hora usando esp_last/baseline quando disponivel
                    for hh in range(24):
                        if hh > now_hour:
                            # Futuro no dia atual: zera para nao herdar acumulado
                            hor[hh]["produzido"] = 0
                            continue

                        base = _safe_int(hor.get(hh, {}).get("_baseline_calc", 0), 0)
                        esp_h = _safe_int(hor.get(hh, {}).get("esp_last", 0), 0)
                        if hh == now_hour and esp_now is not None:
                            # Hora corrente: usa o contador atual do ESP
                            delta = _safe_int(esp_now, 0) - base
                        else:
                            # Horas passadas: se houver esp_last, calcula delta
                            if esp_h > 0:
                                delta = esp_h - base
                            else:
                                delta = None

                        if delta is None:
                            continue
                        if delta < 0:
                            delta = 0
                        hor[hh]["produzido"] = int(delta)
            except Exception:
                pass

            # Monta resposta hora a hora

            horas = []
            for h in range(24):
                hs = datetime(data_ref.year, data_ref.month, data_ref.day, h, 0, 0)
                he = hs + timedelta(hours=1)

                # Para o dia atual: corta a hora em andamento no "agora" e nao preenche futuro.
                he_calc = he
                if now_naive is not None:
                    if now_naive <= hs:
                        he_calc = hs
                    elif now_naive < he:
                        he_calc = now_naive


                meta = _safe_int(hor.get(h, {}).get("meta", 0), 0)
                produzido = _safe_int(hor.get(h, {}).get("produzido", 0), 0)
                refugo = _safe_int(hor.get(h, {}).get("refugo", 0), 0)



                is_np = meta <= 0

                # Se he_calc == hs (hora futura no dia atual), nao inventar 60 min.
                if he_calc == hs:
                    zstate = "NP" if is_np else "STOP"
                    segs = [{
                        "start": hs.strftime("%H:%M:%S"),
                        "end": hs.strftime("%H:%M:%S"),
                        "state": zstate,
                    }]
                    tempo_produzindo_sec, tempo_parado_sec, qtd_paradas = 0, 0, 0
                    # FIX: hora futura/recem-iniciada nao deve herdar produzido/refugo da hora anterior
                    produzido = 0
                    refugo = 0
                    horas.append(
                        {
                            "hour": h,
                            "slot": f"{h:02d}:00-{(h+1)%24:02d}:00",
                            "meta": meta,
                            "produzido": produzido,
                            "refugo": refugo,
                            "segments": segs,
                            "tempo_produzindo_sec": tempo_produzindo_sec,
                            "tempo_parado_sec": tempo_parado_sec,
                            "qtd_paradas": qtd_paradas,
                        }
                    )
                    continue


                # Fallback: se nao houver rastro em machine_state_event para o dia,
                # nao marque a hora inteira como STOP quando houve producao.
                # Regra simples: dentro de hora ativa (meta>0), se produzido>0 => RUN a hora inteira; senao => STOP.
                if (not is_np) and (not run_intervals):
                    if produzido > 0:
                        segs = [{
                            "start": hs.strftime("%H:%M:%S"),
                            "end": he_calc.strftime("%H:%M:%S"),
                            "state": "RUN",
                        }]
                    else:
                        segs = [{
                            "start": hs.strftime("%H:%M:%S"),
                            "end": he_calc.strftime("%H:%M:%S"),
                            "state": "STOP",
                        }]
                else:
                    segs = _build_segments_for_hour(hs, he_calc, is_np, run_intervals)

                if (not is_np) and now_naive is not None and stop_start_naive is not None:
                    try:
                        if hs <= now_naive < he and stop_start_naive <= he_calc:
                            segs = _apply_current_stop_to_segments(segs, stop_start_naive, hs, he_calc)
                    except Exception:
                        pass

                tempo_produzindo_sec, tempo_parado_sec, qtd_paradas = _calc_seg_metrics(segs)

                # FIX: na virada de hora, a nova hora deve iniciar zerada ate haver producao/refugo.
                # Evita carregar o valor acumulado da hora anterior por baseline ainda nao reancorado.
                if now_naive is not None and hs <= now_naive < he:
                    try:
                        if (now_naive - hs).total_seconds() < 120 and int(tempo_produzindo_sec or 0) == 0:
                            produzido = 0
                            refugo = 0
                    except Exception:
                        pass
                horas.append(
                    {
                        "hour": h,
                        "slot": f"{h:02d}:00-{(h+1)%24:02d}:00",
                        "meta": meta,
                        "produzido": produzido,
                        "refugo": refugo,
                        "segments": segs,
                        "tempo_produzindo_sec": tempo_produzindo_sec,
                        "tempo_parado_sec": tempo_parado_sec,
                        "qtd_paradas": qtd_paradas,
                    }
                )

            return jsonify(
                {
                    "ok": True,
                    "machine_id": machine_id,
                    "effective_machine_id": eff_mid,
                    "date": data_ref.isoformat(),
                    "stop_sec": stop_sec,
                    "hours": horas,
                }
            )
        except Exception as e:
            tb = traceback.format_exc()
            try:
                print("ERROR detalhe-dia:\n" + tb, flush=True)
            except Exception:
                pass
            return jsonify({"ok": False, "error": "erro no detalhe-dia", "details": str(e)}), 500
    finally:
        if not callable(get_db):
            try:
                conn.close()
            except Exception:
                pass



def _distribute_int_total(total: int, weights: list[int] | None = None, n: int = 24) -> list[int]:
    if n <= 0:
        return []
    total = _safe_int(total, 0)
    if total <= 0:
        return [0] * n

    if not weights or len(weights) != n:
        base = total // n
        rem = total - (base * n)
        out = [base] * n
        for i in range(rem):
            out[i] += 1
        return out

    w = [max(_safe_int(x, 0), 0) for x in weights]
    s = sum(w)
    if s <= 0:
        return _distribute_int_total(total, None, n)

    raw = [(total * wi) / s for wi in w]
    out = [int(x) for x in raw]
    diff = total - sum(out)
    if diff > 0:
        # distribui o restante nas maiores frações
        fracs = sorted([(raw[i] - out[i], i) for i in range(n)], reverse=True)
        for k in range(diff):
            out[fracs[k % n][1]] += 1
    elif diff < 0:
        # remove excedente nas menores frações, sem ficar negativo
        fracs = sorted([(raw[i] - out[i], i) for i in range(n)])
        k = 0
        to_remove = -diff
        while to_remove > 0 and k < len(fracs):
            i = fracs[k][1]
            if out[i] > 0:
                out[i] -= 1
                to_remove -= 1
            else:
                k += 1
    return out


def _backfill_horaria_for_day(conn: sqlite3.Connection, machine_id: str, data_ref: str) -> dict:
    """
    Gera 24 linhas em producao_horaria a partir de producao_diaria, quando nao existir horaria no dia.

    - Usa _diaria_do_dia() para escolher a linha do dia e obter o machine_id efetivo gravado no diario.
    - Distribui meta e produzido nas 24 horas (estimativa), apenas para destravar o detalhe-dia.
    """
    if not _table_exists(conn, "producao_horaria"):
        return {"ok": False, "error": "tabela producao_horaria nao existe"}

    diaria = _diaria_do_dia(conn, machine_id, data_ref)
    produzido_dia = _safe_int(diaria.get("produzido"), 0)
    meta_dia = _safe_int(diaria.get("meta"), 0)
    target_mid = (diaria.get("_mid") or machine_id or "").strip()

    if produzido_dia <= 0:
        return {"ok": True, "skipped": True, "reason": "produzido_dia_zero", "machine_id": target_mid, "date": data_ref}

    cols = _get_columns(conn, "producao_horaria")
    data_col = _resolve_data_col(conn, "producao_horaria")
    hour_col = "hora_idx" if "hora_idx" in cols else ("hora" if "hora" in cols else ("hora_int" if "hora_int" in cols else None))
    if not hour_col or not data_col:
        return {"ok": False, "error": "schema producao_horaria sem colunas de hora/data"}

    # ja existe horaria?
    try:
        existing = _fetch_scalar(
            conn,
            f"SELECT COUNT(1) FROM producao_horaria WHERE machine_id=? AND {data_col}=?",
            (target_mid, data_ref),
            default=0,
        )
        if _safe_int(existing, 0) > 0:
            return {"ok": True, "skipped": True, "reason": "ja_existe_horaria", "machine_id": target_mid, "date": data_ref}
    except Exception:
        pass

    # meta por hora (uniforme)
    meta_h = _distribute_int_total(meta_dia, None, 24) if meta_dia > 0 else [0] * 24

    # produzido por hora (proporcional à meta quando houver)
    if sum(meta_h) > 0:
        prod_h = _distribute_int_total(produzido_dia, meta_h, 24)
    else:
        prod_h = _distribute_int_total(produzido_dia, None, 24)

    now_sql = _to_sql_dt(datetime.now(TZ_BAHIA).replace(tzinfo=None))

    # monta INSERT dinamico por colunas existentes
    insert_cols = ["machine_id", data_col, hour_col]
    if "baseline_esp" in cols:
        insert_cols.append("baseline_esp")
    if "esp_last" in cols:
        insert_cols.append("esp_last")
    if "produzido" in cols:
        insert_cols.append("produzido")
    if "meta" in cols:
        insert_cols.append("meta")
    if "percentual" in cols:
        insert_cols.append("percentual")
    if "updated_at" in cols:
        insert_cols.append("updated_at")
    if "cliente_id" in cols:
        insert_cols.append("cliente_id")

    placeholders = ",".join(["?"] * len(insert_cols))
    sql = f"INSERT INTO producao_horaria ({', '.join(insert_cols)}) VALUES ({placeholders})"

    inserted = 0
    for h in range(24):
        meta = _safe_int(meta_h[h], 0)
        prod = _safe_int(prod_h[h], 0)
        pct = 0
        if meta > 0:
            try:
                pct = int(round((prod / meta) * 100))
            except Exception:
                pct = 0

        vals = []
        for c in insert_cols:
            if c == "machine_id":
                vals.append(target_mid)
            elif c == data_col:
                vals.append(data_ref)
            elif c == hour_col:
                vals.append(h)
            elif c == "baseline_esp":
                vals.append(0)
            elif c == "esp_last":
                vals.append(0)
            elif c == "produzido":
                vals.append(prod)
            elif c == "meta":
                vals.append(meta)
            elif c == "percentual":
                vals.append(pct)
            elif c == "updated_at":
                vals.append(now_sql)
            elif c == "cliente_id":
                vals.append(None)
            else:
                vals.append(None)

        try:
            conn.execute(sql, tuple(vals))
            inserted += 1
        except Exception:
            # se der erro em uma hora, aborta o dia (roll back externo)
            raise

    return {"ok": True, "inserted_hours": inserted, "machine_id": target_mid, "date": data_ref, "produzido_dia": produzido_dia, "meta_dia": meta_dia}


@historico_bp.route("/api/producao/backfill-horaria", methods=["POST"])
def api_producao_backfill_horaria():
    """
    Endpoint manual para preencher producao_horaria usando producao_diaria.
    Uso:
      POST /producao/api/producao/backfill-horaria?machine_id=maquina005&days=60
      POST /producao/api/producao/backfill-horaria?machine_id=maquina005&all=1
      POST /producao/api/producao/backfill-horaria?machine_id=maquina005&date_from=2026-02-01&date_to=2026-02-23
    """
    machine_id = (request.args.get("machine_id") or "").strip()
    if not machine_id:
        return jsonify({"ok": False, "error": "machine_id obrigatorio"}), 400

    all_flag = (request.args.get("all") or "").strip() == "1"
    days = _safe_int(request.args.get("days"), 60)
    days = max(1, min(days, 366))

    d_from = _parse_date_any((request.args.get("date_from") or "").strip())
    d_to = _parse_date_any((request.args.get("date_to") or "").strip())

    hoje = datetime.now(TZ_BAHIA).date()

    if d_to is None:
        d_to = hoje
    if d_from is None:
        if all_flag:
            # tenta achar o primeiro dia com produzido > 0 no diario (para este machine_id)
            conn0 = _get_conn()
            try:
                if not _table_exists(conn0, "producao_diaria"):
                    return jsonify({"ok": False, "error": "tabela producao_diaria nao existe"}), 400
                col = _resolve_data_col(conn0, "producao_diaria")
                mid_raw = (machine_id or "").strip()
                mid_uns = mid_raw.split("::", 1)[1] if "::" in mid_raw else mid_raw
                like_suffix = f"%::{mid_uns}"
                like_prefix = f"{mid_uns}::%"

                row = _fetch_one(
                    conn0,
                    f"""
                    SELECT MIN({col}) as dmin
                      FROM producao_diaria
                     WHERE COALESCE(produzido,0) > 0
                       AND (
                            machine_id = ?
                         OR machine_id LIKE ?
                         OR machine_id LIKE ?
                       )
                    """,
                    (mid_raw, like_suffix, like_prefix),
                )
                dmin = _parse_date_any(str(row["dmin"]) if row and row["dmin"] else None)
                d_from = dmin or (hoje - timedelta(days=days - 1))
            finally:
                if not callable(get_db):
                    try:
                        conn0.close()
                    except Exception:
                        pass
        else:
            d_from = hoje - timedelta(days=days - 1)

    if d_from > d_to:
        d_from, d_to = d_to, d_from

    conn = _get_conn()
    try:
        if not _table_exists(conn, "producao_diaria"):
            return jsonify({"ok": False, "error": "tabela producao_diaria nao existe"}), 400
        if not _table_exists(conn, "producao_horaria"):
            return jsonify({"ok": False, "error": "tabela producao_horaria nao existe"}), 400

        total_days = 0
        backfilled_days = 0
        inserted_rows = 0
        detalhes: list[dict] = []

        d = d_from
        try:
            conn.execute("BEGIN")
        except Exception:
            pass

        while d <= d_to:
            total_days += 1
            data_ref = d.isoformat()
            try:
                res = _backfill_horaria_for_day(conn, machine_id, data_ref)
                detalhes.append(res)
                if res.get("ok") and (not res.get("skipped")):
                    backfilled_days += 1
                    inserted_rows += _safe_int(res.get("inserted_hours"), 0)
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                return jsonify({"ok": False, "error": "falha no backfill", "date": data_ref, "details": str(e)}), 500
            d = d + timedelta(days=1)

        try:
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.commit()
            except Exception:
                pass

        return jsonify(
            {
                "ok": True,
                "machine_id": machine_id,
                "date_from": d_from.isoformat(),
                "date_to": d_to.isoformat(),
                "days_considered": total_days,
                "days_backfilled": backfilled_days,
                "rows_inserted": inserted_rows,
                "details": detalhes,
            }
        )
    finally:
        if not callable(get_db):
            try:
                conn.close()
            except Exception:
                pass

@historico_bp.route("/historico", methods=["GET"])
def historico_page():
    return render_template("historico.html")
###