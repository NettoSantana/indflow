# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\producao\historico_routes.py
# LAST_RECODE: 2026-02-23 21:05 America/Bahia
# MOTIVO: Corrigir 500 do detalhe-dia (remover dependencia de qtd_boas em producao_evento) e adicionar endpoint manual de backfill para producao_horaria.

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

def _fetch_horaria(conn: sqlite3.Connection, machine_id: str, data_ref: date) -> dict[int, dict]:
    out: dict[int, dict] = {h: {"meta": 0, "produzido": 0, "refugo": 0} for h in range(24)}

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

        if data_col and hora_col:
            sel_cols = [hora_col]
            if prod_col:
                sel_cols.append(prod_col)
            if meta_col:
                sel_cols.append(meta_col)
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
            active_days = cfg.get("active_days")

            if isinstance(active_days, list) and active_days:
                # Python weekday: Mon=0..Sun=6; nossa lista default e 1..7 (Seg=1)
                wd = data_ref.weekday() + 1
                if wd not in set(int(x) for x in active_days if str(x).isdigit()):
                    # Dia nao ativo: tudo NP
                    horas = []
                    for h in range(24):
                        hs = datetime(data_ref.year, data_ref.month, data_ref.day, h, 0, 0)
                        he = hs + timedelta(hours=1)
                        horas.append(
                            {
                                "hour": h,
                                "slot": f"{h:02d}:00-{(h+1)%24:02d}:00",
                                "meta": 0,
                                "produzido": 0,
                                "refugo": 0,
                                "segments": _build_segments_for_hour(hs, he, True, []),
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

            # Busca eventos do dia
            event_times: list[datetime] = []
            if _table_exists(conn, "producao_evento"):
                ts_col = _resolve_ts_col(conn, "producao_evento")
                if ts_col:
                    day_start = datetime(data_ref.year, data_ref.month, data_ref.day, 0, 0, 0, tzinfo=TZ_BAHIA)
                    day_end = day_start + timedelta(days=1)
                    try:
                        sql = f"""
                            SELECT {ts_col} as ts
                            FROM producao_evento
                            WHERE machine_id=?
                              AND datetime({ts_col}) >= datetime(?)
                              AND datetime({ts_col}) < datetime(?)
                            ORDER BY datetime({ts_col}) ASC
                        """
                        rows = conn.execute(sql, (eff_mid, day_start.isoformat(), day_end.isoformat())).fetchall()
                        for r in rows:
                            try:
                                t = r["ts"]
                                if t:
                                    event_times.append(datetime.fromisoformat(str(t)).replace(tzinfo=TZ_BAHIA))
                            except Exception:
                                # tenta parse simples
                                try:
                                    event_times.append(datetime.strptime(str(r["ts"]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_BAHIA))
                                except Exception:
                                    pass
                    except Exception:
                        pass

            run_intervals = _compute_run_intervals(event_times, stop_sec)

            # Tabela horaria (meta/produzido/refugo)
            hor = _fetch_horaria(conn, eff_mid, data_ref)

            # Monta resposta hora a hora
            horas = []
            for h in range(24):
                hs = datetime(data_ref.year, data_ref.month, data_ref.day, h, 0, 0)
                he = hs + timedelta(hours=1)

                meta = _safe_int(hor.get(h, {}).get("meta", 0), 0)
                produzido = _safe_int(hor.get(h, {}).get("produzido", 0), 0)
                refugo = _safe_int(hor.get(h, {}).get("refugo", 0), 0)

                is_np = meta <= 0
                segs = _build_segments_for_hour(hs, he, is_np, run_intervals)

                horas.append(
                    {
                        "hour": h,
                        "slot": f"{h:02d}:00-{(h+1)%24:02d}:00",
                        "meta": meta,
                        "produzido": produzido,
                        "refugo": refugo,
                        "segments": segs,
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