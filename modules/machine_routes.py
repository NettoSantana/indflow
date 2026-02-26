# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\producao\machine_routes.py
# LAST_RECODE: 2026-02-26 21:00 America/Bahia
# MOTIVO: Persistir producao por hora no SQLite (baseline_hora + producao_horaria) para manter horas anteriores congeladas e não perder após deploy.
import os
import json
import sqlite3
import hashlib
import uuid
import re
from urllib.parse import urlencode
from flask import Blueprint, request, jsonify, render_template, session, redirect, url_for
from datetime import datetime, timedelta
from modules.db_indflow import get_db
from modules.machine_state import get_machine
from modules.machine_calc import (
    aplicar_unidades,
    salvar_conversao,
    atualizar_producao_hora,
    verificar_reset_diario,
    reset_contexto,
    calcular_ultima_hora_idx,
    calcular_tempo_medio,
    aplicar_derivados_ml,
    carregar_baseline_diario,
    now_bahia,
    dia_operacional_ref_str,
    TZ_BAHIA,
)
from modules.machine_service import processar_nao_programado
from modules.repos.nao_programado_horaria_repo import load_np_por_hora_24
from modules.repos.machine_config_repo import upsert_machine_config
from modules.repos.refugo_repo import load_refugo_24, upsert_refugo
from modules.admin.routes import login_required

from modules.machine.device_helpers import (
    norm_device_id,
    touch_device_seen,
    get_machine_from_device,
)

machine_bp = Blueprint("machine_bp", __name__)


def _get_tz():
    """
    Retorna tzinfo padrao do projeto (Bahia).
    Evita NameError em rotas que geram timestamps.
    """
    return TZ_BAHIA

def _norm_machine_id(v):
    v = (v or "").strip().lower()
    return v or "maquina01"

def _unscope_machine_id(v: str) -> str:
    """
    Compat: se vier "cliente_id::maquina01", devolve "maquina01".
    """
    s = (v or "").strip().lower()
    if "::" in s:
        return (s.split("::", 1)[1] or "").strip() or "maquina01"
    return s or "maquina01"

def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _ensure_machine_state_event_schema(conn: sqlite3.Connection) -> None:
    """
    Garante tabela de eventos de estado da maquina (timeline).

    Salva apenas transicoes (quando muda de RUN/STOP/NP).
    """
    conn.execute(
        "CREATE TABLE IF NOT EXISTS machine_state_event ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "machine_id TEXT NOT NULL, "
        "effective_machine_id TEXT NOT NULL, "
        "cliente_id TEXT, "
        "ts_ms INTEGER NOT NULL, "
        "ts_iso TEXT NOT NULL, "
        "data_ref TEXT NOT NULL, "
        "hora_idx INTEGER NOT NULL, "
        "state TEXT NOT NULL"
        ")"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mse_mid_day ON machine_state_event (effective_machine_id, data_ref, ts_ms)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mse_mid_ts ON machine_state_event (effective_machine_id, ts_ms)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mse_cid_mid_day ON machine_state_event (cliente_id, effective_machine_id, data_ref, ts_ms)"
    )


def _ensure_baseline_hora_schema(conn: sqlite3.Connection) -> None:
    """Garante tabelas/colunas necessárias para persistência por hora."""
    # Baseline por hora: permite calcular produzido_hora = esp_absoluto - baseline_hora
    conn.execute(
        "CREATE TABLE IF NOT EXISTS baseline_hora ("
        "data_ref TEXT NOT NULL,"
        "hora_idx INTEGER NOT NULL,"
        "machine_id TEXT NOT NULL,"
        "cliente_id TEXT,"
        "baseline_esp INTEGER NOT NULL,"
        "updated_at TEXT"
        ")"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_hora "
        "ON baseline_hora(data_ref, hora_idx, machine_id, COALESCE(cliente_id,''))"
    )

    # Produção por hora (snapshot): usado pelo Histórico
    conn.execute(
        "CREATE TABLE IF NOT EXISTS producao_horaria ("
        "data_ref TEXT NOT NULL,"
        "hora_idx INTEGER NOT NULL,"
        "machine_id TEXT NOT NULL,"
        "cliente_id TEXT,"
        "produzido INTEGER NOT NULL DEFAULT 0,"
        "meta INTEGER NOT NULL DEFAULT 0,"
        "updated_at TEXT"
        ")"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_producao_horaria "
        "ON producao_horaria(data_ref, hora_idx, machine_id, COALESCE(cliente_id,''))"
    )

    # Migrações defensivas: adiciona colunas se tabela já existia com schema antigo
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(producao_horaria)").fetchall()]
    except Exception:
        cols = []
    if "cliente_id" not in cols:
        try:
            conn.execute("ALTER TABLE producao_horaria ADD COLUMN cliente_id TEXT")
        except Exception:
            pass
    if "meta" not in cols:
        try:
            conn.execute("ALTER TABLE producao_horaria ADD COLUMN meta INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
    if "updated_at" not in cols:
        try:
            conn.execute("ALTER TABLE producao_horaria ADD COLUMN updated_at TEXT")
        except Exception:
            pass

    try:
        bcols = [r[1] for r in conn.execute("PRAGMA table_info(baseline_hora)").fetchall()]
    except Exception:
        bcols = []
    if "cliente_id" not in bcols:
        try:
            conn.execute("ALTER TABLE baseline_hora ADD COLUMN cliente_id TEXT")
        except Exception:
            pass
    if "updated_at" not in bcols:
        try:
            conn.execute("ALTER TABLE baseline_hora ADD COLUMN updated_at TEXT")
        except Exception:
            pass


def _persist_producao_horaria_snapshot(
    machine_id: str,
    cliente_id: str | None,
    ts_ms: int,
    esp_absoluto: int,
    meta_hora: int = 0,
) -> None:
    """
    Persistência forte por hora.
    - Garante baseline por hora no primeiro evento da hora.
    - Atualiza 'produzido' da hora como diff do ESP absoluto.
    Isso garante que:
    - A hora anterior fique registrada (não some com deploy).
    - A hora atual continue atualizando.
    """
    try:
        dt_evt = datetime.fromtimestamp(int(ts_ms) / 1000, TZ_BAHIA)
        data_ref = dia_operacional_ref_str(dt_evt)
        hora_idx = int(dt_evt.hour)
        mid = _norm_machine_id(machine_id)
        cid = str(cliente_id).strip() if cliente_id else None
        updated_at = dt_evt.isoformat()

        conn = get_db()
        try:
            _ensure_baseline_hora_schema(conn)

            # Baseline da hora: se não existir, grava (primeiro evento da hora)
            row = conn.execute(
                "SELECT baseline_esp FROM baseline_hora "
                "WHERE data_ref=? AND hora_idx=? AND machine_id=? AND COALESCE(cliente_id,'')=COALESCE(?, '')",
                (data_ref, hora_idx, mid, cid),
            ).fetchone()

            if row and row[0] is not None:
                base = int(row[0])
            else:
                base = int(esp_absoluto)
                conn.execute(
                    "INSERT OR REPLACE INTO baseline_hora(data_ref, hora_idx, machine_id, cliente_id, baseline_esp, updated_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (data_ref, hora_idx, mid, cid, base, updated_at),
                )

            produzido = int(esp_absoluto) - int(base)
            if produzido < 0:
                produzido = 0

            conn.execute(
                "INSERT OR REPLACE INTO producao_horaria(data_ref, hora_idx, machine_id, cliente_id, produzido, meta, updated_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (data_ref, hora_idx, mid, cid, int(produzido), int(meta_hora or 0), updated_at),
            )

            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        pass


def _persist_snapshot_from_machine(m: dict, machine_id: str, ts_ms: int) -> None:
    """Wrapper seguro para persistir snapshot usando estado da máquina em memória."""
    try:
        esp_abs = m.get("esp_absoluto", None)
        if esp_abs is None:
            esp_abs = m.get("esp", None)
        if esp_abs is None:
            return
        try:
            esp_abs = int(esp_abs)
        except Exception:
            return

        try:
            meta_hora = int(m.get("meta_hora_pcs", m.get("meta_hora", m.get("meta_por_hora_atual", 0))) or 0)
        except Exception:
            meta_hora = 0

        cid = m.get("cliente_id") or None
        _persist_producao_horaria_snapshot(machine_id=str(machine_id), cliente_id=cid, ts_ms=int(ts_ms), esp_absoluto=int(esp_abs), meta_hora=int(meta_hora))
    except Exception:
        pass

def _get_last_machine_state(conn: sqlite3.Connection, effective_machine_id: str, cliente_id: str | None = None) -> dict | None:
    try:
        row = conn.execute(
            "SELECT state, ts_ms, data_ref, hora_idx FROM machine_state_event "
            "WHERE effective_machine_id=? AND (cliente_id IS ? OR cliente_id=?) "
            "ORDER BY ts_ms DESC LIMIT 1",
            (effective_machine_id, cliente_id, cliente_id),
        ).fetchone()
        if not row:
            return None
        return {"state": row[0], "ts_ms": row[1], "data_ref": row[2], "hora_idx": row[3]}
    except Exception:
        return None


def _record_machine_state_transition(
    raw_machine_id: str,
    effective_machine_id: str,
    cliente_id: str | None,
    state: str,
    agora: datetime,
    data_ref: str,
    hora_idx: int,
) -> None:
    """
    Persiste a transicao de estado (RUN/STOP/NP) se mudou em relacao ao ultimo evento.
    """
    st = (state or "").strip().upper()
    if st not in ("RUN", "STOP", "NP"):
        return

    ts_ms = int(agora.timestamp() * 1000)
    ts_iso = agora.isoformat()

    conn = get_db()
    try:
        _ensure_machine_state_event_schema(conn)
        last = _get_last_machine_state(conn, effective_machine_id, cliente_id)
        if last and str(last.get("state") or "").upper() == st:
            return
        conn.execute(
            "INSERT INTO machine_state_event (machine_id, effective_machine_id, cliente_id, ts_ms, ts_iso, data_ref, hora_idx, state) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (raw_machine_id, effective_machine_id, cliente_id, ts_ms, ts_iso, data_ref, int(hora_idx), st),
        )
        conn.commit()
    finally:
        conn.close()


def _infer_state_for_timeline(m: dict, hora_atual: int | None) -> str:
    """
    Decide estado apenas para a barra (sem meta):
      - NP: fora de programacao (np_por_hora_24[h] > 0)
      - RUN: status_ui == PRODUZINDO
      - STOP: caso contrario
    """
    try:
        if isinstance(hora_atual, int) and 0 <= hora_atual < 24:
            np24 = m.get("np_por_hora_24") or [0] * 24
            if isinstance(np24, list) and len(np24) == 24 and _safe_int(np24[hora_atual], 0) > 0:
                return "NP"
    except Exception:
        pass

    try:
        if (m.get("status_ui") or "").strip().upper() == "PRODUZINDO":
            return "RUN"
    except Exception:
        pass

    return "STOP"

def _calc_produzido_from_ops(ops: list) -> int:
    """
    Fallback simples para dias em que existem OPs, mas produzido esta 0.
    Regra:
      - Soma op_pcs das OPs ENCERRADAS.
      - Se op_pcs for 0 e houver op_metros + op_conv_m_por_pcs, converte para pcs (metros / conv).
      - Ignora OP ATIVA com pcs=0 para nao inflar historico.
    """
    if not isinstance(ops, list) or not ops:
        return 0

    total = 0
    for op in ops:
        if not isinstance(op, dict):
            continue

        status = (op.get("status") or "").strip().upper()
        if status != "ENCERRADA":
            continue

        pcs = _safe_int(op.get("op_pcs"), 0)
        if pcs > 0:
            total += pcs
            continue

        metros = _safe_int(op.get("op_metros"), 0)
        try:
            conv = float(op.get("op_conv_m_por_pcs") or 0)
        except Exception:
            conv = 0.0

        if metros > 0 and conv > 0:
            try:
                total += int(round(metros / conv))
            except Exception:
                pass

    return int(total)

def _parse_hhmm(hhmm: str) -> tuple[int, int] | None:
    try:
        h_str, m_str = (hhmm or "").strip().split(":", 1)
        h = int(h_str)
        m = int(m_str)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return (h, m)
    except Exception:
        pass
    return None

def _calc_minutos_parados_somente_turno(start_ms: int, end_ms: int, turno_inicio: str | None, turno_fim: str | None) -> int:
    ini = _parse_hhmm(turno_inicio or "")
    fim = _parse_hhmm(turno_fim or "")
    if ini is None or fim is None or end_ms <= start_ms:
        return 0
    a0 = datetime.fromtimestamp(int(start_ms) / 1000, TZ_BAHIA)
    a1 = datetime.fromtimestamp(int(end_ms) / 1000, TZ_BAHIA)
    d = a0.date() - timedelta(days=1)
    d_end = a1.date() + timedelta(days=1)
    total = 0
    while d <= d_end:
        s = datetime(d.year, d.month, d.day, ini[0], ini[1], 0, tzinfo=TZ_BAHIA)
        e = datetime(d.year, d.month, d.day, fim[0], fim[1], 0, tzinfo=TZ_BAHIA)
        if e <= s:
            e = e + timedelta(days=1)
        x0 = a0 if a0 > s else s
        x1 = a1 if a1 < e else e
        if x1 > x0:
            total += int((x1 - x0).total_seconds())
        d = d + timedelta(days=1)
    return int(total // 60)

def _sum_refugo_24(machine_id: str, dia_ref: str) -> int:
    """
    Refugo ainda está por machine_id (legado). Para não misturar,
    usamos sempre o machine_id "limpo" (sem cliente_id::).
    """
    try:
        mid = _unscope_machine_id(machine_id)
        arr = load_refugo_24(_norm_machine_id(mid), (dia_ref or "").strip())
        if not isinstance(arr, list):
            return 0
        return sum(_safe_int(x, 0) for x in arr)
    except Exception:
        return 0

def _sum_eventos_por_dia(cliente_id: str | None, machine_id: str, inicio: str, fim: str) -> dict:
    """
    Retorna {dia_ref: produzido} calculado a partir de producao_evento (delta),
    agrupando pelo dia local Bahia usando ts_ms (epoch ms do ESP).
    inicio/fim: ISO YYYY-MM-DD (inclusive).

    IMPORTANTE:
    - Em ambiente multi-tenant, a tabela pode conter linhas com machine_id "limpo" (ex: maquina004)
      e também "scoped" (ex: <cliente_id>::maquina004).
    - Somar os dois ao mesmo tempo dobra o resultado.
    - Portanto, quando cliente_id existir, priorizamos SEMPRE o scoped_mid.
      Se não houver dados scoped, fazemos fallback para o mid legado.
    """
    mid = _norm_machine_id(_unscope_machine_id(machine_id))

    try:
        d0 = datetime.fromisoformat(inicio).date()
        d1 = datetime.fromisoformat(fim).date()
    except Exception:
        return {}

    tz = TZ_BAHIA
    start_dt = datetime(d0.year, d0.month, d0.day, 0, 0, 0, tzinfo=tz)
    end_dt = datetime(d1.year, d1.month, d1.day, 23, 59, 59, tzinfo=tz)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    conn = get_db()
    try:
        _ensure_producao_evento_table(conn)

        scoped_mid = f"{cliente_id}::{mid}" if cliente_id else None

        def _query_sum(cfilter: str | None, mid_value: str) -> dict:
            params = []
            where_parts = []

            if cfilter:
                where_parts.append("cliente_id = ?")
                params.append(cfilter)

            where_parts.append("machine_id = ?")
            params.append(mid_value)

            where_parts.append("ts_ms >= ?")
            where_parts.append("ts_ms <= ?")
            params.extend([start_ms, end_ms])

            where = " AND ".join(where_parts)

            sql = f"""
                SELECT
                  date(ts_ms/1000, 'unixepoch', '-3 hours') AS dia_ref,
                  SUM(COALESCE(delta, 0)) AS produzido
                FROM producao_evento
                WHERE {where}
                GROUP BY dia_ref
                ORDER BY dia_ref DESC
            """

            cur = conn.execute(sql, tuple(params))
            out_local = {}
            for r in cur.fetchall():
                dia = (r[0] or "").strip()
                out_local[dia] = _safe_int(r[1], 0)
            return out_local

        # =====================================================
        # 1) Se tiver cliente_id: tenta scoped + filtro cliente_id
        # 2) Fallback: legado (mid limpo) + filtro cliente_id
        # 3) Se ainda vazio e cliente_id existe: tenta sem filtro cliente_id (banco legado)
        # =====================================================
        if cliente_id and scoped_mid:
            out = _query_sum(cliente_id, scoped_mid)
            if not out:
                out = _query_sum(cliente_id, mid)
            if not out:
                out = _query_sum(None, scoped_mid)
            if not out:
                out = _query_sum(None, mid)
            return out

        # Sem cliente_id: usa apenas o mid limpo
        return _query_sum(None, mid)

    finally:
        conn.close()
def _looks_like_uuid(v: str) -> bool:
    """
    Validacao simples para evitar usar session['cliente_id'] errado (ex: id de usuario).
    Aceita UUID no formato 8-4-4-4-12 (36 chars, 4 hifens).
    """
    s = (v or "").strip()
    if len(s) != 36:
        return False
    if s.count("-") != 4:
        return False
    parts = s.split("-")
    if len(parts) != 5:
        return False
    sizes = [8, 4, 4, 4, 12]
    for i, p in enumerate(parts):
        if len(p) != sizes[i]:
            return False
        for ch in p:
            if ch not in "0123456789abcdefABCDEF":
                return False
    return True

def _resolve_cliente_id_for_status(m: dict) -> str | None:
    """
    Resolve tenant para leitura do NP no /machine/status.
    Ordem (OPCAO 1):
      1) X-API-Key (se existir)
      2) m['cliente_id'] (gravado no update do ESP)
      3) session['cliente_id'] (web) somente se parecer UUID valido
    """
    try:
        c = _get_cliente_from_api_key()
        if c and c.get("id"):
            return str(c["id"])
    except Exception:
        pass

    try:
        cid_m = (m.get("cliente_id") or "").strip()
        if cid_m:
            return cid_m
    except Exception:
        pass

    try:
        cid_sess = (session.get("cliente_id") or "").strip()
        if cid_sess and _looks_like_uuid(cid_sess):
            return cid_sess
    except Exception:
        pass

    return None

def _machine_id_scoped(cliente_id: str | None, machine_id: str) -> str:
    if cliente_id:
        return f"{cliente_id}::{machine_id}"
    return machine_id

def _load_np_por_hora_24_scoped(machine_id: str, dia_ref: str, cliente_id: str | None) -> list:
    """Carrega NP por hora (24) do banco para a máquina (scoped)."""
    try:
        mid = _machine_id_scoped(cliente_id, machine_id)
        conn = get_db()
        try:
            return load_np_por_hora_24(conn, mid, (dia_ref or "").strip())
        finally:
            conn.close()
    except Exception:
        return [0] * 24

def _admin_token_ok() -> bool:
    """
    Proteção simples:
      - Configure no Railway/ENV: INDFLOW_ADMIN_TOKEN=<seu_token>
      - Envie no header: X-Admin-Token: <seu_token>
    """
    expected = (os.getenv("INDFLOW_ADMIN_TOKEN") or "").strip()
    if not expected:
        return False
    received = (request.headers.get("X-Admin-Token") or "").strip()
    return received == expected

def _get_cliente_from_api_key() -> dict | None:
    """
    AUTH do ESP por header X-API-Key:
      - calcula SHA256(api_key)
      - compara com clientes.api_key_hash
      - exige status 'active'
    Retorna dict {id, nome, status} ou None.
    """
    api_key = (request.headers.get("X-API-Key") or "").strip()
    if not api_key:
        return None

    api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    conn = get_db()
    try:
        cur = conn.execute(
            "SELECT id, nome, status FROM clientes WHERE api_key_hash = ?",
            (api_key_hash,),
        )
        row = cur.fetchone()
        if not row:
            return None
        if (row["status"] or "").strip().lower() != "active":
            return None
        return {"id": row["id"], "nome": row["nome"], "status": row["status"]}
    finally:
        conn.close()

def _get_cliente_id_for_request() -> str | None:
    """
    Resolve tenant do request:
      1) se tiver X-API-Key válida -> cliente_id
      2) senão, se tiver sessão web e parecer UUID valido -> session['cliente_id']
      3) senão -> None
    """
    c = _get_cliente_from_api_key()
    if c:
        return c["id"]

    cid = (session.get("cliente_id") or "").strip()
    if cid and _looks_like_uuid(cid):
        return cid

    return None

def _get_ts_ms_from_payload(data: dict) -> int | None:
    """
    Timestamp preferencial vindo do ESP (epoch ms).
    """
    if not isinstance(data, dict):
        return None

    candidates = ["ts_ms", "timestamp_ms", "ts", "t_ms"]
    v = None
    for k in candidates:
        if k in data:
            v = data.get(k)
            break

    if v is None:
        return None

    try:
        iv = int(v)
    except Exception:
        try:
            iv = int(float(v))
        except Exception:
            return None

    try:
        min_ok = 1577836800000
        now_ms = int(now_bahia().timestamp() * 1000)
        max_ok = now_ms + (7 * 24 * 60 * 60 * 1000)
        if iv < min_ok or iv > max_ok:
            return None
    except Exception:
        return None

    return iv


def _iso_to_ts_ms_bahia(value: str):
    s = (value or "").strip()
    if not s:
        return None

    # Aceitar "YYYY-MM-DD HH:MM:SS" e "YYYY-MM-DDTHH:MM:SS"
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)

    # Aceitar sufixo Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ_BAHIA)

    return int(dt.timestamp() * 1000)


def _enrich_ops_with_esp_counts(conn, cliente_id: str, machine_id: str, rows: list):
    if not cliente_id or not machine_id or not isinstance(rows, list):
        return

    cur = conn.cursor()

    now_ms = int(datetime.now(TZ_BAHIA).timestamp() * 1000)

    for r in rows:
        ops = r.get("ops") if isinstance(r, dict) else None
        if not isinstance(ops, list) or not ops:
            continue

        for op in ops:
            if not isinstance(op, dict):
                continue

            start_ms = _iso_to_ts_ms_bahia(op.get("started_at") or "")
            end_ms = _iso_to_ts_ms_bahia(op.get("ended_at") or "") or now_ms

            if start_ms is None:
                op["qtd_mat_bom_esp"] = None
                continue

            if end_ms < start_ms:
                end_ms = start_ms

            # Soma do delta (mais confiavel do que max-min em layouts com leituras espaçadas)
            row_sum = cur.execute(
                """
                SELECT
                    COALESCE(SUM(delta), 0) AS sum_delta,
                    MIN(esp_absoluto) AS esp_ini,
                    MAX(esp_absoluto) AS esp_fim
                FROM producao_evento
                WHERE cliente_id = ?
                  AND lower(machine_id) = lower(?)
                  AND ts_ms >= ?
                  AND ts_ms <= ?
                """,
                (cliente_id, machine_id, int(start_ms), int(end_ms)),
            ).fetchone()

            if row_sum:
                op["qtd_mat_bom_esp"] = int(row_sum[0] or 0)
                op["esp_ini"] = int(row_sum[1]) if row_sum[1] is not None else None
                op["esp_fim"] = int(row_sum[2]) if row_sum[2] is not None else None
            else:
                op["qtd_mat_bom_esp"] = 0
                op["esp_ini"] = None
                op["esp_fim"] = None

def _backfill_producao_diaria_cliente_id_all(machine_id: str, cliente_id: str) -> None:
    """
    Backfill retroativo:
    - Preenche cliente_id em TODOS os registros de producao_diaria dessa maquina
      onde cliente_id esteja NULL ou vazio.
    - Faz match tanto no machine_id raw quanto no scoped (cliente_id::machine_id).
    """
    cid = (cliente_id or "").strip()
    if not cid:
        return

    raw_mid = _norm_machine_id(machine_id)
    scoped_mid = f"{cid}::{raw_mid}"

    conn = get_db()
    try:
        conn.execute(
            """
            UPDATE producao_diaria
               SET cliente_id = ?
             WHERE (cliente_id IS NULL OR TRIM(cliente_id) = '')
               AND (machine_id = ? OR machine_id = ?)
            """,
            (cid, raw_mid, scoped_mid),
        )
        conn.commit()
    finally:
        conn.close()

def _ensure_devices_table_min(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            machine_id TEXT,
            alias TEXT,
            last_seen TEXT
        )
    """
    )

    try:
        conn.execute("ALTER TABLE devices ADD COLUMN cliente_id TEXT")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE devices ADD COLUMN created_at TEXT")
    except Exception:
        pass

    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_devices_cliente_id ON devices(cliente_id)")
    except Exception:
        pass

    conn.commit()

def _upsert_device_for_cliente(device_id: str, cliente_id: str, now_str: str, allow_takeover: bool = False) -> bool:
    conn = get_db()
    try:
        _ensure_devices_table_min(conn)

        cur = conn.execute("SELECT device_id, cliente_id FROM devices WHERE device_id = ? LIMIT 1", (device_id,))
        row = cur.fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO devices (device_id, cliente_id, machine_id, alias, created_at, last_seen)
                VALUES (?, ?, NULL, NULL, ?, ?)
            """,
                (device_id, cliente_id, now_str, now_str),
            )
            conn.commit()
            return True

        try:
            owner = row["cliente_id"]
        except Exception:
            owner = row[1] if len(row) > 1 else None

        if owner and owner != cliente_id:
            if allow_takeover:
                conn.execute(
                    """
                    UPDATE devices
                       SET cliente_id = ?,
                           last_seen = ?
                     WHERE device_id = ?
                """,
                    (cliente_id, now_str, device_id),
                )
                conn.commit()
                return True
            return False

        conn.execute(
            """
            UPDATE devices
               SET last_seen = ?,
                   cliente_id = COALESCE(cliente_id, ?)
             WHERE device_id = ?
        """,
            (now_str, cliente_id, device_id),
        )
        conn.commit()
        return True

    finally:
        conn.close()

def _get_linked_machine_for_cliente(device_id: str, cliente_id: str) -> str | None:
    conn = get_db()
    try:
        _ensure_devices_table_min(conn)
        cur = conn.execute(
            """
            SELECT machine_id
              FROM devices
             WHERE device_id = ?
               AND cliente_id = ?
             LIMIT 1
        """,
            (device_id, cliente_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        try:
            return row["machine_id"] or None
        except Exception:
            return row[0] or None
    finally:
        conn.close()

def _ensure_machine_stop_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS machine_stop (
            machine_id TEXT PRIMARY KEY,
            stopped_since_ms INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """
    )
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_machine_stop_updated_at ON machine_stop(updated_at)")
    except Exception:
        pass
    conn.commit()

def _get_stopped_since_ms(machine_id: str) -> int | None:
    conn = get_db()
    try:
        _ensure_machine_stop_table(conn)
        cur = conn.execute("SELECT stopped_since_ms FROM machine_stop WHERE machine_id = ? LIMIT 1", (machine_id,))
        row = cur.fetchone()
        if not row:
            return None
        try:
            v = row["stopped_since_ms"]
        except Exception:
            v = row[0]
        try:
            iv = int(v)
            return iv if iv > 0 else None
        except Exception:
            return None
    finally:
        conn.close()

def _set_stopped_since_ms(machine_id: str, stopped_since_ms: int, updated_at: str):
    conn = get_db()
    try:
        _ensure_machine_stop_table(conn)
        conn.execute(
            """
            INSERT INTO machine_stop (machine_id, stopped_since_ms, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(machine_id) DO UPDATE SET
                stopped_since_ms=excluded.stopped_since_ms,
                updated_at=excluded.updated_at
        """,
            (machine_id, int(stopped_since_ms), updated_at),
        )
        conn.commit()
    finally:
        conn.close()

def _clear_stopped_since(machine_id: str, updated_at: str):
    conn = get_db()
    try:
        _ensure_machine_stop_table(conn)
        conn.execute("DELETE FROM machine_stop WHERE machine_id = ?", (machine_id,))
        conn.commit()
    finally:
        conn.close()

def _ensure_baseline_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS baseline_diario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,
            baseline_esp INTEGER NOT NULL,
            esp_last INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """
    )

    try:
        conn.execute("ALTER TABLE baseline_diario ADD COLUMN cliente_id TEXT")
    except Exception:
        pass

    try:
        conn.execute("DROP INDEX IF EXISTS ux_baseline_diario")
    except Exception:
        pass

    try:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario_cliente
            ON baseline_diario(cliente_id, machine_id, dia_ref)
        """
        )
    except Exception:
        pass

    try:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario_legacy
            ON baseline_diario(machine_id, dia_ref)
            WHERE cliente_id IS NULL
        """
        )
    except Exception:
        pass

    conn.commit()

def _has_baseline_for_day(conn, machine_id: str, dia_ref: str, cliente_id: str | None) -> bool:
    try:
        if cliente_id:
            cur = conn.execute(
                "SELECT 1 FROM baseline_diario WHERE machine_id=? AND dia_ref=? AND cliente_id=? LIMIT 1",
                (machine_id, dia_ref, cliente_id),
            )
        else:
            cur = conn.execute(
                "SELECT 1 FROM baseline_diario WHERE machine_id=? AND dia_ref=? AND cliente_id IS NULL LIMIT 1",
                (machine_id, dia_ref),
            )
        return cur.fetchone() is not None
    except Exception:
        return False

def _insert_baseline_for_day(conn, machine_id: str, dia_ref: str, esp_abs: int, updated_at: str, cliente_id: str | None):
    if cliente_id:
        conn.execute(
            """
            INSERT OR IGNORE INTO baseline_diario (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (cliente_id, machine_id, dia_ref, int(esp_abs), int(esp_abs), updated_at),
        )
    else:
        conn.execute(
            """
            INSERT OR IGNORE INTO baseline_diario (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
            VALUES (NULL, ?, ?, ?, ?, ?)
        """,
            (machine_id, dia_ref, int(esp_abs), int(esp_abs), updated_at),
        )
    conn.commit()

def _load_baseline_esp_for_day(conn, machine_id: str, dia_ref: str, cliente_id: str | None) -> int:
    """Carrega baseline_esp do dia (multi-tenant). Retorna 0 se não existir."""
    try:
        if cliente_id:
            cur = conn.execute(
                "SELECT baseline_esp FROM baseline_diario WHERE machine_id=? AND dia_ref=? AND cliente_id=? LIMIT 1",
                (machine_id, dia_ref, cliente_id),
            )
        else:
            cur = conn.execute(
                "SELECT baseline_esp FROM baseline_diario WHERE machine_id=? AND dia_ref=? AND cliente_id IS NULL LIMIT 1",
                (machine_id, dia_ref),
            )
        row = cur.fetchone()
        if not row:
            return 0
        try:
            return int(row[0] or 0)
        except Exception:
            return 0
    except Exception:
        return 0

# =====================================================
# RESET REMOTO DO CONTADOR NO ESP (cmd reset_counter)
# =====================================================

def _ensure_reset_cmd_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS esp_reset_cmd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id TEXT,
            machine_id TEXT NOT NULL,
            cmd_id TEXT NOT NULL,
            pending INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            applied_at TEXT
        )
        """
    )
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_esp_reset_cmd_mid_pending ON esp_reset_cmd(machine_id, pending)")
    except Exception:
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_esp_reset_cmd_cid_mid_pending ON esp_reset_cmd(cliente_id, machine_id, pending)")
    except Exception:
        pass
    conn.commit()

def _issue_reset_cmd(conn, cliente_id: str | None, machine_id: str, created_at: str) -> str:
    _ensure_reset_cmd_table(conn)
    cmd_id = str(uuid.uuid4())
    if cliente_id:
        conn.execute(
            """
            INSERT INTO esp_reset_cmd(cliente_id, machine_id, cmd_id, pending, created_at)
            VALUES(?, ?, ?, 1, ?)
            """,
            (str(cliente_id), str(machine_id), cmd_id, created_at),
        )
    else:
        conn.execute(
            """
            INSERT INTO esp_reset_cmd(cliente_id, machine_id, cmd_id, pending, created_at)
            VALUES(NULL, ?, ?, 1, ?)
            """,
            (str(machine_id), cmd_id, created_at),
        )
    conn.commit()
    return cmd_id

def _get_pending_reset_cmd(conn, cliente_id: str | None, machine_id: str) -> str | None:
    _ensure_reset_cmd_table(conn)
    try:
        if cliente_id:
            row = conn.execute(
                """
                SELECT cmd_id FROM esp_reset_cmd
                WHERE cliente_id=? AND machine_id=? AND pending=1
                ORDER BY id DESC LIMIT 1
                """,
                (str(cliente_id), str(machine_id)),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT cmd_id FROM esp_reset_cmd
                WHERE cliente_id IS NULL AND machine_id=? AND pending=1
                ORDER BY id DESC LIMIT 1
                """,
                (str(machine_id),),
            ).fetchone()
        if not row:
            return None
        return str(row[0])
    except Exception:
        return None

def _ack_reset_cmd(conn, cliente_id: str | None, machine_id: str, cmd_id: str, applied_at: str) -> bool:
    _ensure_reset_cmd_table(conn)
    try:
        if not cmd_id:
            return False

        if cliente_id:
            cur = conn.execute(
                """
                UPDATE esp_reset_cmd
                SET pending=0, applied_at=?
                WHERE cliente_id=? AND machine_id=? AND cmd_id=? AND pending=1
                """,
                (applied_at, str(cliente_id), str(machine_id), str(cmd_id)),
            )
        else:
            cur = conn.execute(
                """
                UPDATE esp_reset_cmd
                SET pending=0, applied_at=?
                WHERE cliente_id IS NULL AND machine_id=? AND cmd_id=? AND pending=1
                """,
                (applied_at, str(machine_id), str(cmd_id)),
            )
        conn.commit()
        return (cur.rowcount or 0) > 0
    except Exception:
        return False

def _force_baseline_for_day(conn, machine_id: str, dia_ref: str, baseline_esp: int, updated_at: str, cliente_id: str | None):
    _ensure_baseline_table(conn)

    if cliente_id:
        cur = conn.execute(
            """
            UPDATE baseline_diario
            SET baseline_esp=?, esp_last=?, updated_at=?
            WHERE machine_id=? AND dia_ref=? AND cliente_id=?
            """,
            (int(baseline_esp), int(baseline_esp), str(updated_at), str(machine_id), str(dia_ref), str(cliente_id)),
        )
        if (cur.rowcount or 0) <= 0:
            conn.execute(
                """
                INSERT OR REPLACE INTO baseline_diario(machine_id, dia_ref, baseline_esp, esp_last, updated_at, cliente_id)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (str(machine_id), str(dia_ref), int(baseline_esp), int(baseline_esp), str(updated_at), str(cliente_id)),
            )
    else:
        cur = conn.execute(
            """
            UPDATE baseline_diario
            SET baseline_esp=?, esp_last=?, updated_at=?
            WHERE machine_id=? AND dia_ref=? AND cliente_id IS NULL
            """,
            (int(baseline_esp), int(baseline_esp), str(updated_at), str(machine_id), str(dia_ref)),
        )
        if (cur.rowcount or 0) <= 0:
            conn.execute(
                """
                INSERT OR REPLACE INTO baseline_diario(machine_id, dia_ref, baseline_esp, esp_last, updated_at, cliente_id)
                VALUES(?, ?, ?, ?, ?, NULL)
                """,
                (str(machine_id), str(dia_ref), int(baseline_esp), int(baseline_esp), str(updated_at)),
            )

    conn.commit()

def _ensure_producao_evento_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS producao_evento (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id TEXT,
            machine_id TEXT NOT NULL,
            ts_ms INTEGER NOT NULL,
            esp_absoluto INTEGER NOT NULL,
            delta INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """
    )
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_producao_evento_mid_ts ON producao_evento(machine_id, ts_ms)")
    except Exception:
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_producao_evento_cid_mid_ts ON producao_evento(cliente_id, machine_id, ts_ms)")
    except Exception:
        pass
    conn.commit()

def _registrar_evento_producao(cliente_id: str, machine_id: str, ts_ms: int, esp_absoluto: int, delta: int, created_at: str) -> None:
    if delta <= 0:
        return

    conn = get_db()
    try:
        _ensure_producao_evento_table(conn)
        conn.execute(
            """
            INSERT INTO producao_evento (cliente_id, machine_id, ts_ms, esp_absoluto, delta, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (cliente_id, machine_id, int(ts_ms), int(esp_absoluto), int(delta), created_at),
        )
        conn.commit()
    finally:
        conn.close()

@machine_bp.route("/admin/hard-reset", methods=["POST"])
def admin_hard_reset():
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    conn = get_db()
    cur = conn.cursor()

    tables = [
        "producao_diaria",
        "producao_horaria",
        "baseline_diario",
        "refugo_horaria",
        "machine_config",
        "machine_stop",
    ]

    deleted = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(1) FROM {t}")
            before = cur.fetchone()[0]
        except Exception:
            before = None

        try:
            cur.execute(f"DELETE FROM {t}")
            deleted[t] = before
        except Exception:
            deleted[t] = "skipped"

    try:
        cur.execute("DELETE FROM sqlite_sequence")
    except Exception:
        pass

    conn.commit()
    conn.close()

    return jsonify(
        {
            "ok": True,
            "deleted_tables": deleted,
            "note": "Banco limpo. Recomece a contagem a partir do próximo envio do ESP.",
        }
    )


# -----------------------------
# CONFIG V2 (SHIFTS + BREAKS)
# Persistencia JSON (SQLite) + compatibilidade
# -----------------------------

_MACHINE_CFG_JSON_READY = False

def _cfgv2_db_path() -> str:
    p = (os.getenv("INDFLOW_DB_PATH") or "indflow.db").strip()
    return p or "indflow.db"

def _cfgv2_db_init():
    global _MACHINE_CFG_JSON_READY
    if _MACHINE_CFG_JSON_READY:
        return

    conn = sqlite3.connect(_cfgv2_db_path(), check_same_thread=False)
    try:
        # Tabela pode existir com schema legado (sem config_json). Nao recriamos; apenas garantimos colunas.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS machine_config (
                machine_id TEXT PRIMARY KEY
            )
            """
        )

        # Migracao defensiva: adiciona colunas se estiverem faltando.
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(machine_config)").fetchall()]
        except Exception:
            cols = []

        if "config_json" not in cols:
            try:
                conn.execute("ALTER TABLE machine_config ADD COLUMN config_json TEXT")
            except Exception:
                pass

        if "updated_at" not in cols:
            try:
                conn.execute("ALTER TABLE machine_config ADD COLUMN updated_at TEXT")
            except Exception:
                pass

        conn.commit()

        # Marca pronto apenas quando a coluna existe (ou foi criada).
        try:
            cols2 = [r[1] for r in conn.execute("PRAGMA table_info(machine_config)").fetchall()]
        except Exception:
            cols2 = cols

        _MACHINE_CFG_JSON_READY = ("config_json" in cols2 and "updated_at" in cols2)
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _cfgv2_db_upsert(machine_id: str, cfg_v2: dict):
    mid = _norm_machine_id(machine_id)
    payload = json.dumps(cfg_v2, ensure_ascii=True, separators=(",", ":"))
    updated_at = datetime.now(_get_tz()).isoformat(timespec="seconds")
    conn = sqlite3.connect(_cfgv2_db_path(), check_same_thread=False)
    try:
        conn.execute(
            """
            INSERT INTO machine_config (machine_id, config_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(machine_id) DO UPDATE SET
                config_json = excluded.config_json,
                updated_at = excluded.updated_at
            """,
            (mid, payload, updated_at),
        )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _cfgv2_db_load(machine_id: str) -> dict | None:
    """Carrega config_v2 persistida (machine_config.config_json)."""
    mid = _norm_machine_id(_unscope_machine_id(machine_id))
    conn = sqlite3.connect(_cfgv2_db_path(), check_same_thread=False)
    try:
        cur = conn.execute("SELECT config_json FROM machine_config WHERE machine_id = ? LIMIT 1", (mid,))
        row = cur.fetchone()
        if not row:
            return None
        raw = row[0]
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _cfgv2_load_apply(m: dict, machine_id: str) -> None:
    """Recarrega do banco e aplica em memoria. Necessario apos deploy (memoria zera)."""
    try:
        _cfgv2_db_init()
        cfg = _cfgv2_db_load(machine_id)
        if cfg:
            _cfgv2_apply_to_memory(m, cfg)
    except Exception:
        pass

def _cfgv2_hhmm_to_min(hhmm: str) -> int:
    s = str(hhmm or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if not m:
        raise ValueError(f"Horario invalido: {s}")
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError(f"Horario invalido: {s}")
    return hh * 60 + mm

def _cfgv2_shift_duration(start_min: int, end_min: int) -> int:
    if end_min <= start_min:
        return (24 * 60 - start_min) + end_min
    return end_min - start_min

def _cfgv2_break_rel(shift_start: int, br_start: int, br_end: int) -> tuple[int, int]:
    # Converte break para eixo do turno (0..dur)
    b0 = br_start
    b1 = br_end
    if b0 < shift_start:
        b0 += 24 * 60
    if b1 < shift_start:
        b1 += 24 * 60
    if b1 <= b0:
        b1 += 24 * 60
    return (b0 - shift_start, b1 - shift_start)

def _cfgv2_validate(raw: dict) -> dict:
    cfg = {}

    # active_days: 1..7
    ad = raw.get("active_days")
    if ad is None:
        ad = [1, 2, 3, 4, 5, 6, 7]
    if not isinstance(ad, list):
        raise ValueError("active_days deve ser lista")
    days = []
    for d in ad:
        try:
            di = int(d)
        except Exception:
            continue
        if 1 <= di <= 7 and di not in days:
            days.append(di)
    if not days:
        days = [1, 2, 3, 4, 5, 6, 7]
    cfg["active_days"] = days

    # units
    units = raw.get("units") if isinstance(raw.get("units"), dict) else {}
    u1 = str(units.get("u1") or raw.get("unidade_1") or "pcs").strip() or "pcs"
    u2 = str(units.get("u2") or raw.get("unidade_2") or "").strip()
    conv_raw = units.get("conv_m_per_pcs")
    if conv_raw is None:
        conv_raw = raw.get("conv_m_per_pcs", raw.get("conv_m_por_pcs"))
    try:
        conv = float(conv_raw) if conv_raw is not None and str(conv_raw).strip() != "" else None
    except Exception:
        conv = None
    if conv is not None and conv <= 0:
        conv = None
    cfg["units"] = {"u1": u1, "u2": (u2 or None), "conv_m_per_pcs": conv}

    # oee
    oee = raw.get("oee") if isinstance(raw.get("oee"), dict) else {}
    ideal_raw = oee.get("ideal_sec_per_piece")
    try:
        ideal = float(ideal_raw) if ideal_raw is not None and str(ideal_raw).strip() != "" else None
    except Exception:
        ideal = None
    if ideal is not None and ideal <= 0:
        ideal = None

    ncss_raw = oee.get("no_count_stop_sec", raw.get("no_count_stop_sec"))
    try:
        ncss = int(ncss_raw) if ncss_raw is not None and str(ncss_raw).strip() != "" else None
    except Exception:
        ncss = None
    if ncss is not None and ncss < 5:
        ncss = None

    ramp_raw = oee.get("ramp_percent", raw.get("rampa", raw.get("rampa_percentual", 0)))
    try:
        ramp = int(ramp_raw or 0)
    except Exception:
        ramp = 0
    if ramp < 0:
        ramp = 0
    if ramp > 100:
        ramp = 100

    cfg["oee"] = {"ideal_sec_per_piece": ideal, "no_count_stop_sec": ncss, "ramp_percent": ramp}

    # shifts
    shifts = raw.get("shifts")
    if not isinstance(shifts, list) or not shifts:
        raise ValueError("shifts obrigatorio (lista)")
    out_shifts = []
    for s in shifts:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name") or "").strip() or "A"
        start = str(s.get("start") or s.get("inicio") or "").strip()
        end = str(s.get("end") or s.get("fim") or "").strip()
        if not start or not end:
            raise ValueError("Turno sem start/end")
        start_min = _cfgv2_hhmm_to_min(start)
        end_min = _cfgv2_hhmm_to_min(end)
        dur = _cfgv2_shift_duration(start_min, end_min)
        if dur <= 0:
            raise ValueError("Turno invalido")

        try:
            meta_pcs = int(s.get("meta_pcs", s.get("meta_turno", 0)) or 0)
        except Exception:
            meta_pcs = 0
        if meta_pcs < 0:
            meta_pcs = 0

        breaks_raw = s.get("breaks") if isinstance(s.get("breaks"), list) else []
        breaks_out = []
        breaks_min = 0
        for br in breaks_raw:
            if not isinstance(br, dict):
                continue
            br_name = str(br.get("name") or "").strip() or "Pausa"
            br_start = str(br.get("start") or "").strip()
            br_end = str(br.get("end") or "").strip()
            if not br_start or not br_end:
                continue
            b0 = _cfgv2_hhmm_to_min(br_start)
            b1 = _cfgv2_hhmm_to_min(br_end)
            rel0, rel1 = _cfgv2_break_rel(start_min, b0, b1)
            if rel1 <= rel0:
                continue
            if rel0 < 0 or rel1 > dur:
                raise ValueError("Pausa deve estar dentro do turno")
            breaks_min += (rel1 - rel0)
            breaks_out.append({"name": br_name, "start": br_start, "end": br_end})

        planned = dur - breaks_min
        if planned <= 0:
            raise ValueError("Turno sem tempo planejado (pausas consomem tudo)")

        out_shifts.append(
            {
                "name": name,
                "start": start,
                "end": end,
                "meta_pcs": meta_pcs,
                "breaks": breaks_out,
                "calc": {"duration_min": dur, "breaks_min": breaks_min, "planned_min": planned},
            }
        )
    cfg["shifts"] = out_shifts
    return cfg

def _cfgv2_normalize_payload(data: dict) -> dict:
    if isinstance(data.get("shifts"), list) and data.get("shifts"):
        return _cfgv2_validate(data)

    # legado -> v2
    try:
        meta_turno = int(data.get("meta_turno", 0))
    except Exception:
        meta_turno = 0
    inicio = str(data.get("inicio") or "").strip()
    fim = str(data.get("fim") or "").strip()
    if not inicio or not fim or meta_turno <= 0:
        raise ValueError("Dados invalidos: meta_turno/inicio/fim")
    try:
        rampa = int(data.get("rampa", 0))
    except Exception:
        rampa = 0
    try:
        ncss = int(data.get("no_count_stop_sec", 0) or 0)
    except Exception:
        ncss = 0

    u1 = str(data.get("unidade_1") or "pcs").strip() or "pcs"
    u2 = str(data.get("unidade_2") or "").strip()
    try:
        conv = float(data.get("conv_m_por_pcs", 0) or 0)
    except Exception:
        conv = 0.0
    if conv <= 0:
        conv = None

    cfg = {
        "active_days": [1, 2, 3, 4, 5, 6, 7],
        "shifts": [{"name": "A", "start": inicio, "end": fim, "meta_pcs": meta_turno, "breaks": []}],
        "oee": {"ideal_sec_per_piece": None, "no_count_stop_sec": (ncss if ncss >= 5 else None), "ramp_percent": rampa},
        "units": {"u1": u1, "u2": (u2 or None), "conv_m_per_pcs": conv},
    }
    return _cfgv2_validate(cfg)

def _cfgv2_weekday(dt) -> int:
    return int(dt.weekday()) + 1

def _cfgv2_is_now_in_shift(dt_now, shift: dict) -> bool:
    start_min = _cfgv2_hhmm_to_min(shift.get("start"))
    end_min = _cfgv2_hhmm_to_min(shift.get("end"))
    now_min = int(dt_now.hour) * 60 + int(dt_now.minute)
    if end_min > start_min:
        return start_min <= now_min < end_min
    return (now_min >= start_min) or (now_min < end_min)

def _cfgv2_pick_shift(cfg_v2: dict, dt_now):
    shifts = cfg_v2.get("shifts") or []
    for s in shifts:
        try:
            if _cfgv2_is_now_in_shift(dt_now, s):
                return s
        except Exception:
            continue
    return shifts[0] if shifts else None

def _cfgv2_apply_to_memory(m: dict, cfg_v2: dict):
    m["config_v2"] = cfg_v2
    m["active_days"] = cfg_v2.get("active_days") or [1, 2, 3, 4, 5, 6, 7]
    m["shifts"] = cfg_v2.get("shifts") or []

    # Meta do dia: soma das metas (pcs) de todos os turnos configurados
    meta_dia = 0
    for s in (m.get("shifts") or []):
        try:
            meta_dia += int(s.get("meta_pcs", s.get("meta_turno", 0)) or 0)
        except Exception:
            continue
    m["meta_dia"] = int(meta_dia)

    oee = cfg_v2.get("oee") or {}
    m["ideal_sec_per_piece"] = oee.get("ideal_sec_per_piece")
    if oee.get("no_count_stop_sec") is not None:
        m["no_count_stop_sec"] = int(oee.get("no_count_stop_sec") or 0)
    m["rampa_percentual"] = int(oee.get("ramp_percent") or 0)

    units = cfg_v2.get("units") or {}
    aplicar_unidades(m, units.get("u1"), units.get("u2"))
    if units.get("conv_m_per_pcs") is not None:
        try:
            m["conv_m_por_pcs"] = float(units.get("conv_m_per_pcs"))
        except Exception:
            pass

    dt_now = datetime.now(_get_tz())
    m["is_active_day"] = (_cfgv2_weekday(dt_now) in (m.get("active_days") or []))

    shift = _cfgv2_pick_shift(cfg_v2, dt_now)
    m["active_shift"] = shift

    # Campos legados: usa turno ativo (ou primeiro)
    if not shift:
        # Sem turno ativo: ainda expõe meta_dia e mantém listas vazias
        m["meta_turno_ativo"] = 0
        m["meta_turno"] = int(m.get("meta_dia", 0) or 0)
        m["turno_inicio"] = None
        m["turno_fim"] = None
        m["horas_turno"] = []
        m["meta_por_hora"] = []
        return

    active_meta = int(shift.get("meta_pcs", 0) or 0)
    m["meta_turno_ativo"] = active_meta
    # Legado: meta_turno representa a meta do DIA (soma dos turnos) para o card e para producao_diaria
    m["meta_turno"] = int(m.get("meta_dia", 0) or 0) if int(m.get("meta_dia", 0) or 0) > 0 else active_meta
    m["turno_inicio"] = shift.get("start")
    m["turno_fim"] = shift.get("end")

    # Horas (slots 1h) e meta_por_hora com pausa heuristica (>=30min de sobreposicao zera slot)
    start_min = _cfgv2_hhmm_to_min(shift.get("start"))
    end_min = _cfgv2_hhmm_to_min(shift.get("end"))
    dur = _cfgv2_shift_duration(start_min, end_min)

    break_intervals = []
    for br in (shift.get("breaks") or []):
        try:
            b0 = _cfgv2_hhmm_to_min(br.get("start"))
            b1 = _cfgv2_hhmm_to_min(br.get("end"))
            rel0, rel1 = _cfgv2_break_rel(start_min, b0, b1)
            break_intervals.append((rel0, rel1))
        except Exception:
            continue

    horas_turno = []
    slot_is_break = []
    t = 0
    while t < dur:
        t2 = min(dur, t + 60)
        abs0 = (start_min + t) % (24 * 60)
        abs1 = (start_min + t2) % (24 * 60)
        h0 = f"{abs0//60:02d}:{abs0%60:02d}"
        h1 = f"{abs1//60:02d}:{abs1%60:02d}"
        horas_turno.append(f"{h0} - {h1}")

        overlap = 0
        for b0, b1 in break_intervals:
            lo = max(t, b0)
            hi = min(t2, b1)
            if hi > lo:
                overlap = max(overlap, hi - lo)
        slot_is_break.append(True if overlap >= 30 else False)

        t = t2

    prod_slots = [i for i, isb in enumerate(slot_is_break) if not isb]
    metas = [0 for _ in horas_turno]

    meta_turno = int(m.get("meta_turno_ativo", m.get("meta_turno", 0)) or 0)
    if meta_turno > 0 and prod_slots:
        ramp = int(m.get("rampa_percentual", 0) or 0)
        if ramp < 0:
            ramp = 0
        if ramp > 100:
            ramp = 100

        qtd_prod = len(prod_slots)
        meta_base = meta_turno / qtd_prod
        meta_primeira = round(meta_base * (ramp / 100))
        if meta_primeira < 0:
            meta_primeira = 0
        if meta_primeira > meta_turno:
            meta_primeira = meta_turno

        restante = meta_turno - meta_primeira
        horas_restantes = qtd_prod - 1

        metas[prod_slots[0]] = meta_primeira
        if horas_restantes > 0:
            base = restante // horas_restantes
            sobra = restante % horas_restantes
            for i in range(horas_restantes):
                metas[prod_slots[i + 1]] = int(base + (1 if i < sobra else 0))

    for i, isb in enumerate(slot_is_break):
        if isb:
            metas[i] = 0

    m["horas_turno"] = horas_turno
    m["meta_por_hora"] = metas

    # runtime
    # Nao resetar baseline_hora/producao_hora ao aplicar config_v2.
    # Esses campos sao controlados pelo fluxo de contagem e persistencia (evento/status).
    if m.get("baseline_hora") is None:
        m["baseline_hora"] = int(m.get("esp_absoluto", 0) or 0)
    try:
        if m.get("ultima_hora") is None:
            m["ultima_hora"] = calcular_ultima_hora_idx(m)
    except Exception:
        pass

@machine_bp.route("/machine/config", methods=["POST"])
def configurar_maquina():
    data = request.get_json(silent=True) or {}
    machine_id_in = (data.get("machine_id", "maquina01") or "").strip()
    machine_id = _norm_machine_id(_unscope_machine_id(machine_id_in))
    m = get_machine(machine_id)

    cliente_id = None
    try:
        cliente_id = _get_cliente_id_for_request()
    except Exception:
        cliente_id = None
    m["cliente_id"] = cliente_id

    # Payload: v2 (shifts/breaks) ou legado (inicio/fim/meta_turno)
    try:
        cfg_v2 = _cfgv2_normalize_payload(data)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception:
        return jsonify({"error": "Payload invalido"}), 400

    # Persistencia JSON (opcao 1)
    try:
        _cfgv2_db_init()
        _cfgv2_db_upsert(machine_id, cfg_v2)
    except Exception as e:
        return jsonify({"error": "Falha ao persistir config_v2 no SQLite", "detail": str(e)}), 500

    # Memoria (compatibilidade com calculos atuais)
    try:
        _cfgv2_apply_to_memory(m, cfg_v2)
    except Exception:
        pass

    # Persistencia antiga (repo) - manter compatibilidade com modulos existentes
    try:
        upsert_machine_config(machine_id, m)
    except Exception:
        pass

    return jsonify(
        {
            "status": "configurado",
            "machine_id": machine_id,
            "config_v2": cfg_v2,
            "meta_por_hora": m.get("meta_por_hora", []),
            "unidade_1": m.get("unidade_1"),
            "unidade_2": m.get("unidade_2"),
            "conv_m_por_pcs": m.get("conv_m_por_pcs"),
        }
    )

def _sum_prev_hours_produzido(conn, machine_id: str, cliente_id: str, dia_ref: str, hora_idx: int) -> int:
    """Soma o produzido de horas anteriores (hora_idx < atual) para o mesmo dia.
    Isso permite derivar baseline_hora sem depender do reset do ESP nem de baseline_repo.
    """
    try:
        if not isinstance(hora_idx, int) or hora_idx <= 0:
            return 0

        mid = str(machine_id or "").strip()
        cid = str(cliente_id or "").strip()

        # Detecta colunas disponíveis (cliente_id pode ou não existir dependendo da migração)
        cols = []
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(producao_horaria)").fetchall()]
        except Exception:
            cols = []

        has_cliente_col = "cliente_id" in cols

        params = [dia_ref, hora_idx, mid]
        sql = "SELECT COALESCE(SUM(CAST(produzido AS INTEGER)), 0) FROM producao_horaria WHERE data_ref = ? AND hora_idx < ? AND machine_id = ?"

        if has_cliente_col and cid:
            sql += " AND cliente_id = ?"
            params.append(cid)

        row = conn.execute(sql, params).fetchone()
        total = int(row[0] or 0) if row else 0
        if total < 0:
            total = 0
        return total
    except Exception:
        return 0
def _sync_producao_diaria_absoluta(machine_id: str, cliente_id: str | None, dia_ref: str, produzido_abs: int, meta: int | None = None) -> None:
    """
    Garante que producao_diaria reflita o valor absoluto (producao_turno) e nao um acumulado incremental.
    Isso elimina o efeito de "dobrar" quando o mesmo pacote do ESP e processado mais de uma vez.
    """
    mid_raw = _norm_machine_id(_unscope_machine_id(machine_id))
    cid = (cliente_id or "").strip() or None
    dia_ref = (dia_ref or "").strip()
    if not dia_ref:
        return

    try:
        produzido_abs = int(produzido_abs or 0)
    except Exception:
        produzido_abs = 0
    if produzido_abs < 0:
        produzido_abs = 0

    try:
        meta_int = int(meta or 0)
    except Exception:
        meta_int = 0
    if meta_int < 0:
        meta_int = 0

    percentual = 0
    try:
        if meta_int > 0:
            percentual = int(round((produzido_abs / float(meta_int)) * 100))
    except Exception:
        percentual = 0

    conn = get_db()
    try:
        cols = []
        try:
            cols = conn.execute("PRAGMA table_info(producao_diaria)").fetchall()
        except Exception:
            cols = []

        colnames = {str(c[1]).lower(): True for c in (cols or [])}
        has_cliente_id = ("cliente_id" in colnames)

        set_parts = ["produzido = ?", "percentual = ?"]
        params_base = [produzido_abs, percentual]

        if "pecas_boas" in colnames:
            set_parts.append("pecas_boas = ?")
            params_base.append(produzido_abs)
        if "refugo_total" in colnames:
            set_parts.append("refugo_total = ?")
            params_base.append(0)
        if "meta" in colnames:
            set_parts.append("meta = ?")
            params_base.append(meta_int)

        set_sql = ", ".join(set_parts)

        mids = [mid_raw] + ([f"{cid}::{mid_raw}"] if cid else [])

        # Atualiza primeiro (se existir)
        updated_any = False
        for mid in mids:
            try:
                if has_cliente_id and cid is not None:
                    cur = conn.execute(
                        f"UPDATE producao_diaria SET {set_sql} WHERE data = ? AND machine_id = ? AND cliente_id = ?",
                        tuple(params_base + [dia_ref, mid, cid]),
                    )
                else:
                    cur = conn.execute(
                        f"UPDATE producao_diaria SET {set_sql} WHERE data = ? AND machine_id = ?",
                        tuple(params_base + [dia_ref, mid]),
                    )
                if cur and getattr(cur, "rowcount", 0) > 0:
                    updated_any = True
            except Exception:
                pass

        # Se nao existia, insere uma linha minima
        if not updated_any:
            try:
                if has_cliente_id and cid is not None:
                    conn.execute(
                        "INSERT INTO producao_diaria (machine_id, cliente_id, data, produzido, meta, percentual) VALUES (?, ?, ?, ?, ?, ?)",
                        (mids[0], cid, dia_ref, produzido_abs, meta_int, percentual),
                    )
                else:
                    conn.execute(
                        "INSERT INTO producao_diaria (machine_id, data, produzido, meta, percentual) VALUES (?, ?, ?, ?, ?)",
                        (mids[0], dia_ref, produzido_abs, meta_int, percentual),
                    )
            except Exception:
                pass

        # Anti-duplicacao: pode existir 2 linhas no producao_diaria (machine_id puro e machine_id escopado 'cliente::machine').
        # O endpoint de Historico normalmente soma por dia; se existirem as duas, o total fica dobrado.
        # Padrao adotado: manter apenas o machine_id SEM escopo na producao_diaria quando a tabela nao tem cliente_id.
        try:
            if (cid is not None) and (not has_cliente_id):
                scoped_mid = f"{cid}::{mid_raw}"
                conn.execute(
                    "DELETE FROM producao_diaria WHERE data = ? AND machine_id = ?",
                    (dia_ref, scoped_mid),
                )
        except Exception:
            pass

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

@machine_bp.route("/machine/update", methods=["POST"])
def update_machine():
    data = request.get_json() or {}

    cliente = _get_cliente_from_api_key()
    if not cliente:
        return jsonify({"error": "unauthorized"}), 401

    cliente_id = cliente["id"]
    allow_takeover = False

    device_id = norm_device_id(data.get("mac") or data.get("device_id") or "")
    if device_id:
        agora = now_bahia()
        now_str = agora.strftime("%Y-%m-%d %H:%M:%S")

        allow_takeover = (os.getenv("INDFLOW_ALLOW_DEVICE_TAKEOVER") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

        ok_owner = _upsert_device_for_cliente(device_id=device_id, cliente_id=cliente_id, now_str=now_str, allow_takeover=allow_takeover)
        if not ok_owner:
            return jsonify({"error": "device pertence a outro cliente", "hint": "se for DEV, libere takeover setando INDFLOW_ALLOW_DEVICE_TAKEOVER=1"}), 403

        try:
            touch_device_seen(device_id)
        except Exception:
            pass

    linked_machine = _get_linked_machine_for_cliente(device_id, cliente_id) if device_id else None
    if linked_machine:
        machine_id = _norm_machine_id(linked_machine)
    else:
        machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    m = get_machine(machine_id)

    m["cliente_id"] = cliente_id

    # Recarrega config persistida (pos-deploy)
    _cfgv2_load_apply(m, machine_id)

    verificar_reset_diario(m, machine_id)

    prev_status = (m.get("status") or "").strip().upper()

    new_status = (data.get("status", "DESCONHECIDO") or "DESCONHECIDO").strip().upper()

    m["status"] = new_status
    m["esp_absoluto"] = int(data.get("producao_turno", 0) or 0)

    # ACK do reset do ESP (zera contador absoluto de forma explicita)
    reset_ack = _safe_int(data.get("reset_ack"), 0)
    cmd_id_in = (data.get("cmd_id") or "").strip()

    if reset_ack == 1:
        try:
            conn_ack = get_db()
            applied = _ack_reset_cmd(conn_ack, cliente_id, machine_id, cmd_id_in, now_bahia().isoformat())
            if applied:
                dia_ref_ack = dia_operacional_ref_str(now_bahia())
                # Forca baseline do dia para 0 (o ESP vai voltar a contar a partir de 0)
                _force_baseline_for_day(conn_ack, machine_id, dia_ref_ack, 0, now_bahia().isoformat(), cliente_id)
                # Compat: baseline legado (sem cliente_id) se existir
                try:
                    _force_baseline_for_day(conn_ack, machine_id, dia_ref_ack, 0, now_bahia().isoformat(), None)
                except Exception:
                    pass

                m["_last_reset_cmd_id"] = cmd_id_in
                m["_last_reset_applied_at"] = now_bahia().isoformat()
        except Exception:
            pass

    try:
        ts_ms_in = _get_ts_ms_from_payload(data)

        agora_lc = now_bahia()
        now_ms_lc = int(agora_lc.timestamp() * 1000)

        effective_ts_ms = int(ts_ms_in) if ts_ms_in is not None else int(now_ms_lc)

        m["_last_esp_ts_ms_seen"] = effective_ts_ms
        m["_last_esp_ts_source"] = "esp" if ts_ms_in is not None else "server_fallback"

        try:
            m["_last_esp_ts_iso_local"] = datetime.fromtimestamp(int(effective_ts_ms) / 1000, TZ_BAHIA).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            m["_last_esp_ts_iso_local"] = None

        esp_now = int(m.get("esp_absoluto", 0) or 0)
        esp_prev = m.get("_last_esp_abs_seen")
        if esp_prev is None:
            esp_prev = esp_now
        esp_prev = int(esp_prev)

        try:
            delta_evt = esp_now - esp_prev
            if delta_evt > 0:
                created_at_evt = agora_lc.strftime("%Y-%m-%d %H:%M:%S")
                _registrar_evento_producao(
                    cliente_id=str(cliente_id),
                    machine_id=str(machine_id),
                    ts_ms=int(effective_ts_ms),
                    esp_absoluto=int(esp_now),
                    delta=int(delta_evt),
                    created_at=created_at_evt,
                )
        except Exception:
            pass

        if delta_evt > 0:
            m["_last_count_ts_ms"] = int(effective_ts_ms)
        elif m.get("_last_count_ts_ms") is None:
            m["_last_count_ts_ms"] = int(effective_ts_ms)
        m["_last_esp_abs_seen"] = esp_now
    except Exception:
        pass

    m["run"] = _safe_int(data.get("run", 0), 0)

    try:
        agora = now_bahia()
        updated_at = agora.strftime("%Y-%m-%d %H:%M:%S")
        now_ms = int(agora.timestamp() * 1000)

        if new_status == "AUTO":
            _clear_stopped_since(machine_id, updated_at)
            m["stopped_since_ms"] = None
        else:
            existing = _get_stopped_since_ms(machine_id)
            if existing is None:
                _set_stopped_since_ms(machine_id, now_ms, updated_at)
                m["stopped_since_ms"] = now_ms
            else:
                m["stopped_since_ms"] = existing
    except Exception:
        pass

    baseline_initialized = False
    try:
        agora = now_bahia()
        dia_ref = dia_operacional_ref_str(agora)
        updated_at = agora.strftime("%Y-%m-%d %H:%M:%S")

        conn = get_db()
        try:
            _ensure_baseline_table(conn)

            if not _has_baseline_for_day(conn, machine_id, dia_ref, cliente_id):
                _insert_baseline_for_day(conn, machine_id, dia_ref, int(m["esp_absoluto"]), updated_at, cliente_id)
                baseline_initialized = True

            baseline_esp = _load_baseline_esp_for_day(conn, machine_id, dia_ref, cliente_id)
        finally:
            conn.close()

    except Exception:
        baseline_initialized = False
        baseline_esp = 0

    # IMPORTANTE: não depende do baseline_repo (evita mismatch de scope). Usa baseline_diario direto.
    try:
        m["baseline_diario"] = int(baseline_esp or 0)
    except Exception:
        m["baseline_diario"] = 0

    # Produção incremental (ESP manda contador absoluto)
    producao_atual = max(int(m.get("esp_absoluto", 0) or 0) - int(m.get("baseline_diario", 0) or 0), 0)

    # PRIMEIRO PACOTE DO DIA: apenas inicializa baseline e começa do zero.
    if baseline_initialized:
        producao_atual = 0
        m["_last_esp_abs_seen"] = int(m.get("esp_absoluto", 0) or 0)

    m["producao_turno"] = producao_atual

    if int(m.get("meta_turno", 0) or 0) > 0:
        m["percentual_turno"] = round((producao_atual / m["meta_turno"]) * 100)
    else:
        m["percentual_turno"] = 0


    atualizar_producao_hora(m)

    # Persistencia por hora (Historico): grava snapshot no banco a cada update do ESP
    _persist_snapshot_from_machine(m, machine_id=str(machine_id), ts_ms=int(effective_ts_ms))


    # Fix: garante que producao_diaria seja valor absoluto do turno (evita Historico dobrar)
    try:
        dia_ref_pd = dia_operacional_ref_str(now_bahia())
        try:
            prod_abs = int(m.get("producao_turno", 0) or 0)
        except Exception:
            prod_abs = 0
        try:
            meta_abs = int(m.get("meta_turno", 0) or 0)
        except Exception:
            meta_abs = 0
        _sync_producao_diaria_absoluta(machine_id=str(machine_id), cliente_id=(m.get("cliente_id") or cid_req), dia_ref=str(dia_ref_pd), produzido_abs=int(prod_abs), meta=int(meta_abs))
    except Exception:
        pass


    try:
        agora_np = now_bahia()
        processar_nao_programado(m=m, machine_id=machine_id, cliente_id=cliente_id, esp_absoluto=int(m.get("esp_absoluto", 0) or 0), agora=agora_np)
    except Exception:
        try:
            processar_nao_programado(m, machine_id, cliente_id)
        except Exception:
            pass
    # --------------------------------------------------
    # TIMELINE (RUN/STOP) - persistencia no /machine/update
    # --------------------------------------------------
    # Sem depender do /machine/status (polling), gravamos transicoes quando o ESP envia payload.
    try:
        thr_u = 0
        try:
            thr_u = int(m.get("no_count_stop_sec", 0) or 0)
        except Exception:
            thr_u = 0

        sem_contar_u = 0
        try:
            last_ts_u = m.get("_last_count_ts_ms")
            if last_ts_u is None:
                last_ts_u = int(effective_ts_ms)
                m["_last_count_ts_ms"] = last_ts_u
            last_ts_u = int(last_ts_u)
            if int(effective_ts_ms) >= last_ts_u:
                sem_contar_u = int((int(effective_ts_ms) - last_ts_u) / 1000)
        except Exception:
            sem_contar_u = 0

        stopped_by_no_count_u = bool(thr_u >= 5 and sem_contar_u >= thr_u)

        # Regra de estado:
        # RUN  = status AUTO + run=1 + nao estourou janela sem contagem
        # STOP = caso contrario
        if new_status == "AUTO" and int(m.get("run") or 0) == 1 and not stopped_by_no_count_u:
            st_evt_u = "RUN"
        else:
            st_evt_u = "STOP"

        dt_evt_u = datetime.fromtimestamp(int(effective_ts_ms) / 1000, TZ_BAHIA)
        hora_evt_u = int(dt_evt_u.hour)
        data_ref_evt_u = dia_operacional_ref_str(dt_evt_u)
        raw_mid_u = _norm_machine_id(machine_id)
        eff_mid_u = raw_mid_u

        _record_machine_state_transition(
            raw_mid_u,
            eff_mid_u,
            str(cliente_id) if cliente_id else None,
            st_evt_u,
            dt_evt_u,
            data_ref_evt_u,
            hora_evt_u,
        )
    except Exception:
        pass



    resp = {
        "message": "OK",
        "machine_id": machine_id,
        "cliente_id": cliente_id,
        "device_id": device_id or None,
        "linked_machine": linked_machine or None,
        "baseline_initialized": bool(baseline_initialized),
        "allow_takeover": bool(allow_takeover),
        "ts_source": (m.get("_last_esp_ts_source") or None),
        "ts_ms": (m.get("_last_esp_ts_ms_seen") or None),
    }

    # Se houver reset pendente, envia comando ao ESP (vai repetir ate receber ACK)
    try:
        conn_cmd = get_db()
        pending_cmd_id = _get_pending_reset_cmd(conn_cmd, cliente_id, machine_id)
        if pending_cmd_id:
            resp["cmd"] = "reset_counter"
            resp["cmd_id"] = pending_cmd_id
    except Exception:
        pass

    return jsonify(resp)

def _admin_zerar_producao_db_day_hour(machine_id: str, dia_ref: str, cliente_id: str | None) -> None:
    mid_raw = _norm_machine_id(_unscope_machine_id(machine_id))
    cid = (cliente_id or "").strip() or None
    like_mid = f"%::{mid_raw}"
    mids = [mid_raw] + ([f"{cid}::{mid_raw}"] if cid else [])

    conn = get_db()
    try:
        for mid in mids:
            try:
                conn.execute("UPDATE producao_horaria SET produzido = 0 WHERE data_ref = ? AND machine_id = ? AND cliente_id = ?", (dia_ref, mid, cid))
            except Exception:
                try:
                    conn.execute("UPDATE producao_horaria SET produzido = 0 WHERE data_ref = ? AND machine_id = ?", (dia_ref, mid))
                except Exception:
                    pass
            try:
                conn.execute("UPDATE producao_diaria SET produzido = 0, pecas_boas = 0, refugo_total = 0, percentual = 0 WHERE data = ? AND machine_id = ? AND cliente_id = ?", (dia_ref, mid, cid))
            except Exception:
                try:
                    conn.execute("UPDATE producao_diaria SET produzido = 0, pecas_boas = 0, refugo_total = 0, percentual = 0 WHERE data = ? AND machine_id = ?", (dia_ref, mid))
                except Exception:
                    pass
        try:
            conn.execute("UPDATE producao_horaria SET produzido = 0 WHERE data_ref = ? AND machine_id LIKE ?", (dia_ref, like_mid))
        except Exception:
            pass
        try:
            conn.execute("UPDATE producao_diaria SET produzido = 0, pecas_boas = 0, refugo_total = 0, percentual = 0 WHERE data = ? AND machine_id LIKE ?", (dia_ref, like_mid))
        except Exception:
            pass
        conn.commit()
    finally:
        conn.close()

@machine_bp.route("/admin/reset-hour", methods=["POST"])
def admin_reset_hour():
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    scope = (data.get("scope") or "").strip().lower()

    if scope == "hour":
        try:
            carregar_baseline_diario(m, machine_id)
        except Exception:
            pass

        try:
            prod_turno = int(m.get("producao_turno", 0) or 0)
        except Exception:
            prod_turno = 0

        if prod_turno <= 0:
            try:
                esp_abs = int(m.get("esp_absoluto", 0) or 0)
            except Exception:
                esp_abs = 0
            try:
                base_d = int(m.get("baseline_diario", 0) or 0)
            except Exception:
                base_d = 0
            prod_turno = max(0, esp_abs - base_d)

        idx = calcular_ultima_hora_idx(m)
        m["ultima_hora"] = idx
        m["baseline_hora"] = int(prod_turno)
        m["producao_hora"] = 0
        m["percentual_hora"] = 0

        try:
            if isinstance(idx, int) and "producao_por_hora" in m and isinstance(m.get("producao_por_hora"), list):
                if 0 <= idx < len(m["producao_por_hora"]):
                    m["producao_por_hora"][idx] = 0
        except Exception:
            pass

        m["_ph_loaded"] = False

        return jsonify({"ok": True, "machine_id": machine_id, "scope": "hour", "hora_idx": idx, "baseline_hora": int(m.get("baseline_hora", 0) or 0), "note": "Hora resetada. Produção da hora volta a contar a partir de agora."})

    try:
        cid = (data.get("cliente_id") or "").strip()
    except Exception:
        cid = ""

    if not cid:
        try:
            cid = (_get_cliente_id_for_request() or "").strip()
        except Exception:
            cid = ""

    if cid:
        m["cliente_id"] = cid

    try:
        esp_abs_now = int(m.get("esp_absoluto", 0) or 0)
    except Exception:
        esp_abs_now = 0

    reset_contexto(m, machine_id)

    try:
        m["ultimo_dia"] = dia_operacional_ref_str(now_bahia())
    except Exception:
        pass

    idx = calcular_ultima_hora_idx(m)
    m["ultima_hora"] = idx
    m["baseline_hora"] = int(esp_abs_now)
    m["producao_hora"] = 0
    m["percentual_hora"] = 0
    m["_ph_loaded"] = False

    try:
        if isinstance(m.get("producao_por_hora"), list):
            for i in range(len(m["producao_por_hora"])):
                m["producao_por_hora"][i] = 0
    except Exception:
        pass

    try:
        from modules.repos.baseline_repo import persistir_baseline_diario as _persistir_bd

        dia_ref = dia_operacional_ref_str(now_bahia())

        try:
            _persistir_bd(machine_id, dia_ref, int(esp_abs_now), int(esp_abs_now))
        except Exception:
            pass

        if cid:
            scoped_mid = f"{cid}::{machine_id}"
            try:
                _persistir_bd(scoped_mid, dia_ref, int(esp_abs_now), int(esp_abs_now))
            except Exception:
                pass
    except Exception:
        pass


    try:
        dia_ref_db = dia_operacional_ref_str(now_bahia())
        _admin_reset_producao_por_data(machine_id=machine_id, dia_ref=dia_ref_db, cliente_id=cid or None)
    except Exception:
        pass

    return jsonify({"ok": True, "machine_id": machine_id, "scope": "day+hour", "hora_idx": idx, "cliente_id": cid or None, "note": "Reset completo executado. Dia e hora zerados a partir de agora."})

# =====================================================
# RESET PRODUCAO POR DATA (ADMIN)
# =====================================================

def _db_cols(conn, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return set([r[1] for r in rows])  # (cid, name, type, notnull, dflt_value, pk)
    except Exception:
        return set()

def _pick_date_col(cols: set[str]) -> str | None:
    for c in ["data_ref", "data", "dia_ref", "date_ref", "date", "ts", "timestamp", "created_at"]:
        if c in cols:
            return c
    return None

def _admin_reset_producao_por_data(machine_id: str, dia_ref: str, cliente_id: str | None = None) -> dict:
    """
    Zera/remover producao de uma maquina em um DIA especifico.
    Funciona mesmo com variacoes de schema, tentando:
      - UPDATE (quando a tabela guarda acumulados)
      - DELETE (quando a tabela guarda eventos/linhas por hora)
    """
    conn = get_db()
    cur = conn.cursor()

    mid_raw = (machine_id or "").strip()
    if not mid_raw:
        return {"ok": False, "error": "machine_id vazio"}

    dia = (dia_ref or "").strip()
    if not dia or len(dia) < 8:
        return {"ok": False, "error": "dia_ref invalido"}

    cid = (cliente_id or "").strip() if cliente_id else None

    # match por: machine_id puro, e (cliente_id:machine_id) quando existir
    mids = [mid_raw]
    if cid:
        mids.append(f"{cid}:{mid_raw}")

    # alguns dados podem ter machine_id com prefixo de cliente_id mesmo quando cid nao foi passado
    # entao tambem tentamos LIKE '%:mid'
    like_suffix = f"%:{mid_raw}"

    tables = [
        "producao_diaria",
        "producao_horaria",
        "producao_evento",
        "machine_count_state",
        "machine_stop",
        "nao_programado_diario",
        "nao_programado_horaria",
        "refugo_horaria",
    ]

    result = {"ok": True, "machine_id": mid_raw, "dia_ref": dia, "cliente_id": cid, "tables": {}}

    for t in tables:
        cols = _db_cols(conn, t)
        if not cols or "machine_id" not in cols:
            continue

        date_col = _pick_date_col(cols)
        where_date_sql = ""
        params_date = []

        if date_col:
            # algumas tabelas guardam 'YYYY-MM-DD' e outras guardam timestamp ISO.
            where_date_sql = f" AND ({date_col} = ? OR {date_col} LIKE ?)"
            params_date = [dia, f"{dia}%"]
        else:
            # sem coluna de data, nao mexe (evita apagar tudo)
            continue

        # se tiver cliente_id na tabela e cid foi informado, filtra por cliente_id tambem
        where_cid_sql = ""
        params_cid = []
        if cid and "cliente_id" in cols:
            where_cid_sql = " AND cliente_id = ?"
            params_cid = [cid]

        deleted = 0
        updated = 0

        # 1) Tenta UPDATE para zerar campos de acumulado, se existirem
        set_parts = []
        for c in ["produzido", "pecas_boas", "refugo_total", "refugo", "op_pcs"]:
            if c in cols:
                set_parts.append(f"{c}=0")
        if set_parts:
            sql_up = f"UPDATE {t} SET {', '.join(set_parts)} WHERE (machine_id = ? OR machine_id = ?) OR machine_id LIKE ?"
            # adiciona data e cid
            sql_up += where_date_sql
            sql_up += where_cid_sql

            try:
                cur.execute(sql_up, [mids[0], mids[1] if len(mids) > 1 else mids[0], like_suffix] + params_date + params_cid)
                updated += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            except Exception:
                # ignora e tenta delete
                pass

        # 2) Sempre tenta DELETE para remover linhas (horas/eventos)
        sql_del = f"DELETE FROM {t} WHERE (machine_id = ? OR machine_id = ?) OR machine_id LIKE ?"
        sql_del += where_date_sql
        sql_del += where_cid_sql

        try:
            cur.execute(sql_del, [mids[0], mids[1] if len(mids) > 1 else mids[0], like_suffix] + params_date + params_cid)
            deleted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except Exception:
            pass

        if updated or deleted:
            result["tables"][t] = {"updated": updated, "deleted": deleted}

    # 3) Reancora baseline do DIA para o valor atual do contador do ESP (para o status/card voltar a 0 apos reset).
    try:
        esp_abs_now = None
        mcs_cols = _db_cols(conn, 'machine_count_state') or set()
        # tenta ler o ultimo absoluto conhecido do ESP
        if mcs_cols and 'machine_id' in mcs_cols:
            # escolhe uma coluna possivel para o absoluto
            cand_abs = None
            for c in ['esp_absoluto', 'esp_abs', 'esp_last', 'last_esp', 'last_esp_abs', 'last_esp_abs_seen', '_last_esp_abs_seen', '_bd_esp_last', '_np_last_esp']:
                if c in mcs_cols:
                    cand_abs = c
                    break
            if cand_abs:
                sql_mcs = f"SELECT {cand_abs} FROM machine_count_state WHERE (machine_id = ? OR machine_id = ?) OR machine_id LIKE ?"
                params = [mids[0], mids[1] if len(mids) > 1 else mids[0], like_suffix]
                if cid and 'cliente_id' in mcs_cols:
                    sql_mcs += " AND cliente_id = ?"
                    params.append(cid)
                row = cur.execute(sql_mcs + " LIMIT 1", params).fetchone()
                if row and row[0] is not None:
                    try:
                        esp_abs_now = int(float(row[0]))
                    except Exception:
                        esp_abs_now = None

        # aplica baseline_diario = esp_abs_now para este dia_ref (se a tabela existir e tiver colunas suportadas)
        if esp_abs_now is not None:
            b_cols = _db_cols(conn, 'baseline_diario') or set()
            if b_cols and 'machine_id' in b_cols:
                b_date_col = _pick_date_col(b_cols)
                # coluna onde gravamos o baseline
                b_val_col = None
                for c in ['baseline_diario', 'baseline', 'valor', 'value', 'baseline_pcs', 'baseline_count']:
                    if c in b_cols:
                        b_val_col = c
                        break
                if b_date_col and b_val_col:
                    # tenta UPDATE, se nao existir linha faz INSERT
                    where = f"(machine_id = ? OR machine_id = ?) OR machine_id LIKE ?"
                    params = [mids[0], mids[1] if len(mids) > 1 else mids[0], like_suffix]
                    if cid and 'cliente_id' in b_cols:
                        where += " AND cliente_id = ?"
                        params.append(cid)
                    where += f" AND {b_date_col} = ?"
                    params.append(dia)
                    sql_up_base = f"UPDATE baseline_diario SET {b_val_col} = ? WHERE " + where
                    cur.execute(sql_up_base, [esp_abs_now] + params)
                    base_updated = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                    base_inserted = 0
                    if base_updated == 0:
                        cols_ins = ['machine_id', b_date_col, b_val_col]
                        vals_ins = [mids[0], dia, esp_abs_now]
                        if cid and 'cliente_id' in b_cols:
                            cols_ins.insert(0, 'cliente_id')
                            vals_ins.insert(0, cid)
                        placeholders = ','.join(['?'] * len(cols_ins))
                        sql_ins = f"INSERT INTO baseline_diario ({', '.join(cols_ins)}) VALUES ({placeholders})"
                        try:
                            cur.execute(sql_ins, vals_ins)
                            base_inserted = 1
                        except Exception:
                            base_inserted = 0

                    if base_updated or base_inserted:
                        result['tables']['baseline_diario'] = {'updated': int(base_updated), 'inserted': int(base_inserted), 'baseline_set_to': int(esp_abs_now)}
    except Exception:
        # baseline é melhor-esforco: nao deixa o reset falhar por isso
        pass

    # 4) Zera/encerra OPs do DIA (somente tabelas que se parecem com 'OP')
    try:
        now_iso = datetime.now().isoformat(timespec='seconds')
        # lista tabelas e tenta identificar uma tabela de OP
        trows = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        for (tname,) in trows:
            if not tname or not isinstance(tname, str):
                continue
            # evita mexer em tabelas do proprio reset e do sistema
            if tname in ('sqlite_sequence',):
                continue
            cols = _db_cols(conn, tname) or set()
            if 'machine_id' not in cols:
                continue
            # heuristica: tabela tem cara de OP se tiver started_at e status e (lote ou operador ou os ou op_id)
            if 'started_at' not in cols or 'status' not in cols:
                continue
            if not ({'lote', 'operador', 'os', 'op_id', 'op'} & set(cols)):
                continue

            # filtra por data no started_at (YYYY-MM-DD) e por machine_id
            where = "((machine_id = ? OR machine_id = ?) OR machine_id LIKE ?) AND (started_at = ? OR started_at LIKE ?)"
            params = [mids[0], mids[1] if len(mids) > 1 else mids[0], like_suffix, dia, f"{dia}%"]
            if cid and 'cliente_id' in cols:
                where += " AND cliente_id = ?"
                params.append(cid)

            op_updated = 0
            op_deleted = 0

            # se tiver ended_at, encerra; senao exclui
            if 'ended_at' in cols:
                try:
                    cur.execute(f"UPDATE {tname} SET status='ENCERRADA', ended_at=? WHERE " + where, [now_iso] + params)
                    op_updated = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                except Exception:
                    op_updated = 0

            # sempre tenta DELETE tambem (garante que some do historico)
            try:
                cur.execute(f"DELETE FROM {tname} WHERE " + where, params)
                op_deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            except Exception:
                op_deleted = 0

            if op_updated or op_deleted:
                result['tables'][f"ops:{tname}"] = {'updated': int(op_updated), 'deleted': int(op_deleted)}
    except Exception:
        pass
    conn.commit()
    return result


@machine_bp.route("/admin/reset-date", methods=["POST"])
@login_required
def admin_reset_date():
    """
    Endpoint para zerar producao por DATA.
    Body JSON:
      - machine_id: "maquina005"
      - dia_ref: "YYYY-MM-DD" (aceita tambem data_ref/data/date)
    """

    payload = request.get_json(silent=True) or {}
    machine_id = (payload.get("machine_id") or "").strip()

    dia_ref = (
        payload.get("dia_ref")
        or payload.get("data_ref")
        or payload.get("data")
        or payload.get("date")
        or ""
    )
    dia_ref = str(dia_ref).strip()

    # valida formato simples YYYY-MM-DD
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", dia_ref):
        return jsonify({"ok": False, "error": "dia_ref deve ser YYYY-MM-DD", "dia_ref": dia_ref}), 400

    cid = _get_cliente_id_for_request()  # FIX: reset-date usa helper existente; evita NameError

    out = _admin_reset_producao_por_data(machine_id=machine_id, dia_ref=dia_ref, cliente_id=cid)
    return jsonify(out)


@machine_bp.route("/admin/esp-reset-counter", methods=["POST"])
def admin_esp_reset_counter():
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    machine_id = _norm_machine_id(payload.get("machine_id", ""))
    cliente_id_in = (payload.get("cliente_id") or "").strip() or None

    if not machine_id:
        return jsonify({"ok": False, "error": "machine_id obrigatório"}), 400

    try:
        conn = get_db()
        cmd_id = _issue_reset_cmd(conn, cliente_id_in, machine_id, now_bahia().isoformat())
        return jsonify(
            {
                "ok": True,
                "cmd": "reset_counter",
                "cmd_id": cmd_id,
                "machine_id": machine_id,
                "cliente_id": cliente_id_in,
                "note": "Comando registrado. ESP vai zerar no proximo /machine/update e enviar ACK (reset_ack=1).",
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "error": "falha ao registrar comando", "detail": str(e)}), 500

@machine_bp.route("/admin/reset-manual", methods=["POST"])
def reset_manual():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    # Reseta contexto em memoria (estado atual da maquina)
    reset_contexto(m, machine_id)

    # Zera producao consolidada do dia no banco, para historico nao manter valor antigo.
    try:
        cid = None
        try:
            cid = _get_cliente_id_for_request()
        except Exception:
            cid = None

        dia_ref_db = dia_operacional_ref_str(now_bahia())

        _admin_zerar_producao_db_day_hour(machine_id=machine_id, dia_ref=dia_ref_db, cliente_id=cid or None)

        # Opcional: se existir refugo_horaria, zerar refugo do dia para evitar resquicios na UI.
        try:
            conn = get_db()
            try:
                mid_raw = _norm_machine_id(_unscope_machine_id(machine_id))
                like_mid = f"%::{mid_raw}"
                mids = [mid_raw] + ([f"{cid}::{mid_raw}"] if cid else [])

                for mid in mids:
                    try:
                        conn.execute("UPDATE refugo_horaria SET refugo = 0 WHERE dia_ref = ? AND machine_id = ? AND cliente_id = ?", (dia_ref_db, mid, cid))
                    except Exception:
                        try:
                            conn.execute("UPDATE refugo_horaria SET refugo = 0 WHERE dia_ref = ? AND machine_id = ?", (dia_ref_db, mid))
                        except Exception:
                            pass

                try:
                    conn.execute("UPDATE refugo_horaria SET refugo = 0 WHERE dia_ref = ? AND machine_id LIKE ?", (dia_ref_db, like_mid))
                except Exception:
                    pass

                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass
    except Exception:
        pass

    return jsonify({"status": "resetado", "machine_id": machine_id})


@machine_bp.route("/admin/baseline-manual", methods=["POST"])
def admin_baseline_manual():
    """
    Define o baseline do dia manualmente para uma máquina.

    Baseline é o valor de referência do contador do ESP no início do dia. A produção do dia
    é calculada como (esp_last - baseline_esp).

    Body JSON:
      - machine_id (obrigatório)
      - dia_ref (opcional, YYYY-MM-DD). Se não vier, usa o dia atual (Bahia).
      - baseline_esp (opcional) ou esp_absoluto (opcional). Um dos dois deve vir.
      - cliente_id (opcional)

    Auth:
      - Se INDFLOW_ADMIN_TOKEN estiver definido, exige header: X-Admin-Token
      - Se INDFLOW_ADMIN_TOKEN não estiver definido (ambiente local), aceita header: Admin=admin
    """
    data = request.get_json(silent=True) or {}

    expected = os.getenv("INDFLOW_ADMIN_TOKEN", "").strip()
    if expected:
        token_in = (request.headers.get("X-Admin-Token") or "").strip()
        if not _admin_token_ok(token_in):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
    else:
        if (request.headers.get("Admin") or "").strip() != "admin":
            return jsonify({"ok": False, "error": "unauthorized"}), 401

    machine_id = (data.get("machine_id") or "").strip()
    if not machine_id:
        return jsonify({"ok": False, "error": "machine_id é obrigatório"}), 400

    dia_ref = (data.get("dia_ref") or "").strip()
    if not dia_ref:
        dia_ref = dia_operacional_ref_str(now_bahia())

    baseline_esp = data.get("baseline_esp", None)
    if baseline_esp is None:
        baseline_esp = data.get("esp_absoluto", None)
    if baseline_esp is None:
        return jsonify({"ok": False, "error": "baseline_esp ou esp_absoluto é obrigatório"}), 400

    try:
        baseline_esp = int(baseline_esp)
    except Exception:
        return jsonify({"ok": False, "error": "baseline_esp inválido (precisa ser inteiro)"}), 400

    cliente_id = data.get("cliente_id", None)
    if cliente_id is not None:
        cliente_id = str(cliente_id).strip() or None

    updated_at = now_bahia().isoformat()
    conn = get_db()
    try:
        _force_baseline_for_day(
            conn, machine_id=machine_id, dia_ref=dia_ref, baseline_esp=baseline_esp, updated_at=updated_at, cliente_id=cliente_id
        )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return jsonify({
        "ok": True,
        "machine_id": machine_id,
        "dia_ref": dia_ref,
        "baseline_esp": baseline_esp,
        "updated_at": updated_at,
        "cliente_id": cliente_id,
        "note": "Baseline definido. A produção do dia passa a contar a partir deste valor."
    })
@machine_bp.route("/machine/refugo", methods=["POST"])
def salvar_refugo():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    agora = now_bahia()
    dia_atual = dia_operacional_ref_str(agora)

    dia_ref = (data.get("dia_ref") or "").strip() or dia_atual
    hora_dia = _safe_int(data.get("hora_dia"), -1)
    refugo = _safe_int(data.get("refugo"), 0)

    if hora_dia < 0 or hora_dia > 23:
        return jsonify({"ok": False, "error": "hora_dia invalida (0..23)"}), 400

    if refugo < 0:
        refugo = 0

    if dia_ref > dia_atual:
        return jsonify({"ok": False, "error": "dia_ref futuro nao permitido"}), 400

    if dia_ref == dia_atual:
        hora_atual = int(agora.hour)
        if hora_dia >= hora_atual:
            return jsonify({"ok": False, "error": "So e permitido lancar refugo em horas passadas"}), 400

    ok = upsert_refugo(machine_id=machine_id, dia_ref=dia_ref, hora_dia=hora_dia, refugo=refugo, updated_at_iso=agora.isoformat())

    if not ok:
        return jsonify({"ok": False, "error": "Falha ao salvar no banco"}), 500

    return jsonify({"ok": True, "machine_id": machine_id, "dia_ref": dia_ref, "hora_dia": hora_dia, "refugo": refugo})

@machine_bp.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = _norm_machine_id(request.args.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    # Recarrega config persistida (pos-deploy)
    _cfgv2_load_apply(m, machine_id)

    cid_req = None
    try:
        cid_req = _get_cliente_id_for_request()
    except Exception:
        cid_req = None

    if cid_req:
        m["cliente_id"] = cid_req

        if not m.get("_pd_backfill_done"):
            try:
                _backfill_producao_diaria_cliente_id_all(machine_id, cid_req)
                m["_pd_backfill_done"] = True
            except Exception:
                pass

    dia_ref_before = str(m.get("ultimo_dia") or "").strip()
    try:
        verificar_reset_diario(m, machine_id)
    except Exception:
        pass
    dia_ref_after = str(m.get("ultimo_dia") or "").strip()

    if cid_req and dia_ref_before and dia_ref_after and dia_ref_before != dia_ref_after:
        try:
            raw_mid = _norm_machine_id(machine_id)
            scoped_mid = f"{cid_req}::{raw_mid}"
            conn = get_db()
            try:
                conn.execute(
                    "UPDATE producao_diaria SET cliente_id=? "
                    "WHERE (cliente_id IS NULL OR cliente_id='') "
                    "AND data=? AND (machine_id=? OR machine_id=?)",
                    (cid_req, dia_ref_before, raw_mid, scoped_mid),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    carregar_baseline_diario(m, machine_id)

    atualizar_producao_hora(m)

    # Persistencia por hora (Historico): polling do front ajuda a congelar horas mesmo sem novos eventos do ESP
    try:
        _persist_snapshot_from_machine(m, machine_id=str(machine_id), ts_ms=int(now_bahia().timestamp() * 1000))
    except Exception:
        pass


    # Fix: garante que producao_diaria seja valor absoluto do turno (evita Historico dobrar)
    try:
        dia_ref_pd = dia_operacional_ref_str(now_bahia())
        try:
            prod_abs = int(m.get("producao_turno", 0) or 0)
        except Exception:
            prod_abs = 0
        try:
            meta_abs = int(m.get("meta_turno", 0) or 0)
        except Exception:
            meta_abs = 0
        _sync_producao_diaria_absoluta(machine_id=str(machine_id), cliente_id=(m.get("cliente_id") or cid_req), dia_ref=str(dia_ref_pd), produzido_abs=int(prod_abs), meta=int(meta_abs))
    except Exception:
        pass

    calcular_tempo_medio(m)
    aplicar_derivados_ml(m)

    dia_ref = dia_operacional_ref_str(now_bahia())
    m["refugo_por_hora"] = load_refugo_24(machine_id, dia_ref)

    try:
        cid = _resolve_cliente_id_for_status(m)
        m["np_por_hora_24"] = _load_np_por_hora_24_scoped(machine_id, dia_ref, cid)
    except Exception:
        m["np_por_hora_24"] = [0] * 24

    try:
        exib = [0] * 24

        horas_turno = m.get("horas_turno") or []
        prod_turno = m.get("producao_por_hora") or []

        if isinstance(horas_turno, list) and isinstance(prod_turno, list):
            for i, faixa in enumerate(horas_turno):
                if i >= len(prod_turno):
                    break
                try:
                    h_ini = int(str(faixa).split("-", 1)[0].strip().split(":", 1)[0])
                except Exception:
                    continue
                if 0 <= h_ini <= 23:
                    v = prod_turno[i]
                    if v is None:
                        continue
                    exib[h_ini] = _safe_int(v, 0)

        np24 = m.get("np_por_hora_24") or [0] * 24
        if isinstance(np24, list) and len(np24) == 24:
            for h in range(24):
                if exib[h] == 0 and _safe_int(np24[h], 0) > 0:
                    exib[h] = _safe_int(np24[h], 0)

        m["producao_exibicao_24"] = exib
    except Exception:
        m["producao_exibicao_24"] = [0] * 24

    if "run" not in m:
        m["run"] = 0

    try:
        status = (m.get("status") or "").strip().upper()
        run_val = int(m.get("run") or 0)
        agora = now_bahia()
        now_ms = int(agora.timestamp() * 1000)

        thr = 0
        try:
            thr = int(m.get("no_count_stop_sec", 0) or 0)
        except Exception:
            thr = 0

        sem_contar = 0
        try:
            last_ts = m.get("_last_count_ts_ms")
            if last_ts is None:
                last_ts = now_ms
                m["_last_count_ts_ms"] = last_ts
            last_ts = int(last_ts)
            if now_ms >= last_ts:
                sem_contar = int((now_ms - last_ts) / 1000)
        except Exception:
            sem_contar = 0

        stopped_by_no_count = bool(thr >= 5 and sem_contar >= thr)

        if status == "AUTO" and run_val == 1 and not stopped_by_no_count:
            m["status_ui"] = "PRODUZINDO"
            m["parado_min"] = None
            try:
                _clear_stopped_since_ms(machine_id)
            except Exception:
                pass
        else:
            m["status_ui"] = "PARADA"
            ss = _get_stopped_since_ms(machine_id)
            if ss is None:
                try:
                    updated_at = agora.strftime("%Y-%m-%d %H:%M:%S")
                    # quando parar, ancora no ultimo timestamp conhecido (se tiver)
                    anchor_ms = int(m.get("_last_count_ts_ms", now_ms) or now_ms)
                    _set_stopped_since_ms(machine_id, anchor_ms, updated_at)
                    ss = anchor_ms
                except Exception:
                    ss = None

            turno_inicio = (m.get("turno_inicio") or "").strip()
            turno_fim = (m.get("turno_fim") or "").strip()
            if isinstance(ss, int):
                if turno_inicio and turno_fim:
                    m["parado_min"] = _calc_minutos_parados_somente_turno(int(ss), now_ms, turno_inicio, turno_fim)
                else:
                    diff_ms = max(0, now_ms - int(ss))
                    m["parado_min"] = int(diff_ms // 60000)
            else:
                m["parado_min"] = None
    except Exception:
        m["status_ui"] = "PRODUZINDO" if (m.get("status") == "AUTO") else "PARADA"
        m["parado_min"] = None

    try:
        np_prod = int(m.get("np_producao", 0) or 0)
    except Exception:
        np_prod = 0

    if m.get("ultima_hora") is None and np_prod > 0:
        m["producao_hora"] = np_prod
        m["percentual_hora"] = 0
        m["fora_turno"] = True
        m["producao_hora_liquida"] = np_prod
        try:
            agora_evt = now_bahia()
            hora_evt = int(agora_evt.hour)
            data_ref_evt = dia_operacional_ref_str(agora_evt)
            cid_evt = (m.get("cliente_id") or None)
            raw_mid = _norm_machine_id(machine_id)
            eff_mid = raw_mid
            _record_machine_state_transition(raw_mid, eff_mid, cid_evt, "NP", agora_evt, data_ref_evt, hora_evt)
        except Exception:
            pass
        return jsonify(m)

    try:
        hora_atual = int(now_bahia().hour)
    except Exception:
        hora_atual = None

    try:
        ph = int(m.get("producao_hora", 0) or 0)
    except Exception:
        ph = 0

    if isinstance(hora_atual, int) and 0 <= hora_atual < 24:
        m["producao_hora_liquida"] = max(0, ph - int(m["refugo_por_hora"][hora_atual] or 0))
    else:
        m["producao_hora_liquida"] = ph

    try:
        agora_evt = now_bahia()
        hora_evt = int(agora_evt.hour)
        data_ref_evt = dia_operacional_ref_str(agora_evt)
        cid_evt = (m.get("cliente_id") or None)
        raw_mid = _norm_machine_id(machine_id)
        eff_mid = raw_mid
        st_evt = _infer_state_for_timeline(m, hora_evt)
        _record_machine_state_transition(raw_mid, eff_mid, cid_evt, st_evt, agora_evt, data_ref_evt, hora_evt)
    except Exception:
        pass
    return jsonify(m)


@machine_bp.route("/maquina/<machine_id>/historico", methods=["GET"])
@login_required
def maquina_historico(machine_id):
    """
    Compat MAIN: o HUB chama /maquina/<machine_id>/historico.
    No DEV a tela usa /producao/historico?machine_id=...
    Aqui redirecionamos para manter a UI funcionando sem alterar o front.
    Preserva querystring (inicio/fim/format) quando existir.
    """
    mid = _norm_machine_id(machine_id)

    args = {}
    try:
        args = dict(request.args.items())
    except Exception:
        args = {}
    args["machine_id"] = mid

    qs = urlencode(args) if args else ""
    base = url_for("machine_bp.historico_page")
    target = f"{base}?{qs}" if qs else base
    return redirect(target)

@machine_bp.route("/producao/historico", methods=["GET"])
@login_required
def historico_page():
    accept = (request.headers.get("Accept") or "").lower()
    xhr = (request.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest"
    fmt = (request.args.get("format") or "").lower()

    if "application/json" in accept or xhr or fmt == "json":
        return historico_producao_api()

    return render_template("historico.html")

@machine_bp.route("/api/producao/historico", methods=["GET"])
def historico_producao_api():
    cliente_id = _get_cliente_id_for_request()
    machine_id = _norm_machine_id(request.args.get("machine_id", "maquina01"))

    inicio = (request.args.get("inicio") or "").strip()
    fim = (request.args.get("fim") or "").strip()

    if not inicio or not fim:
        hoje = now_bahia().date()
        d0 = hoje - timedelta(days=29)
        inicio = d0.isoformat()
        fim = hoje.isoformat()

    base = _get_historico_producao(cliente_id, machine_id, inicio, fim) or []

    base_por_dia = {item.get("data"): item for item in base if item.get("data")}

    dias = set(base_por_dia.keys())

    if not dias:
        return jsonify([])

    m = get_machine(machine_id)

    # Recarrega config persistida (pos-deploy)
    _cfgv2_load_apply(m, machine_id)

    meta_default = _safe_int(m.get("meta_turno"), 0)
    conn = get_db()


    out = []
    for dia in sorted(dias, reverse=True):
        item = dict(base_por_dia.get(dia) or {})
        item["data"] = dia
        item["machine_id"] = machine_id

        item["meta"] = _safe_int(item.get("meta"), meta_default)

        # pecas_boas/refugo
        pecas_boas = _safe_int(item.get("pecas_boas"), 0)
        refugo_total = _safe_int(item.get("refugo_total"), 0)
        produzido = _safe_int(item.get("produzido"), 0)

        # Fallback: se tem OPs no dia mas produzido esta 0, calcula pelo somatorio das OPs ENCERRADAS.
        try:
            ops_list = item.get("ops") or []
            # Enriquecer OPs com qtd_mat_bom_esp (soma do delta do ESP no intervalo da OP)
            _enrich_ops_with_esp_counts(conn, cliente_id, machine_id, [item])

            if produzido == 0 and isinstance(ops_list, list) and len(ops_list) > 0:
                produzido_ops = _calc_produzido_from_ops(ops_list)
                if produzido_ops > 0:
                    produzido = int(produzido_ops)
                    item["produzido"] = produzido
        except Exception:
            pass

        # Se a base nao trouxe pecas_boas, assume produzido - refugo
        if "pecas_boas" not in item or pecas_boas == 0:
            if produzido > 0:
                pecas_boas = max(0, produzido - refugo_total)
                item["pecas_boas"] = pecas_boas

        # percentual
        meta = _safe_int(item.get("meta"), 0)
        if meta > 0:
            item["percentual"] = round((produzido / meta) * 100)
        else:
            item["percentual"] = 0

        if "ops" not in item:
            item["ops"] = []

        out.append(item)

    return jsonify(out)

# =====================================================
# GIT (PowerShell copiar e colar)
# =====================================================
# git add -A
# @"
# fix: historico calcula produzido por OP quando eventos/base nao tem produzido
#
# - Se existir ops no dia e produzido=0, calcula produzido somando op_pcs das OPs ENCERRADAS (ou metros/conv)
# - Mantem logica de producao_evento e producao_diaria
# "@ | Set-Content commitmsg.txt -Encoding UTF8
# git commit -F commitmsg.txt
# git pufgf