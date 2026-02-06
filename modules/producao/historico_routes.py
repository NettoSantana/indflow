# PATH: modules/producao/historico_routes.py
# LAST_RECODE: 2026-02-06 03:05 America/Bahia
# MOTIVO: Historico usar somente producao_diaria e evitar dobra quando existem registros duplicados (legado/scoped ou gravacao repetida) escolhendo valor correto do dia.

from __future__ import annotations

import os
import sqlite3
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
    Regra: usar UMA única fonte por dia.
    - Se vier scoped (<cliente>::maquina), usa direto.
    - Se não, e existir scoped para o dia, usa SOMENTE scoped.
    - Caso contrário, usa legacy.
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
           AND machine_id LIKE ?
         ORDER BY produzido DESC
         LIMIT 1
    """
    like = f"%::{mid.lower()}"
    try:
        row = _fetch_one(conn, sql, (data_ref, like))
        if row and row["machine_id"]:
            return str(row["machine_id"])
    except Exception:
        pass
    return mid


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

    chosen = 0
    uniq = sorted(set([v for v in vals if v is not None]))
    if len(uniq) == 2 and uniq[0] > 0 and uniq[1] == 2 * uniq[0]:
        chosen = uniq[0]
    else:
        chosen = max(uniq) if uniq else 0

    chosen_row = None
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
    col = _resolve_data_col(conn, "ordens_producao")
    eff_mid = _resolve_effective_machine_id(conn, machine_id, data_ref)

    sql = f"""
        SELECT op, lote, operador, inicio_iso, fim_iso, status
          FROM ordens_producao
         WHERE machine_id = ?
           AND {col} = ?
         ORDER BY inicio_iso ASC
    """
    try:
        rows = conn.execute(sql, (eff_mid, data_ref)).fetchall()
    except Exception:
        try:
            rows = conn.execute(sql, (machine_id, data_ref)).fetchall()
        except Exception:
            return []

    itens = []
    for r in rows:
        itens.append(
            {
                "op": r["op"],
                "lote": r["lote"],
                "operador": r["operador"],
                "inicio_iso": r["inicio_iso"],
                "fim_iso": r["fim_iso"],
                "status": r["status"],
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


@historico_bp.route("/historico", methods=["GET"])
def historico_page():
    return render_template("historico.html")