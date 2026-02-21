# PATH: modules/clientes/routes.py
# LAST_RECODE: 2026-02-20 21:40 America/Bahia
# MOTIVO: Separar tela de clientes em lista + formulario usando templates reais.

from flask import Blueprint, request, redirect, url_for, session, jsonify, render_template
import hashlib
import secrets
import uuid
from datetime import datetime, timezone

from modules.db_indflow import get_db
from modules.admin.routes import login_required

clientes_bp = Blueprint("clientes", __name__, template_folder="templates")


# ============================================================
# HELPERS
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_admin() -> bool:
    role = (session.get("role") or "").strip().lower()
    return role in ("admin", "superadmin")


def _require_admin_or_403():
    if not _is_admin():
        return ("Acesso negado (admin/superadmin).", 403)
    return None


def _norm_nome(v: str) -> str:
    s = (v or "").strip()
    if len(s) > 60:
        s = s[:60]
    return s


def _gen_api_key() -> str:
    return "IND_" + secrets.token_hex(16)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _ensure_clientes_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id TEXT PRIMARY KEY,
            nome TEXT NOT NULL,
            api_key_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS ix_clientes_status
        ON clientes(status)
    """)
    conn.commit()


def _get_clientes(conn):
    cur = conn.execute("""
        SELECT id, nome, status, created_at
        FROM clientes
        ORDER BY created_at DESC
    """)
    return cur.fetchall()


# ============================================================
# PÁGINAS
# ============================================================

@clientes_bp.route("/", methods=["GET"])
@login_required
def home():
    deny = _require_admin_or_403()
    if deny:
        return deny

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        rows = _get_clientes(conn)
    finally:
        conn.close()

    return render_template("clientes_list.html", rows=rows)


@clientes_bp.route("/novo", methods=["GET"])
@login_required
def novo_cliente():
    deny = _require_admin_or_403()
    if deny:
        return deny

    return render_template("clientes_form.html")


@clientes_bp.route("/create", methods=["POST"])
@login_required
def create_cliente():
    deny = _require_admin_or_403()
    if deny:
        return deny

    nome = _norm_nome(request.form.get("nome"))
    if not nome:
        return ("Nome inválido.", 400)

    cliente_id = str(uuid.uuid4())
    api_key = _gen_api_key()
    api_key_hash = _sha256_hex(api_key)
    created_at = _utc_iso()

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        conn.execute("""
            INSERT INTO clientes (id, nome, api_key_hash, status, created_at)
            VALUES (?, ?, ?, 'active', ?)
        """, (cliente_id, nome, api_key_hash, created_at))
        conn.commit()
    finally:
        conn.close()

    return redirect(url_for("clientes.home"))


@clientes_bp.route("/deactivate", methods=["POST"])
@login_required
def deactivate_cliente():
    deny = _require_admin_or_403()
    if deny:
        return deny

    cid = (request.form.get("cliente_id") or "").strip()
    if cid:
        conn = get_db()
        try:
            _ensure_clientes_table(conn)
            conn.execute("UPDATE clientes SET status='inactive' WHERE id=?", (cid,))
            conn.commit()
        finally:
            conn.close()

    return redirect(url_for("clientes.home"))


@clientes_bp.route("/activate", methods=["POST"])
@login_required
def activate_cliente():
    deny = _require_admin_or_403()
    if deny:
        return deny

    cid = (request.form.get("cliente_id") or "").strip()
    if cid:
        conn = get_db()
        try:
            _ensure_clientes_table(conn)
            conn.execute("UPDATE clientes SET status='active' WHERE id=?", (cid,))
            conn.commit()
        finally:
            conn.close()

    return redirect(url_for("clientes.home"))


# ============================================================
# API
# ============================================================

@clientes_bp.route("/api/list", methods=["GET"])
@login_required
def api_list_clientes():
    deny = _require_admin_or_403()
    if deny:
        return deny

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        rows = conn.execute("""
            SELECT id, nome, status, created_at
            FROM clientes
            ORDER BY created_at DESC
        """).fetchall()
    finally:
        conn.close()

    return jsonify([
        {
            "id": r["id"],
            "nome": r["nome"],
            "status": r["status"],
            "created_at": r["created_at"],
        } for r in rows
    ])