# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\clientes\routes.py
# LAST_RECODE: 2026-02-20 21:55 America/Bahia
# MOTIVO: Migracao automatica da tabela clientes para suportar campos completos e CRUD (visualizar/editar/excluir fisico) usando templates.

from __future__ import annotations

from flask import Blueprint, request, redirect, url_for, session, jsonify, render_template, abort
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


def _norm(v: str, max_len: int) -> str:
    s = (v or "").strip()
    if len(s) > max_len:
        s = s[:max_len]
    return s


def _norm_nome(v: str) -> str:
    return _norm(v, 60)


def _norm_email(v: str) -> str:
    return _norm(v, 120)


def _norm_phone(v: str) -> str:
    return _norm(v, 40)


def _norm_site(v: str) -> str:
    return _norm(v, 160)


def _norm_tipo(v: str) -> str:
    return _norm(v, 40)


def _norm_responsavel(v: str) -> str:
    return _norm(v, 80)


def _gen_api_key() -> str:
    return "IND_" + secrets.token_hex(16)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _table_has_column(conn, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = [r["name"] for r in cur.fetchall()]
    return col in cols


def _ensure_clientes_table(conn):
    # Tabela base (v1)
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

    # Migracao v2: colunas adicionais para cadastro completo
    # Obs: SQLite suporta ALTER TABLE ADD COLUMN. Mantemos idempotente.
    cols_to_add = [
        ("tipo_cliente", "TEXT"),
        ("email", "TEXT"),
        ("telefone_comercial", "TEXT"),
        ("telefone_celular", "TEXT"),
        ("fax", "TEXT"),
        ("site", "TEXT"),
        ("responsavel", "TEXT"),
        ("updated_at", "TEXT"),
    ]

    for col, typ in cols_to_add:
        if not _table_has_column(conn, "clientes", col):
            conn.execute(f"ALTER TABLE clientes ADD COLUMN {col} {typ}")
            conn.commit()


def _get_clientes(conn):
    _ensure_clientes_table(conn)
    cur = conn.execute("""
        SELECT
            id, nome, status, created_at,
            tipo_cliente, email, telefone_comercial, telefone_celular, fax, site, responsavel, updated_at
        FROM clientes
        ORDER BY created_at DESC
    """)
    return cur.fetchall()


def _get_cliente_by_id(conn, cid: str):
    _ensure_clientes_table(conn)
    cur = conn.execute("""
        SELECT
            id, nome, api_key_hash, status, created_at,
            tipo_cliente, email, telefone_comercial, telefone_celular, fax, site, responsavel, updated_at
        FROM clientes
        WHERE id = ?
        LIMIT 1
    """, (cid,))
    return cur.fetchone()


# ============================================================
# PAGES
# ============================================================

@clientes_bp.route("/", methods=["GET"])
@login_required
def home():
    deny = _require_admin_or_403()
    if deny:
        return deny

    conn = get_db()
    try:
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

    # Reaproveita o mesmo template para criar (form deve ler defaults)
    return render_template(
        "clientes_form.html",
        mode="create",
        cliente=None,
        action_url=url_for("clientes.create_cliente"),
    )


@clientes_bp.route("/<cid>", methods=["GET"])
@login_required
def visualizar_cliente(cid: str):
    deny = _require_admin_or_403()
    if deny:
        return deny

    conn = get_db()
    try:
        r = _get_cliente_by_id(conn, cid)
    finally:
        conn.close()

    if not r:
        abort(404)

    # Se voce criar um template dedicado depois, basta trocar aqui.
    # Por enquanto renderizamos o mesmo form em modo read-only (template pode ignorar).
    return render_template(
        "clientes_form.html",
        mode="view",
        cliente=r,
        action_url=None,
    )


@clientes_bp.route("/<cid>/editar", methods=["GET"])
@login_required
def editar_cliente(cid: str):
    deny = _require_admin_or_403()
    if deny:
        return deny

    conn = get_db()
    try:
        r = _get_cliente_by_id(conn, cid)
    finally:
        conn.close()

    if not r:
        abort(404)

    return render_template(
        "clientes_form.html",
        mode="edit",
        cliente=r,
        action_url=url_for("clientes.update_cliente", cid=cid),
    )


@clientes_bp.route("/<cid>/update", methods=["POST"])
@login_required
def update_cliente(cid: str):
    deny = _require_admin_or_403()
    if deny:
        return deny

    nome = _norm_nome(request.form.get("nome"))
    if not nome:
        return ("Nome invalido.", 400)

    tipo_cliente = _norm_tipo(request.form.get("tipo_cliente"))
    email = _norm_email(request.form.get("email"))
    telefone_comercial = _norm_phone(request.form.get("telefone_comercial"))
    telefone_celular = _norm_phone(request.form.get("telefone_celular"))
    fax = _norm_phone(request.form.get("fax"))
    site = _norm_site(request.form.get("site"))
    responsavel = _norm_responsavel(request.form.get("responsavel"))
    updated_at = _utc_iso()

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        cur = conn.execute("SELECT id FROM clientes WHERE id=? LIMIT 1", (cid,))
        if not cur.fetchone():
            return ("Cliente nao encontrado.", 404)

        conn.execute(
            """
            UPDATE clientes
            SET
                nome = ?,
                tipo_cliente = ?,
                email = ?,
                telefone_comercial = ?,
                telefone_celular = ?,
                fax = ?,
                site = ?,
                responsavel = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                nome,
                tipo_cliente,
                email,
                telefone_comercial,
                telefone_celular,
                fax,
                site,
                responsavel,
                updated_at,
                cid,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return redirect(url_for("clientes.home"))


@clientes_bp.route("/create", methods=["POST"])
@login_required
def create_cliente():
    deny = _require_admin_or_403()
    if deny:
        return deny

    nome = _norm_nome(request.form.get("nome"))
    if not nome:
        return ("Nome invalido.", 400)

    tipo_cliente = _norm_tipo(request.form.get("tipo_cliente"))
    email = _norm_email(request.form.get("email"))
    telefone_comercial = _norm_phone(request.form.get("telefone_comercial"))
    telefone_celular = _norm_phone(request.form.get("telefone_celular"))
    fax = _norm_phone(request.form.get("fax"))
    site = _norm_site(request.form.get("site"))
    responsavel = _norm_responsavel(request.form.get("responsavel"))

    cliente_id = str(uuid.uuid4())
    api_key = _gen_api_key()
    api_key_hash = _sha256_hex(api_key)
    created_at = _utc_iso()
    updated_at = created_at

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        conn.execute(
            """
            INSERT INTO clientes (
                id, nome, api_key_hash, status, created_at,
                tipo_cliente, email, telefone_comercial, telefone_celular, fax, site, responsavel, updated_at
            )
            VALUES (
                ?, ?, ?, 'active', ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                cliente_id,
                nome,
                api_key_hash,
                created_at,
                tipo_cliente,
                email,
                telefone_comercial,
                telefone_celular,
                fax,
                site,
                responsavel,
                updated_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    # Exibe a API Key uma vez (nao persistimos em texto)
    return render_template(
        "clientes_form.html",
        mode="created",
        cliente={
            "id": cliente_id,
            "nome": nome,
            "status": "active",
            "created_at": created_at,
            "tipo_cliente": tipo_cliente,
            "email": email,
            "telefone_comercial": telefone_comercial,
            "telefone_celular": telefone_celular,
            "fax": fax,
            "site": site,
            "responsavel": responsavel,
            "updated_at": updated_at,
        },
        api_key=api_key,
        action_url=None,
    )


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
            conn.execute("UPDATE clientes SET status='inactive', updated_at=? WHERE id=?", (_utc_iso(), cid))
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
            conn.execute("UPDATE clientes SET status='active', updated_at=? WHERE id=?", (_utc_iso(), cid))
            conn.commit()
        finally:
            conn.close()

    return redirect(url_for("clientes.home"))


@clientes_bp.route("/<cid>/delete", methods=["POST"])
@login_required
def delete_cliente(cid: str):
    deny = _require_admin_or_403()
    if deny:
        return deny

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        conn.execute("DELETE FROM clientes WHERE id=?", (cid,))
        conn.commit()
    finally:
        conn.close()

    return redirect(url_for("clientes.home"))


# ============================================================
# API (JSON)
# ============================================================

@clientes_bp.route("/api/list", methods=["GET"])
@login_required
def api_list_clientes():
    deny = _require_admin_or_403()
    if deny:
        return deny

    conn = get_db()
    try:
        rows = _get_clientes(conn)
    finally:
        conn.close()

    out = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "nome": r["nome"],
                "status": r["status"],
                "created_at": r["created_at"],
                "tipo_cliente": r["tipo_cliente"],
                "email": r["email"],
                "telefone_comercial": r["telefone_comercial"],
                "telefone_celular": r["telefone_celular"],
                "fax": r["fax"],
                "site": r["site"],
                "responsavel": r["responsavel"],
                "updated_at": r["updated_at"],
            }
        )
    return jsonify(out)
