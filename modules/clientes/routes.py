# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\clientes\routes.py
# LAST_RECODE: 2026-02-20 22:05 America/Bahia
# MOTIVO: Implementar acoes Visualizar/Editar/Excluir e restaurar exibicao unica da API Key apos criar.

from flask import Blueprint, request, redirect, url_for, session, jsonify, render_template
from flask import render_template_string
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS clientes (
            id TEXT PRIMARY KEY,
            nome TEXT NOT NULL,
            api_key_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_clientes_status
        ON clientes(status)
        """
    )
    conn.commit()


def _get_clientes(conn):
    cur = conn.execute(
        """
        SELECT id, nome, status, created_at
        FROM clientes
        ORDER BY created_at DESC
        """
    )
    return cur.fetchall()


def _get_cliente_by_id(conn, cliente_id: str):
    cur = conn.execute(
        """
        SELECT id, nome, status, created_at
        FROM clientes
        WHERE id = ?
        """,
        (cliente_id,),
    )
    return cur.fetchone()


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
        conn.execute(
            """
            INSERT INTO clientes (id, nome, api_key_hash, status, created_at)
            VALUES (?, ?, ?, 'active', ?)
            """,
            (cliente_id, nome, api_key_hash, created_at),
        )
        conn.commit()
    finally:
        conn.close()

    # Exibe a API Key apenas uma vez (na criacao).
    html = """
{% extends "base.html" %}
{% block title %}Cliente criado — IndFlow{% endblock %}
{% block content %}
<style>
  .wrap{max-width:900px;margin:0 auto;padding:24px;}
  .card{background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:16px;margin-bottom:16px;}
  .muted{color:#64748b;font-size:14px;line-height:1.35;}
  code{display:block;padding:12px;border-radius:10px;background:#0b1220;color:#e2e8f0;overflow:auto;}
  a.btn{display:inline-block;padding:10px 14px;border-radius:10px;border:1px solid #e5e7eb;background:#ffffff;color:#0f172a;text-decoration:none;font-weight:600;}
</style>

<div class="wrap">
  <div class="card">
    <h1 style="margin:0 0 6px 0;">Cliente criado</h1>
    <div class="muted">Anote a API Key agora. Ela nao sera exibida novamente.</div>
  </div>

  <div class="card">
    <div><b>Cliente:</b> {{ nome }}</div>
    <div class="muted" style="margin-top:6px;"><b>ID:</b> {{ cliente_id }}</div>

    <h3 style="margin:14px 0 8px 0;">API Key</h3>
    <code>{{ api_key }}</code>

    <div class="muted" style="margin-top:10px;">No ESP, envie no header: X-API-Key</div>

    <div style="margin-top:14px;">
      <a class="btn" href="{{ url_for('clientes.home') }}">Voltar</a>
    </div>
  </div>
</div>
{% endblock %}
"""
    return render_template_string(html, nome=nome, cliente_id=cliente_id, api_key=api_key)


@clientes_bp.route("/<cliente_id>", methods=["GET"])
@login_required
def visualizar_cliente(cliente_id: str):
    deny = _require_admin_or_403()
    if deny:
        return deny

    cid = (cliente_id or "").strip()
    if not cid:
        return redirect(url_for("clientes.home"))

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        r = _get_cliente_by_id(conn, cid)
    finally:
        conn.close()

    if not r:
        return ("Cliente não encontrado.", 404)

    html = """
{% extends "base.html" %}
{% block title %}Cliente — IndFlow{% endblock %}
{% block content %}
<style>
  .wrap{max-width:900px;margin:0 auto;padding:24px;}
  .card{background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:16px;margin-bottom:16px;}
  .muted{color:#64748b;font-size:14px;line-height:1.35;}
  .pill{display:inline-block;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:700;}
  .on{background:#dcfce7;color:#166534;}
  .off{background:#fee2e2;color:#991b1b;}
  .actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px;}
  a.btn{display:inline-block;padding:10px 14px;border-radius:10px;border:1px solid #e5e7eb;background:#ffffff;color:#0f172a;text-decoration:none;font-weight:600;}
  a.primary{background:#2563eb;border-color:#2563eb;color:#ffffff;}
  form{display:inline;}
  button.danger{padding:10px 14px;border:0;border-radius:10px;background:#ef4444;color:#ffffff;font-weight:600;cursor:pointer;}
</style>

<div class="wrap">
  <div class="card">
    <h1 style="margin:0 0 6px 0;">Cliente</h1>
    <div class="muted">Visualizacao do cadastro.</div>
  </div>

  <div class="card">
    <div><b>Nome:</b> {{ r["nome"] }}</div>
    <div class="muted" style="margin-top:6px;"><b>ID:</b> {{ r["id"] }}</div>
    <div class="muted" style="margin-top:6px;"><b>Criado em (UTC):</b> {{ r["created_at"] }}</div>
    <div style="margin-top:10px;">
      <b>Status:</b>
      {% if (r["status"] or "") == "active" %}
        <span class="pill on">ATIVO</span>
      {% else %}
        <span class="pill off">INATIVO</span>
      {% endif %}
    </div>

    <div class="actions">
      <a class="btn" href="{{ url_for('clientes.home') }}">Voltar</a>
      <a class="btn primary" href="/clientes/{{ r['id'] }}/editar">Editar</a>
      <form method="post" action="/clientes/delete" onsubmit="return confirm('Excluir este cliente definitivamente?');">
        <input type="hidden" name="cliente_id" value="{{ r['id'] }}">
        <button type="submit" class="danger">Excluir</button>
      </form>
    </div>
  </div>
</div>
{% endblock %}
"""
    return render_template_string(html, r=r)


@clientes_bp.route("/<cliente_id>/editar", methods=["GET", "POST"])
@login_required
def editar_cliente(cliente_id: str):
    deny = _require_admin_or_403()
    if deny:
        return deny

    cid = (cliente_id or "").strip()
    if not cid:
        return redirect(url_for("clientes.home"))

    if request.method == "POST":
        nome = _norm_nome(request.form.get("nome"))
        if not nome:
            return ("Nome inválido.", 400)

        conn = get_db()
        try:
            _ensure_clientes_table(conn)
            r = _get_cliente_by_id(conn, cid)
            if not r:
                return ("Cliente não encontrado.", 404)
            conn.execute("UPDATE clientes SET nome=? WHERE id=?", (nome, cid))
            conn.commit()
        finally:
            conn.close()

        return redirect(f"/clientes/{cid}")

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        r = _get_cliente_by_id(conn, cid)
    finally:
        conn.close()

    if not r:
        return ("Cliente não encontrado.", 404)

    html = """
{% extends "base.html" %}
{% block title %}Editar cliente — IndFlow{% endblock %}
{% block content %}
<style>
  .wrap{max-width:900px;margin:0 auto;padding:24px;}
  .card{background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:16px;margin-bottom:16px;}
  .muted{color:#64748b;font-size:14px;line-height:1.35;}
  label{display:block;font-size:13px;color:#334155;margin-bottom:6px;}
  input{padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;min-width:320px;max-width:100%;}
  .actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:12px;}
  button{padding:10px 14px;border:0;border-radius:10px;background:#2563eb;color:#ffffff;font-weight:600;cursor:pointer;}
  a.btn{display:inline-block;padding:10px 14px;border-radius:10px;border:1px solid #e5e7eb;background:#ffffff;color:#0f172a;text-decoration:none;font-weight:600;}
</style>

<div class="wrap">
  <div class="card">
    <h1 style="margin:0 0 6px 0;">Editar cliente</h1>
    <div class="muted">Altere apenas o nome. A API Key nao e regenerada aqui.</div>
  </div>

  <div class="card">
    <form method="post" action="/clientes/{{ r['id'] }}/editar">
      <div>
        <label>Nome do cliente</label>
        <input name="nome" value="{{ r['nome'] }}" required>
      </div>

      <div class="actions">
        <button type="submit">Salvar</button>
        <a class="btn" href="/clientes/{{ r['id'] }}">Cancelar</a>
      </div>
    </form>
  </div>
</div>
{% endblock %}
"""
    return render_template_string(html, r=r)


@clientes_bp.route("/delete", methods=["POST"])
@login_required
def delete_cliente():
    deny = _require_admin_or_403()
    if deny:
        return deny

    cid = (request.form.get("cliente_id") or "").strip()
    if not cid:
        return redirect(url_for("clientes.home"))

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        conn.execute("DELETE FROM clientes WHERE id=?", (cid,))
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
        rows = conn.execute(
            """
            SELECT id, nome, status, created_at
            FROM clientes
            ORDER BY created_at DESC
            """
        ).fetchall()
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
            }
        )
    return jsonify(out)
