# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\clientes\routes.py
# CODED_AT: 2026-01-15 23:00:54 America/Bahia
#
# Objetivo:
# - Cadastro de clientes (multi-tenant) com API Key própria (armazenada como hash)
# - Mostrar a API Key apenas no momento da criação (não persistimos a chave em texto)
# - Acesso: somente usuário logado com role=admin (via session)

from flask import Blueprint, request, redirect, url_for, session, jsonify
from flask import render_template_string
import hashlib
import secrets
import uuid
from datetime import datetime, timezone

from modules.db_indflow import get_db
from modules.admin.routes import login_required

clientes_bp = Blueprint("clientes", __name__)


# ============================================================
# HELPERS
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_admin() -> bool:
    return (session.get("role") or "").strip().lower() == "admin"


def _require_admin_or_403():
    if not _is_admin():
        return ("Acesso negado (admin).", 403)
    return None


def _norm_nome(v: str) -> str:
    s = (v or "").strip()
    if len(s) > 60:
        s = s[:60]
    return s


def _gen_api_key() -> str:
    # curta, simples, mas forte. Ex: IND_9f3a... (hex)
    return "IND_" + secrets.token_hex(16)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _ensure_clientes_table(conn):
    # Segurança extra: caso init_db ainda não tenha criado por algum motivo
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
# PÁGINAS (HTML simples, sem template novo)
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

    # HTML simples e funcional (sem criar arquivo de template agora)
    html = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <title>Clientes — IndFlow</title>
  <style>
    body{font-family:Arial, sans-serif; background:#f3f6f9; margin:0; padding:24px;}
    .wrap{max-width:1100px; margin:0 auto;}
    .card{background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin-bottom:16px;}
    h1{margin:0 0 8px 0;}
    .muted{color:#64748b; font-size:14px;}
    .row{display:flex; gap:12px; flex-wrap:wrap; align-items:end;}
    label{display:block; font-size:13px; color:#334155; margin-bottom:6px;}
    input{padding:10px 12px; border:1px solid #cbd5e1; border-radius:10px; min-width:280px;}
    button{padding:10px 14px; border:0; border-radius:10px; background:#2563eb; color:#fff; font-weight:600; cursor:pointer;}
    button.secondary{background:#475569;}
    table{width:100%; border-collapse:collapse; margin-top:10px;}
    th,td{padding:10px; border-bottom:1px solid #e5e7eb; text-align:left; font-size:14px;}
    th{background:#f1f5f9;}
    .pill{display:inline-block; padding:3px 10px; border-radius:999px; font-size:12px; font-weight:700;}
    .on{background:#dcfce7; color:#166534;}
    .off{background:#fee2e2; color:#991b1b;}
    .actions form{display:inline;}
    .actions button{background:#ef4444;}
    .actions button.secondary{background:#475569;}
    .top-actions{display:flex; gap:10px; justify-content:flex-end;}
    a{color:#2563eb; text-decoration:none;}
  </style>
</head>
<body>
  <div class="wrap">

    <div class="top-actions">
      <a href="/admin/" class="muted">Voltar</a>
    </div>

    <div class="card">
      <h1>Clientes</h1>
      <div class="muted">
        Crie o cliente (tenant) e gere a API Key. A chave aparece <b>somente</b> no momento da criação.
      </div>
    </div>

    <div class="card">
      <h2 style="margin:0 0 10px 0;">Criar novo cliente</h2>
      <form method="post" action="/clientes/create">
        <div class="row">
          <div>
            <label>Nome do cliente</label>
            <input name="nome" placeholder="Ex: Knauf / Cliente A / Fábrica X" required>
          </div>
          <div>
            <button type="submit">Criar + gerar API Key</button>
          </div>
        </div>
      </form>
    </div>

    <div class="card">
      <h2 style="margin:0 0 10px 0;">Lista de clientes</h2>
      <table>
        <thead>
          <tr>
            <th>Nome</th>
            <th>Status</th>
            <th>Criado em (UTC)</th>
            <th style="width:220px;">Ações</th>
          </tr>
        </thead>
        <tbody>
          {% for r in rows %}
            <tr>
              <td><b>{{ r["nome"] }}</b><div class="muted">{{ r["id"] }}</div></td>
              <td>
                {% if (r["status"] or "") == "active" %}
                  <span class="pill on">ATIVO</span>
                {% else %}
                  <span class="pill off">INATIVO</span>
                {% endif %}
              </td>
              <td class="muted">{{ r["created_at"] }}</td>
              <td class="actions">
                {% if (r["status"] or "") == "active" %}
                  <form method="post" action="/clientes/deactivate">
                    <input type="hidden" name="cliente_id" value="{{ r["id"] }}">
                    <button type="submit">Desativar</button>
                  </form>
                {% else %}
                  <form method="post" action="/clientes/activate">
                    <input type="hidden" name="cliente_id" value="{{ r["id"] }}">
                    <button type="submit" class="secondary">Ativar</button>
                  </form>
                {% endif %}
              </td>
            </tr>
          {% endfor %}
          {% if not rows %}
            <tr><td colspan="4" class="muted">Nenhum cliente cadastrado ainda.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>

  </div>
</body>
</html>
"""
    return render_template_string(html, rows=rows)


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

    # Mostra a API Key uma vez (não guardamos em texto no banco)
    html = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <title>Cliente criado — IndFlow</title>
  <style>
    body{font-family:Arial, sans-serif; background:#f3f6f9; margin:0; padding:24px;}
    .wrap{max-width:900px; margin:0 auto;}
    .card{background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:16px;}
    .muted{color:#64748b; font-size:14px;}
    code{display:block; padding:12px; border-radius:10px; background:#0b1220; color:#e2e8f0; overflow:auto;}
    a{color:#2563eb; text-decoration:none;}
    button{padding:10px 14px; border:0; border-radius:10px; background:#2563eb; color:#fff; font-weight:600; cursor:pointer;}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1 style="margin:0 0 8px 0;">Cliente criado</h1>
      <div class="muted">Anote a API Key agora. Ela não será exibida novamente.</div>

      <p><b>Cliente:</b> {{ nome }}</p>
      <p class="muted"><b>ID:</b> {{ cliente_id }}</p>

      <h3>API Key</h3>
      <code id="k">{{ api_key }}</code>

      <p class="muted">No ESP, envie no header: <b>X-API-Key</b></p>
      <code>Header: X-API-Key: {{ api_key }}</code>

      <p style="margin-top:14px;">
        <a href="/clientes/">Voltar para clientes</a>
      </p>
    </div>
  </div>
</body>
</html>
"""
    return render_template_string(html, nome=nome, cliente_id=cliente_id, api_key=api_key)


@clientes_bp.route("/deactivate", methods=["POST"])
@login_required
def deactivate_cliente():
    deny = _require_admin_or_403()
    if deny:
        return deny

    cid = (request.form.get("cliente_id") or "").strip()
    if not cid:
        return redirect(url_for("clientes.home"))

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
    if not cid:
        return redirect(url_for("clientes.home"))

    conn = get_db()
    try:
        _ensure_clientes_table(conn)
        conn.execute("UPDATE clientes SET status='active' WHERE id=?", (cid,))
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
        _ensure_clientes_table(conn)
        rows = conn.execute("""
            SELECT id, nome, status, created_at
            FROM clientes
            ORDER BY created_at DESC
        """).fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        out.append({
            "id": r["id"],
            "nome": r["nome"],
            "status": r["status"],
            "created_at": r["created_at"],
        })
    return jsonify(out)
