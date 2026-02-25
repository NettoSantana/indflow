# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\admin\routes.py
# LAST_RECODE: 2026-02-25 12:20 America/Bahia
# MOTIVO: Adicionar rota temporaria de diagnostico do SQLite (producao_horaria) protegida por token, para validar persistencia no MAIN sem Shell.
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, render_template_string
from datetime import datetime, timedelta
import uuid
import secrets
import hashlib
import os
from functools import wraps

from modules.db_indflow import get_db

admin_bp = Blueprint("admin", __name__, template_folder="templates")

# ============================================================
# AUTH (sessao + sha256)
# ============================================================
LOGIN_FORM_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>IndFlow - Login</title>
  <link rel="stylesheet" href="/static/style.css?v=2">
  <style>
    body { margin:0; background:#f8fafc; font-family: Arial, sans-serif; }
    .login-wrap { min-height:100vh; display:flex; align-items:center; justify-content:center; }
    .login-card { width:420px; background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; padding:28px; box-shadow:0 10px 25px rgba(0,0,0,.06); }
    .logo { display:flex; justify-content:center; margin-bottom:14px; }
    .logo img { height:64px; width:auto; }
    h1 { text-align:center; font-size:20px; margin:0 0 18px 0; color:#0f172a; }
    label { display:block; font-size:13px; margin:10px 0 6px; color:#334155; }
    input { width:100%; padding:10px 12px; border-radius:10px; border:1px solid #cbd5e1; background:#ffffff; color:#0f172a; }
    button { width:100%; margin-top:14px; padding:10px 12px; border-radius:10px; border:0; background:#2563eb; color:white; font-weight:700; cursor:pointer; }
    .err { margin-top: 10px; color:#dc2626; font-size: 13px; text-align:center; }
    .hint { margin-top: 10px; color:#64748b; font-size: 12px; text-align:center; }
  </style>
</head>
<body>
  <div class="login-wrap">
    <div class="login-card">
      <div class="logo">
        <img src="/static/img/logo.png" alt="NettSan Technology">
      </div>
      <h1>IndFlow</h1>
      <form method="post">
        <label>Email</label>
        <input name="email" type="email" autocomplete="username" required />
        <label>Senha</label>
        <input name="senha" type="password" autocomplete="current-password" required />
        <button type="submit">Entrar</button>
        {% if error %}<div class="err">{{ error }}</div>{% endif %}
        <div class="hint">Acesso restrito.</div>
      </form>
    </div>
  </div>
</body>
</html>
"""

USERS_HOME_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Usuarios - IndFlow</title>
  <link rel="stylesheet" href="/static/style.css?v=2">
  <style>
    body { margin:0; background:#f8fafc; font-family: Arial, sans-serif; }
    .wrap { max-width:1100px; margin:0 auto; padding:24px; }
    .card { background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin-bottom:16px; }
    h1 { margin:0 0 8px 0; }
    .muted { color:#64748b; font-size:13px; }
    .row { display:flex; gap:12px; flex-wrap:wrap; align-items:flex-end; }
    label { display:block; font-size:13px; margin:10px 0 6px; color:#334155; }
    input, select { padding:10px 12px; border-radius:10px; border:1px solid #cbd5e1; background:#ffffff; color:#0f172a; min-width:260px; }
    button { padding:10px 14px; border-radius:10px; border:0; background:#2563eb; color:white; font-weight:700; cursor:pointer; }
    button.secondary { background:#475569; }
    button.danger { background:#dc2626; }
    table { width:100%; border-collapse:collapse; }
    th, td { padding:10px; border-bottom:1px solid #e5e7eb; text-align:left; font-size:14px; vertical-align:top; }
    th { background:#f1f5f9; }
    .pill { display:inline-block; padding:3px 10px; border-radius:999px; font-size:12px; font-weight:700; }
    .on { background:#dcfce7; color:#166534; }
    .off { background:#fee2e2; color:#991b1b; }
    .top-actions { display:flex; gap:10px; justify-content:flex-end; margin-bottom:10px; }
    a { color:#2563eb; text-decoration:none; }
    .msg { margin-top:8px; font-size:13px; }
    .msg.ok { color:#166534; }
    .msg.err { color:#991b1b; }
  </style>
</head>
<body>
  <div class="wrap">

    <div class="top-actions">
      <a href="/admin/" class="muted">Voltar</a>
      <a href="/admin/logout" class="muted">Sair</a>
    </div>

    <div class="card">
      <h1>Usuarios</h1>
      <div class="muted">
        Limite: {{ max_active }} usuarios ativos por cliente.
      </div>
      {% if message %}
        <div class="msg ok">{{ message }}</div>
      {% endif %}
      {% if error %}
        <div class="msg err">{{ error }}</div>
      {% endif %}
    </div>

    <div class="card">
      <h2 style="margin:0 0 10px 0;">Criar usuario</h2>
      <form method="post" action="/admin/usuarios/create">
        <div class="row">
          {% if is_superadmin %}
          <div>
            <label>Cliente (cliente_id)</label>
            <select name="cliente_id" required>
              {% for c in clientes %}
                <option value="{{ c.id }}">{{ c.nome }} ({{ c.id }})</option>
              {% endfor %}
            </select>
          </div>
          {% endif %}

          <div>
            <label>Email</label>
            <input name="email" type="email" placeholder="ex: pessoa@cliente.com" required>
          </div>

          <div>
            <label>Senha</label>
            <input name="senha" type="text" placeholder="defina uma senha" required>
          </div>

          <div>
            <label>Role</label>
            <select name="role" required>
              <option value="admin">admin</option>
              <option value="viewer">viewer</option>
            </select>
          </div>

          <div>
            <button type="submit">Criar</button>
          </div>
        </div>
      </form>
      <div class="muted" style="margin-top:10px;">
        Dica: admin pode gerenciar usuarios. viewer apenas usa o sistema.
      </div>
    </div>

    <div class="card">
      <h2 style="margin:0 0 10px 0;">Lista</h2>
      <table>
        <thead>
          <tr>
            <th>Email</th>
            <th>Role</th>
            <th>Status</th>
            <th>Criado em (UTC)</th>
            {% if is_superadmin %}<th>Cliente</th>{% endif %}
            <th style="width:340px;">Acoes</th>
          </tr>
        </thead>
        <tbody>
          {% for u in usuarios %}
          <tr>
            <td><b>{{ u.email }}</b></td>
            <td class="muted">{{ u.role }}</td>
            <td>
              {% if u.status == "active" %}
                <span class="pill on">ATIVO</span>
              {% else %}
                <span class="pill off">INATIVO</span>
              {% endif %}
            </td>
            <td class="muted">{{ u.created_at }}</td>
            {% if is_superadmin %}
              <td class="muted">{{ u.cliente_id }}</td>
            {% endif %}
            <td>
              <div style="display:flex; gap:8px; flex-wrap:wrap;">
                <form method="post" action="/admin/usuarios/toggle" style="margin:0;">
                  <input type="hidden" name="user_id" value="{{ u.id }}">
                  <button type="submit" class="secondary">
                    {% if u.status == "active" %}Desativar{% else %}Ativar{% endif %}
                  </button>
                </form>

                <form method="post" action="/admin/usuarios/role" style="margin:0;">
                  <input type="hidden" name="user_id" value="{{ u.id }}">
                  <select name="role" required>
                    <option value="admin" {% if u.role=="admin" %}selected{% endif %}>admin</option>
                    <option value="viewer" {% if u.role=="viewer" %}selected{% endif %}>viewer</option>
                  </select>
                  <button type="submit" class="secondary">Salvar role</button>
                </form>

                <form method="post" action="/admin/usuarios/reset-senha" style="margin:0;">
                  <input type="hidden" name="user_id" value="{{ u.id }}">
                  <input name="senha" type="text" placeholder="nova senha" required style="min-width:180px;">
                  <button type="submit" class="danger">Trocar senha</button>
                </form>
              </div>
            </td>
          </tr>
          {% endfor %}
          {% if not usuarios %}
            <tr><td colspan="6" class="muted">Nenhum usuario encontrado.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>

  </div>
</body>
</html>
"""


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _exists_any_user() -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM usuarios LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row is not None


def _get_user_by_email(email: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, email, senha_hash, cliente_id, role, status FROM usuarios WHERE email = ? LIMIT 1",
        ((email or "").strip().lower(),),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0],
        "email": row[1],
        "senha_hash": row[2],
        "cliente_id": row[3],
        "role": row[4],
        "status": row[5],
    }


def _is_logged_in() -> bool:
    return bool(session.get("user_id"))


def _role() -> str:
    return (session.get("role") or "").strip().lower()


def _is_superadmin() -> bool:
    return _role() == "superadmin"


def _is_admin() -> bool:
    return _role() in ("admin", "superadmin")


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_logged_in():
            return redirect(url_for("admin.login"))
        return fn(*args, **kwargs)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_logged_in():
            return redirect(url_for("admin.login"))
        if not _is_admin():
            return "Acesso negado", 403
        return fn(*args, **kwargs)
    return wrapper


def _require_same_cliente_or_superadmin(target_cliente_id: str) -> bool:
    if _is_superadmin():
        return True
    return (session.get("cliente_id") or "") == (target_cliente_id or "")


def _password_ok(pw: str) -> bool:
    pw = (pw or "").strip()
    return len(pw) >= 6


def _normalize_role(role: str) -> str:
    r = (role or "").strip().lower()
    if r not in ("admin", "viewer"):
        return "viewer"
    return r


def _count_active_users(cliente_id: str) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM usuarios WHERE cliente_id = ? AND status = 'active'", (cliente_id,))
    row = cur.fetchone()
    conn.close()
    return int(row[0] or 0) if row else 0


def _list_users_for_cliente(cliente_id: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, email, role, status, created_at, cliente_id FROM usuarios WHERE cliente_id = ? ORDER BY created_at DESC",
        (cliente_id,),
    )
    rows = cur.fetchall()
    conn.close()
    out = []
    for r in rows or []:
        out.append(
            {
                "id": r[0],
                "email": r[1],
                "role": r[2],
                "status": r[3],
                "created_at": r[4],
                "cliente_id": r[5],
            }
        )
    return out


def _list_all_users():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, email, role, status, created_at, cliente_id FROM usuarios ORDER BY created_at DESC"
    )
    rows = cur.fetchall()
    conn.close()
    out = []
    for r in rows or []:
        out.append(
            {
                "id": r[0],
                "email": r[1],
                "role": r[2],
                "status": r[3],
                "created_at": r[4],
                "cliente_id": r[5],
            }
        )
    return out


def _list_clientes():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, nome FROM clientes ORDER BY created_at DESC")
    rows = cur.fetchall()
    conn.close()
    out = []
    for r in rows or []:
        out.append({"id": r[0], "nome": r[1]})
    return out


def _get_user_by_id(user_id: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, email, role, status, created_at, cliente_id FROM usuarios WHERE id = ? LIMIT 1",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0],
        "email": row[1],
        "role": row[2],
        "status": row[3],
        "created_at": row[4],
        "cliente_id": row[5],
    }


def _update_user_status(user_id: str, status: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE usuarios SET status = ? WHERE id = ?", (status, user_id))
    conn.commit()
    conn.close()


def _update_user_role(user_id: str, role: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE usuarios SET role = ? WHERE id = ?", (role, user_id))
    conn.commit()
    conn.close()


def _update_user_password(user_id: str, senha_plain: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE usuarios SET senha_hash = ? WHERE id = ?", (_sha256(senha_plain), user_id))
    conn.commit()
    conn.close()


def _create_user(email: str, senha: str, cliente_id: str, role: str) -> str:
    user_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at) VALUES (?, ?, ?, ?, ?, 'active', ?)",
        (user_id, (email or "").strip().lower(), _sha256(senha), cliente_id, role, now),
    )
    conn.commit()
    conn.close()
    return user_id


def _upsert_admin_user(email: str, senha: str) -> dict:
    email = (email or "").strip().lower()
    senha = senha or ""
    if not email or not senha:
        raise ValueError("email e senha sao obrigatorios")

    senha_hash = _sha256(senha)
    now = datetime.utcnow().isoformat()

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id, cliente_id FROM usuarios WHERE email = ? LIMIT 1", (email,))
    row = cur.fetchone()

    if row is None:
        cliente_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())

        cur.execute(
            "INSERT INTO clientes (id, nome, api_key_hash, status, created_at) VALUES (?, ?, ?, 'active', ?)",
            (cliente_id, "DEFAULT", "INIT", now),
        )

        cur.execute(
            "INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at) VALUES (?, ?, ?, ?, 'admin', 'active', ?)",
            (user_id, email, senha_hash, cliente_id, now),
        )
        conn.commit()
        conn.close()
        return {"mode": "created", "email": email}

    cur.execute("UPDATE usuarios SET senha_hash = ? WHERE email = ?", (senha_hash, email))
    conn.commit()
    conn.close()
    return {"mode": "updated", "email": email}


@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if _is_logged_in():
            return redirect(url_for("admin.home"))
        return render_template_string(LOGIN_FORM_HTML, error=None)

    email = (request.form.get("email") or "").strip().lower()
    senha = request.form.get("senha") or ""

    if not email or not senha:
        return render_template_string(LOGIN_FORM_HTML, error="Informe email e senha.")

    user = _get_user_by_email(email)
    if not user or user.get("status") != "active":
        return render_template_string(LOGIN_FORM_HTML, error="Email ou senha invalidos.")

    if _sha256(senha) != (user.get("senha_hash") or ""):
        return render_template_string(LOGIN_FORM_HTML, error="Email ou senha invalidos.")

    session["user_id"] = user["id"]
    session["email"] = user["email"]
    session["cliente_id"] = user["cliente_id"]

    sess_role = (user.get("role") or "viewer").strip().lower()

    if (user.get("email") or "").strip().lower() == "admin@admin":
        sess_role = "superadmin"
        try:
            conn = get_db()
            try:
                conn.execute("UPDATE usuarios SET role = 'superadmin' WHERE email = ?", ("admin@admin",))
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    session["role"] = sess_role

    return redirect(url_for("admin.home"))


@admin_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("admin.login"))


@admin_bp.route("/")
@login_required
def home():
    return render_template("admin_home.html")


# ============================================================
# CLIENTES (no ADMIN)
# ============================================================
def _clientes_rows():
    """Carrega lista de clientes para a tela de listagem (defensivo para schema)."""
    db = get_db()

    # Descobrir colunas existentes
    cols = []
    try:
        cur = db.execute("PRAGMA table_info(clientes)")
        cols = [r[1] for r in cur.fetchall()]  # name
    except Exception:
        cols = []

    # Mapear colunas (preferencia)
    col_id = "id" if "id" in cols else None
    col_nome = "nome" if "nome" in cols else ("razao_social" if "razao_social" in cols else None)
    col_status = "status" if "status" in cols else ("ativo" if "ativo" in cols else None)
    col_created = "created_at" if "created_at" in cols else ("created_at_utc" if "created_at_utc" in cols else None)

    if not col_id or not col_nome:
        return []

    select_cols = [col_id, col_nome]
    if col_status:
        select_cols.append(col_status)
    if col_created:
        select_cols.append(col_created)

    q = "SELECT " + ", ".join(select_cols) + " FROM clientes"
    if col_created:
        q += " ORDER BY " + col_created + " DESC"
    else:
        q += " ORDER BY " + col_id + " DESC"

    try:
        cur = db.execute(q)
        raw = cur.fetchall()
    except Exception:
        return []

    rows = []
    for r in raw:
        d = dict(r)
        nome = d.get(col_nome) or ""
        status_val = ""
        if col_status:
            sv = d.get(col_status)
            if col_status == "ativo":
                status_val = "active" if int(sv or 0) == 1 else "inactive"
            else:
                status_val = (sv or "")
        created_val = d.get(col_created) if col_created else ""
        rows.append({
            "id": d.get(col_id),
            "nome": nome,
            "status": status_val,
            "created_at": created_val,
        })
    return rows


@admin_bp.route("/clientes", methods=["GET"])
@admin_required
def admin_clientes_list():
    rows = _clientes_rows()
    return render_template("clientes_list.html", rows=rows)


@admin_bp.route("/clientes/novo", methods=["GET"])
@admin_required
def admin_clientes_novo():
    return render_template("clientes_form.html", mode="create")

@admin_bp.route("/usuarios", methods=["GET"])
@admin_required
def usuarios_home():
    max_active = 5
    msg = (request.args.get("msg") or "").strip()
    err = (request.args.get("err") or "").strip()

    if _is_superadmin():
        usuarios = _list_all_users()
        clientes = _list_clientes()
        return render_template_string(
            USERS_HOME_HTML,
            usuarios=usuarios,
            clientes=clientes,
            is_superadmin=True,
            max_active=max_active,
            message=msg if msg else None,
            error=err if err else None,
        )

    cliente_id = session.get("cliente_id") or ""
    usuarios = _list_users_for_cliente(cliente_id)
    return render_template_string(
        USERS_HOME_HTML,
        usuarios=usuarios,
        clientes=[],
        is_superadmin=False,
        max_active=max_active,
        message=msg if msg else None,
        error=err if err else None,
    )


@admin_bp.route("/usuarios/create", methods=["POST"])
@admin_required
def usuarios_create():
    max_active = 5

    role = _normalize_role(request.form.get("role"))
    email = (request.form.get("email") or "").strip().lower()
    senha = (request.form.get("senha") or "").strip()

    if not email or not senha:
        return redirect(url_for("admin.usuarios_home", err="Informe email e senha."))

    if not _password_ok(senha):
        return redirect(url_for("admin.usuarios_home", err="Senha muito curta (minimo 6)."))

    if _is_superadmin():
        cliente_id = (request.form.get("cliente_id") or "").strip()
        if not cliente_id:
            return redirect(url_for("admin.usuarios_home", err="cliente_id obrigatorio."))
    else:
        cliente_id = (session.get("cliente_id") or "").strip()

    active_count = _count_active_users(cliente_id)
    if active_count >= max_active:
        return redirect(url_for("admin.usuarios_home", err="Limite de 5 usuarios ativos atingido para este cliente."))

    existing = _get_user_by_email(email)
    if existing:
        return redirect(url_for("admin.usuarios_home", err="Email ja existe."))

    _create_user(email=email, senha=senha, cliente_id=cliente_id, role=role)
    return redirect(url_for("admin.usuarios_home", msg="Usuario criado."))


@admin_bp.route("/usuarios/toggle", methods=["POST"])
@admin_required
def usuarios_toggle():
    user_id = (request.form.get("user_id") or "").strip()
    if not user_id:
        return redirect(url_for("admin.usuarios_home", err="user_id invalido."))

    target = _get_user_by_id(user_id)
    if not target:
        return redirect(url_for("admin.usuarios_home", err="Usuario nao encontrado."))

    if not _require_same_cliente_or_superadmin(target.get("cliente_id")):
        return ("Acesso negado", 403)

    if (session.get("user_id") or "") == target.get("id"):
        return redirect(url_for("admin.usuarios_home", err="Voce nao pode desativar seu proprio usuario."))

    new_status = "inactive" if (target.get("status") == "active") else "active"

    if new_status == "active":
        max_active = 5
        active_count = _count_active_users(target.get("cliente_id"))
        if active_count >= max_active:
            return redirect(url_for("admin.usuarios_home", err="Limite de 5 usuarios ativos atingido para este cliente."))

    _update_user_status(user_id, new_status)
    return redirect(url_for("admin.usuarios_home", msg="Status atualizado."))


@admin_bp.route("/usuarios/role", methods=["POST"])
@admin_required
def usuarios_set_role():
    user_id = (request.form.get("user_id") or "").strip()
    role = _normalize_role(request.form.get("role"))

    if not user_id:
        return redirect(url_for("admin.usuarios_home", err="user_id invalido."))

    target = _get_user_by_id(user_id)
    if not target:
        return redirect(url_for("admin.usuarios_home", err="Usuario nao encontrado."))

    if not _require_same_cliente_or_superadmin(target.get("cliente_id")):
        return ("Acesso negado", 403)

    _update_user_role(user_id, role)
    return redirect(url_for("admin.usuarios_home", msg="Role atualizado."))


@admin_bp.route("/usuarios/reset-senha", methods=["POST"])
@admin_required
def usuarios_reset_senha():
    user_id = (request.form.get("user_id") or "").strip()
    senha = (request.form.get("senha") or "").strip()

    if not user_id:
        return redirect(url_for("admin.usuarios_home", err="user_id invalido."))

    if not _password_ok(senha):
        return redirect(url_for("admin.usuarios_home", err="Senha muito curta (minimo 6)."))

    target = _get_user_by_id(user_id)
    if not target:
        return redirect(url_for("admin.usuarios_home", err="Usuario nao encontrado."))

    if not _require_same_cliente_or_superadmin(target.get("cliente_id")):
        return ("Acesso negado", 403)

    _update_user_password(user_id, senha)
    return redirect(url_for("admin.usuarios_home", msg="Senha atualizada."))


@admin_bp.route("/bootstrap", methods=["POST"])
def bootstrap():
    if _exists_any_user():
        if not _is_logged_in():
            return jsonify({"error": "Login obrigatorio"}), 401
        if not _is_admin():
            return jsonify({"error": "Apenas admin pode executar bootstrap"}), 403

    data = request.get_json(silent=True) or {}
    nome_cliente = data.get("nome_cliente")
    email = data.get("email")
    senha = data.get("senha")

    if not nome_cliente or not email or not senha:
        return jsonify({"error": "Campos obrigatorios: nome_cliente, email, senha"}), 400

    if not _password_ok(senha):
        return jsonify({"error": "Senha muito curta (minimo 6)."}), 400

    cliente_id = str(uuid.uuid4())
    usuario_id = str(uuid.uuid4())

    api_key_plain = secrets.token_urlsafe(32)
    api_key_hash = hashlib.sha256(api_key_plain.encode("utf-8")).hexdigest()

    senha_hash = _sha256(senha)

    now = datetime.utcnow().isoformat()

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO clientes (id, nome, api_key_hash, status, created_at) VALUES (?, ?, ?, 'active', ?)",
            (cliente_id, nome_cliente, api_key_hash, now),
        )
        cur.execute(
            "INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at) VALUES (?, ?, ?, ?, 'admin', 'active', ?)",
            (usuario_id, email.strip().lower(), senha_hash, cliente_id, now),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": "Falha ao criar cliente", "details": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return jsonify(
        {
            "cliente_id": cliente_id,
            "usuario_admin": email.strip().lower(),
            "api_key": api_key_plain,
            "warning": "Guarde esta API Key. Ela NAO sera exibida novamente.",
        }
    ), 201


@admin_bp.route("/dev-reset-admin", methods=["POST"])
def dev_reset_admin():
    expected = (os.getenv("ADMIN_RESET_TOKEN") or "").strip()
    if not expected:
        return jsonify({"error": "ADMIN_RESET_TOKEN nao configurado"}), 403

    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if token != expected:
        return jsonify({"error": "Token invalido"}), 403

    try:
        result = _upsert_admin_user(email=email, senha=senha)
        return jsonify({"status": "ok", **result}), 200
    except Exception as e:
        return jsonify({"error": "Falha ao resetar admin", "details": str(e)}), 400


@admin_bp.route("/db-dump-producao-horaria", methods=["POST"])
def db_dump_producao_horaria():
    expected = (os.getenv("ADMIN_RESET_TOKEN") or "").strip()
    if not expected:
        return jsonify({"error": "ADMIN_RESET_TOKEN nao configurado"}), 403

    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    if token != expected:
        return jsonify({"error": "Token invalido"}), 403

    machine_id = (data.get("machine_id") or "").strip()

    conn = get_db()
    cur = conn.cursor()

    out = {
        "ok": True,
        "now_utc": datetime.utcnow().isoformat(),
        "env": {
            "INDFLOW_DB_PATH": (os.getenv("INDFLOW_DB_PATH") or "").strip() or None,
            "is_railway": bool((os.getenv("RAILWAY_PROJECT_ID") or "").strip()),
        },
        "limits": {"top_refs": 10, "top_machines": 50, "sample_rows": 50},
    }

    try:
        row = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='producao_horaria'"
        ).fetchone()
        out["table_exists"] = bool(row)
        if not out["table_exists"]:
            return jsonify(out), 200

        top_refs = []
        for r in cur.execute(
            """
            SELECT data_ref, COUNT(*) AS qtd
            FROM producao_horaria
            GROUP BY data_ref
            ORDER BY qtd DESC
            LIMIT 10
            """
        ):
            top_refs.append({"data_ref": r[0], "qtd": int(r[1] or 0)})
        out["top_data_ref"] = top_refs

        today = datetime.utcnow().date().isoformat()
        yest = (datetime.utcnow().date() - timedelta(days=1)).isoformat()

        def _count_for(dref: str) -> int:
            r = cur.execute(
                "SELECT COUNT(*) FROM producao_horaria WHERE data_ref = ?",
                (dref,),
            ).fetchone()
            return int(r[0] or 0) if r else 0

        out["count_today_utc"] = {"data_ref": today, "qtd": _count_for(today)}
        out["count_yesterday_utc"] = {"data_ref": yest, "qtd": _count_for(yest)}

        def _top_machines(dref: str):
            rows = []
            for r in cur.execute(
                """
                SELECT COALESCE(cliente_id,'__NULL__') AS cliente_id, machine_id, COUNT(*) AS qtd
                FROM producao_horaria
                WHERE data_ref = ?
                GROUP BY COALESCE(cliente_id,'__NULL__'), machine_id
                ORDER BY qtd DESC
                LIMIT 50
                """,
                (dref,),
            ):
                rows.append({"cliente_id": r[0], "machine_id": r[1], "qtd": int(r[2] or 0)})
            return rows

        out["top_machines_today_utc"] = _top_machines(today)
        out["top_machines_yesterday_utc"] = _top_machines(yest)

        if machine_id:
            sample = []
            for r in cur.execute(
                """
                SELECT COALESCE(cliente_id,'__NULL__') AS cliente_id,
                       machine_id, data_ref, hora_idx,
                       baseline_esp, esp_last, produzido, meta, percentual, updated_at
                FROM producao_horaria
                WHERE machine_id = ?
                ORDER BY data_ref DESC, hora_idx DESC
                LIMIT 50
                """,
                (machine_id,),
            ):
                sample.append(
                    {
                        "cliente_id": r[0],
                        "machine_id": r[1],
                        "data_ref": r[2],
                        "hora_idx": int(r[3] or 0),
                        "baseline_esp": int(r[4] or 0),
                        "esp_last": int(r[5] or 0),
                        "produzido": int(r[6] or 0),
                        "meta": int(r[7] or 0),
                        "percentual": int(r[8] or 0),
                        "updated_at": r[9],
                    }
                )
            out["sample_by_machine_id"] = {"machine_id": machine_id, "rows": sample}

    except Exception as e:
        out["ok"] = False
        out["error"] = "Falha ao consultar producao_horaria"
        out["details"] = str(e)
        return jsonify(out), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return jsonify(out), 200
