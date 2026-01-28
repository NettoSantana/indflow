# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\admin\routes.py
# LAST_RECODE: 2026-01-28 21:10 America/Bahia
# MOTIVO: Criar Admin do cliente (opcao 2): telas e rotas /admin/usuarios e /admin/dispositivos, escopadas por cliente_id, com roles admin/viewer.

from flask import Blueprint, request, jsonify, session, redirect, url_for, render_template_string
from datetime import datetime
import uuid
import secrets
import hashlib
import os
from functools import wraps

from modules.db_indflow import get_db

admin_bp = Blueprint("admin", __name__, template_folder="templates")

# ============================================================
# AUTH (V1 simples) - sessao + sha256
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
    input, select { width:100%; padding:10px 12px; border-radius:10px; border:1px solid #cbd5e1; background:#ffffff; color:#0f172a; }
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


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def _exists_any_user() -> bool:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM usuarios LIMIT 1")
        return cur.fetchone() is not None
    finally:
        conn.close()


def _get_user_by_email(email: str):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, email, senha_hash, cliente_id, role, status
            FROM usuarios
            WHERE email = ?
            LIMIT 1
            """,
            (email,),
        )
        row = cur.fetchone()
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
    finally:
        conn.close()


def _is_logged_in() -> bool:
    return bool(session.get("user_id"))


def _is_admin() -> bool:
    return session.get("role") == "admin"


def _cliente_id_or_none() -> str | None:
    cid = (session.get("cliente_id") or "").strip()
    return cid or None


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


def _upsert_admin_user(email: str, senha: str) -> dict:
    """
    Cria admin se nao existir, ou atualiza senha se existir.
    Retorna: {"mode": "created"|"updated", "email": "..."}
    """
    email = (email or "").strip().lower()
    senha = senha or ""
    if not email or not senha:
        raise ValueError("email e senha sao obrigatorios")

    senha_hash = _sha256(senha)
    now = _utc_now_iso()

    conn = get_db()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id, cliente_id FROM usuarios WHERE email = ? LIMIT 1", (email,))
        row = cur.fetchone()

        if row is None:
            cliente_id = str(uuid.uuid4())
            user_id = str(uuid.uuid4())

            cur.execute(
                """
                INSERT INTO clientes (id, nome, api_key_hash, status, created_at)
                VALUES (?, ?, ?, 'active', ?)
                """,
                (cliente_id, "DEFAULT", "INIT", now),
            )

            cur.execute(
                """
                INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at)
                VALUES (?, ?, ?, ?, 'admin', 'active', ?)
                """,
                (user_id, email, senha_hash, cliente_id, now),
            )

            conn.commit()
            return {"mode": "created", "email": email}

        cur.execute("UPDATE usuarios SET senha_hash = ? WHERE email = ?", (senha_hash, email))
        conn.commit()
        return {"mode": "updated", "email": email}

    finally:
        conn.close()


# ============================================================
# ADMIN HOME (com atalhos)
# ============================================================
ADMIN_HOME_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin - IndFlow</title>
  <link rel="stylesheet" href="/static/style.css?v=2">
  <style>
    body { margin:0; background:#f8fafc; font-family: Arial, sans-serif; }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 24px; }
    h1 { margin: 0 0 8px 0; color:#0f172a; }
    p { margin: 0 0 18px 0; color:#334155; }
    .grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
    .card { background:#fff; border:1px solid #e5e7eb; border-radius: 12px; padding: 16px; }
    .card h2 { margin:0 0 6px 0; font-size: 16px; color:#0f172a; }
    .card p { margin:0 0 10px 0; font-size: 13px; color:#475569; }
    .btn { display:inline-block; padding:10px 12px; border-radius: 10px; background:#2563eb; color:#fff; text-decoration:none; font-weight:700; }
    .btn.secondary { background:#0f172a; }
    .muted { font-size: 12px; color:#64748b; margin-top: 10px; }
    @media (max-width: 860px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Painel Administrativo</h1>
    <p>Gerenciamento do cliente atual.</p>

    {% if is_admin %}
      <div class="grid">
        <div class="card">
          <h2>Usuarios</h2>
          <p>Criar e gerenciar usuarios do seu cliente.</p>
          <a class="btn" href="/admin/usuarios">Abrir</a>
        </div>
        <div class="card">
          <h2>Dispositivos</h2>
          <p>Ver e vincular MAC (device) a uma maquina.</p>
          <a class="btn secondary" href="/admin/dispositivos">Abrir</a>
        </div>
      </div>
      <div class="muted">Role: admin</div>
    {% else %}
      <div class="card">
        <h2>Acesso</h2>
        <p>Seu perfil e viewer. Voce pode acessar apenas Producao.</p>
      </div>
      <div class="muted">Role: viewer</div>
    {% endif %}
  </div>
</body>
</html>
"""


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
    session["role"] = user["role"]

    return redirect(url_for("admin.home"))


@admin_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("admin.login"))


@admin_bp.route("/")
@login_required
def home():
    return render_template_string(ADMIN_HOME_HTML, is_admin=_is_admin())


# ============================================================
# DEV RESET ADMIN (para acertar o banco do Railway)
# ============================================================
@admin_bp.route("/dev-reset-admin", methods=["POST"])
def dev_reset_admin():
    """
    Uso:
      POST /admin/dev-reset-admin
      JSON: { "token": "<ADMIN_RESET_TOKEN>", "email": "admin@admin", "senha": "Admin123" }

    So funciona se existir a env ADMIN_RESET_TOKEN.
    """
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


# ============================================================
# ADMIN BOOTSTRAP
# Cria cliente + usuario admin + API Key (mostrada 1x)
# Regra:
#   - Se NAO existir nenhum usuario ainda -> bootstrap liberado (primeira vez)
#   - Se ja existir -> exige login e role admin
# ============================================================
@admin_bp.route("/bootstrap", methods=["POST"])
def bootstrap_cliente():
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

    cliente_id = str(uuid.uuid4())
    usuario_id = str(uuid.uuid4())

    api_key_plain = secrets.token_urlsafe(32)
    api_key_hash = hashlib.sha256(api_key_plain.encode()).hexdigest()

    senha_hash = hashlib.sha256(senha.encode()).hexdigest()

    now = datetime.utcnow().isoformat()

    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO clientes (id, nome, api_key_hash, status, created_at)
            VALUES (?, ?, ?, 'active', ?)
            """,
            (cliente_id, nome_cliente, api_key_hash, now),
        )

        cur.execute(
            """
            INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at)
            VALUES (?, ?, ?, ?, 'admin', 'active', ?)
            """,
            (usuario_id, email, senha_hash, cliente_id, now),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        return jsonify({"error": "Falha ao criar cliente", "details": str(e)}), 500

    finally:
        conn.close()

    return jsonify(
        {
            "cliente_id": cliente_id,
            "usuario_admin": email,
            "api_key": api_key_plain,
            "warning": "Guarde esta API Key. Ela NAO sera exibida novamente.",
        }
    ), 201


# ============================================================
# ADMIN - USUARIOS (escopado por cliente_id)
# ============================================================
USUARIOS_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Usuarios - Admin</title>
  <link rel="stylesheet" href="/static/style.css?v=2">
  <style>
    body { margin:0; background:#f8fafc; font-family: Arial, sans-serif; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 24px; }
    h1 { margin: 0 0 12px 0; color:#0f172a; }
    .top { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
    a.link { color:#2563eb; text-decoration:none; font-weight:700; }
    .card { background:#fff; border:1px solid #e5e7eb; border-radius: 12px; padding: 16px; margin-top: 12px; }
    table { width:100%; border-collapse: collapse; }
    th, td { text-align:left; padding:10px; border-bottom:1px solid #e5e7eb; font-size: 13px; }
    th { color:#0f172a; font-weight:700; }
    td { color:#334155; }
    .row { display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    label { display:block; font-size: 12px; color:#334155; margin-bottom: 6px; }
    input, select { width:100%; padding:10px 12px; border-radius:10px; border:1px solid #cbd5e1; }
    button { padding:10px 12px; border-radius:10px; border:0; background:#2563eb; color:#fff; font-weight:800; cursor:pointer; }
    .btn2 { background:#0f172a; }
    .err { color:#dc2626; font-size: 13px; margin-top: 8px; }
    .ok { color:#16a34a; font-size: 13px; margin-top: 8px; }
    @media (max-width: 980px) { .row { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Usuarios</h1>
      <div>
        <a class="link" href="/admin/">Voltar</a>
      </div>
    </div>

    <div class="card">
      <h3 style="margin:0 0 10px 0;">Criar usuario</h3>
      <form method="post" action="/admin/usuarios/criar">
        <div class="row">
          <div>
            <label>Email</label>
            <input name="email" type="email" required />
          </div>
          <div>
            <label>Senha</label>
            <input name="senha" type="text" required />
          </div>
          <div>
            <label>Role</label>
            <select name="role" required>
              <option value="viewer">viewer</option>
              <option value="admin">admin</option>
            </select>
          </div>
          <div style="display:flex; align-items:flex-end;">
            <button type="submit">Criar</button>
          </div>
        </div>
        {% if msg_ok %}<div class="ok">{{ msg_ok }}</div>{% endif %}
        {% if msg_err %}<div class="err">{{ msg_err }}</div>{% endif %}
      </form>
    </div>

    <div class="card">
      <h3 style="margin:0 0 10px 0;">Lista</h3>
      <table>
        <thead>
          <tr>
            <th>Email</th>
            <th>Role</th>
            <th>Status</th>
            <th>Acoes</th>
          </tr>
        </thead>
        <tbody>
          {% for u in users %}
          <tr>
            <td>{{ u.email }}</td>
            <td>{{ u.role }}</td>
            <td>{{ u.status }}</td>
            <td>
              <form method="post" action="/admin/usuarios/status" style="margin:0; display:inline;">
                <input type="hidden" name="user_id" value="{{ u.id }}" />
                <input type="hidden" name="next_status" value="{{ 'inactive' if u.status=='active' else 'active' }}" />
                <button class="btn2" type="submit">{{ 'Desativar' if u.status=='active' else 'Ativar' }}</button>
              </form>
            </td>
          </tr>
          {% endfor %}
          {% if users|length == 0 %}
          <tr><td colspan="4">Nenhum usuario encontrado.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""


def _list_users_by_cliente(cliente_id: str) -> list[dict]:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, email, role, status
            FROM usuarios
            WHERE cliente_id = ?
            ORDER BY email ASC
            """,
            (cliente_id,),
        )
        rows = cur.fetchall() or []
        out = []
        for r in rows:
            out.append({"id": r[0], "email": r[1], "role": r[2], "status": r[3]})
        return out
    finally:
        conn.close()


def _create_user_for_cliente(cliente_id: str, email: str, senha: str, role: str) -> tuple[bool, str]:
    email = (email or "").strip().lower()
    senha = (senha or "").strip()
    role = (role or "").strip().lower()

    if not email or not senha:
        return False, "Email e senha sao obrigatorios."

    if role not in ("admin", "viewer"):
        return False, "Role invalida."

    user_id = str(uuid.uuid4())
    senha_hash = _sha256(senha)
    now = _utc_now_iso()

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM usuarios WHERE email = ? LIMIT 1", (email,))
        if cur.fetchone() is not None:
            return False, "Email ja existe."

        cur.execute(
            """
            INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?)
            """,
            (user_id, email, senha_hash, cliente_id, role, now),
        )
        conn.commit()
        return True, "Usuario criado."

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"Falha ao criar usuario: {e}"

    finally:
        conn.close()


def _set_user_status(cliente_id: str, user_id: str, next_status: str) -> tuple[bool, str]:
    user_id = (user_id or "").strip()
    next_status = (next_status or "").strip().lower()
    if not user_id:
        return False, "user_id obrigatorio"
    if next_status not in ("active", "inactive"):
        return False, "status invalido"

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE usuarios SET status = ? WHERE id = ? AND cliente_id = ?",
            (next_status, user_id, cliente_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return False, "Usuario nao encontrado para este cliente."
        return True, "Status atualizado."
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"Falha ao atualizar status: {e}"
    finally:
        conn.close()


@admin_bp.route("/usuarios", methods=["GET"])
@admin_required
def usuarios_page():
    cid = _cliente_id_or_none()
    if not cid:
        return "Cliente nao definido na sessao", 400

    msg_ok = request.args.get("ok")
    msg_err = request.args.get("err")

    users = _list_users_by_cliente(cid)
    return render_template_string(USUARIOS_HTML, users=users, msg_ok=msg_ok, msg_err=msg_err)


@admin_bp.route("/usuarios/criar", methods=["POST"])
@admin_required
def usuarios_criar():
    cid = _cliente_id_or_none()
    if not cid:
        return redirect("/admin/usuarios?err=Cliente%20nao%20definido")

    email = request.form.get("email") or ""
    senha = request.form.get("senha") or ""
    role = request.form.get("role") or "viewer"

    ok, msg = _create_user_for_cliente(cid, email=email, senha=senha, role=role)
    if ok:
        return redirect("/admin/usuarios?ok=Usuario%20criado")
    return redirect("/admin/usuarios?err=" + msg.replace(" ", "%20"))


@admin_bp.route("/usuarios/status", methods=["POST"])
@admin_required
def usuarios_status():
    cid = _cliente_id_or_none()
    if not cid:
        return redirect("/admin/usuarios?err=Cliente%20nao%20definido")

    user_id = request.form.get("user_id") or ""
    next_status = request.form.get("next_status") or ""

    ok, msg = _set_user_status(cid, user_id=user_id, next_status=next_status)
    if ok:
        return redirect("/admin/usuarios?ok=Status%20atualizado")
    return redirect("/admin/usuarios?err=" + msg.replace(" ", "%20"))


# ============================================================
# ADMIN - DISPOSITIVOS (escopado por cliente_id)
# ============================================================
DISPOSITIVOS_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Dispositivos - Admin</title>
  <link rel="stylesheet" href="/static/style.css?v=2">
  <style>
    body { margin:0; background:#f8fafc; font-family: Arial, sans-serif; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 24px; }
    h1 { margin: 0 0 12px 0; color:#0f172a; }
    .top { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
    a.link { color:#2563eb; text-decoration:none; font-weight:700; }
    .card { background:#fff; border:1px solid #e5e7eb; border-radius: 12px; padding: 16px; margin-top: 12px; }
    table { width:100%; border-collapse: collapse; }
    th, td { text-align:left; padding:10px; border-bottom:1px solid #e5e7eb; font-size: 13px; }
    th { color:#0f172a; font-weight:700; }
    td { color:#334155; }
    input { width:100%; padding:10px 12px; border-radius:10px; border:1px solid #cbd5e1; }
    button { padding:10px 12px; border-radius:10px; border:0; background:#0f172a; color:#fff; font-weight:800; cursor:pointer; }
    .ok { color:#16a34a; font-size: 13px; margin-top: 8px; }
    .err { color:#dc2626; font-size: 13px; margin-top: 8px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Dispositivos</h1>
      <div>
        <a class="link" href="/admin/">Voltar</a>
      </div>
    </div>

    <div class="card">
      <h3 style="margin:0 0 10px 0;">Lista (por cliente)</h3>

      {% if msg_ok %}<div class="ok">{{ msg_ok }}</div>{% endif %}
      {% if msg_err %}<div class="err">{{ msg_err }}</div>{% endif %}

      <table>
        <thead>
          <tr>
            <th>Device ID</th>
            <th>Machine ID</th>
            <th>Alias</th>
            <th>Last Seen</th>
            <th>Salvar</th>
          </tr>
        </thead>
        <tbody>
          {% for d in devices %}
          <tr>
            <form method="post" action="/admin/dispositivos/update">
              <td>
                {{ d.device_id }}
                <input type="hidden" name="device_id" value="{{ d.device_id }}" />
              </td>
              <td><input name="machine_id" value="{{ d.machine_id or '' }}" placeholder="maquina01" /></td>
              <td><input name="alias" value="{{ d.alias or '' }}" placeholder="CORPO" /></td>
              <td>{{ d.last_seen or '' }}</td>
              <td><button type="submit">Salvar</button></td>
            </form>
          </tr>
          {% endfor %}
          {% if devices|length == 0 %}
          <tr><td colspan="5">Nenhum dispositivo encontrado para este cliente.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""


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


def _list_devices_by_cliente(cliente_id: str) -> list[dict]:
    conn = get_db()
    try:
        _ensure_devices_table_min(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT device_id, machine_id, alias, last_seen
            FROM devices
            WHERE cliente_id = ?
            ORDER BY last_seen DESC
            """,
            (cliente_id,),
        )
        rows = cur.fetchall() or []
        out = []
        for r in rows:
            out.append(
                {
                    "device_id": r[0],
                    "machine_id": r[1],
                    "alias": r[2],
                    "last_seen": r[3],
                }
            )
        return out
    finally:
        conn.close()


def _update_device(cliente_id: str, device_id: str, machine_id: str, alias: str) -> tuple[bool, str]:
    device_id = (device_id or "").strip()
    machine_id = (machine_id or "").strip().lower()
    alias = (alias or "").strip()

    if not device_id:
        return False, "device_id obrigatorio"

    conn = get_db()
    try:
        _ensure_devices_table_min(conn)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE devices
               SET machine_id = ?,
                   alias = ?
             WHERE device_id = ?
               AND cliente_id = ?
            """,
            (machine_id or None, alias or None, device_id, cliente_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return False, "Dispositivo nao encontrado para este cliente."
        return True, "Dispositivo atualizado."
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"Falha ao atualizar dispositivo: {e}"
    finally:
        conn.close()


@admin_bp.route("/dispositivos", methods=["GET"])
@admin_required
def dispositivos_page():
    cid = _cliente_id_or_none()
    if not cid:
        return "Cliente nao definido na sessao", 400

    msg_ok = request.args.get("ok")
    msg_err = request.args.get("err")

    devices = _list_devices_by_cliente(cid)
    return render_template_string(DISPOSITIVOS_HTML, devices=devices, msg_ok=msg_ok, msg_err=msg_err)


@admin_bp.route("/dispositivos/update", methods=["POST"])
@admin_required
def dispositivos_update():
    cid = _cliente_id_or_none()
    if not cid:
        return redirect("/admin/dispositivos?err=Cliente%20nao%20definido")

    device_id = request.form.get("device_id") or ""
    machine_id = request.form.get("machine_id") or ""
    alias = request.form.get("alias") or ""

    ok, msg = _update_device(cid, device_id=device_id, machine_id=machine_id, alias=alias)
    if ok:
        return redirect("/admin/dispositivos?ok=Salvo")
    return redirect("/admin/dispositivos?err=" + msg.replace(" ", "%20"))
