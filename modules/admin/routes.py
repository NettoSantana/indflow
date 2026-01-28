from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, render_template_string
from datetime import datetime
import uuid
import secrets
import hashlib
import os
from functools import wraps

from modules.db_indflow import get_db

admin_bp = Blueprint("admin", __name__, template_folder="templates")

# ============================================================
# AUTH (V1 simples) — sessão + sha256
# ============================================================
LOGIN_FORM_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>IndFlow — Login</title>
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


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()


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
        cur.execute("""
            SELECT id, email, senha_hash, cliente_id, role, status
            FROM usuarios
            WHERE email = ?
            LIMIT 1
        """, (email,))
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


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_logged_in():
            return redirect(url_for("admin.login"))
        return fn(*args, **kwargs)
    return wrapper


def _upsert_admin_user(email: str, senha: str) -> dict:
    """
    Cria admin se não existir, ou atualiza senha se existir.
    Retorna: {"mode": "created"|"updated", "email": "..."}
    """
    email = (email or "").strip().lower()
    senha = senha or ""
    if not email or not senha:
        raise ValueError("email e senha são obrigatórios")

    senha_hash = _sha256(senha)
    now = datetime.utcnow().isoformat()

    conn = get_db()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id, cliente_id FROM usuarios WHERE email = ? LIMIT 1", (email,))
        row = cur.fetchone()

        if row is None:
            # cria cliente mínimo + admin
            cliente_id = str(uuid.uuid4())
            user_id = str(uuid.uuid4())

            # cliente precisa ter api_key_hash; como isso é só para login web,
            # colocamos um placeholder. (API real do ESP continua no bootstrap normal.)
            cur.execute("""
                INSERT INTO clientes (id, nome, api_key_hash, status, created_at)
                VALUES (?, ?, ?, 'active', ?)
            """, (cliente_id, "DEFAULT", "INIT", now))

            cur.execute("""
                INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at)
                VALUES (?, ?, ?, ?, 'admin', 'active', ?)
            """, (user_id, email, senha_hash, cliente_id, now))

            conn.commit()
            return {"mode": "created", "email": email}

        # atualiza senha
        cur.execute("UPDATE usuarios SET senha_hash = ? WHERE email = ?", (senha_hash, email))
        conn.commit()
        return {"mode": "updated", "email": email}

    finally:
        conn.close()


@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if _is_logged_in():
            return redirect(url_for("admin.home"))
        return render_template_string(LOGIN_FORM_HTML, error=None)

    # POST
    email = (request.form.get("email") or "").strip().lower()
    senha = request.form.get("senha") or ""

    if not email or not senha:
        return render_template_string(LOGIN_FORM_HTML, error="Informe email e senha.")

    user = _get_user_by_email(email)
    if not user or user.get("status") != "active":
        return render_template_string(LOGIN_FORM_HTML, error="Email ou senha inválidos.")

    if _sha256(senha) != (user.get("senha_hash") or ""):
        return render_template_string(LOGIN_FORM_HTML, error="Email ou senha inválidos.")

    # OK: cria sessão
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
    return render_template("admin_home.html")


# ============================================================
# DEV RESET ADMIN (para acertar o banco do Railway)
# ============================================================
@admin_bp.route("/dev-reset-admin", methods=["POST"])
def dev_reset_admin():
    """
    Uso:
      POST /admin/dev-reset-admin
      JSON: { "token": "<ADMIN_RESET_TOKEN>", "email": "admin@admin", "senha": "Admin123" }

    Só funciona se existir a env ADMIN_RESET_TOKEN.
    """
    expected = (os.getenv("ADMIN_RESET_TOKEN") or "").strip()
    if not expected:
        return jsonify({"error": "ADMIN_RESET_TOKEN não configurado"}), 403

    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    email = (data.get("email") or "").strip().lower()
    senha = data.get("senha") or ""

    if token != expected:
        return jsonify({"error": "Token inválido"}), 403

    try:
        result = _upsert_admin_user(email=email, senha=senha)
        return jsonify({"status": "ok", **result}), 200
    except Exception as e:
        return jsonify({"error": "Falha ao resetar admin", "details": str(e)}), 400


# ============================================================
# ADMIN BOOTSTRAP
# Cria cliente + usuário admin + API Key (mostrada 1x)
# Regra:
#   - Se NÃO existir nenhum usuário ainda -> bootstrap liberado (primeira vez)
#   - Se já existir -> exige login e role admin
# ============================================================
@admin_bp.route("/bootstrap", methods=["POST"])
def bootstrap_cliente():
    if _exists_any_user():
        # Já tem sistema bootstrapado: só admin logado pode usar
        if not _is_logged_in():
            return jsonify({"error": "Login obrigatório"}), 401
        if not _is_admin():
            return jsonify({"error": "Apenas admin pode executar bootstrap"}), 403

    data = request.get_json(silent=True) or {}

    nome_cliente = data.get("nome_cliente")
    email = data.get("email")
    senha = data.get("senha")

    if not nome_cliente or not email or not senha:
        return jsonify({
            "error": "Campos obrigatórios: nome_cliente, email, senha"
        }), 400

    # IDs
    cliente_id = str(uuid.uuid4())
    usuario_id = str(uuid.uuid4())

    # Geração da API KEY (mostrada uma única vez)
    api_key_plain = secrets.token_urlsafe(32)
    api_key_hash = hashlib.sha256(api_key_plain.encode()).hexdigest()

    # Hash da senha (simples e seguro para v1)
    senha_hash = hashlib.sha256(senha.encode()).hexdigest()

    now = datetime.utcnow().isoformat()

    conn = get_db()
    cur = conn.cursor()

    try:
        # Cliente
        cur.execute("""
            INSERT INTO clientes (id, nome, api_key_hash, status, created_at)
            VALUES (?, ?, ?, 'active', ?)
        """, (cliente_id, nome_cliente, api_key_hash, now))

        # Usuário admin
        cur.execute("""
            INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at)
            VALUES (?, ?, ?, ?, 'admin', 'active', ?)
        """, (usuario_id, email, senha_hash, cliente_id, now))

        conn.commit()

    except Exception as e:
        conn.rollback()
        return jsonify({
            "error": "Falha ao criar cliente",
            "details": str(e)
        }), 500

    finally:
        conn.close()

    return jsonify({
        "cliente_id": cliente_id,
        "usuario_admin": email,
        "api_key": api_key_plain,
        "warning": "Guarde esta API Key. Ela NÃO será exibida novamente."
    }), 201
