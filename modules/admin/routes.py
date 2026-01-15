from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, render_template_string
from datetime import datetime
import uuid
import secrets
import hashlib
from functools import wraps

from modules.db_indflow import get_db

admin_bp = Blueprint("admin", __name__, template_folder="templates")

# ============================================================
# CREDENCIAIS PADRÃO (APENAS PRIMEIRO ACESSO)
# ============================================================
DEFAULT_ADMIN_EMAIL = "admin@admin"
DEFAULT_ADMIN_PASSWORD = "Admin123"

# ============================================================
# LOGIN TEMPLATE (PADRÃO INDFLOW)
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
    body {
      margin:0;
      background:#f8fafc;
      font-family: Arial, sans-serif;
    }
    .login-wrap {
      min-height:100vh;
      display:flex;
      align-items:center;
      justify-content:center;
    }
    .login-card {
      width:380px;
      background:#ffffff;
      border:1px solid #e5e7eb;
      border-radius:12px;
      padding:32px;
      box-shadow:0 10px 25px rgba(0,0,0,.06);
    }
    .logo {
      display:flex;
      justify-content:center;
      margin-bottom:18px;
    }
    .logo img {
      height:60px;
    }
    h1 {
      text-align:center;
      font-size:20px;
      margin-bottom:20px;
      color:#0f172a;
    }
    label {
      font-size:13px;
      color:#334155;
      display:block;
      margin-top:12px;
      margin-bottom:4px;
    }
    input {
      width:100%;
      padding:10px 12px;
      border-radius:8px;
      border:1px solid #cbd5e1;
      font-size:14px;
    }
    button {
      margin-top:20px;
      width:100%;
      padding:10px;
      border-radius:8px;
      border:none;
      background:#2563eb;
      color:white;
      font-weight:600;
      font-size:15px;
      cursor:pointer;
    }
    .err {
      margin-top:14px;
      font-size:13px;
      color:#dc2626;
      text-align:center;
    }
  </style>
</head>
<body>
  <div class="login-wrap">
    <div class="login-card">
      <div class="logo">
        <img src="/static/img/logo.png" alt="IndFlow">
      </div>
      <h1>IndFlow</h1>
      <form method="post">
        <label>Email</label>
        <input name="email" type="email" required>
        <label>Senha</label>
        <input name="senha" type="password" required>
        <button type="submit">Entrar</button>
        {% if error %}<div class="err">{{ error }}</div>{% endif %}
      </form>
    </div>
  </div>
</body>
</html>
"""

# ============================================================
# HELPERS
# ============================================================
def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()

def _exists_any_user() -> bool:
    conn = get_db()
    try:
        cur = conn.execute("SELECT 1 FROM usuarios LIMIT 1")
        return cur.fetchone() is not None
    finally:
        conn.close()

def _get_user_by_email(email: str):
    conn = get_db()
    try:
        cur = conn.execute("""
            SELECT id, email, senha_hash, cliente_id, role, status
            FROM usuarios WHERE email = ? LIMIT 1
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

def _auto_create_default_admin():
    conn = get_db()
    try:
        cliente_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        senha_hash = _sha256(DEFAULT_ADMIN_PASSWORD)

        conn.execute("""
            INSERT INTO clientes (id, nome, api_key_hash, status, created_at)
            VALUES (?, 'DEFAULT', 'INIT', 'active', ?)
        """, (cliente_id, now))

        conn.execute("""
            INSERT INTO usuarios (id, email, senha_hash, cliente_id, role, status, created_at)
            VALUES (?, ?, ?, ?, 'admin', 'active', ?)
        """, (user_id, DEFAULT_ADMIN_EMAIL, senha_hash, cliente_id, now))

        conn.commit()
    finally:
        conn.close()

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("admin.login"))
        return fn(*args, **kwargs)
    return wrapper

# ============================================================
# ROTAS
# ============================================================
@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template_string(LOGIN_FORM_HTML, error=None)

    email = (request.form.get("email") or "").strip().lower()
    senha = request.form.get("senha") or ""

    # AUTO BOOTSTRAP
    if not _exists_any_user():
        if email == DEFAULT_ADMIN_EMAIL and senha == DEFAULT_ADMIN_PASSWORD:
            _auto_create_default_admin()
        else:
            return render_template_string(LOGIN_FORM_HTML, error="Credenciais inválidas.")

    user = _get_user_by_email(email)
    if not user or _sha256(senha) != user["senha_hash"]:
        return render_template_string(LOGIN_FORM_HTML, error="Email ou senha inválidos.")

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
