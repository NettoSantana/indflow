from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, render_template_string
from datetime import datetime
import uuid
import secrets
import hashlib
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
  <style>
    body { font-family: Arial, sans-serif; background:#0b1220; color:#e5e7eb; margin:0; }
    .wrap { max-width: 420px; margin: 70px auto; padding: 22px; background:#111827; border:1px solid #1f2937; border-radius:12px; }
    h1 { font-size: 20px; margin:0 0 14px 0; }
    label { display:block; font-size: 13px; margin:10px 0 6px; color:#cbd5e1; }
    input { width:100%; padding:10px 12px; border-radius:10px; border:1px solid #334155; background:#0b1220; color:#e5e7eb; }
    button { width:100%; margin-top:14px; padding:10px 12px; border-radius:10px; border:0; background:#2563eb; color:white; font-weight:700; cursor:pointer; }
    .err { margin-top: 10px; color:#fca5a5; font-size: 13px; }
    .hint { margin-top: 10px; color:#94a3b8; font-size: 12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Login — IndFlow</h1>
    <form method="post">
      <label>Email</label>
      <input name="email" type="email" autocomplete="username" required />
      <label>Senha</label>
      <input name="senha" type="password" autocomplete="current-password" required />
      <button type="submit">Entrar</button>
      {% if error %}<div class="err">{{ error }}</div>{% endif %}
      <div class="hint">Se for o primeiro acesso, rode o bootstrap (uma única vez) para criar o admin.</div>
    </form>
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
        return render_template_string(LOGIN_FORM_HTML, error="Usuário inválido ou inativo.")

    if _sha256(senha) != (user.get("senha_hash") or ""):
        return render_template_string(LOGIN_FORM_HTML, error="Senha incorreta.")

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
