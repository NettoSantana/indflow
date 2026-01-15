from flask import Blueprint, render_template, request, jsonify
from datetime import datetime
import uuid
import secrets
import hashlib

from modules.db_indflow import get_db

admin_bp = Blueprint("admin", __name__, template_folder="templates")


@admin_bp.route("/")
def home():
    return render_template("admin_home.html")


# ============================================================
# ADMIN BOOTSTRAP
# Cria cliente + usuário admin + API Key (mostrada 1x)
# ============================================================
@admin_bp.route("/bootstrap", methods=["POST"])
def bootstrap_cliente():
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
