# PATH: server.py
# LAST_RECODE: 2026-02-04 11:45 America/Bahia
# MOTIVO: Criar endpoint admin temporario /admin/db-check para inspecionar o SQLite real (/data/indflow.db) no Railway sem acesso a Shell.

import os
import logging
import sqlite3
from flask import Flask, render_template, request, jsonify

# ============================================================
# BLUEPRINTS (já existentes)
# ============================================================
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp
from modules.utilidades.routes import utilidades_bp

# ============================================================
# NOVO: CLIENTES (TENANT)
# ============================================================
from modules.clientes.routes import clientes_bp

# ============================================================
# NOVOS MÓDULOS (extraídos do server)
# ============================================================
from modules.db_indflow import init_db
from modules.machine_routes import machine_bp

# ============================================================
# LOGGING (mínimo, para Railway)
# ============================================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("indflow")

app = Flask(__name__)

# ============================================================
# SESSÃO / LOGIN (base)
# ============================================================
# Necessário para session/cookies (login web).
# Configure a env var FLASK_SECRET_KEY (string longa e secreta).
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "dev-inseguro-trocar")

# ============================================================
# BANCO SQLITE
# ============================================================
def _log_db_context():
    db_env = os.getenv("INDFLOW_DB_PATH")
    cwd = os.getcwd()
    log.info("startup: CWD=%s", cwd)
    log.info("startup: INDFLOW_DB_PATH=%s", db_env)

_log_db_context()

try:
    log.info("startup: init_db() begin")
    init_db()
    log.info("startup: init_db() ok")
except Exception:
    log.exception("startup: init_db() failed")
    raise

# ============================================================
# LOG REQUESTS (mínimo)
# ============================================================
@app.after_request
def _log_request(response):
    try:
        xfwd = request.headers.get("X-Forwarded-For", "")
        addr = xfwd.split(",")[0].strip() if xfwd else (request.remote_addr or "")
        log.info("http: %s %s %s ip=%s", request.method, request.path, response.status_code, addr)
    except Exception:
        # Nunca quebrar request por causa de log.
        pass
    return response

# ============================================================
# ADMIN: DB CHECK (TEMPORÁRIO)
# ============================================================
def _admin_token_ok() -> bool:
    # Aceita token via query (?token=) ou header X-Admin-Token.
    token_in = (request.args.get("token") or "").strip()
    if not token_in:
        token_in = (request.headers.get("X-Admin-Token") or "").strip()

    # Tokens aceitos (Railway Variables):
    # - INDFLOW_ADMIN_TOKEN (preferencial)
    # - ADMIN_RESET_TOKEN (fallback, já existe no projeto)
    expected = (os.getenv("INDFLOW_ADMIN_TOKEN") or "").strip()
    if not expected:
        expected = (os.getenv("ADMIN_RESET_TOKEN") or "").strip()

    if not expected:
        # Sem token configurado: não expor endpoint.
        return False

    return token_in == expected


@app.get("/admin/db-check")
def admin_db_check():
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    db_path = os.getenv("INDFLOW_DB_PATH", "/data/indflow.db")
    out = {
        "ok": True,
        "db_path": db_path,
        "tables": [],
        "counts": {},
        "samples": {},
        "errors": [],
    }

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        tables = [r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()]
        out["tables"] = tables

        def safe_count(tbl: str):
            try:
                n = cur.execute(f"SELECT COUNT(1) FROM {tbl}").fetchone()[0]
                out["counts"][tbl] = int(n)
            except Exception as e:
                out["counts"][tbl] = None
                out["errors"].append(f"count {tbl}: {e}")

        for t in [
            "producao_diaria",
            "producao_horaria",
            "machine_config",
            "devices",
            "usuarios",
            "clientes",
        ]:
            if t in tables:
                safe_count(t)

        # Samples maquina02 (se existirem as colunas esperadas)
        if "producao_horaria" in tables:
            try:
                rows = cur.execute(
                    "SELECT data_ref, machine_id, hora_idx, produzido, meta, updated_at "
                    "FROM producao_horaria "
                    "WHERE machine_id='maquina02' "
                    "ORDER BY data_ref DESC, hora_idx DESC "
                    "LIMIT 20"
                ).fetchall()
                out["samples"]["producao_horaria_maquina02"] = rows
            except Exception as e:
                out["errors"].append(f"sample producao_horaria_maquina02: {e}")

        if "producao_diaria" in tables:
            try:
                rows = cur.execute(
                    "SELECT data, machine_id, produzido, meta, percentual "
                    "FROM producao_diaria "
                    "WHERE machine_id='maquina02' "
                    "ORDER BY data DESC "
                    "LIMIT 20"
                ).fetchall()
                out["samples"]["producao_diaria_maquina02"] = rows
            except Exception as e:
                out["errors"].append(f"sample producao_diaria_maquina02: {e}")

        conn.close()

    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)

    return jsonify(out)

# ============================================================
# ROTAS PRINCIPAIS
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")

# ============================================================
# BLUEPRINTS
# ============================================================
app.register_blueprint(machine_bp)  # mantém /machine/*, /admin/reset-manual, /producao/historico

app.register_blueprint(producao_bp, url_prefix="/producao")
app.register_blueprint(manutencao_bp, url_prefix="/manutencao")
app.register_blueprint(ativos_bp, url_prefix="/ativos")
app.register_blueprint(admin_bp, url_prefix="/admin")
app.register_blueprint(clientes_bp, url_prefix="/clientes")
app.register_blueprint(api_bp, url_prefix="/api")
app.register_blueprint(devices_bp, url_prefix="/devices")
app.register_blueprint(utilidades_bp, url_prefix="/utilidades")
