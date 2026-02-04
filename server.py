# PATH: server.py
# LAST_RECODE: 2026-02-04 11:32 America/Bahia
# MOTIVO: Adicionar logging minimo (requests + init_db + caminho do DB) para diagnosticar gravacao no Railway DEV.

import os
import logging
from flask import Flask, render_template, request

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
