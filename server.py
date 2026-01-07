from flask import Flask, render_template

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
# NOVOS MÓDULOS (extraídos do server)
# ============================================================
from modules.db_indflow import init_db
from modules.machine_routes import machine_bp

app = Flask(__name__)

# ============================================================
# BANCO SQLITE
# ============================================================
init_db()

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
app.register_blueprint(api_bp, url_prefix="/api")
app.register_blueprint(devices_bp, url_prefix="/devices")
app.register_blueprint(utilidades_bp, url_prefix="/utilidades")
