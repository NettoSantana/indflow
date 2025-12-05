from flask import Flask, render_template
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp

app = Flask(__name__)

# Registro dos módulos (blueprints)
app.register_blueprint(producao_bp, url_prefix="/producao")
app.register_blueprint(manutencao_bp, url_prefix="/manutencao")
app.register_blueprint(ativos_bp, url_prefix="/ativos")
app.register_blueprint(admin_bp, url_prefix="/admin")
app.register_blueprint(api_bp, url_prefix="/api")
app.register_blueprint(devices_bp, url_prefix="/devices")

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
