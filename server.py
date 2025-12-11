from flask import Flask, render_template, request, jsonify
import os, json

# Blueprints existentes
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp

app = Flask(__name__)

# ============================================================
# ARMAZENAMENTO EM MEMÓRIA DOS DADOS DO ESP32
# ============================================================
machine_data = {
    "maquina01": {
        "nome": "MAQUINA 01",
        "status": "DESCONHECIDO",
        "meta_turno": 0,
        "producao_turno": 0,
        "percentual": 0
    }
}

# ============================================================
# ARQUIVO DE CONFIGURAÇÃO (PERSISTÊNCIA)
# ============================================================
DATA_DIR = "data"
SETTINGS_FILE = os.path.join(DATA_DIR, "machine_settings.json")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

if not os.path.exists(SETTINGS_FILE):
    with open(SETTINGS_FILE, "w") as f:
        json.dump({}, f)


def load_settings():
    with open(SETTINGS_FILE, "r") as f:
        return json.load(f)


def save_settings(data):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(data, f, indent=4)


# ============================================================
# ROTA RECEBIDA PELO ESP32
# ============================================================
@app.route("/machine/update", methods=["POST"])
def update_machine():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "JSON não enviado"}), 400

        machine_id = data.get("id")
        nome = data.get("nome")
        status = data.get("status")
        meta_turno = data.get("meta_turno")
        producao_turno = data.get("producao_turno")

        if machine_id != "maquina01":
            return jsonify({"error": "Máquina não cadastrada"}), 404

        # Calcula percentual
        percentual = 0
        if meta_turno and meta_turno > 0:
            percentual = round((producao_turno / meta_turno) * 100)

        machine_data[machine_id] = {
            "nome": nome,
            "status": status,
            "meta_turno": meta_turno,
            "producao_turno": producao_turno,
            "percentual": percentual
        }

        return jsonify({"message": "Dados atualizados com sucesso"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# CONSULTA DO DASHBOARD
# ============================================================
@app.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = request.args.get("id", "maquina01")
    data = machine_data.get(machine_id)

    if not data:
        return jsonify({"error": "Máquina não encontrada"}), 404

    return jsonify(data)



# ============================================================
# 🔥 TELA DE CONFIGURAÇÃO DA MÁQUINA
# ============================================================
@app.route("/producao/config/<machine_id>")
def config_page(machine_id):

    settings = load_settings()
    machine_settings = settings.get(machine_id, {})

    # Se ainda não existir nada salvo → valores padrão
    defaults = {
        "rampa": 50,
        "turno1": {"ini": "06:00", "fim": "16:00", "meta": 1000},
        "turno2": {"ini": "", "fim": "", "meta": 0},
        "turno3": {"ini": "", "fim": "", "meta": 0}
    }

    merged = {**defaults, **machine_settings}

    return render_template("config_maquina.html",
                           machine={"nome": machine_data[machine_id]["nome"]},
                           config=merged)


# ============================================================
# SALVAR CONFIGURAÇÕES
# ============================================================
@app.route("/producao/config/<machine_id>/salvar", methods=["POST"])
def salvar_config(machine_id):

    data = request.get_json()

    settings = load_settings()
    settings[machine_id] = data
    save_settings(settings)

    return jsonify({"status": "ok", "msg": "Configurações salvas"})


# ============================================================
# BLUEPRINTS E INDEX
# ============================================================
app.register_blueprint(producao_bp, url_prefix="/producao")
app.register_blueprint(manutencao_bp, url_prefix="/manutencao")
app.register_blueprint(ativos_bp, url_prefix="/ativos")
app.register_blueprint(admin_bp, url_prefix="/admin")
app.register_blueprint(api_bp, url_prefix="/api")
app.register_blueprint(devices_bp, url_prefix="/devices")


@app.route("/")
def index():
    return render_template("index.html")


# ============================================================
# EXECUÇÃO LOCAL
# ============================================================
if __name__ == "__main__":
    app.run(debug=True)
