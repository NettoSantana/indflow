from flask import Flask, render_template, request, jsonify
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp

app = Flask(__name__)

# ============================================================
# ARMAZENAMENTO EM MEMÓRIA DOS DADOS DAS MÁQUINAS
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
# ROTA PARA O ESP32 ENVIAR DADOS (HTTP POST)
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

        # Evita divisão por zero
        if meta_turno and meta_turno > 0:
            percentual = round((producao_turno / meta_turno) * 100)
        else:
            percentual = 0

        # Atualiza dados
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
# ROTA PARA O DASHBOARD CONSULTAR OS DADOS DA MÁQUINA
# ============================================================

@app.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = request.args.get("id", "maquina01")
    data = machine_data.get(machine_id)

    if not data:
        return jsonify({"error": "Máquina não encontrada"}), 404

    return jsonify(data)

# ============================================================
# BLUEPRINTS E ROTA PRINCIPAL
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


if __name__ == "__main__":
    app.run(debug=True)
