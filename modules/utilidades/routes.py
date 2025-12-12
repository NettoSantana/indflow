from flask import Blueprint, render_template, jsonify, request
from datetime import datetime
from server import utilidades_data  # vamos criar essa estrutura no server.py

utilidades_bp = Blueprint("utilidades", __name__, template_folder="templates")


# ===============================================
# HOME UTILIDADES
# ===============================================
@utilidades_bp.route("/")
def utilidades_home():
    return render_template("utilidades_home.html")


# ===============================================
# STATUS DE UMA UTILIDADE
# ===============================================
@utilidades_bp.route("/status", methods=["GET"])
def utilidades_status():
    machine_id = request.args.get("machine_id")
    if not machine_id:
        return jsonify({"error": "machine_id não informado"}), 400

    if machine_id not in utilidades_data:
        return jsonify({"error": "equipamento não encontrado"}), 404

    return jsonify(utilidades_data[machine_id])


# ===============================================
# CONFIGURAÇÃO DA UTILIDADE
# ===============================================
@utilidades_bp.route("/config/<machine_id>")
def utilidades_config(machine_id):
    return render_template("utilidades_config.html", machine_id=machine_id)


# ===============================================
# ATUALIZAÇÃO VIA ESP32
# ===============================================
@utilidades_bp.route("/update", methods=["POST"])
def utilidades_update():
    try:
        data = request.get_json()
        machine_id = data.get("machine_id")

        if machine_id not in utilidades_data:
            return jsonify({"error": "machine_id inválido"}), 400

        util = utilidades_data[machine_id]

        util["ligado"] = int(data.get("ligado", 0))
        util["falha"] = int(data.get("falha", 0))
        util["horas_vida"] = float(data.get("horas_vida", util["horas_vida"]))
        util["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")

        return jsonify({"message": "OK"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
