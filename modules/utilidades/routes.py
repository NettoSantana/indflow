from flask import Blueprint, render_template, request, jsonify
from datetime import datetime
from .data import utilidades_data

from modules.admin.routes import login_required

utilidades_bp = Blueprint("utilidades", __name__, template_folder="templates")

# HOME — Lista de equipamentos
@utilidades_bp.route("/")
@login_required
def home():
    return render_template("utilidades_home.html")

# STATUS INDIVIDUAL
@utilidades_bp.route("/status", methods=["GET"])
def status_utilidade():
    machine_id = request.args.get("machine_id")

    if not machine_id or machine_id not in utilidades_data:
        return jsonify({"error": "Equipamento não encontrado"}), 404

    return jsonify(utilidades_data[machine_id])

# TELA DE CONFIGURAÇÃO
@utilidades_bp.route("/config/<machine_id>")
@login_required
def config(machine_id):
    return render_template("utilidades_config.html", machine_id=machine_id)

# RECEBER DADOS DO ESP32
@utilidades_bp.route("/update", methods=["POST"])
def update():
    data = request.get_json()

    machine_id = data.get("machine_id")
    if machine_id not in utilidades_data:
        return jsonify({"error": "machine_id inválido"}), 400

    util = utilidades_data[machine_id]

    util["ligado"] = int(data.get("ligado", util["ligado"]))
    util["falha"] = int(data.get("falha", util["falha"]))
    util["horas_vida"] = float(data.get("horas_vida", util["horas_vida"]))
    util["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")

    return jsonify({"message": "OK"})
