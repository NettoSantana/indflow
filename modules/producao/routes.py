from flask import Blueprint, render_template

producao_bp = Blueprint("producao", __name__, template_folder="templates")

# ===============================
# PÁGINA PRINCIPAL DA PRODUÇÃO
# ===============================
@producao_bp.route("/")
def home():
    return render_template("producao_home.html")


# ===============================
# PÁGINA DE CONFIGURAÇÃO DA MÁQUINA
# ===============================
@producao_bp.route("/config/<machine_id>")
def config_machine(machine_id):
    return render_template("config_maquina.html", machine_id=machine_id)
