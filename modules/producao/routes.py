from flask import Blueprint, render_template, redirect

producao_bp = Blueprint("producao", __name__, template_folder="templates")

# ===============================
# REDIRECIONAR /producao PARA /
# ===============================
@producao_bp.route("/")
def home():
    # Sempre envia para o dashboard principal
    return redirect("/")

# ===============================
# PÁGINA DE CONFIGURAÇÃO DA MÁQUINA
# ===============================
@producao_bp.route("/config/<machine_id>")
def config_machine(machine_id):
    return render_template("config_maquina.html", machine_id=machine_id)
