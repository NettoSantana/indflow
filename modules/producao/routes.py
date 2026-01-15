from flask import Blueprint, render_template, redirect, request, jsonify
from datetime import datetime, timedelta

# =====================================================
# AUTH
# =====================================================
from modules.admin.routes import login_required

# =====================================================
# BLUEPRINT
# =====================================================
producao_bp = Blueprint("producao", __name__, template_folder="templates")

# =====================================================
# CONTEXTO EM MEMÓRIA (MESMO PADRÃO DO SERVER)
# =====================================================
machine_data = {}

def get_machine(machine_id: str):
    if machine_id not in machine_data:
        machine_data[machine_id] = {
            "machine_id": machine_id,
            "meta_turno": 0,
            "hora_inicio": None,
            "hora_fim": None,
            "rampa_percentual": 0,
            "horas_turno": [],
            "meta_por_hora": []
        }
    return machine_data[machine_id]

# =====================================================
# REDIRECIONAR /producao PARA /
# =====================================================
@producao_bp.route("/")
@login_required
def home():
    return redirect("/")

# =====================================================
# PÁGINA DE CONFIGURAÇÃO
# =====================================================
@producao_bp.route("/config/<machine_id>")
@login_required
def config_machine(machine_id):
    return render_template("config_maquina.html", machine_id=machine_id)

# =====================================================
# SALVAR CONFIGURAÇÃO DA MÁQUINA
# =====================================================
@producao_bp.route("/config/<machine_id>", methods=["POST"])
@login_required
def salvar_config(machine_id):

    data = request.get_json()

    meta_turno = int(data.get("meta_turno", 0))
    hora_inicio = data.get("hora_inicio")  # "08:00"
    hora_fim = data.get("hora_fim")        # "18:00"
    rampa = int(data.get("rampa_percentual", 0))

    if meta_turno <= 0 or not hora_inicio or not hora_fim:
        return jsonify({"error": "Dados inválidos"}), 400

    # -------------------------------------------------
    # CÁLCULO DE HORAS DO TURNO
    # -------------------------------------------------
    fmt = "%H:%M"
    inicio = datetime.strptime(hora_inicio, fmt)
    fim = datetime.strptime(hora_fim, fmt)

    if fim <= inicio:
        return jsonify({"error": "Hora fim deve ser maior que início"}), 400

    horas_totais = int((fim - inicio).total_seconds() / 3600)

    if horas_totais <= 0:
        return jsonify({"error": "Turno inválido"}), 400

    # -------------------------------------------------
    # CÁLCULO DE METAS
    # -------------------------------------------------
    meta_base = meta_turno / horas_totais

    horas_turno = []
    meta_por_hora = []

    hora_atual = inicio

    for i in range(horas_totais):
        horas_turno.append(hora_atual.strftime("%H:%M"))

        if i == 0 and rampa > 0:
            meta_hora = round(meta_base * (rampa / 100))
        else:
            meta_hora = round(meta_base)

        meta_por_hora.append(meta_hora)
        hora_atual += timedelta(hours=1)

    # -------------------------------------------------
    # SALVA NO CONTEXTO
    # -------------------------------------------------
    m = get_machine(machine_id)
    m["meta_turno"] = meta_turno
    m["hora_inicio"] = hora_inicio
    m["hora_fim"] = hora_fim
    m["rampa_percentual"] = rampa
    m["horas_turno"] = horas_turno
    m["meta_por_hora"] = meta_por_hora

    return jsonify({
        "status": "ok",
        "machine_id": machine_id,
        "horas_turno": horas_turno,
        "meta_por_hora": meta_por_hora
    })
