from flask import Flask, render_template, request, jsonify
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp

import os
import json
from datetime import datetime, timedelta

app = Flask(__name__)

# ============================================================
# GARANTE A EXISTÊNCIA DA PASTA /data
# ============================================================
DATA_DIR = "data"
SETTINGS_FILE = f"{DATA_DIR}/machine_settings.json"

os.makedirs(DATA_DIR, exist_ok=True)

# ============================================================
# ARQUIVO DE CONFIGURAÇÃO INICIAL (MÚLTIPLAS MÁQUINAS)
# ============================================================
if not os.path.exists(SETTINGS_FILE):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "maquina01": {},
            "maquina02": {},
            "maquina03": {}
        }, f, indent=4)

# ============================================================
# FUNÇÕES DE LEITURA / ESCRITA JSON
# ============================================================
def load_settings():
    with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_settings(data):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

# ============================================================
# DADOS EM MEMÓRIA DO ESP32 (PRODUÇÃO DO TURNO)
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
# ROTA PARA O ESP32 ENVIAR DADOS
# ============================================================
@app.route("/machine/update", methods=["POST"])
def update_machine():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "JSON não enviado"}), 400

        machine_id = data.get("id")

        if machine_id not in machine_data:
            return jsonify({"error": "Máquina não cadastrada"}), 404

        nome = data.get("nome")
        status = data.get("status")
        meta_turno = data.get("meta_turno")
        producao_turno = data.get("producao_turno")

        if meta_turno and meta_turno > 0:
            percentual = round((producao_turno / meta_turno) * 100)
        else:
            percentual = 0

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
# ROTA PARA O DASHBOARD LER OS DADOS
# ============================================================
@app.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = request.args.get("id", "maquina01")
    data = machine_data.get(machine_id)

    if not data:
        return jsonify({"error": "Máquina não encontrada"}), 404

    return jsonify(data)

# ============================================================
# FUNÇÃO INDUSTRIAL — CÁLCULO DA META HORÁRIA
# ============================================================
def calcular_meta_horaria(turnos, rampa_percent):
    """
    turnos = [
      {"inicio": "06:00", "fim": "14:00", "meta": 500},
      {"inicio": "14:00", "fim": "22:00", "meta": 300},
      {"inicio": "22:00", "fim": "06:00", "meta": 200}
    ]
    """

    tabela = []
    primeiro_turno = True

    for idx, turno in enumerate(turnos):

        inicio = turno["inicio"]
        fim = turno["fim"]
        meta_total = int(turno["meta"])

        if meta_total <= 0:
            continue  # turno sem meta

        h_inicio = datetime.strptime(inicio, "%H:%M")
        h_fim = datetime.strptime(fim, "%H:%M")

        # turno que cruza a meia-noite
        if h_fim <= h_inicio:
            h_fim += timedelta(days=1)

        duracao_horas = (h_fim - h_inicio).total_seconds() / 3600

        # meta média por hora
        meta_media = meta_total / duracao_horas

        # rampa somente no PRIMEIRO turno do dia
        if primeiro_turno:
            meta_rampa = int(meta_media * (rampa_percent / 100))
            restante = meta_total - meta_rampa
            horas_restantes = duracao_horas - 1

            if horas_restantes > 0:
                meta_hora_restante = int(restante // horas_restantes)
            else:
                meta_hora_restante = 0

            primeiro_turno = False
        else:
            # turnos seguintes = sem rampa
            meta_rampa = None
            meta_hora_restante = int(meta_media)

        # GERA A TABELA DO TURNO
        hora_atual = h_inicio
        hora_final = h_fim

        tabela.append({"turno": f"Turno {idx+1}", "is_header": True})

        primeira_hora = True

        while hora_atual < hora_final:

            proxima = hora_atual + timedelta(hours=1)
            faixa_inicio = hora_atual.strftime("%H:%M")
            faixa_fim = proxima.strftime("%H:%M")

            if primeira_hora and meta_rampa is not None:
                meta_faixa = meta_rampa
                primeira_hora = False
            else:
                meta_faixa = meta_hora_restante

            tabela.append({
                "faixa": f"{faixa_inicio} — {faixa_fim}",
                "meta": meta_faixa
            })

            hora_atual = proxima

    return tabela

# ============================================================
# ROTA PRINCIPAL DA PÁGINA DA MÁQUINA
# ============================================================
@app.route("/maquina/<machine_id>")
def pagina_maquina(machine_id):
    settings = load_settings()
    conf = settings.get(machine_id, {})

    return render_template(
        "config_maquina.html",
        machine_id=machine_id,
        configuracao=conf,
        dados_machine=machine_data.get(machine_id, {})
    )

# ============================================================
# ROTA PARA CALCULAR TABELA HORÁRIA
# ============================================================
@app.route("/maquina/<machine_id>/calcular", methods=["POST"])
def calcular(machine_id):
    data = request.get_json()

    turnos = data.get("turnos", [])
    rampa = int(data.get("rampa", 0))

    tabela = calcular_meta_horaria(turnos, rampa)
    return jsonify({"tabela": tabela})

# ============================================================
# ROTA PARA SALVAR CONFIGURAÇÃO
# ============================================================
@app.route("/maquina/<machine_id>/salvar", methods=["POST"])
def salvar(machine_id):
    data = request.get_json()
    settings = load_settings()

    settings[machine_id] = data
    save_settings(settings)

    return jsonify({"message": "Configurações salvas com sucesso!"})

# ============================================================
# BLUEPRINTS E ROTA INICIAL
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
