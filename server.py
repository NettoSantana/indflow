from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta

# ============================================================
# BLUEPRINTS
# ============================================================
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp
from modules.utilidades.routes import utilidades_bp

app = Flask(__name__)

# ============================================================
# ====================== PRODUÇÃO =============================
# ============================================================

machine_data = {}

def get_machine(machine_id: str):
    if machine_id not in machine_data:
        machine_data[machine_id] = {
            "nome": machine_id.upper(),
            "status": "DESCONHECIDO",

            # Configuração
            "meta_turno": 0,
            "turno_inicio": None,
            "turno_fim": None,
            "rampa_percentual": 0,

            # ESP (contador absoluto)
            "esp_absoluto": 0,

            # Baseline diário (backend)
            "baseline_diario": 0,

            # Produção calculada
            "producao_turno": 0,
            "producao_turno_anterior": 0,

            # Controle por hora
            "horas_turno": [],
            "meta_por_hora": [],
            "producao_hora": 0,
            "percentual_hora": 0,
            "ultima_hora": None,

            # Dashboard
            "percentual_turno": 0,

            # Controle de dia
            "ultimo_dia": datetime.now().date()
        }

    return machine_data[machine_id]

# ============================================================
# RESET LÓGICO DO DIA (BACKEND)
# ============================================================
def reset_contexto(m):
    m["baseline_diario"] = m["esp_absoluto"]

    m["producao_turno"] = 0
    m["producao_turno_anterior"] = 0
    m["producao_hora"] = 0

    m["percentual_hora"] = 0
    m["percentual_turno"] = 0

    m["ultima_hora"] = None
    m["ultimo_dia"] = datetime.now().date()

# ============================================================
# GERAR TABELA DE HORAS DO TURNO
# ============================================================
def gerar_tabela_horas(machine_id: str):
    m = get_machine(machine_id)

    inicio = datetime.strptime(m["turno_inicio"], "%H:%M")
    fim = datetime.strptime(m["turno_fim"], "%H:%M")

    if fim <= inicio:
        fim += timedelta(days=1)

    duracao = int((fim - inicio).total_seconds() // 3600)

    meta_total = m["meta_turno"]
    rampa = m["rampa_percentual"]

    horas = []
    metas = []

    meta_base = meta_total / duracao
    meta_rampa = meta_base * (rampa / 100)
    meta_restante = meta_total - meta_rampa
    meta_restante_por_hora = meta_restante / (duracao - 1)

    for i in range(duracao):
        h0 = inicio + timedelta(hours=i)
        h1 = h0 + timedelta(hours=1)

        horas.append(f"{h0.strftime('%H:%M')} - {h1.strftime('%H:%M')}")
        metas.append(
            round(meta_rampa) if i == 0 else round(meta_restante_por_hora)
        )

    m["horas_turno"] = horas
    m["meta_por_hora"] = metas

# ============================================================
# CONFIGURAÇÃO DA MÁQUINA
# ============================================================
@app.route("/machine/config", methods=["POST"])
def config_machine():
    data = request.json
    machine_id = data.get("machine_id", "maquina01")
    m = get_machine(machine_id)

    m["meta_turno"] = int(data["meta_turno"])
    m["turno_inicio"] = data["inicio"]
    m["turno_fim"] = data["fim"]
    m["rampa_percentual"] = int(data["rampa"])

    gerar_tabela_horas(machine_id)

    return jsonify({"message": "Configuração salva."})

# ============================================================
# UPDATE VINDO DO ESP32
# ============================================================
@app.route("/machine/update", methods=["POST"])
def update_machine():
    try:
        data = request.get_json()
        machine_id = data.get("machine_id", "maquina01")
        m = get_machine(machine_id)

        # Reset automático por virada de dia
        if m["ultimo_dia"] != datetime.now().date():
            reset_contexto(m)

        m["nome"] = data.get("nome", m["nome"])
        m["status"] = data.get("status", "DESCONHECIDO")

        # Contador absoluto do ESP
        m["esp_absoluto"] = int(data["producao_turno"])

        # Produção do dia (absoluto - baseline)
        producao_atual = m["esp_absoluto"] - m["baseline_diario"]
        if producao_atual < 0:
            producao_atual = 0

        m["producao_turno"] = producao_atual

        if m["meta_turno"] > 0:
            m["percentual_turno"] = round(
                (producao_atual / m["meta_turno"]) * 100
            )

        # ======================
        # CONTROLE POR HORA
        # ======================
        hora_atual = datetime.now().strftime("%H:%M")
        hora_dt = datetime.strptime(hora_atual, "%H:%M")

        faixa_idx = None
        meta_hora = 0

        for idx, faixa in enumerate(m["horas_turno"]):
            inicio, fim = faixa.split(" - ")
            h0 = datetime.strptime(inicio, "%H:%M")
            h1 = datetime.strptime(fim, "%H:%M")

            if h1 <= h0:
                h1 += timedelta(days=1)

            if h0 <= hora_dt < h1:
                faixa_idx = idx
                meta_hora = m["meta_por_hora"][idx]
                break

        if m["ultima_hora"] != faixa_idx:
            m["producao_turno_anterior"] = producao_atual
            m["producao_hora"] = 0
            m["ultima_hora"] = faixa_idx

        diff = producao_atual - m["producao_turno_anterior"]
        m["producao_hora"] = max(diff, 0)

        if meta_hora > 0:
            m["percentual_hora"] = round(
                (m["producao_hora"] / meta_hora) * 100
            )
        else:
            m["percentual_hora"] = 0

        return jsonify({"message": "OK"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============================================================
# STATUS PARA DASHBOARD
# ============================================================
@app.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = request.args.get("machine_id", "maquina01")
    return jsonify(get_machine(machine_id))

# ============================================================
# RESET MANUAL DO DIA (ADMIN)
# ============================================================
@app.route("/admin/reset-dia", methods=["POST"])
def reset_dia():
    for m in machine_data.values():
        reset_contexto(m)
    return jsonify({"message": "Reset diário executado com sucesso."})

# ============================================================
# REGISTRO DE BLUEPRINTS
# ============================================================
app.register_blueprint(producao_bp, url_prefix="/producao")
app.register_blueprint(manutencao_bp, url_prefix="/manutencao")
app.register_blueprint(ativos_bp, url_prefix="/ativos")
app.register_blueprint(admin_bp, url_prefix="/admin")
app.register_blueprint(api_bp, url_prefix="/api")
app.register_blueprint(devices_bp, url_prefix="/devices")
app.register_blueprint(utilidades_bp, url_prefix="/utilidades")

# ============================================================
# HOME
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
