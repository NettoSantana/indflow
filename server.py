from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp

app = Flask(__name__)

# ============================================================
# ESTRUTURA PRINCIPAL DA MÁQUINA
# ============================================================

machine_data = {
    "maquina01": {
        "nome": "MAQUINA 01",
        "status": "DESCONHECIDO",
        "meta_turno": 0,
        "producao_turno": 0,
        "percentual_turno": 0,

        # NOVOS CAMPOS
        "turno_inicio": None,        # "06:00"
        "turno_fim": None,           # "16:00"
        "rampa_percentual": 0,       # Ex: 50 (%)
        "horas_turno": [],           # Lista com faixas horárias
        "meta_por_hora": [],         # Lista de metas hora a hora
        "producao_hora": 0,          # Reinicia a cada hora
        "percentual_hora": 0,
        "ultima_hora": None          # Controle de troca de hora
    }
}


# ============================================================
# FUNÇÃO GERAR ESTRUTURA HORA A HORA (USADO NA CONFIGURAÇÃO)
# ============================================================

def gerar_tabela_horas(machine_id):
    m = machine_data[machine_id]

    inicio = datetime.strptime(m["turno_inicio"], "%H:%M")
    fim = datetime.strptime(m["turno_fim"], "%H:%M")
    meta_total = m["meta_turno"]
    rampa = m["rampa_percentual"]

    horas = []
    metas = []

    # Suporte a turno passando da meia-noite
    if fim <= inicio:
        fim = fim + timedelta(days=1)

    duracao = int((fim - inicio).total_seconds() // 3600)

    m["horas_turno"] = []
    m["meta_por_hora"] = []

    # META POR HORA BASE
    meta_base = meta_total / duracao
    meta_rampa = meta_base * (rampa / 100)
    meta_restante = meta_total - meta_rampa
    meta_restante_por_hora = meta_restante / (duracao - 1)

    for i in range(duracao):
        hora_i = inicio + timedelta(hours=i)
        hora_f = hora_i + timedelta(hours=1)

        faixa = f"{hora_i.strftime('%H:%M')} - {hora_f.strftime('%H:%M')}"
        horas.append(faixa)

        if i == 0:
            metas.append(round(meta_rampa))
        else:
            metas.append(round(meta_restante_por_hora))

    m["horas_turno"] = horas
    m["meta_por_hora"] = metas


# ============================================================
# ROTA DE CONFIGURAÇÃO (SALVA TURNOS + RAMPA + META)
# ============================================================

@app.route("/machine/config", methods=["POST"])
def config_machine():
    data = request.json
    m = machine_data["maquina01"]

    m["meta_turno"] = int(data["meta_turno"])
    m["turno_inicio"] = data["inicio"]
    m["turno_fim"] = data["fim"]
    m["rampa_percentual"] = int(data["rampa"])

    gerar_tabela_horas("maquina01")

    return jsonify({"message": "Configuração salva com sucesso"})


# ============================================================
# ROTA PARA O ESP32 ENVIAR PRODUÇÃO
# ============================================================

@app.route("/machine/update", methods=["POST"])
def update_machine():
    try:
        data = request.get_json()
        m = machine_data["maquina01"]

        m["nome"] = data.get("nome")
        m["status"] = data.get("status", "DESCONHECIDO")
        m["producao_turno"] = int(data["producao_turno"])

        # Percentual do turno (não zera)
        if m["meta_turno"] > 0:
            m["percentual_turno"] = round((m["producao_turno"] / m["meta_turno"]) * 100)
        else:
            m["percentual_turno"] = 0

        # ======================================================
        # CÁLCULO DA META DA HORA ATUAL
        # ======================================================

        agora = datetime.now().strftime("%H:%M")
        hora_atual = datetime.strptime(agora, "%H:%M")

        horas = m["horas_turno"]
        metas = m["meta_por_hora"]

        meta_hora = 0
        faixa_atual = None

        for idx, faixa in enumerate(horas):
            inicio, fim = faixa.split(" - ")
            h0 = datetime.strptime(inicio, "%H:%M")
            h1 = datetime.strptime(fim, "%H:%M")

            if h1 <= h0:
                h1 += timedelta(days=1)

            if h0 <= hora_atual < h1:
                faixa_atual = idx
                meta_hora = metas[idx]
                break

        # ======================================================
        # PRODUÇÃO POR HORA (ZERA A CADA MUDANÇA DE HORA)
        # ======================================================

        if m["ultima_hora"] != faixa_atual:
            m["producao_hora"] = 0
            m["ultima_hora"] = faixa_atual

        # Produção da hora = incremento do turno desde último update
        # sem diferença acumulada de horas anteriores
        m["producao_hora"] += 1  # 1 pulso por update (ajustável depois)

        # Percentual da hora
        if meta_hora > 0:
            m["percentual_hora"] = round((m["producao_hora"] / meta_hora) * 100)
        else:
            m["percentual_hora"] = 0

        return jsonify({"message": "OK"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# DASHBOARD CONSULTA DADOS
# ============================================================

@app.route("/machine/status", methods=["GET"])
def machine_status():
    return jsonify(machine_data["maquina01"])


# ============================================================
# BLUEPRINTS
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
