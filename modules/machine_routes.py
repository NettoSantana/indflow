from flask import Blueprint, request, jsonify
from datetime import datetime, time, timedelta

from modules.db_indflow import get_db
from modules.machine_state import get_machine

machine_bp = Blueprint("machine_bp", __name__)

# ============================================================
# RESET + HISTÓRICO
# ============================================================
def reset_contexto(m, machine_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO producao_diaria (machine_id, data, produzido, meta, percentual)
        VALUES (?, ?, ?, ?, ?)
    """, (
        machine_id,
        m["ultimo_dia"].isoformat(),
        m["producao_turno"],
        m["meta_turno"],
        m["percentual_turno"]
    ))

    conn.commit()
    conn.close()

    m["baseline_diario"] = m["esp_absoluto"]
    m["producao_turno"] = 0
    m["producao_turno_anterior"] = 0
    m["producao_hora"] = 0
    m["percentual_hora"] = 0
    m["percentual_turno"] = 0
    m["tempo_medio_min_por_peca"] = None
    m["ultima_hora"] = None
    m["ultimo_dia"] = datetime.now().date()
    m["reset_executado_hoje"] = True

def verificar_reset_diario(m, machine_id):
    agora = datetime.now()
    horario_reset = time(23, 59)

    if agora.time() >= horario_reset and not m["reset_executado_hoje"]:
        reset_contexto(m, machine_id)

    if agora.date() != m["ultimo_dia"]:
        m["reset_executado_hoje"] = False

# ============================================================
# CONFIGURAÇÃO DA MÁQUINA
# ============================================================
@machine_bp.route("/machine/config", methods=["POST"])
def configurar_maquina():
    data = request.get_json()
    machine_id = data.get("machine_id", "maquina01")
    m = get_machine(machine_id)

    meta_turno = int(data["meta_turno"])
    rampa = int(data["rampa"])

    m["meta_turno"] = meta_turno
    m["turno_inicio"] = data["inicio"]
    m["turno_fim"] = data["fim"]
    m["rampa_percentual"] = rampa

    inicio = datetime.strptime(m["turno_inicio"], "%H:%M")
    fim = datetime.strptime(m["turno_fim"], "%H:%M")

    if fim <= inicio:
        fim += timedelta(days=1)

    horas = []
    atual = inicio
    while atual < fim:
        proxima = atual + timedelta(hours=1)
        horas.append(f"{atual.strftime('%H:%M')} - {proxima.strftime('%H:%M')}")
        atual = proxima

    m["horas_turno"] = horas

    qtd_horas = len(horas)
    if qtd_horas > 0:
        meta_base = meta_turno / qtd_horas

        meta_primeira = round(meta_base * (rampa / 100))
        restante = meta_turno - meta_primeira
        horas_restantes = qtd_horas - 1

        metas = [meta_primeira]

        if horas_restantes > 0:
            meta_restante_base = restante // horas_restantes
            sobra = restante % horas_restantes

            for i in range(horas_restantes):
                valor = meta_restante_base + (1 if i < sobra else 0)
                metas.append(valor)

        m["meta_por_hora"] = metas

    return jsonify({
        "status": "configurado",
        "meta_por_hora": m["meta_por_hora"]
    })

# ============================================================
# UPDATE ESP
# ============================================================
@machine_bp.route("/machine/update", methods=["POST"])
def update_machine():
    data = request.get_json()
    machine_id = data.get("machine_id", "maquina01")
    m = get_machine(machine_id)

    verificar_reset_diario(m, machine_id)

    m["status"] = data.get("status", "DESCONHECIDO")
    m["esp_absoluto"] = int(data["producao_turno"])

    producao_atual = max(m["esp_absoluto"] - m["baseline_diario"], 0)
    m["producao_turno"] = producao_atual

    if m["meta_turno"] > 0:
        m["percentual_turno"] = round((producao_atual / m["meta_turno"]) * 100)

    return jsonify({"message": "OK"})

# ============================================================
# RESET MANUAL
# ============================================================
@machine_bp.route("/admin/reset-manual", methods=["POST"])
def reset_manual():
    data = request.get_json()
    machine_id = data.get("machine_id", "maquina01")
    m = get_machine(machine_id)
    reset_contexto(m, machine_id)
    return jsonify({"status": "resetado"})

# ============================================================
# STATUS (tempo médio acumulado desde início do turno)
# ============================================================
@machine_bp.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = request.args.get("machine_id", "maquina01")
    m = get_machine(machine_id)

    try:
        produzido = int(m.get("producao_turno", 0) or 0)
        inicio_str = m.get("turno_inicio")

        if produzido > 0 and inicio_str:
            agora = datetime.now()

            inicio_dt = datetime.strptime(inicio_str, "%H:%M")
            inicio_dt = inicio_dt.replace(year=agora.year, month=agora.month, day=agora.day)

            if agora < inicio_dt:
                inicio_dt -= timedelta(days=1)

            minutos = (agora - inicio_dt).total_seconds() / 60
            minutos = max(minutos, 1)

            m["tempo_medio_min_por_peca"] = round(minutos / produzido, 2)
        else:
            m["tempo_medio_min_por_peca"] = None
    except Exception:
        m["tempo_medio_min_por_peca"] = None

    return jsonify(m)

# ============================================================
# HISTÓRICO
# ============================================================
@machine_bp.route("/producao/historico", methods=["GET"])
def historico_producao():
    machine_id = request.args.get("machine_id")
    inicio = request.args.get("inicio")
    fim = request.args.get("fim")

    query = """
        SELECT machine_id, data, produzido, meta, percentual
        FROM producao_diaria
        WHERE 1=1
    """
    params = []

    if machine_id:
        query += " AND machine_id = ?"
        params.append(machine_id)

    if inicio:
        query += " AND data >= ?"
        params.append(inicio)

    if fim:
        query += " AND data <= ?"
        params.append(fim)

    query += " ORDER BY data DESC"

    conn = get_db()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])
