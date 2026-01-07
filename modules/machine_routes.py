from flask import Blueprint, request, jsonify
from datetime import datetime, time, timedelta

from modules.db_indflow import get_db
from modules.machine_state import get_machine

machine_bp = Blueprint("machine_bp", __name__)

# ============================================================
# UNIDADES (simples e travado)
# ============================================================
UNIDADES_VALIDAS = {"pcs", "m", "m2"}

def _norm_u(v):
    if v is None:
        return None
    v = str(v).strip().lower()
    if v == "" or v == "none":
        return None
    return v if v in UNIDADES_VALIDAS else None

def _aplicar_unidades(m, u1, u2):
    u1 = _norm_u(u1)
    u2 = _norm_u(u2)

    # não permitir duplicado
    if u1 and u2 and u1 == u2:
        u2 = None

    m["unidade_1"] = u1
    m["unidade_2"] = u2

# ============================================================
# NOVO: FUNÇÕES DA HORA (para rodar Meta/Produção/Percentual da hora)
# ============================================================
def _get_turno_inicio_dt(m, agora):
    """
    Retorna datetime do início do turno (hoje ou ontem se atravessou meia-noite).
    """
    inicio_str = m.get("turno_inicio")
    if not inicio_str:
        return None

    inicio_dt = datetime.strptime(inicio_str, "%H:%M")
    inicio_dt = inicio_dt.replace(year=agora.year, month=agora.month, day=agora.day)

    # turno atravessou meia-noite (agora < inicio)
    if agora < inicio_dt:
        inicio_dt -= timedelta(days=1)

    return inicio_dt

def _calcular_ultima_hora_idx(m):
    """
    Calcula índice da hora atual do turno (0..n-1) baseado no turno_inicio e horas_turno.
    """
    horas = m.get("horas_turno") or []
    if not horas:
        return None

    agora = datetime.now()
    inicio_dt = _get_turno_inicio_dt(m, agora)
    if not inicio_dt:
        return None

    diff_h = int((agora - inicio_dt).total_seconds() // 3600)
    if diff_h < 0:
        diff_h = 0

    # limita ao tamanho do turno
    if diff_h >= len(horas):
        diff_h = len(horas) - 1

    return diff_h

def _atualizar_producao_hora(m):
    """
    Atualiza:
    - ultima_hora (idx)
    - baseline_hora (abs)
    - producao_hora
    - percentual_hora
    """
    idx = _calcular_ultima_hora_idx(m)

    # se não tem turno configurado
    if idx is None:
        m["ultima_hora"] = None
        m["producao_hora"] = 0
        m["percentual_hora"] = 0
        return

    # se mudou a hora, zera a hora e reseta baseline
    if m.get("ultima_hora") is None or m.get("ultima_hora") != idx:
        m["ultima_hora"] = idx
        # baseline da hora deve ser ABSOLUTO (esp_absoluto) no início da hora
        m["baseline_hora"] = int(m.get("esp_absoluto", 0) or 0)
        m["producao_hora"] = 0
        m["percentual_hora"] = 0
        return

    # mesma hora: calcula produção da hora por diferença do absoluto
    esp_abs = int(m.get("esp_absoluto", 0) or 0)
    base_h = int(m.get("baseline_hora", esp_abs) or esp_abs)

    prod_h = esp_abs - base_h
    if prod_h < 0:
        prod_h = 0

    m["producao_hora"] = int(prod_h)

    # percentual da hora baseado na meta da hora (pcs)
    meta_h = 0
    try:
        meta_h = (m.get("meta_por_hora") or [])[idx]
    except Exception:
        meta_h = 0

    if meta_h and meta_h > 0:
        m["percentual_hora"] = round((m["producao_hora"] / meta_h) * 100)
    else:
        m["percentual_hira"] = 0  # (mantém sua estrutura, mas não usamos)
        m["percentual_hora"] = 0

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

    # NOVO: reset baseline da hora junto
    m["baseline_hora"] = m["esp_absoluto"]

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

    # NOVO: unidade (até 2)
    _aplicar_unidades(m, data.get("unidade_1"), data.get("unidade_2"))

    # NOVO: conversão (1 pcs = X metros)
    try:
        if "conv_m_por_pcs" in data and data.get("conv_m_por_pcs") not in (None, "", "none"):
            conv = float(data.get("conv_m_por_pcs"))
            if conv > 0:
                m["conv_m_por_pcs"] = conv
    except Exception:
        pass

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

    # NOVO: ao configurar, define baseline da hora e hora atual
    m["baseline_hora"] = int(m.get("esp_absoluto", 0) or 0)
    m["ultima_hora"] = _calcular_ultima_hora_idx(m)
    m["producao_hora"] = 0
    m["percentual_hora"] = 0

    return jsonify({
        "status": "configurado",
        "meta_por_hora": m["meta_por_hora"],
        "unidade_1": m.get("unidade_1"),
        "unidade_2": m.get("unidade_2"),
        "conv_m_por_pcs": m.get("conv_m_por_pcs")
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

    # NOVO: atualiza produção/percentual da hora a cada update do ESP
    _atualizar_producao_hora(m)

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
# + DERIVADOS EM ML (via conv_m_por_pcs)
# ============================================================
@machine_bp.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = request.args.get("machine_id", "maquina01")
    m = get_machine(machine_id)

    # NOVO: garante que hora esteja atualizada mesmo sem update do ESP
    _atualizar_producao_hora(m)

    # ===== tempo médio =====
    try:
        produzido = int(m.get("producao_turno", 0) or 0)
        inicio_str = m.get("turno_inicio")

        if produzido > 0 and inicio_str:
            agora = datetime.now()

            inicio_dt = datetime.strptime(inicio_str, "%H:%M")
            inicio_dt = inicio_dt.replace(year=agora.year, month=agora.month, day=agora.day)

            # turno atravessou meia-noite
            if agora < inicio_dt:
                inicio_dt -= timedelta(days=1)

            minutos = (agora - inicio_dt).total_seconds() / 60
            minutos = max(minutos, 1)

            m["tempo_medio_min_por_peca"] = round(minutos / produzido, 2)
        else:
            m["tempo_medio_min_por_peca"] = None
    except Exception:
        m["tempo_medio_min_por_peca"] = None

    # ===== derivados (pcs -> ml) =====
    try:
        conv = float(m.get("conv_m_por_pcs", 1.0) or 1.0)
        if conv <= 0:
            conv = 1.0
    except Exception:
        conv = 1.0

    m["conv_m_por_pcs"] = conv

    # turno
    m["meta_turno_ml"] = round((m.get("meta_turno", 0) or 0) * conv, 2)
    m["producao_turno_ml"] = round((m.get("producao_turno", 0) or 0) * conv, 2)

    # hora (meta_hora_pcs depende de ultima_hora)
    meta_hora_pcs = 0
    try:
        idx = m.get("ultima_hora")
        if isinstance(idx, int) and idx >= 0:
            meta_hora_pcs = (m.get("meta_por_hora") or [])[idx]
    except Exception:
        meta_hora_pcs = 0

    m["meta_hora_pcs"] = int(meta_hora_pcs or 0)
    m["meta_hora_ml"] = round(m["meta_hora_pcs"] * conv, 2)
    m["producao_hora_ml"] = round((m.get("producao_hora", 0) or 0) * conv, 2)

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
