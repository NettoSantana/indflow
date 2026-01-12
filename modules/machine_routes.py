# modules/machine_routes.py
from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta

from modules.db_indflow import get_db
from modules.machine_state import get_machine
from modules.machine_calc import (
    aplicar_unidades,
    salvar_conversao,
    atualizar_producao_hora,
    verificar_reset_diario,
    reset_contexto,
    calcular_ultima_hora_idx,
    calcular_tempo_medio,
    aplicar_derivados_ml,
    carregar_baseline_diario,
    now_bahia,
    dia_operacional_ref_str,
)

from modules.repos.machine_config_repo import upsert_machine_config
from modules.repos.refugo_repo import load_refugo_24, upsert_refugo

machine_bp = Blueprint("machine_bp", __name__)


def _norm_machine_id(v):
    v = (v or "").strip().lower()
    return v or "maquina01"


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


# ============================================================
# CONFIGURAÇÃO DA MÁQUINA
# ============================================================
@machine_bp.route("/machine/config", methods=["POST"])
def configurar_maquina():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    meta_turno = int(data["meta_turno"])
    rampa = int(data["rampa"])

    m["meta_turno"] = meta_turno
    m["turno_inicio"] = data["inicio"]
    m["turno_fim"] = data["fim"]
    m["rampa_percentual"] = rampa

    aplicar_unidades(m, data.get("unidade_1"), data.get("unidade_2"))
    salvar_conversao(m, data)

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
    metas = []
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

    m["baseline_hora"] = int(m.get("esp_absoluto", 0) or 0)
    m["ultima_hora"] = calcular_ultima_hora_idx(m)
    m["producao_hora"] = 0
    m["percentual_hora"] = 0

    try:
        upsert_machine_config(machine_id, m)
    except Exception:
        pass

    return jsonify({
        "status": "configurado",
        "machine_id": machine_id,
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
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    verificar_reset_diario(m, machine_id)

    m["status"] = data.get("status", "DESCONHECIDO")
    m["esp_absoluto"] = int(data["producao_turno"])

    carregar_baseline_diario(m, machine_id)

    producao_atual = max(
        int(m.get("esp_absoluto", 0) or 0) - int(m.get("baseline_diario", 0) or 0),
        0
    )
    m["producao_turno"] = producao_atual

    if int(m.get("meta_turno", 0) or 0) > 0:
        m["percentual_turno"] = round((producao_atual / m["meta_turno"]) * 100)
    else:
        m["percentual_turno"] = 0

    atualizar_producao_hora(m)

    return jsonify({"message": "OK", "machine_id": machine_id})


# ============================================================
# RESET MANUAL
# ============================================================
@machine_bp.route("/admin/reset-manual", methods=["POST"])
def reset_manual():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)
    reset_contexto(m, machine_id)
    return jsonify({"status": "resetado", "machine_id": machine_id})


# ============================================================
# REFUGO
# ============================================================
@machine_bp.route("/machine/refugo", methods=["POST"])
def salvar_refugo():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    agora = now_bahia()
    dia_atual = dia_operacional_ref_str(agora)

    dia_ref = (data.get("dia_ref") or "").strip() or dia_atual
    hora_dia = _safe_int(data.get("hora_dia"), -1)
    refugo = _safe_int(data.get("refugo"), 0)

    if hora_dia < 0 or hora_dia > 23:
        return jsonify({"ok": False, "error": "hora_dia inválida"}), 400

    if dia_ref > dia_atual:
        return jsonify({"ok": False, "error": "dia_ref futuro"}), 400

    if dia_ref == dia_atual and hora_dia >= agora.hour:
        return jsonify({"ok": False, "error": "hora futura"}), 400

    ok = upsert_refugo(
        machine_id=machine_id,
        dia_ref=dia_ref,
        hora_dia=hora_dia,
        refugo=refugo,
        updated_at_iso=agora.isoformat(),
    )

    if not ok:
        return jsonify({"ok": False}), 500

    return jsonify({"ok": True})


# ============================================================
# HISTÓRICO (JSON FINAL DO DIA)
# ============================================================
@machine_bp.route("/producao/historico", methods=["GET"])
def historico_producao():
    machine_id = request.args.get("machine_id")

    conn = get_db()
    cur = conn.cursor()

    query = """
        SELECT machine_id, data, produzido, meta, percentual
        FROM producao_diaria
        WHERE 1=1
    """
    params = []

    if machine_id:
        query += " AND machine_id = ?"
        params.append(_norm_machine_id(machine_id))

    query += " ORDER BY data DESC"

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        d = dict(r)

        mid = d["machine_id"]
        dia = d["data"]

        ref_list = load_refugo_24(mid, dia)
        refugo_total = sum(int(x or 0) for x in ref_list)

        produzido = int(d.get("produzido", 0))
        pecas_boas = max(0, produzido - refugo_total)

        d["refugo_total"] = refugo_total
        d["pecas_boas"] = pecas_boas

        out.append(d)

    return jsonify(out)
