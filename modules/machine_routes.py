# modules/machine_routes.py
from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import json

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
    carregar_baseline_diario,  # ✅ correto para o seu machine_calc.py
)

machine_bp = Blueprint("machine_bp", __name__)


def _norm_machine_id(v):
    v = (v or "").strip().lower()
    return v or "maquina01"


def _ensure_machine_config_table():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS machine_config (
            machine_id TEXT PRIMARY KEY,
            meta_turno INTEGER NOT NULL DEFAULT 0,
            turno_inicio TEXT,
            turno_fim TEXT,
            rampa_percentual INTEGER NOT NULL DEFAULT 0,
            horas_turno_json TEXT NOT NULL DEFAULT '[]',
            meta_por_hora_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


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

    # ✅ PERSISTE CONFIG (pra meta não virar 0 após deploy/restart)
    try:
        _ensure_machine_config_table()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO machine_config
            (machine_id, meta_turno, turno_inicio, turno_fim, rampa_percentual, horas_turno_json, meta_por_hora_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(machine_id) DO UPDATE SET
                meta_turno=excluded.meta_turno,
                turno_inicio=excluded.turno_inicio,
                turno_fim=excluded.turno_fim,
                rampa_percentual=excluded.rampa_percentual,
                horas_turno_json=excluded.horas_turno_json,
                meta_por_hora_json=excluded.meta_por_hora_json,
                updated_at=excluded.updated_at
        """, (
            machine_id,  # ✅ normalizado
            int(m.get("meta_turno") or 0),
            m.get("turno_inicio"),
            m.get("turno_fim"),
            int(m.get("rampa_percentual") or 0),
            json.dumps(m.get("horas_turno") or []),
            json.dumps(m.get("meta_por_hora") or []),
            datetime.now().isoformat()
        ))
        conn.commit()
        conn.close()
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

    # ✅ reset pelo dia operacional (vira 23:59)
    verificar_reset_diario(m, machine_id)

    m["status"] = data.get("status", "DESCONHECIDO")
    m["esp_absoluto"] = int(data["producao_turno"])

    # ✅ baseline diário persistido no SQLite (dia operacional)
    carregar_baseline_diario(m, machine_id)

    producao_atual = max(int(m.get("esp_absoluto", 0) or 0) - int(m.get("baseline_diario", 0) or 0), 0)
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
# STATUS
# ============================================================
@machine_bp.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = _norm_machine_id(request.args.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    # garante baseline carregado mesmo se o dashboard abrir logo após restart
    carregar_baseline_diario(m, machine_id)

    atualizar_producao_hora(m)
    calcular_tempo_medio(m)
    aplicar_derivados_ml(m)

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
        params.append(_norm_machine_id(machine_id))

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
