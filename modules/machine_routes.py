# modules/machine_routes.py
import os
from flask import Blueprint, request, jsonify, render_template
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


def _sum_refugo_24(machine_id: str, dia_ref: str) -> int:
    try:
        arr = load_refugo_24(_norm_machine_id(machine_id), (dia_ref or "").strip())
        if not isinstance(arr, list):
            return 0
        return sum(_safe_int(x, 0) for x in arr)
    except Exception:
        return 0


def _admin_token_ok() -> bool:
    """
    Proteção simples:
      - Configure no Railway/ENV: INDFLOW_ADMIN_TOKEN=<seu_token>
      - Envie no header: X-Admin-Token: <seu_token>
    """
    expected = (os.getenv("INDFLOW_ADMIN_TOKEN") or "").strip()
    if not expected:
        return False
    received = (request.headers.get("X-Admin-Token") or "").strip()
    return received == expected


# ============================================================
# ADMIN - HARD RESET (LIMPA BANCO)
# ============================================================
@machine_bp.route("/admin/hard-reset", methods=["POST"])
def admin_hard_reset():
    """
    HARD RESET: apaga TODOS os dados do SQLite (tabelas principais).
    Não depende de shell do Railway.
    """
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    conn = get_db()
    cur = conn.cursor()

    # Lista de tabelas a limpar (tolerante a tabela inexistente)
    tables = [
        "producao_diaria",
        "producao_horaria",
        "baseline_diario",
        "refugo_horaria",
        "machine_config",
    ]

    deleted = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(1) FROM {t}")
            before = cur.fetchone()[0]
        except Exception:
            before = None

        try:
            cur.execute(f"DELETE FROM {t}")
            deleted[t] = before
        except Exception:
            deleted[t] = "skipped"

    # tenta resetar autoincrement (se existir)
    try:
        cur.execute("DELETE FROM sqlite_sequence")
    except Exception:
        pass

    conn.commit()
    conn.close()

    # Nota: machine_state é memória; não limpamos aqui para evitar efeitos colaterais.
    # Após limpar banco, as telas vão repovoar conforme novos dados entrarem.
    return jsonify({
        "ok": True,
        "deleted_tables": deleted,
        "note": "Banco limpo. Recomece a contagem a partir do próximo envio do ESP."
    })


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

    # ✅ Persistência da config (agora via repo)
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
# REFUGO: SALVAR (PERSISTENTE)
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
        return jsonify({"ok": False, "error": "hora_dia inválida (0..23)"}), 400

    if refugo < 0:
        refugo = 0

    if dia_ref > dia_atual:
        return jsonify({"ok": False, "error": "dia_ref futuro não permitido"}), 400

    if dia_ref == dia_atual:
        hora_atual = int(agora.hour)
        if hora_dia >= hora_atual:
            return jsonify({"ok": False, "error": "Só é permitido lançar refugo em horas passadas"}), 400

    ok = upsert_refugo(
        machine_id=machine_id,
        dia_ref=dia_ref,
        hora_dia=hora_dia,
        refugo=refugo,
        updated_at_iso=agora.isoformat(),
    )

    if not ok:
        return jsonify({"ok": False, "error": "Falha ao salvar no banco"}), 500

    return jsonify({
        "ok": True,
        "machine_id": machine_id,
        "dia_ref": dia_ref,
        "hora_dia": hora_dia,
        "refugo": refugo
    })


# ============================================================
# STATUS
# ============================================================
@machine_bp.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = _norm_machine_id(request.args.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    carregar_baseline_diario(m, machine_id)

    atualizar_producao_hora(m)
    calcular_tempo_medio(m)
    aplicar_derivados_ml(m)

    dia_ref = dia_operacional_ref_str(now_bahia())
    m["refugo_por_hora"] = load_refugo_24(machine_id, dia_ref)

    # produzido líquido da hora atual (helper)
    try:
        hora_atual = int(now_bahia().hour)
    except Exception:
        hora_atual = None

    try:
        ph = int(m.get("producao_hora", 0) or 0)
    except Exception:
        ph = 0

    if isinstance(hora_atual, int) and 0 <= hora_atual < 24:
        m["producao_hora_liquida"] = max(0, ph - int(m["refugo_por_hora"][hora_atual] or 0))
    else:
        m["producao_hora_liquida"] = ph

    return jsonify(m)


# ============================================================
# HISTÓRICO - TELA (HTML)
# ============================================================
@machine_bp.route("/producao/historico", methods=["GET"])
def historico_page():
    # A tela usa JS para buscar o JSON na rota /api/producao/historico
    return render_template("historico.html")


# ============================================================
# HISTÓRICO - API (JSON)
# ============================================================
@machine_bp.route("/api/producao/historico", methods=["GET"])
def historico_producao_api():
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

    out = []
    for r in rows:
        d = dict(r)

        mid = d.get("machine_id") or "maquina01"
        dia_ref = d.get("data") or ""

        refugo_total = _sum_refugo_24(mid, dia_ref)

        produzido = _safe_int(d.get("produzido"), 0)
        pecas_boas = max(0, produzido - refugo_total)

        d["refugo_total"] = refugo_total
        d["pecas_boas"] = pecas_boas

        out.append(d)

    return jsonify(out)
