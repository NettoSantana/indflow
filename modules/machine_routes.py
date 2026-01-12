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
    carregar_baseline_diario,
    now_bahia,
    dia_operacional_ref_str,
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
# REFUGO (PERSISTENTE) - SQLITE
# ============================================================
def _ensure_refugo_table():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS refugo_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,          -- dia operacional (vira 23:59)
            hora_dia INTEGER NOT NULL,      -- 0..23 (hora do dia)
            refugo INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_refugo_horaria
        ON refugo_horaria(machine_id, dia_ref, hora_dia)
    """)
    conn.commit()
    conn.close()


def _load_refugo_24(machine_id: str, dia_ref: str):
    out = [0] * 24
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT hora_dia, refugo
            FROM refugo_horaria
            WHERE machine_id=? AND dia_ref=?
        """, (machine_id, dia_ref))
        rows = cur.fetchall() or []
        conn.close()

        for r in rows:
            try:
                h = int(r[0])
                v = int(r[1])
                if 0 <= h < 24:
                    out[h] = max(0, v)
            except Exception:
                continue
    except Exception:
        pass
    return out


def _upsert_refugo(machine_id: str, dia_ref: str, hora_dia: int, refugo: int):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO refugo_horaria (machine_id, dia_ref, hora_dia, refugo, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, dia_ref, hora_dia)
            DO UPDATE SET
                refugo=excluded.refugo,
                updated_at=excluded.updated_at
        """, (machine_id, dia_ref, int(hora_dia), int(refugo), now_bahia().isoformat()))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


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
            machine_id,
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
# REFUGO: SALVAR (PERSISTENTE)
# ============================================================
@machine_bp.route("/machine/refugo", methods=["POST"])
def salvar_refugo():
    """
    Payload:
      {
        "machine_id": "maquina01",
        "dia_ref": "YYYY-MM-DD" (opcional),
        "hora_dia": 0..23,
        "refugo": 0..N
      }

    Regras:
      - dia_ref futuro: bloqueia
      - se dia_ref == dia operacional atual:
          só aceita hora_dia < hora_atual (passado). Hora atual e futuro bloqueado.
      - dia_ref passado: aceita (todas as horas são passadas)
    """
    _ensure_refugo_table()

    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    agora = now_bahia()
    dia_atual = dia_operacional_ref_str(agora)

    dia_ref = (data.get("dia_ref") or "").strip()
    if not dia_ref:
        dia_ref = dia_atual

    hora_dia = _safe_int(data.get("hora_dia"), -1)
    refugo = _safe_int(data.get("refugo"), 0)

    if hora_dia < 0 or hora_dia > 23:
        return jsonify({"ok": False, "error": "hora_dia inválida (0..23)"}), 400

    if refugo < 0:
        refugo = 0

    # bloqueia dia futuro
    if dia_ref > dia_atual:
        return jsonify({"ok": False, "error": "dia_ref futuro não permitido"}), 400

    # se for o dia atual, só aceita passado (hora_dia < hora atual)
    if dia_ref == dia_atual:
        hora_atual = int(agora.hour)
        if hora_dia >= hora_atual:
            return jsonify({"ok": False, "error": "Só é permitido lançar refugo em horas passadas"}), 400

    ok = _upsert_refugo(machine_id, dia_ref, hora_dia, refugo)
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

    # garante baseline carregado mesmo se o dashboard abrir logo após restart
    carregar_baseline_diario(m, machine_id)

    atualizar_producao_hora(m)
    calcular_tempo_medio(m)
    aplicar_derivados_ml(m)

    # refugo do dia operacional atual (lista 24 por hora do dia)
    try:
        _ensure_refugo_table()
        dia_ref = dia_operacional_ref_str(now_bahia())
        ref24 = _load_refugo_24(machine_id, dia_ref)
    except Exception:
        dia_ref = dia_operacional_ref_str(now_bahia())
        ref24 = [0] * 24

    m["refugo_por_hora"] = ref24

    # produzido líquido por hora (se UI quiser usar direto)
    prod_list = m.get("producao_por_hora")
    if isinstance(prod_list, list):
        liquid = []
        for idx, val in enumerate(prod_list):
            try:
                p = int(val) if val is not None else None
            except Exception:
                p = None

            # aqui "idx" é índice do TURNO; a UI remapeia p/ hora do dia.
            # Ainda assim, damos um helper líquido por idx do turno (mesmo tamanho).
            # Como refugo é por hora do dia, não dá pra subtrair aqui com precisão
            # sem saber o mapeamento (turno_inicio). Então só expomos lista bruta
            # e a UI subtrai corretamente depois.
            liquid.append(p)
        m["producao_por_hora_liquida"] = liquid
    else:
        m["producao_por_hora_liquida"] = []

    # produzido líquido da hora atual (não deve ter refugo editável agora, mas expomos)
    try:
        hora_atual = int(now_bahia().hour)
    except Exception:
        hora_atual = None

    try:
        ph = int(m.get("producao_hora", 0) or 0)
    except Exception:
        ph = 0

    if isinstance(hora_atual, int) and 0 <= hora_atual < 24:
        m["producao_hora_liquida"] = max(0, ph - int(ref24[hora_atual] or 0))
    else:
        m["producao_hora_liquida"] = ph

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
