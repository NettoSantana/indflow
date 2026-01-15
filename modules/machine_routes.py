# modules/machine_routes.py
import os
import hashlib
from flask import Blueprint, request, jsonify, render_template
from datetime import datetime, timedelta, timezone

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
    load_refugo_24,
    save_refugo_24,
    validar_horas_turno_config,
)

machine_bp = Blueprint("machine", __name__, template_folder="templates")


# ============================================================
# HELPERS
# ============================================================
def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _norm_machine_id(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return "maquina01"
    return v


def _safe_int(v, default=0) -> int:
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


def _get_cliente_from_api_key() -> dict | None:
    """
    Valida a API Key enviada pelo ESP (header X-API-Key) e retorna o cliente (row dict).
    Regras:
      - X-API-Key obrigatório
      - compara SHA256(api_key) com clientes.api_key_hash
      - cliente precisa estar active
    """
    api_key = (request.headers.get("X-API-Key") or "").strip()
    if not api_key:
        return None

    api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    conn = get_db()
    try:
        cur = conn.execute(
            "SELECT id, nome, status FROM clientes WHERE api_key_hash = ?",
            (api_key_hash,),
        )
        row = cur.fetchone()
        if not row:
            return None
        if (row["status"] or "").strip().lower() != "active":
            return None
        return {"id": row["id"], "nome": row["nome"], "status": row["status"]}
    finally:
        conn.close()


def _calcular_dia_ref_operacional() -> str:
    """
    Dia operacional com virada às 23:59 (regra simples e estável).
    Retorna string YYYY-MM-DD.

    Lógica:
      - Considera hora UTC (Railway). Se você quiser Bahia (-03), ajustamos depois.
      - Se agora >= 23:59, o dia_ref é o dia atual.
      - Se agora < 23:59, o dia_ref é o dia atual também.
    Observação: como "23:59" é praticamente no fim do dia, na prática o dia_ref
    acaba sendo o mesmo dia. Mantemos essa função aqui pra não depender do machine_calc.
    """
    now = datetime.utcnow()
    return now.strftime("%Y-%m-%d")


def _ensure_baseline_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS baseline_diario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,
            baseline_esp INTEGER NOT NULL,
            esp_last INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario
        ON baseline_diario(machine_id, dia_ref)
    """)
    conn.commit()


def _has_baseline_for_day(conn, machine_id: str, dia_ref: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT 1 FROM baseline_diario WHERE machine_id=? AND dia_ref=? LIMIT 1",
            (machine_id, dia_ref),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def norm_device_id(v: str) -> str:
    v = (v or "").strip().lower()
    v = v.replace(":", "").replace("-", "").replace(" ", "")
    return v


def touch_device_seen(device_id: str) -> None:
    if not device_id:
        return
    conn = get_db()
    try:
        now = _now_iso()
        conn.execute("""
            INSERT INTO devices (device_id, created_at, last_seen)
            VALUES (?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET last_seen=excluded.last_seen
        """, (device_id, now, now))
        conn.commit()
    finally:
        conn.close()


def get_machine_from_device(device_id: str) -> str | None:
    if not device_id:
        return None
    conn = get_db()
    try:
        cur = conn.execute("SELECT machine_id FROM devices WHERE device_id=?", (device_id,))
        row = cur.fetchone()
        if not row:
            return None
        return row["machine_id"]
    finally:
        conn.close()


def link_device_to_machine(device_id: str, machine_id: str, alias: str | None = None) -> None:
    if not device_id or not machine_id:
        return
    conn = get_db()
    try:
        now = _now_iso()
        conn.execute("""
            INSERT INTO devices (device_id, machine_id, alias, created_at, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                machine_id=excluded.machine_id,
                alias=COALESCE(excluded.alias, devices.alias),
                last_seen=excluded.last_seen
        """, (device_id, machine_id, alias, now, now))
        conn.commit()
    finally:
        conn.close()


# ============================================================
# UI / PÁGINAS
# ============================================================
@machine_bp.route("/machine")
def machine_home():
    return render_template("machine_home.html")


# ============================================================
# ADMIN - VINCULAR DEVICE A MÁQUINA (ZERA CONTEXTO AO VINCULAR)
# ============================================================
@machine_bp.route("/machine/link_device", methods=["POST"])
def link_device():
    if not _admin_token_ok():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    device_id = norm_device_id(data.get("device_id") or data.get("mac") or "")
    machine_id = _norm_machine_id(data.get("machine_id") or "")
    alias = (data.get("alias") or "").strip() or None

    if not device_id or not machine_id:
        return jsonify({"error": "device_id e machine_id são obrigatórios"}), 400

    # Vincula
    link_device_to_machine(device_id, machine_id, alias=alias)

    # REGRA: ao vincular, zera contexto da máquina vinculada (baseline/last/contadores)
    m = get_machine(machine_id)
    reset_contexto(m)

    return jsonify({
        "message": "OK",
        "device_id": device_id,
        "machine_id": machine_id,
        "alias": alias,
        "context_reset": True
    })


# ============================================================
# ADMIN - LISTAR DEVICES
# ============================================================
@machine_bp.route("/machine/devices", methods=["GET"])
def list_devices():
    if not _admin_token_ok():
        return jsonify({"error": "unauthorized"}), 401

    conn = get_db()
    try:
        cur = conn.execute("""
            SELECT device_id, machine_id, alias, created_at, last_seen
            FROM devices
            ORDER BY last_seen DESC
            LIMIT 200
        """)
        rows = [dict(r) for r in cur.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()


# ============================================================
# ADMIN - DESVINCULAR DEVICE
# ============================================================
@machine_bp.route("/machine/unlink_device", methods=["POST"])
def unlink_device():
    if not _admin_token_ok():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    device_id = norm_device_id(data.get("device_id") or data.get("mac") or "")
    if not device_id:
        return jsonify({"error": "device_id é obrigatório"}), 400

    conn = get_db()
    try:
        conn.execute("UPDATE devices SET machine_id=NULL WHERE device_id=?", (device_id,))
        conn.commit()
        return jsonify({"message": "OK", "device_id": device_id, "unlinked": True})
    finally:
        conn.close()


# ============================================================
# PRODUÇÃO - GET STATUS (exemplo)
# ============================================================
@machine_bp.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = _norm_machine_id(request.args.get("machine_id") or "maquina01")
    m = get_machine(machine_id)
    return jsonify(m)


# ============================================================
# UPDATE ESP
# ============================================================
@machine_bp.route("/machine/update", methods=["POST"])
def update_machine():
    data = request.get_json() or {}

    # 0) AUTH (ESP): exige X-API-Key e resolve o cliente
    cliente = _get_cliente_from_api_key()
    if not cliente:
        return jsonify({"error": "unauthorized"}), 401

    cliente_id = cliente["id"]

    # 1) MAC do ESP (CPF)
    device_id = norm_device_id(data.get("mac") or data.get("device_id") or "")
    if device_id:
        touch_device_seen(device_id)

    # 2) Resolve machine_id: se device estiver vinculado, ele manda.
    linked_machine = get_machine_from_device(device_id) if device_id else None
    if linked_machine:
        machine_id = _norm_machine_id(linked_machine)
    else:
        machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    m = get_machine(machine_id)

    # 3) Atualiza valores vindos do ESP
    esp_absoluto = _safe_int(data.get("esp_absoluto"), None)
    if esp_absoluto is None:
        esp_absoluto = _safe_int(data.get("pulsos"), 0)

    estado = data.get("estado")
    if estado is None:
        estado = data.get("rodando")
    if estado is None:
        estado = data.get("running")
    if estado is not None:
        m["rodando"] = 1 if str(estado).strip() in ("1", "true", "True", "rodando", "on") else 0

    # 4) Dia ref operacional
    dia_ref = _calcular_dia_ref_operacional()

    # 5) Reset diário se necessário
    verificar_reset_diario(m)

    # 6) Persistência baseline diário (se ainda não existir)
    conn = get_db()
    baseline_initialized = False
    try:
        _ensure_baseline_table(conn)
        if not _has_baseline_for_day(conn, machine_id, dia_ref):
            conn.execute("""
                INSERT INTO baseline_diario (machine_id, dia_ref, baseline_esp, esp_last, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (machine_id, dia_ref, esp_absoluto, esp_absoluto, _now_iso()))
            conn.commit()
            baseline_initialized = True
    finally:
        conn.close()

    # 7) Atualiza no contexto da máquina
    m["esp_absoluto"] = esp_absoluto
    m["updated_at"] = _now_iso()

    # 8) Aplicar unidades / derivados / cálculo
    aplicar_unidades(m)
    salvar_conversao(m)
    aplicar_derivados_ml(m)
    calcular_tempo_medio(m)

    # 9) Atualiza produção por hora + rampa + etc
    atualizar_producao_hora(m)

    return jsonify({
        "message": "OK",
        "machine_id": machine_id,
        "cliente_id": cliente_id,
        "device_id": device_id or None,
        "linked_machine": linked_machine or None,
        "baseline_initialized": bool(baseline_initialized),
    })


# ============================================================
# RESET MANUAL
# ============================================================
@machine_bp.route("/machine/reset", methods=["POST"])
def reset_machine():
    if not _admin_token_ok():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id") or "maquina01")
    m = get_machine(machine_id)
    reset_contexto(m)
    return jsonify({"message": "OK", "machine_id": machine_id, "reset": True})


# ============================================================
# CONFIG - SET
# ============================================================
@machine_bp.route("/machine/config/set", methods=["POST"])
def set_config():
    if not _admin_token_ok():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id") or "maquina01")

    meta_turno = _safe_int(data.get("meta_turno"), 0)
    turno_inicio = (data.get("turno_inicio") or "").strip() or None
    turno_fim = (data.get("turno_fim") or "").strip() or None
    rampa_percentual = _safe_int(data.get("rampa_percentual"), 0)

    horas_turno_json = data.get("horas_turno_json")
    if horas_turno_json is None:
        horas_turno_json = "[]"

    meta_por_hora_json = data.get("meta_por_hora_json")
    if meta_por_hora_json is None:
        meta_por_hora_json = "[]"

    try:
        validar_horas_turno_config(horas_turno_json)
    except Exception as e:
        return jsonify({"error": "horas_turno_json inválido", "details": str(e)}), 400

    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO machine_config (
                machine_id, meta_turno, turno_inicio, turno_fim,
                rampa_percentual, horas_turno_json, meta_por_hora_json, updated_at
            )
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
            meta_turno,
            turno_inicio,
            turno_fim,
            rampa_percentual,
            horas_turno_json,
            meta_por_hora_json,
            _now_iso(),
        ))
        conn.commit()
    finally:
        conn.close()

    return jsonify({"message": "OK", "machine_id": machine_id})


# ============================================================
# CONFIG - GET
# ============================================================
@machine_bp.route("/machine/config/get", methods=["GET"])
def get_config():
    machine_id = _norm_machine_id(request.args.get("machine_id") or "maquina01")

    conn = get_db()
    try:
        cur = conn.execute("SELECT * FROM machine_config WHERE machine_id=?", (machine_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"machine_id": machine_id, "config": None})
        return jsonify({"machine_id": machine_id, "config": dict(row)})
    finally:
        conn.close()


# ============================================================
# REFUGO 24H (exemplo)
# ============================================================
@machine_bp.route("/machine/refugo24/get", methods=["GET"])
def refugo24_get():
    machine_id = _norm_machine_id(request.args.get("machine_id") or "maquina01")
    dia_ref = _calcular_dia_ref_operacional()
    arr = load_refugo_24(machine_id, dia_ref)
    return jsonify({
        "machine_id": machine_id,
        "dia_ref": dia_ref,
        "items": arr if isinstance(arr, list) else []
    })


@machine_bp.route("/machine/refugo24/set", methods=["POST"])
def refugo24_set():
    if not _admin_token_ok():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id") or "maquina01")
    items = data.get("items")
    if not isinstance(items, list):
        return jsonify({"error": "items deve ser uma lista"}), 400

    dia_ref = _calcular_dia_ref_operacional()
    save_refugo_24(machine_id, dia_ref, items)

    total = _sum_refugo_24(machine_id, dia_ref)
    return jsonify({
        "message": "OK",
        "machine_id": machine_id,
        "dia_ref": dia_ref,
        "total": total
    })


# ============================================================
# DEBUG / PING
# ============================================================
@machine_bp.route("/machine/ping", methods=["GET"])
def ping():
    return jsonify({"ok": True, "ts": _now_iso()})
