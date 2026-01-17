# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\utilidades\routes.py
# Último recode: 2026-01-16 22:35 (America/Bahia)
# Motivo: Adicionar backend V1 para "sistemas de utilidades" (card pai), com update/status e cálculo de PRODUZINDO/ATENCAO/PARADA + parado_min, mantendo rotas legadas de equipamentos individuais.

from flask import Blueprint, render_template, request, jsonify
from datetime import datetime
from zoneinfo import ZoneInfo

from .data import (
    utilidades_data,         # legado (equipamentos individuais)
    utilidades_systems,      # novo (sistemas)
    get_or_create_system,
    now_bahia_iso,
)

from modules.admin.routes import login_required

utilidades_bp = Blueprint("utilidades", __name__, template_folder="templates")

TZ_BAHIA = ZoneInfo("America/Bahia")


# ============================================================
# CONFIGS (V1) — simples via ENV
# ============================================================

def _env_int(name: str, default: int) -> int:
    try:
        v = int((__import__("os").getenv(name) or "").strip())
        return v
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float((__import__("os").getenv(name) or "").strip().replace(",", "."))
        return v
    except Exception:
        return default


# Se o ESP não atualizar dentro desse tempo, consideramos OFFLINE => PARADA
UTILIDADES_OFFLINE_SECONDS = _env_int("UTILIDADES_OFFLINE_SECONDS", 30)

# Limite de consumo para ATENÇÃO (opcional). Se 0, não aplica.
# Pode ajustar depois por sistema (por enquanto global simples).
UTILIDADES_POWER_KW_LIMIT = _env_float("UTILIDADES_POWER_KW_LIMIT", 0.0)


def _now_ms_bahia() -> int:
    return int(datetime.now(TZ_BAHIA).timestamp() * 1000)


def _parse_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _calc_system_status(system: dict) -> str:
    """
    Regras V1 (fechadas com você):
      - PRODUZINDO (verde): system_running=True e pressure_ok=True e online
      - ATENCAO (amarelo): system_running=True e pressure_ok=False OU consumo alto (se habilitado)
      - PARADA (vermelho): system_running=False OU offline
      - DESCONHECIDO: sem dados suficientes
    """
    # Offline?
    last_seen = system.get("last_seen")
    if last_seen:
        try:
            last_dt = datetime.fromisoformat(last_seen)
            now_dt = datetime.now(TZ_BAHIA)
            if (now_dt - last_dt).total_seconds() > UTILIDADES_OFFLINE_SECONDS:
                return "PARADA"
        except Exception:
            # se não consegue parsear, não marca offline por isso
            pass
    else:
        return "DESCONHECIDO"

    running = system.get("system_running")
    pressure_ok = system.get("pressure_ok")

    # Se o ESP explicitou parada
    if running is False:
        return "PARADA"

    # Se está rodando mas pressão fora
    if running is True and pressure_ok is False:
        return "ATENCAO"

    # Consumo alto (se configurado)
    if running is True and UTILIDADES_POWER_KW_LIMIT > 0:
        pw = system.get("power_kw")
        try:
            if pw is not None and float(pw) > float(UTILIDADES_POWER_KW_LIMIT):
                return "ATENCAO"
        except Exception:
            pass

    # Produzindo (condição ideal)
    if running is True and pressure_ok is True:
        return "PRODUZINDO"

    # Se está rodando mas não temos pressão_ok ainda
    if running is True and pressure_ok is None:
        return "ATENCAO"

    return "DESCONHECIDO"


def _update_stopped_clock(system: dict, status: str):
    """
    Controle de tempo parado oficial (backend).
    - Se status == PRODUZINDO -> limpa stopped_since_ms / parado_min
    - Se status == PARADA -> ancora stopped_since_ms se vazio
    - ATENCAO/ DESCONHECIDO -> não mexe no relógio (por enquanto)
    """
    now_ms = _now_ms_bahia()

    if status == "PRODUZINDO":
        system["stopped_since_ms"] = None
        system["parado_min"] = None
        return

    if status == "PARADA":
        if system.get("stopped_since_ms") is None:
            system["stopped_since_ms"] = now_ms

        ss = system.get("stopped_since_ms")
        try:
            diff_ms = max(0, now_ms - int(ss))
            system["parado_min"] = int(diff_ms // 60000)
        except Exception:
            system["parado_min"] = None
        return

    # ATENCAO / DESCONHECIDO: deixa como está


def _equip_summary(system: dict) -> dict:
    eqs = system.get("equipments") or []
    if not isinstance(eqs, list):
        eqs = []

    total = 0
    running = 0
    for e in eqs:
        if not isinstance(e, dict):
            continue
        total += 1
        if _parse_bool(e.get("running")) is True:
            running += 1

    return {"total": total, "running": running}


# ============================================================
# HOME — Lista de equipamentos (LEGADO)
# ============================================================
@utilidades_bp.route("/")
@login_required
def home():
    return render_template("utilidades_home.html")


# ============================================================
# STATUS INDIVIDUAL (LEGADO)
# ============================================================
@utilidades_bp.route("/status", methods=["GET"])
def status_utilidade():
    machine_id = request.args.get("machine_id")

    if not machine_id or machine_id not in utilidades_data:
        return jsonify({"error": "Equipamento não encontrado"}), 404

    return jsonify(utilidades_data[machine_id])


# ============================================================
# TELA DE CONFIGURAÇÃO (LEGADO)
# ============================================================
@utilidades_bp.route("/config/<machine_id>")
@login_required
def config(machine_id):
    return render_template("utilidades_config.html", machine_id=machine_id)


# ============================================================
# RECEBER DADOS DO ESP32 (LEGADO) — equipamentos individuais
# ============================================================
@utilidades_bp.route("/update", methods=["POST"])
def update():
    data = request.get_json() or {}

    machine_id = data.get("machine_id")
    if machine_id not in utilidades_data:
        return jsonify({"error": "machine_id inválido"}), 400

    util = utilidades_data[machine_id]

    util["ligado"] = int(data.get("ligado", util["ligado"]))
    util["falha"] = int(data.get("falha", util["falha"]))
    util["horas_vida"] = float(data.get("horas_vida", util["horas_vida"]))
    util["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")

    return jsonify({"message": "OK"})


# ============================================================
# NOVO (V1) — SISTEMAS (CARD PAI)
# ============================================================

@utilidades_bp.route("/system/update", methods=["POST"])
def system_update():
    """
    Payload padrão (V1):
      {
        "system_type": "AIR",
        "system_id": "air_01",
        "pressure_ok": true/false,
        "system_running": true/false,
        "power_kw": 18.4,
        "energy_kwh_day": 124.7,
        "equipments": [{"id":"comp01","running":true}, ...],   (opcional)
        "timestamp": "ISO",                                    (opcional)
        "fw_version": "1.0.0"                                  (opcional)
      }
    """
    data = request.get_json() or {}

    system_type = (data.get("system_type") or "").strip().upper()
    system_id = (data.get("system_id") or "").strip()

    if not system_type or not system_id:
        return jsonify({"error": "system_type e system_id são obrigatórios"}), 400

    try:
        system = get_or_create_system(system_type, system_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    # fatos (ESP)
    system["pressure_ok"] = _parse_bool(data.get("pressure_ok"))
    system["system_running"] = _parse_bool(data.get("system_running"))

    system["power_kw"] = _safe_float(data.get("power_kw"))
    system["energy_kwh_day"] = _safe_float(data.get("energy_kwh_day"))

    eqs = data.get("equipments")
    if isinstance(eqs, list):
        cleaned = []
        for e in eqs:
            if not isinstance(e, dict):
                continue
            eid = (e.get("id") or "").strip()
            if not eid:
                continue
            cleaned.append({
                "id": eid,
                "running": bool(_parse_bool(e.get("running"))),
            })
        system["equipments"] = cleaned

    system["timestamp"] = (data.get("timestamp") or None)
    system["fw_version"] = (data.get("fw_version") or None)

    # controle
    system["last_seen"] = now_bahia_iso()

    # calcula status
    status = _calc_system_status(system)
    system["status"] = status

    # controla tempo parado
    _update_stopped_clock(system, status)

    return jsonify({
        "ok": True,
        "system_id": system_id,
        "system_type": system_type,
        "status": system["status"],
        "parado_min": system.get("parado_min"),
        "last_seen": system.get("last_seen"),
    })


@utilidades_bp.route("/system/status", methods=["GET"])
def system_status():
    """
    Retorno pronto para o card pai.
    Query:
      - system_id (opcional). Se não enviar, lista todos.
    """
    system_id = (request.args.get("system_id") or "").strip()

    def build_out(s: dict) -> dict:
        # recalcula offline/status no GET também (caso o ESP pare de falar)
        status = _calc_system_status(s)
        s["status"] = status
        _update_stopped_clock(s, status)

        eq = _equip_summary(s)

        return {
            "system_type": s.get("system_type"),
            "system_id": s.get("system_id"),
            "status": s.get("status"),
            "pressure_ok": s.get("pressure_ok"),
            "system_running": s.get("system_running"),
            "power_kw": s.get("power_kw"),
            "energy_kwh_day": s.get("energy_kwh_day"),
            "equipments_total": eq["total"],
            "equipments_running": eq["running"],
            "parado_min": s.get("parado_min"),
            "last_seen": s.get("last_seen"),
        }

    if system_id:
        s = utilidades_systems.get(system_id)
        if not s:
            return jsonify({"error": "system_id não encontrado"}), 404
        return jsonify(build_out(s))

    # lista todos
    out = [build_out(s) for s in utilidades_systems.values()]
    return jsonify(out)
