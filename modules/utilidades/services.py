# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\utilidades\services.py
# Último recode: 2026-01-16 22:55 (America/Bahia)
# Motivo: Centralizar regras de negócio do módulo Utilidades (status do sistema, offline e tempo parado)

from datetime import datetime
from zoneinfo import ZoneInfo

TZ_BAHIA = ZoneInfo("America/Bahia")


# ============================================================
# CONFIGURAÇÕES PADRÃO (V1)
# ============================================================

UTILIDADES_OFFLINE_SECONDS = 30      # tempo sem update = OFFLINE
UTILIDADES_POWER_KW_LIMIT = 0.0      # 0 = desabilitado (V1 simples)


# ============================================================
# HELPERS
# ============================================================

def now_bahia():
    return datetime.now(TZ_BAHIA)


def now_bahia_ms() -> int:
    return int(now_bahia().timestamp() * 1000)


def minutes_since(ms: int | None) -> int | None:
    if ms is None:
        return None
    diff = now_bahia_ms() - int(ms)
    if diff < 0:
        return 0
    return diff // 60000


# ============================================================
# REGRAS DE STATUS — FECHADAS
# ============================================================

def calc_system_status(system: dict) -> str:
    """
    Regras oficiais V1:

    PRODUZINDO (verde):
      - system_running = True
      - pressure_ok = True
      - online

    ATENCAO (amarelo):
      - system_running = True
      - pressure_ok = False
      - OU consumo alto (se habilitado)

    PARADA (vermelho):
      - system_running = False
      - OU sistema OFFLINE

    DESCONHECIDO:
      - dados insuficientes
    """

    last_seen = system.get("last_seen")

    # OFFLINE → PARADA
    if last_seen:
        try:
            last_dt = datetime.fromisoformat(last_seen)
            if (now_bahia() - last_dt).total_seconds() > UTILIDADES_OFFLINE_SECONDS:
                return "PARADA"
        except Exception:
            pass
    else:
        return "DESCONHECIDO"

    running = system.get("system_running")
    pressure_ok = system.get("pressure_ok")

    if running is False:
        return "PARADA"

    if running is True and pressure_ok is False:
        return "ATENCAO"

    if running is True and pressure_ok is True:
        # consumo alto opcional
        if UTILIDADES_POWER_KW_LIMIT > 0:
            try:
                pw = float(system.get("power_kw") or 0)
                if pw > UTILIDADES_POWER_KW_LIMIT:
                    return "ATENCAO"
            except Exception:
                pass
        return "PRODUZINDO"

    if running is True and pressure_ok is None:
        return "ATENCAO"

    return "DESCONHECIDO"


# ============================================================
# CONTROLE DE TEMPO PARADO (OFICIAL)
# ============================================================

def update_stopped_clock(system: dict, status: str):
    """
    Controle único e oficial do tempo parado.

    - PRODUZINDO → limpa relógio
    - PARADA     → ancora stopped_since_ms
    - ATENCAO    → não altera
    """

    if status == "PRODUZINDO":
        system["stopped_since_ms"] = None
        system["parado_min"] = None
        return

    if status == "PARADA":
        if system.get("stopped_since_ms") is None:
            system["stopped_since_ms"] = now_bahia_ms()

        system["parado_min"] = minutes_since(system.get("stopped_since_ms"))
        return

    # ATENCAO / DESCONHECIDO → mantém estado atual
