# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\utilidades\data.py
# Último recode: 2026-01-16 22:10 (America/Bahia)
# Motivo: Separar store de "sistemas de utilidades" (ex: Ar Comprimido) do legado de equipamentos individuais, preparando backend V1.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

TZ_BAHIA = ZoneInfo("America/Bahia")


def now_bahia_iso() -> str:
    return datetime.now(TZ_BAHIA).isoformat(timespec="seconds")


# ============================================================
# LEGADO (equipamentos individuais) — mantém como está
# ============================================================
utilidades_data: Dict[str, Dict[str, Any]] = {
    "util_comp01": {
        "nome": "Compressor 01",
        "tipo": "compressor",
        "ligado": 0,
        "falha": 0,
        "horas_vida": 0,
        "ultima_atualizacao": None,
    },
    "util_ger01": {
        "nome": "Gerador 01",
        "tipo": "gerador",
        "ligado": 0,
        "falha": 0,
        "horas_vida": 0,
        "ultima_atualizacao": None,
    },
}


# ============================================================
# NOVO (V1): sistemas de utilidades (card PAI)
# Ex.: Ar Comprimido = 1 sistema, com N equipamentos "filhos"
# ============================================================

# Estrutura mínima do payload vindo do ESP (utilidades):
# - system_type: "AIR"
# - system_id: "air_01"
# - pressure_ok: bool
# - system_running: bool
# - power_kw: float
# - energy_kwh_day: float
# - equipments: [{id, running}] (opcional)
#
# O backend vai calcular:
# - status: "PRODUZINDO" | "ATENCAO" | "PARADA"
# - parado_min (a partir de stopped_since_ms)
#
# Obs: aqui é só o store em memória (V1). Persistência em sqlite vem depois.

def _default_system(system_type: str, system_id: str) -> Dict[str, Any]:
    return {
        "system_type": (system_type or "UNKNOWN").strip().upper(),
        "system_id": (system_id or "").strip(),
        # sinais digitais / estado
        "pressure_ok": None,          # bool | None
        "system_running": None,       # bool | None
        # consumo do sistema (linha)
        "power_kw": None,             # float | None
        "energy_kwh_day": None,       # float | None
        # lista de equipamentos filhos (opcional)
        "equipments": [],             # list[{"id": str, "running": bool}]
        # calculados pelo backend
        "status": "DESCONHECIDO",     # PRODUZINDO | ATENCAO | PARADA | DESCONHECIDO
        "stopped_since_ms": None,     # int | None (epoch ms)
        "parado_min": None,           # int | None
        # controle
        "last_seen": None,            # iso str
        "fw_version": None,           # str | None
        "timestamp": None,            # iso str do ESP (se vier)
    }


# Cards pré-configurados (V1) — pode adicionar depois:
# AIR: Ar Comprimido
utilidades_systems: Dict[str, Dict[str, Any]] = {
    "air_01": _default_system("AIR", "air_01"),
}


def get_or_create_system(system_type: str, system_id: str) -> Dict[str, Any]:
    sid = (system_id or "").strip()
    stype = (system_type or "UNKNOWN").strip().upper()

    if not sid:
        raise ValueError("system_id é obrigatório")

    if sid not in utilidades_systems:
        utilidades_systems[sid] = _default_system(stype, sid)
    else:
        # mantém o type atualizado caso venha diferente (não quebra)
        utilidades_systems[sid]["system_type"] = stype

    return utilidades_systems[sid]
