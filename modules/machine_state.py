# modules/machine_state.py
from datetime import datetime
import json

from modules.db_indflow import get_db
from modules.machine_calc import now_bahia, dia_operacional_ref_str

machine_data = {}


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


def _load_machine_config(machine_id: str):
    _ensure_machine_config_table()

    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        return None

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT meta_turno, turno_inicio, turno_fim, rampa_percentual, horas_turno_json, meta_por_hora_json
        FROM machine_config
        WHERE machine_id=?
        LIMIT 1
    """, (machine_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    try:
        meta_turno = int(row[0] or 0)
    except Exception:
        meta_turno = 0

    turno_inicio = row[1]
    turno_fim = row[2]

    try:
        rampa = int(row[3] or 0)
    except Exception:
        rampa = 0

    try:
        horas_turno = json.loads(row[4] or "[]")
        if not isinstance(horas_turno, list):
            horas_turno = []
    except Exception:
        horas_turno = []

    try:
        meta_por_hora = json.loads(row[5] or "[]")
        if not isinstance(meta_por_hora, list):
            meta_por_hora = []
    except Exception:
        meta_por_hora = []

    return {
        "meta_turno": meta_turno,
        "turno_inicio": turno_inicio,
        "turno_fim": turno_fim,
        "rampa_percentual": rampa,
        "horas_turno": horas_turno,
        "meta_por_hora": meta_por_hora,
    }


def _load_ultimo_dia_from_db(machine_id: str) -> str | None:
    """
    ✅ Fonte da verdade do "dia operacional" pós-deploy:
    usa baseline_diario (já persistido e atualizado pelo sistema).
    Retorna dia_ref (YYYY-MM-DD) ou None.
    """
    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        return None

    try:
        conn = get_db()
        cur = conn.cursor()

        # baseline_diario: (machine_id, dia_ref, updated_at...)
        cur.execute("""
            SELECT dia_ref
            FROM baseline_diario
            WHERE machine_id=?
            ORDER BY updated_at DESC
            LIMIT 1
        """, (machine_id,))
        row = cur.fetchone()
        conn.close()

        if row and row[0]:
            return str(row[0])
    except Exception:
        return None

    return None


def get_machine(machine_id: str):
    # ✅ normaliza sempre (evita MAQUINA01 vs maquina01 virar duas máquinas)
    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        machine_id = "maquina01"

    if machine_id not in machine_data:
        agora = now_bahia()

        # ✅ pós-deploy: tenta recuperar o "dia operacional" do DB (baseline_diario)
        ultimo_dia_db = _load_ultimo_dia_from_db(machine_id)
        if ultimo_dia_db:
            ultimo_dia = ultimo_dia_db
        else:
            ultimo_dia = dia_operacional_ref_str(agora)

        machine_data[machine_id] = {
            "nome": machine_id.upper(),
            "status": "DESCONHECIDO",

            "meta_turno": 0,
            "turno_inicio": None,
            "turno_fim": None,
            "rampa_percentual": 0,

            "unidade_1": None,
            "unidade_2": None,
            "conv_m_por_pcs": 1.0,

            "esp_absoluto": 0,
            "baseline_diario": 0,
            "baseline_hora": 0,

            "producao_turno": 0,
            "producao_turno_anterior": 0,

            "horas_turno": [],
            "meta_por_hora": [],
            "producao_hora": 0,
            "percentual_hora": 0,
            "ultima_hora": None,

            "percentual_turno": 0,
            "tempo_medio_min_por_peca": None,

            # ✅ agora é STRING YYYY-MM-DD (combina com machine_calc.verificar_reset_diario)
            "ultimo_dia": ultimo_dia,
            "reset_executado_hoje": False
        }

        # Carrega config persistida (se existir) e aplica no estado
        cfg = _load_machine_config(machine_id)
        if cfg:
            machine_data[machine_id]["meta_turno"] = cfg.get("meta_turno", 0) or 0
            machine_data[machine_id]["turno_inicio"] = cfg.get("turno_inicio")
            machine_data[machine_id]["turno_fim"] = cfg.get("turno_fim")
            machine_data[machine_id]["rampa_percentual"] = cfg.get("rampa_percentual", 0) or 0
            machine_data[machine_id]["horas_turno"] = cfg.get("horas_turno") or []
            machine_data[machine_id]["meta_por_hora"] = cfg.get("meta_por_hora") or []

    return machine_data[machine_id]
