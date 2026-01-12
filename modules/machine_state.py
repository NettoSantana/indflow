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


def _load_baseline_diario_state(machine_id: str):
    """
    ✅ Evita 'ancorar 0' após deploy.
    Carrega do SQLite o último estado conhecido do dia operacional:
      - dia_ref
      - baseline_esp
      - esp_last
    Retorna dict ou None.
    """
    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        return None

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT dia_ref, baseline_esp, esp_last
            FROM baseline_diario
            WHERE machine_id=?
            ORDER BY updated_at DESC
            LIMIT 1
        """, (machine_id,))
        row = cur.fetchone()
        conn.close()

        if not row:
            return None

        dia_ref = str(row[0]) if row[0] else None
        try:
            baseline_esp = int(row[1])
        except Exception:
            baseline_esp = None
        try:
            esp_last = int(row[2])
        except Exception:
            esp_last = None

        if not dia_ref or baseline_esp is None or esp_last is None:
            return None

        return {"dia_ref": dia_ref, "baseline_esp": baseline_esp, "esp_last": esp_last}
    except Exception:
        return None


def get_machine(machine_id: str):
    # ✅ normaliza sempre
    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        machine_id = "maquina01"

    if machine_id not in machine_data:
        agora = now_bahia()

        # ✅ Fonte da verdade pós-deploy: baseline_diario (se existir)
        st = _load_baseline_diario_state(machine_id)
        if st:
            ultimo_dia = st["dia_ref"]
            baseline_diario = st["baseline_esp"]
            esp_absoluto = st["esp_last"]
            bd_dia_ref = st["dia_ref"]
            bd_esp_last = st["esp_last"]
        else:
            ultimo_dia = dia_operacional_ref_str(agora)
            baseline_diario = 0
            esp_absoluto = 0
            bd_dia_ref = None
            bd_esp_last = None

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

            # ✅ nasce do DB quando possível (evita ancorar baseline em 0 no status)
            "esp_absoluto": esp_absoluto,
            "baseline_diario": baseline_diario,
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

            # ✅ string YYYY-MM-DD
            "ultimo_dia": ultimo_dia,
            "reset_executado_hoje": False,

            # ✅ cache do baseline diário (machine_calc usa isso)
            "_bd_dia_ref": bd_dia_ref,
            "_bd_esp_last": bd_esp_last,
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
