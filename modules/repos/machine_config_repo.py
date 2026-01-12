# modules/repos/machine_config_repo.py

import json
from datetime import datetime
from modules.db_indflow import get_db


def ensure_machine_config_table():
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


def upsert_machine_config(machine_id: str, m: dict):
    """
    Persiste a config calculada no SQLite.
    Espera em m:
      - meta_turno
      - turno_inicio, turno_fim
      - rampa_percentual
      - horas_turno (list)
      - meta_por_hora (list)
    """
    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        return False

    try:
        ensure_machine_config_table()

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
        return True
    except Exception:
        return False
