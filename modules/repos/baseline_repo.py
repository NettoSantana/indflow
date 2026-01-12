# modules/repos/baseline_repo.py

from datetime import timedelta
from modules.db_indflow import get_db
from modules.machine_calc import now_bahia, dia_operacional_ref_str


# ============================================================
# BASELINE DIÁRIO (DIA OPERACIONAL — vira às 23:59)
# ============================================================

def ensure_baseline_diario_table():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS baseline_diario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,
            baseline_esp INTEGER NOT NULL,
            esp_last INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario
        ON baseline_diario(machine_id, dia_ref)
    """)
    conn.commit()
    conn.close()


def persistir_baseline_diario(machine_id: str, esp_abs: int):
    """
    Usado em reset manual e virada de dia operacional.
    """
    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        return

    try:
        esp_abs = int(esp_abs or 0)
    except Exception:
        esp_abs = 0

    agora = now_bahia()
    dia_ref = dia_operacional_ref_str(agora)

    try:
        conn = get_db()
        ensure_baseline_diario_table()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO baseline_diario
                (machine_id, dia_ref, baseline_esp, esp_last, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, dia_ref)
            DO UPDATE SET
                baseline_esp=excluded.baseline_esp,
                esp_last=excluded.esp_last,
                updated_at=excluded.updated_at
        """, (
            machine_id,
            dia_ref,
            esp_abs,
            esp_abs,
            agora.isoformat()
        ))

        conn.commit()
        conn.close()
    except Exception:
        pass


def carregar_baseline_diario(m: dict, machine_id: str):
    """
    Carrega ou cria baseline diário REAL no SQLite.

    Regras:
    - dia_ref = dia operacional (vira às 23:59)
    - se não existir → baseline = esp_absoluto atual
    - se esp_absoluto diminuir → reancora baseline
    """

    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        return

    agora = now_bahia()
    dia_ref = dia_operacional_ref_str(agora)

    try:
        esp_abs = int(m.get("esp_absoluto", 0) or 0)
    except Exception:
        esp_abs = 0

    # micro-cache para evitar SQL repetido
    if (
        m.get("_bd_dia_ref") == dia_ref and
        m.get("_bd_esp_last") == esp_abs and
        isinstance(m.get("baseline_diario"), int)
    ):
        return

    try:
        conn = get_db()
        ensure_baseline_diario_table()
        cur = conn.cursor()

        cur.execute("""
            SELECT baseline_esp
            FROM baseline_diario
            WHERE machine_id=? AND dia_ref=?
            LIMIT 1
        """, (machine_id, dia_ref))

        row = cur.fetchone()

        if row and row[0] is not None:
            try:
                baseline = int(row[0])
            except Exception:
                baseline = esp_abs
        else:
            baseline = esp_abs

        # ESP voltou → reancora baseline
        if esp_abs < baseline:
            baseline = esp_abs

        cur.execute("""
            INSERT INTO baseline_diario
                (machine_id, dia_ref, baseline_esp, esp_last, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, dia_ref)
            DO UPDATE SET
                baseline_esp=excluded.baseline_esp,
                esp_last=excluded.esp_last,
                updated_at=excluded.updated_at
        """, (
            machine_id,
            dia_ref,
            baseline,
            esp_abs,
            agora.isoformat()
        ))

        conn.commit()
        conn.close()

        # aplica no estado em memória
        m["baseline_diario"] = int(baseline)
        m["_bd_dia_ref"] = dia_ref
        m["_bd_esp_last"] = esp_abs

    except Exception:
        # fallback seguro
        if "baseline_diario" not in m:
            m["baseline_diario"] = esp_abs
