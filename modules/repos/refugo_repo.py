# modules/repos/refugo_repo.py

from modules.db_indflow import get_db


def ensure_refugo_table():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS refugo_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,
            hora_dia INTEGER NOT NULL,
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


def load_refugo_24(machine_id: str, dia_ref: str):
    """
    Retorna lista 24 (hora do dia 0..23) com refugo.
    """
    out = [0] * 24
    machine_id = (machine_id or "").strip().lower()

    try:
        ensure_refugo_table()

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


def upsert_refugo(machine_id: str, dia_ref: str, hora_dia: int, refugo: int, updated_at_iso: str):
    """
    Upsert por machine_id + dia_ref + hora_dia
    """
    machine_id = (machine_id or "").strip().lower()
    try:
        ensure_refugo_table()

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO refugo_horaria (machine_id, dia_ref, hora_dia, refugo, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, dia_ref, hora_dia)
            DO UPDATE SET
                refugo=excluded.refugo,
                updated_at=excluded.updated_at
        """, (machine_id, dia_ref, int(hora_dia), int(refugo), updated_at_iso))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False
