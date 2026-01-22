# Caminho: modules/repos/refugo_repo.py
# Último recode: 2026-01-22 20:30 (America/Bahia)
# Motivo: Corrigir falha de salvamento de refugo por conflito de chave (cliente_id x legacy)

from modules.db_indflow import get_db


# ============================================================
# HELPERS
# ============================================================

def _split_scoped_machine_id(machine_id: str):
    """
    Aceita:
      - cliente_id::machine_id
      - machine_id simples (legado)
    Retorna (cliente_id|None, machine_id)
    """
    s = (machine_id or "").strip().lower()
    if not s:
        return (None, "")

    if "::" in s:
        cid, mid = s.split("::", 1)
        cid = cid.strip()
        mid = mid.strip()
        if cid and mid:
            return (cid, mid)

    return (None, s)


def _has_column(conn, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def _dedupe_keep_latest(conn):
    """
    Remove duplicados mantendo o registro mais recente (maior id)
    """
    conn.execute("""
        DELETE FROM refugo_horaria
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM refugo_horaria
            GROUP BY
              COALESCE(cliente_id,'__NULL__'),
              machine_id,
              dia_ref,
              hora_dia
        )
    """)


# ============================================================
# TABELA / ÍNDICES
# ============================================================

def ensure_refugo_table():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS refugo_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id TEXT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,
            hora_dia INTEGER NOT NULL,
            refugo INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    _dedupe_keep_latest(conn)

    cur.execute("DROP INDEX IF EXISTS ux_refugo_horaria")

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_refugo_multi
        ON refugo_horaria(cliente_id, machine_id, dia_ref, hora_dia)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_refugo_legacy
        ON refugo_horaria(machine_id, dia_ref, hora_dia)
        WHERE cliente_id IS NULL
    """)

    conn.commit()
    conn.close()


# ============================================================
# LOAD
# ============================================================

def load_refugo_24(machine_id: str, dia_ref: str):
    out = [0] * 24
    cid, mid = _split_scoped_machine_id(machine_id)
    if not mid:
        return out

    ensure_refugo_table()
    conn = get_db()
    cur = conn.cursor()

    if cid:
        cur.execute("""
            SELECT hora_dia, refugo
            FROM refugo_horaria
            WHERE cliente_id=? AND machine_id=? AND dia_ref=?
        """, (cid, mid, dia_ref))
    else:
        cur.execute("""
            SELECT hora_dia, refugo
            FROM refugo_horaria
            WHERE cliente_id IS NULL AND machine_id=? AND dia_ref=?
        """, (mid, dia_ref))

    for h, v in cur.fetchall():
        if 0 <= h < 24:
            out[h] = max(0, int(v))

    conn.close()
    return out


# ============================================================
# UPSERT (CORRIGIDO)
# ============================================================

def upsert_refugo(machine_id: str, dia_ref: str, hora_dia: int, refugo: int, updated_at_iso: str):
    cid, mid = _split_scoped_machine_id(machine_id)
    if not mid:
        return False

    try:
        ensure_refugo_table()
        conn = get_db()
        cur = conn.cursor()

        # SEMPRE usa cliente_id quando existir
        cur.execute("""
            INSERT INTO refugo_horaria (
                cliente_id, machine_id, dia_ref, hora_dia, refugo, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(cliente_id, machine_id, dia_ref, hora_dia)
            DO UPDATE SET
                refugo=excluded.refugo,
                updated_at=excluded.updated_at
        """, (
            cid,
            mid,
            dia_ref,
            int(hora_dia),
            int(refugo),
            updated_at_iso
        ))

        conn.commit()
        conn.close()
        return True

    except Exception as e:
        return False
