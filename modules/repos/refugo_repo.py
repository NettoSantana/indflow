# modules/repos/refugo_repo.py

from modules.db_indflow import get_db


# ============================================================
# HELPERS
# ============================================================

def _split_scoped_machine_id(machine_id: str):
    """
    Compatibilidade com formato "cliente_id::machine_id".
    Retorna (cliente_id, raw_machine_id).
    Se não estiver no formato, retorna (None, machine_id_normalizado).
    """
    s = (machine_id or "").strip().lower()
    if not s:
        return (None, "")

    if "::" in s:
        cid, mid = s.split("::", 1)
        cid = (cid or "").strip()
        mid = (mid or "").strip()
        if cid and mid:
            return (cid, mid)

    return (None, s)


def _has_column(conn, table: str, col: str) -> bool:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        return col in cols
    except Exception:
        return False


def _dedupe_keep_latest(conn, table: str):
    """
    Remove duplicados mantendo o maior id (mais recente) por:
      - cliente_id (ou NULL)
      - machine_id
      - dia_ref
      - hora_dia
    """
    try:
        conn.execute(f"""
            DELETE FROM {table}
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM {table}
                GROUP BY COALESCE(cliente_id,'__NULL__'), machine_id, dia_ref, hora_dia
            )
        """)
    except Exception:
        pass


# ============================================================
# TABELA / ÍNDICES
# ============================================================

def ensure_refugo_table():
    conn = get_db()
    cur = conn.cursor()

    # Tabela base (legado) + migração leve
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

    # Migração: adiciona cliente_id
    if not _has_column(conn, "refugo_horaria", "cliente_id"):
        try:
            cur.execute("ALTER TABLE refugo_horaria ADD COLUMN cliente_id TEXT")
        except Exception:
            pass

    # Dedup antes dos UNIQUE (evita crash ao criar índices)
    _dedupe_keep_latest(conn, "refugo_horaria")

    # Remove índice antigo que colide quando começarmos a separar por cliente
    try:
        cur.execute("DROP INDEX IF EXISTS ux_refugo_horaria")
    except Exception:
        pass

    # UNIQUE multi-tenant
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_refugo_horaria_cliente
        ON refugo_horaria(cliente_id, machine_id, dia_ref, hora_dia)
    """)

    # UNIQUE legado parcial (somente registros antigos com cliente_id NULL)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_refugo_horaria_legacy
        ON refugo_horaria(machine_id, dia_ref, hora_dia)
        WHERE cliente_id IS NULL
    """)

    # Índice para busca por cliente
    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_refugo_horaria_cliente_id
        ON refugo_horaria(cliente_id)
    """)

    conn.commit()
    conn.close()


# ============================================================
# API
# ============================================================

def load_refugo_24(machine_id: str, dia_ref: str):
    """
    Retorna lista 24 (hora do dia 0..23) com refugo.

    Multi-tenant nativo:
      - se machine_id vier "cliente::maquina" -> filtra por cliente_id + machine_id
      - senão -> legado (cliente_id IS NULL)
    """
    out = [0] * 24
    cid, mid = _split_scoped_machine_id(machine_id)
    if not mid:
        return out

    try:
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
    Upsert por:
      - multi-tenant: (cliente_id, machine_id, dia_ref, hora_dia)
      - legado: (machine_id, dia_ref, hora_dia) quando cliente_id IS NULL
    """
    cid, mid = _split_scoped_machine_id(machine_id)
    if not mid:
        return False

    try:
        ensure_refugo_table()

        conn = get_db()
        cur = conn.cursor()

        if cid:
            cur.execute("""
                INSERT INTO refugo_horaria (cliente_id, machine_id, dia_ref, hora_dia, refugo, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cliente_id, machine_id, dia_ref, hora_dia)
                DO UPDATE SET
                    refugo=excluded.refugo,
                    updated_at=excluded.updated_at
            """, (cid, mid, dia_ref, int(hora_dia), int(refugo), updated_at_iso))
        else:
            # FIX MINIMO:
            # Evita ON CONFLICT em indice parcial (WHERE cliente_id IS NULL).
            # Faz UPDATE primeiro; se nao atualizou nada, faz INSERT.
            cur.execute("""
                UPDATE refugo_horaria
                   SET refugo=?,
                       updated_at=?
                 WHERE cliente_id IS NULL
                   AND machine_id=?
                   AND dia_ref=?
                   AND hora_dia=?
            """, (int(refugo), updated_at_iso, mid, dia_ref, int(hora_dia)))

            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO refugo_horaria (cliente_id, machine_id, dia_ref, hora_dia, refugo, updated_at)
                    VALUES (NULL, ?, ?, ?, ?, ?)
                """, (mid, dia_ref, int(hora_dia), int(refugo), updated_at_iso))

        conn.commit()
        conn.close()
        return True
    except Exception:
        return False
