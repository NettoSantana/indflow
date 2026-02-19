# modules/repos/producao_horaria_repo.py

from modules.db_indflow import get_db
from modules.machine_calc import now_bahia


# ============================================================
# HELPERS
# ============================================================

def _split_scoped_machine_id(machine_id: str):
    """
    Suporta compatibilidade com o formato "cliente_id::machine_id".
    Retorna (cliente_id, raw_machine_id).
    Se não estiver no formato, retorna (None, machine_id_normalizado).
    """
    s = (machine_id or "").strip().lower()
    if not s:
        return (None, "")

    if "::" in s:
        parts = s.split("::", 1)
        cid = (parts[0] or "").strip()
        mid = (parts[1] or "").strip()
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


# ============================================================
# PRODUÇÃO HORÁRIA (PERSISTENTE) - SQLITE
# ============================================================

def ensure_producao_horaria_table():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id TEXT,
            machine_id TEXT NOT NULL,
            data_ref TEXT NOT NULL,         -- dia operacional (YYYY-MM-DD)
            hora_idx INTEGER NOT NULL,      -- índice da hora dentro do turno (0..n-1)
            baseline_esp INTEGER NOT NULL,  -- esp_absoluto no início da hora
            esp_last INTEGER NOT NULL,
            produzido INTEGER NOT NULL,
            meta INTEGER NOT NULL,
            percentual INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # migração leve: banco antigo sem cliente_id
    if not _has_column(conn, "producao_horaria", "cliente_id"):
        try:
            cur.execute("ALTER TABLE producao_horaria ADD COLUMN cliente_id TEXT")
        except Exception:
            pass

    # índice antigo pode impedir multi-tenant (colisão entre clientes)
    try:
        cur.execute("DROP INDEX IF EXISTS ux_producao_horaria")
    except Exception:
        pass

    # multi-tenant nativo
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_producao_horaria_cliente
        ON producao_horaria(cliente_id, machine_id, data_ref, hora_idx)
    """)

    # legado: garante que linhas antigas continuem únicas quando cliente_id IS NULL
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_producao_horaria_legacy
        ON producao_horaria(machine_id, data_ref, hora_idx)
        WHERE cliente_id IS NULL
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_producao_horaria_cliente_id
        ON producao_horaria(cliente_id)
    """)

    conn.commit()
    conn.close()


def upsert_hora(
    machine_id: str,
    data_ref: str,
    hora_idx: int,
    baseline_esp: int,
    esp_last: int,
    produzido: int,
    meta: int,
    percentual: int
):
    try:
        ensure_producao_horaria_table()

        cid, mid = _split_scoped_machine_id(machine_id)
        if not mid:
            return False

        conn = get_db()
        cur = conn.cursor()

        if cid:
            cur.execute("""
                INSERT INTO producao_horaria
                (cliente_id, machine_id, data_ref, hora_idx, baseline_esp, esp_last, produzido, meta, percentual, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cliente_id, machine_id, data_ref, hora_idx)
                DO UPDATE SET
                    baseline_esp=excluded.baseline_esp,
                    esp_last=excluded.esp_last,
                    produzido=excluded.produzido,
                    meta=excluded.meta,
                    percentual=excluded.percentual,
                    updated_at=excluded.updated_at
            """, (
                cid,
                mid,
                str(data_ref),
                int(hora_idx),
                int(baseline_esp),
                int(esp_last),
                int(produzido),
                int(meta),
                int(percentual),
                now_bahia().isoformat()
            ))
        else:
            # legado (cliente_id NULL)
            cur.execute("""
                INSERT INTO producao_horaria
                (cliente_id, machine_id, data_ref, hora_idx, baseline_esp, esp_last, produzido, meta, percentual, updated_at)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(machine_id, data_ref, hora_idx)
                DO UPDATE SET
                    baseline_esp=excluded.baseline_esp,
                    esp_last=excluded.esp_last,
                    produzido=excluded.produzido,
                    meta=excluded.meta,
                    percentual=excluded.percentual,
                    updated_at=excluded.updated_at
            """, (
                mid,
                str(data_ref),
                int(hora_idx),
                int(baseline_esp),
                int(esp_last),
                int(produzido),
                int(meta),
                int(percentual),
                now_bahia().isoformat()
            ))

        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def get_baseline_for_hora(machine_id: str, data_ref: str, hora_idx: int):
    try:
        ensure_producao_horaria_table()

        cid, mid = _split_scoped_machine_id(machine_id)
        if not mid:
            return None

        conn = get_db()
        cur = conn.cursor()

        if cid:
            cur.execute("""
                SELECT baseline_esp
                FROM producao_horaria
                WHERE cliente_id=? AND machine_id=? AND data_ref=? AND hora_idx=?
                LIMIT 1
            """, (cid, mid, str(data_ref), int(hora_idx)))
        else:
            cur.execute("""
                SELECT baseline_esp
                FROM producao_horaria
                WHERE cliente_id IS NULL AND machine_id=? AND data_ref=? AND hora_idx=?
                LIMIT 1
            """, (mid, str(data_ref), int(hora_idx)))

        row = cur.fetchone()
        conn.close()

        if row and row[0] is not None:
            try:
                return int(row[0])
            except Exception:
                return None
        return None
    except Exception:
        return None


def load_producao_por_hora(machine_id: str, data_ref: str, n_horas: int):
    out = [None] * int(n_horas or 0)
    try:
        ensure_producao_horaria_table()

        cid, mid = _split_scoped_machine_id(machine_id)
        if not mid:
            return out

        conn = get_db()
        cur = conn.cursor()

        if cid:
            cur.execute("""
                SELECT hora_idx, produzido
                FROM producao_horaria
                WHERE cliente_id=? AND machine_id=? AND data_ref=?
            """, (cid, mid, str(data_ref)))
        else:
            cur.execute("""
                SELECT hora_idx, produzido
                FROM producao_horaria
                WHERE cliente_id IS NULL AND machine_id=? AND data_ref=?
            """, (mid, str(data_ref)))

        rows = cur.fetchall() or []
        conn.close()

        for r in rows:
            try:
                idx = int(r[0])
                val = int(r[1])
                if 0 <= idx < len(out):
                    out[idx] = val
            except Exception:
                continue
    except Exception:
        pass

    return out
