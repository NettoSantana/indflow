# modules/repos/producao_horaria_repo.py

from modules.db_indflow import get_db
from modules.machine_calc import now_bahia


# ============================================================
# PRODUÇÃO HORÁRIA (PERSISTENTE) - SQLITE
# ============================================================

def ensure_producao_horaria_table():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_producao_horaria
        ON producao_horaria(machine_id, data_ref, hora_idx)
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
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO producao_horaria
            (machine_id, data_ref, hora_idx, baseline_esp, esp_last, produzido, meta, percentual, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, data_ref, hora_idx)
            DO UPDATE SET
                baseline_esp=excluded.baseline_esp,
                esp_last=excluded.esp_last,
                produzido=excluded.produzido,
                meta=excluded.meta,
                percentual=excluded.percentual,
                updated_at=excluded.updated_at
        """, (
            (machine_id or "").strip().lower(),
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
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT baseline_esp
            FROM producao_horaria
            WHERE machine_id=? AND data_ref=? AND hora_idx=?
            LIMIT 1
        """, (
            (machine_id or "").strip().lower(),
            str(data_ref),
            int(hora_idx)
        ))
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
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT hora_idx, produzido
            FROM producao_horaria
            WHERE machine_id=? AND data_ref=?
        """, (
            (machine_id or "").strip().lower(),
            str(data_ref)
        ))

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
