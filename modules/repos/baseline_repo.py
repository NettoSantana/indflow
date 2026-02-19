# modules/repos/baseline_repo.py

from modules.db_indflow import get_db
from modules.machine_calc import now_bahia, dia_operacional_ref_str


# ============================================================
# HELPERS
# ============================================================

def _split_scoped_machine_id(machine_id: str):
    """
    Suporta compatibilidade com o formato antigo "cliente_id::machine_id".
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
# BASELINE DIÁRIO (DIA OPERACIONAL — vira às 23:59)
# ============================================================

def ensure_baseline_diario_table():
    conn = get_db()
    cur = conn.cursor()

    # Tabela (com cliente_id)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS baseline_diario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id TEXT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,
            baseline_esp INTEGER NOT NULL,
            esp_last INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # Migração leve (se banco antigo já existir sem cliente_id)
    if not _has_column(conn, "baseline_diario", "cliente_id"):
        try:
            cur.execute("ALTER TABLE baseline_diario ADD COLUMN cliente_id TEXT")
        except Exception:
            pass

    # Índices:
    # - Remove o índice antigo (se existir), pois ele impede multi-tenant (colide machine_id)
    try:
        cur.execute("DROP INDEX IF EXISTS ux_baseline_diario")
    except Exception:
        pass

    # - Novo índice multi-tenant
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario_cliente
        ON baseline_diario(cliente_id, machine_id, dia_ref)
    """)

    # - Compat: garante unicidade do legado (linhas com cliente_id NULL)
    #   (SQLite suporta índice parcial)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario_legacy
        ON baseline_diario(machine_id, dia_ref)
        WHERE cliente_id IS NULL
    """)

    conn.commit()
    conn.close()


def persistir_baseline_diario(machine_id: str, esp_abs: int):
    """
    Usado em reset manual e virada de dia operacional.
    Multi-tenant nativo:
      - se machine_id vier como "cliente::maquina" => separa e grava cliente_id + machine_id
      - se não vier => grava como legado (cliente_id NULL)
    """
    cid, mid = _split_scoped_machine_id(machine_id)
    if not mid:
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

        if cid:
            cur.execute("""
                INSERT INTO baseline_diario
                    (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cliente_id, machine_id, dia_ref)
                DO UPDATE SET
                    baseline_esp=excluded.baseline_esp,
                    esp_last=excluded.esp_last,
                    updated_at=excluded.updated_at
            """, (
                cid,
                mid,
                dia_ref,
                esp_abs,
                esp_abs,
                agora.isoformat()
            ))
        else:
            # legado: cliente_id NULL (usa índice parcial)
            cur.execute("""
                INSERT INTO baseline_diario
                    (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
                VALUES (NULL, ?, ?, ?, ?, ?)
                ON CONFLICT(machine_id, dia_ref)
                DO UPDATE SET
                    baseline_esp=excluded.baseline_esp,
                    esp_last=excluded.esp_last,
                    updated_at=excluded.updated_at
            """, (
                mid,
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

    Multi-tenant nativo:
      - se machine_id vier "cliente::maquina" => separa e consulta por cliente_id+machine_id
      - se não vier => consulta legado (cliente_id IS NULL)
    """
    cid, mid = _split_scoped_machine_id(machine_id)
    if not mid:
        return

    agora = now_bahia()
    dia_ref = dia_operacional_ref_str(agora)

    try:
        esp_abs = int(m.get("esp_absoluto", 0) or 0)
    except Exception:
        esp_abs = 0

    # micro-cache para evitar SQL repetido
    cache_key = f"{cid or 'legacy'}::{mid}"
    if (
        m.get("_bd_dia_ref") == dia_ref and
        m.get("_bd_esp_last") == esp_abs and
        m.get("_bd_key") == cache_key and
        isinstance(m.get("baseline_diario"), int)
    ):
        return

    try:
        conn = get_db()
        ensure_baseline_diario_table()
        cur = conn.cursor()

        if cid:
            cur.execute("""
                SELECT baseline_esp
                FROM baseline_diario
                WHERE cliente_id=? AND machine_id=? AND dia_ref=?
                LIMIT 1
            """, (cid, mid, dia_ref))
        else:
            cur.execute("""
                SELECT baseline_esp
                FROM baseline_diario
                WHERE cliente_id IS NULL AND machine_id=? AND dia_ref=?
                LIMIT 1
            """, (mid, dia_ref))

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

        if cid:
            cur.execute("""
                INSERT INTO baseline_diario
                    (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cliente_id, machine_id, dia_ref)
                DO UPDATE SET
                    baseline_esp=excluded.baseline_esp,
                    esp_last=excluded.esp_last,
                    updated_at=excluded.updated_at
            """, (
                cid,
                mid,
                dia_ref,
                baseline,
                esp_abs,
                agora.isoformat()
            ))
        else:
            cur.execute("""
                INSERT INTO baseline_diario
                    (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
                VALUES (NULL, ?, ?, ?, ?, ?)
                ON CONFLICT(machine_id, dia_ref)
                DO UPDATE SET
                    baseline_esp=excluded.baseline_esp,
                    esp_last=excluded.esp_last,
                    updated_at=excluded.updated_at
            """, (
                mid,
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
        m["_bd_key"] = cache_key

    except Exception:
        # fallback seguro
        if "baseline_diario" not in m:
            m["baseline_diario"] = esp_abs
