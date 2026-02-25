# modules/repos/producao_horaria_repo.py
# LAST_RECODE: 2026-02-25 14:35 America/Bahia
# MOTIVO: Corrigir divergencia de cliente_id na persistencia de producao_horaria: ignorar cid de machine_id scoped e resolver cliente_id consistente por consulta ao DB antes do upsert/leitura.

from modules.db_indflow import get_db
from modules.machine_calc import now_bahia


# ============================================================
# HELPERS
# ============================================================
_CLIENTE_CACHE = {}  # mid_lower -> cliente_id or None
_SCHEMA_CACHE = None  # list of (table, machine_col, cliente_col)

def _resolve_cliente_id(conn, mid: str, fallback_cid: str | None):
    """
    Resolve cliente_id consistente para um machine_id (mid) consultando o SQLite.
    - Tenta tabelas que tenham colunas de machine_id e cliente_id.
    - Cacheia o schema encontrado para evitar custo em chamadas repetidas.
    - Se nao encontrar, usa fallback_cid (quando veio no formato scoped) ou None.
    """
    mid_norm = (mid or "").strip().lower()
    if not mid_norm:
        return fallback_cid or None

    if mid_norm in _CLIENTE_CACHE:
        return _CLIENTE_CACHE[mid_norm] if _CLIENTE_CACHE[mid_norm] else (fallback_cid or None)

    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is None:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall() or []
        schema = []
        for (tname,) in tables:
            try:
                cols = conn.execute(f"PRAGMA table_info({tname})").fetchall() or []
                colnames = [c[1] for c in cols]
                if "cliente_id" not in colnames:
                    continue

                machine_col = None
                for cand in ("machine_id", "maquina_id", "id_maquina", "machine"):
                    if cand in colnames:
                        machine_col = cand
                        break

                if not machine_col:
                    continue

                schema.append((tname, machine_col, "cliente_id"))
            except Exception:
                continue

        # prioriza tabelas mais provaveis
        def _score(item):
            tname = item[0].lower()
            score = 0
            for key in ("config", "maquina", "machine", "device", "vinc", "binding", "producao"):
                if key in tname:
                    score += 1
            return -score, tname

        schema.sort(key=_score)
        _SCHEMA_CACHE = schema

    resolved = None
    for (tname, machine_col, cliente_col) in (_SCHEMA_CACHE or [])[:30]:
        try:
            row = conn.execute(
                f"SELECT {cliente_col} FROM {tname} WHERE {machine_col} = ? LIMIT 1",
                (mid_norm,),
            ).fetchone()
            if row and row[0]:
                resolved = str(row[0]).strip()
                break
        except Exception:
            continue

    # fallback: tenta match sem lower (algumas tabelas podem guardar case)
    if not resolved:
        for (tname, machine_col, cliente_col) in (_SCHEMA_CACHE or [])[:30]:
            try:
                row = conn.execute(
                    f"SELECT {cliente_col} FROM {tname} WHERE {machine_col} = ? LIMIT 1",
                    (mid,),
                ).fetchone()
                if row and row[0]:
                    resolved = str(row[0]).strip()
                    break
            except Exception:
                continue

    _CLIENTE_CACHE[mid_norm] = resolved or ""
    return resolved or (fallback_cid or None)


def _normalize_machine_cliente(conn, machine_id: str):
    """
    Normaliza machine_id e resolve cliente_id.
    - Sempre retorna (cliente_id_resolvido_ou_fallback, raw_mid_normalizado).
    """
    cid_scoped, mid = _split_scoped_machine_id(machine_id)
    if not mid:
        return (None, "")
    cid = _resolve_cliente_id(conn, mid, cid_scoped)
    return (cid, mid)


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

        conn = get_db()
        cur = conn.cursor()

        cid, mid = _normalize_machine_cliente(conn, machine_id)
        if not mid:
            try:
                conn.close()
            except Exception:
                pass
            return False

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