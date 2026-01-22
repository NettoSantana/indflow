# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\repos\nao_programado_horaria_repo.py
# Último recode: 2026-01-22 07:40 (America/Bahia)
# Motivo: Corrigir NP horaria que nao atualiza apesar de upsert/load "OK": detectar schema antigo e migrar tabela nao_programado_horaria automaticamente, evitando falha silenciosa e garantindo coluna produzido/hora_dia.

from __future__ import annotations

from typing import List, Optional, Tuple


# ============================================================
# Import do get_db (padrão do projeto) — com fallback
#   - Preferência: import relativo (repos -> modules)
#   - Se não encontrar, tenta import absoluto
# ============================================================
try:
    # modules/repos -> modules
    from ..db_indflow import get_db  # type: ignore
except Exception:
    try:
        from modules.db_indflow import get_db  # type: ignore
    except Exception:
        get_db = None  # type: ignore


# ============================================================
# Schema
# ============================================================
DDL_NAO_PROGRAMADO_HORARIA = """
CREATE TABLE IF NOT EXISTS nao_programado_horaria (
    machine_id   TEXT    NOT NULL,
    data_ref     TEXT    NOT NULL,   -- dia operacional (YYYY-MM-DD)
    hora_dia     INTEGER NOT NULL,   -- 0..23 (hora do dia)
    produzido    INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT,
    PRIMARY KEY (machine_id, data_ref, hora_dia)
);
"""


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _table_columns(conn, table_name: str) -> List[str]:
    """
    Lista colunas da tabela via PRAGMA table_info.
    """
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        cols = []
        for row in cur.fetchall():
            # row: (cid, name, type, notnull, dflt_value, pk)
            try:
                cols.append(str(row[1]))
            except Exception:
                pass
        return cols
    except Exception:
        return []


def _pick_first_existing(cols: List[str], candidates: List[str]) -> Optional[str]:
    s = {c.lower(): c for c in cols}
    for cand in candidates:
        key = cand.lower()
        if key in s:
            return s[key]
    return None


def _migrate_nao_programado_horaria_if_needed(conn) -> None:
    """
    Se a tabela existir com schema antigo (sem colunas esperadas), migra para o schema atual.

    A causa do problema observado (upsert/load "OK" mas np_por_hora_24 nao muda) normalmente e:
    - tabela ja existia com outro nome de coluna (ex: "producao" ao inves de "produzido")
    - ou hora_dia como TEXT / coluna ausente
    - e o repo engole excecoes, entao nao aparece erro no service
    """
    cols = _table_columns(conn, "nao_programado_horaria")
    if not cols:
        return

    required = {"machine_id", "data_ref", "hora_dia", "produzido", "updated_at"}
    cols_lower = {c.lower() for c in cols}

    if required.issubset(cols_lower):
        return

    src_produzido = _pick_first_existing(
        cols,
        ["produzido", "producao", "valor", "qtd", "quantidade", "np_produzido"],
    )
    src_updated_at = _pick_first_existing(
        cols,
        ["updated_at", "atualizado_em", "updated", "updatedat"],
    )

    # Se nao achar coluna de produzido, copia como 0 (nao quebra)
    if not src_produzido:
        src_produzido = None

    if not src_updated_at:
        src_updated_at = None

    # Migra: renomeia tabela antiga, cria nova no schema correto, copia dados e dropa a antiga
    try:
        conn.execute("ALTER TABLE nao_programado_horaria RENAME TO nao_programado_horaria_old")
    except Exception:
        # Se falhar renome, nao tenta prosseguir
        return

    try:
        # cria nova tabela no schema correto (sem IF NOT EXISTS)
        conn.execute("""
        CREATE TABLE nao_programado_horaria (
            machine_id   TEXT    NOT NULL,
            data_ref     TEXT    NOT NULL,
            hora_dia     INTEGER NOT NULL,
            produzido    INTEGER NOT NULL DEFAULT 0,
            updated_at   TEXT,
            PRIMARY KEY (machine_id, data_ref, hora_dia)
        );
        """)

        if src_produzido and src_updated_at:
            conn.execute(
                f"""
                INSERT INTO nao_programado_horaria (machine_id, data_ref, hora_dia, produzido, updated_at)
                SELECT
                    machine_id,
                    data_ref,
                    CAST(hora_dia AS INTEGER),
                    COALESCE(CAST({src_produzido} AS INTEGER), 0),
                    {src_updated_at}
                FROM nao_programado_horaria_old
                """
            )
        elif src_produzido and not src_updated_at:
            conn.execute(
                f"""
                INSERT INTO nao_programado_horaria (machine_id, data_ref, hora_dia, produzido, updated_at)
                SELECT
                    machine_id,
                    data_ref,
                    CAST(hora_dia AS INTEGER),
                    COALESCE(CAST({src_produzido} AS INTEGER), 0),
                    NULL
                FROM nao_programado_horaria_old
                """
            )
        elif (not src_produzido) and src_updated_at:
            conn.execute(
                f"""
                INSERT INTO nao_programado_horaria (machine_id, data_ref, hora_dia, produzido, updated_at)
                SELECT
                    machine_id,
                    data_ref,
                    CAST(hora_dia AS INTEGER),
                    0,
                    {src_updated_at}
                FROM nao_programado_horaria_old
                """
            )
        else:
            conn.execute(
                """
                INSERT INTO nao_programado_horaria (machine_id, data_ref, hora_dia, produzido, updated_at)
                SELECT
                    machine_id,
                    data_ref,
                    CAST(hora_dia AS INTEGER),
                    0,
                    NULL
                FROM nao_programado_horaria_old
                """
            )

        # remove tabela antiga
        conn.execute("DROP TABLE IF EXISTS nao_programado_horaria_old")
        conn.commit()
    except Exception:
        # Se der ruim no meio, tenta pelo menos nao travar o sistema:
        # - mantem a tabela antiga para nao perder dados
        try:
            conn.execute("DROP TABLE IF EXISTS nao_programado_horaria")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE nao_programado_horaria_old RENAME TO nao_programado_horaria")
        except Exception:
            pass
        conn.commit()
        return


def ensure_table(conn) -> None:
    """
    Garante que a tabela nao_programado_horaria existe.
    Tambem valida/migra schema antigo para evitar falha silenciosa no upsert.
    """
    conn.execute(DDL_NAO_PROGRAMADO_HORARIA)
    conn.commit()

    # Se ja existia com schema antigo, migra para o schema esperado
    _migrate_nao_programado_horaria_if_needed(conn)


def upsert_delta(
    conn,
    machine_id: str,
    data_ref: str,
    hora_dia: int,
    delta: int,
    updated_at: str,
) -> None:
    """
    Soma o delta na linha (machine_id, data_ref, hora_dia).

    Regras:
      - delta <= 0 => ignora
      - hora_dia precisa estar em 0..23
      - machine_id e data_ref não podem ser vazios
    """
    mid = (machine_id or "").strip()
    dr = (data_ref or "").strip()
    hd = _safe_int(hora_dia, -1)
    d = _safe_int(delta, 0)

    if not mid or not dr:
        return
    if hd < 0 or hd > 23:
        return
    if d <= 0:
        return

    ensure_table(conn)

    # UPSERT moderno (SQLite 3.24+)
    try:
        conn.execute(
            """
            INSERT INTO nao_programado_horaria (machine_id, data_ref, hora_dia, produzido, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, data_ref, hora_dia)
            DO UPDATE SET
                produzido = produzido + excluded.produzido,
                updated_at = excluded.updated_at
            """,
            (mid, dr, hd, d, updated_at),
        )
        conn.commit()
        return
    except Exception:
        pass

    # Fallback (SQLite antigo)
    try:
        cur = conn.execute(
            """
            SELECT produzido
            FROM nao_programado_horaria
            WHERE machine_id = ? AND data_ref = ? AND hora_dia = ?
            LIMIT 1
            """,
            (mid, dr, hd),
        )
        row = cur.fetchone()
        if row:
            novo = _safe_int(row[0], 0) + d
            conn.execute(
                """
                UPDATE nao_programado_horaria
                SET produzido = ?, updated_at = ?
                WHERE machine_id = ? AND data_ref = ? AND hora_dia = ?
                """,
                (novo, updated_at, mid, dr, hd),
            )
        else:
            conn.execute(
                """
                INSERT INTO nao_programado_horaria (machine_id, data_ref, hora_dia, produzido, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (mid, dr, hd, d, updated_at),
            )
        conn.commit()
    except Exception:
        # Não levanta erro para não quebrar o update da máquina
        return


def load_np_por_hora_24(conn, machine_id: str, data_ref: str) -> List[int]:
    """
    Retorna array 24 posições (0..23) com produção NP por hora.
    """
    mid = (machine_id or "").strip()
    dr = (data_ref or "").strip()

    arr = [0] * 24
    if not mid or not dr:
        return arr

    ensure_table(conn)

    try:
        cur = conn.execute(
            """
            SELECT hora_dia, produzido
            FROM nao_programado_horaria
            WHERE machine_id = ? AND data_ref = ?
            """,
            (mid, dr),
        )
        for hora, produzido in cur.fetchall():
            h = _safe_int(hora, -1)
            if 0 <= h <= 23:
                arr[h] = _safe_int(produzido, 0)
    except Exception:
        return arr

    return arr


# ============================================================
# Conveniência: abre conexão via get_db (se existir)
# ============================================================
def upsert_delta_db(
    machine_id: str,
    data_ref: str,
    hora_dia: int,
    delta: int,
    updated_at: str,
) -> None:
    """
    Versão que usa get_db() padrão do projeto.
    """
    if get_db is None:
        raise RuntimeError("get_db() não encontrado. Use upsert_delta(conn, ...) passando a conexão.")

    conn = get_db()
    try:
        upsert_delta(conn, machine_id, data_ref, hora_dia, delta, updated_at)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def load_np_por_hora_24_db(machine_id: str, data_ref: str) -> List[int]:
    """
    Versão que usa get_db() padrão do projeto.
    """
    if get_db is None:
        raise RuntimeError("get_db() não encontrado. Use load_np_por_hora_24(conn, ...) passando a conexão.")

    conn = get_db()
    try:
        return load_np_por_hora_24(conn, machine_id, data_ref)
    finally:
        try:
            conn.close()
        except Exception:
            pass
