# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\repos\nao_programado_horaria_repo.py
# Último recode: 2026-01-21 17:34 (America/Bahia)
# Motivo: Isolar persistência do "Não Programado" (hora extra) em um repo: garantir criação de tabela, upsert de delta por hora e leitura np_por_hora_24.

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


def ensure_table(conn) -> None:
    """
    Garante que a tabela nao_programado_horaria existe.
    """
    conn.execute(DDL_NAO_PROGRAMADO_HORARIA)
    conn.commit()


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
