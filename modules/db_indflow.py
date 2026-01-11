# modules/db_indflow.py
import os
import sqlite3
from pathlib import Path

def _default_db_path() -> str:
    """
    Prioridade:
    1) INDFLOW_DB_PATH (Railway/DEV)
    2) /data/indflow.db (quando existir volume montado em /data)
    3) ./indflow.db (local)
    """
    env_path = os.getenv("INDFLOW_DB_PATH", "").strip()
    if env_path:
        return env_path

    if Path("/data").exists():
        return "/data/indflow.db"

    return "indflow.db"

DB_PATH = _default_db_path()

def get_db():
    # check_same_thread=False ajuda quando Waitress/Flask usa threads
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    # garante pasta do DB (quando for path tipo /data/indflow.db)
    db_file = Path(DB_PATH)
    if db_file.parent and str(db_file.parent) not in ("", "."):
        db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = get_db()
    cur = conn.cursor()

    # ============================================
    # 1) HISTÓRICO DIÁRIO (já existia)
    # ============================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_diaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT,
            data TEXT,
            produzido INTEGER,
            meta INTEGER,
            percentual INTEGER
        )
    """)

    # ============================================
    # 2) CONFIG DA MÁQUINA (PERSISTENTE)
    # ============================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS machine_config (
            machine_id TEXT PRIMARY KEY,
            meta_turno INTEGER NOT NULL DEFAULT 0,
            turno_inicio TEXT,
            turno_fim TEXT,
            rampa_percentual INTEGER NOT NULL DEFAULT 0,
            horas_turno_json TEXT NOT NULL DEFAULT '[]',
            meta_por_hora_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        )
    """)

    # ============================================
    # 3) PRODUÇÃO POR HORA (PERSISTENTE)
    # ============================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            data_ref TEXT NOT NULL,         -- data do início do turno (YYYY-MM-DD)
            hora_idx INTEGER NOT NULL,      -- índice da hora no turno (0..n-1)
            baseline_esp INTEGER NOT NULL,  -- esp_absoluto no início da hora
            esp_last INTEGER NOT NULL,      -- último esp_absoluto visto
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
