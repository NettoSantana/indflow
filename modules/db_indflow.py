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

    conn.commit()
    conn.close()
