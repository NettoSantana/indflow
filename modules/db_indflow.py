# modules/db_indflow.py
import os
import sqlite3
from pathlib import Path


def _is_railway() -> bool:
    """
    Detecta execução no Railway.
    Basta existir qualquer uma dessas envs.
    """
    keys = [
        "RAILWAY_ENVIRONMENT",
        "RAILWAY_PROJECT_ID",
        "RAILWAY_SERVICE_ID",
        "RAILWAY_STATIC_URL",
    ]
    return any((os.getenv(k) or "").strip() for k in keys)


def _default_db_path() -> str:
    """
    Prioridade:
    1) INDFLOW_DB_PATH (override explícito)
    2) Railway: /data/indflow.db (volume)  -> força persistência
    3) /data/indflow.db (quando existir /data)
    4) ./indflow.db (local)
    """
    env_path = os.getenv("INDFLOW_DB_PATH", "").strip()
    if env_path:
        return env_path

    # No Railway, queremos SEMPRE persistir em /data
    if _is_railway():
        return "/data/indflow.db"

    if Path("/data").exists():
        return "/data/indflow.db"

    return "indflow.db"


def _ensure_db_dir(db_path: str) -> None:
    db_file = Path(db_path)
    if db_file.parent and str(db_file.parent) not in ("", "."):
        db_file.parent.mkdir(parents=True, exist_ok=True)


def get_db():
    """
    IMPORTANTÍSSIMO:
    - NÃO usar DB_PATH global (evita ficar "congelado" num caminho errado)
    - Resolve o path em runtime, toda vez, garantindo consistência no Railway.
    """
    db_path = _default_db_path()
    _ensure_db_dir(db_path)

    # check_same_thread=False ajuda quando Waitress/Flask usa threads
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    # ============================================================
    # AUTH (V1) — CLIENTES + USUÁRIOS (1 cliente = 1 usuário por enquanto)
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id TEXT PRIMARY KEY,                 -- UUID/slug interno
            nome TEXT NOT NULL,
            api_key_hash TEXT NOT NULL,          -- NUNCA salvar a api_key em texto puro
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id TEXT PRIMARY KEY,                 -- UUID interno
            email TEXT NOT NULL,
            senha_hash TEXT NOT NULL,            -- hash da senha (ex.: pbkdf2/bcrypt)
            cliente_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'admin',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            FOREIGN KEY (cliente_id) REFERENCES clientes(id)
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_usuarios_email
        ON usuarios(email)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_usuarios_cliente_id
        ON usuarios(cliente_id)
    """)

    # ============================================================
    # 0) DEVICES (ESP) — MAC = CPF (device_id)
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,      -- MAC normalizado (sem ":" e "-")
            machine_id TEXT,                 -- vínculo atual (opcional)
            alias TEXT,                      -- apelido (opcional)
            created_at TEXT,                 -- primeira vez visto
            last_seen TEXT                   -- último contato
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_devices_machine_id
        ON devices(machine_id)
    """)

    # ============================================================
    # 1) HISTÓRICO DIÁRIO
    # ============================================================
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

    # ============================================================
    # 2) CONFIG DA MÁQUINA (PERSISTENTE)
    # ============================================================
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

    # ============================================================
    # 3) PRODUÇÃO POR HORA (PERSISTENTE)
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            data_ref TEXT NOT NULL,         -- data do início do turno
            hora_idx INTEGER NOT NULL,      -- índice da hora no turno
            baseline_esp INTEGER NOT NULL,
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

    # ============================================================
    # 4) BASELINE DIÁRIO (DIA OPERACIONAL 23:59)
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS baseline_diario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,            -- dia operacional (vira às 23:59)
            baseline_esp INTEGER NOT NULL,    -- esp_absoluto no início do dia
            esp_last INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario
        ON baseline_diario(machine_id, dia_ref)
    """)

    conn.commit()
    conn.close()
