# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\repos\machine_config_repo.py
# Último recode: 2026-01-20 19:20 (America/Bahia)
# Motivo: Persistir configuração por máquina do "Alerta de Parada por Inatividade" (alerta_sem_contagem_seg) no machine_config.

import json
from datetime import datetime
from modules.db_indflow import get_db


def _ensure_column(conn, table: str, col: str, ddl: str) -> None:
    """
    Garante que a coluna exista. Se não existir, cria via ALTER TABLE.
    ddl: string do tipo 'TEXT', 'REAL', etc. (sem o nome da coluna)
    """
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]  # (cid, name, type, notnull, dflt_value, pk)
        if col not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
            conn.commit()
    except Exception:
        # não derruba o sistema por causa de migração
        pass


def ensure_machine_config_table():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS machine_config (
            machine_id TEXT PRIMARY KEY,
            meta_turno INTEGER NOT NULL DEFAULT 0,
            turno_inicio TEXT,
            turno_fim TEXT,
            rampa_percentual INTEGER NOT NULL DEFAULT 0,
            horas_turno_json TEXT NOT NULL DEFAULT '[]',
            meta_por_hora_json TEXT NOT NULL DEFAULT '[]',

            -- ✅ NOVO: persistir unidades e conversão
            unidade_1 TEXT,
            unidade_2 TEXT,
            conv_m_por_pcs REAL,

            -- ✅ NOVO: alerta de parada por falta de contagem (segundos)
            alerta_sem_contagem_seg INTEGER,

            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()

    # ✅ MIGRAÇÃO SEGURA (para bancos já existentes)
    _ensure_column(conn, "machine_config", "unidade_1", "TEXT")
    _ensure_column(conn, "machine_config", "unidade_2", "TEXT")
    _ensure_column(conn, "machine_config", "conv_m_por_pcs", "REAL")

    # ✅ NOVO: migração segura do alerta
    _ensure_column(conn, "machine_config", "alerta_sem_contagem_seg", "INTEGER")

    conn.close()


def upsert_machine_config(machine_id: str, m: dict):
    """
    Persiste a config calculada no SQLite.
    Espera em m:
      - meta_turno
      - turno_inicio, turno_fim
      - rampa_percentual
      - horas_turno (list)
      - meta_por_hora (list)
      - unidade_1, unidade_2 (opcional)
      - conv_m_por_pcs (opcional)
      - alerta_sem_contagem_seg (opcional)  ✅
    """
    machine_id = (machine_id or "").strip().lower()
    if not machine_id:
        return False

    try:
        ensure_machine_config_table()

        conn = get_db()
        cur = conn.cursor()

        # conversão (REAL) - tenta guardar como float se vier válido
        conv = m.get("conv_m_por_pcs", None)
        try:
            conv = float(conv) if conv not in (None, "", "none") else None
        except Exception:
            conv = None

        # ✅ NOVO: alerta de parada por falta de contagem (segundos)
        # saneamento: mínimo 5s, máximo 86400s (24h) pra evitar absurdos
        alerta_sec = m.get("alerta_sem_contagem_seg", None)
        try:
            if alerta_sec in (None, "", "none"):
                alerta_sec = None
            else:
                alerta_sec = int(alerta_sec)
                if alerta_sec < 5:
                    alerta_sec = 5
                if alerta_sec > 86400:
                    alerta_sec = 86400
        except Exception:
            alerta_sec = None

        cur.execute("""
            INSERT INTO machine_config
            (
              machine_id,
              meta_turno,
              turno_inicio,
              turno_fim,
              rampa_percentual,
              horas_turno_json,
              meta_por_hora_json,
              unidade_1,
              unidade_2,
              conv_m_por_pcs,
              alerta_sem_contagem_seg,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(machine_id) DO UPDATE SET
                meta_turno=excluded.meta_turno,
                turno_inicio=excluded.turno_inicio,
                turno_fim=excluded.turno_fim,
                rampa_percentual=excluded.rampa_percentual,
                horas_turno_json=excluded.horas_turno_json,
                meta_por_hora_json=excluded.meta_por_hora_json,
                unidade_1=excluded.unidade_1,
                unidade_2=excluded.unidade_2,
                conv_m_por_pcs=excluded.conv_m_por_pcs,
                alerta_sem_contagem_seg=excluded.alerta_sem_contagem_seg,
                updated_at=excluded.updated_at
        """, (
            machine_id,
            int(m.get("meta_turno") or 0),
            m.get("turno_inicio"),
            m.get("turno_fim"),
            int(m.get("rampa_percentual") or 0),
            json.dumps(m.get("horas_turno") or []),
            json.dumps(m.get("meta_por_hora") or []),
            (m.get("unidade_1") or None),
            (m.get("unidade_2") or None),
            conv,
            alerta_sec,
            datetime.now().isoformat()
        ))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False
