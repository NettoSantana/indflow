import sqlite3
from datetime import date
from pathlib import Path

# ============================================
# CONFIG
# ============================================
DB_PATH = Path("indflow.db")

# ============================================
# CONEXÃO
# ============================================
def get_conn():
    return sqlite3.connect(DB_PATH)

# ============================================
# INIT DB
# ============================================
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_diaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            data TEXT NOT NULL,
            produzido INTEGER NOT NULL,
            meta INTEGER NOT NULL
        )
    """)

    conn.commit()
    conn.close()

# ============================================
# SALVAR PRODUÇÃO DO DIA
# ============================================
def salvar_producao_diaria(machine_id, produzido, meta):
    conn = get_conn()
    cur = conn.cursor()

    hoje = date.today().isoformat()

    cur.execute("""
        INSERT INTO producao_diaria (machine_id, data, produzido, meta)
        VALUES (?, ?, ?, ?)
    """, (machine_id, hoje, produzido, meta))

    conn.commit()
    conn.close()

# ============================================
# LER HISTÓRICO
# ============================================
def listar_historico(machine_id=None, limit=30):
    conn = get_conn()
    cur = conn.cursor()

    if machine_id:
        cur.execute("""
            SELECT machine_id, data, produzido, meta
            FROM producao_diaria
            WHERE machine_id = ?
            ORDER BY data DESC
            LIMIT ?
        """, (machine_id, limit))
    else:
        cur.execute("""
            SELECT machine_id, data, produzido, meta
            FROM producao_diaria
            ORDER BY data DESC
            LIMIT ?
        """, (limit,))

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "machine_id": r[0],
            "data": r[1],
            "produzido": r[2],
            "meta": r[3],
            "percentual": round((r[2] / r[3]) * 100) if r[3] > 0 else 0
        }
        for r in rows
    ]
