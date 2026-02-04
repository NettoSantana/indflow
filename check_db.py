import os
import sqlite3

db = os.getenv("INDFLOW_DB_PATH", "indflow.db")
print("DB =", db)

conn = sqlite3.connect(db)
cur = conn.cursor()

print("\nTABELAS:")
tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print([t[0] for t in tables])

def try_count(table):
    try:
        n = cur.execute(f"SELECT COUNT(1) FROM {table}").fetchone()[0]
        print(f"{table}: {n}")
    except Exception as e:
        print(f"{table}: ERRO ({e})")

print("\nCONTAGENS PRINCIPAIS:")
for t in ["producao_diaria", "producao_horaria", "producao_evento", "producao_bp", "historico_bp"]:
    try_count(t)

print("\nPRODUCAO_EVENTO amostra (se existir):")
try:
    rows = cur.execute("SELECT * FROM producao_evento ORDER BY id DESC LIMIT 5").fetchall()
    print("ROWS =", len(rows))
    for r in rows:
        print(r)
except Exception as e:
    print("SEM producao_evento:", e)

conn.close()
