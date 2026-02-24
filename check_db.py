import sqlite3

print("Abrindo indflow.db...\n")

conn = sqlite3.connect("indflow.db")
cur = conn.cursor()

print("--- TABELAS ---")
tables = cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()

for t in tables:
    print(t[0])

print("\n--- ULTIMOS EVENTOS (50) ---")
rows = cur.execute(
    "SELECT datetime(ts_ms/1000,'unixepoch'), effective_machine_id, data_ref, state "
    "FROM machine_state_event "
    "ORDER BY ts_ms DESC LIMIT 50"
).fetchall()

if not rows:
    print("Nenhum evento encontrado.")
else:
    for r in rows:
        print(r)

conn.close()
print("\nFim.")