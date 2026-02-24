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

conn.close()
print("\nFim.")