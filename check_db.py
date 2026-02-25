import sqlite3

conn = sqlite3.connect("indflow.db")
cur = conn.cursor()

print("=== TOP 10 data_ref em producao_horaria ===")
for row in cur.execute("""
    SELECT data_ref, COUNT(*) AS qtd
    FROM producao_horaria
    GROUP BY data_ref
    ORDER BY qtd DESC
    LIMIT 10
"""):
    print(row)

conn.close()