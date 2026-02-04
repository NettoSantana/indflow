import sqlite3

conn = sqlite3.connect("indflow.db")
cur = conn.cursor()

rows = cur.execute("""
SELECT
  data,
  data_ref,
  machine_id,
  produzido,
  pecas_boas,
  refugo_total
FROM producao_diaria
WHERE machine_id = 'maquina005'
ORDER BY COALESCE(data_ref, data) DESC
LIMIT 5
""").fetchall()

print(rows)

conn.close()
