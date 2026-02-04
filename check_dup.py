# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\check_dup.py
# LAST_RECODE: 2026-02-04 09:00 America/Bahia
# MOTIVO: Consulta local no SQLite para verificar duplicidade (maquina004 vs scoped) em producao_diaria.

import sqlite3

DB = "indflow.db"

sql = """
SELECT data, machine_id, produzido
FROM producao_diaria
WHERE data = '2026-02-04'
  AND (machine_id = 'maquina004' OR machine_id LIKE '%::maquina004')
ORDER BY machine_id;
"""

conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute(sql)
rows = cur.fetchall()
print("ROWS:", len(rows))
for r in rows:
    print(r)
conn.close()
