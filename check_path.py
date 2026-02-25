import os
from modules.db_indflow import _default_db_path

print("DB PATH:", _default_db_path())
print("ENV INDFLOW_DB_PATH:", os.getenv("INDFLOW_DB_PATH"))
