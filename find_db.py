import os
from pathlib import Path

root = Path(".").resolve()

print("CWD =", root)
print("ENV INDFLOW_DB_PATH =", os.getenv("INDFLOW_DB_PATH"))

candidates = []
for p in root.rglob("*.db"):
    try:
        size = p.stat().st_size
        candidates.append((size, str(p)))
    except:
        pass

candidates.sort(reverse=True)

print("\nDBs encontrados (maiores primeiro):")
for size, path in candidates[:30]:
    print(size, path)

