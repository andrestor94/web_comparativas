import sys
from sqlalchemy import create_engine, text
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))

from web_comparativas.models import Upload

# Configuración DB
print(f"CWD: {Path.cwd()}")
# Intentar encontrar la DB
possible_paths = [
    Path("web_comparativas/app.db"),
    Path("c:/Users/ANDRES.TORRES/Desktop/web_comparativas_v2- ok/web_comparativas_v2- ok/web_comparativas_v2/web_comparativas/app.db")
]
DB_PATH = None
for p in possible_paths:
    if p.exists():
        DB_PATH = p.resolve()
        break

if not DB_PATH:
    print("ERROR: DB file not found in known locations!")
    exit(1)

print(f"Connecting to DB at: {DB_PATH}")
DB_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DB_URL)

with engine.connect() as conn:
    print("\n--- Last 10 Uploads ---")
    result = conn.execute(text("SELECT id, proceso_nro, base_dir, original_path, created_at FROM uploads ORDER BY id DESC LIMIT 10"))
    rows = list(result)
    print(f"Found {len(rows)} rows.")
    for row in rows:
        print(f"ID: {row.id} | Proc: {row.proceso_nro}")
        print(f"   Base: {row.base_dir}")
        print(f"   Orig: {row.original_path}")
