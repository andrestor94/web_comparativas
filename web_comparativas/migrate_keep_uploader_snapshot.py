from pathlib import Path
from sqlalchemy import create_engine, text

DB = Path(__file__).resolve().parents[1] / "app.db"
engine = create_engine(f"sqlite:///{DB}")

DDL = [
    # Agrega columnas si no existen
    "ALTER TABLE uploads ADD COLUMN uploaded_by_name TEXT",
    "ALTER TABLE uploads ADD COLUMN uploaded_by_email TEXT",
]

with engine.begin() as conn:
    # detectar columnas existentes para evitar errores si ya corriste el script
    cols = {r[1] for r in conn.exec_driver_sql("PRAGMA table_info('uploads')").fetchall()}
    if "uploaded_by_name" not in cols:
        conn.execute(text(DDL[0]))
    if "uploaded_by_email" not in cols:
        conn.execute(text(DDL[1]))

print("OK: columnas uploaded_by_name / uploaded_by_email presentes.")
