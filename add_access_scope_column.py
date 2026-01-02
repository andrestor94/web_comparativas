import sys
import os
from sqlalchemy import create_engine, text

# Add parent dir to path to find web_comparativas if needed, but we can just use the db url directly
# Assuming SQLite for local dev as per models.py defaults

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Models.py logic:
RENDER_MODE = os.getenv("RENDER") == "true" or "render" in os.getenv("RENDER_EXTERNAL_HOSTNAME", "").lower()
DB_FILE = os.path.join(BASE_DIR, "web_comparativas/app.db")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    DATABASE_URL = f"sqlite:///{DB_FILE}"

print(f"Connecting to: {DATABASE_URL}")

engine = create_engine(DATABASE_URL)

def run_migration():
    with engine.connect() as conn:
        print("Checking columns in 'users' table...")
        # Simple check: try to select the column
        try:
            conn.execute(text("SELECT access_scope FROM users LIMIT 1"))
            print("Column 'access_scope' already exists. No action needed.")
        except Exception:
            print("Column 'access_scope' not found. Adding it...")
            # SQLite does not support IF NOT EXISTS in ADD COLUMN well in older versions, but we are in try/except
            # Also SQLite doesn't support multiple ADD COLUMN in one statement usually, strict alter.
            try:
                conn.execute(text("ALTER TABLE users ADD COLUMN access_scope VARCHAR DEFAULT 'todos'"))
                conn.commit()  # Important for some drivers
                print("Column 'access_scope' added successfully.")
            except Exception as e:
                print(f"Error adding column: {e}")
                # Use raw sqlite3 if sqlalchemy fails for sqlite specific syntax issues?
                # But ALTER TABLE ADD COLUMN is standard enough.
                
if __name__ == "__main__":
    run_migration()
