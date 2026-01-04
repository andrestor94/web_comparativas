from sqlalchemy import text
from web_comparativas.models import engine

def ensure_access_scope_column():
    """
    Verifica si la tabla 'users' tiene la columna 'access_scope'.
    Si no la tiene, la agrega (ALTER TABLE).
    Esto es para soportar la migración en Render (PostgreSQL) y local (SQLite).
    """

    try:
        print("[MIGRATION] Intentando agregar columna 'access_scope' a 'users'...", flush=True)
        # Intentamos agregar la columna directamente. Si ya existe, fallará y capturamos el error.
        # Esto evita usar inspect() que estaba bloqueándose en Render.
        with engine.begin() as conn:
            conn.execute(
                text("ALTER TABLE users ADD COLUMN access_scope VARCHAR(50) DEFAULT 'todos'")
            )
        print("[MIGRATION] Columna 'access_scope' agregada exitosamente.", flush=True)

    except Exception as e:
        msg = str(e).lower()
        # Detectar errores comunes de "ya existe"
        if "already exists" in msg or "duplicate column" in msg:
            print("[MIGRATION] La columna 'access_scope' ya existe. (OK)", flush=True)
        elif "no such table" in msg or "undefined table" in msg or "does not exist" in msg:
            print("[MIGRATION] La tabla 'users' no existe aun. (Saltando)", flush=True)
        else:
            print(f"[MIGRATION] Error intentando agregar columna: {e}", flush=True)
