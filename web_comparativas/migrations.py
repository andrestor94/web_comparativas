from sqlalchemy import inspect, text, String
from web_comparativas.models import engine

def ensure_access_scope_column():
    """
    Verifica si la tabla 'users' tiene la columna 'access_scope'.
    Si no la tiene, la agrega (ALTER TABLE).
    Esto es para soportar la migración en Render (PostgreSQL) y local (SQLite).
    """
    try:
        insp = inspect(engine)
        
        # Verificar si existe la tabla users
        if "users" not in insp.get_table_names():
            print("[MIGRATION] Tabla 'users' no encontrada. Saltando migración.")
            return

        cols = [c["name"] for c in insp.get_columns("users")]
        if "access_scope" in cols:
            print("[MIGRATION] La columna 'access_scope' ya existe en 'users'.")
            return

        print("[MIGRATION] Agregando columna 'access_scope' a tabla 'users'...")
        
        # Detectar el motor para ajustar la sintaxis si fuera necesario
        # (Aunque ADD COLUMN funciona igual en PG y SQLite modernos para columnas simples)
        # Model definition: access_scope = Column(String, default="todos")
        # En SQL: VARCHAR o TEXT. Usaremos VARCHAR(50) o TEXT.
        # SQLite soporta ADD COLUMN pero con algunas restricciones.
        # Postgres lo soporta nativamente.
        
        # Usamos engine.begin() para transacción automática commit/rollback
        with engine.begin() as conn:
            conn.execute(
                text("ALTER TABLE users ADD COLUMN access_scope VARCHAR(50) DEFAULT 'todos'")
            )
            
        print("[MIGRATION] Columna 'access_scope' agregada exitosamente.")

    except Exception as e:
        print(f"[MIGRATION] Error intentando agregar columna: {e}")
        # No re-lanzamos la excepción para no impedir el arranque si es algo menor,
        # pero esto es crítico si el código lo usa.
        # En este caso, dejamos el log.
