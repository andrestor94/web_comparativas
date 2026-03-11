from pathlib import Path
from sqlalchemy import text
from web_comparativas.models import engine, IS_SQLITE


def _add_column_safe(conn, ddl: str, description: str):
    """
    Ejecuta un ALTER TABLE. Ignora silenciosamente si la columna ya existe.
    """
    try:
        conn.execute(text(ddl))
        print(f"[MIGRATION] {description}: columna/tabla agregada.", flush=True)
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate column" in msg or "duplicate object" in msg:
            print(f"[MIGRATION] {description}: ya existe. (OK)", flush=True)
        elif "no such table" in msg or "undefined table" in msg or "does not exist" in msg:
            print(f"[MIGRATION] {description}: tabla no existe aún. (Saltando)", flush=True)
        else:
            print(f"[MIGRATION] {description}: advertencia – {e}", flush=True)


def ensure_access_scope_column():
    """
    Verifica si la tabla 'users' tiene la columna 'access_scope'.
    Si no la tiene, la agrega (ALTER TABLE).
    Esto es para soportar la migración en Render (PostgreSQL) y local (SQLite).
    """
    try:
        print("[MIGRATION] Intentando agregar columna 'access_scope' a 'users'...", flush=True)
        with engine.begin() as conn:
            conn.execute(
                text("ALTER TABLE users ADD COLUMN access_scope VARCHAR(50) DEFAULT 'todos'")
            )
        print("[MIGRATION] Columna 'access_scope' agregada exitosamente.", flush=True)

    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate column" in msg:
            print("[MIGRATION] La columna 'access_scope' ya existe. (OK)", flush=True)
        elif "no such table" in msg or "undefined table" in msg or "does not exist" in msg:
            print("[MIGRATION] La tabla 'users' no existe aun. (Saltando)", flush=True)
        else:
            print(f"[MIGRATION] Error intentando agregar columna: {e}", flush=True)


def ensure_password_reset_columns():
    """
    Migración para el flujo de restablecimiento de contraseña corporativo.
    Agrega 'must_change_password' a users y crea la tabla password_reset_requests.

    IMPORTANTE: cada operación usa su PROPIA transacción para que un fallo en una
    no revierta las demás (en PostgreSQL, un error dentro de una transacción marca
    toda la conexión como abortada, deshaciendo cambios previos del mismo bloque).
    """
    # 1. Columna must_change_password en users — transacción separada
    with engine.begin() as conn:
        _add_column_safe(
            conn,
            "ALTER TABLE users ADD COLUMN must_change_password BOOLEAN DEFAULT FALSE NOT NULL",
            "users.must_change_password",
        )

    # 2. Tabla password_reset_requests — transacción separada con sintaxis compatible
    # SQLite usa AUTOINCREMENT; PostgreSQL usa SERIAL. Ramificamos para evitar
    # errores de sintaxis que abortan la transacción y revierten columnas ya agregadas.
    if IS_SQLITE:
        pk_col = "id INTEGER PRIMARY KEY AUTOINCREMENT"
        ts_type = "DATETIME"
    else:
        pk_col = "id SERIAL PRIMARY KEY"
        ts_type = "TIMESTAMP"

    ddl = f"""
        CREATE TABLE IF NOT EXISTS password_reset_requests (
            {pk_col},
            user_email      VARCHAR(255) NOT NULL,
            full_name       VARCHAR(255) NOT NULL,
            department      VARCHAR(120),
            comment         TEXT,
            request_date    {ts_type} NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status          VARCHAR(30)  NOT NULL DEFAULT 'Pendiente',
            handled_by      VARCHAR(255),
            handled_date    {ts_type},
            admin_observation TEXT,
            temporary_password_generated BOOLEAN NOT NULL DEFAULT FALSE,
            must_change_password_on_next_login BOOLEAN NOT NULL DEFAULT TRUE
        )
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
        print("[MIGRATION] Tabla 'password_reset_requests' verificada/creada.", flush=True)
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg:
            print("[MIGRATION] Tabla 'password_reset_requests': ya existe. (OK)", flush=True)
        else:
            print(f"[MIGRATION] Tabla 'password_reset_requests': advertencia – {e}", flush=True)


def ensure_normalized_storage_columns():
    """
    Agrega columnas de persistencia robusta a la tabla 'uploads':
    - normalized_content: almacena el Excel procesado como bytes (BYTEA / BLOB)
    - dashboard_json: almacena el JSON del dashboard como texto

    Esto permite que los datos sobrevivan redespliegues en Render
    (el filesystem de Render es efímero; PostgreSQL sí es persistente).

    Compatible con SQLite (local) y PostgreSQL (Render).
    """
    # En PostgreSQL BYTEA; en SQLite BLOB (ambos mapean a LargeBinary en SQLAlchemy)
    blob_type = "BYTEA" if not IS_SQLITE else "BLOB"

    # Transacciones separadas: si una columna ya existe y falla, no revierte la otra
    with engine.begin() as conn:
        _add_column_safe(
            conn,
            f"ALTER TABLE uploads ADD COLUMN normalized_content {blob_type}",
            "uploads.normalized_content",
        )
    with engine.begin() as conn:
        _add_column_safe(
            conn,
            "ALTER TABLE uploads ADD COLUMN dashboard_json TEXT",
            "uploads.dashboard_json",
        )
    print("[MIGRATION] Columnas de persistencia de archivos verificadas/creadas.", flush=True)


def backfill_normalized_content():
    """
    Recorre uploads procesados (status 'reviewing' o 'done') que aún no tienen
    normalized_content guardado en DB, y si el archivo existe en disco lo guarda.

    Se ejecuta en startup para proteger los archivos actuales del siguiente deploy.
    Devuelve el número de uploads respaldados.
    """
    from web_comparativas.models import SessionLocal, Upload as UploadModel
    import json as _json

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    backed_up = 0

    session = SessionLocal()
    try:
        uploads = (
            session.query(UploadModel)
            .filter(
                UploadModel.status.in_(["reviewing", "done", "dashboard"]),
                UploadModel.normalized_content.is_(None),
            )
            .all()
        )
        print(f"[BACKFILL] {len(uploads)} uploads sin contenido en DB. Iniciando respaldo...", flush=True)

        for up in uploads:
            try:
                # Determinar ruta del normalized.xlsx
                base_dir = getattr(up, "base_dir", None)
                if base_dir:
                    p = Path(base_dir)
                    if not p.is_absolute():
                        p = (PROJECT_ROOT / p).resolve()
                    norm_path = p / "processed" / "normalized.xlsx"
                else:
                    norm_path = PROJECT_ROOT / "data" / "uploads" / f"iso_{up.id}" / "processed" / "normalized.xlsx"

                if norm_path.exists():
                    up.normalized_content = norm_path.read_bytes()

                    # También respaldar dashboard.json si existe y no está guardado
                    if not getattr(up, "dashboard_json", None):
                        dash_path = norm_path.parent / "dashboard.json"
                        if dash_path.exists():
                            up.dashboard_json = dash_path.read_text(encoding="utf-8")

                    session.add(up)
                    backed_up += 1
                    print(f"[BACKFILL] Upload {up.id} respaldado en DB ({norm_path.stat().st_size} bytes).", flush=True)
            except Exception as e:
                print(f"[BACKFILL] Upload {up.id}: error al respaldar – {e}", flush=True)
                continue

        if backed_up:
            session.commit()
        print(f"[BACKFILL] Respaldo completado: {backed_up} uploads guardados en DB.", flush=True)
    except Exception as e:
        session.rollback()
        print(f"[BACKFILL] Error general: {e}", flush=True)
    finally:
        session.close()

    return backed_up
