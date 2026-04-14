import datetime as dt
from pathlib import Path
from sqlalchemy import text
from web_comparativas.models import engine, IS_SQLITE, ForecastUserOverride


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


def ensure_original_content_column():
    """
    Agrega la columna original_content a la tabla uploads.
    Almacena los bytes del archivo original subido por el usuario.
    Esto permite que el archivo sobreviva redespliegues en Render
    (filesystem efímero), sirviendo como fallback cuando el archivo
    en disco ya no existe.
    """
    blob_type = "BYTEA" if not IS_SQLITE else "BLOB"
    with engine.begin() as conn:
        _add_column_safe(
            conn,
            f"ALTER TABLE uploads ADD COLUMN original_content {blob_type}",
            "uploads.original_content",
        )
    print("[MIGRATION] Columna uploads.original_content verificada/creada.", flush=True)


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


def ensure_forecast_override_storage():
    """
    Crea la tabla persistente de overrides de Forecast y sus índices de lookup.

    Esta tabla es la fuente de verdad del escenario ajustado por usuario.
    La base forecast original permanece intacta.
    """
    try:
        ForecastUserOverride.__table__.create(bind=engine, checkfirst=True)
        print("[MIGRATION] Tabla 'forecast_user_overrides' verificada/creada.", flush=True)
    except Exception as e:
        print(f"[MIGRATION] Tabla 'forecast_user_overrides': advertencia – {e}", flush=True)

    indexes = [
        (
            "ix_fc_override_user_client_active",
            "forecast_user_overrides",
            "(user_id, source_module, client_selector, is_active)",
        ),
        (
            "ix_fc_override_scope_lookup",
            "forecast_user_overrides",
            "(user_id, source_module, override_scope, client_selector, subneg, codigo_serie, forecast_month, is_active)",
        ),
        (
            "ix_fc_override_context_lookup",
            "forecast_user_overrides",
            "(source_module, context_key, user_id, updated_at)",
        ),
    ]

    for idx_name, table_name, expr in indexes:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} {expr}")
                )
            print(f"[MIGRATION] Índice '{idx_name}' verificado/creado.", flush=True)
        except Exception as e:
            msg = str(e).lower()
            if "already exists" in msg or "duplicate" in msg:
                print(f"[MIGRATION] Índice '{idx_name}': ya existe. (OK)", flush=True)
            else:
                print(f"[MIGRATION] Índice '{idx_name}': advertencia – {e}", flush=True)


def ensure_dimensionamiento_indexes():
    """
    Crea índices funcionales en dimensionamiento_records para las expresiones
    UPPER(TRIM(CAST(COALESCE(col, '') AS TEXT))) usadas en _apply_common_filters.

    Sin estos índices, los WHERE con funciones en la columna hacen seq scan completo
    sobre 400k+ filas. Con ellos, PostgreSQL puede usar index scan.

    Solo aplica en PostgreSQL. En SQLite se omite (no soporta índices funcionales
    con las mismas funciones).

    También agrega el índice funcional sobre el CASE WHEN de cliente_visible para
    acelerar las búsquedas de clientes en _distinct_visible_clients.
    """
    if IS_SQLITE:
        print("[MIGRATION] ensure_dimensionamiento_indexes: SQLite, saltando.", flush=True)
        return

    indexes = [
        (
            "ix_dim_records_plataforma_norm",
            "dimensionamiento_records",
            "upper(trim(cast(coalesce(plataforma, '') as text)))",
        ),
        (
            "ix_dim_records_familia_norm",
            "dimensionamiento_records",
            "upper(trim(cast(coalesce(familia, '') as text)))",
        ),
        (
            "ix_dim_records_provincia_norm",
            "dimensionamiento_records",
            "upper(trim(cast(coalesce(provincia, '') as text)))",
        ),
        (
            "ix_dim_records_resultado_norm",
            "dimensionamiento_records",
            "upper(trim(cast(coalesce(resultado_participacion, '') as text)))",
        ),
        (
            "ix_dim_records_unidad_norm",
            "dimensionamiento_records",
            "upper(trim(cast(coalesce(unidad_negocio, '') as text)))",
        ),
        (
            "ix_dim_records_subunidad_norm",
            "dimensionamiento_records",
            "upper(trim(cast(coalesce(subunidad_negocio, '') as text)))",
        ),
        (
            "ix_dim_records_cliente_hom_norm",
            "dimensionamiento_records",
            "upper(trim(cast(coalesce(cliente_nombre_homologado, '') as text)))",
        ),
    ]

    for idx_name, table_name, expr in indexes:
        ddl = (
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} "
            f"ON {table_name} ({expr})"
        )
        try:
            # CONCURRENTLY no puede ejecutarse dentro de una transacción explícita.
            # Usamos autocommit=True via raw connection.
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(ddl))
            print(f"[MIGRATION] Índice funcional '{idx_name}' verificado/creado.", flush=True)
        except Exception as e:
            msg = str(e).lower()
            if "already exists" in msg or "duplicate" in msg:
                print(f"[MIGRATION] Índice '{idx_name}': ya existe. (OK)", flush=True)
            else:
                print(f"[MIGRATION] Índice '{idx_name}': advertencia – {e}", flush=True)


def ensure_dimensionamiento_summary_populated():
    """
    Detecta si dimensionamiento_records tiene datos pero la tabla de resumen mensual
    (dimensionamiento_family_monthly_summary) está vacía. Esto ocurre cuando la
    ingesta de datos cargó los registros correctamente pero la reconstrucción de la
    tabla resumen falló (por ejemplo, por timeout en la primera carga en Render).

    Si se detecta el problema, intenta reconstruir la tabla resumen con los datos
    existentes. Si falla, loguea la advertencia sin interrumpir el inicio de la app.

    Impacto si no se ejecuta:
    - get_filter_options fast-path devuelve listas vacías de provincias, familias, etc.
    - El dashboard parece no tener datos aunque haya 300k+ registros en DB.
    """
    from sqlalchemy import select, func as sa_func

    try:
        from web_comparativas.dimensionamiento.models import (
            DimensionamientoRecord,
            DimensionamientoFamilyMonthlySummary,
            DimensionamientoImportRun,
        )
        from web_comparativas.models import SessionLocal

        session = SessionLocal()
        try:
            records_count = session.execute(
                select(sa_func.count()).select_from(DimensionamientoRecord)
            ).scalar_one()
            summary_count = session.execute(
                select(sa_func.count()).select_from(DimensionamientoFamilyMonthlySummary)
            ).scalar_one()
            raw_min_month, raw_max_month = session.execute(
                text(
                    "SELECT MIN(month), MAX(month) "
                    "FROM dimensionamiento_family_monthly_summary"
                )
            ).one()

            print(
                "[MIGRATION] Dimensionamiento: "
                f"records={records_count} summary_rows={summary_count} "
                f"min_month={raw_min_month!r} max_month={raw_max_month!r}",
                flush=True,
            )

            min_month_text = str(raw_min_month or "").strip()
            max_month_text = str(raw_max_month or "").strip()
            summary_has_valid_months = (
                summary_count == 0
                or (
                    len(min_month_text) >= 7
                    and "-" in min_month_text
                    and len(max_month_text) >= 7
                    and "-" in max_month_text
                )
            )
            needs_rebuild = records_count > 0 and (
                summary_count == 0 or not summary_has_valid_months
            )

            if needs_rebuild:
                print(
                    "[MIGRATION] ALERTA: dimensionamiento_records tiene datos pero "
                    "dimensionamiento_family_monthly_summary está vacía o inválida. "
                    "Reconstruyendo tabla resumen...",
                    flush=True,
                )
                # Buscar el último import_run exitoso o el más reciente
                latest_run = session.execute(
                    select(DimensionamientoImportRun)
                    .order_by(
                        DimensionamientoImportRun.status.desc(),
                        DimensionamientoImportRun.id.desc(),
                    )
                    .limit(1)
                ).scalar_one_or_none()

                if latest_run is None:
                    print(
                        "[MIGRATION] No se encontró import_run. "
                        "Creando import_run sintético para poder reconstruir la summary.",
                        flush=True,
                    )
                    latest_run = DimensionamientoImportRun(
                        source_path="reconstructed://dimensionamiento_records",
                        source_hash=None,
                        source_mtime=None,
                        mode="rebuild-summary",
                        status="success",
                        chunk_size=0,
                        started_at=dt.datetime.utcnow(),
                        finished_at=dt.datetime.utcnow(),
                        rows_processed=records_count,
                        rows_inserted=0,
                        rows_updated=records_count,
                        rows_rejected=0,
                        expected_columns=None,
                        observed_columns=None,
                        summary={
                            "reason": "summary_rebuild_without_import_run",
                            "records_count": records_count,
                        },
                        error_message=None,
                    )
                    session.add(latest_run)
                    session.commit()
                    session.refresh(latest_run)
                    print(
                        f"[MIGRATION] Import_run sintético creado id={latest_run.id}.",
                        flush=True,
                    )

                from web_comparativas.dimensionamiento.ingestion import _rebuild_summary_table

                # Desactivar timeout local para esta operación batch
                if not IS_SQLITE:
                    session.execute(text("SET LOCAL statement_timeout = 0"))

                _rebuild_summary_table(session, latest_run.id)
                session.commit()
                print(
                    f"[MIGRATION] Tabla resumen reconstruida para import_run_id={latest_run.id}.",
                    flush=True,
                )
            else:
                print("[MIGRATION] Tabla resumen OK, no requiere reconstrucción.", flush=True)
        except Exception as e:
            session.rollback()
            print(
                f"[MIGRATION] ensure_dimensionamiento_summary_populated: advertencia – {e}",
                flush=True,
            )
        finally:
            session.close()
    except ImportError as e:
        print(
            f"[MIGRATION] ensure_dimensionamiento_summary_populated: import error – {e}",
            flush=True,
        )


def ensure_ticket_pliego_columns():
    """
    Agrega columnas de contexto de origen al modelo Ticket para soporte
    del widget de comentarios rápidos en Lectura de Pliegos.

    Columnas nuevas:
      - modulo_origen      VARCHAR(50)  – identifica la sección de origen (e.g. "lectura_pliegos")
      - pliego_solicitud_id INTEGER FK  – vínculo al caso PliegoSolicitud
      - contexto_extra      TEXT        – JSON con datos contextuales del pliego al momento del envío

    Cada ALTER TABLE corre en su propia transacción para que el fallo de
    una columna ya existente no revierta las demás.
    """
    with engine.begin() as conn:
        _add_column_safe(
            conn,
            "ALTER TABLE tickets ADD COLUMN modulo_origen VARCHAR(50)",
            "tickets.modulo_origen",
        )
    with engine.begin() as conn:
        _add_column_safe(
            conn,
            "ALTER TABLE tickets ADD COLUMN pliego_solicitud_id INTEGER REFERENCES pliego_solicitudes(id) ON DELETE SET NULL",
            "tickets.pliego_solicitud_id",
        )
    with engine.begin() as conn:
        _add_column_safe(
            conn,
            "ALTER TABLE tickets ADD COLUMN contexto_extra TEXT",
            "tickets.contexto_extra",
        )
    print("[MIGRATION] Columnas de widget Lectura de Pliegos verificadas/creadas.", flush=True)


def backfill_normalized_content():
    """
    Recorre uploads procesados que aún no tienen normalized_content en DB,
    y si el archivo existe en disco lo guarda. Procesa en lotes de 5 para
    evitar OOM en Render (512MB límite).
    """
    import gc
    from web_comparativas.models import SessionLocal, Upload as UploadModel
    import json as _json
    import os as _os

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    _uploads_env = _os.environ.get("UPLOADS_PATH", "").strip()
    _uploads_root = Path(_uploads_env) if _uploads_env else PROJECT_ROOT / "data" / "uploads"
    backed_up = 0
    BATCH_SIZE = 5  # Procesar de 5 en 5 para no acumular en RAM

    session = SessionLocal()
    try:
        # Solo obtener IDs — no cargar objetos completos todavía
        upload_ids = [
            row[0] for row in session.query(UploadModel.id)
            .filter(
                UploadModel.status.in_(["reviewing", "done", "dashboard"]),
                UploadModel.normalized_content.is_(None),
            )
            .all()
        ]
        print(f"[BACKFILL] {len(upload_ids)} uploads sin contenido en DB.", flush=True)

        for i in range(0, len(upload_ids), BATCH_SIZE):
            batch_ids = upload_ids[i:i + BATCH_SIZE]
            batch = session.query(UploadModel).filter(UploadModel.id.in_(batch_ids)).all()

            for up in batch:
                try:
                    base_dir = getattr(up, "base_dir", None)
                    if base_dir:
                        p = Path(base_dir)
                        if not p.is_absolute():
                            p = (PROJECT_ROOT / p).resolve()
                        if p == _uploads_root:
                            norm_path = p / f"iso_{up.id}" / "processed" / "normalized.xlsx"
                        else:
                            norm_path = p / "processed" / "normalized.xlsx"
                    else:
                        norm_path = _uploads_root / f"iso_{up.id}" / "processed" / "normalized.xlsx"

                    if norm_path.exists():
                        up.normalized_content = norm_path.read_bytes()
                        if not getattr(up, "dashboard_json", None):
                            dash_path = norm_path.parent / "dashboard.json"
                            if dash_path.exists():
                                up.dashboard_json = dash_path.read_text(encoding="utf-8")
                        session.add(up)
                        backed_up += 1
                except Exception as e:
                    print(f"[BACKFILL] Upload {up.id}: error – {e}", flush=True)

            session.commit()
            # Liberar referencias para no acumular en RAM
            session.expire_all()
            del batch
            gc.collect()

        print(f"[BACKFILL] Completado: {backed_up} uploads respaldados.", flush=True)
    except Exception as e:
        session.rollback()
        print(f"[BACKFILL] Error general: {e}", flush=True)
    finally:
        session.close()

    return backed_up


def backfill_original_content():
    """
    Recorre uploads sin original_content y los respalda desde disco.
    Procesa en lotes de 5 para evitar OOM en Render.
    """
    import gc
    from web_comparativas.models import SessionLocal, Upload as UploadModel

    backed_up = 0
    BATCH_SIZE = 5
    session = SessionLocal()
    try:
        upload_ids = [
            row[0] for row in session.query(UploadModel.id)
            .filter(
                UploadModel.original_content.is_(None),
                UploadModel.original_path.isnot(None),
            )
            .all()
        ]
        print(f"[BACKFILL_ORIG] {len(upload_ids)} uploads sin original_content en DB.", flush=True)

        for i in range(0, len(upload_ids), BATCH_SIZE):
            batch_ids = upload_ids[i:i + BATCH_SIZE]
            batch = session.query(UploadModel).filter(UploadModel.id.in_(batch_ids)).all()

            for up in batch:
                try:
                    orig_path = getattr(up, "original_path", None)
                    if not orig_path:
                        continue
                    p = Path(orig_path)
                    if not p.is_absolute():
                        PROJECT_ROOT = Path(__file__).resolve().parents[1]
                        p = (PROJECT_ROOT / p).resolve()
                    if p.exists():
                        up.original_content = p.read_bytes()
                        session.add(up)
                        backed_up += 1
                except Exception as e:
                    print(f"[BACKFILL_ORIG] Upload {up.id}: error – {e}", flush=True)

            session.commit()
            session.expire_all()
            del batch
            gc.collect()

        print(f"[BACKFILL_ORIG] Completado: {backed_up} uploads respaldados.", flush=True)
    except Exception as e:
        session.rollback()
        print(f"[BACKFILL_ORIG] Error general: {e}", flush=True)
    finally:
        session.close()

    return backed_up


def _col_type(table: str, column: str) -> str:
    """Returns the data_type from information_schema for a given table.column, or '' if not found."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c"
            ), {"t": table, "c": column}).fetchone()
        return (row[0] or "").lower() if row else ""
    except Exception:
        return ""


def ensure_forecast_perf_indexes():
    """
    Crea índices en forecast_main y forecast_valorizado para acelerar las
    queries de agregación del módulo Forecast.

    Sin estos índices, cada request hace seq scan completo sobre las tablas
    de 700k+ filas.  Con ellos, PostgreSQL puede usar index scan para los
    WHERE tipo='hist', perfil, neg, subneg más frecuentes.

    CREATE INDEX CONCURRENTLY no puede ejecutarse dentro de una transacción
    explícita — se usa autocommit=True via raw connection.

    Solo aplica en PostgreSQL.  En SQLite se omite.
    """
    if IS_SQLITE:
        print("[MIGRATION] ensure_forecast_perf_indexes: SQLite, saltando.", flush=True)
        return

    indexes = [
        (
            "ix_fc_main_tipo",
            "forecast_main",
            "(tipo)",
        ),
        (
            "ix_fc_main_fecha",
            "forecast_main",
            "(fecha)",
        ),
        (
            "ix_fc_main_tipo_fecha",
            "forecast_main",
            "(tipo, fecha)",
        ),
        (
            "ix_fc_main_perfil",
            "forecast_main",
            "(perfil)",
        ),
        (
            "ix_fc_main_codigo_serie",
            "forecast_main",
            "(codigo_serie)",
        ),
        (
            "ix_fc_val_fecha",
            "forecast_valorizado",
            "(fecha)",
        ),
        (
            "ix_fc_val_perfil_neg_subneg",
            "forecast_valorizado",
            "(perfil, neg, subneg)",
        ),
        (
            "ix_fc_val_cliente_id",
            "forecast_valorizado",
            "(cliente_id)",
        ),
    ]

    for idx_name, table_name, expr in indexes:
        ddl = (
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} "
            f"ON {table_name} {expr}"
        )
        try:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(ddl))
            print(f"[MIGRATION] Índice Forecast '{idx_name}' verificado/creado.", flush=True)
        except Exception as e:
            msg = str(e).lower()
            if "already exists" in msg or "duplicate" in msg:
                print(f"[MIGRATION] Índice Forecast '{idx_name}': ya existe. (OK)", flush=True)
            else:
                print(f"[MIGRATION] Índice Forecast '{idx_name}': advertencia – {e}", flush=True)


def ensure_dimensionamiento_text_columns():
    """
    Convierte de VARCHAR(255) a TEXT las columnas descriptivas largas de las
    tablas de dimensionamiento en PostgreSQL.

    IDEMPOTENTE: verifica information_schema antes de cada ALTER. Si la columna
    ya es TEXT (deploys posteriores), la operación no se ejecuta y el impacto
    en memoria es prácticamente nulo.
    """
    if IS_SQLITE:
        print("[MIGRATION] ensure_dimensionamiento_text_columns: SQLite, saltando.", flush=True)
        return

    targets = [
        ("dimensionamiento_records", [
            "producto_nombre_original",
            "cliente_nombre_homologado",
            "cliente_nombre_original",
            "clasificacion_suizo",
            "familia",
            "unidad_negocio",
            "subunidad_negocio",
        ]),
        ("dimensionamiento_family_monthly_summary", [
            "cliente_nombre_homologado",
            "familia",
            "unidad_negocio",
            "subunidad_negocio",
        ]),
    ]

    any_altered = False
    for table, columns in targets:
        for col in columns:
            current_type = _col_type(table, col)
            if current_type == "text":
                print(f"[MIGRATION] {table}.{col}: ya es TEXT. (OK)", flush=True)
                continue
            if not current_type:
                # Tabla o columna no existe aún — se creará con el tipo correcto
                print(f"[MIGRATION] {table}.{col}: columna no encontrada, saltando.", flush=True)
                continue
            # Needs conversion
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {col} TYPE TEXT"))
                print(f"[MIGRATION] {table}.{col}: convertida a TEXT.", flush=True)
                any_altered = True
            except Exception as e:
                print(f"[MIGRATION] {table}.{col}: advertencia ALTER TYPE – {e}", flush=True)

    if any_altered:
        print("[MIGRATION] ensure_dimensionamiento_text_columns: columnas convertidas.", flush=True)
    else:
        print("[MIGRATION] ensure_dimensionamiento_text_columns: todo ya era TEXT. (OK)", flush=True)


def ensure_dimensionamiento_summary_perf_indexes():
    """
    Crea índices compuestos en dimensionamiento_family_monthly_summary para
    acelerar las consultas de agregación generadas por cada widget del dashboard.

    La tabla resumen ya tiene índices en columnas individuales, pero cuando
    hay filtros activos + agregación, PostgreSQL necesita índices compuestos
    que cubran las columnas del WHERE y del SELECT simultáneamente.

    Índices creados:
      - ix_dim_sum_is_client_cliente  : speeds KPIs client count + clients_by_result
      - ix_dim_sum_resultado_plat     : speeds results_breakdown con filtro plataforma
      - ix_dim_sum_provincia_month    : speeds geo_distribution con filtro fecha
      - ix_dim_sum_unidad_month_total : speeds series por unidad de negocio
      - ix_dim_sum_familia_qty        : speeds top_families / family_consumption

    Solo aplica en PostgreSQL. CREATE INDEX CONCURRENTLY no puede ejecutarse
    dentro de una transacción explícita.
    """
    if IS_SQLITE:
        print("[MIGRATION] ensure_dimensionamiento_summary_perf_indexes: SQLite, saltando.", flush=True)
        return

    indexes = [
        (
            "ix_dim_sum_is_client_cliente",
            "dimensionamiento_family_monthly_summary",
            "(is_client, cliente_nombre_homologado)",
        ),
        (
            "ix_dim_sum_resultado_plat",
            "dimensionamiento_family_monthly_summary",
            "(resultado_participacion, plataforma)",
        ),
        (
            "ix_dim_sum_provincia_month",
            "dimensionamiento_family_monthly_summary",
            "(provincia, month)",
        ),
        (
            "ix_dim_sum_unidad_month_total",
            "dimensionamiento_family_monthly_summary",
            "(unidad_negocio, month, total_registros)",
        ),
        (
            "ix_dim_sum_familia_qty",
            "dimensionamiento_family_monthly_summary",
            "(familia, total_cantidad)",
        ),
    ]

    for idx_name, table_name, expr in indexes:
        ddl = (
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} "
            f"ON {table_name} {expr}"
        )
        try:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(ddl))
            print(f"[MIGRATION] Índice summary '{idx_name}' verificado/creado.", flush=True)
        except Exception as e:
            msg = str(e).lower()
            if "already exists" in msg or "duplicate" in msg:
                print(f"[MIGRATION] Índice summary '{idx_name}': ya existe. (OK)", flush=True)
            else:
                print(f"[MIGRATION] Índice summary '{idx_name}': advertencia – {e}", flush=True)
