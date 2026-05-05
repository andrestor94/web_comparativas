from __future__ import annotations

import argparse
import csv
import datetime as dt
import gc
import hashlib
import io
import json
import logging
import os
import re
import shutil
import tempfile
import unicodedata
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd
from sqlalchemy import Date, case, cast, delete, func, insert, or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from web_comparativas.models import IS_POSTGRES, IS_SQLITE, SessionLocal, engine

from .models import (
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoImportError,
    DimensionamientoImportRun,
    DimensionamientoRecord,
)
from .query_service import invalidate_query_cache, refresh_default_dashboard_snapshot

logger = logging.getLogger("wc.dimensionamiento.ingestion")

EXPECTED_COLUMNS = [
    "fecha",
    "plataforma",
    "cliente_nombre_homologado",
    "cliente_nombre_original",
    "cuit",
    "provincia",
    "cuenta_interna",
    "codigo_articulo",
    "descripcion",
    "clasificacion_suizo",
    "descripcion_articulo",
    "familia",
    "unidad_negocio",
    "subunidad_negocio",
    "cantidad_demandada",
    "resultado_participacion",
    "producto_nombre_original",
    "id_registro_unico",
    "fecha_procesamiento",
]

OPTIONAL_FLAG_COLUMNS = [
    "identificado",
    "is_identified",
    "identificado_flag",
    "cliente",
    "is_client",
    "cliente_flag",
    "valorizacion_estimada",
]

CSV_COLUMNS_TO_READ = tuple(dict.fromkeys([*EXPECTED_COLUMNS, *OPTIONAL_FLAG_COLUMNS]))

CSV_CATEGORY_COLUMNS = {
    "plataforma",
    "provincia",
    "familia",
    "unidad_negocio",
    "subunidad_negocio",
    "resultado_participacion",
    "clasificacion_suizo",
    "cliente_nombre_homologado",
}

CSV_DTYPE_BY_COLUMN = {
    column: ("category" if column in CSV_CATEGORY_COLUMNS else "string")
    for column in CSV_COLUMNS_TO_READ
}

DEFAULT_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "dataset_unificado.csv"
DEFAULT_CHUNK_SIZE = 10000
SQLITE_SAFE_BATCH_SIZE = 200
POSTGRES_STAGE_TABLE_PREFIX = "dimensionamiento_stage_"
STARTUP_MODE_VALIDATE = "validate"
STARTUP_MODE_INGEST_IF_EMPTY = "ingest-if-empty"
SUMMARY_CLIENT_NAME_STRATEGY = "visible_fallback_v1"


STARTUP_MODE_FORCE_INGEST = "force-ingest"

_STARTUP_RUN_LOCK = Lock()
_STARTUP_RUN_COMPLETED = False

PLATFORM_MAP = {
    "bionexo": "BIONEXO",
    "medox": "MEDOX",
    "portada": "PORTADA",
}

PROVINCE_MAP = {
    "caba": "CABA",
    "capital federal": "CABA",
    "ciudad autonoma de buenos aires": "CABA",
    "buenos aires": "Buenos Aires",
    "cordoba": "Cordoba",
    "córdoba": "Cordoba",
    "entre rios": "Entre Rios",
    "entre ríos": "Entre Rios",
    "neuquen": "Neuquen",
    "neuquén": "Neuquen",
    "rio negro": "Rio Negro",
    "río negro": "Rio Negro",
    "tucuman": "Tucuman",
    "tucumán": "Tucuman",
}


def _dim_log(level: str, message: str, *args: Any, exc_info: bool = False) -> None:
    rendered = message % args if args else message
    print(rendered, flush=True)
    getattr(logger, level, logger.info)(rendered, exc_info=exc_info)


def _db_target_summary() -> str:
    url = engine.url
    backend = url.get_backend_name()
    host = getattr(url, "host", None) or "-"
    database = getattr(url, "database", None) or "-"
    return (
        f"backend={backend} host={host} database={database} "
        "table=dimensionamiento_records summary_table=dimensionamiento_family_monthly_summary"
    )


def _clean_header(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def _normalize_platform(value: Any) -> str:
    text = (_clean_text(value) or "").lower()
    return PLATFORM_MAP.get(text, (text or "SIN_PLATAFORMA").upper())


def _normalize_province(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    key = text.strip().lower()
    return PROVINCE_MAP.get(key, text.strip().title())


def _parse_date(value: Any) -> dt.date | None:
    text = _clean_text(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def _parse_datetime(value: Any) -> dt.datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _parse_float(value: Any) -> float:
    text = _clean_text(value)
    if not text:
        return 0.0
    compact = text.replace("$", "").replace(" ", "")
    if "," in compact and "." in compact:
        if compact.rfind(",") > compact.rfind("."):
            compact = compact.replace(".", "").replace(",", ".")
        else:
            compact = compact.replace(",", "")
    elif "," in compact:
        compact = compact.replace(".", "").replace(",", ".")
    try:
        return float(compact)
    except ValueError:
        return 0.0


def _bool_from_optional_field(row: dict[str, Any], candidates: list[str]) -> bool | None:
    for field in candidates:
        value = _clean_text(row.get(field))
        if value is None:
            continue
        lowered = value.lower()
        if lowered in {"1", "true", "si", "sí", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False
    return None


def _derive_is_identified(row: dict[str, Any]) -> bool:
    explicit = _bool_from_optional_field(
        row,
        ["identificado", "is_identified", "identificado_flag"],
    )
    if explicit is not None:
        return explicit
    return any(
        _clean_text(row.get(field))
        for field in ("clasificacion_suizo", "codigo_articulo", "familia")
    )


_SIN_DATO_NORM: frozenset[str] = frozenset({"sin dato", "sin_dato"})
TARGET_RECORD_COLUMNS = [
    "id_registro_unico",
    "fecha",
    "plataforma",
    "cliente_nombre_homologado",
    "cliente_nombre_original",
    "cliente_visible",
    "cuit",
    "provincia",
    "cuenta_interna",
    "codigo_articulo",
    "descripcion",
    "clasificacion_suizo",
    "descripcion_articulo",
    "familia",
    "unidad_negocio",
    "subunidad_negocio",
    "cantidad_demandada",
    "valorizacion_estimada",
    "resultado_participacion",
    "producto_nombre_original",
    "fecha_procesamiento",
    "is_identified",
    "is_client",
    "import_run_id",
]
POSTGRES_STAGE_COPY_COLUMNS = ["source_row_number", *TARGET_RECORD_COLUMNS]


def _is_sin_dato(text: str | None) -> bool:
    """Devuelve True si el valor representa 'SIN DATO' (sin cliente identificado)."""
    if not text:
        return True
    return text.strip().lower().replace("_", " ") in _SIN_DATO_NORM


def _derive_is_client(row: dict[str, Any]) -> bool:
    """
    Fuente de verdad: cliente_nombre_homologado.
    Es cliente cuando tiene un nombre real (no nulo, no vacío, no variante de SIN DATO).
    """
    nombre = _clean_text(row.get("cliente_nombre_homologado"))
    return bool(nombre) and not _is_sin_dato(nombre)


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _detect_delimiter(path: Path) -> str:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(16384)
    first_line = sample.splitlines()[0] if sample else ""
    fallback_delimiter = max(
        (",", ";", "|", "\t"),
        key=lambda candidate: first_line.count(candidate),
    )
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except csv.Error:
        return fallback_delimiter if first_line.count(fallback_delimiter) > 0 else ","


def _read_csv_header(path: Path, delimiter: str) -> tuple[list[str], list[str]]:
    header_frame = pd.read_csv(
        path,
        sep=delimiter,
        nrows=0,
        encoding="utf-8-sig",
    )
    original_columns = list(header_frame.columns)
    observed_columns = [_clean_header(column) for column in original_columns]
    return original_columns, observed_columns


def _resolve_csv_read_config(path: Path) -> tuple[str, list[str], list[str], dict[str, str]]:
    detected_delimiter = _detect_delimiter(path)
    delimiter_candidates = list(dict.fromkeys([detected_delimiter, ";", ",", "|", "\t"]))

    delimiter = detected_delimiter
    original_columns: list[str] = []
    observed_columns: list[str] = []
    last_missing: list[str] = []

    for candidate in delimiter_candidates:
        original_columns, observed_columns = _read_csv_header(path, candidate)
        missing = [column for column in EXPECTED_COLUMNS if column not in observed_columns]
        if not missing:
            delimiter = candidate
            break
        last_missing = missing
    else:
        raise ValueError(f"Faltan columnas obligatorias: {', '.join(last_missing)}")

    if delimiter != detected_delimiter:
        _dim_log(
            "warning",
            "[DIM] CSV delimiter auto-corrected detected=%r resolved=%r path=%s",
            detected_delimiter,
            delimiter,
            path,
        )

    selected_original_columns = [
        original
        for original, cleaned in zip(original_columns, observed_columns)
        if cleaned in CSV_COLUMNS_TO_READ
    ]
    selected_dtype_map = {
        original: CSV_DTYPE_BY_COLUMN[cleaned]
        for original, cleaned in zip(original_columns, observed_columns)
        if cleaned in CSV_COLUMNS_TO_READ
    }
    return delimiter, observed_columns, selected_original_columns, selected_dtype_map


def _iter_csv_chunks(
    path: Path,
    chunk_size: int,
    *,
    delimiter: str,
    usecols: list[str],
    dtype_map: dict[str, str],
):
    return pd.read_csv(
        path,
        sep=delimiter,
        chunksize=chunk_size,
        usecols=usecols,
        dtype=dtype_map,
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8-sig",
        low_memory=True,
    )


def _batched(items: list[Any], batch_size: int):
    for start in range(0, len(items), batch_size):
        yield items[start:start + batch_size]


def _count_dimensionamiento_rows() -> int:
    session = SessionLocal()
    try:
        return session.execute(
            select(func.count()).select_from(DimensionamientoRecord)
        ).scalar_one()
    finally:
        session.close()


def _normalize_startup_mode() -> str:
    raw_mode = (os.getenv("DIMENSIONAMIENTO_STARTUP_MODE") or "").strip().lower()
    legacy_force = (os.getenv("DIMENSIONAMIENTO_AUTO_INGEST") or "").strip().lower()

    if legacy_force in {"1", "true", "yes", "si", "sí"}:
        return STARTUP_MODE_FORCE_INGEST
    if raw_mode in {"", STARTUP_MODE_VALIDATE}:
        return STARTUP_MODE_VALIDATE
    if raw_mode in {"bootstrap", "auto", STARTUP_MODE_INGEST_IF_EMPTY}:
        return STARTUP_MODE_INGEST_IF_EMPTY
    if raw_mode in {"force", STARTUP_MODE_FORCE_INGEST}:
        return STARTUP_MODE_FORCE_INGEST

    _dim_log(
        "warning",
        "[DIM] Unknown startup mode %r. Falling back to %s",
        raw_mode,
        STARTUP_MODE_VALIDATE,
    )
    return STARTUP_MODE_VALIDATE


def _resolve_ingestion_source(
    csv_path: str | os.PathLike[str] | None = None,
    source_url: str | None = None,
) -> tuple[str, str]:
    if csv_path:
        return "path", str(Path(csv_path).expanduser().resolve())
    if source_url:
        return "url", source_url.strip()

    csv_path_env = (os.getenv("DIMENSIONAMIENTO_CSV_PATH") or "").strip()
    csv_url_env = (os.getenv("DIMENSIONAMIENTO_CSV_URL") or "").strip()

    if csv_path_env:
        path = Path(csv_path_env).expanduser()
        if path.exists():
            return "path", str(path.resolve())
        _dim_log("warning", "[DIM] Configured path does not exist: %s", path)

    if csv_url_env:
        return "url", csv_url_env

    if DEFAULT_CSV_PATH.exists():
        return "path", str(DEFAULT_CSV_PATH.resolve())

    raise FileNotFoundError(
        "No source found in DIMENSIONAMIENTO_CSV_PATH, DIMENSIONAMIENTO_CSV_URL or DEFAULT_CSV_PATH."
    )


def _looks_like_html(content_type: str, preview: bytes) -> bool:
    lowered_type = (content_type or "").lower()
    preview_text = preview[:2048].decode("utf-8", errors="ignore").lstrip().lower()
    if "html" in lowered_type or "xml" in lowered_type:
        return True
    return preview_text.startswith("<!doctype html") or preview_text.startswith("<html") or "<html" in preview_text


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    homologado = _clean_text(row.get("cliente_nombre_homologado"))
    original = _clean_text(row.get("cliente_nombre_original"))
    cliente_visible = homologado
    if not homologado or _is_sin_dato(homologado):
        cliente_visible = original if original else homologado

    record = {
        "id_registro_unico": _clean_text(row.get("id_registro_unico")),
        "fecha": _parse_date(row.get("fecha")),
        "plataforma": _normalize_platform(row.get("plataforma")),
        "cliente_nombre_homologado": homologado,
        "cliente_nombre_original": original,
        "cliente_visible": cliente_visible,
        "cuit": _clean_text(row.get("cuit")),
        "provincia": _normalize_province(row.get("provincia")),
        "cuenta_interna": _clean_text(row.get("cuenta_interna")),
        "codigo_articulo": _clean_text(row.get("codigo_articulo")),
        "descripcion": _clean_text(row.get("descripcion")),
        "clasificacion_suizo": _clean_text(row.get("clasificacion_suizo")),
        "descripcion_articulo": _clean_text(row.get("descripcion_articulo")),
        "familia": _clean_text(row.get("familia")) or "Sin familia",
        "unidad_negocio": _clean_text(row.get("unidad_negocio")) or "Sin unidad",
        "subunidad_negocio": _clean_text(row.get("subunidad_negocio")) or "Sin subunidad",
        "cantidad_demandada": _parse_float(row.get("cantidad_demandada")),
        "valorizacion_estimada": _parse_float(row.get("valorizacion_estimada")),
        "resultado_participacion": _clean_text(row.get("resultado_participacion")) or "Sin resultado",
        "producto_nombre_original": _clean_text(row.get("producto_nombre_original")),
        "fecha_procesamiento": _parse_datetime(row.get("fecha_procesamiento")),
    }
    record["is_identified"] = _derive_is_identified(row)
    record["is_client"] = _derive_is_client(row)
    return record


def _validate_required_columns(columns: list[str]) -> None:
    missing = [column for column in EXPECTED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"Faltan columnas obligatorias: {', '.join(missing)}")


def _record_error(session: Session, run_id: int, row_number: int, error_message: str, raw_payload: dict[str, Any]) -> None:
    session.add(
        DimensionamientoImportError(
            import_run_id=run_id,
            row_number=row_number,
            error_message=error_message,
            raw_payload=raw_payload,
        )
    )


def _record_errors_bulk(
    session: Session,
    run_id: int,
    errors: list[dict[str, Any]],
) -> None:
    if not errors:
        return
    session.execute(
        insert(DimensionamientoImportError),
        [
            {
                "import_run_id": run_id,
                "row_number": error["row_number"],
                "error_message": error["error_message"],
                "raw_payload": error["raw_payload"],
            }
            for error in errors
        ],
    )


def _postgres_stage_table_name(run_id: int) -> str:
    return f"{POSTGRES_STAGE_TABLE_PREFIX}{int(run_id)}"


def _postgres_stage_columns_ddl() -> str:
    return """
        source_row_number BIGINT NOT NULL,
        id_registro_unico TEXT NOT NULL,
        fecha DATE NOT NULL,
        plataforma VARCHAR(40) NOT NULL,
        cliente_nombre_homologado TEXT,
        cliente_nombre_original TEXT,
        cliente_visible TEXT,
        cuit VARCHAR(32),
        provincia VARCHAR(120),
        cuenta_interna VARCHAR(120),
        codigo_articulo VARCHAR(120),
        descripcion TEXT,
        clasificacion_suizo TEXT,
        descripcion_articulo TEXT,
        familia TEXT,
        unidad_negocio TEXT,
        subunidad_negocio TEXT,
        cantidad_demandada DOUBLE PRECISION NOT NULL,
        valorizacion_estimada DOUBLE PRECISION NOT NULL DEFAULT 0,
        resultado_participacion VARCHAR(120),
        producto_nombre_original TEXT,
        fecha_procesamiento TIMESTAMP NULL,
        is_identified BOOLEAN NOT NULL,
        is_client BOOLEAN NOT NULL,
        import_run_id INTEGER NOT NULL
    """.strip()


def _create_postgres_stage_table(session: Session, table_name: str) -> None:
    session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    session.execute(
        text(
            f"""
            CREATE UNLOGGED TABLE {table_name} (
                {_postgres_stage_columns_ddl()}
            )
            """
        )
    )
    session.commit()


def _drop_postgres_stage_table(table_name: str) -> None:
    cleanup_session = SessionLocal()
    try:
        cleanup_session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        cleanup_session.commit()
    except Exception as exc:
        cleanup_session.rollback()
        _dim_log("warning", "[DIM] Could not drop staging table %s: %s", table_name, exc)
    finally:
        cleanup_session.close()


def _copy_scalar_for_postgres(value: Any) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)


def _copy_rows_to_postgres_stage(table_name: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow([_copy_scalar_for_postgres(row.get(column)) for column in POSTGRES_STAGE_COPY_COLUMNS])
    buffer.seek(0)

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = 0")
            cursor.copy_expert(
                f"""
                COPY {table_name} ({", ".join(POSTGRES_STAGE_COPY_COLUMNS)})
                FROM STDIN WITH (FORMAT CSV, NULL '\\N')
                """,
                buffer,
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def _postgres_dedup_select_sql(table_name: str) -> str:
    cols = ", ".join(TARGET_RECORD_COLUMNS)
    return f"""
        SELECT DISTINCT ON (id_registro_unico) {cols}
        FROM {table_name}
        WHERE id_registro_unico IS NOT NULL
        ORDER BY id_registro_unico, source_row_number DESC
    """


def _postgres_count_dedup_rows(session: Session, table_name: str) -> int:
    return int(
        session.execute(
            text(f"SELECT COUNT(*) FROM ({_postgres_dedup_select_sql(table_name)}) AS dedup")
        ).scalar_one()
    )


def _postgres_count_existing_matches(session: Session, table_name: str) -> int:
    return int(
        session.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM ({_postgres_dedup_select_sql(table_name)}) AS dedup
                INNER JOIN dimensionamiento_records existing
                    ON existing.id_registro_unico = dedup.id_registro_unico
                """
            )
        ).scalar_one()
    )


def _finalize_postgres_stage(
    session: Session,
    table_name: str,
    *,
    mode: str,
) -> tuple[int, int]:
    session.execute(text("SET LOCAL statement_timeout = 0"))
    session.execute(text(f"CREATE INDEX IF NOT EXISTS {table_name}_id_row_idx ON {table_name} (id_registro_unico, source_row_number DESC)"))
    session.execute(text(f"ANALYZE {table_name}"))

    dedup_rows = _postgres_count_dedup_rows(session, table_name)
    existing_matches = 0

    # created_at / updated_at are NOT NULL in dimensionamiento_records but are NOT
    # stored in the staging table. We inject CURRENT_TIMESTAMP explicitly in the
    # SELECT so the INSERT never receives NULL for those columns.
    insert_columns = [*TARGET_RECORD_COLUMNS, "created_at", "updated_at"]
    insert_columns_sql = ", ".join(insert_columns)

    # Wrap the dedup SELECT to append the two timestamps as computed columns.
    dedup_cols_sql = ", ".join(f"dedup.{col}" for col in TARGET_RECORD_COLUMNS)
    full_select_sql = (
        f"SELECT {dedup_cols_sql},"
        f" CURRENT_TIMESTAMP AS created_at,"
        f" CURRENT_TIMESTAMP AS updated_at"
        f" FROM ({_postgres_dedup_select_sql(table_name)}) AS dedup"
    )

    if mode == "replace":
        session.execute(text("TRUNCATE TABLE dimensionamiento_records RESTART IDENTITY"))
        session.execute(
            text(
                f"""
                INSERT INTO dimensionamiento_records ({insert_columns_sql})
                {full_select_sql}
                """
            )
        )
        return dedup_rows, existing_matches

    existing_matches = _postgres_count_existing_matches(session, table_name)
    update_columns = [
        column
        for column in TARGET_RECORD_COLUMNS
        if column not in {"id_registro_unico", "import_run_id"}
    ]
    update_set_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    # On conflict: refresh updated_at but preserve the original created_at.
    update_set_sql += ", import_run_id = EXCLUDED.import_run_id, updated_at = NOW()"
    session.execute(
        text(
            f"""
            INSERT INTO dimensionamiento_records ({insert_columns_sql})
            {full_select_sql}
            ON CONFLICT (id_registro_unico) DO UPDATE
            SET {update_set_sql}
            """
        )
    )
    return dedup_rows, existing_matches


def _normalize_rows_for_chunk(
    chunk: pd.DataFrame,
    *,
    run_id: int,
    line_offset: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    prepared_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total_processed = 0
    chunk_columns = list(chunk.columns)

    for row_number, values in enumerate(chunk.itertuples(index=False, name=None), start=line_offset + 1):
        total_processed += 1
        row = dict(zip(chunk_columns, values))
        try:
            normalized = _normalize_row(row)
            if not normalized["id_registro_unico"]:
                raise ValueError("id_registro_unico vacío")
            if not normalized["fecha"]:
                raise ValueError("fecha inválida")
            normalized["import_run_id"] = run_id
            normalized["source_row_number"] = row_number
            prepared_rows.append(normalized)
        except Exception as exc:
            errors.append(
                {
                    "row_number": row_number,
                    "error_message": str(exc),
                    "raw_payload": row,
                }
            )
    return prepared_rows, errors, total_processed


def _build_upsert_statement(rows: list[dict[str, Any]]):
    if IS_POSTGRES:
        from sqlalchemy.dialects.postgresql import insert as dialect_insert
    elif IS_SQLITE:
        from sqlalchemy.dialects.sqlite import insert as dialect_insert
    else:
        dialect_insert = insert

    # Filtrar campos que no son columnas del modelo antes de construir el stmt.
    # source_row_number es metadata de staging/tracking y nunca va al modelo.
    _NON_MODEL_KEYS = {"id", "created_at", "id_registro_unico", "source_row_number"}
    model_columns = {c.key for c in DimensionamientoRecord.__table__.columns}
    clean_rows = [
        {k: v for k, v in row.items() if k in model_columns}
        for row in rows
    ]
    stmt = dialect_insert(DimensionamientoRecord).values(clean_rows)
    update_columns = {
        key: stmt.excluded[key]
        for key in clean_rows[0].keys()
        if key not in _NON_MODEL_KEYS
    }
    if hasattr(stmt, "on_conflict_do_update"):
        return stmt.on_conflict_do_update(
            index_elements=[DimensionamientoRecord.id_registro_unico],
            set_=update_columns,
        )
    return stmt


def _rebuild_summary_table(session: Session, run_id: int) -> None:
    logger.info("Rebuilding monthly summary table for import_run_id=%s", run_id)
    if IS_POSTGRES:
        session.execute(text("SET LOCAL statement_timeout = 0"))
    session.execute(delete(DimensionamientoFamilyMonthlySummary))

    if IS_SQLITE:
        # En SQLite, insert().from_select() sobre esta tabla deja filas visibles
        # dentro de la transaccion pero no las persiste de forma confiable al commit.
        session.execute(
            text(
                """
                INSERT INTO dimensionamiento_family_monthly_summary (
                    month,
                    plataforma,
                    cliente_nombre_homologado,
                    cliente_visible,
                    provincia,
                    familia,
                    unidad_negocio,
                    subunidad_negocio,
                    resultado_participacion,
                    is_identified,
                    is_client,
                    total_cantidad,
                    total_valorizacion,
                    total_registros,
                    clientes_unicos,
                    import_run_id
                )
                SELECT
                    date(fecha, 'start of month') AS month,
                    plataforma,
                    cliente_nombre_homologado,
                    cliente_visible,
                    provincia,
                    familia,
                    unidad_negocio,
                    subunidad_negocio,
                    resultado_participacion,
                    is_identified,
                    is_client,
                    COALESCE(SUM(cantidad_demandada), 0) AS total_cantidad,
                    COALESCE(SUM(valorizacion_estimada), 0) AS total_valorizacion,
                    COUNT(id) AS total_registros,
                    COUNT(DISTINCT cliente_visible) AS clientes_unicos,
                    :run_id AS import_run_id
                FROM dimensionamiento_records
                GROUP BY
                    date(fecha, 'start of month'),
                    plataforma,
                    cliente_nombre_homologado,
                    cliente_visible,
                    provincia,
                    familia,
                    unidad_negocio,
                    subunidad_negocio,
                    resultado_participacion,
                    is_identified,
                    is_client
                """
            ),
            {"run_id": run_id},
        )
        return

    month_bucket = cast(func.date_trunc("month", DimensionamientoRecord.fecha), Date)

    summary_select = (
        select(
            month_bucket.label("month"),
            DimensionamientoRecord.plataforma,
            DimensionamientoRecord.cliente_nombre_homologado,
            DimensionamientoRecord.cliente_visible,
            DimensionamientoRecord.provincia,
            DimensionamientoRecord.familia,
            DimensionamientoRecord.unidad_negocio,
            DimensionamientoRecord.subunidad_negocio,
            DimensionamientoRecord.resultado_participacion,
            DimensionamientoRecord.is_identified,
            DimensionamientoRecord.is_client,
            func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0).label("total_cantidad"),
            func.coalesce(func.sum(DimensionamientoRecord.valorizacion_estimada), 0).label("total_valorizacion"),
            func.count(DimensionamientoRecord.id).label("total_registros"),
            func.count(func.distinct(DimensionamientoRecord.cliente_visible)).label("clientes_unicos"),
            func.cast(run_id, DimensionamientoFamilyMonthlySummary.import_run_id.type).label("import_run_id"),
        )
        .group_by(
            month_bucket,
            DimensionamientoRecord.plataforma,
            DimensionamientoRecord.cliente_nombre_homologado,
            DimensionamientoRecord.cliente_visible,
            DimensionamientoRecord.provincia,
            DimensionamientoRecord.familia,
            DimensionamientoRecord.unidad_negocio,
            DimensionamientoRecord.subunidad_negocio,
            DimensionamientoRecord.resultado_participacion,
            DimensionamientoRecord.is_identified,
            DimensionamientoRecord.is_client,
        )
    )

    insert_stmt = pg_insert(DimensionamientoFamilyMonthlySummary).from_select(
        [
            "month",
            "plataforma",
            "cliente_nombre_homologado",
            "cliente_visible",
            "provincia",
            "familia",
            "unidad_negocio",
            "subunidad_negocio",
            "resultado_participacion",
            "is_identified",
            "is_client",
            "total_cantidad",
            "total_valorizacion",
            "total_registros",
            "clientes_unicos",
            "import_run_id",
        ],
        summary_select,
    ).on_conflict_do_nothing(
        index_elements=[
            "month", "plataforma", "cliente_nombre_homologado", "cliente_visible",
            "provincia", "familia", "unidad_negocio", "subunidad_negocio",
            "resultado_participacion", "is_identified", "is_client",
        ]
    )
    session.execute(insert_stmt)


def _ingest_dimensionamiento_via_sqlalchemy(
    session: Session,
    *,
    path: Path,
    run: DimensionamientoImportRun,
    chunk_size: int,
) -> tuple[int, int, int, int, list[str]]:
    total_processed = 0
    total_inserted = 0
    total_updated = 0
    total_rejected = 0
    line_offset = 1
    delimiter, observed_columns, selected_columns, selected_dtype_map = _resolve_csv_read_config(path)
    _dim_log(
        "info",
        "[DIM] CSV reader configured selected_columns=%s chunk_size=%s",
        len(selected_columns),
        chunk_size,
    )

    for chunk_number, chunk in enumerate(
        _iter_csv_chunks(
            path,
            chunk_size,
            delimiter=delimiter,
            usecols=selected_columns,
            dtype_map=selected_dtype_map,
        ),
        start=1,
    ):
        chunk.columns = [_clean_header(column) for column in chunk.columns]
        chunk_rows = len(chunk.index)
        _dim_log("info", "[DIM] Processing chunk %s rows=%s", chunk_number, chunk_rows)
        prepared_rows, errors, processed_in_chunk = _normalize_rows_for_chunk(
            chunk,
            run_id=run.id,
            line_offset=line_offset,
        )
        total_processed += processed_in_chunk
        total_rejected += len(errors)
        line_offset += chunk_rows

        if errors:
            _record_errors_bulk(session, run.id, errors)

        if not prepared_rows:
            session.commit()
            _dim_log("info", "[DIM] Inserted chunk %s rows=0", chunk_number)
            del chunk, prepared_rows, errors
            gc.collect()
            continue

        ids = [row["id_registro_unico"] for row in prepared_rows]
        existing_ids: set[str] = set()
        id_batch_size = SQLITE_SAFE_BATCH_SIZE if IS_SQLITE else chunk_size
        for id_batch in _batched(ids, id_batch_size):
            existing_ids.update(
                session.execute(
                    select(DimensionamientoRecord.id_registro_unico).where(
                        DimensionamientoRecord.id_registro_unico.in_(id_batch)
                    )
                ).scalars().all()
            )

        total_inserted += len([row_id for row_id in ids if row_id not in existing_ids])
        total_updated += len([row_id for row_id in ids if row_id in existing_ids])

        write_batch_size = SQLITE_SAFE_BATCH_SIZE if IS_SQLITE else len(prepared_rows)
        for row_batch in _batched(prepared_rows, write_batch_size):
            stmt = _build_upsert_statement(row_batch)
            session.execute(stmt)

        session.commit()
        _dim_log("info", "[DIM] Inserted chunk %s", chunk_number)
        del chunk, prepared_rows, errors, ids, existing_ids
        gc.collect()

    return total_processed, total_inserted, total_updated, total_rejected, observed_columns


def _ingest_dimensionamiento_via_postgres_copy(
    session: Session,
    *,
    path: Path,
    run: DimensionamientoImportRun,
    chunk_size: int,
    mode: str,
) -> tuple[int, int, int, int, list[str]]:
    total_processed = 0
    total_rejected = 0
    line_offset = 1
    stage_table_name = _postgres_stage_table_name(run.id)
    delimiter, observed_columns, selected_columns, selected_dtype_map = _resolve_csv_read_config(path)
    _dim_log(
        "info",
        "[DIM] PostgreSQL staging configured selected_columns=%s chunk_size=%s stage_table=%s",
        len(selected_columns),
        chunk_size,
        stage_table_name,
    )

    _create_postgres_stage_table(session, stage_table_name)
    try:
        for chunk_number, chunk in enumerate(
            _iter_csv_chunks(
                path,
                chunk_size,
                delimiter=delimiter,
                usecols=selected_columns,
                dtype_map=selected_dtype_map,
            ),
            start=1,
        ):
            chunk.columns = [_clean_header(column) for column in chunk.columns]
            chunk_rows = len(chunk.index)
            _dim_log("info", "[DIM] Processing chunk %s rows=%s via COPY", chunk_number, chunk_rows)
            prepared_rows, errors, processed_in_chunk = _normalize_rows_for_chunk(
                chunk,
                run_id=run.id,
                line_offset=line_offset,
            )
            total_processed += processed_in_chunk
            total_rejected += len(errors)
            line_offset += chunk_rows

            if errors:
                _record_errors_bulk(session, run.id, errors)
                session.commit()

            if prepared_rows:
                _copy_rows_to_postgres_stage(stage_table_name, prepared_rows)
                _dim_log("info", "[DIM] Copied chunk %s to staging rows=%s", chunk_number, len(prepared_rows))
            else:
                _dim_log("info", "[DIM] Copied chunk %s to staging rows=0", chunk_number)

            del chunk, prepared_rows, errors
            gc.collect()

        dedup_rows, existing_matches = _finalize_postgres_stage(
            session,
            stage_table_name,
            mode=mode,
        )
        session.commit()
        total_inserted = dedup_rows if mode == "replace" else max(dedup_rows - existing_matches, 0)
        total_updated = 0 if mode == "replace" else existing_matches
        return total_processed, total_inserted, total_updated, total_rejected, observed_columns
    finally:
        _drop_postgres_stage_table(stage_table_name)


def _ingest_dimensionamiento_csv_legacy(
    csv_path: str | os.PathLike[str] | None = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    mode: str = "replace",
    force: bool = False,
) -> dict[str, Any]:
    path = Path(csv_path or os.getenv("DIMENSIONAMIENTO_CSV_PATH") or DEFAULT_CSV_PATH).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el CSV de dimensionamiento: {path}")

    _dim_log(
        "info",
        "[DIM] Ingestion started path=%s chunk_size=%s mode=%s force=%s target=%s",
        path,
        chunk_size,
        mode,
        force,
        _db_target_summary(),
    )
    source_hash = _compute_sha256(path)
    source_mtime = dt.datetime.fromtimestamp(path.stat().st_mtime)

    session = SessionLocal()
    run = None
    try:
        latest_success = session.execute(
            select(DimensionamientoImportRun)
            .where(DimensionamientoImportRun.status == "success")
            .order_by(DimensionamientoImportRun.finished_at.desc(), DimensionamientoImportRun.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest_success and latest_success.source_hash == source_hash and not force:
            _dim_log("info", "[DIM] Ingestion skipped because source hash is unchanged")
            return {
                "status": "skipped",
                "reason": "source_unchanged",
                "source_path": str(path),
                "source_hash": source_hash,
                "last_run_id": latest_success.id,
            }

        run = DimensionamientoImportRun(
            source_path=str(path),
            source_hash=source_hash,
            source_mtime=source_mtime,
            mode=mode,
            status="running",
            chunk_size=chunk_size,
            expected_columns=EXPECTED_COLUMNS,
        )
        session.add(run)
        session.commit()
        session.refresh(run)

        total_processed = 0
        total_inserted = 0
        total_updated = 0
        total_rejected = 0
        line_offset = 1
        delimiter, observed_columns, selected_columns, selected_dtype_map = _resolve_csv_read_config(path)
        _dim_log(
            "info",
            "[DIM] CSV reader configured selected_columns=%s chunk_size=%s",
            len(selected_columns),
            chunk_size,
        )

        for chunk_number, chunk in enumerate(
            _iter_csv_chunks(
                path,
                chunk_size,
                delimiter=delimiter,
                usecols=selected_columns,
                dtype_map=selected_dtype_map,
            ),
            start=1,
        ):
            chunk.columns = [_clean_header(column) for column in chunk.columns]
            chunk_rows = len(chunk.index)
            _dim_log("info", "[DIM] Processing chunk %s rows=%s", chunk_number, chunk_rows)
            prepared_rows: list[dict[str, Any]] = []
            seen_ids: set[str] = set()
            chunk_columns = list(chunk.columns)

            for index, values in enumerate(chunk.itertuples(index=False, name=None), start=line_offset + 1):
                total_processed += 1
                row = dict(zip(chunk_columns, values))
                try:
                    normalized = _normalize_row(row)
                    if not normalized["id_registro_unico"]:
                        raise ValueError("id_registro_unico vacío")
                    if not normalized["fecha"]:
                        raise ValueError("fecha inválida")
                    if normalized["id_registro_unico"] in seen_ids:
                        continue
                    seen_ids.add(normalized["id_registro_unico"])
                    normalized["import_run_id"] = run.id
                    prepared_rows.append(normalized)
                except Exception as exc:
                    total_rejected += 1
                    _record_error(session, run.id, index, str(exc), row)

            line_offset += chunk_rows
            if not prepared_rows:
                _dim_log("info", "[DIM] Inserted chunk %s rows=0", chunk_number)
                del chunk, prepared_rows, seen_ids
                gc.collect()
                continue

            ids = [row["id_registro_unico"] for row in prepared_rows]
            existing_ids: set[str] = set()
            id_batch_size = SQLITE_SAFE_BATCH_SIZE if IS_SQLITE else chunk_size
            for id_batch in _batched(ids, id_batch_size):
                existing_ids.update(
                    session.execute(
                        select(DimensionamientoRecord.id_registro_unico).where(
                            DimensionamientoRecord.id_registro_unico.in_(id_batch)
                        )
                    ).scalars().all()
                )

            total_inserted += len([row_id for row_id in ids if row_id not in existing_ids])
            total_updated += len([row_id for row_id in ids if row_id in existing_ids])

            write_batch_size = SQLITE_SAFE_BATCH_SIZE if IS_SQLITE else len(prepared_rows)
            for row_batch in _batched(prepared_rows, write_batch_size):
                stmt = _build_upsert_statement(row_batch)
                session.execute(stmt)

            session.commit()
            _dim_log("info", "[DIM] Inserted chunk %s", chunk_number)
            del chunk, prepared_rows, ids, existing_ids, seen_ids
            gc.collect()

        if total_processed == 0:
            raise ValueError("El CSV no contiene filas de datos.")

        if mode == "replace":
            session.execute(
                delete(DimensionamientoRecord).where(
                    or_(
                        DimensionamientoRecord.import_run_id.is_(None),
                        DimensionamientoRecord.import_run_id != run.id,
                    )
                )
            )

        _rebuild_summary_table(session, run.id)

        run.status = "success"
        run.finished_at = dt.datetime.utcnow()
        run.observed_columns = observed_columns
        run.rows_processed = total_processed
        run.rows_inserted = total_inserted
        run.rows_updated = total_updated
        run.rows_rejected = total_rejected
        run.summary = {
            "mode": mode,
            "client_name_strategy": SUMMARY_CLIENT_NAME_STRATEGY,
            "platforms": sorted(
                {
                    platform
                    for platform in session.execute(
                        select(DimensionamientoRecord.plataforma).distinct()
                    ).scalars().all()
                    if platform
                }
            ),
        }
        invalidate_query_cache()
        try:
            refresh_default_dashboard_snapshot(session, import_run_id=run.id, commit=False)
            _dim_log("info", "[DIM] Dashboard snapshot refreshed for run_id=%s", run.id)
        except Exception as snapshot_exc:
            _dim_log(
                "warning",
                "[DIM] Dashboard snapshot refresh failed for run_id=%s: %s",
                run.id,
                snapshot_exc,
            )
        session.commit()
        # Invalidar caché de queries para que todos los filtros lean datos frescos
        invalidate_query_cache()

        _dim_log("info", "[DIM] CSV loaded with %s rows", total_processed)
        _dim_log(
            "info",
            "[DIM] Ingestion completed run_id=%s processed=%s inserted=%s updated=%s rejected=%s",
            run.id,
            total_processed,
            total_inserted,
            total_updated,
            total_rejected,
        )
        return {
            "status": "success",
            "run_id": run.id,
            "source_path": str(path),
            "source_hash": source_hash,
            "rows_processed": total_processed,
            "rows_inserted": total_inserted,
            "rows_updated": total_updated,
            "rows_rejected": total_rejected,
        }
    except Exception as exc:
        _dim_log("error", "[DIM] ERROR: ingestion failed for %s: %s", path, exc, exc_info=True)
        session.rollback()
        if run is not None:
            run = session.merge(run)
            run.status = "failed"
            run.finished_at = dt.datetime.utcnow()
            run.error_message = str(exc)
            session.commit()
        raise
    finally:
        session.close()


def ingest_dimensionamiento_csv(
    csv_path: str | os.PathLike[str] | None = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    mode: str = "replace",
    force: bool = False,
) -> dict[str, Any]:
    path = Path(csv_path or os.getenv("DIMENSIONAMIENTO_CSV_PATH") or DEFAULT_CSV_PATH).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No se encontrÃ³ el CSV de dimensionamiento: {path}")

    _dim_log(
        "info",
        "[DIM] Ingestion started path=%s chunk_size=%s mode=%s force=%s target=%s",
        path,
        chunk_size,
        mode,
        force,
        _db_target_summary(),
    )
    source_hash = _compute_sha256(path)
    source_mtime = dt.datetime.fromtimestamp(path.stat().st_mtime)

    session = SessionLocal()
    run = None
    try:
        latest_success = session.execute(
            select(DimensionamientoImportRun)
            .where(DimensionamientoImportRun.status == "success")
            .order_by(DimensionamientoImportRun.finished_at.desc(), DimensionamientoImportRun.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest_success and latest_success.source_hash == source_hash and not force:
            _dim_log("info", "[DIM] Ingestion skipped because source hash is unchanged")
            return {
                "status": "skipped",
                "reason": "source_unchanged",
                "source_path": str(path),
                "source_hash": source_hash,
                "last_run_id": latest_success.id,
            }

        run = DimensionamientoImportRun(
            source_path=str(path),
            source_hash=source_hash,
            source_mtime=source_mtime,
            mode=mode,
            status="running",
            chunk_size=chunk_size,
            expected_columns=EXPECTED_COLUMNS,
        )
        session.add(run)
        session.commit()
        session.refresh(run)

        if IS_POSTGRES:
            (
                total_processed,
                total_inserted,
                total_updated,
                total_rejected,
                observed_columns,
            ) = _ingest_dimensionamiento_via_postgres_copy(
                session,
                path=path,
                run=run,
                chunk_size=chunk_size,
                mode=mode,
            )
        else:
            (
                total_processed,
                total_inserted,
                total_updated,
                total_rejected,
                observed_columns,
            ) = _ingest_dimensionamiento_via_sqlalchemy(
                session,
                path=path,
                run=run,
                chunk_size=chunk_size,
            )

        if total_processed == 0:
            raise ValueError("El CSV no contiene filas de datos.")

        if mode == "replace" and not IS_POSTGRES:
            session.execute(
                delete(DimensionamientoRecord).where(
                    or_(
                        DimensionamientoRecord.import_run_id.is_(None),
                        DimensionamientoRecord.import_run_id != run.id,
                    )
                )
            )

        _rebuild_summary_table(session, run.id)

        run.status = "success"
        run.finished_at = dt.datetime.utcnow()
        run.observed_columns = observed_columns
        run.rows_processed = total_processed
        run.rows_inserted = total_inserted
        run.rows_updated = total_updated
        run.rows_rejected = total_rejected
        run.summary = {
            "mode": mode,
            "client_name_strategy": SUMMARY_CLIENT_NAME_STRATEGY,
            "platforms": sorted(
                {
                    platform
                    for platform in session.execute(
                        select(DimensionamientoRecord.plataforma).distinct()
                    ).scalars().all()
                    if platform
                }
            ),
        }
        try:
            refresh_default_dashboard_snapshot(session, import_run_id=run.id, commit=False)
            _dim_log("info", "[DIM] Dashboard snapshot refreshed for run_id=%s", run.id)
        except Exception as snapshot_exc:
            _dim_log(
                "warning",
                "[DIM] Dashboard snapshot refresh failed for run_id=%s: %s",
                run.id,
                snapshot_exc,
            )
        session.commit()
        # Invalidar caché de queries para que todos los filtros lean datos frescos
        invalidate_query_cache()

        _dim_log("info", "[DIM] CSV loaded with %s rows", total_processed)
        _dim_log(
            "info",
            "[DIM] Ingestion completed run_id=%s processed=%s inserted=%s updated=%s rejected=%s",
            run.id,
            total_processed,
            total_inserted,
            total_updated,
            total_rejected,
        )
        return {
            "status": "success",
            "run_id": run.id,
            "source_path": str(path),
            "source_hash": source_hash,
            "rows_processed": total_processed,
            "rows_inserted": total_inserted,
            "rows_updated": total_updated,
            "rows_rejected": total_rejected,
        }
    except Exception as exc:
        _dim_log("error", "[DIM] ERROR: ingestion failed for %s: %s", path, exc, exc_info=True)
        session.rollback()
        if run is not None:
            run = session.merge(run)
            run.status = "failed"
            run.finished_at = dt.datetime.utcnow()
            run.error_message = str(exc)
            session.commit()
        raise
    finally:
        session.close()


def _tables_are_empty() -> bool:
    """Devuelve True si la tabla dimensionamiento_records no tiene ningún registro."""
    try:
        return _count_dimensionamiento_rows() == 0
    except Exception:
        return True


def _download_csv_from_url(url: str, dest: Path) -> None:
    """Descarga el CSV desde una URL al destino indicado."""
    import requests
    if "drive.google.com/file/d/" in url:
        file_id = url.split("/file/d/")[1].split("/")[0]
        url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
    _dim_log("info", "[DIM] Using source URL %s", url)
    _dim_log("info", "[DIM] Download started")
    with requests.get(url, stream=True, timeout=(15, 300), allow_redirects=True) as response:
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        content_disposition = response.headers.get("Content-Disposition", "")
        iterator = response.iter_content(chunk_size=1024 * 1024)
        first_chunk = next(iterator, b"")
        if _looks_like_html(content_type, first_chunk):
            preview = first_chunk[:300].decode("utf-8", errors="ignore").strip().replace("\n", " ")
            _dim_log(
                "error",
                "[DIM] ERROR: Source URL returned HTML instead of CSV. final_url=%s content_type=%s preview=%s",
                response.url,
                content_type or "-",
                preview or "-",
            )
            raise ValueError(
                "Source URL returned HTML instead of CSV. "
                f"content_type={content_type or '-'} content_disposition={content_disposition or '-'}"
            )
        dest.parent.mkdir(parents=True, exist_ok=True)
        bytes_written = 0
        with dest.open("wb") as handle:
            if first_chunk:
                handle.write(first_chunk)
                bytes_written += len(first_chunk)
            for chunk in iterator:
                if not chunk:
                    continue
                handle.write(chunk)
                bytes_written += len(chunk)
    _dim_log(
        "info",
        "[DIM] Download completed bytes=%s content_type=%s content_disposition=%s",
        bytes_written,
        content_type or "-",
        content_disposition or "-",
    )


def bootstrap_dimensionamiento(
    *,
    csv_path: str | os.PathLike[str] | None = None,
    source_url: str | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    mode: str = "replace",
    force: bool = False,
    require_postgres: bool = False,
) -> dict[str, Any]:
    if require_postgres and not IS_POSTGRES:
        raise RuntimeError(f"[DIM] ERROR: Refusing to ingest into non-PostgreSQL target: {_db_target_summary()}")

    source_kind, source_value = _resolve_ingestion_source(csv_path=csv_path, source_url=source_url)
    temp_dir: Path | None = None

    _dim_log("info", "[DIM] Target database = %s", _db_target_summary())

    try:
        if source_kind == "path":
            working_path = Path(source_value).resolve()
            _dim_log("info", "[DIM] Using source path %s", working_path)
        else:
            temp_dir = Path(tempfile.mkdtemp(prefix="dim-bootstrap-"))
            working_path = temp_dir / "dimensionamiento_bootstrap.csv"
            _download_csv_from_url(source_value, working_path)

        return ingest_dimensionamiento_csv(
            csv_path=working_path,
            chunk_size=chunk_size,
            mode=mode,
            force=force,
        )
    finally:
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def maybe_run_startup_ingestion() -> dict[str, Any]:
    """
    Ingesta de arranque para Dimensionamiento.

    Lógica:
    1. Si la tabla ya tiene datos → no hace nada (PostgreSQL es la fuente de verdad).
    2. Si la tabla está vacía, solo ingiere si el startup está en modo bootstrap.
    3. Si se encontró fuente → ingesta controlada con mode="replace".
    4. Si no hay fuente disponible → avisa en logs y sale sin errores.

    Por defecto el startup solo valida si la tabla ya tiene datos.
    Para habilitar bootstrap automático en startup:
    - DIMENSIONAMIENTO_STARTUP_MODE=ingest-if-empty
    - o DIMENSIONAMIENTO_AUTO_INGEST=true (legado, equivale a force-ingest)
    """
    global _STARTUP_RUN_COMPLETED

    with _STARTUP_RUN_LOCK:
        if _STARTUP_RUN_COMPLETED:
            result = {"status": "skipped", "reason": "already_ran_in_process"}
            _dim_log("info", "[DIM] Ingestion skipped because startup already ran in this process")
            return result
        _STARTUP_RUN_COMPLETED = True

    _dim_log("info", "[DIM] Startup ingestion start")
    _dim_log("info", "[DIM] Target database = %s", _db_target_summary())

    row_count = _count_dimensionamiento_rows()
    startup_mode = _normalize_startup_mode()

    if IS_SQLITE:
        _dim_log("info", "[DIM] Local SQLite detected")
    elif IS_POSTGRES:
        _dim_log("info", "[DIM] PostgreSQL detected, preserving production startup behavior")

    _dim_log("info", "[DIM] dimensionamiento_records count=%s", row_count)
    _dim_log("info", "[DIM] Startup mode = %s", startup_mode)

    if row_count > 0 and startup_mode != STARTUP_MODE_FORCE_INGEST:
        result = {"status": "skipped", "reason": "table_has_data", "row_count": row_count}
        _dim_log("info", "[DIM] Dimensionamiento already loaded, skipping ingestion")
        return result

    if IS_SQLITE and row_count == 0:
        dataset_path = Path(os.getenv("DIMENSIONAMIENTO_CSV_PATH") or DEFAULT_CSV_PATH).expanduser().resolve()
        if not dataset_path.exists():
            _dim_log("warning", "[DIM] dataset_unificado.csv not found at expected path: %s", dataset_path)
            return {"status": "skipped", "reason": "missing_source", "row_count": row_count}

        _dim_log("info", "[DIM] dataset_unificado.csv found at %s", dataset_path)
        _dim_log("info", "[DIM] Starting local ingestion from dataset_unificado.csv")
        result = bootstrap_dimensionamiento(
            csv_path=dataset_path,
            chunk_size=DEFAULT_CHUNK_SIZE,
            mode="replace",
            force=True,
        )
        loaded_count = _count_dimensionamiento_rows()
        _dim_log("info", "[DIM] Local ingestion finished: records=%s", loaded_count)
        _dim_log("info", "[DIM] Startup ingestion result = %s", json.dumps(result, ensure_ascii=False))
        return result

    if row_count == 0 and startup_mode == STARTUP_MODE_VALIDATE:
        result = {"status": "skipped", "reason": "startup_validate_only", "row_count": row_count}
        _dim_log(
            "warning",
            "[DIM] Table is empty but startup mode is validate. Run a manual one-off bootstrap to PostgreSQL or set DIMENSIONAMIENTO_STARTUP_MODE=ingest-if-empty",
        )
        return result

    try:
        result = bootstrap_dimensionamiento(
            chunk_size=DEFAULT_CHUNK_SIZE,
            mode="replace",
            force=startup_mode == STARTUP_MODE_FORCE_INGEST,
        )
        _dim_log("info", "[DIM] Startup ingestion result = %s", json.dumps(result, ensure_ascii=False))
        _dim_log("info", "[DIM] Table row count after startup = %s", _count_dimensionamiento_rows())
        return result
    except FileNotFoundError:
        _dim_log(
            "warning",
            "[DIM] No source found in DIMENSIONAMIENTO_CSV_PATH, DIMENSIONAMIENTO_CSV_URL or DEFAULT_CSV_PATH",
        )
        return {"status": "skipped", "reason": "missing_source"}
    except Exception as exc:
        _dim_log("error", "[DIM] ERROR: %s", exc, exc_info=True)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingesta del CSV unificado de Dimensionamiento.")
    parser.add_argument("--csv-path", dest="csv_path", default=None, help="Ruta al CSV unificado.")
    parser.add_argument("--source-url", dest="source_url", default=None, help="URL para descargar el CSV.")
    parser.add_argument("--chunk-size", dest="chunk_size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument(
        "--mode",
        dest="mode",
        choices=["replace", "upsert"],
        default="replace",
        help="replace: recarga total lógica; upsert: conserva registros ausentes.",
    )
    parser.add_argument("--force", dest="force", action="store_true", help="Ignora hash previo.")
    parser.add_argument(
        "--require-postgres",
        dest="require_postgres",
        action="store_true",
        help="Falla si el target actual no es PostgreSQL.",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _build_arg_parser().parse_args()
    result = bootstrap_dimensionamiento(
        csv_path=args.csv_path,
        source_url=args.source_url,
        chunk_size=args.chunk_size,
        mode=args.mode,
        force=args.force,
        require_postgres=args.require_postgres,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
