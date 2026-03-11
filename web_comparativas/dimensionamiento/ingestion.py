from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import logging
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Date, cast, delete, func, insert, or_, select
from sqlalchemy.orm import Session

from web_comparativas.models import IS_POSTGRES, IS_SQLITE, SessionLocal

from .models import (
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoImportError,
    DimensionamientoImportRun,
    DimensionamientoRecord,
)

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

DEFAULT_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "dataset_unificado.csv"
DEFAULT_CHUNK_SIZE = 10000
SQLITE_SAFE_BATCH_SIZE = 200

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
        sample = handle.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except csv.Error:
        return ","


def _iter_csv_chunks(path: Path, chunk_size: int):
    delimiter = _detect_delimiter(path)
    return pd.read_csv(
        path,
        sep=delimiter,
        chunksize=chunk_size,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        low_memory=True,
    )


def _batched(items: list[Any], batch_size: int):
    for start in range(0, len(items), batch_size):
        yield items[start:start + batch_size]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    record = {
        "id_registro_unico": _clean_text(row.get("id_registro_unico")),
        "fecha": _parse_date(row.get("fecha")),
        "plataforma": _normalize_platform(row.get("plataforma")),
        "cliente_nombre_homologado": _clean_text(row.get("cliente_nombre_homologado")),
        "cliente_nombre_original": _clean_text(row.get("cliente_nombre_original")),
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


def _build_upsert_statement(rows: list[dict[str, Any]]):
    if IS_POSTGRES:
        from sqlalchemy.dialects.postgresql import insert as dialect_insert
    elif IS_SQLITE:
        from sqlalchemy.dialects.sqlite import insert as dialect_insert
    else:
        dialect_insert = insert

    stmt = dialect_insert(DimensionamientoRecord).values(rows)
    update_columns = {
        key: stmt.excluded[key]
        for key in rows[0].keys()
        if key not in {"id", "created_at", "id_registro_unico"}
    }
    if hasattr(stmt, "on_conflict_do_update"):
        return stmt.on_conflict_do_update(
            index_elements=[DimensionamientoRecord.id_registro_unico],
            set_=update_columns,
        )
    return stmt


def _rebuild_summary_table(session: Session, run_id: int) -> None:
    logger.info("Rebuilding monthly summary table for import_run_id=%s", run_id)
    session.execute(delete(DimensionamientoFamilyMonthlySummary))

    if IS_SQLITE:
        month_expr = func.date(DimensionamientoRecord.fecha, "start of month")
    else:
        month_expr = func.date_trunc("month", DimensionamientoRecord.fecha)

    summary_select = (
        select(
            cast(month_expr, Date).label("month"),
            DimensionamientoRecord.plataforma,
            DimensionamientoRecord.cliente_nombre_homologado,
            DimensionamientoRecord.provincia,
            DimensionamientoRecord.familia,
            DimensionamientoRecord.unidad_negocio,
            DimensionamientoRecord.subunidad_negocio,
            DimensionamientoRecord.resultado_participacion,
            DimensionamientoRecord.is_identified,
            DimensionamientoRecord.is_client,
            func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0).label("total_cantidad"),
            func.count(DimensionamientoRecord.id).label("total_registros"),
            func.count(func.distinct(DimensionamientoRecord.cliente_nombre_homologado)).label("clientes_unicos"),
            func.cast(run_id, DimensionamientoFamilyMonthlySummary.import_run_id.type).label("import_run_id"),
        )
        .group_by(
            cast(month_expr, Date),
            DimensionamientoRecord.plataforma,
            DimensionamientoRecord.cliente_nombre_homologado,
            DimensionamientoRecord.provincia,
            DimensionamientoRecord.familia,
            DimensionamientoRecord.unidad_negocio,
            DimensionamientoRecord.subunidad_negocio,
            DimensionamientoRecord.resultado_participacion,
            DimensionamientoRecord.is_identified,
            DimensionamientoRecord.is_client,
        )
    )

    insert_stmt = insert(DimensionamientoFamilyMonthlySummary).from_select(
        [
            "month",
            "plataforma",
            "cliente_nombre_homologado",
            "provincia",
            "familia",
            "unidad_negocio",
            "subunidad_negocio",
            "resultado_participacion",
            "is_identified",
            "is_client",
            "total_cantidad",
            "total_registros",
            "clientes_unicos",
            "import_run_id",
        ],
        summary_select,
    )
    session.execute(insert_stmt)


def ingest_dimensionamiento_csv(
    csv_path: str | os.PathLike[str] | None = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    mode: str = "replace",
    force: bool = False,
) -> dict[str, Any]:
    path = Path(csv_path or os.getenv("DIMENSIONAMIENTO_CSV_PATH") or DEFAULT_CSV_PATH).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el CSV de dimensionamiento: {path}")

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
            logger.info("Dimensionamiento ingest skipped; source hash unchanged.")
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
        observed_columns: list[str] | None = None
        line_offset = 1

        for chunk in _iter_csv_chunks(path, chunk_size):
            chunk.columns = [_clean_header(column) for column in chunk.columns]
            if observed_columns is None:
                observed_columns = list(chunk.columns)
                _validate_required_columns(observed_columns)

            rows = chunk.to_dict(orient="records")
            prepared_rows: list[dict[str, Any]] = []
            seen_ids: set[str] = set()

            for index, row in enumerate(rows, start=line_offset + 1):
                total_processed += 1
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

            line_offset += len(rows)
            if not prepared_rows:
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

        if observed_columns is None:
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
        session.commit()

        logger.info(
            "Dimensionamiento ingest success run_id=%s processed=%s inserted=%s updated=%s rejected=%s",
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
        logger.exception("Dimensionamiento ingest failed for %s", path)
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
    session = SessionLocal()
    try:
        count = session.execute(
            select(func.count()).select_from(DimensionamientoRecord)
        ).scalar_one()
        return count == 0
    except Exception:
        return True
    finally:
        session.close()


def _download_csv_from_url(url: str, dest: Path) -> None:
    """Descarga el CSV desde una URL al destino indicado."""
    import urllib.request
    logger.info("[DIMENSIONAMIENTO] Descargando CSV desde URL: %s → %s", url, dest)
    # Soporte para links de Google Drive (convierte /file/d/ID/view → /uc?export=download&id=ID)
    if "drive.google.com/file/d/" in url:
        file_id = url.split("/file/d/")[1].split("/")[0]
        url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
    urllib.request.urlretrieve(url, str(dest))
    logger.info("[DIMENSIONAMIENTO] Descarga completada: %s bytes", dest.stat().st_size)


def maybe_run_startup_ingestion() -> None:
    """
    Ingesta de arranque para Dimensionamiento.

    Lógica:
    1. Si la tabla ya tiene datos → no hace nada (PostgreSQL es la fuente de verdad).
    2. Si la tabla está vacía, busca la fuente en este orden:
       a. DIMENSIONAMIENTO_CSV_PATH  → ruta local al CSV
       b. DIMENSIONAMIENTO_CSV_URL   → URL para descargar el CSV (ej. Google Drive, S3)
       c. DEFAULT_CSV_PATH           → ruta local por defecto (solo en entornos con el archivo)
    3. Si se encontró fuente → ingesta con mode="replace", force=True.
    4. Si no hay fuente disponible → avisa en logs y sale sin errores.

    Se activa automáticamente si la tabla está vacía, independientemente de DIMENSIONAMIENTO_AUTO_INGEST.
    La variable DIMENSIONAMIENTO_AUTO_INGEST=true también puede forzar la ingesta incluso con datos.
    """
    force_flag = (os.getenv("DIMENSIONAMIENTO_AUTO_INGEST") or "").strip().lower()
    force_even_with_data = force_flag in {"1", "true", "yes", "si", "sí"}

    tables_empty = _tables_are_empty()

    if not tables_empty and not force_even_with_data:
        logger.info(
            "[DIMENSIONAMIENTO] Tabla con datos existentes. Startup ingestion omitida. "
            "(Usar DIMENSIONAMIENTO_AUTO_INGEST=true para forzar re-ingesta.)"
        )
        return

    if not tables_empty and force_even_with_data:
        logger.info("[DIMENSIONAMIENTO] DIMENSIONAMIENTO_AUTO_INGEST=true: forzando re-ingesta.")

    if tables_empty:
        logger.info("[DIMENSIONAMIENTO] Tabla vacía detectada. Intentando ingesta automática de startup.")

    # Determinar fuente del CSV
    csv_path_env = os.getenv("DIMENSIONAMIENTO_CSV_PATH") or ""
    csv_url_env = os.getenv("DIMENSIONAMIENTO_CSV_URL") or ""
    tmp_path: Path | None = None

    try:
        if csv_path_env and Path(csv_path_env).exists():
            # Opción A: ruta local configurada
            logger.info("[DIMENSIONAMIENTO] Usando CSV desde DIMENSIONAMIENTO_CSV_PATH: %s", csv_path_env)
            result = ingest_dimensionamiento_csv(csv_path=csv_path_env, mode="replace", force=True)

        elif csv_url_env:
            # Opción B: descarga desde URL (Google Drive, S3, Dropbox direct link, etc.)
            import tempfile
            tmp_dir = Path(tempfile.mkdtemp())
            tmp_path = tmp_dir / "dataset_unificado.csv"
            try:
                _download_csv_from_url(csv_url_env, tmp_path)
                result = ingest_dimensionamiento_csv(csv_path=tmp_path, mode="replace", force=True)
            finally:
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)

        elif DEFAULT_CSV_PATH.exists():
            # Opción C: archivo local por defecto (útil en desarrollo)
            logger.info("[DIMENSIONAMIENTO] Usando CSV por defecto: %s", DEFAULT_CSV_PATH)
            result = ingest_dimensionamiento_csv(mode="replace", force=force_even_with_data)

        else:
            logger.warning(
                "[DIMENSIONAMIENTO] No se encontró fuente de datos (DIMENSIONAMIENTO_CSV_PATH, "
                "DIMENSIONAMIENTO_CSV_URL ni archivo local). "
                "Configurá una de estas variables en Render para poblar la base de datos."
            )
            return

        logger.info("[DIMENSIONAMIENTO] Startup ingestion result: %s", json.dumps(result, ensure_ascii=False))

    except Exception as exc:
        logger.error("[DIMENSIONAMIENTO] Error en startup ingestion: %s", exc, exc_info=True)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingesta del CSV unificado de Dimensionamiento.")
    parser.add_argument("--csv-path", dest="csv_path", default=None, help="Ruta al CSV unificado.")
    parser.add_argument("--chunk-size", dest="chunk_size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument(
        "--mode",
        dest="mode",
        choices=["replace", "upsert"],
        default="replace",
        help="replace: recarga total lógica; upsert: conserva registros ausentes.",
    )
    parser.add_argument("--force", dest="force", action="store_true", help="Ignora hash previo.")
    return parser


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _build_arg_parser().parse_args()
    result = ingest_dimensionamiento_csv(
        csv_path=args.csv_path,
        chunk_size=args.chunk_size,
        mode=args.mode,
        force=args.force,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
