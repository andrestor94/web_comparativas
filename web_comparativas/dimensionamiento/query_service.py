from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from sqlalchemy import Date, Text, case, cast, distinct, func, inspect as sa_db_inspect, literal, or_, select, text
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from web_comparativas.models import IS_POSTGRES, IS_SQLITE

from .identity import canon as _entity_canon
from .models import (
    DimensionamientoClienteEntidad,
    DimensionamientoDashboardSnapshot,
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoImportRun,
    DimensionamientoRecord,
)

logger = logging.getLogger("wc.dimensionamiento.query")
_NO_FILTER_TOKENS = frozenset({"__all__", "__todos__", "todos", "todas", "all", "*"})
DEFAULT_DASHBOARD_SNAPSHOT_KEY = "default_dashboard_bootstrap"
DEFAULT_DASHBOARD_SNAPSHOT_VERSION = "v9"
_SUMMARY_REQUIRED_COLUMNS = frozenset(
    {
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
    }
)

# ── In-memory query result cache ─────────────────────────────────────────────
# Evita recalcular resultados idénticos cuando el usuario cambia y revierte
# filtros en rápida sucesión. Invalidado después de cada importación exitosa.
# Thread-safe: las asignaciones de dict en CPython son atómicas bajo el GIL,
# pero usamos un lock explícito al limpiar para garantizar consistencia.
#
# TTLs (segundos):
#   _TTL_SUMMARY_HEALTH        : snapshot de salud de la tabla resumen (muy barato)
#   _TTL_FILTER_OPTIONS_DFLT   : opciones de filtro sin filtros activos (estable)
#   _TTL_FILTER_OPTIONS_FILT   : opciones de filtro con filtros activos
#   _TTL_QUERY_RESULT          : todos los demás widgets
_TTL_SUMMARY_HEALTH = 10.0
_TTL_FILTER_OPTIONS_DFLT = 300.0
_TTL_FILTER_OPTIONS_FILT = 60.0
_TTL_QUERY_RESULT = 120.0
_QUERY_CACHE_MAX = 100          # Entradas máximas antes de limpiar completo

_QUERY_CACHE: dict[str, dict] = {}
_QUERY_CACHE_LOCK = threading.Lock()
_CACHE_MISS = object()          # Sentinel para distinguir cache miss de None

# Micro-cache dedicado al health snapshot (no necesita clave por filtros)
_SUMMARY_HEALTH_CACHE: dict[int | None, dict] = {}

# Micro-cache para get_status: los conteos globales raramente cambian; 30s evita
# 3 queries extras en cada carga inicial y cada reload sin invalidar datos útiles.
_STATUS_CACHE: dict[int | None, dict] = {}
_TTL_STATUS = 30.0

# Registro de entidades-cliente resueltas por corrida (identity.py), con etiquetas ya
# desambiguadas. Cache pequeño; se invalida en invalidate_query_cache().
_ENTITY_REGISTRY_CACHE: dict[int | None, dict[str, Any]] = {}


def _make_cache_key(fn_name: str, filters: "DimensionamientoFilters", **extra: Any) -> str:
    """Clave MD5 determinista a partir del nombre de función + filtros + params extra."""
    d = _filters_debug_dict(filters)
    # Normalizar listas para que el orden no genere keys distintas
    for k in ("clientes", "cliente_entidad_ids", "provincias", "familias", "plataformas",
              "unidades_negocio", "unidades_negocio_excluir", "subunidades_negocio", "resultados"):
        if isinstance(d.get(k), list):
            d[k] = sorted(d[k])
    if extra:
        d["_x"] = {k: str(v) for k, v in sorted(extra.items())}
    raw = f"{fn_name}:{json.dumps(d, sort_keys=True, default=str)}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key: str, ttl: float) -> Any:
    """Retorna el valor cacheado o _CACHE_MISS si no existe o expiró."""
    entry = _QUERY_CACHE.get(key)
    if entry is None:
        return _CACHE_MISS
    if time.perf_counter() - entry["ts"] > ttl:
        with _QUERY_CACHE_LOCK:
            _QUERY_CACHE.pop(key, None)
        return _CACHE_MISS
    return entry["val"]


def _cache_set(key: str, val: Any) -> None:
    """Almacena un valor. Limpia el caché completo si supera el tamaño máximo."""
    with _QUERY_CACHE_LOCK:
        if len(_QUERY_CACHE) >= _QUERY_CACHE_MAX:
            _QUERY_CACHE.clear()
        _QUERY_CACHE[key] = {"ts": time.perf_counter(), "val": val}


def invalidate_query_cache() -> None:
    """Limpia todos los resultados cacheados. Llamar después de cada importación."""
    with _QUERY_CACHE_LOCK:
        count = len(_QUERY_CACHE)
        _QUERY_CACHE.clear()
    # Resetear también los micro-caches de salud y status
    _SUMMARY_HEALTH_CACHE.clear()
    _STATUS_CACHE.clear()
    _ENTITY_REGISTRY_CACHE.clear()
    logger.info("[DIM][CACHE] Caché invalidado. %d entradas eliminadas.", count)


def _get_date_column(model):
    """Retorna la columna de fecha correcta para el modelo dado.
    Usa 'month' si existe como columna mapeada, de lo contrario 'fecha'.
    Esto evita usar hasattr() que no es confiable con descriptores SQLAlchemy.
    """
    try:
        mapper = sa_inspect(model)
        mapped_cols = {attr.key for attr in mapper.mapper.column_attrs}
        if "month" in mapped_cols:
            return model.month
        return model.fecha
    except Exception:
        return model.fecha


@dataclass
class DimensionamientoFilters:
    clientes: list[str] = field(default_factory=list)
    # Selección de entidades-cliente por id (resolución de identidad). Es la vía canónica
    # del filtro "Cliente": el desplegable manda entidad_id, el WHERE filtra por
    # cliente_entidad_id. `clientes` (strings) queda como compat/legacy.
    cliente_entidad_ids: list[int] = field(default_factory=list)
    provincias: list[str] = field(default_factory=list)
    familias: list[str] = field(default_factory=list)
    plataformas: list[str] = field(default_factory=list)
    unidades_negocio: list[str] = field(default_factory=list)
    subunidades_negocio: list[str] = field(default_factory=list)
    resultados: list[str] = field(default_factory=list)
    fecha_desde: dt.date | None = None
    fecha_hasta: dt.date | None = None
    is_client: bool | None = None
    # Exclusión de unidades de negocio: proviene de la interacción con la leyenda del gráfico
    unidades_negocio_excluir: list[str] = field(default_factory=list)
    import_run_id: int | None = None


def _filters_debug_dict(filters: DimensionamientoFilters) -> dict[str, Any]:
    return {
        "clientes": filters.clientes,
        "cliente_entidad_ids": filters.cliente_entidad_ids,
        "provincias": filters.provincias,
        "familias": filters.familias,
        "plataformas": filters.plataformas,
        "unidades_negocio": filters.unidades_negocio,
        "unidades_negocio_excluir": filters.unidades_negocio_excluir,
        "subunidades_negocio": filters.subunidades_negocio,
        "resultados": filters.resultados,
        "fecha_desde": filters.fecha_desde.isoformat() if filters.fecha_desde else None,
        "fecha_hasta": filters.fecha_hasta.isoformat() if filters.fecha_hasta else None,
        "is_client": filters.is_client,
        "import_run_id": filters.import_run_id,
    }


def _empty_filter_options() -> dict[str, Any]:
    return {
        "clientes": [],
        "provincias": [],
        "familias": [],
        "plataformas": [],
        "unidades_negocio": [],
        "subunidades_negocio": [],
        "resultados": [],
        "date_range": {"min": None, "max": None},
    }


def _clone_filters(filters: DimensionamientoFilters) -> DimensionamientoFilters:
    return DimensionamientoFilters(
        clientes=list(filters.clientes),
        cliente_entidad_ids=list(filters.cliente_entidad_ids),
        provincias=list(filters.provincias),
        familias=list(filters.familias),
        plataformas=list(filters.plataformas),
        unidades_negocio=list(filters.unidades_negocio),
        unidades_negocio_excluir=list(filters.unidades_negocio_excluir),
        subunidades_negocio=list(filters.subunidades_negocio),
        resultados=list(filters.resultados),
        fecha_desde=filters.fecha_desde,
        fecha_hasta=filters.fecha_hasta,
        is_client=filters.is_client,
        import_run_id=filters.import_run_id,
    )


def _has_active_filters(filters: DimensionamientoFilters) -> bool:
    return any(
        [
            filters.clientes,
            filters.cliente_entidad_ids,
            filters.provincias,
            filters.familias,
            filters.plataformas,
            filters.unidades_negocio,
            filters.unidades_negocio_excluir,
            filters.subunidades_negocio,
            filters.resultados,
            filters.fecha_desde is not None,
            filters.fecha_hasta is not None,
            filters.is_client is not None,
        ]
    )


def _coerce_date_value(value: Any) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text_value = str(value).strip()
    if not text_value:
        return None
    normalized = text_value[:10] if len(text_value) >= 10 else text_value
    try:
        return dt.date.fromisoformat(normalized)
    except ValueError:
        return None


def _month_value_to_iso(value: Any) -> str:
    coerced = _coerce_date_value(value)
    if coerced is not None:
        return coerced.isoformat()
    text_value = str(value or "").strip()
    if text_value.isdigit() and len(text_value) == 4:
        return f"{text_value}-01-01"
    if len(text_value) == 7 and text_value.count("-") == 1:
        return f"{text_value}-01"
    return text_value


def _date_range_payload(min_value: Any, max_value: Any) -> dict[str, str | None]:
    min_date = _coerce_date_value(min_value)
    max_date = _coerce_date_value(max_value)
    return {
        "min": min_date.isoformat() if min_date else None,
        "max": max_date.isoformat() if max_date else None,
    }


def _table_columns(session: Session, table_name: str) -> set[str]:
    try:
        inspector = sa_db_inspect(session.get_bind())
        return {column["name"] for column in inspector.get_columns(table_name)}
    except Exception:
        logger.exception("[DIM][SUMMARY] Could not inspect table=%s", table_name)
        return set()


def _summary_health_snapshot(session: Session, import_run_id: int | None = None) -> dict[str, Any]:
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None

    summary_columns = _table_columns(session, "dimensionamiento_family_monthly_summary")
    missing_columns = sorted(_SUMMARY_REQUIRED_COLUMNS - summary_columns)

    if import_run_id is None:
        return {
            "rows": 0,
            "raw_min_month": None,
            "raw_max_month": None,
            "min_month": None,
            "max_month": None,
            "missing_columns": missing_columns,
            "summary_total_valorizacion": 0.0,
            "records_total_valorizacion": 0.0,
            "valorizacion_mismatch": False,
            "usable": False,
        }

    try:
        summary_rows, raw_min_month, raw_max_month, summary_total_valorizacion = session.execute(
            text(
                "SELECT COUNT(*), MIN(month), MAX(month), COALESCE(SUM(total_valorizacion), 0) "
                "FROM dimensionamiento_family_monthly_summary "
                "WHERE import_run_id = :run_id"
            ),
            {"run_id": import_run_id}
        ).one()
    except Exception:
        logger.exception("[DIM][SUMMARY] Could not read dimensionamiento_family_monthly_summary health snapshot")
        summary_rows, raw_min_month, raw_max_month, summary_total_valorizacion = 0, None, None, 0
    min_month = _coerce_date_value(raw_min_month)
    max_month = _coerce_date_value(raw_max_month)
    valorizacion_mismatch = False
    records_total_valorizacion = None

    # Proteccion contra una summary desalineada: si el agregado monetario quedo en 0
    # pero la tabla base tiene valorizacion real, la summary no debe seguir usandose.
    if (
        not missing_columns
        and summary_rows
        and "total_valorizacion" in summary_columns
        and abs(float(summary_total_valorizacion or 0)) < 0.01
    ):
        try:
            records_total_valorizacion = float(
                session.execute(
                    text(
                        "SELECT COALESCE(SUM(valorizacion_estimada), 0) "
                        "FROM dimensionamiento_records "
                        "WHERE import_run_id = :run_id"
                    ),
                    {"run_id": import_run_id}
                ).scalar_one()
                or 0
            )
            valorizacion_mismatch = abs(records_total_valorizacion) >= 0.01
        except Exception:
            logger.exception("[DIM][SUMMARY] Could not compare summary vs base valorizacion totals")

    if missing_columns:
        logger.warning(
            "[DIM][SUMMARY] Summary schema mismatch missing_columns=%s rows=%s",
            missing_columns,
            summary_rows,
        )
    if valorizacion_mismatch:
        logger.warning(
            "[DIM][SUMMARY] Summary valorizacion mismatch summary_total=%s records_total=%s",
            float(summary_total_valorizacion or 0),
            records_total_valorizacion,
        )
    return {
        "rows": int(summary_rows or 0),
        "raw_min_month": raw_min_month,
        "raw_max_month": raw_max_month,
        "min_month": min_month,
        "max_month": max_month,
        "missing_columns": missing_columns,
        "summary_total_valorizacion": float(summary_total_valorizacion or 0),
        "records_total_valorizacion": records_total_valorizacion,
        "valorizacion_mismatch": valorizacion_mismatch,
        "usable": (
            bool(summary_rows)
            and min_month is not None
            and max_month is not None
            and not missing_columns
            and not valorizacion_mismatch
        ),
    }


def _summary_health_snapshot_cached(session: Session, import_run_id: int | None = None) -> dict[str, Any]:
    """Versión cacheada de _summary_health_snapshot con TTL de 10 segundos."""
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None

    now = time.perf_counter()
    cached = _SUMMARY_HEALTH_CACHE.get(import_run_id)
    if cached is not None and now - cached["ts"] < _TTL_SUMMARY_HEALTH:
        return cached["val"]
    val = _summary_health_snapshot(session, import_run_id)
    _SUMMARY_HEALTH_CACHE[import_run_id] = {"ts": now, "val": val}
    return val


def _global_date_bounds(session: Session, import_run_id: int | None = None) -> tuple[dt.date | None, dt.date | None]:
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None

    summary_state = _summary_health_snapshot_cached(session, import_run_id)
    if summary_state["usable"]:
        return summary_state["min_month"], summary_state["max_month"]

    if import_run_id is None:
        return None, None

    min_date, max_date = session.execute(
        select(
            func.min(DimensionamientoRecord.fecha),
            func.max(DimensionamientoRecord.fecha),
        ).where(DimensionamientoRecord.import_run_id == import_run_id)
    ).one()
    return _coerce_date_value(min_date), _coerce_date_value(max_date)


def _default_platform_values(session: Session, import_run_id: int | None = None) -> list[str]:
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None

    cache_key = f"dimensionamiento.default_platform_values.{import_run_id}"
    cached = _cache_get(cache_key, _TTL_FILTER_OPTIONS_DFLT)
    if cached is not _CACHE_MISS:
        return list(cached)

    if import_run_id is None:
        return []

    summary_state = _summary_health_snapshot_cached(session, import_run_id)
    model = DimensionamientoFamilyMonthlySummary if summary_state["usable"] else DimensionamientoRecord
    stmt = (
        select(distinct(model.plataforma))
        .where(model.plataforma.is_not(None))
        .where(model.import_run_id == import_run_id)
        .order_by(model.plataforma)
    )
    payload = [value for value in session.execute(stmt).scalars().all() if value not in (None, "")]
    _cache_set(cache_key, payload)
    return payload


def _normalize_dashboard_filters(
    session: Session,
    filters: DimensionamientoFilters,
) -> DimensionamientoFilters:
    normalized = _clone_filters(filters)
    if normalized.import_run_id is None:
        latest = _latest_success_import_run(session)
        normalized.import_run_id = latest.id if latest else None

    default_platforms = _default_platform_values(session, normalized.import_run_id)
    if normalized.plataformas and default_platforms:
        requested_platforms = {value.strip().upper() for value in normalized.plataformas if value}
        all_platforms = {value.strip().upper() for value in default_platforms if value}
        if requested_platforms == all_platforms:
            normalized.plataformas = []

    min_date, max_date = _global_date_bounds(session, normalized.import_run_id)
    if min_date is not None and normalized.fecha_desde is not None and normalized.fecha_desde <= min_date:
        normalized.fecha_desde = None
    if max_date is not None and normalized.fecha_hasta is not None and normalized.fecha_hasta >= max_date:
        normalized.fecha_hasta = None
    return normalized


def _resolve_aggregate_model(
    session: Session,
    endpoint_tag: str,
    import_run_id: int | None = None,
    *,
    summary_message: str = "using summary table",
    base_message: str = "using base table",
):
    summary_state = _summary_health_snapshot_cached(session, import_run_id)
    if summary_state["usable"]:
        logger.info(
            "[DIM][%s] %s rows=%s min_month=%s max_month=%s",
            endpoint_tag,
            summary_message,
            summary_state["rows"],
            summary_state["min_month"].isoformat(),
            summary_state["max_month"].isoformat(),
        )
        return DimensionamientoFamilyMonthlySummary

    logger.warning(
        "[DIM][%s] %s reason=summary_unavailable summary_rows=%s raw_min_month=%r raw_max_month=%r",
        endpoint_tag,
        base_message,
        summary_state["rows"],
        summary_state["raw_min_month"],
        summary_state["raw_max_month"],
    )
    return DimensionamientoRecord


def _apply_local_statement_timeout(session: Session, milliseconds: int) -> None:
    if IS_POSTGRES:
        safe_milliseconds = max(int(milliseconds), 1000)
        session.execute(text(f"SET LOCAL statement_timeout = {safe_milliseconds}"))


def _log_query_success(name: str, started_at: float, **counts: Any) -> None:
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    logger.info("[DIM][QUERY] %s completed in %sms counts=%s", name, elapsed_ms, counts)


def _log_query_start(name: str, filters: DimensionamientoFilters | None = None, **extra: Any) -> float:
    payload = dict(extra)
    if filters is not None:
        payload["filters"] = _filters_debug_dict(filters)
    logger.info("[DIM][QUERY] %s start payload=%s", name, payload)
    return time.perf_counter()


def _month_expr(column):
    if IS_SQLITE:
        return func.date(column, "start of month")
    return func.date_trunc("month", column)


def _normalize_list(values: Iterable[str] | None) -> list[str]:
    if not values:
        return []
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        for part in str(value).split(","):
            item = " ".join(str(part).strip().split())
            if not item or item.lower() in _NO_FILTER_TOKENS:
                continue
            dedupe_key = item.casefold()
            if dedupe_key not in seen:
                seen.add(dedupe_key)
                cleaned.append(item)
    return cleaned


def _sql_normalized_text(column):
    return func.upper(func.trim(cast(func.coalesce(column, ""), Text)))


def _normalized_filter_values(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = " ".join(str(value or "").strip().split())
        if not item or item.lower() in _NO_FILTER_TOKENS:
            continue
        key = item.upper()
        if key not in seen:
            seen.add(key)
            normalized.append(key)
    return normalized


def _model_column_or_literal(model, column_name: str, default: str = ""):
    mapper = sa_inspect(model)
    mapped_cols = {attr.key for attr in mapper.mapper.column_attrs}
    if column_name in mapped_cols:
        return getattr(model, column_name)
    return literal(default)


def _compile_sql(session: Session, stmt) -> str:
    try:
        return str(
            stmt.compile(
                dialect=session.bind.dialect if session.bind is not None else None,
                compile_kwargs={"literal_binds": True},
            )
        )
    except Exception:
        return str(stmt)


def _log_query_statement(
    session: Session,
    name: str,
    model,
    stmt,
    filters: DimensionamientoFilters,
    applied_conditions: list[str],
) -> None:
    logger.info(
        "[DIM][QUERY] %s statement model=%s filters=%s conditions=%s sql=%s",
        name,
        getattr(model, "__tablename__", str(model)),
        _filters_debug_dict(filters),
        applied_conditions,
        _compile_sql(session, stmt),
    )


_SUMMARY_CLIENT_EXCLUDE: frozenset[str] = frozenset({"SIN DATO", "SIN_DATO"})


def _is_sin_dato_sql(column):
    """Expresión SQL que devuelve True si el valor de la columna equivale a SIN DATO.

    Cubre: NULL, vacío, 'SIN DATO', 'SIN_DATO' (case-insensitive, trim).
    Usada para separar correctamente el universo cliente vs no-cliente en las queries.
    """
    normalized = func.upper(func.trim(func.replace(func.coalesce(column, ""), "_", " ")))
    return or_(
        column.is_(None),
        func.coalesce(column, "") == "",
        normalized == "SIN DATO",
    )


def _distinct_summary_clients(session: Session, import_run_id: int | None = None) -> list[str]:
    """Fast path (sin filtros activos): nombres homologados únicos de clientes reales.

    Universo 'Sí': registros con is_client=True.
    Fuente: cliente_nombre_homologado (no cliente_visible, que puede contener originales).
    Excluye variantes de SIN DATO, nulos y vacíos.
    """
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None
    if import_run_id is None:
        return []
    model = DimensionamientoFamilyMonthlySummary
    stmt = (
        select(distinct(model.cliente_nombre_homologado))
        .where(model.import_run_id == import_run_id)
        .where(model.is_client.is_(True))
        .where(model.cliente_nombre_homologado.isnot(None))
        .where(func.coalesce(model.cliente_nombre_homologado, "") != "")
        .order_by(model.cliente_nombre_homologado)
    )
    all_clients = [c for c in session.execute(stmt).scalars().all() if c]
    return [
        c for c in all_clients
        if c.strip().upper().replace("_", " ") not in _SUMMARY_CLIENT_EXCLUDE
    ]


def _distinct_summary_non_clients(session: Session, import_run_id: int | None = None) -> list[str]:
    """Fast path (sin filtros activos): nombres originales únicos de no-clientes.

    Universo 'No': registros donde is_client=False (homologado ausente o SIN DATO).
    Fuente: cliente_nombre_original — NUNCA cliente_visible ni homologado.
    Excluye nulos y vacíos.
    """
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None
    if import_run_id is None:
        return []
    model = DimensionamientoFamilyMonthlySummary
    # cliente_nombre_original no existe en la summary table; usamos cliente_visible
    # de filas con is_client=False (que en ingesta ya fue asignado desde original).
    # Excluimos adicionalmente cualquier residual de SIN DATO proveniente del original.
    stmt = (
        select(distinct(model.cliente_visible))
        .where(model.import_run_id == import_run_id)
        .where(model.is_client.is_(False))
        .where(model.cliente_visible.isnot(None))
        .where(func.coalesce(model.cliente_visible, "") != "")
        .order_by(model.cliente_visible)
    )
    all_names = [c for c in session.execute(stmt).scalars().all() if c]
    return [
        c for c in all_names
        if c.strip().upper().replace("_", " ") not in _SUMMARY_CLIENT_EXCLUDE
    ]


def _distinct_visible_clients(session: Session, filters: DimensionamientoFilters) -> list[str]:
    """Universo Sí (is_client=True): nombres homologados únicos desde dimensionamiento_records.

    Usa cliente_nombre_homologado como fuente exclusiva (no cliente_visible).
    Excluye SIN DATO, nulos y vacíos.
    """
    col = DimensionamientoRecord.cliente_nombre_homologado
    inner_stmt = _apply_common_filters(
        select(col.label("nombre_visible")),
        DimensionamientoRecord,
        filters,
    )
    inner_stmt = inner_stmt.where(col.isnot(None))
    inner_stmt = inner_stmt.where(func.coalesce(col, "") != "")
    inner_stmt = inner_stmt.where(~_is_sin_dato_sql(col))
    subq = inner_stmt.subquery()

    outer_stmt = (
        select(distinct(subq.c.nombre_visible))
        .where(subq.c.nombre_visible.isnot(None))
        .where(subq.c.nombre_visible != "")
        .order_by(subq.c.nombre_visible)
    )
    all_clients = [v for v in session.execute(outer_stmt).scalars().all() if v not in (None, "")]
    return [c for c in all_clients if c.strip().upper().replace("_", " ") not in _SUMMARY_CLIENT_EXCLUDE]


def _distinct_visible_non_clients(session: Session, filters: DimensionamientoFilters) -> list[str]:
    """Universo No (is_client=False): nombres originales únicos desde dimensionamiento_records.

    Fuente: cliente_visible de registros con is_client=False.
    En ingesta, para estos registros cliente_visible = cliente_nombre_original.
    Excluye SIN DATO, nulos y vacíos.
    """
    col = DimensionamientoRecord.cliente_visible
    inner_stmt = _apply_common_filters(
        select(col.label("nombre_visible")),
        DimensionamientoRecord,
        filters,
    )
    # _apply_common_filters ya aplica is_client=False si está en filters.
    # Garantizamos que solo incluimos filas con is_client=False explícitamente.
    inner_stmt = inner_stmt.where(DimensionamientoRecord.is_client.is_(False))
    inner_stmt = inner_stmt.where(col.isnot(None))
    inner_stmt = inner_stmt.where(func.coalesce(col, "") != "")
    inner_stmt = inner_stmt.where(~_is_sin_dato_sql(col))
    subq = inner_stmt.subquery()

    outer_stmt = (
        select(distinct(subq.c.nombre_visible))
        .where(subq.c.nombre_visible.isnot(None))
        .where(subq.c.nombre_visible != "")
        .order_by(subq.c.nombre_visible)
    )
    return [v for v in session.execute(outer_stmt).scalars().all() if v not in (None, "")]



def build_filters(
    clientes: Iterable[str] | None = None,
    provincias: Iterable[str] | None = None,
    familias: Iterable[str] | None = None,
    plataformas: Iterable[str] | None = None,
    unidades_negocio: Iterable[str] | None = None,
    unidades_negocio_excluir: Iterable[str] | None = None,
    subunidades_negocio: Iterable[str] | None = None,
    resultados: Iterable[str] | None = None,
    fecha_desde: dt.date | None = None,
    fecha_hasta: dt.date | None = None,
    is_client: bool | None = None,
    cliente_entidad_ids: Iterable[int] | None = None,
) -> DimensionamientoFilters:
    return DimensionamientoFilters(
        clientes=_normalize_list(clientes),
        cliente_entidad_ids=[int(x) for x in (cliente_entidad_ids or []) if str(x).strip() != ""],
        provincias=_normalize_list(provincias),
        familias=_normalize_list(familias),
        plataformas=_normalize_list(plataformas),
        unidades_negocio=_normalize_list(unidades_negocio),
        unidades_negocio_excluir=_normalize_list(unidades_negocio_excluir),
        subunidades_negocio=_normalize_list(subunidades_negocio),
        resultados=_normalize_list(resultados),
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        is_client=is_client,
    )


def _apply_common_filters(stmt, model, filters: DimensionamientoFilters, applied_conditions: list[str] | None = None):
    if filters.import_run_id is not None:
        stmt = stmt.where(model.import_run_id == filters.import_run_id)
        if applied_conditions is not None:
            applied_conditions.append(f"import_run_id = {filters.import_run_id}")
    use_direct_match = model is DimensionamientoFamilyMonthlySummary
    # Filtro "Cliente" por ENTIDAD resuelta (vía canónica): trae todas las filas de la
    # entidad, en todas las plataformas, homologadas y no homologadas. Ambas tablas tienen
    # cliente_entidad_id.
    if filters.cliente_entidad_ids:
        stmt = stmt.where(model.cliente_entidad_id.in_(filters.cliente_entidad_ids))
        if applied_conditions is not None:
            applied_conditions.append(f"cliente_entidad_id IN ({len(filters.cliente_entidad_ids)} entidades)")
    elif filters.clientes:
        # Compat legacy: selección por string de cliente_visible (una sola forma).
        if use_direct_match:
            _visible = model.cliente_visible
            stmt = stmt.where(_visible.in_(filters.clientes))
            if applied_conditions is not None:
                applied_conditions.append(f"cliente_visible IN {filters.clientes}")
        else:
            if filters.is_client is False:
                _filter_col = model.cliente_visible
                col_label = "cliente_visible"
            else:
                _filter_col = model.cliente_nombre_homologado
                col_label = "cliente_nombre_homologado"
            normalized_clients = _normalized_filter_values(filters.clientes)
            if normalized_clients:
                stmt = stmt.where(_sql_normalized_text(_filter_col).in_(normalized_clients))
                if applied_conditions is not None:
                    applied_conditions.append(f"{col_label} IN {normalized_clients}")
    if filters.provincias:
        if use_direct_match:
            stmt = stmt.where(model.provincia.in_(filters.provincias))
            if applied_conditions is not None:
                applied_conditions.append(f"provincia IN {filters.provincias}")
        else:
            normalized_provincias = _normalized_filter_values(filters.provincias)
            if normalized_provincias:
                stmt = stmt.where(_sql_normalized_text(model.provincia).in_(normalized_provincias))
                if applied_conditions is not None:
                    applied_conditions.append(f"provincia IN {normalized_provincias}")
    if filters.familias:
        if use_direct_match:
            stmt = stmt.where(model.familia.in_(filters.familias))
            if applied_conditions is not None:
                applied_conditions.append(f"familia IN {filters.familias}")
        else:
            normalized_familias = _normalized_filter_values(filters.familias)
            if normalized_familias:
                stmt = stmt.where(_sql_normalized_text(model.familia).in_(normalized_familias))
                if applied_conditions is not None:
                    applied_conditions.append(f"familia IN {normalized_familias}")
    if filters.plataformas:
        if use_direct_match:
            stmt = stmt.where(model.plataforma.in_(filters.plataformas))
            if applied_conditions is not None:
                applied_conditions.append(f"plataforma IN {filters.plataformas}")
        else:
            normalized_plataformas = _normalized_filter_values(filters.plataformas)
            if normalized_plataformas:
                stmt = stmt.where(_sql_normalized_text(model.plataforma).in_(normalized_plataformas))
                if applied_conditions is not None:
                    applied_conditions.append(f"plataforma IN {normalized_plataformas}")
    if filters.unidades_negocio:
        if use_direct_match:
            stmt = stmt.where(model.unidad_negocio.in_(filters.unidades_negocio))
            if applied_conditions is not None:
                applied_conditions.append(f"unidad_negocio IN {filters.unidades_negocio}")
        else:
            normalized_unidades = _normalized_filter_values(filters.unidades_negocio)
            if normalized_unidades:
                stmt = stmt.where(_sql_normalized_text(model.unidad_negocio).in_(normalized_unidades))
                if applied_conditions is not None:
                    applied_conditions.append(f"unidad_negocio IN {normalized_unidades}")
    if filters.unidades_negocio_excluir:
        if use_direct_match:
            stmt = stmt.where(model.unidad_negocio.notin_(filters.unidades_negocio_excluir))
            if applied_conditions is not None:
                applied_conditions.append(f"unidad_negocio NOT IN {filters.unidades_negocio_excluir}")
        else:
            normalized_excluir = _normalized_filter_values(filters.unidades_negocio_excluir)
            if normalized_excluir:
                stmt = stmt.where(_sql_normalized_text(model.unidad_negocio).notin_(normalized_excluir))
                if applied_conditions is not None:
                    applied_conditions.append(f"unidad_negocio NOT IN {normalized_excluir}")
    if filters.subunidades_negocio:
        if use_direct_match:
            stmt = stmt.where(model.subunidad_negocio.in_(filters.subunidades_negocio))
            if applied_conditions is not None:
                applied_conditions.append(f"subunidad_negocio IN {filters.subunidades_negocio}")
        else:
            normalized_subunidades = _normalized_filter_values(filters.subunidades_negocio)
            if normalized_subunidades:
                stmt = stmt.where(_sql_normalized_text(model.subunidad_negocio).in_(normalized_subunidades))
                if applied_conditions is not None:
                    applied_conditions.append(f"subunidad_negocio IN {normalized_subunidades}")
    if filters.resultados:
        if use_direct_match:
            stmt = stmt.where(model.resultado_participacion.in_(filters.resultados))
            if applied_conditions is not None:
                applied_conditions.append(f"resultado_participacion IN {filters.resultados}")
        else:
            normalized_resultados = _normalized_filter_values(filters.resultados)
            if normalized_resultados:
                stmt = stmt.where(_sql_normalized_text(model.resultado_participacion).in_(normalized_resultados))
                if applied_conditions is not None:
                    applied_conditions.append(f"resultado_participacion IN {normalized_resultados}")
    if filters.is_client is not None:
        # ¿Cliente? es a nivel ENTIDAD (no fila): una entidad es "Sí" si tiene ≥1 fila
        # homologada. En el summary usamos la columna denormalizada es_cliente_entidad;
        # en records, un subquery contra el registry de entidades.
        if use_direct_match:
            stmt = stmt.where(model.es_cliente_entidad.is_(filters.is_client))
        else:
            _reg_sub = (
                select(DimensionamientoClienteEntidad.entidad_key)
                .where(DimensionamientoClienteEntidad.import_run_id == filters.import_run_id)
                .where(DimensionamientoClienteEntidad.es_cliente.is_(filters.is_client))
            )
            stmt = stmt.where(model.cliente_entidad_id.in_(_reg_sub))
        if applied_conditions is not None:
            applied_conditions.append(f"es_cliente_entidad IS {filters.is_client}")
    if filters.fecha_desde is not None:
        date_column = _get_date_column(model)
        stmt = stmt.where(date_column >= filters.fecha_desde)
        if applied_conditions is not None:
            applied_conditions.append(f"{date_column.key} >= {filters.fecha_desde.isoformat()}")
    if filters.fecha_hasta is not None:
        date_column = _get_date_column(model)
        stmt = stmt.where(date_column <= filters.fecha_hasta)
        if applied_conditions is not None:
            applied_conditions.append(f"{date_column.key} <= {filters.fecha_hasta.isoformat()}")

    return stmt


def _distinct_values(session: Session, column, filters: DimensionamientoFilters, order_by=None) -> list[str]:
    """Retorna valores únicos de una columna aplicando filtros comunes.

    Usa subquery con label para evitar sqlalchemy.exc.NoSuchColumnError al combinar
    SELECT DISTINCT con filtros dinámicos adicionales (mismo patrón que _distinct_visible_clients).
    """
    inner_stmt = _apply_common_filters(
        select(column.label("_val")).where(column.is_not(None)),
        DimensionamientoRecord,
        filters,
    )
    subq = inner_stmt.subquery()
    outer_stmt = (
        select(distinct(subq.c._val))
        .where(subq.c._val.isnot(None))
        .order_by(order_by if order_by is not None else subq.c._val)
    )
    return [value for value in session.execute(outer_stmt).scalars().all() if value not in (None, "")]


def _distinct_summary_values(session: Session, column, import_run_id: int | None = None, order_by=None) -> list[str]:
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None
    if import_run_id is None:
        return []
    model = column.class_
    stmt = (
        select(distinct(column.label("_val")))
        .where(model.import_run_id == import_run_id)
        .where(column.is_not(None))
        .order_by(order_by if order_by is not None else column)
    )
    return [value for value in session.execute(stmt).scalars().all() if value not in (None, "")]


def _distinct_filtered_summary_clients(session: Session, filters: DimensionamientoFilters) -> list[str]:
    """Universo Sí (is_client=True): nombres homologados únicos desde summary con filtros activos.

    Fuente: cliente_nombre_homologado de filas con is_client=True.
    No usa cliente_visible para evitar contaminación con nombres originales de no-clientes.
    """
    model = DimensionamientoFamilyMonthlySummary
    col = model.cliente_nombre_homologado
    base_stmt = (
        select(col.label("_val"))
        .where(model.is_client.is_(True))
        .where(col.isnot(None))
        .where(func.coalesce(col, "") != "")
    )
    inner_stmt = _apply_common_filters(base_stmt, model, filters)
    subq = inner_stmt.subquery()
    outer_stmt = (
        select(distinct(subq.c._val))
        .where(subq.c._val.isnot(None))
        .where(subq.c._val != "")
        .order_by(subq.c._val)
    )
    all_clients = [v for v in session.execute(outer_stmt).scalars().all() if v]
    return [c for c in all_clients if c.strip().upper().replace("_", " ") not in _SUMMARY_CLIENT_EXCLUDE]


def _distinct_filtered_summary_non_clients(session: Session, filters: DimensionamientoFilters) -> list[str]:
    """Universo No (is_client=False): nombres originales únicos desde summary con filtros activos.

    Fuente: cliente_visible de filas con is_client=False.
    En la summary table, cliente_visible para no-clientes es igual a cliente_nombre_original.
    Excluye SIN DATO, nulos y vacíos.
    """
    model = DimensionamientoFamilyMonthlySummary
    col = model.cliente_visible
    base_stmt = (
        select(col.label("_val"))
        .where(model.is_client.is_(False))
        .where(col.isnot(None))
        .where(func.coalesce(col, "") != "")
    )
    inner_stmt = _apply_common_filters(base_stmt, model, filters)
    subq = inner_stmt.subquery()
    outer_stmt = (
        select(distinct(subq.c._val))
        .where(subq.c._val.isnot(None))
        .where(subq.c._val != "")
        .order_by(subq.c._val)
    )
    all_names = [v for v in session.execute(outer_stmt).scalars().all() if v]
    return [c for c in all_names if c.strip().upper().replace("_", " ") not in _SUMMARY_CLIENT_EXCLUDE]


def _distinct_filtered_summary_values(
    session: Session, column, filters: DimensionamientoFilters
) -> list[str]:
    """Valores únicos de una columna de la tabla resumen aplicando filtros activos."""
    model = DimensionamientoFamilyMonthlySummary
    inner_stmt = _apply_common_filters(
        select(column.label("_val")).where(column.is_not(None)),
        model,
        filters,
    )
    subq = inner_stmt.subquery()
    outer_stmt = (
        select(distinct(subq.c._val))
        .where(subq.c._val.isnot(None))
        .order_by(subq.c._val)
    )
    return [v for v in session.execute(outer_stmt).scalars().all() if v not in (None, "")]


def _latest_success_import_run(session: Session) -> DimensionamientoImportRun | None:
    return session.execute(
        select(DimensionamientoImportRun)
        .where(DimensionamientoImportRun.status == "success")
        .order_by(DimensionamientoImportRun.finished_at.desc(), DimensionamientoImportRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def _get_dashboard_snapshot(session: Session, import_run_id: int | None = None) -> DimensionamientoDashboardSnapshot | None:
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None
    if import_run_id is None:
        return None
    # Busca por snapshot_key e import_run_id
    return session.execute(
        select(DimensionamientoDashboardSnapshot)
        .where(
            DimensionamientoDashboardSnapshot.snapshot_key == DEFAULT_DASHBOARD_SNAPSHOT_KEY,
            DimensionamientoDashboardSnapshot.import_run_id == import_run_id,
        )
        .limit(1)
    ).scalar_one_or_none()


def _snapshot_meta_payload(snapshot: DimensionamientoDashboardSnapshot | None) -> dict[str, Any]:
    return {
        "snapshot_key": DEFAULT_DASHBOARD_SNAPSHOT_KEY,
        "snapshot_version": DEFAULT_DASHBOARD_SNAPSHOT_VERSION,
        "generated_at": snapshot.generated_at.isoformat() if snapshot and snapshot.generated_at else None,
        "import_run_id": snapshot.import_run_id if snapshot else None,
    }


def _build_dashboard_bootstrap_payload(session: Session) -> dict[str, Any]:
    base_filters = build_filters()
    return {
        "status": get_status(session),
        "filters": get_filter_options(session, base_filters),
        "kpis": get_kpis(session, base_filters),
        "series": get_series(session, base_filters),
        "results": get_results_breakdown(session, base_filters),
        "top_families": get_top_families(session, base_filters),
        "geo": get_geography_distribution(session, base_filters),
        "clients_by_result": get_clients_by_result(session, base_filters, limit=10),
        "family_consumption": get_family_consumption_table(session, base_filters),
    }


def _family_consumption_payload_needs_refresh(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return True
    family_consumption = payload.get("family_consumption")
    if not isinstance(family_consumption, dict):
        return True
    rows = family_consumption.get("rows")
    if not isinstance(rows, list):
        return True
    if not isinstance(family_consumption.get("months"), list):
        return True
    total = family_consumption.get("total")
    if total is None:
        return True
    try:
        if int(total) != len(rows):
            return True
    except (TypeError, ValueError):
        return True
    return False


def _bootstrap_payload_supports_valorizacion(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False

    kpis = payload.get("kpis")
    if not isinstance(kpis, dict) or "valorizacion" not in kpis:
        return False

    series = payload.get("series")
    datasets = series.get("datasets") if isinstance(series, dict) else None
    if not isinstance(datasets, list):
        return False
    if datasets and "valorizacion" not in datasets[0]:
        return False

    for key in ("results", "top_families", "geo"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            return False
        if rows and "valorizacion" not in rows[0]:
            return False

    clients = payload.get("clients_by_result")
    if not isinstance(clients, list):
        return False
    if clients and "resultados_val" not in clients[0]:
        return False

    family_consumption = payload.get("family_consumption")
    fc_rows = family_consumption.get("rows") if isinstance(family_consumption, dict) else None
    if not isinstance(fc_rows, list):
        return False
    if fc_rows and "valorizacion" not in fc_rows[0]:
        return False

    return True


def _snapshot_payload_needs_refresh(snapshot: DimensionamientoDashboardSnapshot | None) -> bool:
    if snapshot is None:
        return True
    if snapshot.version != DEFAULT_DASHBOARD_SNAPSHOT_VERSION:
        return True
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    if _family_consumption_payload_needs_refresh(payload):
        return True
    return not _bootstrap_payload_supports_valorizacion(payload)


def _refresh_bootstrap_family_consumption(
    session: Session,
    payload: dict[str, Any],
    filters: DimensionamientoFilters,
) -> dict[str, Any]:
    refreshed = dict(payload)
    refreshed["family_consumption"] = get_family_consumption_table(session, filters)
    return refreshed


def refresh_default_dashboard_snapshot(
    session: Session,
    *,
    import_run_id: int | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    started_at = _log_query_start("refresh_default_dashboard_snapshot", import_run_id=import_run_id)
    latest = _latest_success_import_run(session)
    target_run_id = import_run_id if import_run_id is not None else (latest.id if latest else None)

    f = build_filters()
    f.import_run_id = target_run_id

    payload = get_dashboard_bootstrap(
        session,
        f,
        include_status=True,
        bypass_snapshot=True,
    )
    snapshot = _get_dashboard_snapshot(session, target_run_id)
    if snapshot is None:
        # El índice único en SQLite puede ser solo sobre snapshot_key (sin run_id),
        # así que buscamos primero por clave sola para reutilizar la fila existente
        # en lugar de intentar un INSERT que violaría el constraint.
        snapshot = session.execute(
            select(DimensionamientoDashboardSnapshot)
            .where(DimensionamientoDashboardSnapshot.snapshot_key == DEFAULT_DASHBOARD_SNAPSHOT_KEY)
            .limit(1)
        ).scalar_one_or_none()
    if snapshot is None:
        snapshot = DimensionamientoDashboardSnapshot(
            snapshot_key=DEFAULT_DASHBOARD_SNAPSHOT_KEY,
            version=DEFAULT_DASHBOARD_SNAPSHOT_VERSION,
            import_run_id=target_run_id,
        )
    payload["meta"] = {
        **_snapshot_meta_payload(snapshot),
        "source": "snapshot",
        "stale": False,
    }
    snapshot.version = DEFAULT_DASHBOARD_SNAPSHOT_VERSION
    snapshot.import_run_id = target_run_id
    snapshot.generated_at = dt.datetime.utcnow()
    payload["meta"].update(_snapshot_meta_payload(snapshot))
    snapshot.payload = payload
    session.add(snapshot)
    if commit:
        session.commit()
        session.refresh(snapshot)
        snapshot.payload["meta"].update(_snapshot_meta_payload(snapshot))
    _log_query_success(
        "refresh_default_dashboard_snapshot",
        started_at,
        import_run_id=snapshot.import_run_id,
    )
    return snapshot.payload


def ensure_default_dashboard_snapshot(session: Session) -> dict[str, Any] | None:
    latest = _latest_success_import_run(session)
    snapshot = _get_dashboard_snapshot(session, latest.id if latest else None)
    if latest is None:
        return snapshot.payload if snapshot else None
    if snapshot and snapshot.import_run_id == latest.id and not _snapshot_payload_needs_refresh(snapshot):
        return snapshot.payload
    return refresh_default_dashboard_snapshot(session, import_run_id=latest.id, commit=True)


def get_status(session: Session, import_run_id: int | None = None) -> dict[str, Any]:
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None

    now = time.perf_counter()
    cached = _STATUS_CACHE.get(import_run_id)
    if cached is not None and now - cached["ts"] < _TTL_STATUS:
        logger.debug("[DIM][CACHE] get_status hit")
        return cached["val"]

    started_at = now
    logger.info("[DIM][QUERY] get_status start run_id=%s", import_run_id)
    _apply_local_statement_timeout(session, 50000)

    if import_run_id is not None:
        run_obj = session.get(DimensionamientoImportRun, import_run_id)
    else:
        run_obj = None

    if run_obj is None:
        run_obj = _latest_success_import_run(session)

    if run_obj is not None:
        total_rows = session.execute(
            select(func.count(DimensionamientoRecord.id))
            .where(DimensionamientoRecord.import_run_id == run_obj.id)
        ).scalar_one()

        platform_rows = session.execute(
            select(
                DimensionamientoRecord.plataforma,
                func.count(DimensionamientoRecord.id),
            )
            .where(DimensionamientoRecord.import_run_id == run_obj.id)
            .group_by(DimensionamientoRecord.plataforma)
            .order_by(DimensionamientoRecord.plataforma)
        ).all()
    else:
        total_rows = 0
        platform_rows = []

    payload = {
        "has_data": total_rows > 0,
        "total_rows": total_rows,
        "platforms": [{"name": name, "rows": rows} for name, rows in platform_rows],
        "last_import": {
            "id": run_obj.id,
            "source_path": run_obj.source_path,
            "source_hash": run_obj.source_hash,
            "finished_at": run_obj.finished_at.isoformat() if run_obj.finished_at else None,
            "rows_processed": run_obj.rows_processed,
            "rows_inserted": run_obj.rows_inserted,
            "rows_updated": run_obj.rows_updated,
            "rows_rejected": run_obj.rows_rejected,
        }
        if run_obj
        else None,
    }
    _log_query_success("get_status", started_at, total_rows=total_rows, platforms=len(platform_rows))
    _STATUS_CACHE[import_run_id] = {
        "ts": time.perf_counter(),
        "val": payload
    }
    return payload


def get_debug_snapshot(session: Session, import_run_id: int | None = None) -> dict[str, Any]:
    if import_run_id is None:
        latest = _latest_success_import_run(session)
        import_run_id = latest.id if latest else None

    started_at = _log_query_start("get_debug_snapshot", import_run_id=import_run_id)
    _apply_local_statement_timeout(session, 50000)

    if import_run_id is None:
        return {
            "table": DimensionamientoRecord.__tablename__,
            "summary_table": DimensionamientoFamilyMonthlySummary.__tablename__,
            "columns": [column.name for column in DimensionamientoRecord.__table__.columns],
            "total_registros": 0,
            "count_distinct_plataforma": 0,
            "count_distinct_cliente_visible": 0,
            "count_distinct_familia": 0,
            "count_distinct_provincia": 0,
            "min_fecha": None,
            "max_fecha": None,
            "top_10_resultado_participacion": [],
            "sample_values": {},
        }

    total_rows = session.execute(
        select(func.count(DimensionamientoRecord.id))
        .where(DimensionamientoRecord.import_run_id == import_run_id)
    ).scalar_one()
    distinct_platforms = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.plataforma))))
        .where(DimensionamientoRecord.import_run_id == import_run_id)
    ).scalar_one()
    distinct_clients = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.cliente_visible))))
        .where(DimensionamientoRecord.import_run_id == import_run_id)
    ).scalar_one()
    distinct_families = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.familia))))
        .where(DimensionamientoRecord.import_run_id == import_run_id)
    ).scalar_one()
    distinct_provinces = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.provincia))))
        .where(DimensionamientoRecord.import_run_id == import_run_id)
    ).scalar_one()
    min_date, max_date = session.execute(
        select(func.min(DimensionamientoRecord.fecha), func.max(DimensionamientoRecord.fecha))
        .where(DimensionamientoRecord.import_run_id == import_run_id)
    ).one()
    top_results_stmt = (
        select(
            DimensionamientoRecord.resultado_participacion,
            func.count(DimensionamientoRecord.id).label("rows"),
        )
        .where(DimensionamientoRecord.import_run_id == import_run_id)
        .group_by(DimensionamientoRecord.resultado_participacion)
        .order_by(func.count(DimensionamientoRecord.id).desc(), DimensionamientoRecord.resultado_participacion.asc())
        .limit(10)
    )
    top_results = [
        {"resultado_participacion": resultado or "Sin resultado", "rows": rows or 0}
        for resultado, rows in session.execute(top_results_stmt).all()
    ]
    f = build_filters()
    f.import_run_id = import_run_id
    sample_values = {
        "plataformas": _distinct_values(session, DimensionamientoRecord.plataforma, f)[:10],
        "familias": _distinct_values(session, DimensionamientoRecord.familia, f)[:10],
        "provincias": _distinct_values(session, DimensionamientoRecord.provincia, f)[:10],
        "unidades_negocio": _distinct_values(session, DimensionamientoRecord.unidad_negocio, f)[:10],
        "subunidades_negocio": _distinct_values(session, DimensionamientoRecord.subunidad_negocio, f)[:10],
    }
    payload = {
        "table": DimensionamientoRecord.__tablename__,
        "summary_table": DimensionamientoFamilyMonthlySummary.__tablename__,
        "columns": [column.name for column in DimensionamientoRecord.__table__.columns],
        "total_registros": total_rows or 0,
        "count_distinct_plataforma": distinct_platforms or 0,
        "count_distinct_cliente_visible": distinct_clients or 0,
        "count_distinct_familia": distinct_families or 0,
        "count_distinct_provincia": distinct_provinces or 0,
        "min_fecha": min_date.isoformat() if min_date else None,
        "max_fecha": max_date.isoformat() if max_date else None,
        "top_10_resultado_participacion": top_results,
        "sample_values": sample_values,
    }
    _log_query_success(
        "get_debug_snapshot",
        started_at,
        total_registros=payload["total_registros"],
        top_resultados=len(top_results),
    )
    return payload


def get_filter_options(session: Session, filters: DimensionamientoFilters) -> dict[str, Any]:
    filters = _normalize_dashboard_filters(session, filters)
    # ── Caché: hit frecuente al limpiar/reutilizar un mismo set de filtros ──
    _ttl = _TTL_FILTER_OPTIONS_DFLT if not _has_active_filters(filters) else _TTL_FILTER_OPTIONS_FILT
    _ck = _make_cache_key("get_filter_options", filters)
    _hit = _cache_get(_ck, _ttl)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_filter_options hit key=%s", _ck)
        return _hit

    started_at = time.perf_counter()
    logger.info("[DIM][QUERY] get_filter_options start filters=%s", _filters_debug_dict(filters))
    _apply_local_statement_timeout(session, 50000)

    try:
        summary_state = _summary_health_snapshot_cached(session, filters.import_run_id)
        has_active_filters = _has_active_filters(filters)
        if (
            not has_active_filters
            and summary_state["usable"]
        ):
            logger.info(
                "[DIM][FILTERS] using summary clients path rows=%s min_month=%s max_month=%s",
                summary_state["rows"],
                summary_state["min_month"].isoformat(),
                summary_state["max_month"].isoformat(),
            )
            # Sin filtros activos: el dropdown Cliente lista TODAS las entidades resueltas
            # (256: [{id,label}]), una por entidad, con etiqueta desambiguada. Misma fuente
            # y resolución que la card (única fuente de verdad).
            payload = {
                "clientes": _client_dropdown(session, filters),
                "provincias": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.provincia, filters.import_run_id),
                "familias": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.familia, filters.import_run_id),
                "plataformas": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.plataforma, filters.import_run_id),
                "unidades_negocio": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.unidad_negocio, filters.import_run_id),
                "subunidades_negocio": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.subunidad_negocio, filters.import_run_id),
                "resultados": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.resultado_participacion, filters.import_run_id),
                "date_range": _date_range_payload(
                    summary_state["min_month"],
                    summary_state["max_month"],
                ),
            }
        else:
            applied_conditions: list[str] = []
            use_summary = summary_state["usable"]
            filt_model = DimensionamientoFamilyMonthlySummary if use_summary else DimensionamientoRecord
            if use_summary:
                logger.info(
                    "[DIM][FILTERS] using summary clients path rows=%s min_month=%s max_month=%s",
                    summary_state["rows"],
                    summary_state["min_month"].isoformat(),
                    summary_state["max_month"].isoformat(),
                )
            else:
                logger.info(
                    "[DIM][FILTERS] using distinct_visible_clients base path reason=summary_unavailable summary_rows=%s raw_min_month=%r raw_max_month=%r",
                    summary_state["rows"],
                    summary_state["raw_min_month"],
                    summary_state["raw_max_month"],
                )
            date_col = _get_date_column(filt_model)
            date_aggregate_col = cast(date_col, Text) if (use_summary and IS_SQLITE) else date_col
            date_bounds_stmt = _apply_common_filters(
                select(func.min(date_aggregate_col), func.max(date_aggregate_col)),
                filt_model,
                filters,
                applied_conditions,
            )
            _log_query_statement(session, "get_filter_options.date_bounds", filt_model, date_bounds_stmt, filters, applied_conditions)
            min_date, max_date = session.execute(date_bounds_stmt).one()

            if use_summary:
                s = DimensionamientoFamilyMonthlySummary
                # Dropdown Cliente: entidades resueltas presentes bajo los filtros activos,
                # restringidas por ¿Cliente? (None=256 · Sí=158 · No=98). Fuente única.
                clientes_payload = _client_dropdown(session, filters)
                logger.info("[DIM][FILTERS] client dropdown (summary): %d entidades (is_client=%s)", len(clientes_payload), filters.is_client)
                payload = {
                    "clientes": clientes_payload,
                    "provincias": _distinct_filtered_summary_values(session, s.provincia, filters),
                    "familias": _distinct_filtered_summary_values(session, s.familia, filters),
                    "plataformas": _distinct_filtered_summary_values(session, s.plataforma, filters),
                    "unidades_negocio": _distinct_filtered_summary_values(session, s.unidad_negocio, filters),
                    "subunidades_negocio": _distinct_filtered_summary_values(session, s.subunidad_negocio, filters),
                    "resultados": _distinct_filtered_summary_values(session, s.resultado_participacion, filters),
                    "date_range": _date_range_payload(min_date, max_date),
                }
            else:
                # Tabla base (summary no disponible): misma lógica de entidades resueltas.
                clientes_payload = _client_dropdown(session, filters)
                logger.info("[DIM][FILTERS] client dropdown (base table): %d entidades (is_client=%s)", len(clientes_payload), filters.is_client)
                payload = {
                    "clientes": clientes_payload,
                    "provincias": _distinct_values(session, DimensionamientoRecord.provincia, filters),
                    "familias": _distinct_values(session, DimensionamientoRecord.familia, filters),
                    "plataformas": _distinct_values(session, DimensionamientoRecord.plataforma, filters),
                    "unidades_negocio": _distinct_values(session, DimensionamientoRecord.unidad_negocio, filters),
                    "subunidades_negocio": _distinct_values(session, DimensionamientoRecord.subunidad_negocio, filters),
                    "resultados": _distinct_values(session, DimensionamientoRecord.resultado_participacion, filters),
                    "date_range": _date_range_payload(min_date, max_date),
                }

        _log_query_success(
            "get_filter_options",
            started_at,
            clientes=len(payload["clientes"]),
            provincias=len(payload["provincias"]),
            familias=len(payload["familias"]),
            plataformas=len(payload["plataformas"]),
            unidades_negocio=len(payload["unidades_negocio"]),
            subunidades_negocio=len(payload["subunidades_negocio"]),
            resultados=len(payload["resultados"]),
        )
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_filter_options failed filters=%s", _filters_debug_dict(filters))
        raise


def _entity_registry(session: Session, import_run_id: int | None) -> dict[str, Any]:
    """Registro de entidades resueltas de la corrida, con etiquetas desambiguadas.

    Cacheado por corrida. Desambiguación cuando dos entidades comparten nombre canónico
    (ignorando espacios): sufijo por provincia si difieren; si no, por CUIT (últimos 4);
    huérfana sin CUIT → "(sin CUIT)".
    """
    cached = _ENTITY_REGISTRY_CACHE.get(import_run_id)
    if cached is not None:
        return cached
    by_key: dict[int, dict[str, Any]] = {}
    if import_run_id is not None:
        rows = session.execute(
            select(
                DimensionamientoClienteEntidad.entidad_key,
                DimensionamientoClienteEntidad.es_cliente,
                DimensionamientoClienteEntidad.nombre_visible,
                DimensionamientoClienteEntidad.provincia,
                DimensionamientoClienteEntidad.cuits,
            ).where(DimensionamientoClienteEntidad.import_run_id == import_run_id)
        ).all()
        for key, es_cli, vis, prov, cuits in rows:
            try:
                cuit_list = json.loads(cuits) if cuits else []
            except (ValueError, TypeError):
                cuit_list = []
            by_key[key] = {
                "key": key,
                "es_cliente": bool(es_cli),
                "nombre": vis or "",
                "provincia": prov,
                "cuits": cuit_list,
                "label": vis or "",
            }
    # Desambiguación por nombre canónico sin espacios
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in by_key.values():
        groups[_entity_canon(e["nombre"]).replace(" ", "")].append(e)
    for grp in groups.values():
        if len(grp) <= 1:
            continue
        def _real_prov(e):
            p = (e["provincia"] or "").strip()
            return p if p and p.upper() != "SIN DATO" else ""
        provs = [_real_prov(e) for e in grp]
        provs_disambiguate = all(provs) and len(set(provs)) == len(provs)
        for e in grp:
            p = _real_prov(e)
            if provs_disambiguate:
                e["label"] = f"{e['nombre']} ({p})"
            elif e["cuits"]:
                e["label"] = f"{e['nombre']} (CUIT …{str(e['cuits'][0])[-4:]})"
            elif p:
                e["label"] = f"{e['nombre']} ({p})"
            else:
                e["label"] = f"{e['nombre']} (sin CUIT)"
    result = {
        "by_key": by_key,
        "client_keys": {k for k, e in by_key.items() if e["es_cliente"]},
        "noclient_keys": {k for k, e in by_key.items() if not e["es_cliente"]},
    }
    _ENTITY_REGISTRY_CACHE[import_run_id] = result
    return result


def _present_entity_ids(session: Session, filters: DimensionamientoFilters) -> set[int]:
    """Ids de entidad presentes bajo los filtros activos, IGNORANDO ¿Cliente?.

    Se ignora is_client para poder computar total/Sí/No de una sola pasada (la
    clasificación Sí/No sale del registry, no de la fila)."""
    model = _resolve_aggregate_model(session, "CLIENT_ENTITY", filters.import_run_id)
    f = _clone_filters(filters)
    f.is_client = None
    stmt = _apply_common_filters(
        select(distinct(model.cliente_entidad_id)).where(model.cliente_entidad_id.isnot(None)),
        model,
        f,
    )
    return {r for (r,) in session.execute(stmt) if r is not None}


def _client_entity_counts(session: Session, filters: DimensionamientoFilters) -> tuple[int, int, int]:
    """(total, si, no) de entidades-cliente bajo los filtros (Sí/No por registry)."""
    reg = _entity_registry(session, filters.import_run_id)
    present = _present_entity_ids(session, filters)
    si = len(present & reg["client_keys"])
    no = len(present & reg["noclient_keys"])
    total = len(present)
    return total, si, no


def _client_dropdown(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    """Opciones del desplegable Cliente: [{id, label}] de entidades presentes bajo los
    filtros activos (ignorando la propia selección de cliente), restringidas por ¿Cliente?."""
    reg = _entity_registry(session, filters.import_run_id)
    f = _clone_filters(filters)
    f.cliente_entidad_ids = []
    f.clientes = []
    present = _present_entity_ids(session, f)
    if filters.is_client is True:
        present &= reg["client_keys"]
    elif filters.is_client is False:
        present &= reg["noclient_keys"]
    items = [
        {"value": k, "label": reg["by_key"][k]["label"]}
        for k in present
        if k in reg["by_key"]
    ]
    items.sort(key=lambda d: d["label"].upper())
    return items


def get_kpis(session: Session, filters: DimensionamientoFilters) -> dict[str, Any]:
    filters = _normalize_dashboard_filters(session, filters)
    _ck = _make_cache_key("get_kpis", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_kpis hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_kpis", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, "KPIS", filters.import_run_id)
        applied_conditions: list[str] = []

        if model is DimensionamientoRecord:
            # Tabla de detalle: KPIs por fila (los clientes se cuentan por entidad, aparte).
            stmt = _apply_common_filters(
                select(
                    func.count(model.id),
                    func.count(model.id),
                    func.count(distinct(model.familia)),
                    func.count(distinct(model.provincia)),
                    func.coalesce(func.sum(model.valorizacion_estimada), 0),
                ),
                model,
                filters,
                applied_conditions,
            )
            _log_query_statement(session, "get_kpis", model, stmt, filters, applied_conditions)
            total_rows, total_records, total_families, total_provincias, total_valorizacion = session.execute(stmt).one()
        else:
            # Tabla resumen: 1 query para totales/familias (clientes = entidades, aparte).
            main_stmt = _apply_common_filters(
                select(
                    func.coalesce(func.sum(model.total_registros), 0),
                    func.count(distinct(model.familia)),
                    func.count(distinct(model.provincia)),
                    func.coalesce(func.sum(model.total_valorizacion), 0),
                ),
                model,
                filters,
                applied_conditions,
            )
            _log_query_statement(session, "get_kpis", model, main_stmt, filters, applied_conditions)
            total_rows, total_families, total_provincias, total_valorizacion = session.execute(main_stmt).one()
            total_records = total_rows

        # Clientes = ENTIDADES resueltas (cada entidad cuenta una vez, colapsando plataformas
        # y formas de escritura). El desglose Sí/No sale del registry; el filtro ¿Cliente?
        # elige qué número encabeza la card y el desglose muestra la composición completa.
        total_entities, clientes_si, clientes_no = _client_entity_counts(session, filters)
        if filters.is_client is True:
            total_clients = clientes_si
        elif filters.is_client is False:
            total_clients = clientes_no
        else:
            total_clients = total_entities

        payload = {
            "total_rows": int(total_rows or 0),
            "clientes": int(total_clients or 0),
            "clientes_si": int(clientes_si),
            "clientes_no": int(clientes_no),
            "renglones": int(total_records or 0),
            "familias": int(total_families or 0),
            "provincias": int(total_provincias or 0),
            "valorizacion": float(total_valorizacion or 0),
        }
        _log_query_success(
            "get_kpis", started_at, total_rows=payload["total_rows"],
            clientes=payload["clientes"], clientes_si=clientes_si, clientes_no=clientes_no,
        )
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_kpis failed filters=%s", _filters_debug_dict(filters))
        raise


def get_series(session: Session, filters: DimensionamientoFilters, limit: int = 5) -> dict[str, Any]:
    filters = _normalize_dashboard_filters(session, filters)
    _ck = _make_cache_key("get_series", filters, limit=limit)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_series hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_series", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, "SERIES", filters.import_run_id)
        negocio_expr = func.coalesce(model.unidad_negocio, "Sin negocio")
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        val_expr = func.coalesce(
            func.sum(model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion), 0
        )
        top_business_conditions: list[str] = []
        top_business_stmt = _apply_common_filters(
            select(
                negocio_expr.label("negocio"),
                count_expr.label("renglones"),
            )
            .group_by(negocio_expr)
            .order_by(count_expr.desc(), negocio_expr.asc())
            .limit(limit),
            model,
            filters,
            top_business_conditions,
        )
        _log_query_statement(session, "get_series.top_businesses", model, top_business_stmt, filters, top_business_conditions)
        top_businesses = [row[0] for row in session.execute(top_business_stmt).all()]

        if model is DimensionamientoRecord:
            month_expr = cast(_month_expr(model.fecha), Date) if not IS_SQLITE else func.date(model.fecha, "start of month")
        else:
            month_expr = cast(model.month, Text) if IS_SQLITE else model.month
        series_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                month_expr.label("month"),
                negocio_expr.label("negocio"),
                count_expr.label("renglones"),
                val_expr.label("valorizacion"),
            )
            .where(negocio_expr.in_(top_businesses) if top_businesses else False)
            .group_by(month_expr, negocio_expr)
            .order_by(month_expr.asc(), negocio_expr.asc()),
            model,
            filters,
            series_conditions,
        )
        if top_businesses:
            series_conditions.append(f"negocio IN {top_businesses}")
        _log_query_statement(session, "get_series", model, stmt, filters, series_conditions)
        series_map: dict[str, dict[str, float]] = {}
        series_val_map: dict[str, dict[str, float]] = {}
        for month, negocio, total, val in session.execute(stmt).all():
            month_key = _month_value_to_iso(month)
            series_map.setdefault(month_key, {})[negocio] = float(total or 0)
            series_val_map.setdefault(month_key, {})[negocio] = float(val or 0)

        months = sorted(series_map.keys())
        datasets = []
        for negocio in top_businesses:
            datasets.append(
                {
                    "label": negocio,
                    "values": [series_map.get(month, {}).get(negocio, 0) for month in months],
                    "valorizacion": [series_val_map.get(month, {}).get(negocio, 0.0) for month in months],
                }
            )
        payload = {"months": months, "datasets": datasets}
        _log_query_success("get_series", started_at, months=len(months), datasets=len(datasets))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_series failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise


def get_results_breakdown(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    filters = _normalize_dashboard_filters(session, filters)
    _ck = _make_cache_key("get_results_breakdown", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_results_breakdown hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_results_breakdown", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, "RESULTS", filters.import_run_id)
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        val_expr = func.coalesce(
            func.sum(model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion), 0
        )
        applied_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                model.resultado_participacion,
                count_expr.label("rows"),
                val_expr.label("valorizacion"),
            )
            .group_by(model.resultado_participacion)
            .order_by(count_expr.desc()),
            model,
            filters,
            applied_conditions,
        )
        _log_query_statement(session, "get_results_breakdown", model, stmt, filters, applied_conditions)
        payload = [
            {
                "resultado": resultado or "Sin resultado",
                "renglones": rows or 0,
                "valorizacion": float(val or 0),
            }
            for resultado, rows, val in session.execute(stmt).all()
        ]
        _log_query_success("get_results_breakdown", started_at, rows=len(payload))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_results_breakdown failed filters=%s", _filters_debug_dict(filters))
        raise


def get_top_families(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    filters = _normalize_dashboard_filters(session, filters)
    _ck = _make_cache_key("get_top_families", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_top_families hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_top_families", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, "TOP_FAMILIES", filters.import_run_id)
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        quantity_expr = func.coalesce(
            func.sum(model.cantidad_demandada if model is DimensionamientoRecord else model.total_cantidad),
            0,
        )
        val_expr = func.coalesce(
            func.sum(model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion), 0
        )
        applied_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                model.familia,
                count_expr.label("rows"),
                quantity_expr.label("quantity"),
                val_expr.label("valorizacion"),
            )
            .where(model.familia.is_not(None))
            .group_by(model.familia)
            .order_by(count_expr.desc(), model.familia.asc()),
            model,
            filters,
            applied_conditions,
        )
        _log_query_statement(session, "get_top_families", model, stmt, filters, applied_conditions)
        payload = [
            {
                "familia": familia or "Sin familia",
                "renglones": rows or 0,
                "cantidad": float(quantity or 0),
                "valorizacion": float(val or 0),
            }
            for familia, rows, quantity, val in session.execute(stmt).all()
        ]
        _log_query_success("get_top_families", started_at, rows=len(payload))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_top_families failed filters=%s", _filters_debug_dict(filters))
        raise


def get_geography_distribution(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    filters = _normalize_dashboard_filters(session, filters)
    _ck = _make_cache_key("get_geography_distribution", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_geography_distribution hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_geography_distribution", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, "GEO", filters.import_run_id)
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        val_expr = func.coalesce(
            func.sum(model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion), 0
        )
        applied_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                model.provincia,
                count_expr.label("rows"),
                val_expr.label("valorizacion"),
            )
            .group_by(model.provincia)
            .order_by(count_expr.desc()),
            model,
            filters,
            applied_conditions,
        )
        _log_query_statement(session, "get_geography_distribution", model, stmt, filters, applied_conditions)
        payload = [
            {
                "provincia": provincia or "Sin provincia",
                "renglones": rows or 0,
                "valorizacion": float(val or 0),
            }
            for provincia, rows, val in session.execute(stmt).all()
        ]
        _log_query_success("get_geography_distribution", started_at, rows=len(payload))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_geography_distribution failed filters=%s", _filters_debug_dict(filters))
        raise


def get_clients_by_result(
    session: Session,
    filters: DimensionamientoFilters,
    limit: int = 10,
) -> list[dict[str, Any]]:
    filters = _normalize_dashboard_filters(session, filters)
    _ck = _make_cache_key("get_clients_by_result", filters, limit=limit)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_clients_by_result hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_clients_by_result", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(
            session,
            "CLIENTS_BY_RESULT",
            filters.import_run_id,
            summary_message="using summary path",
            base_message="using base path",
        )
        subquery_conditions: list[str] = []

        if model is DimensionamientoRecord:
            # Tabla de detalle
            _visible_raw = model.cliente_visible
            total_expr = func.count(model.id)
            val_sub_expr = func.coalesce(func.sum(model.valorizacion_estimada), 0)
            subquery = _apply_common_filters(
                select(
                    _visible_raw.label("cliente"),
                    model.resultado_participacion.label("resultado"),
                    total_expr.label("total"),
                    val_sub_expr.label("val_total"),
                )
                .where(_visible_raw.isnot(None))
                .where(func.coalesce(_visible_raw, "") != "")
                .group_by(_visible_raw, model.resultado_participacion),
                model,
                filters,
                subquery_conditions,
            ).subquery()
        else:
            total_expr = func.coalesce(func.sum(model.total_registros), 0)
            val_sub_expr = func.coalesce(func.sum(model.total_valorizacion), 0)
            visible_client = model.cliente_visible
            summary_stmt = (
                select(
                    visible_client.label("cliente"),
                    model.resultado_participacion.label("resultado"),
                    total_expr.label("total"),
                    val_sub_expr.label("val_total"),
                )
                .where(visible_client.isnot(None))
                .where(visible_client != "")
                .group_by(visible_client, model.resultado_participacion)
            )
            if filters.is_client is None:
                summary_stmt = summary_stmt.where(model.is_client == True)  # noqa: E712
            subquery = _apply_common_filters(
                summary_stmt,
                model,
                filters,
                subquery_conditions,
            ).subquery()
        _log_query_statement(session, "get_clients_by_result.subquery", model, select(subquery), filters, subquery_conditions)

        top_clients_stmt = select(
            subquery.c.cliente,
            func.sum(subquery.c.total).label("grand_total"),
        ).group_by(subquery.c.cliente).order_by(func.sum(subquery.c.total).desc()).limit(limit)
        _log_query_statement(session, "get_clients_by_result.top_clients", model, top_clients_stmt, filters, subquery_conditions)
        top_clients = [row[0] for row in session.execute(top_clients_stmt).all()]
        if not top_clients:
            _log_query_success("get_clients_by_result", started_at, rows=0)
            return []

        detail_stmt = (
            select(subquery.c.cliente, subquery.c.resultado, subquery.c.total, subquery.c.val_total)
            .where(subquery.c.cliente.in_(top_clients))
            .order_by(subquery.c.cliente.asc(), subquery.c.resultado.asc())
        )
        detail_conditions = list(subquery_conditions)
        detail_conditions.append(f"cliente IN {top_clients}")
        _log_query_statement(session, "get_clients_by_result.detail", model, detail_stmt, filters, detail_conditions)

        client_map: dict[str, dict[str, float]] = {}
        client_val_map: dict[str, dict[str, float]] = {}
        for cliente, resultado, total, val_total in session.execute(detail_stmt).all():
            result_key = resultado or "Sin resultado"
            client_map.setdefault(cliente, {})[result_key] = float(total or 0)
            client_val_map.setdefault(cliente, {})[result_key] = float(val_total or 0)

        payload = [
            {
                "cliente": cliente,
                "resultados": client_map.get(cliente, {}),
                "resultados_val": client_val_map.get(cliente, {}),
            }
            for cliente in top_clients
        ]
        _log_query_success("get_clients_by_result", started_at, rows=len(payload))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_clients_by_result failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise


def get_family_consumption_table(
    session: Session,
    filters: DimensionamientoFilters,
) -> dict[str, Any]:
    filters = _normalize_dashboard_filters(session, filters)
    _ck = _make_cache_key("get_family_consumption_table", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_family_consumption_table hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_family_consumption_table", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, "FAMILY_CONSUMPTION", filters.import_run_id)
        total_expr = func.coalesce(
            func.sum(model.cantidad_demandada if model is DimensionamientoRecord else model.total_cantidad),
            0,
        )
        val_expr = func.coalesce(
            func.sum(model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion), 0
        )

        # Paso 1: obtener el universo completo de familias ordenado por cantidad total.
        applied_conditions: list[str] = []
        family_totals_stmt = _apply_common_filters(
            select(
                model.familia,
                total_expr.label("grand_total"),
            )
            .where(model.familia.is_not(None))
            .group_by(model.familia)
            .order_by(total_expr.desc(), model.familia.asc()),
            model,
            filters,
            applied_conditions,
        )
        _log_query_statement(session, "get_family_consumption_table.family_totals", model, family_totals_stmt, filters, applied_conditions)
        family_totals_rows = session.execute(family_totals_stmt).all()
        if not family_totals_rows:
            payload = {
                "months": [],
                "rows": [],
                "total": 0,
            }
            _log_query_success("get_family_consumption_table", started_at, months=0, rows=0, total=0)
            return payload

        ordered_families = [family or "Sin familia" for family, _ in family_totals_rows]

        # Paso 2: consumir el detalle mensual completo y calcular el promedio real por mes entre años.
        if model is DimensionamientoRecord:
            month_bucket = cast(_month_expr(model.fecha), Date) if not IS_SQLITE else func.date(model.fecha, "start of month")
        else:
            month_bucket = cast(model.month, Text) if IS_SQLITE else model.month
        monthly_conditions: list[str] = []
        monthly_stmt = _apply_common_filters(
            select(
                month_bucket.label("month_bucket"),
                model.familia,
                total_expr.label("total"),
                val_expr.label("valorizacion"),
            )
            .where(model.familia.is_not(None))
            .group_by(month_bucket, model.familia)
            .order_by(month_bucket.asc(), model.familia.asc()),
            model,
            filters,
            monthly_conditions,
        )
        _log_query_statement(session, "get_family_consumption_table.monthly", model, monthly_stmt, filters, monthly_conditions)
        rows = session.execute(monthly_stmt).all()

        month_keys = [f"{index:02d}" for index in range(1, 13)]
        monthly_values: dict[str, dict[str, list[float]]] = {}
        monthly_val_values: dict[str, dict[str, list[float]]] = {}
        for month_date, family, total, val in rows:
            month_iso = _month_value_to_iso(month_date)
            month_key = month_iso[5:7] if len(month_iso) >= 7 else "01"
            family_name = family or "Sin familia"
            monthly_values.setdefault(family_name, {}).setdefault(month_key, []).append(float(total or 0))
            monthly_val_values.setdefault(family_name, {}).setdefault(month_key, []).append(float(val or 0))

        data = []
        for family in ordered_families:
            family_months = monthly_values.get(family, {})
            family_val_months = monthly_val_values.get(family, {})
            data.append(
                {
                    "familia": family,
                    "values": [
                        (
                            sum(family_months.get(month, [])) / len(family_months.get(month, []))
                            if family_months.get(month)
                            else 0
                        )
                        for month in month_keys
                    ],
                    "valorizacion": [
                        (
                            sum(family_val_months.get(month, [])) / len(family_val_months.get(month, []))
                            if family_val_months.get(month)
                            else 0
                        )
                        for month in month_keys
                    ],
                }
            )
        payload = {
            "months": month_keys,
            "rows": data,
            "total": len(data),
        }
        _log_query_success("get_family_consumption_table", started_at, months=len(month_keys), rows=len(data), total=len(data))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_family_consumption_table failed filters=%s", _filters_debug_dict(filters))
        raise


def _real_client_name(value: str | None) -> str | None:
    # Retorna el nombre visible para cualquier fila, sin importar is_client.
    # Solo excluimos vacíos y variantes de "SIN DATO".
    text_value = str(value or "").strip()
    if not text_value:
        return None
    if text_value.upper().replace("_", " ") in _SUMMARY_CLIENT_EXCLUDE:
        return None
    return text_value


def _fetch_summary_rows_for_bootstrap(
    session: Session,
    filters: DimensionamientoFilters,
) -> list[tuple[Any, ...]]:
    model = DimensionamientoFamilyMonthlySummary
    applied_conditions: list[str] = []
    stmt = _apply_common_filters(
        select(
            model.month,
            model.plataforma,
            model.cliente_visible,
            model.provincia,
            model.familia,
            model.unidad_negocio,
            model.subunidad_negocio,
            model.resultado_participacion,
            model.is_identified,
            model.is_client,
            model.total_cantidad,
            model.total_valorizacion,
            model.total_registros,
        ),
        model,
        filters,
        applied_conditions,
    )
    _log_query_statement(session, "get_dashboard_bootstrap.summary_rows", model, stmt, filters, applied_conditions)
    return session.execute(stmt).all()


def _compute_series_from_rows(rows: list[tuple[Any, ...]], series_limit: int = 5) -> dict[str, Any]:
    """Calcula solo el widget 'series' (evolución mensual por negocio) desde un listado de rows.

    Se usa cuando hay negocios excluidos: el gráfico de series siempre recibe los rows
    SIN la exclusión, para que el usuario pueda ver y re-activar las series excluidas
    desde la leyenda. El resto del dashboard usa los rows CON exclusión.

    Posiciones en la tupla según la SELECT en _fetch_summary_rows_for_bootstrap:
        0: month, 5: unidad_negocio, 11: total_valorizacion, 12: total_registros
    """
    business_totals: dict[str, int] = defaultdict(int)
    business_by_month: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    business_val_by_month: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        month_value = row[0]
        unidad_negocio = row[5]
        total_valorizacion = row[11]
        total_registros = row[12]
        row_count = int(total_registros or 0)
        val_amount = float(total_valorizacion or 0)
        month_iso = _month_value_to_iso(month_value)
        business_name = unidad_negocio or "Sin negocio"
        business_totals[business_name] += row_count
        business_by_month[month_iso][business_name] += row_count
        business_val_by_month[month_iso][business_name] += val_amount
    top_businesses = [
        name
        for name, _ in sorted(business_totals.items(), key=lambda item: (-item[1], item[0]))[:series_limit]
    ]
    months = sorted(business_by_month.keys())
    return {
        "months": months,
        "datasets": [
            {
                "label": business_name,
                "values": [business_by_month.get(month, {}).get(business_name, 0) for month in months],
                "valorizacion": [business_val_by_month.get(month, {}).get(business_name, 0.0) for month in months],
            }
            for business_name in top_businesses
        ],
    }


def _aggregate_bootstrap_from_summary_rows(
    rows: list[tuple[Any, ...]],
    *,
    series_limit: int = 5,
    clients_limit: int = 10,
    series_rows: list[tuple[Any, ...]] | None = None,
) -> dict[str, Any]:
    distinct_clients: set[str] = set()
    distinct_provincias: set[str] = set()
    distinct_familias: set[str] = set()
    distinct_plataformas: set[str] = set()
    distinct_unidades: set[str] = set()
    distinct_subunidades: set[str] = set()
    distinct_resultados: set[str] = set()

    total_rows = 0
    total_quantity = 0.0
    total_valorizacion_kpi = 0.0
    min_month: dt.date | None = None
    max_month: dt.date | None = None

    business_totals: dict[str, int] = defaultdict(int)
    business_by_month: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    business_val_by_month: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    result_totals: dict[str, int] = defaultdict(int)
    result_val_totals: dict[str, float] = defaultdict(float)
    family_rows: dict[str, int] = defaultdict(int)
    family_quantities: dict[str, float] = defaultdict(float)
    family_val_totals: dict[str, float] = defaultdict(float)
    family_consumption_quantities: dict[str, float] = defaultdict(float)
    family_consumption_val: dict[str, float] = defaultdict(float)
    geo_totals: dict[str, int] = defaultdict(int)
    geo_val_totals: dict[str, float] = defaultdict(float)
    client_totals: dict[str, int] = defaultdict(int)
    client_val_totals: dict[str, float] = defaultdict(float)
    client_result_totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    client_result_val_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    family_month_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    family_consumption_month_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    family_val_month_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    family_consumption_val_month_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for (
        month_value,
        plataforma,
        cliente,
        provincia,
        familia,
        unidad_negocio,
        subunidad_negocio,
        resultado,
        _is_identified,
        is_client,
        total_cantidad,
        total_valorizacion,
        total_registros,
    ) in rows:
        month_date = _coerce_date_value(month_value)
        if month_date is not None:
            min_month = month_date if min_month is None or month_date < min_month else min_month
            max_month = month_date if max_month is None or month_date > max_month else max_month
        month_iso = _month_value_to_iso(month_value)

        row_count = int(total_registros or 0)
        quantity = float(total_cantidad or 0)
        val_amount = float(total_valorizacion or 0)
        total_rows += row_count
        total_quantity += quantity
        total_valorizacion_kpi += val_amount

        if plataforma not in (None, ""):
            distinct_plataformas.add(plataforma)
        if provincia not in (None, ""):
            distinct_provincias.add(provincia)
        if familia not in (None, ""):
            distinct_familias.add(familia)
        if unidad_negocio not in (None, ""):
            distinct_unidades.add(unidad_negocio)
        if subunidad_negocio not in (None, ""):
            distinct_subunidades.add(subunidad_negocio)
        if resultado not in (None, ""):
            distinct_resultados.add(resultado)

        family_name = familia or "Sin familia"
        business_name = unidad_negocio or "Sin negocio"
        province_name = provincia or "Sin provincia"
        result_name = resultado or "Sin resultado"

        family_rows[family_name] += row_count
        family_quantities[family_name] += quantity
        family_val_totals[family_name] += val_amount
        business_totals[business_name] += row_count
        business_by_month[month_iso][business_name] += row_count
        business_val_by_month[month_iso][business_name] += val_amount
        result_totals[result_name] += row_count
        result_val_totals[result_name] += val_amount
        geo_totals[province_name] += row_count
        geo_val_totals[province_name] += val_amount
        family_month_totals[family_name][month_iso] += quantity
        family_val_month_totals[family_name][month_iso] += val_amount
        if familia not in (None, ""):
            family_consumption_quantities[family_name] += quantity
            family_consumption_val[family_name] += val_amount
            family_consumption_month_totals[family_name][month_iso] += quantity
            family_consumption_val_month_totals[family_name][month_iso] += val_amount

        client_name = _real_client_name(cliente)
        if client_name:
            distinct_clients.add(client_name)
            client_totals[client_name] += row_count
            client_val_totals[client_name] += val_amount
            client_result_totals[client_name][result_name] += row_count
            client_result_val_totals[client_name][result_name] += val_amount

    top_businesses = [
        name
        for name, _ in sorted(business_totals.items(), key=lambda item: (-item[1], item[0]))[:series_limit]
    ]
    months = sorted(business_by_month.keys())
    series = {
        "months": months,
        "datasets": [
            {
                "label": business_name,
                "values": [business_by_month.get(month, {}).get(business_name, 0) for month in months],
                "valorizacion": [business_val_by_month.get(month, {}).get(business_name, 0.0) for month in months],
            }
            for business_name in top_businesses
        ],
    }

    # Si se proveyeron rows sin exclusión de negocios para el gráfico de series,
    # sobrescribir con esos datos para que el usuario vea y pueda reactivar todas las series.
    if series_rows is not None:
        series = _compute_series_from_rows(series_rows, series_limit)

    results_payload = [
        {"resultado": result_name, "renglones": rows_count, "valorizacion": float(result_val_totals.get(result_name, 0))}
        for result_name, rows_count in sorted(result_totals.items(), key=lambda item: (-item[1], item[0]))
    ]

    top_families_payload = [
        {
            "familia": family_name,
            "renglones": rows_count,
            "cantidad": float(family_quantities.get(family_name, 0)),
            "valorizacion": float(family_val_totals.get(family_name, 0)),
        }
        for family_name, rows_count in sorted(family_rows.items(), key=lambda item: (-item[1], item[0]))
    ]

    geo_payload = [
        {"provincia": province_name, "renglones": rows_count, "valorizacion": float(geo_val_totals.get(province_name, 0))}
        for province_name, rows_count in sorted(geo_totals.items(), key=lambda item: (-item[1], item[0]))
    ]

    top_clients = [
        client_name
        for client_name, _ in sorted(client_totals.items(), key=lambda item: (-item[1], item[0]))[:clients_limit]
    ]
    clients_payload = [
        {
            "cliente": client_name,
            "resultados": {
                result_name: rows_count
                for result_name, rows_count in sorted(client_result_totals.get(client_name, {}).items(), key=lambda item: item[0])
            },
            "resultados_val": {
                result_name: float(val)
                for result_name, val in sorted(client_result_val_totals.get(client_name, {}).items(), key=lambda item: item[0])
            },
        }
        for client_name in top_clients
    ]

    month_keys = [f"{index:02d}" for index in range(1, 13)]
    family_consumption_payload = []
    ordered_family_consumption = [
        family_name
        for family_name, _ in sorted(family_consumption_quantities.items(), key=lambda item: (-item[1], item[0]))
    ]
    for family_name in ordered_family_consumption:
        month_lists: dict[str, list[float]] = defaultdict(list)
        val_month_lists: dict[str, list[float]] = defaultdict(list)
        for month_iso_key, quantity in family_consumption_month_totals.get(family_name, {}).items():
            month_key = month_iso_key[5:7] if len(month_iso_key) >= 7 else "01"
            month_lists[month_key].append(quantity)
        for month_iso_key, val in family_consumption_val_month_totals.get(family_name, {}).items():
            month_key = month_iso_key[5:7] if len(month_iso_key) >= 7 else "01"
            val_month_lists[month_key].append(val)
        family_consumption_payload.append(
            {
                "familia": family_name,
                "values": [
                    (
                        sum(month_lists.get(month_key, [])) / len(month_lists.get(month_key, []))
                        if month_lists.get(month_key)
                        else 0
                    )
                    for month_key in month_keys
                ],
                "valorizacion": [
                    (
                        sum(val_month_lists.get(month_key, [])) / len(val_month_lists.get(month_key, []))
                        if val_month_lists.get(month_key)
                        else 0
                    )
                    for month_key in month_keys
                ],
            }
        )

    return {
        "filters": {
            "clientes": sorted(distinct_clients),
            "provincias": sorted(distinct_provincias),
            "familias": sorted(distinct_familias),
            "plataformas": sorted(distinct_plataformas),
            "unidades_negocio": sorted(distinct_unidades),
            "subunidades_negocio": sorted(distinct_subunidades),
            "resultados": sorted(distinct_resultados),
            "date_range": _date_range_payload(min_month, max_month),
        },
        "kpis": {
            "total_rows": int(total_rows or 0),
            "clientes": len(distinct_clients),
            "renglones": int(total_rows or 0),
            "familias": len(distinct_familias),
            "provincias": len(distinct_provincias),
            "valorizacion": total_valorizacion_kpi,
        },
        "series": series,
        "results": results_payload,
        "top_families": top_families_payload,
        "geo": geo_payload,
        "clients_by_result": clients_payload,
        "family_consumption": {
            "months": month_keys,
            "rows": family_consumption_payload,
            "total": len(family_consumption_payload),
        },
    }


def _get_aggregated_dashboard_bootstrap(
    session: Session,
    filters: DimensionamientoFilters,
    *,
    include_status: bool,
) -> dict[str, Any]:
    cache_key = _make_cache_key("get_dashboard_bootstrap.aggregated", filters, include_status=include_status)
    cached = _cache_get(cache_key, _TTL_QUERY_RESULT)
    if cached is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_dashboard_bootstrap.aggregated hit key=%s", cache_key)
        return cached

    # Cuando hay negocios excluidos, el gráfico de series debe recibir los datos SIN esa
    # exclusión para que el usuario pueda ver y reactivar las series desde la leyenda.
    # Todo lo demás (KPIs, donut, mapa, tablas) usa los rows filtrados con exclusión.
    series_rows = None
    if filters.unidades_negocio_excluir:
        filters_for_series = _clone_filters(filters)
        filters_for_series.unidades_negocio_excluir = []
        series_rows = _fetch_summary_rows_for_bootstrap(session, filters_for_series)

    rows = _fetch_summary_rows_for_bootstrap(session, filters)
    payload = _aggregate_bootstrap_from_summary_rows(rows, series_rows=series_rows)

    # La agregación single-pass contaba clientes por cliente_visible (374). El conteo
    # canónico es por ENTIDAD resuelta: sobreescribimos card + desglose + desplegable
    # con las MISMAS funciones que usan get_kpis y get_filter_options, para que /kpis y
    # la vista nunca diverjan.
    total_entities, clientes_si, clientes_no = _client_entity_counts(session, filters)
    if filters.is_client is True:
        headline_clients = clientes_si
    elif filters.is_client is False:
        headline_clients = clientes_no
    else:
        headline_clients = total_entities
    payload.setdefault("kpis", {})
    payload["kpis"]["clientes"] = int(headline_clients)
    payload["kpis"]["clientes_si"] = int(clientes_si)
    payload["kpis"]["clientes_no"] = int(clientes_no)
    payload.setdefault("filters", {})
    payload["filters"]["clientes"] = _client_dropdown(session, filters)

    if include_status:
        payload["status"] = get_status(session, filters.import_run_id)
    payload["meta"] = {
        "source": "live",
        "stale": False,
        "snapshot_key": DEFAULT_DASHBOARD_SNAPSHOT_KEY,
        "snapshot_version": DEFAULT_DASHBOARD_SNAPSHOT_VERSION,
        "strategy": "summary_single_pass",
        "rows_scanned": len(rows),
    }
    _cache_set(cache_key, payload)
    return payload


def get_dashboard_bootstrap(
    session: Session,
    filters: DimensionamientoFilters | None = None,
    *,
    include_status: bool = True,
    bypass_snapshot: bool = False,
) -> dict[str, Any]:
    filters = _normalize_dashboard_filters(session, filters or build_filters())
    started_at = _log_query_start(
        "get_dashboard_bootstrap",
        filters,
        include_status=include_status,
        bypass_snapshot=bypass_snapshot,
    )
    try:
        _apply_local_statement_timeout(session, 50000)
        summary_state = _summary_health_snapshot_cached(session, filters.import_run_id)
        if not bypass_snapshot and not _has_active_filters(filters):
            snapshot = _get_dashboard_snapshot(session, filters.import_run_id)
            latest = _latest_success_import_run(session)
            snapshot_compatible = snapshot is not None and not _snapshot_payload_needs_refresh(snapshot)
            skip_snapshot_due_to_summary = summary_state.get("valorizacion_mismatch", False)
            if snapshot_compatible and not skip_snapshot_due_to_summary and (latest is None or snapshot.import_run_id == latest.id or snapshot.import_run_id == filters.import_run_id):
                payload = dict(snapshot.payload or {})
                if _family_consumption_payload_needs_refresh(payload):
                    payload = _refresh_bootstrap_family_consumption(session, payload, filters)
                if not include_status:
                    payload.pop("status", None)
                payload["meta"] = {
                    **dict(payload.get("meta") or {}),
                    **_snapshot_meta_payload(snapshot),
                    "source": "snapshot",
                    "stale": False,
                }
                _log_query_success(
                    "get_dashboard_bootstrap",
                    started_at,
                    source="snapshot",
                    import_run_id=snapshot.import_run_id,
                )
                return payload

            if snapshot_compatible and snapshot and not skip_snapshot_due_to_summary:
                payload = dict(snapshot.payload or {})
                if _family_consumption_payload_needs_refresh(payload):
                    payload = _refresh_bootstrap_family_consumption(session, payload, filters)
                if not include_status:
                    payload.pop("status", None)
                payload["meta"] = {
                    **dict(payload.get("meta") or {}),
                    **_snapshot_meta_payload(snapshot),
                    "source": "snapshot",
                    "stale": True,
                    "latest_import_run_id": latest.id if latest else None,
                }
                _log_query_success(
                    "get_dashboard_bootstrap",
                    started_at,
                    source="snapshot_stale",
                    import_run_id=snapshot.import_run_id,
                )
                return payload

        if summary_state["usable"]:
            payload = _get_aggregated_dashboard_bootstrap(
                session,
                filters,
                include_status=include_status,
            )
        else:
            # El gráfico de series siempre recibe datos SIN la exclusión de negocios,
            # para que las series excluidas sigan visibles y el usuario pueda reactivarlas.
            filters_for_series = _clone_filters(filters)
            filters_for_series.unidades_negocio_excluir = []
            payload = {
                "filters": get_filter_options(session, filters),
                "kpis": get_kpis(session, filters),
                "series": get_series(session, filters_for_series),
                "results": get_results_breakdown(session, filters),
                "top_families": get_top_families(session, filters),
                "geo": get_geography_distribution(session, filters),
                "clients_by_result": get_clients_by_result(session, filters, limit=10),
                "family_consumption": get_family_consumption_table(session, filters),
                "meta": {
                    "source": "live",
                    "stale": False,
                    "snapshot_key": DEFAULT_DASHBOARD_SNAPSHOT_KEY,
                    "snapshot_version": DEFAULT_DASHBOARD_SNAPSHOT_VERSION,
                    "strategy": "composed_queries",
                },
            }
            if include_status:
                payload["status"] = get_status(session)
        _log_query_success(
            "get_dashboard_bootstrap",
            started_at,
            source=payload["meta"]["source"],
            has_data=(payload.get("status") or {}).get("has_data"),
        )
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_dashboard_bootstrap failed filters=%s", _filters_debug_dict(filters))
        raise
