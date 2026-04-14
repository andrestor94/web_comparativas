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

from sqlalchemy import Date, Text, case, cast, distinct, func, literal, or_, select, text
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from web_comparativas.models import IS_POSTGRES, IS_SQLITE

from .models import (
    DimensionamientoDashboardSnapshot,
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoImportRun,
    DimensionamientoRecord,
)

logger = logging.getLogger("wc.dimensionamiento.query")
_NO_FILTER_TOKENS = frozenset({"todos", "todas", "all", "*"})
DEFAULT_DASHBOARD_SNAPSHOT_KEY = "default_dashboard_bootstrap"
DEFAULT_DASHBOARD_SNAPSHOT_VERSION = "v1"

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
_SUMMARY_HEALTH_CACHE: dict = {"ts": 0.0, "val": None}


def _make_cache_key(fn_name: str, filters: "DimensionamientoFilters", **extra: Any) -> str:
    """Clave MD5 determinista a partir del nombre de función + filtros + params extra."""
    d = _filters_debug_dict(filters)
    # Normalizar listas para que el orden no genere keys distintas
    for k in ("clientes", "provincias", "familias", "plataformas",
              "unidades_negocio", "subunidades_negocio", "resultados"):
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
    # Resetear también el micro-cache de salud de la tabla resumen
    _SUMMARY_HEALTH_CACHE["val"] = None
    _SUMMARY_HEALTH_CACHE["ts"] = 0.0
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
    provincias: list[str] = field(default_factory=list)
    familias: list[str] = field(default_factory=list)
    plataformas: list[str] = field(default_factory=list)
    unidades_negocio: list[str] = field(default_factory=list)
    subunidades_negocio: list[str] = field(default_factory=list)
    resultados: list[str] = field(default_factory=list)
    fecha_desde: dt.date | None = None
    fecha_hasta: dt.date | None = None
    identified: bool | None = None
    is_client: bool | None = None
    search: str | None = None


def _filters_debug_dict(filters: DimensionamientoFilters) -> dict[str, Any]:
    return {
        "clientes": filters.clientes,
        "provincias": filters.provincias,
        "familias": filters.familias,
        "plataformas": filters.plataformas,
        "unidades_negocio": filters.unidades_negocio,
        "subunidades_negocio": filters.subunidades_negocio,
        "resultados": filters.resultados,
        "fecha_desde": filters.fecha_desde.isoformat() if filters.fecha_desde else None,
        "fecha_hasta": filters.fecha_hasta.isoformat() if filters.fecha_hasta else None,
        "identified": filters.identified,
        "is_client": filters.is_client,
        "search": filters.search,
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
        provincias=list(filters.provincias),
        familias=list(filters.familias),
        plataformas=list(filters.plataformas),
        unidades_negocio=list(filters.unidades_negocio),
        subunidades_negocio=list(filters.subunidades_negocio),
        resultados=list(filters.resultados),
        fecha_desde=filters.fecha_desde,
        fecha_hasta=filters.fecha_hasta,
        identified=filters.identified,
        is_client=filters.is_client,
        search=filters.search,
    )


def _has_active_filters(filters: DimensionamientoFilters) -> bool:
    return any(
        [
            filters.clientes,
            filters.provincias,
            filters.familias,
            filters.plataformas,
            filters.unidades_negocio,
            filters.subunidades_negocio,
            filters.resultados,
            filters.fecha_desde is not None,
            filters.fecha_hasta is not None,
            filters.identified is not None,
            filters.is_client is not None,
            filters.search,
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


def _summary_health_snapshot(session: Session) -> dict[str, Any]:
    summary_rows, raw_min_month, raw_max_month = session.execute(
        text(
            "SELECT COUNT(*), MIN(month), MAX(month) "
            "FROM dimensionamiento_family_monthly_summary"
        )
    ).one()
    min_month = _coerce_date_value(raw_min_month)
    max_month = _coerce_date_value(raw_max_month)
    return {
        "rows": int(summary_rows or 0),
        "raw_min_month": raw_min_month,
        "raw_max_month": raw_max_month,
        "min_month": min_month,
        "max_month": max_month,
        "usable": bool(summary_rows) and min_month is not None and max_month is not None,
    }


def _summary_health_snapshot_cached(session: Session) -> dict[str, Any]:
    """Versión cacheada de _summary_health_snapshot con TTL de 10 segundos.

    Evita ejecutar SELECT COUNT/MIN/MAX en la tabla resumen 7+ veces por cada
    cambio de filtro (una vez por cada función de widget que llama a
    _resolve_aggregate_model). Con caché, solo se ejecuta una vez cada 10s.
    """
    now = time.perf_counter()
    cached_val = _SUMMARY_HEALTH_CACHE["val"]
    if cached_val is not None and now - _SUMMARY_HEALTH_CACHE["ts"] < _TTL_SUMMARY_HEALTH:
        return cached_val
    val = _summary_health_snapshot(session)
    _SUMMARY_HEALTH_CACHE["ts"] = time.perf_counter()
    _SUMMARY_HEALTH_CACHE["val"] = val
    return val


def _global_date_bounds(session: Session) -> tuple[dt.date | None, dt.date | None]:
    summary_state = _summary_health_snapshot_cached(session)
    if summary_state["usable"]:
        return summary_state["min_month"], summary_state["max_month"]

    min_date, max_date = session.execute(
        select(
            func.min(DimensionamientoRecord.fecha),
            func.max(DimensionamientoRecord.fecha),
        )
    ).one()
    return _coerce_date_value(min_date), _coerce_date_value(max_date)


def _default_platform_values(session: Session) -> list[str]:
    cache_key = "dimensionamiento.default_platform_values"
    cached = _cache_get(cache_key, _TTL_FILTER_OPTIONS_DFLT)
    if cached is not _CACHE_MISS:
        return list(cached)

    summary_state = _summary_health_snapshot_cached(session)
    model = DimensionamientoFamilyMonthlySummary if summary_state["usable"] else DimensionamientoRecord
    stmt = (
        select(distinct(model.plataforma))
        .where(model.plataforma.is_not(None))
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
    if (
        not normalized.plataformas
        and normalized.fecha_desde is None
        and normalized.fecha_hasta is None
    ):
        return normalized

    default_platforms = _default_platform_values(session)
    if normalized.plataformas and default_platforms:
        requested_platforms = {value.strip().upper() for value in normalized.plataformas if value}
        all_platforms = {value.strip().upper() for value in default_platforms if value}
        if requested_platforms == all_platforms:
            normalized.plataformas = []

    min_date, max_date = _global_date_bounds(session)
    if min_date is not None and normalized.fecha_desde == min_date:
        normalized.fecha_desde = None
    if max_date is not None and normalized.fecha_hasta == max_date:
        normalized.fecha_hasta = None
    return normalized


def _resolve_aggregate_model(
    session: Session,
    filters: DimensionamientoFilters,
    endpoint_tag: str,
    *,
    summary_message: str = "using summary table",
    base_message: str = "using base table",
):
    if filters.search:
        logger.info("[DIM][%s] %s reason=search", endpoint_tag, base_message)
        return DimensionamientoRecord

    summary_state = _summary_health_snapshot_cached(session)
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


_SIN_DATO_SQL = ("SIN DATO", "SIN_DATO")
_SUMMARY_CLIENT_EXCLUDE: frozenset[str] = frozenset({"SIN DATO", "SIN_DATO"})


def _distinct_summary_clients(session: Session) -> list[str]:
    """Fast path: nombres únicos de clientes desde la tabla de resumen mensual.

    Evita un full scan sobre dimensionamiento_records (400k–500k filas) cuando no hay
    filtros activos. La tabla resumen ya tiene los datos pre-agregados y normalizados.
    Excluye variantes de 'SIN DATO' y valores vacíos.
    """
    stmt = (
        select(distinct(DimensionamientoFamilyMonthlySummary.cliente_nombre_homologado))
        .where(DimensionamientoFamilyMonthlySummary.cliente_nombre_homologado.isnot(None))
        .where(func.coalesce(DimensionamientoFamilyMonthlySummary.cliente_nombre_homologado, "") != "")
        .order_by(DimensionamientoFamilyMonthlySummary.cliente_nombre_homologado)
    )
    all_clients = [c for c in session.execute(stmt).scalars().all() if c]
    return [
        c for c in all_clients
        if c.strip().upper().replace("_", " ") not in _SUMMARY_CLIENT_EXCLUDE
    ]


def _cliente_visible_expr(model):
    """
    Expresión SQL para el nombre visible del cliente.

    Para la tabla resumen (DimensionamientoFamilyMonthlySummary):
      - Retorna cliente_nombre_homologado directamente. No hay CASE WHEN ni
        funciones de cadena, lo que permite que el planificador use índices.

    Para la tabla de detalle (DimensionamientoRecord):
      - CASE WHEN: usa homologado si es válido, fallback a cliente_nombre_original,
        NULL si ambos son inválidos o SIN DATO.
    """
    mapper = sa_inspect(model)
    mapped_cols = {attr.key for attr in mapper.mapper.column_attrs}

    # Tabla resumen: no tiene cliente_nombre_original.
    # Retornar la columna directo elimina el CASE WHEN más costoso del módulo.
    if "cliente_nombre_original" not in mapped_cols:
        return model.cliente_nombre_homologado

    original_column = model.cliente_nombre_original
    _homologado_upper = func.upper(func.trim(func.coalesce(model.cliente_nombre_homologado, "")))
    _homologado_is_invalid = or_(
        func.trim(func.coalesce(model.cliente_nombre_homologado, "")) == "",
        _homologado_upper.in_(list(_SIN_DATO_SQL)),
    )
    return case(
        (_homologado_is_invalid, original_column),
        else_=model.cliente_nombre_homologado,
    )


def _distinct_visible_clients(session: Session, filters: DimensionamientoFilters) -> list[str]:
    """Retorna nombres visibles únicos de clientes, sin 'SIN DATO' ni vacíos.

    Usa subquery con label para evitar sqlalchemy.exc.NoSuchColumnError al aplicar
    filtros dinámicos sobre la expresión CASE compleja con distinct().
    """
    visible_expr = _cliente_visible_expr(DimensionamientoRecord)
    # Usamos .label() para que SQLAlchemy pueda localizar la columna en el resultado.
    # Luego aplicamos distinct() en un subquery separado para evitar el error de
    # indexación al combinar SELECT DISTINCT CASE WHEN... con WHERE dinámicos.
    inner_stmt = _apply_common_filters(
        select(visible_expr.label("nombre_visible")),
        DimensionamientoRecord,
        filters,
    )
    inner_stmt = inner_stmt.where(visible_expr.isnot(None))
    inner_stmt = inner_stmt.where(func.coalesce(visible_expr, "") != "")
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
    subunidades_negocio: Iterable[str] | None = None,
    resultados: Iterable[str] | None = None,
    fecha_desde: dt.date | None = None,
    fecha_hasta: dt.date | None = None,
    identified: bool | None = None,
    is_client: bool | None = None,
    search: str | None = None,
) -> DimensionamientoFilters:
    return DimensionamientoFilters(
        clientes=_normalize_list(clientes),
        provincias=_normalize_list(provincias),
        familias=_normalize_list(familias),
        plataformas=_normalize_list(plataformas),
        unidades_negocio=_normalize_list(unidades_negocio),
        subunidades_negocio=_normalize_list(subunidades_negocio),
        resultados=_normalize_list(resultados),
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        identified=identified,
        is_client=is_client,
        search=(search or "").strip() or None,
    )


def _apply_common_filters(stmt, model, filters: DimensionamientoFilters, applied_conditions: list[str] | None = None):
    use_direct_match = model is DimensionamientoFamilyMonthlySummary
    if filters.clientes:
        _visible = _cliente_visible_expr(model)
        if use_direct_match:
            stmt = stmt.where(_visible.in_(filters.clientes))
            if applied_conditions is not None:
                applied_conditions.append(f"cliente_visible IN {filters.clientes}")
        else:
            normalized_clients = _normalized_filter_values(filters.clientes)
            if normalized_clients:
                stmt = stmt.where(_sql_normalized_text(_visible).in_(normalized_clients))
                if applied_conditions is not None:
                    applied_conditions.append(f"cliente_visible IN {normalized_clients}")
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
    if filters.identified is not None:
        stmt = stmt.where(model.is_identified.is_(filters.identified))
        if applied_conditions is not None:
            applied_conditions.append(f"is_identified IS {filters.identified}")
    if filters.is_client is not None:
        stmt = stmt.where(model.is_client.is_(filters.is_client))
        if applied_conditions is not None:
            applied_conditions.append(f"is_client IS {filters.is_client}")
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

    if filters.search:
        token = f"%{filters.search.lower()}%"
        cliente_original_column = _model_column_or_literal(model, "cliente_nombre_original")
        codigo_articulo_column = _model_column_or_literal(model, "codigo_articulo")
        producto_original_column = _model_column_or_literal(model, "producto_nombre_original")
        stmt = stmt.where(
            func.lower(func.coalesce(model.cliente_nombre_homologado, "")).like(token)
            | func.lower(func.coalesce(cliente_original_column, "")).like(token)
            | func.lower(func.coalesce(model.familia, "")).like(token)
            | func.lower(func.coalesce(codigo_articulo_column, "")).like(token)
            | func.lower(func.coalesce(producto_original_column, "")).like(token)
        )
        if applied_conditions is not None:
            applied_conditions.append(f"search LIKE {token}")
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


def _distinct_summary_values(session: Session, column, order_by=None) -> list[str]:
    stmt = (
        select(distinct(column.label("_val")))
        .where(column.is_not(None))
        .order_by(order_by if order_by is not None else column)
    )
    return [value for value in session.execute(stmt).scalars().all() if value not in (None, "")]


def _distinct_filtered_summary_clients(session: Session, filters: DimensionamientoFilters) -> list[str]:
    """Clientes únicos desde la tabla resumen aplicando filtros activos.

    Más rápido que _distinct_visible_clients: no hace CASE WHEN ni full scan sobre
    dimensionamiento_records. Filtra is_client=True para excluir variantes de SIN DATO.
    Usa el mismo criterio de exclusión que _distinct_summary_clients.
    """
    model = DimensionamientoFamilyMonthlySummary
    inner_stmt = _apply_common_filters(
        select(model.cliente_nombre_homologado.label("_val"))
        .where(model.cliente_nombre_homologado.isnot(None))
        .where(model.is_client == True),  # noqa: E712
        model,
        filters,
    )
    subq = inner_stmt.subquery()
    outer_stmt = (
        select(distinct(subq.c._val))
        .where(subq.c._val.isnot(None))
        .where(subq.c._val != "")
        .order_by(subq.c._val)
    )
    all_clients = [v for v in session.execute(outer_stmt).scalars().all() if v]
    return [c for c in all_clients if c.strip().upper().replace("_", " ") not in _SUMMARY_CLIENT_EXCLUDE]


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


def _get_dashboard_snapshot(session: Session) -> DimensionamientoDashboardSnapshot | None:
    return session.execute(
        select(DimensionamientoDashboardSnapshot)
        .where(
            DimensionamientoDashboardSnapshot.snapshot_key == DEFAULT_DASHBOARD_SNAPSHOT_KEY,
            DimensionamientoDashboardSnapshot.version == DEFAULT_DASHBOARD_SNAPSHOT_VERSION,
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
        "top_families": get_top_families(session, base_filters, limit=10),
        "geo": get_geography_distribution(session, base_filters),
        "clients_by_result": get_clients_by_result(session, base_filters, limit=10),
        "family_consumption": get_family_consumption_table(session, base_filters, limit=20),
    }


def refresh_default_dashboard_snapshot(
    session: Session,
    *,
    import_run_id: int | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    started_at = _log_query_start("refresh_default_dashboard_snapshot", import_run_id=import_run_id)
    payload = _build_dashboard_bootstrap_payload(session)
    snapshot = _get_dashboard_snapshot(session)
    latest = _latest_success_import_run(session)
    if snapshot is None:
        snapshot = DimensionamientoDashboardSnapshot(
            snapshot_key=DEFAULT_DASHBOARD_SNAPSHOT_KEY,
            version=DEFAULT_DASHBOARD_SNAPSHOT_VERSION,
        )
    payload["meta"] = {
        **_snapshot_meta_payload(snapshot),
        "source": "snapshot",
        "stale": False,
    }
    snapshot.version = DEFAULT_DASHBOARD_SNAPSHOT_VERSION
    snapshot.import_run_id = import_run_id if import_run_id is not None else (latest.id if latest else None)
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
    snapshot = _get_dashboard_snapshot(session)
    if latest is None:
        return snapshot.payload if snapshot else None
    if snapshot and snapshot.import_run_id == latest.id:
        return snapshot.payload
    return refresh_default_dashboard_snapshot(session, import_run_id=latest.id, commit=True)


def get_status(session: Session) -> dict[str, Any]:
    started_at = time.perf_counter()
    logger.info("[DIM][QUERY] get_status start")
    _apply_local_statement_timeout(session, 50000)

    latest = session.execute(
        select(DimensionamientoImportRun)
        .where(DimensionamientoImportRun.status == "success")
        .order_by(DimensionamientoImportRun.finished_at.desc(), DimensionamientoImportRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()

    total_rows = session.execute(
        select(func.count(DimensionamientoRecord.id))
    ).scalar_one()

    platform_rows = session.execute(
        select(
            DimensionamientoRecord.plataforma,
            func.count(DimensionamientoRecord.id),
        )
        .group_by(DimensionamientoRecord.plataforma)
        .order_by(DimensionamientoRecord.plataforma)
    ).all()

    payload = {
        "has_data": total_rows > 0,
        "total_rows": total_rows,
        "platforms": [{"name": name, "rows": rows} for name, rows in platform_rows],
        "last_import": {
            "id": latest.id,
            "source_path": latest.source_path,
            "source_hash": latest.source_hash,
            "finished_at": latest.finished_at.isoformat() if latest and latest.finished_at else None,
            "rows_processed": latest.rows_processed if latest else 0,
            "rows_inserted": latest.rows_inserted if latest else 0,
            "rows_updated": latest.rows_updated if latest else 0,
            "rows_rejected": latest.rows_rejected if latest else 0,
        }
        if latest
        else None,
    }
    _log_query_success("get_status", started_at, total_rows=total_rows, platforms=len(platform_rows))
    return payload


def get_debug_snapshot(session: Session) -> dict[str, Any]:
    started_at = _log_query_start("get_debug_snapshot")
    _apply_local_statement_timeout(session, 50000)

    total_rows = session.execute(select(func.count(DimensionamientoRecord.id))).scalar_one()
    distinct_platforms = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.plataforma))))
    ).scalar_one()
    distinct_clients = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.cliente_nombre_homologado))))
    ).scalar_one()
    distinct_families = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.familia))))
    ).scalar_one()
    distinct_provinces = session.execute(
        select(func.count(distinct(_sql_normalized_text(DimensionamientoRecord.provincia))))
    ).scalar_one()
    min_date, max_date = session.execute(
        select(func.min(DimensionamientoRecord.fecha), func.max(DimensionamientoRecord.fecha))
    ).one()
    top_results_stmt = (
        select(
            DimensionamientoRecord.resultado_participacion,
            func.count(DimensionamientoRecord.id).label("rows"),
        )
        .group_by(DimensionamientoRecord.resultado_participacion)
        .order_by(func.count(DimensionamientoRecord.id).desc(), DimensionamientoRecord.resultado_participacion.asc())
        .limit(10)
    )
    top_results = [
        {"resultado_participacion": resultado or "Sin resultado", "rows": rows or 0}
        for resultado, rows in session.execute(top_results_stmt).all()
    ]
    sample_values = {
        "plataformas": _distinct_values(session, DimensionamientoRecord.plataforma, build_filters())[:10],
        "familias": _distinct_values(session, DimensionamientoRecord.familia, build_filters())[:10],
        "provincias": _distinct_values(session, DimensionamientoRecord.provincia, build_filters())[:10],
        "unidades_negocio": _distinct_values(session, DimensionamientoRecord.unidad_negocio, build_filters())[:10],
        "subunidades_negocio": _distinct_values(session, DimensionamientoRecord.subunidad_negocio, build_filters())[:10],
    }
    payload = {
        "table": DimensionamientoRecord.__tablename__,
        "summary_table": DimensionamientoFamilyMonthlySummary.__tablename__,
        "columns": [column.name for column in DimensionamientoRecord.__table__.columns],
        "total_registros": total_rows or 0,
        "count_distinct_plataforma": distinct_platforms or 0,
        "count_distinct_cliente_nombre_homologado": distinct_clients or 0,
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
        summary_state = _summary_health_snapshot_cached(session)
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
            payload = {
                "clientes": _distinct_summary_clients(session),
                "provincias": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.provincia),
                "familias": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.familia),
                "plataformas": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.plataforma),
                "unidades_negocio": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.unidad_negocio),
                "subunidades_negocio": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.subunidad_negocio),
                "resultados": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.resultado_participacion),
                "date_range": _date_range_payload(
                    summary_state["min_month"],
                    summary_state["max_month"],
                ),
            }
        else:
            applied_conditions: list[str] = []
            use_summary = not filters.search and summary_state["usable"]
            filt_model = DimensionamientoFamilyMonthlySummary if use_summary else DimensionamientoRecord
            if use_summary:
                logger.info(
                    "[DIM][FILTERS] using summary clients path rows=%s min_month=%s max_month=%s",
                    summary_state["rows"],
                    summary_state["min_month"].isoformat(),
                    summary_state["max_month"].isoformat(),
                )
            else:
                reason = "search" if filters.search else "summary_unavailable"
                logger.info(
                    "[DIM][FILTERS] using distinct_visible_clients base path reason=%s summary_rows=%s raw_min_month=%r raw_max_month=%r",
                    reason,
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
                payload = {
                    "clientes": _distinct_filtered_summary_clients(session, filters),
                    "provincias": _distinct_filtered_summary_values(session, s.provincia, filters),
                    "familias": _distinct_filtered_summary_values(session, s.familia, filters),
                    "plataformas": _distinct_filtered_summary_values(session, s.plataforma, filters),
                    "unidades_negocio": _distinct_filtered_summary_values(session, s.unidad_negocio, filters),
                    "subunidades_negocio": _distinct_filtered_summary_values(session, s.subunidad_negocio, filters),
                    "resultados": _distinct_filtered_summary_values(session, s.resultado_participacion, filters),
                    "date_range": _date_range_payload(min_date, max_date),
                }
            else:
                payload = {
                    "clientes": _distinct_visible_clients(session, filters),
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


def get_kpis(session: Session, filters: DimensionamientoFilters) -> dict[str, Any]:
    _ck = _make_cache_key("get_kpis", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_kpis hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_kpis", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, filters, "KPIS")
        applied_conditions: list[str] = []

        if model is DimensionamientoRecord:
            # Tabla de detalle: query única con CASE WHEN para cliente visible
            _visible = _cliente_visible_expr(model)
            stmt = _apply_common_filters(
                select(
                    func.count(model.id),
                    func.count(distinct(_visible)),
                    func.count(model.id),
                    func.count(distinct(model.familia)),
                    func.coalesce(func.sum(model.cantidad_demandada), 0),
                ),
                model,
                filters,
                applied_conditions,
            )
            _log_query_statement(session, "get_kpis", model, stmt, filters, applied_conditions)
            total_rows, total_clients, total_records, total_families, total_quantity = session.execute(stmt).one()
        else:
            # Tabla resumen: 2 queries rápidas evitan full scan de 400k+ filas
            # Query 1: totales, familias, cantidad
            main_stmt = _apply_common_filters(
                select(
                    func.coalesce(func.sum(model.total_registros), 0),
                    func.count(distinct(model.familia)),
                    func.coalesce(func.sum(model.total_cantidad), 0),
                ),
                model,
                filters,
                applied_conditions,
            )
            _log_query_statement(session, "get_kpis", model, main_stmt, filters, applied_conditions)
            total_rows, total_families, total_quantity = session.execute(main_stmt).one()
            # Query 2: clientes únicos reales (is_client=True excluye SIN DATO)
            normalized_client = _sql_normalized_text(model.cliente_nombre_homologado)
            client_stmt = _apply_common_filters(
                select(func.count(distinct(model.cliente_nombre_homologado)))
                .where(model.is_client == True)  # noqa: E712
                .where(model.cliente_nombre_homologado.isnot(None))
                .where(model.cliente_nombre_homologado != "")
                .where(~normalized_client.in_(list(_SIN_DATO_SQL))),
                model,
                filters,
            )
            total_clients = session.execute(client_stmt).scalar_one()
            total_records = total_rows

        payload = {
            "total_rows": int(total_rows or 0),
            "clientes": int(total_clients or 0),
            "renglones": int(total_records or 0),
            "familias": int(total_families or 0),
            "cantidad_demandada": float(total_quantity or 0),
        }
        _log_query_success("get_kpis", started_at, total_rows=payload["total_rows"], clientes=payload["clientes"])
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_kpis failed filters=%s", _filters_debug_dict(filters))
        raise


def get_series(session: Session, filters: DimensionamientoFilters, limit: int = 5) -> dict[str, Any]:
    _ck = _make_cache_key("get_series", filters, limit=limit)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_series hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_series", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, filters, "SERIES")
        negocio_expr = func.coalesce(model.unidad_negocio, "Sin negocio")
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
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
        for month, negocio, total in session.execute(stmt).all():
            month_key = _month_value_to_iso(month)
            series_map.setdefault(month_key, {})[negocio] = float(total or 0)

        months = sorted(series_map.keys())
        datasets = []
        for negocio in top_businesses:
            datasets.append(
                {
                    "label": negocio,
                    "values": [series_map.get(month, {}).get(negocio, 0) for month in months],
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
    _ck = _make_cache_key("get_results_breakdown", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_results_breakdown hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_results_breakdown", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, filters, "RESULTS")
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        applied_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                model.resultado_participacion,
                count_expr.label("rows"),
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
            }
            for resultado, rows in session.execute(stmt).all()
        ]
        _log_query_success("get_results_breakdown", started_at, rows=len(payload))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_results_breakdown failed filters=%s", _filters_debug_dict(filters))
        raise


def get_top_families(session: Session, filters: DimensionamientoFilters, limit: int = 10) -> list[dict[str, Any]]:
    _ck = _make_cache_key("get_top_families", filters, limit=limit)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_top_families hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_top_families", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, filters, "TOP_FAMILIES")
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        quantity_expr = func.coalesce(
            func.sum(model.cantidad_demandada if model is DimensionamientoRecord else model.total_cantidad),
            0,
        )
        applied_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                model.familia,
                count_expr.label("rows"),
                quantity_expr.label("quantity"),
            )
            .where(model.familia.is_not(None))
            .group_by(model.familia)
            .order_by(count_expr.desc(), model.familia.asc())
            .limit(limit),
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
            }
            for familia, rows, quantity in session.execute(stmt).all()
        ]
        _log_query_success("get_top_families", started_at, rows=len(payload))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_top_families failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise


def get_geography_distribution(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    _ck = _make_cache_key("get_geography_distribution", filters)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_geography_distribution hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_geography_distribution", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, filters, "GEO")
        count_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        applied_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                model.provincia,
                count_expr.label("rows"),
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
            }
            for provincia, rows in session.execute(stmt).all()
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
            filters,
            "CLIENTS_BY_RESULT",
            summary_message="using summary path",
            base_message="using base path",
        )
        subquery_conditions: list[str] = []

        if model is DimensionamientoRecord:
            # Tabla de detalle: CASE WHEN necesario para resolver cliente_nombre_original
            _visible_raw = _cliente_visible_expr(model)
            total_expr = func.count(model.id)
            subquery = _apply_common_filters(
                select(
                    _visible_raw.label("cliente"),
                    model.resultado_participacion.label("resultado"),
                    total_expr.label("total"),
                )
                .where(_visible_raw.isnot(None))
                .where(func.coalesce(_visible_raw, "") != "")
                .group_by(_visible_raw, model.resultado_participacion),
                model,
                filters,
                subquery_conditions,
            ).subquery()
        else:
            # Tabla resumen: sin CASE WHEN, columna directa + is_client=True
            # es_client=True garantiza que son clientes reales (no SIN DATO)
            total_expr = func.coalesce(func.sum(model.total_registros), 0)
            normalized_client = _sql_normalized_text(model.cliente_nombre_homologado)
            subquery = _apply_common_filters(
                select(
                    model.cliente_nombre_homologado.label("cliente"),
                    model.resultado_participacion.label("resultado"),
                    total_expr.label("total"),
                )
                .where(model.is_client == True)  # noqa: E712
                .where(model.cliente_nombre_homologado.isnot(None))
                .where(model.cliente_nombre_homologado != "")
                .where(~normalized_client.in_(list(_SIN_DATO_SQL)))
                .group_by(model.cliente_nombre_homologado, model.resultado_participacion),
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
            select(subquery.c.cliente, subquery.c.resultado, subquery.c.total)
            .where(subquery.c.cliente.in_(top_clients))
            .order_by(subquery.c.cliente.asc(), subquery.c.resultado.asc())
        )
        detail_conditions = list(subquery_conditions)
        detail_conditions.append(f"cliente IN {top_clients}")
        _log_query_statement(session, "get_clients_by_result.detail", model, detail_stmt, filters, detail_conditions)

        client_map: dict[str, dict[str, float]] = {}
        for cliente, resultado, total in session.execute(detail_stmt).all():
            client_map.setdefault(cliente, {})[resultado or "Sin resultado"] = float(total or 0)

        payload = [
            {"cliente": cliente, "resultados": client_map.get(cliente, {})}
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
    limit: int = 20,
) -> dict[str, Any]:
    _ck = _make_cache_key("get_family_consumption_table", filters, limit=limit)
    _hit = _cache_get(_ck, _TTL_QUERY_RESULT)
    if _hit is not _CACHE_MISS:
        logger.debug("[DIM][CACHE] get_family_consumption_table hit key=%s", _ck)
        return _hit

    started_at = _log_query_start("get_family_consumption_table", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _resolve_aggregate_model(session, filters, "FAMILY_CONSUMPTION")
        total_expr = func.coalesce(
            func.sum(model.cantidad_demandada if model is DimensionamientoRecord else model.total_cantidad),
            0,
        )

        # Paso 1: identificar las top N familias por cantidad total (query liviana con GROUP BY familia).
        # Esto limita el scan principal a solo esas familias, evitando traer todos los datos.
        applied_conditions: list[str] = []
        top_families_stmt = _apply_common_filters(
            select(
                model.familia,
                total_expr.label("grand_total"),
            )
            .where(model.familia.is_not(None))
            .group_by(model.familia)
            .order_by(total_expr.desc())
            .limit(limit),
            model,
            filters,
            applied_conditions,
        )
        _log_query_statement(session, "get_family_consumption_table.top_families", model, top_families_stmt, filters, applied_conditions)
        top_families_rows = session.execute(top_families_stmt).all()
        if not top_families_rows:
            payload = {"months": [], "rows": []}
            _log_query_success("get_family_consumption_table", started_at, months=0, rows=0)
            return payload

        top_families = [f for f, _ in top_families_rows]

        # Paso 2: query mensual solo para las familias seleccionadas.
        # Usamos date_trunc('month', fecha) en lugar de to_char(fecha, 'MM') para que el
        # planificador pueda usar el índice en 'fecha'. Extraemos el número de mes en Python.
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
            )
            .where(model.familia.in_(top_families))
            .group_by(month_bucket, model.familia)
            .order_by(month_bucket.asc(), model.familia.asc()),
            model,
            filters,
            monthly_conditions,
        )
        monthly_conditions.append(f"familia IN {top_families}")
        _log_query_statement(session, "get_family_consumption_table.monthly", model, monthly_stmt, filters, monthly_conditions)
        rows = session.execute(monthly_stmt).all()

        month_keys = [f"{index:02d}" for index in range(1, 13)]
        monthly_values: dict[str, dict[str, list[float]]] = {}
        for month_date, family, total in rows:
            month_iso = _month_value_to_iso(month_date)
            month_key = month_iso[5:7] if len(month_iso) >= 7 else "01"
            family_name = family or "Sin familia"
            monthly_values.setdefault(family_name, {}).setdefault(month_key, []).append(float(total or 0))

        data = []
        for family in top_families:
            family_months = monthly_values.get(family, {})
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
                }
            )
        payload = {"months": month_keys, "rows": data}
        _log_query_success("get_family_consumption_table", started_at, months=len(payload["months"]), rows=len(payload["rows"]))
        _cache_set(_ck, payload)
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_family_consumption_table failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise


def _real_client_name(value: str | None, is_client: bool) -> str | None:
    text_value = str(value or "").strip()
    if not is_client or not text_value:
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
            model.cliente_nombre_homologado,
            model.provincia,
            model.familia,
            model.unidad_negocio,
            model.subunidad_negocio,
            model.resultado_participacion,
            model.is_identified,
            model.is_client,
            model.total_cantidad,
            model.total_registros,
        ),
        model,
        filters,
        applied_conditions,
    )
    _log_query_statement(session, "get_dashboard_bootstrap.summary_rows", model, stmt, filters, applied_conditions)
    return session.execute(stmt).all()


def _aggregate_bootstrap_from_summary_rows(
    rows: list[tuple[Any, ...]],
    *,
    series_limit: int = 5,
    top_families_limit: int = 10,
    clients_limit: int = 10,
    family_consumption_limit: int = 20,
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
    min_month: dt.date | None = None
    max_month: dt.date | None = None

    business_totals: dict[str, int] = defaultdict(int)
    business_by_month: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    result_totals: dict[str, int] = defaultdict(int)
    family_rows: dict[str, int] = defaultdict(int)
    family_quantities: dict[str, float] = defaultdict(float)
    geo_totals: dict[str, int] = defaultdict(int)
    client_totals: dict[str, int] = defaultdict(int)
    client_result_totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    family_month_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

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
        total_registros,
    ) in rows:
        month_date = _coerce_date_value(month_value)
        if month_date is not None:
            min_month = month_date if min_month is None or month_date < min_month else min_month
            max_month = month_date if max_month is None or month_date > max_month else max_month
        month_iso = _month_value_to_iso(month_value)

        row_count = int(total_registros or 0)
        quantity = float(total_cantidad or 0)
        total_rows += row_count
        total_quantity += quantity

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
        business_totals[business_name] += row_count
        business_by_month[month_iso][business_name] += row_count
        result_totals[result_name] += row_count
        geo_totals[province_name] += row_count
        family_month_totals[family_name][month_iso] += quantity

        client_name = _real_client_name(cliente, bool(is_client))
        if client_name:
            distinct_clients.add(client_name)
            client_totals[client_name] += row_count
            client_result_totals[client_name][result_name] += row_count

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
            }
            for business_name in top_businesses
        ],
    }

    results_payload = [
        {"resultado": result_name, "renglones": rows_count}
        for result_name, rows_count in sorted(result_totals.items(), key=lambda item: (-item[1], item[0]))
    ]

    top_families_payload = [
        {
            "familia": family_name,
            "renglones": rows_count,
            "cantidad": float(family_quantities.get(family_name, 0)),
        }
        for family_name, rows_count in sorted(family_rows.items(), key=lambda item: (-item[1], item[0]))[:top_families_limit]
    ]

    geo_payload = [
        {"provincia": province_name, "renglones": rows_count}
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
        }
        for client_name in top_clients
    ]

    month_keys = [f"{index:02d}" for index in range(1, 13)]
    family_consumption_payload = []
    top_family_consumption = [
        family_name
        for family_name, _ in sorted(family_quantities.items(), key=lambda item: (-item[1], item[0]))[:family_consumption_limit]
    ]
    for family_name in top_family_consumption:
        month_lists: dict[str, list[float]] = defaultdict(list)
        for month_iso, quantity in family_month_totals.get(family_name, {}).items():
            month_key = month_iso[5:7] if len(month_iso) >= 7 else "01"
            month_lists[month_key].append(quantity)
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
            "cantidad_demandada": float(total_quantity or 0),
        },
        "series": series,
        "results": results_payload,
        "top_families": top_families_payload,
        "geo": geo_payload,
        "clients_by_result": clients_payload,
        "family_consumption": {
            "months": month_keys,
            "rows": family_consumption_payload,
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

    rows = _fetch_summary_rows_for_bootstrap(session, filters)
    payload = _aggregate_bootstrap_from_summary_rows(rows)
    if include_status:
        payload["status"] = get_status(session)
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
        if not bypass_snapshot and not _has_active_filters(filters):
            snapshot = _get_dashboard_snapshot(session)
            latest = _latest_success_import_run(session)
            if snapshot and (latest is None or snapshot.import_run_id == latest.id):
                payload = dict(snapshot.payload or {})
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

            if snapshot:
                payload = dict(snapshot.payload or {})
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

        summary_state = _summary_health_snapshot_cached(session)
        if not filters.search and summary_state["usable"]:
            payload = _get_aggregated_dashboard_bootstrap(
                session,
                filters,
                include_status=include_status,
            )
        else:
            payload = {
                "filters": get_filter_options(session, filters),
                "kpis": get_kpis(session, filters),
                "series": get_series(session, filters),
                "results": get_results_breakdown(session, filters),
                "top_families": get_top_families(session, filters, limit=10),
                "geo": get_geography_distribution(session, filters),
                "clients_by_result": get_clients_by_result(session, filters, limit=10),
                "family_consumption": get_family_consumption_table(session, filters, limit=20),
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
