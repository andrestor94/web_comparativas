from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from sqlalchemy import Date, Text, case, cast, distinct, func, literal, or_, select, text
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from web_comparativas.models import IS_POSTGRES, IS_SQLITE

from .models import DimensionamientoFamilyMonthlySummary, DimensionamientoImportRun, DimensionamientoRecord

logger = logging.getLogger("wc.dimensionamiento.query")
_NO_FILTER_TOKENS = frozenset({"todos", "todas", "all", "*"})


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
    Expresión SQL para el nombre visible del cliente:
    - cliente_nombre_homologado si es válido (no vacío, no variante de SIN DATO)
    - cliente_nombre_original como fallback cuando homologado es inválido
    - NULL si ambos son nulos/vacíos
    """
    mapper = sa_inspect(model)
    mapped_cols = {attr.key for attr in mapper.mapper.column_attrs}
    original_column = model.cliente_nombre_original if "cliente_nombre_original" in mapped_cols else literal(None)
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
    if filters.clientes:
        _visible = _cliente_visible_expr(model)
        normalized_clients = _normalized_filter_values(filters.clientes)
        if normalized_clients:
            stmt = stmt.where(_sql_normalized_text(_visible).in_(normalized_clients))
            if applied_conditions is not None:
                applied_conditions.append(f"cliente_visible IN {normalized_clients}")
    if filters.provincias:
        normalized_provincias = _normalized_filter_values(filters.provincias)
        if normalized_provincias:
            stmt = stmt.where(_sql_normalized_text(model.provincia).in_(normalized_provincias))
            if applied_conditions is not None:
                applied_conditions.append(f"provincia IN {normalized_provincias}")
    if filters.familias:
        normalized_familias = _normalized_filter_values(filters.familias)
        if normalized_familias:
            stmt = stmt.where(_sql_normalized_text(model.familia).in_(normalized_familias))
            if applied_conditions is not None:
                applied_conditions.append(f"familia IN {normalized_familias}")
    if filters.plataformas:
        normalized_plataformas = _normalized_filter_values(filters.plataformas)
        if normalized_plataformas:
            stmt = stmt.where(_sql_normalized_text(model.plataforma).in_(normalized_plataformas))
            if applied_conditions is not None:
                applied_conditions.append(f"plataforma IN {normalized_plataformas}")
    if filters.unidades_negocio:
        normalized_unidades = _normalized_filter_values(filters.unidades_negocio)
        if normalized_unidades:
            stmt = stmt.where(_sql_normalized_text(model.unidad_negocio).in_(normalized_unidades))
            if applied_conditions is not None:
                applied_conditions.append(f"unidad_negocio IN {normalized_unidades}")
    if filters.subunidades_negocio:
        normalized_subunidades = _normalized_filter_values(filters.subunidades_negocio)
        if normalized_subunidades:
            stmt = stmt.where(_sql_normalized_text(model.subunidad_negocio).in_(normalized_subunidades))
            if applied_conditions is not None:
                applied_conditions.append(f"subunidad_negocio IN {normalized_subunidades}")
    if filters.resultados:
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


def _aggregate_model_for_filters(filters: DimensionamientoFilters):
    return DimensionamientoRecord



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
    started_at = time.perf_counter()
    logger.info("[DIM][QUERY] get_filter_options start filters=%s", _filters_debug_dict(filters))
    _apply_local_statement_timeout(session, 50000)

    try:
        if (
            not filters.clientes
            and not filters.provincias
            and not filters.familias
            and not filters.plataformas
            and not filters.unidades_negocio
            and not filters.subunidades_negocio
            and not filters.resultados
            and filters.fecha_desde is None
            and filters.fecha_hasta is None
            and filters.identified is None
            and filters.is_client is None
            and not filters.search
        ):
            logger.info("[DIM][QUERY] get_filter_options using summary table fast path")
            min_date, max_date = session.execute(
                select(
                    func.min(DimensionamientoFamilyMonthlySummary.month),
                    func.max(DimensionamientoFamilyMonthlySummary.month),
                )
            ).one()
            payload = {
                # Usar la tabla resumen para clientes evita un full scan de 400k+ filas
                # en dimensionamiento_records. La tabla resumen ya tiene los datos normalizados.
                "clientes": _distinct_summary_clients(session),
                "provincias": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.provincia),
                "familias": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.familia),
                "plataformas": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.plataforma),
                "unidades_negocio": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.unidad_negocio),
                "subunidades_negocio": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.subunidad_negocio),
                "resultados": _distinct_summary_values(session, DimensionamientoFamilyMonthlySummary.resultado_participacion),
                "date_range": {
                    "min": min_date.isoformat() if min_date else None,
                    "max": max_date.isoformat() if max_date else None,
                },
            }
        else:
            applied_conditions: list[str] = []
            date_bounds_stmt = _apply_common_filters(
                select(
                    func.min(DimensionamientoRecord.fecha),
                    func.max(DimensionamientoRecord.fecha),
                ),
                DimensionamientoRecord,
                filters,
                applied_conditions,
            )
            _log_query_statement(session, "get_filter_options.date_bounds", DimensionamientoRecord, date_bounds_stmt, filters, applied_conditions)
            min_date, max_date = session.execute(date_bounds_stmt).one()
            payload = {
                "clientes": _distinct_visible_clients(session, filters),
                "provincias": _distinct_values(session, DimensionamientoRecord.provincia, filters),
                "familias": _distinct_values(session, DimensionamientoRecord.familia, filters),
                "plataformas": _distinct_values(session, DimensionamientoRecord.plataforma, filters),
                "unidades_negocio": _distinct_values(session, DimensionamientoRecord.unidad_negocio, filters),
                "subunidades_negocio": _distinct_values(session, DimensionamientoRecord.subunidad_negocio, filters),
                "resultados": _distinct_values(session, DimensionamientoRecord.resultado_participacion, filters),
                "date_range": {
                    "min": min_date.isoformat() if min_date else None,
                    "max": max_date.isoformat() if max_date else None,
                },
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
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_filter_options failed filters=%s", _filters_debug_dict(filters))
        raise


def get_kpis(session: Session, filters: DimensionamientoFilters) -> dict[str, Any]:
    started_at = _log_query_start("get_kpis", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _aggregate_model_for_filters(filters)
        _visible = _cliente_visible_expr(model)
        applied_conditions: list[str] = []
        stmt = _apply_common_filters(
            select(
                func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0),
                func.count(distinct(_visible)),
                func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0),
                func.count(distinct(model.familia)),
                func.coalesce(
                    func.sum(model.cantidad_demandada if model is DimensionamientoRecord else model.total_cantidad),
                    0,
                ),
            ),
            model,
            filters,
            applied_conditions,
        )
        _log_query_statement(session, "get_kpis", model, stmt, filters, applied_conditions)
        total_rows, total_clients, total_records, total_families, total_quantity = session.execute(stmt).one()
        payload = {
            "total_rows": total_rows or 0,
            "clientes": total_clients or 0,
            "renglones": total_records or 0,
            "familias": total_families or 0,
            "cantidad_demandada": float(total_quantity or 0),
        }
        _log_query_success("get_kpis", started_at, total_rows=payload["total_rows"], clientes=payload["clientes"])
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_kpis failed filters=%s", _filters_debug_dict(filters))
        raise


def get_series(session: Session, filters: DimensionamientoFilters, limit: int = 5) -> dict[str, Any]:
    started_at = _log_query_start("get_series", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _aggregate_model_for_filters(filters)
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

        date_column = model.fecha if model is DimensionamientoRecord else model.month
        month_expr = cast(_month_expr(date_column), Date) if (model is DimensionamientoRecord and not IS_SQLITE) else date_column
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
            month_key = month.isoformat() if hasattr(month, "isoformat") else str(month)
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
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_series failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise


def get_results_breakdown(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    started_at = _log_query_start("get_results_breakdown", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _aggregate_model_for_filters(filters)
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
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_results_breakdown failed filters=%s", _filters_debug_dict(filters))
        raise


def get_top_families(session: Session, filters: DimensionamientoFilters, limit: int = 10) -> list[dict[str, Any]]:
    started_at = _log_query_start("get_top_families", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _aggregate_model_for_filters(filters)
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
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_top_families failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise


def get_geography_distribution(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    started_at = _log_query_start("get_geography_distribution", filters)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _aggregate_model_for_filters(filters)
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
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_geography_distribution failed filters=%s", _filters_debug_dict(filters))
        raise


def get_clients_by_result(
    session: Session,
    filters: DimensionamientoFilters,
    limit: int = 10,
) -> list[dict[str, Any]]:
    started_at = _log_query_start("get_clients_by_result", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _aggregate_model_for_filters(filters)
        _visible_raw = _cliente_visible_expr(model)
        total_expr = func.count(model.id) if model is DimensionamientoRecord else func.coalesce(func.sum(model.total_registros), 0)
        subquery_conditions: list[str] = []
        subquery = _apply_common_filters(
            select(
                _visible_raw.label("cliente"),
                model.resultado_participacion.label("resultado"),
                total_expr.label("total"),
            )
            .where(_visible_raw.isnot(None))
            .where(func.coalesce(_visible_raw, "") != "")
            .group_by(
                _visible_raw,
                model.resultado_participacion,
            ),
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
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_clients_by_result failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise


def get_family_consumption_table(
    session: Session,
    filters: DimensionamientoFilters,
    limit: int = 20,
) -> dict[str, Any]:
    started_at = _log_query_start("get_family_consumption_table", filters, limit=limit)
    try:
        _apply_local_statement_timeout(session, 50000)
        model = _aggregate_model_for_filters(filters)
        date_column = model.fecha if model is DimensionamientoRecord else model.month
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
        month_bucket = cast(_month_expr(date_column), Date)
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
            # Extraer número de mes del objeto date (o del string 'YYYY-MM-DD')
            if hasattr(month_date, "month"):
                month_key = f"{month_date.month:02d}"
            else:
                try:
                    month_key = str(month_date)[5:7]
                except Exception:
                    month_key = "01"
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
        return payload
    except Exception:
        logger.exception("[DIM][QUERY] get_family_consumption_table failed filters=%s limit=%s", _filters_debug_dict(filters), limit)
        raise
