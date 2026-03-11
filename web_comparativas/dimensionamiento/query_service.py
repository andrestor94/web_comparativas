from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Iterable

from sqlalchemy import Date, case, cast, distinct, func, or_, select
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from web_comparativas.models import IS_SQLITE

from .models import DimensionamientoImportRun, DimensionamientoRecord


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


def _month_expr(column):
    if IS_SQLITE:
        return func.date(column, "start of month")
    return func.date_trunc("month", column)


def _normalize_list(values: Iterable[str] | None) -> list[str]:
    if not values:
        return []
    cleaned: list[str] = []
    for value in values:
        if value is None:
            continue
        for part in str(value).split(","):
            item = part.strip()
            if item:
                cleaned.append(item)
    return cleaned


_SIN_DATO_SQL = ("SIN DATO", "SIN_DATO")


def _cliente_visible_expr(model):
    """
    Expresión SQL para el nombre visible del cliente:
    - cliente_nombre_homologado si es válido (no vacío, no variante de SIN DATO)
    - cliente_nombre_original como fallback cuando homologado es inválido
    - NULL si ambos son nulos/vacíos
    """
    _homologado_upper = func.upper(func.trim(func.coalesce(model.cliente_nombre_homologado, "")))
    _homologado_is_invalid = or_(
        func.trim(func.coalesce(model.cliente_nombre_homologado, "")) == "",
        _homologado_upper.in_(list(_SIN_DATO_SQL)),
    )
    return case(
        (_homologado_is_invalid, model.cliente_nombre_original),
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


def _apply_common_filters(stmt, model, filters: DimensionamientoFilters):
    if filters.clientes:
        # Filtrar por nombre visible: homologado si válido, original como fallback
        _visible = _cliente_visible_expr(model)
        stmt = stmt.where(_visible.in_(filters.clientes))
    if filters.provincias:
        stmt = stmt.where(model.provincia.in_(filters.provincias))
    if filters.familias:
        stmt = stmt.where(model.familia.in_(filters.familias))
    if filters.plataformas:
        stmt = stmt.where(model.plataforma.in_(filters.plataformas))
    if filters.unidades_negocio:
        stmt = stmt.where(model.unidad_negocio.in_(filters.unidades_negocio))
    if filters.subunidades_negocio:
        stmt = stmt.where(model.subunidad_negocio.in_(filters.subunidades_negocio))
    if filters.resultados:
        stmt = stmt.where(model.resultado_participacion.in_(filters.resultados))
    if filters.identified is not None:
        stmt = stmt.where(model.is_identified.is_(filters.identified))
    if filters.is_client is not None:
        # Fuente de verdad: cliente_nombre_homologado.
        # "SIN DATO" y sus variantes (sin_dato, mayúsculas, espacios extra) = no cliente.
        _nombre_upper = func.upper(func.trim(func.coalesce(model.cliente_nombre_homologado, "")))
        _is_empty_or_sin_dato = or_(
            func.trim(func.coalesce(model.cliente_nombre_homologado, "")) == "",
            _nombre_upper.in_(list(_SIN_DATO_SQL)),
        )
        if filters.is_client:
            # Sí: nombre real (no vacío, no variante de SIN DATO)
            stmt = stmt.where(~_is_empty_or_sin_dato)
        else:
            # No: nulo, vacío, o cualquier variante de SIN DATO
            stmt = stmt.where(_is_empty_or_sin_dato)

    if filters.fecha_desde is not None:
        date_column = _get_date_column(model)
        stmt = stmt.where(date_column >= filters.fecha_desde)
    if filters.fecha_hasta is not None:
        date_column = _get_date_column(model)
        stmt = stmt.where(date_column <= filters.fecha_hasta)

    if filters.search:
        token = f"%{filters.search.lower()}%"
        stmt = stmt.where(
            func.lower(func.coalesce(model.cliente_nombre_homologado, "")).like(token)
            | func.lower(func.coalesce(getattr(model, "cliente_nombre_original", ""), "")).like(token)
            | func.lower(func.coalesce(model.familia, "")).like(token)
            | func.lower(func.coalesce(getattr(model, "codigo_articulo", ""), "")).like(token)
            | func.lower(func.coalesce(getattr(model, "producto_nombre_original", ""), "")).like(token)
        )
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



def get_status(session: Session) -> dict[str, Any]:
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

    return {
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


def get_filter_options(session: Session, filters: DimensionamientoFilters) -> dict[str, Any]:
    date_bounds_stmt = _apply_common_filters(
        select(
            func.min(DimensionamientoRecord.fecha),
            func.max(DimensionamientoRecord.fecha),
        ),
        DimensionamientoRecord,
        filters,
    )
    min_date, max_date = session.execute(date_bounds_stmt).one()

    return {
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


def get_kpis(session: Session, filters: DimensionamientoFilters) -> dict[str, Any]:
    _visible = _cliente_visible_expr(DimensionamientoRecord)
    stmt = _apply_common_filters(
        select(
            func.count(DimensionamientoRecord.id),
            func.count(distinct(_visible)),
            func.count(DimensionamientoRecord.id),
            func.count(distinct(DimensionamientoRecord.familia)),
            func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0),
        ),
        DimensionamientoRecord,
        filters,
    )
    total_rows, total_clients, total_records, total_families, total_quantity = session.execute(stmt).one()
    return {
        "total_rows": total_rows or 0,
        "clientes": total_clients or 0,
        "renglones": total_records or 0,
        "familias": total_families or 0,
        "cantidad_demandada": float(total_quantity or 0),
    }


def get_series(session: Session, filters: DimensionamientoFilters, limit: int = 5) -> dict[str, Any]:
    negocio_expr = func.coalesce(DimensionamientoRecord.unidad_negocio, "Sin negocio")
    top_business_stmt = _apply_common_filters(
        select(
            negocio_expr.label("negocio"),
            func.count(DimensionamientoRecord.id).label("renglones"),
        )
        .group_by(negocio_expr)
        .order_by(func.count(DimensionamientoRecord.id).desc(), negocio_expr.asc())
        .limit(limit),
        DimensionamientoRecord,
        filters,
    )
    top_businesses = [row[0] for row in session.execute(top_business_stmt).all()]

    month_expr = cast(_month_expr(DimensionamientoRecord.fecha), Date) if not IS_SQLITE else _month_expr(DimensionamientoRecord.fecha)
    stmt = _apply_common_filters(
        select(
            month_expr.label("month"),
            negocio_expr.label("negocio"),
            func.count(DimensionamientoRecord.id).label("renglones"),
        )
        .where(negocio_expr.in_(top_businesses) if top_businesses else False)
        .group_by(month_expr, negocio_expr)
        .order_by(month_expr.asc(), negocio_expr.asc()),
        DimensionamientoRecord,
        filters,
    )
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
    return {"months": months, "datasets": datasets}


def get_results_breakdown(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    stmt = _apply_common_filters(
        select(
            DimensionamientoRecord.resultado_participacion,
            func.count(DimensionamientoRecord.id).label("rows"),
        )
        .group_by(DimensionamientoRecord.resultado_participacion)
        .order_by(func.count(DimensionamientoRecord.id).desc()),
        DimensionamientoRecord,
        filters,
    )
    return [
        {
            "resultado": resultado or "Sin resultado",
            "renglones": rows or 0,
        }
        for resultado, rows in session.execute(stmt).all()
    ]


def get_top_families(session: Session, filters: DimensionamientoFilters, limit: int = 10) -> list[dict[str, Any]]:
    stmt = _apply_common_filters(
        select(
            DimensionamientoRecord.familia,
            func.count(DimensionamientoRecord.id).label("rows"),
            func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0).label("quantity"),
        )
        .where(DimensionamientoRecord.familia.is_not(None))
        .group_by(DimensionamientoRecord.familia)
        .order_by(func.count(DimensionamientoRecord.id).desc(), DimensionamientoRecord.familia.asc())
        .limit(limit),
        DimensionamientoRecord,
        filters,
    )
    return [
        {
            "familia": familia or "Sin familia",
            "renglones": rows or 0,
            "cantidad": float(quantity or 0),
        }
        for familia, rows, quantity in session.execute(stmt).all()
    ]


def get_geography_distribution(session: Session, filters: DimensionamientoFilters) -> list[dict[str, Any]]:
    stmt = _apply_common_filters(
        select(
            DimensionamientoRecord.provincia,
            func.count(DimensionamientoRecord.id).label("rows"),
        )
        .group_by(DimensionamientoRecord.provincia)
        .order_by(func.count(DimensionamientoRecord.id).desc()),
        DimensionamientoRecord,
        filters,
    )
    return [
        {
            "provincia": provincia or "Sin provincia",
            "renglones": rows or 0,
        }
        for provincia, rows in session.execute(stmt).all()
    ]


def get_clients_by_result(
    session: Session,
    filters: DimensionamientoFilters,
    limit: int = 10,
) -> list[dict[str, Any]]:
    # Nombre visible: homologado si válido, original como fallback (nunca "SIN DATO")
    _visible_raw = _cliente_visible_expr(DimensionamientoRecord)
    subquery = _apply_common_filters(
        select(
            _visible_raw.label("cliente"),
            DimensionamientoRecord.resultado_participacion.label("resultado"),
            func.count(DimensionamientoRecord.id).label("total"),
        )
        .where(_visible_raw.isnot(None))
        .where(func.coalesce(_visible_raw, "") != "")
        .group_by(
            _visible_raw,
            DimensionamientoRecord.resultado_participacion,
        ),
        DimensionamientoRecord,
        filters,
    ).subquery()

    top_clients_stmt = select(
        subquery.c.cliente,
        func.sum(subquery.c.total).label("grand_total"),
    ).group_by(subquery.c.cliente).order_by(func.sum(subquery.c.total).desc()).limit(limit)
    top_clients = [row[0] for row in session.execute(top_clients_stmt).all()]
    if not top_clients:
        return []

    detail_stmt = (
        select(subquery.c.cliente, subquery.c.resultado, subquery.c.total)
        .where(subquery.c.cliente.in_(top_clients))
        .order_by(subquery.c.cliente.asc(), subquery.c.resultado.asc())
    )

    client_map: dict[str, dict[str, float]] = {}
    for cliente, resultado, total in session.execute(detail_stmt).all():
        client_map.setdefault(cliente, {})[resultado or "Sin resultado"] = float(total or 0)

    return [
        {"cliente": cliente, "resultados": client_map.get(cliente, {})}
        for cliente in top_clients
    ]


def get_family_consumption_table(
    session: Session,
    filters: DimensionamientoFilters,
    limit: int = 20,
) -> dict[str, Any]:
    month_number = func.strftime("%m", DimensionamientoRecord.fecha) if IS_SQLITE else func.to_char(DimensionamientoRecord.fecha, "MM")
    year_number = func.strftime("%Y", DimensionamientoRecord.fecha) if IS_SQLITE else func.to_char(DimensionamientoRecord.fecha, "YYYY")
    stmt = _apply_common_filters(
        select(
            year_number.label("year_number"),
            month_number.label("month_number"),
            DimensionamientoRecord.familia,
            func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0).label("total"),
        )
        .where(DimensionamientoRecord.familia.is_not(None))
        .group_by(
            year_number,
            month_number,
            DimensionamientoRecord.familia,
        )
        .order_by(DimensionamientoRecord.familia.asc(), year_number.asc(), month_number.asc()),
        DimensionamientoRecord,
        filters,
    )
    rows = session.execute(stmt).all()
    if not rows:
        return {"months": [], "rows": []}

    family_totals: dict[str, float] = {}
    month_keys = [f"{index:02d}" for index in range(1, 13)]
    monthly_values: dict[str, dict[str, list[float]]] = {}
    for _, month_number_value, family, total in rows:
        month_key = str(month_number_value).zfill(2)
        family_name = family or "Sin familia"
        family_totals[family_name] = family_totals.get(family_name, 0) + float(total or 0)
        monthly_values.setdefault(family_name, {}).setdefault(month_key, []).append(float(total or 0))

    top_families = [
        family
        for family, _ in sorted(family_totals.items(), key=lambda item: item[1], reverse=True)[:limit]
    ]
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
    return {"months": month_keys, "rows": data}
