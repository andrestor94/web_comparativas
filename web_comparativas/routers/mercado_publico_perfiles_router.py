"""
Router de analytics para el módulo Reporte de Perfiles de Mercado Público.
Todos los endpoints consumen la tabla comparativa_rows (fuente normalizada).
Las agregaciones son server-side; el frontend recibe datos ya procesados.
"""
from __future__ import annotations

import datetime as dt
import io
import re
import statistics
import time
import hashlib
import json
from typing import Optional

from fastapi import APIRouter, Depends, Query, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, func, extract, case, distinct, text, and_, or_, cast, Float as SAFloat
from sqlalchemy.orm import Session

from web_comparativas.models import (
    SessionLocal, db_session, User, Upload, ComparativaRow
)
from web_comparativas.auth import require_roles

router = APIRouter(prefix="/api/mercado-publico/perfiles", tags=["perfiles"])

# ── Cache in-memory ──────────────────────────────────────────────────────────
_CACHE: dict[str, dict] = {}
_CACHE_MAX = 256
_TTL_FILTERS = 300.0   # 5 min — filter options cambian poco
_TTL_ANALYTICS = 120.0  # 2 min — agregaciones

def _cache_key(*parts) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()

def _cache_get(key: str, ttl: float):
    entry = _CACHE.get(key)
    if entry is None:
        return None
    if time.perf_counter() - entry["ts"] > ttl:
        _CACHE.pop(key, None)
        return None
    return entry["val"]

def _cache_set(key: str, val):
    if len(_CACHE) >= _CACHE_MAX:
        _CACHE.clear()
    _CACHE[key] = {"ts": time.perf_counter(), "val": val}

def invalidate_perfiles_cache():
    _CACHE.clear()


# ── Helpers de filtro ────────────────────────────────────────────────────────

def _get_session(request: Request) -> Session:
    return getattr(request.state, "db", None) or db_session

def _apply_date_filters(q, fecha_desde: Optional[str], fecha_hasta: Optional[str]):
    if fecha_desde:
        try:
            q = q.where(ComparativaRow.fecha_apertura >= dt.date.fromisoformat(fecha_desde))
        except ValueError:
            pass
    if fecha_hasta:
        try:
            q = q.where(ComparativaRow.fecha_apertura <= dt.date.fromisoformat(fecha_hasta))
        except ValueError:
            pass
    return q

def _apply_multi(q, column, values: Optional[str]):
    """Filtra por una lista de valores separados por coma."""
    if not values:
        return q
    vals = _split_filter_values(values)
    if vals:
        q = q.where(column.in_(vals))
    return q

def _split_filter_values(values: Optional[str]) -> list[str]:
    if not values:
        return []
    # Separator is "||" to avoid splitting on commas in values (e.g. "0,6 ml" in Spanish decimal notation)
    seen = set()
    parsed: list[str] = []
    for raw in values.split("||"):
        value = raw.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        parsed.append(value)
    return parsed

def _apply_exact_text(q, column, values: Optional[str]):
    vals = _split_filter_values(values)
    if vals:
        q = q.where(column.in_(vals))
    return q

def _apply_filter_search_context(
    q,
    campo: str,
    *,
    descripcion: str = "",
    marca: str = "",
    proveedor: str = "",
    rubro: str = "",
    plataforma: str = "",
    fecha_desde: str = "",
    fecha_hasta: str = "",
):
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    if campo != "descripcion":
        q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    if campo != "marca":
        q = _apply_multi(q, ComparativaRow.marca, marca)
    if campo != "proveedor":
        q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    if campo != "rubro":
        q = _apply_multi(q, ComparativaRow.rubro, rubro)
    return q


def _articulos_participaciones_unicas_subquery(
    *,
    descripcion: str = "",
    fecha_desde: str = "",
    fecha_hasta: str = "",
    marca: str = "",
    proveedor: str = "",
    rubro: str = "",
    plataforma: str = "",
):
    """Base unica de participaciones reales para articulo/proveedor.

    Evita contar duplicados tecnicos de la misma fila normalizada, sin colapsar
    procesos o renglones distintos que comparten fecha/precio/marca/posicion.
    """
    key_cols = (
        ComparativaRow.upload_id,
        ComparativaRow.nro_proceso,
        ComparativaRow.fecha_apertura,
        ComparativaRow.renglon,
        ComparativaRow.alternativa,
        ComparativaRow.codigo,
        ComparativaRow.descripcion,
        ComparativaRow.proveedor,
        ComparativaRow.marca,
        ComparativaRow.precio_unitario,
        ComparativaRow.posicion,
    )
    rn = func.row_number().over(
        partition_by=key_cols,
        order_by=ComparativaRow.id.asc(),
    ).label("participacion_rank")
    q = (
        select(
            ComparativaRow.id,
            ComparativaRow.upload_id,
            ComparativaRow.nro_proceso,
            ComparativaRow.fecha_apertura,
            ComparativaRow.comprador,
            ComparativaRow.proveedor,
            ComparativaRow.renglon,
            ComparativaRow.alternativa,
            ComparativaRow.codigo,
            ComparativaRow.descripcion,
            ComparativaRow.precio_unitario,
            ComparativaRow.cantidad_ofertada,
            ComparativaRow.total_por_renglon,
            ComparativaRow.marca,
            ComparativaRow.posicion,
            ComparativaRow.rubro,
            ComparativaRow.plataforma,
            rn,
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.proveedor.isnot(None))
    )
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    ranked = q.subquery("participaciones_ranked")
    return (
        select(ranked)
        .where(ranked.c.participacion_rank == 1)
        .subquery("participaciones_unicas")
    )

def _resolve_grouped_primary_value(rows) -> dict:
    ranked: list[tuple[str, int]] = []
    for raw_value, raw_count in rows:
        value = (raw_value or "").strip()
        if not value:
            continue
        ranked.append((value, int(raw_count or 0)))

    if not ranked:
        return {
            "value": None,
            "count": 0,
            "multiple": False,
            "values": [],
        }

    ranked.sort(key=lambda item: (-item[1], item[0].lower(), item[0]))
    values = [value for value, _ in ranked]
    return {
        "value": values[0],
        "count": len(values),
        "multiple": len(values) > 1,
        "values": values,
    }

def _period_label(year, quarter) -> str:
    return f"Q{int(quarter)} {int(year)}"

def _quarter_expr():
    m = extract("month", ComparativaRow.fecha_apertura)
    return case(
        (m.in_([1, 2, 3]), 1),
        (m.in_([4, 5, 6]), 2),
        (m.in_([7, 8, 9]), 3),
        else_=4,
    )


# ── Sync ─────────────────────────────────────────────────────────────────────

@router.get("/sync/status")
def sync_status(
    request: Request,
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    session = _get_session(request)
    total_uploads = session.execute(
        select(func.count()).select_from(Upload)
        .where(Upload.status.in_(["done", "reviewing", "dashboard"]))
        .where(Upload.normalized_content.isnot(None))
    ).scalar_one()

    synced_uploads = session.execute(
        select(func.count(distinct(ComparativaRow.upload_id)))
    ).scalar_one()

    total_rows = session.execute(
        select(func.count()).select_from(ComparativaRow)
    ).scalar_one()

    return {"ok": True, "data": {
        "total_uploads": total_uploads,
        "synced_uploads": synced_uploads,
        "pending_uploads": max(0, total_uploads - synced_uploads),
        "total_rows": total_rows,
    }}


@router.post("/sync")
def trigger_sync(
    background_tasks: BackgroundTasks,
    user: User = Depends(require_roles("admin")),
):
    def _run_sync():
        from web_comparativas.migrations import backfill_comparativa_rows
        backfill_comparativa_rows()
        invalidate_perfiles_cache()

    background_tasks.add_task(_run_sync)
    return {"ok": True, "msg": "Sync iniciado en background."}


# ── Opciones de filtro ────────────────────────────────────────────────────────

@router.get("/filtros")
def get_filtros(
    request: Request,
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("filtros_globales")
    cached = _cache_get(ck, _TTL_FILTERS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)

    def _vals(col):
        rows = session.execute(
            select(col).where(col.isnot(None)).where(col != "").distinct().order_by(col)
        ).scalars().all()
        return [r for r in rows if r]

    plataformas = _vals(ComparativaRow.plataforma)
    compradores = _vals(ComparativaRow.comprador)
    provincias = _vals(ComparativaRow.provincia)
    rubros = _vals(ComparativaRow.rubro)

    data = {
        "plataformas": plataformas,
        "compradores": compradores[:200],
        "provincias": provincias,
        "rubros": rubros,
    }
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/filtros/search")
def search_filtro(
    request: Request,
    campo: str = Query(..., description="descripcion | proveedor | marca | comprador"),
    q: str = Query("", description="Término de búsqueda"),
    limit: int = Query(50, ge=1, le=5000),
    descripcion: str = Query(""),
    marca: str = Query(""),
    proveedor: str = Query(""),
    rubro: str = Query(""),
    plataforma: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    CAMPO_MAP = {
        "descripcion": ComparativaRow.descripcion,
        "proveedor": ComparativaRow.proveedor,
        "marca": ComparativaRow.marca,
        "comprador": ComparativaRow.comprador,
    }
    col = CAMPO_MAP.get(campo)
    if col is None:
        return JSONResponse({"ok": False, "error": "Campo inválido"}, status_code=400)

    session = _get_session(request)
    term = f"%{q.strip()}%" if q.strip() else "%"
    stmt = select(col).where(col.isnot(None)).where(col != "").where(col.ilike(term))
    stmt = _apply_filter_search_context(
        stmt,
        campo,
        descripcion=descripcion,
        marca=marca,
        proveedor=proveedor,
        rubro=rubro,
        plataforma=plataforma,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
    )
    rows = session.execute(stmt.distinct().order_by(col).limit(limit)).scalars().all()
    return {"ok": True, "data": [r for r in rows if r]}


@router.get("/filtros/rango-fechas")
def filtros_rango_fechas(
    request: Request,
    descripcion: str = Query(""),
    proveedor: str = Query(""),
    marca: str = Query(""),
    rubro: str = Query(""),
    comprador: str = Query(""),
    provincia: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("rango_fechas", descripcion, proveedor, marca, rubro, comprador, provincia, plataforma)
    cached = _cache_get(ck, _TTL_FILTERS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            func.min(ComparativaRow.fecha_apertura).label("fecha_min"),
            func.max(ComparativaRow.fecha_apertura).label("fecha_max"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
    )
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.comprador, comprador)
    q = _apply_multi(q, ComparativaRow.provincia, provincia)

    row = session.execute(q).one_or_none()
    data = {
        "fecha_min": row.fecha_min.isoformat() if row and row.fecha_min else None,
        "fecha_max": row.fecha_max.isoformat() if row and row.fecha_max else None,
    }
    _cache_set(ck, data)
    return {"ok": True, "data": data}


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — ARTÍCULOS
# ══════════════════════════════════════════════════════════════════════════════

def _articulos_base(session, descripcion, fecha_desde, fecha_hasta, marca, proveedor, rubro, plataforma=""):
    q = select(ComparativaRow).where(ComparativaRow.fecha_apertura.isnot(None))
    if descripcion:
        q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)
    return q


@router.get("/articulos/kpis")
def articulos_kpis(
    request: Request,
    descripcion: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    marca: str = Query(""),
    proveedor: str = Query(""),
    rubro: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("art_kpis", descripcion, fecha_desde, fecha_hasta, marca, proveedor, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)

    base = (select(
        func.count(distinct(ComparativaRow.proveedor)).label("proveedores_unicos"),
        func.count(distinct(ComparativaRow.marca)).label("marcas_distintas"),
        func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
        func.sum(ComparativaRow.cantidad_solicitada).label("cant_solicitada"),
        func.sum(ComparativaRow.cantidad_ofertada).label("cant_ofertada"),
        func.sum(
            case((ComparativaRow.posicion == 1, ComparativaRow.precio_unitario * ComparativaRow.cantidad_ofertada), else_=0)
        ).label("total_adjudicado"),
        func.min(ComparativaRow.posicion).label("mejor_posicion"),
    )
    .where(ComparativaRow.fecha_apertura.isnot(None)))

    base = _apply_exact_text(base, ComparativaRow.descripcion, descripcion)
    base = _apply_date_filters(base, fecha_desde, fecha_hasta)
    base = _apply_multi(base, ComparativaRow.marca, marca)
    base = _apply_multi(base, ComparativaRow.proveedor, proveedor)
    base = _apply_multi(base, ComparativaRow.rubro, rubro)
    base = _apply_exact_text(base, ComparativaRow.plataforma, plataforma)

    row = session.execute(base).one_or_none()

    # Mediana de precio unitario: calculada en Python
    precios_q = select(ComparativaRow.precio_unitario).where(
        ComparativaRow.precio_unitario.isnot(None),
        ComparativaRow.fecha_apertura.isnot(None),
    )
    precios_q = _apply_exact_text(precios_q, ComparativaRow.descripcion, descripcion)
    precios_q = _apply_date_filters(precios_q, fecha_desde, fecha_hasta)
    precios_q = _apply_multi(precios_q, ComparativaRow.marca, marca)
    precios_q = _apply_multi(precios_q, ComparativaRow.proveedor, proveedor)
    precios_q = _apply_multi(precios_q, ComparativaRow.rubro, rubro)
    precios_q = _apply_exact_text(precios_q, ComparativaRow.plataforma, plataforma)
    prices = [r[0] for r in session.execute(precios_q).all() if r[0] is not None]
    mediana = round(statistics.median(prices), 2) if prices else None

    rubro_q = (
        select(
            ComparativaRow.rubro.label("value"),
            func.count().label("total"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.rubro.isnot(None))
        .where(ComparativaRow.rubro != "")
        .group_by(ComparativaRow.rubro)
    )
    rubro_q = _apply_exact_text(rubro_q, ComparativaRow.descripcion, descripcion)
    rubro_q = _apply_date_filters(rubro_q, fecha_desde, fecha_hasta)
    rubro_q = _apply_multi(rubro_q, ComparativaRow.marca, marca)
    rubro_q = _apply_multi(rubro_q, ComparativaRow.proveedor, proveedor)
    rubro_q = _apply_multi(rubro_q, ComparativaRow.rubro, rubro)
    rubro_q = _apply_exact_text(rubro_q, ComparativaRow.plataforma, plataforma)
    rubro_info = _resolve_grouped_primary_value(session.execute(rubro_q).all())

    data = {
        "proveedores_unicos": row.proveedores_unicos if row else 0,
        "marcas_distintas": row.marcas_distintas if row else 0,
        "procesos": row.procesos if row else 0,
        "cantidad_solicitada": round(row.cant_solicitada or 0, 2) if row else 0,
        "cantidad_ofertada": round(row.cant_ofertada or 0, 2) if row else 0,
        "total_adjudicado": round(row.total_adjudicado or 0, 2) if row else 0,
        "mediana_precio": mediana,
        "mejor_posicion": row.mejor_posicion if row else None,
        "rubro_principal": rubro_info["value"],
        "rubros_detectados": rubro_info["count"],
        "rubros_multiples": rubro_info["multiple"],
        "rubros_lista": rubro_info["values"],
    }
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/articulos/evolucion")
def articulos_evolucion(
    request: Request,
    descripcion: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    marca: str = Query(""),
    proveedor: str = Query(""),
    rubro: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("art_evol", descripcion, fecha_desde, fecha_hasta, marca, proveedor, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _year  = extract("year",  ComparativaRow.fecha_apertura)
    _month = extract("month", ComparativaRow.fecha_apertura)
    _quarter = case(
        (_month.in_([1, 2, 3]), 1),
        (_month.in_([4, 5, 6]), 2),
        (_month.in_([7, 8, 9]), 3),
        else_=4,
    )

    q = (
        select(
            _year.label("year"), _quarter.label("quarter"), _month.label("month"),
            func.avg(ComparativaRow.precio_unitario).label("avg_precio"),
            func.sum(ComparativaRow.cantidad_solicitada).label("cant_solicitada"),
            func.sum(ComparativaRow.cantidad_ofertada).label("cant_ofertada"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
        )
        .where(
            ComparativaRow.fecha_apertura.isnot(None),
            ComparativaRow.precio_unitario.isnot(None),
        )
        .group_by(_year, _quarter, _month)
        .order_by(_year, _quarter, _month)
    )
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()
    data = [
        {
            "year": int(r.year),
            "quarter": int(r.quarter),
            "month": int(r.month),
            "period": _period_label(r.year, r.quarter),
            "month_label": f"{_MONTH_NAMES[int(r.month) - 1]} {int(r.year)}",
            "avg_precio": round(r.avg_precio or 0, 2),
            "cantidad_solicitada": round(r.cant_solicitada or 0, 2),
            "cantidad_ofertada": round(r.cant_ofertada or 0, 2),
            "procesos": r.procesos,
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/articulos/por-marca")
def articulos_por_marca(
    request: Request,
    descripcion: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    proveedor: str = Query(""),
    rubro: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("art_marca_ganador", descripcion, fecha_desde, fecha_hasta, proveedor, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.fecha_apertura,
            ComparativaRow.marca,
            ComparativaRow.precio_unitario.label("precio_ganador")
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.marca.isnot(None))
        .where(ComparativaRow.posicion == 1)
        .where(ComparativaRow.precio_unitario.isnot(None))
        .order_by(ComparativaRow.fecha_apertura.asc())
        .limit(100)
    )
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()
    data = [
        {
            "fecha": r.fecha_apertura.isoformat() if hasattr(r.fecha_apertura, 'isoformat') else str(r.fecha_apertura),
            "marca": r.marca,
            "precio_ganador": round(r.precio_ganador or 0, 2),
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/articulos/por-proveedor")
def articulos_por_proveedor(
    request: Request,
    descripcion: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    marca: str = Query(""),
    rubro: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("art_prov_v4", descripcion, fecha_desde, fecha_hasta, marca, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    participaciones = _articulos_participaciones_unicas_subquery(
        descripcion=descripcion,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        marca=marca,
        rubro=rubro,
        plataforma=plataforma,
    )
    _adj_expr = func.sum(
        case((participaciones.c.posicion == 1, participaciones.c.precio_unitario * participaciones.c.cantidad_ofertada), else_=0)
    )
    q = (
        select(
            participaciones.c.proveedor,
            func.count(case((participaciones.c.posicion == 1, 1))).label("veces_ganado"),
            func.sum(participaciones.c.total_por_renglon).label("total_ofertado"),
            _adj_expr.label("total_adjudicado"),
            func.count().label("count_filas"),
            func.count(distinct(participaciones.c.upload_id)).label("procesos"),
        )
        .select_from(participaciones)
        .group_by(participaciones.c.proveedor)
    )

    rows = session.execute(q).all()

    def _median(vals: list) -> float:
        if not vals:
            return 0.0
        s = sorted(vals)
        n = len(s)
        mid = n // 2
        return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0

    # Precios del período para calcular mediana y último precio por proveedor.
    # Ordenados por fecha desc → primer registro = precio más reciente.
    prov_names = [r.proveedor for r in rows]
    prices_by_prov: dict = {}       # prov -> list of unit prices (desc by date)
    ultimo_precio_by_prov: dict = {}  # prov -> most recent valid price
    if prov_names:
        hist_q = (
            select(
                participaciones.c.proveedor,
                participaciones.c.precio_unitario,
            )
            .select_from(participaciones)
            .where(participaciones.c.proveedor.in_(prov_names))
            .where(participaciones.c.precio_unitario.isnot(None))
            .where(participaciones.c.precio_unitario > 0)
            .order_by(participaciones.c.fecha_apertura.desc(), participaciones.c.id.asc())
        )
        for hr in session.execute(hist_q).all():
            prov = hr.proveedor
            prices_by_prov.setdefault(prov, []).append(hr.precio_unitario)
            if prov not in ultimo_precio_by_prov:
                ultimo_precio_by_prov[prov] = hr.precio_unitario

    data = [
        {
            "proveedor": r.proveedor,
            "mediana_precio": round(_median(prices_by_prov.get(r.proveedor, [])), 2),
            "veces_ganado": int(r.veces_ganado or 0),
            "total_adjudicado": round(r.total_adjudicado or 0, 2),
            "count": r.count_filas,
            "procesos": r.procesos,
            "ultimo_precio": round(ultimo_precio_by_prov.get(r.proveedor, 0), 2),
            "efectividad": round(int(r.veces_ganado or 0) / r.count_filas * 100, 1) if r.count_filas else 0,
        }
        for r in rows
    ]

    # Ordenar por: efectividad DESC, veces_ganado DESC, total_adjudicado DESC,
    # mediana_precio ASC (menor precio = más competitivo), proveedor ASC (desempate estable)
    data.sort(
        key=lambda x: (
            -(x["efectividad"] or 0),
            -x["veces_ganado"],
            -(x["total_adjudicado"] or 0),
            x["mediana_precio"] if x["mediana_precio"] else 9_999_999,
            (x["proveedor"] or "").lower(),
        )
    )

    _cache_set(ck, data)
    return {"ok": True, "data": data}


def _normalize_marca(m: str) -> str:
    """Normaliza nombre de marca para agrupar: strip, colapsa espacios, mayúsculas."""
    if not m:
        return ""
    return re.sub(r'\s+', ' ', m.strip()).upper()


@router.get("/articulos/evolucion-marca")
def articulos_evolucion_marca(
    request: Request,
    descripcion: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    marca: str = Query(""),
    proveedor: str = Query(""),
    rubro: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("art_evol_marca_v2", descripcion, fecha_desde, fecha_hasta, marca, proveedor, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _year  = extract("year",  ComparativaRow.fecha_apertura)
    _month = extract("month", ComparativaRow.fecha_apertura)

    # Traer filas individuales para calcular mediana real por marca normalizada.
    # No agrupamos por proveedor en SQL — la consolidación ocurre en Python.
    q = (
        select(
            _year.label("year"), _month.label("month"),
            ComparativaRow.marca.label("marca"),
            ComparativaRow.precio_unitario.label("precio_unitario"),
        )
        .where(
            ComparativaRow.fecha_apertura.isnot(None),
            ComparativaRow.precio_unitario.isnot(None),
            ComparativaRow.marca.isnot(None),
        )
        .order_by(_year, _month, ComparativaRow.marca)
    )
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()

    # Agrupar por (year, month, marca_normalizada) y calcular mediana de precio_unitario.
    # marca_display conserva el primer nombre encontrado con su capitalización original.
    groups: dict[tuple, list] = {}
    marca_display: dict[str, str] = {}

    for r in rows:
        y, mo = int(r.year), int(r.month)
        marca_norm = _normalize_marca(r.marca or "")
        if not marca_norm:
            continue
        if marca_norm not in marca_display:
            marca_display[marca_norm] = (r.marca or "").strip()
        key = (y, mo, marca_norm)
        groups.setdefault(key, []).append(r.precio_unitario)

    data = [
        {
            "year": y,
            "month": mo,
            "month_label": f"{_MONTH_NAMES[mo - 1]} {y}",
            "marca": marca_display[marca_norm],
            "mediana_precio": round(statistics.median(prices), 2),
        }
        for (y, mo, marca_norm), prices in sorted(groups.items())
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/articulos/proveedor-historico")
def articulos_proveedor_historico(
    request: Request,
    proveedor: str = Query(""),
    descripcion: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    marca: str = Query(""),
    rubro: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    if not proveedor.strip():
        return {"ok": True, "data": []}

    ck = _cache_key("art_prov_hist_v3", proveedor, descripcion, fecha_desde, fecha_hasta, marca, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    participaciones = _articulos_participaciones_unicas_subquery(
        descripcion=descripcion,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        marca=marca,
        proveedor=proveedor,
        rubro=rubro,
        plataforma=plataforma,
    )
    q = (
        select(
            participaciones.c.fecha_apertura,
            participaciones.c.precio_unitario,
            participaciones.c.marca,
            participaciones.c.posicion,
            participaciones.c.nro_proceso,
            participaciones.c.upload_id,
            participaciones.c.renglon,
            participaciones.c.codigo,
            participaciones.c.descripcion,
            participaciones.c.comprador,
        )
        .select_from(participaciones)
        .order_by(participaciones.c.fecha_apertura.desc(), participaciones.c.id.asc())
        .limit(500)
    )

    rows = session.execute(q).all()
    data = [
        {
            "fecha": r.fecha_apertura.isoformat() if r.fecha_apertura else None,
            "precio": round(r.precio_unitario or 0, 2),
            "marca": r.marca or "-",
            "posicion": r.posicion,
            "proceso": r.nro_proceso or (f"Upload {r.upload_id}" if r.upload_id else "-"),
            "renglon": r.renglon or "-",
            "codigo": r.codigo or "",
            "descripcion": r.descripcion or "",
            "comprador": r.comprador or "",
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — COMPETIDOR
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/competidor/kpis")
def competidor_kpis(
    request: Request,
    proveedor: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    rubro: str = Query(""),
    descripcion: str = Query(""),
    marca: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_kpis_v3", proveedor, fecha_desde, fecha_hasta, rubro, descripcion, marca, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    q = (
        select(
            _adj.label("total_adjudicado"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
            func.count(distinct(ComparativaRow.descripcion)).label("descripciones"),
            func.count(distinct(ComparativaRow.rubro)).label("rubros"),
            func.min(ComparativaRow.posicion).label("mejor_posicion"),
            func.count(distinct(ComparativaRow.marca)).label("marcas"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
    )
    q = _apply_exact_text(q, ComparativaRow.proveedor, proveedor)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    row = session.execute(q).one_or_none()
    data = {
        "total_adjudicado": round(row.total_adjudicado or 0, 2) if row else 0,
        "procesos": row.procesos if row else 0,
        "descripciones_cotizadas": row.descripciones if row else 0,
        "rubros_cubiertos": row.rubros if row else 0,
        "mejor_posicion": row.mejor_posicion if row else None,
        "marcas_utilizadas": row.marcas if row else 0,
    }
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/competidor/evolucion")
def competidor_evolucion(
    request: Request,
    proveedor: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    rubro: str = Query(""),
    descripcion: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_evol_v3", proveedor, fecha_desde, fecha_hasta, rubro, descripcion, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _year  = extract("year",  ComparativaRow.fecha_apertura)
    _month = extract("month", ComparativaRow.fecha_apertura)
    _quarter = case(
        (_month.in_([1, 2, 3]), 1),
        (_month.in_([4, 5, 6]), 2),
        (_month.in_([7, 8, 9]), 3),
        else_=4,
    )
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))

    q = (
        select(
            _year.label("year"), _quarter.label("quarter"), _month.label("month"),
            _adj.label("monto_total"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .group_by(_year, _quarter, _month)
        .order_by(_year, _quarter, _month)
    )
    q = _apply_exact_text(q, ComparativaRow.proveedor, proveedor)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()
    data = [
        {
            "year": int(r.year),
            "quarter": int(r.quarter),
            "month": int(r.month),
            "period": _period_label(r.year, r.quarter),
            "month_label": f"{_MONTH_NAMES[int(r.month) - 1]} {int(r.year)}",
            "monto_total": round(r.monto_total or 0, 2),
            "procesos": r.procesos,
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/competidor/rubros")
def competidor_rubros(
    request: Request,
    proveedor: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    rubro: str = Query(""),
    descripcion: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_rubros_v3", proveedor, fecha_desde, fecha_hasta, rubro, descripcion, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    q = (
        select(
            ComparativaRow.rubro,
            _adj.label("monto_total"),
            func.count().label("count_filas"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.rubro.isnot(None))
        .group_by(ComparativaRow.rubro)
        .order_by(_adj.desc())
        .limit(15)
    )
    q = _apply_exact_text(q, ComparativaRow.proveedor, proveedor)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()
    total = sum(r.monto_total or 0 for r in rows)
    data = [
        {
            "rubro": r.rubro or "Sin clasificar",
            "monto_total": round(r.monto_total or 0, 2),
            "pct": round((r.monto_total or 0) / total * 100, 1) if total else 0,
            "count": r.count_filas,
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/competidor/posiciones")
def competidor_posiciones(
    request: Request,
    proveedor: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    rubro: str = Query(""),
    descripcion: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_pos_v5", proveedor, fecha_desde, fecha_hasta, rubro, descripcion, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj_monto = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    _veces_ganado = func.count(case((ComparativaRow.posicion == 1, 1)))
    q = (
        select(
            ComparativaRow.descripcion,
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
            func.min(ComparativaRow.posicion).label("mejor_posicion"),
            func.count().label("count_filas"),
            _adj_monto.label("monto_total"),
            _veces_ganado.label("veces_ganado"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.descripcion.isnot(None))
        .group_by(ComparativaRow.descripcion)
        .order_by(func.avg(ComparativaRow.posicion).asc())
        .limit(30)
    )
    q = _apply_exact_text(q, ComparativaRow.proveedor, proveedor)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()

    # Mediana de precio_unitario por descripción (mismo filtro de proveedor + fechas)
    desc_list = [r.descripcion for r in rows]
    prices_by_desc: dict = {}
    if desc_list and proveedor.strip():
        price_q = (
            select(ComparativaRow.descripcion, ComparativaRow.precio_unitario)
            .where(ComparativaRow.proveedor == proveedor)
            .where(ComparativaRow.precio_unitario.isnot(None))
            .where(ComparativaRow.descripcion.in_(desc_list))
        )
        price_q = _apply_date_filters(price_q, fecha_desde, fecha_hasta)
        price_q = _apply_multi(price_q, ComparativaRow.rubro, rubro)
        price_q = _apply_exact_text(price_q, ComparativaRow.plataforma, plataforma)
        for pr in session.execute(price_q).all():
            prices_by_desc.setdefault(pr.descripcion, []).append(pr.precio_unitario)

    def _median_pos(vals: list) -> float:
        if not vals:
            return 0.0
        s = sorted(vals)
        n = len(s)
        mid = n // 2
        return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0

    data = [
        {
            "descripcion": r.descripcion,
            "posicion_promedio": round(r.posicion_promedio or 0, 1),
            "mejor_posicion": r.mejor_posicion,
            "count": r.count_filas,
            "monto_total": round(r.monto_total or 0, 2),
            "veces_ganado": int(r.veces_ganado or 0),
            "efectividad": round(int(r.veces_ganado or 0) / r.count_filas * 100, 1) if r.count_filas else 0,
            "precio_mediana": round(_median_pos(prices_by_desc.get(r.descripcion, [])), 2),
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/competidor/top-marcas")
def competidor_top_marcas(
    request: Request,
    proveedor: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    """Mantenido por compatibilidad. El frontend usa /competidor/productos-competitivos."""
    ck = _cache_key("comp_marcas", proveedor, fecha_desde, fecha_hasta, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.marca,
            func.count().label("count_filas"),
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.marca.isnot(None))
        .group_by(ComparativaRow.marca)
        .order_by(func.sum(ComparativaRow.total_por_renglon).desc())
        .limit(15)
    )
    q = _apply_exact_text(q, ComparativaRow.proveedor, proveedor)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()
    data = [
        {"marca": r.marca, "count": r.count_filas, "monto_total": round(r.monto_total or 0, 2)}
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/competidor/productos-competitivos")
def competidor_productos_competitivos(
    request: Request,
    proveedor: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    rubro: str = Query(""),
    descripcion: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    """Artículos donde el competidor más veces fue adjudicado (posicion=1)."""
    ck = _cache_key("comp_prod_comp_v2", proveedor, fecha_desde, fecha_hasta, rubro, descripcion, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _veces_adj = func.count(case((ComparativaRow.posicion == 1, 1)))
    _monto_adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    q = (
        select(
            ComparativaRow.descripcion,
            _veces_adj.label("veces_adjudicado"),
            _monto_adj.label("monto_adjudicado"),
            func.count().label("participaciones"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.descripcion.isnot(None))
        .group_by(ComparativaRow.descripcion)
        .order_by(_veces_adj.desc())
        .limit(15)
    )
    q = _apply_exact_text(q, ComparativaRow.proveedor, proveedor)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()
    data = [
        {
            "descripcion": r.descripcion,
            "veces_adjudicado": int(r.veces_adjudicado or 0),
            "monto_adjudicado": round(r.monto_adjudicado or 0, 2),
            "participaciones": int(r.participaciones or 0),
        }
        for r in rows
        if (r.veces_adjudicado or 0) > 0
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/competidor/top-articulos")
def competidor_top_articulos(
    request: Request,
    proveedor: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    rubro: str = Query(""),
    descripcion: str = Query(""),
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_art_v3", proveedor, fecha_desde, fecha_hasta, rubro, descripcion, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    q = (
        select(
            ComparativaRow.descripcion,
            _adj.label("monto_total"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
            func.avg(ComparativaRow.precio_unitario).label("avg_precio"),
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.descripcion.isnot(None))
        .group_by(ComparativaRow.descripcion)
        .order_by(_adj.desc())
        .limit(25)
    )
    q = _apply_exact_text(q, ComparativaRow.proveedor, proveedor)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_exact_text(q, ComparativaRow.descripcion, descripcion)
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)

    rows = session.execute(q).all()
    data = [
        {
            "descripcion": r.descripcion,
            "monto_total": round(r.monto_total or 0, 2),
            "procesos": r.procesos,
            "avg_precio": round(r.avg_precio or 0, 2),
            "posicion_promedio": round(r.posicion_promedio or 0, 1),
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CLIENTE
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/cliente/kpis")
def cliente_kpis(
    request: Request,
    comprador: str = Query(""),
    nro_proceso: str = Query(""),
    plataforma: str = Query(""),
    provincia: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("cli_kpis_v2", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    q = (
        select(
            _adj.label("monto_adjudicado"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
            func.count(distinct(ComparativaRow.proveedor)).label("proveedores"),
            func.count(distinct(ComparativaRow.descripcion)).label("descripciones"),
            func.count(distinct(ComparativaRow.rubro)).label("rubros"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
    )
    q = _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    row = session.execute(q).one_or_none()

    monto_adj = row.monto_adjudicado or 0 if row else 0

    data = {
        "monto_total_cotizado": round(monto_adj, 2),
        "procesos_analizados": row.procesos if row else 0,
        "proveedores_unicos": row.proveedores if row else 0,
        "descripciones_unicas": row.descripciones if row else 0,
        "rubros_distintos": row.rubros if row else 0,
    }
    _cache_set(ck, data)
    return {"ok": True, "data": data}


def _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia):
    q = _apply_exact_text(q, ComparativaRow.comprador, comprador)
    if nro_proceso:
        q = q.where(ComparativaRow.nro_proceso.ilike(f"%{nro_proceso}%"))
    q = _apply_exact_text(q, ComparativaRow.plataforma, plataforma)
    q = _apply_exact_text(q, ComparativaRow.provincia, provincia)
    return q


@router.get("/cliente/evolucion")
def cliente_evolucion(
    request: Request,
    comprador: str = Query(""),
    nro_proceso: str = Query(""),
    plataforma: str = Query(""),
    provincia: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("cli_evol_v2", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _year  = extract("year",  ComparativaRow.fecha_apertura)
    _month = extract("month", ComparativaRow.fecha_apertura)
    _quarter = case(
        (_month.in_([1, 2, 3]), 1),
        (_month.in_([4, 5, 6]), 2),
        (_month.in_([7, 8, 9]), 3),
        else_=4,
    )
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))

    q = (
        select(
            _year.label("year"), _quarter.label("quarter"), _month.label("month"),
            _adj.label("monto_total"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .group_by(_year, _quarter, _month)
        .order_by(_year, _quarter, _month)
    )
    q = _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    rows = session.execute(q).all()
    data = [
        {
            "year": int(r.year),
            "quarter": int(r.quarter),
            "month": int(r.month),
            "period": _period_label(r.year, r.quarter),
            "month_label": f"{_MONTH_NAMES[int(r.month) - 1]} {int(r.year)}",
            "monto_total": round(r.monto_total or 0, 2),
            "procesos": r.procesos,
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/cliente/proveedores")
def cliente_proveedores(
    request: Request,
    comprador: str = Query(""),
    nro_proceso: str = Query(""),
    plataforma: str = Query(""),
    provincia: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("cli_prov_v3", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj_monto = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    _adj_cant = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.cantidad_ofertada), else_=0))
    q = (
        select(
            ComparativaRow.proveedor,
            _adj_monto.label("monto_total"),
            _adj_cant.label("cant_adjudicada"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.proveedor.isnot(None))
        .group_by(ComparativaRow.proveedor)
        .order_by(_adj_monto.desc())
        .limit(20)
    )
    q = _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    rows = session.execute(q).all()
    total_monto = sum(r.monto_total or 0 for r in rows)
    data = [
        {
            "proveedor": r.proveedor,
            "monto_total": round(r.monto_total or 0, 2),
            "cant_adjudicada": round(r.cant_adjudicada or 0, 2),
            "pct": round((r.monto_total or 0) / total_monto * 100, 1) if total_monto else 0,
            "procesos": r.procesos,
            "posicion_promedio": round(r.posicion_promedio or 0, 1),
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/cliente/rubros")
def cliente_rubros(
    request: Request,
    comprador: str = Query(""),
    nro_proceso: str = Query(""),
    plataforma: str = Query(""),
    provincia: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("cli_rubros_v2", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    q = (
        select(
            ComparativaRow.rubro,
            _adj.label("monto_total"),
            func.count().label("count_filas"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.rubro.isnot(None))
        .group_by(ComparativaRow.rubro)
        .order_by(_adj.desc())
        .limit(15)
    )
    q = _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    rows = session.execute(q).all()
    total = sum(r.monto_total or 0 for r in rows)
    data = [
        {
            "rubro": r.rubro or "Sin clasificar",
            "monto_total": round(r.monto_total or 0, 2),
            "pct": round((r.monto_total or 0) / total * 100, 1) if total else 0,
            "count": r.count_filas,
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/cliente/articulos")
def cliente_articulos(
    request: Request,
    comprador: str = Query(""),
    nro_proceso: str = Query(""),
    plataforma: str = Query(""),
    provincia: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("cli_art_v3", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    _adj = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.total_por_renglon), else_=0))
    _adj_cant = func.sum(case((ComparativaRow.posicion == 1, ComparativaRow.cantidad_ofertada), else_=0))
    q = (
        select(
            ComparativaRow.descripcion,
            _adj_cant.label("cant_adjudicada"),
            func.count(distinct(ComparativaRow.upload_id)).label("frecuencia"),
            _adj.label("monto_total"),
            func.avg(ComparativaRow.precio_unitario).label("avg_precio"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.descripcion.isnot(None))
        .group_by(ComparativaRow.descripcion)
        .order_by(_adj.desc(), _adj_cant.desc(), ComparativaRow.descripcion.asc())
    )
    q = _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    rows = session.execute(q).all()
    data = [
        {
            "descripcion": r.descripcion,
            "cant_adjudicada": round(r.cant_adjudicada or 0, 2),
            "frecuencia": r.frecuencia,
            "monto_total": round(r.monto_total or 0, 2),
            "avg_precio": round(r.avg_precio or 0, 2),
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


@router.get("/cliente/articulo-detalle")
def cliente_articulo_detalle(
    request: Request,
    descripcion: str = Query(""),
    comprador: str = Query(""),
    plataforma: str = Query(""),
    provincia: str = Query(""),
    fecha_desde: str = Query(""),
    fecha_hasta: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    """Detalle de registros de un artículo para el desplegable de la tabla Artículos Solicitados."""
    if not descripcion.strip():
        return {"ok": True, "data": []}

    ck = _cache_key("cli_art_det_v2", descripcion, comprador, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.fecha_apertura,
            ComparativaRow.marca,
            ComparativaRow.precio_unitario,
            ComparativaRow.proveedor,
        )
        .where(ComparativaRow.descripcion == descripcion)
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .order_by(ComparativaRow.fecha_apertura.desc())
        .limit(150)
    )
    q = _apply_cliente_filters(q, comprador, "", plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    rows = session.execute(q).all()
    data = [
        {
            "fecha": r.fecha_apertura.isoformat() if r.fecha_apertura else None,
            "marca": r.marca or "-",
            "precio": round(r.precio_unitario or 0, 2),
            "proveedor": r.proveedor or "-",
        }
        for r in rows
    ]
    _cache_set(ck, data)
    return {"ok": True, "data": data}


# ── Constantes ───────────────────────────────────────────────────────────────
_MONTH_NAMES = [
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
]
