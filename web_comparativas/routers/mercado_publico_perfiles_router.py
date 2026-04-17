"""
Router de analytics para el módulo Reporte de Perfiles de Mercado Público.
Todos los endpoints consumen la tabla comparativa_rows (fuente normalizada).
Las agregaciones son server-side; el frontend recibe datos ya procesados.
"""
from __future__ import annotations

import datetime as dt
import io
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
    vals = [v.strip() for v in values.split(",") if v.strip()]
    if vals:
        q = q.where(column.in_(vals))
    return q

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
    rows = session.execute(
        select(col).where(col.isnot(None)).where(col.ilike(term))
        .distinct().order_by(col).limit(50)
    ).scalars().all()
    return {"ok": True, "data": [r for r in rows if r]}


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — ARTÍCULOS
# ══════════════════════════════════════════════════════════════════════════════

def _articulos_base(session, descripcion, fecha_desde, fecha_hasta, marca, proveedor, rubro, plataforma=""):
    q = select(ComparativaRow).where(ComparativaRow.fecha_apertura.isnot(None))
    if descripcion:
        q = q.where(ComparativaRow.descripcion.ilike(f"%{descripcion}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))
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
        func.sum(ComparativaRow.total_por_renglon).label("total_ofertado"),
        func.min(ComparativaRow.posicion).label("mejor_posicion"),
    )
    .where(ComparativaRow.fecha_apertura.isnot(None)))

    if descripcion:
        base = base.where(ComparativaRow.descripcion.ilike(f"%{descripcion}%"))
    base = _apply_date_filters(base, fecha_desde, fecha_hasta)
    base = _apply_multi(base, ComparativaRow.marca, marca)
    base = _apply_multi(base, ComparativaRow.proveedor, proveedor)
    base = _apply_multi(base, ComparativaRow.rubro, rubro)
    if plataforma:
        base = base.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

    row = session.execute(base).one_or_none()

    # Mediana de precio unitario: calculada en Python
    precios_q = select(ComparativaRow.precio_unitario).where(
        ComparativaRow.precio_unitario.isnot(None),
        ComparativaRow.fecha_apertura.isnot(None),
    )
    if descripcion:
        precios_q = precios_q.where(ComparativaRow.descripcion.ilike(f"%{descripcion}%"))
    precios_q = _apply_date_filters(precios_q, fecha_desde, fecha_hasta)
    precios_q = _apply_multi(precios_q, ComparativaRow.marca, marca)
    precios_q = _apply_multi(precios_q, ComparativaRow.proveedor, proveedor)
    precios_q = _apply_multi(precios_q, ComparativaRow.rubro, rubro)
    if plataforma:
        precios_q = precios_q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))
    prices = [r[0] for r in session.execute(precios_q).all() if r[0] is not None]
    mediana = round(statistics.median(prices), 2) if prices else None

    data = {
        "proveedores_unicos": row.proveedores_unicos if row else 0,
        "marcas_distintas": row.marcas_distintas if row else 0,
        "procesos": row.procesos if row else 0,
        "cantidad_solicitada": round(row.cant_solicitada or 0, 2) if row else 0,
        "cantidad_ofertada": round(row.cant_ofertada or 0, 2) if row else 0,
        "total_ofertado": round(row.total_ofertado or 0, 2) if row else 0,
        "mediana_precio": mediana,
        "mejor_posicion": row.mejor_posicion if row else None,
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
    if descripcion:
        q = q.where(ComparativaRow.descripcion.ilike(f"%{descripcion}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

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
    ck = _cache_key("art_marca", descripcion, fecha_desde, fecha_hasta, proveedor, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.marca,
            func.avg(ComparativaRow.precio_unitario).label("avg_precio"),
            func.sum(ComparativaRow.total_por_renglon).label("total_ofertado"),
            func.count().label("count_filas"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.marca.isnot(None))
        .group_by(ComparativaRow.marca)
        .order_by(func.avg(ComparativaRow.precio_unitario).desc())
        .limit(20)
    )
    if descripcion:
        q = q.where(ComparativaRow.descripcion.ilike(f"%{descripcion}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.proveedor, proveedor)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

    rows = session.execute(q).all()
    data = [
        {
            "marca": r.marca,
            "avg_precio": round(r.avg_precio or 0, 2),
            "total_ofertado": round(r.total_ofertado or 0, 2),
            "count": r.count_filas,
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
    ck = _cache_key("art_prov", descripcion, fecha_desde, fecha_hasta, marca, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.proveedor,
            func.avg(ComparativaRow.precio_unitario).label("avg_precio"),
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
            func.min(ComparativaRow.posicion).label("mejor_posicion"),
            func.sum(ComparativaRow.total_por_renglon).label("total_ofertado"),
            func.count().label("count_filas"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.proveedor.isnot(None))
        .group_by(ComparativaRow.proveedor)
        .order_by(func.sum(ComparativaRow.total_por_renglon).desc())
        .limit(30)
    )
    if descripcion:
        q = q.where(ComparativaRow.descripcion.ilike(f"%{descripcion}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

    rows = session.execute(q).all()
    data = [
        {
            "proveedor": r.proveedor,
            "avg_precio": round(r.avg_precio or 0, 2),
            "posicion_promedio": round(r.posicion_promedio or 0, 1),
            "mejor_posicion": r.mejor_posicion,
            "total_ofertado": round(r.total_ofertado or 0, 2),
            "count": r.count_filas,
            "procesos": r.procesos,
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
    ck = _cache_key("comp_kpis", proveedor, fecha_desde, fecha_hasta, rubro, descripcion, marca, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            func.sum(ComparativaRow.total_por_renglon).label("total_ofertado"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
            func.count(distinct(ComparativaRow.descripcion)).label("descripciones"),
            func.count(distinct(ComparativaRow.rubro)).label("rubros"),
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
            func.min(ComparativaRow.posicion).label("mejor_posicion"),
            func.count(distinct(ComparativaRow.marca)).label("marcas"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
    )
    if proveedor:
        q = q.where(ComparativaRow.proveedor.ilike(f"%{proveedor}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    q = _apply_multi(q, ComparativaRow.marca, marca)
    if descripcion:
        q = q.where(ComparativaRow.descripcion.ilike(f"%{descripcion}%"))
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

    row = session.execute(q).one_or_none()
    data = {
        "total_ofertado": round(row.total_ofertado or 0, 2) if row else 0,
        "procesos": row.procesos if row else 0,
        "descripciones_cotizadas": row.descripciones if row else 0,
        "rubros_cubiertos": row.rubros if row else 0,
        "posicion_promedio": round(row.posicion_promedio or 0, 1) if row else None,
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
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_evol", proveedor, fecha_desde, fecha_hasta, rubro, plataforma)
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
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .group_by(_year, _quarter, _month)
        .order_by(_year, _quarter, _month)
    )
    if proveedor:
        q = q.where(ComparativaRow.proveedor.ilike(f"%{proveedor}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

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
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_rubros", proveedor, fecha_desde, fecha_hasta, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.rubro,
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
            func.count().label("count_filas"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.rubro.isnot(None))
        .group_by(ComparativaRow.rubro)
        .order_by(func.sum(ComparativaRow.total_por_renglon).desc())
        .limit(15)
    )
    if proveedor:
        q = q.where(ComparativaRow.proveedor.ilike(f"%{proveedor}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

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
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_pos", proveedor, fecha_desde, fecha_hasta, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.descripcion,
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
            func.min(ComparativaRow.posicion).label("mejor_posicion"),
            func.count().label("count_filas"),
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.descripcion.isnot(None))
        .group_by(ComparativaRow.descripcion)
        .order_by(func.avg(ComparativaRow.posicion).asc())
        .limit(30)
    )
    if proveedor:
        q = q.where(ComparativaRow.proveedor.ilike(f"%{proveedor}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

    rows = session.execute(q).all()
    data = [
        {
            "descripcion": r.descripcion,
            "posicion_promedio": round(r.posicion_promedio or 0, 1),
            "mejor_posicion": r.mejor_posicion,
            "count": r.count_filas,
            "monto_total": round(r.monto_total or 0, 2),
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
    if proveedor:
        q = q.where(ComparativaRow.proveedor.ilike(f"%{proveedor}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

    rows = session.execute(q).all()
    data = [
        {"marca": r.marca, "count": r.count_filas, "monto_total": round(r.monto_total or 0, 2)}
        for r in rows
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
    plataforma: str = Query(""),
    user: User = Depends(require_roles("admin", "supervisor", "auditor")),
):
    ck = _cache_key("comp_art", proveedor, fecha_desde, fecha_hasta, rubro, plataforma)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.descripcion,
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
            func.avg(ComparativaRow.precio_unitario).label("avg_precio"),
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.descripcion.isnot(None))
        .group_by(ComparativaRow.descripcion)
        .order_by(func.sum(ComparativaRow.total_por_renglon).desc())
        .limit(25)
    )
    if proveedor:
        q = q.where(ComparativaRow.proveedor.ilike(f"%{proveedor}%"))
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)
    q = _apply_multi(q, ComparativaRow.rubro, rubro)
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))

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
    ck = _cache_key("cli_kpis", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
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

    # Ticket promedio por proceso
    procesos_count = row.procesos if row else 0
    monto_total = row.monto_total or 0 if row else 0
    ticket_promedio = round(monto_total / procesos_count, 2) if procesos_count else 0

    data = {
        "monto_total_cotizado": round(monto_total, 2),
        "procesos_analizados": procesos_count,
        "proveedores_unicos": row.proveedores if row else 0,
        "descripciones_unicas": row.descripciones if row else 0,
        "rubros_distintos": row.rubros if row else 0,
        "ticket_promedio": ticket_promedio,
    }
    _cache_set(ck, data)
    return {"ok": True, "data": data}


def _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia):
    if comprador:
        q = q.where(ComparativaRow.comprador.ilike(f"%{comprador}%"))
    if nro_proceso:
        q = q.where(ComparativaRow.nro_proceso.ilike(f"%{nro_proceso}%"))
    if plataforma:
        q = q.where(ComparativaRow.plataforma.ilike(f"%{plataforma}%"))
    if provincia:
        q = q.where(ComparativaRow.provincia.ilike(f"%{provincia}%"))
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
    ck = _cache_key("cli_evol", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
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
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
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
    ck = _cache_key("cli_prov", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.proveedor,
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
            func.count(distinct(ComparativaRow.upload_id)).label("procesos"),
            func.avg(ComparativaRow.posicion).label("posicion_promedio"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.proveedor.isnot(None))
        .group_by(ComparativaRow.proveedor)
        .order_by(func.sum(ComparativaRow.total_por_renglon).desc())
        .limit(20)
    )
    q = _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    rows = session.execute(q).all()
    total = sum(r.monto_total or 0 for r in rows)
    data = [
        {
            "proveedor": r.proveedor,
            "monto_total": round(r.monto_total or 0, 2),
            "pct": round((r.monto_total or 0) / total * 100, 1) if total else 0,
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
    ck = _cache_key("cli_rubros", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.rubro,
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
            func.count().label("count_filas"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.rubro.isnot(None))
        .group_by(ComparativaRow.rubro)
        .order_by(func.sum(ComparativaRow.total_por_renglon).desc())
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
    ck = _cache_key("cli_art", comprador, nro_proceso, plataforma, provincia, fecha_desde, fecha_hasta)
    cached = _cache_get(ck, _TTL_ANALYTICS)
    if cached is not None:
        return {"ok": True, "data": cached}

    session = _get_session(request)
    q = (
        select(
            ComparativaRow.descripcion,
            func.sum(ComparativaRow.cantidad_solicitada).label("cant_total"),
            func.count(distinct(ComparativaRow.upload_id)).label("frecuencia"),
            func.sum(ComparativaRow.total_por_renglon).label("monto_total"),
            func.avg(ComparativaRow.precio_unitario).label("avg_precio"),
        )
        .where(ComparativaRow.fecha_apertura.isnot(None))
        .where(ComparativaRow.descripcion.isnot(None))
        .group_by(ComparativaRow.descripcion)
        .order_by(func.sum(ComparativaRow.cantidad_solicitada).desc())
        .limit(25)
    )
    q = _apply_cliente_filters(q, comprador, nro_proceso, plataforma, provincia)
    q = _apply_date_filters(q, fecha_desde, fecha_hasta)

    rows = session.execute(q).all()
    data = [
        {
            "descripcion": r.descripcion,
            "cant_total": round(r.cant_total or 0, 2),
            "frecuencia": r.frecuencia,
            "monto_total": round(r.monto_total or 0, 2),
            "avg_precio": round(r.avg_precio or 0, 2),
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
