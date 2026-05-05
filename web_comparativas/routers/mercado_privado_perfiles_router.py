"""
Router de analytics para Reporte de Perfiles — Mercado Privado.
Consume las tablas DimensionamientoRecord y DimensionamientoFamilyMonthlySummary.
Dos vistas: Artículo y Cliente.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import statistics
import time
from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy import cast, Date, distinct, func, select, Text
from sqlalchemy.orm import Session

from web_comparativas.auth import require_roles
from web_comparativas.dimensionamiento.models import (
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoRecord,
)
from web_comparativas.dimensionamiento.query_service import (
    DimensionamientoFilters,
    _apply_common_filters,
    _month_expr,
    _month_value_to_iso,
    _summary_health_snapshot,
    build_filters,
    get_filter_options,
)
from web_comparativas.models import IS_SQLITE, User, db_session

router = APIRouter(
    prefix="/api/mercado-privado/perfiles",
    tags=["perfiles-privado"],
)

# ── Cache ─────────────────────────────────────────────────────────────────────
_CACHE: dict[str, dict] = {}
_CACHE_MAX = 256
_TTL_FILTERS = 300.0
_TTL_ANALYTICS = 120.0


def _ck(*parts) -> str:
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


# ── Auth ──────────────────────────────────────────────────────────────────────
AllowedUser = Depends(require_roles("admin", "supervisor", "auditor"))


def _get_db(request: Request) -> Session:
    return getattr(request.state, "db", None) or db_session


# ── Helpers para filtros desde payload JSON ───────────────────────────────────
def _str_list(payload: dict | None, key: str) -> list[str] | None:
    if not payload:
        return None
    val = payload.get(key)
    if not val:
        return None
    if isinstance(val, list):
        return [str(v) for v in val if v]
    return [str(val)]


def _str_val(payload: dict | None, key: str) -> str | None:
    if not payload:
        return None
    val = payload.get(key)
    return str(val).strip() if val else None


def _date_val(payload: dict | None, key: str) -> dt.date | None:
    raw = _str_val(payload, key)
    if not raw:
        return None
    try:
        return dt.date.fromisoformat(raw)
    except ValueError:
        return None


def _build_filters_from_payload(payload: dict | None) -> DimensionamientoFilters:
    return build_filters(
        clientes=_str_list(payload, "clientes"),
        familias=_str_list(payload, "familias"),
        plataformas=_str_list(payload, "plataformas"),
        fecha_desde=_date_val(payload, "fecha_desde"),
        fecha_hasta=_date_val(payload, "fecha_hasta"),
    )


def _month_bucket(model):
    if model is DimensionamientoRecord:
        col = model.fecha
        if IS_SQLITE:
            return func.date(col, "start of month")
        return cast(_month_expr(col), Date)
    else:
        col = model.month
        if IS_SQLITE:
            return cast(col, Text)
        return col


def _resolve_model(db: Session):
    """Usa tabla resumen si está disponible, de lo contrario tabla base."""
    try:
        health = _summary_health_snapshot(db)
        if health.get("usable"):
            return DimensionamientoFamilyMonthlySummary
    except Exception:
        pass
    return DimensionamientoRecord


def _val_col(model):
    if model is DimensionamientoRecord:
        return func.coalesce(func.sum(model.valorizacion_estimada), 0)
    return func.coalesce(func.sum(model.total_valorizacion), 0)


def _cnt_col(model):
    if model is DimensionamientoRecord:
        return func.coalesce(func.sum(model.cantidad_demandada), 0)
    return func.coalesce(func.sum(model.total_cantidad), 0)


# ── FILTROS ───────────────────────────────────────────────────────────────────
@router.api_route("/filters", methods=["GET", "POST"])
def privado_perfiles_filters(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("filters", filters)
    hit = _cache_get(key, _TTL_FILTERS)
    if hit is not None:
        return {"ok": True, "data": hit}
    try:
        data = get_filter_options(db, filters)
        result = {
            "familias": data.get("familias", []),
            "clientes": data.get("clientes", []),
            "plataformas": data.get("plataformas", []),
            "date_range": data.get("date_range", {"min": None, "max": None}),
        }
        _cache_set(key, result)
        return {"ok": True, "data": result}
    except Exception:
        return {"ok": False, "data": {"familias": [], "clientes": [], "plataformas": [], "date_range": {"min": None, "max": None}}}


# ── ARTÍCULO: KPIs ────────────────────────────────────────────────────────────
@router.api_route("/articulo/kpis", methods=["GET", "POST"])
def privado_articulo_kpis(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("art_kpis", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        # KPI 1 & 3: suma valorizacion y suma cantidad — desde tabla resumen
        model = _resolve_model(db)
        stmt = _apply_common_filters(
            select(
                func.coalesce(func.sum(
                    model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
                ), 0).label("total_valorizado"),
                func.coalesce(func.sum(
                    model.cantidad_demandada if model is DimensionamientoRecord else model.total_cantidad
                ), 0).label("total_cantidad"),
            ),
            model,
            filters,
        )
        row = db.execute(stmt).one()
        total_valorizado = float(row.total_valorizado or 0)
        total_cantidad = float(row.total_cantidad or 0)

        # KPI 2: mediana precio unitario — requiere datos por registro
        mediana_precio = None
        try:
            precio_stmt = _apply_common_filters(
                select(
                    DimensionamientoRecord.valorizacion_estimada,
                    DimensionamientoRecord.cantidad_demandada,
                ).where(
                    DimensionamientoRecord.cantidad_demandada.isnot(None),
                    DimensionamientoRecord.cantidad_demandada > 0,
                    DimensionamientoRecord.valorizacion_estimada.isnot(None),
                    DimensionamientoRecord.valorizacion_estimada > 0,
                ),
                DimensionamientoRecord,
                filters,
            )
            ratios = [
                row_r.valorizacion_estimada / row_r.cantidad_demandada
                for row_r in db.execute(precio_stmt).all()
            ]
            if ratios:
                mediana_precio = round(statistics.median(ratios), 2)
        except Exception:
            pass

        data = {
            "total_valorizado": total_valorizado,
            "mediana_precio_unitario": mediana_precio,
            "total_cantidad": total_cantidad,
        }
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"total_valorizado": 0, "mediana_precio_unitario": None, "total_cantidad": 0}}


# ── ARTÍCULO: Evolución precio en el tiempo ───────────────────────────────────
@router.api_route("/articulo/precio-evolucion", methods=["GET", "POST"])
def privado_articulo_precio_evolucion(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("art_precio_evol", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        if IS_SQLITE:
            month_expr = func.date(DimensionamientoRecord.fecha, "start of month")
        else:
            month_expr = cast(_month_expr(DimensionamientoRecord.fecha), Date)

        stmt = _apply_common_filters(
            select(
                month_expr.label("month"),
                DimensionamientoRecord.valorizacion_estimada,
                DimensionamientoRecord.cantidad_demandada,
            ).where(
                DimensionamientoRecord.cantidad_demandada.isnot(None),
                DimensionamientoRecord.cantidad_demandada > 0,
                DimensionamientoRecord.valorizacion_estimada.isnot(None),
                DimensionamientoRecord.valorizacion_estimada > 0,
            ),
            DimensionamientoRecord,
            filters,
        )
        rows = db.execute(stmt).all()

        month_ratios: dict[str, list[float]] = {}
        for row in rows:
            m = _month_value_to_iso(row.month)
            ratio = row.valorizacion_estimada / row.cantidad_demandada
            month_ratios.setdefault(m, []).append(ratio)

        months = sorted(month_ratios.keys())
        values = [round(statistics.median(month_ratios[m]), 2) for m in months]

        data = {"months": months, "values": values}
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"months": [], "values": []}}


# ── ARTÍCULO: Evolución monto en el tiempo ────────────────────────────────────
@router.api_route("/articulo/monto-evolucion", methods=["GET", "POST"])
def privado_articulo_monto_evolucion(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("art_monto_evol", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        mb = _month_bucket(model)
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )
        stmt = _apply_common_filters(
            select(
                mb.label("month"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .group_by(mb)
            .order_by(mb),
            model,
            filters,
        )
        rows = db.execute(stmt).all()
        months = [_month_value_to_iso(r.month) for r in rows]
        values = [float(r.total or 0) for r in rows]
        data = {"months": months, "values": values}
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"months": [], "values": []}}


# ── ARTÍCULO: Valorización por plataforma ────────────────────────────────────
@router.api_route("/articulo/plataforma", methods=["GET", "POST"])
def privado_articulo_plataforma(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("art_plataforma", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        plat_col = model.plataforma
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )
        stmt = _apply_common_filters(
            select(
                func.coalesce(plat_col, "Sin plataforma").label("plataforma"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .group_by(plat_col)
            .order_by(func.coalesce(func.sum(val_col), 0).desc()),
            model,
            filters,
        )
        rows = db.execute(stmt).all()
        total_global = sum(float(r.total or 0) for r in rows) or 1
        data = [
            {
                "plataforma": r.plataforma or "Sin plataforma",
                "total_valorizado": float(r.total or 0),
                "porcentaje": round(float(r.total or 0) / total_global * 100, 2),
            }
            for r in rows
        ]
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": []}


# ── ARTÍCULO: Valorización por cliente (treemap) ─────────────────────────────
@router.api_route("/articulo/clientes", methods=["GET", "POST"])
def privado_articulo_clientes(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("art_clientes", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        cli_col = (
            model.cliente_nombre_homologado if model is DimensionamientoRecord
            else model.cliente_visible
        )
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )
        stmt = _apply_common_filters(
            select(
                func.coalesce(cli_col, "Sin cliente").label("cliente"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .where(cli_col.isnot(None), cli_col != "")
            .group_by(cli_col)
            .order_by(func.coalesce(func.sum(val_col), 0).desc())
            .limit(30),
            model,
            filters,
        )
        rows = db.execute(stmt).all()
        data = [
            {"cliente": r.cliente, "total_valorizado": float(r.total or 0)}
            for r in rows
        ]
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": []}


# ── ARTÍCULO: Consumo mensual (tabla) ─────────────────────────────────────────
@router.api_route("/articulo/consumo-mensual", methods=["GET", "POST"])
def privado_articulo_consumo_mensual(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("art_consumo", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        # Paso 1: obtener cantidad_demandada por familia + año + mes (suma por combinación)
        from collections import defaultdict

        if IS_SQLITE:
            year_expr_sql = func.strftime("%Y", DimensionamientoRecord.fecha)
            month_expr_sql = func.strftime("%m", DimensionamientoRecord.fecha)
        else:
            year_expr_sql = cast(func.extract("year", DimensionamientoRecord.fecha), Text)
            month_expr_sql = cast(func.extract("month", DimensionamientoRecord.fecha), Text)

        # Agrupar por familia + año + mes y sumar cantidad_demandada
        stmt = _apply_common_filters(
            select(
                DimensionamientoRecord.familia,
                year_expr_sql.label("anio"),
                month_expr_sql.label("mes"),
                func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0).label("total_mes"),
            ).where(
                DimensionamientoRecord.familia.isnot(None),
            ).group_by(
                DimensionamientoRecord.familia,
                year_expr_sql,
                month_expr_sql,
            ),
            DimensionamientoRecord,
            filters,
        )
        rows = db.execute(stmt).all()

        # Paso 2: para cada familia+mes, acumular totales anuales
        # familia_mes[familia][mes_num] = [total_anio_1, total_anio_2, ...]
        familia_mes: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
        for r in rows:
            familia = r.familia or "Sin familia"
            try:
                mes = int(r.mes)
            except (TypeError, ValueError):
                continue
            familia_mes[familia][mes].append(float(r.total_mes))

        # Paso 3: calcular mediana de totales anuales por cada mes calendario
        result_rows = []
        for familia, mes_dict in sorted(familia_mes.items()):
            meses_data = {}
            total = 0.0
            for m in range(1, 13):
                anio_totals = mes_dict.get(m, [])
                med = round(statistics.median(anio_totals)) if anio_totals else 0
                meses_data[m] = med
                total += med
            result_rows.append({
                "familia": familia,
                "meses": meses_data,
                "total": round(total),
            })

        result_rows.sort(key=lambda x: x["total"], reverse=True)

        data = {"rows": result_rows}
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"rows": []}}


# ── CLIENTE: KPIs ─────────────────────────────────────────────────────────────
@router.api_route("/cliente/kpis", methods=["GET", "POST"])
def privado_cliente_kpis(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("cli_kpis", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )

        # KPI 1: total valorizado + KPI 2: familias distintas
        stmt = _apply_common_filters(
            select(
                func.coalesce(func.sum(val_col), 0).label("total_valorizado"),
                func.count(distinct(model.familia)).label("familias"),
            ),
            model,
            filters,
        )
        row = db.execute(stmt).one()
        total_valorizado = float(row.total_valorizado or 0)
        familias_count = int(row.familias or 0)

        # KPI 3: plataforma dominante
        plat_stmt = _apply_common_filters(
            select(
                func.coalesce(model.plataforma, "SIN DATO").label("plataforma"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .where(model.plataforma.isnot(None))
            .group_by(model.plataforma)
            .order_by(func.coalesce(func.sum(val_col), 0).desc())
            .limit(1),
            model,
            filters,
        )
        plat_row = db.execute(plat_stmt).first()
        plataforma = plat_row.plataforma if plat_row else "SIN DATO"

        # KPI 4: provincia del cliente (la más frecuente)
        prov_col = model.provincia
        prov_stmt = _apply_common_filters(
            select(
                func.coalesce(prov_col, "SIN DATO").label("provincia"),
                func.count().label("cnt"),
            )
            .where(prov_col.isnot(None), prov_col != "")
            .group_by(prov_col)
            .order_by(func.count().desc())
            .limit(1),
            model,
            filters,
        )
        prov_row = db.execute(prov_stmt).first()
        provincia = prov_row.provincia if prov_row else "SIN DATO"
        if not provincia or provincia.strip() in ("", "SIN DATO", "Sin dato"):
            provincia = "SIN DATO"

        data = {
            "total_valorizado": total_valorizado,
            "familias": familias_count,
            "plataforma": plataforma or "SIN DATO",
            "provincia": provincia,
        }
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"total_valorizado": 0, "familias": 0, "plataforma": "SIN DATO", "provincia": "SIN DATO"}}


# ── CLIENTE: Valorizado en el tiempo por negocio ──────────────────────────────
@router.api_route("/cliente/negocio-evolucion", methods=["GET", "POST"])
def privado_cliente_negocio_evolucion(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("cli_negocio_evol", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        mb = _month_bucket(model)
        negocio_col = func.coalesce(model.unidad_negocio, "Sin negocio")
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )

        # Top 5 negocios por valorización total
        top_stmt = _apply_common_filters(
            select(
                negocio_col.label("negocio"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .group_by(model.unidad_negocio)
            .order_by(func.coalesce(func.sum(val_col), 0).desc())
            .limit(5),
            model,
            filters,
        )
        top_negocios = [r.negocio for r in db.execute(top_stmt).all()]

        if not top_negocios:
            return {"ok": True, "data": {"months": [], "datasets": []}}

        series_stmt = _apply_common_filters(
            select(
                mb.label("month"),
                negocio_col.label("negocio"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .where(negocio_col.in_(top_negocios))
            .group_by(mb, model.unidad_negocio)
            .order_by(mb),
            model,
            filters,
        )
        series_rows = db.execute(series_stmt).all()

        month_negocio: dict[str, dict[str, float]] = {}
        for r in series_rows:
            m = _month_value_to_iso(r.month)
            month_negocio.setdefault(m, {})[r.negocio] = float(r.total or 0)

        months = sorted(month_negocio.keys())
        datasets = [
            {
                "label": neg,
                "values": [month_negocio.get(m, {}).get(neg, 0) for m in months],
            }
            for neg in top_negocios
        ]

        data = {"months": months, "datasets": datasets}
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"months": [], "datasets": []}}


# ── CLIENTE: Valorizado total en el tiempo ────────────────────────────────────
@router.api_route("/cliente/total-evolucion", methods=["GET", "POST"])
def privado_cliente_total_evolucion(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("cli_total_evol", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        mb = _month_bucket(model)
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )
        stmt = _apply_common_filters(
            select(
                mb.label("month"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .group_by(mb)
            .order_by(mb),
            model,
            filters,
        )
        rows = db.execute(stmt).all()
        months = [_month_value_to_iso(r.month) for r in rows]
        values = [float(r.total or 0) for r in rows]
        data = {"months": months, "values": values}
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"months": [], "values": []}}


# ── CLIENTE: Ranking de productos por valorización ────────────────────────────
@router.api_route("/cliente/producto-ranking", methods=["GET", "POST"])
def privado_cliente_producto_ranking(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    raw_limit = (payload or {}).get("limit")
    try:
        limit = int(raw_limit) if raw_limit not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        limit = None
    key = _ck("cli_ranking", filters, limit)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )
        stmt = _apply_common_filters(
            select(
                func.coalesce(model.familia, "Sin familia").label("familia"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .where(model.familia.isnot(None))
            .group_by(model.familia)
            .order_by(func.coalesce(func.sum(val_col), 0).desc()),
            model,
            filters,
        )
        if limit is not None:
            stmt = stmt.limit(max(1, limit))
        rows = db.execute(stmt).all()
        data = [
            {"familia": r.familia, "total_valorizado": float(r.total or 0)}
            for r in rows
        ]
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": []}


# ── CLIENTE: Valorización por subnegocio ──────────────────────────────────────
@router.api_route("/cliente/subnegocio", methods=["GET", "POST"])
def privado_cliente_subnegocio(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("cli_subneg", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        model = _resolve_model(db)
        val_col = (
            model.valorizacion_estimada if model is DimensionamientoRecord else model.total_valorizacion
        )
        stmt = _apply_common_filters(
            select(
                func.coalesce(model.subunidad_negocio, "Sin subnegocio").label("subnegocio"),
                func.coalesce(func.sum(val_col), 0).label("total"),
            )
            .group_by(model.subunidad_negocio)
            .order_by(func.coalesce(func.sum(val_col), 0).desc()),
            model,
            filters,
        )
        rows = db.execute(stmt).all()
        data = [
            {"subnegocio": r.subnegocio or "Sin subnegocio", "total_valorizado": float(r.total or 0)}
            for r in rows
        ]
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": []}


# ── CLIENTE: Consumo mensual por artículo/familia ────────────────────────────
@router.api_route("/cliente/consumo-mensual", methods=["GET", "POST"])
def privado_cliente_consumo_mensual(
    request: Request,
    _: User = AllowedUser,
    payload: dict | None = Body(default=None),
    db: Session = Depends(_get_db),
):
    filters = _build_filters_from_payload(payload)
    key = _ck("cli_consumo", filters)
    hit = _cache_get(key, _TTL_ANALYTICS)
    if hit is not None:
        return {"ok": True, "data": hit}

    try:
        # Paso 1: sumar cantidad_demandada por familia + año + mes (un total por año)
        from collections import defaultdict

        if IS_SQLITE:
            year_expr_sql = func.strftime("%Y", DimensionamientoRecord.fecha)
            month_expr_sql = func.strftime("%m", DimensionamientoRecord.fecha)
        else:
            year_expr_sql = cast(func.extract("year", DimensionamientoRecord.fecha), Text)
            month_expr_sql = cast(func.extract("month", DimensionamientoRecord.fecha), Text)

        stmt = _apply_common_filters(
            select(
                DimensionamientoRecord.familia,
                year_expr_sql.label("anio"),
                month_expr_sql.label("mes"),
                func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0).label("total_mes"),
            ).where(
                DimensionamientoRecord.familia.isnot(None),
            ).group_by(
                DimensionamientoRecord.familia,
                year_expr_sql,
                month_expr_sql,
            ),
            DimensionamientoRecord,
            filters,
        )
        rows = db.execute(stmt).all()

        # Paso 2: agrupar totales anuales por familia+mes
        # familia_mes[familia][mes_num] = [total_anio_1, total_anio_2, ...]
        familia_mes: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
        for r in rows:
            familia = r.familia or "Sin familia"
            try:
                mes = int(r.mes)
            except (TypeError, ValueError):
                continue
            familia_mes[familia][mes].append(float(r.total_mes))

        # Paso 3: mediana de los totales anuales para cada mes calendario
        result_rows = []
        for familia, mes_dict in sorted(familia_mes.items()):
            meses_data = {}
            total = 0.0
            for m in range(1, 13):
                anio_totals = mes_dict.get(m, [])
                med = round(statistics.median(anio_totals)) if anio_totals else 0
                meses_data[m] = med
                total += med
            result_rows.append({
                "familia": familia,
                "meses": meses_data,
                "total": round(total),
            })

        result_rows.sort(key=lambda x: x["total"], reverse=True)

        data = {"rows": result_rows}
        _cache_set(key, data)
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": False, "data": {"rows": []}}
