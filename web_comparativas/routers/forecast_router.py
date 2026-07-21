"""
Forecast router — integrates the Forecast module into the SIEM platform.
Routes:
  GET  /forecast/              → renders the dashboard template
  GET  /api/forecast/filter-options
  GET  /api/forecast/product-list
  GET  /api/forecast/chart-data
  GET  /api/forecast/client-table
  POST /api/forecast/reload    → (admin) force re-load CSVs from disk
  POST /forecast/api/comments  → widget: crea/agrega nota en ticket de Forecast
  GET  /forecast/api/comments/summary → widget: badge + historial
"""
from __future__ import annotations

import datetime as dt
import json as _json
import logging
import re
import time
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from pydantic import BaseModel, Field

from web_comparativas.models import User, Ticket, TicketMessage
from web_comparativas import forecast_service as svc
from web_comparativas.policy import (
    require_module,
    can_access as _can_access_tpl,
    can_switch_market as _can_switch_market_tpl,
    puede_ver_aprobaciones_forecast,
    puede_editar_aprobaciones_forecast,
)

logger = logging.getLogger("wc.forecast.router")
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals["can_access"] = _can_access_tpl
templates.env.globals["can_switch_market"] = _can_switch_market_tpl

router = APIRouter(prefix="/forecast", tags=["forecast"])


def _approx_json_bytes(payload) -> int:
    if isinstance(payload, (bytes, bytearray)):
        return len(payload)
    try:
        return len(_json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"))
    except Exception:
        return -1


def _result_rows(payload) -> int:
    if isinstance(payload, (bytes, bytearray)):
        return -1
    if isinstance(payload, list):
        return len(payload)
    if not isinstance(payload, dict):
        return -1
    for key in ("rows", "forecast", "ids", "history", "records"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
    return -1


def _log_api_perf(endpoint: str, started: float, payload) -> None:
    total_ms = (time.perf_counter() - started) * 1000
    rows = _result_rows(payload)
    json_bytes = _approx_json_bytes(payload)
    logger.info(
        "[FORECAST API] endpoint=%s total_ms=%.1f rows=%s json_bytes=%s",
        endpoint,
        total_ms,
        rows,
        json_bytes,
    )
    print(
        f"[FORECAST API] endpoint={endpoint} total_ms={total_ms:.1f} rows={rows} json_bytes={json_bytes}",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Auth helper (same pattern as other routers)
# ---------------------------------------------------------------------------

def _require_user(request: Request) -> User:
    user = getattr(request.state, "user", None)
    if not user:
        # Log session state to diagnose 401s in production
        uid = getattr(request.session, "get", lambda k, d=None: d)("uid") if hasattr(request, "session") else None
        logger.warning(
            "forecast 401 — path=%s uid_in_session=%s has_state_user=%s",
            request.url.path, uid, hasattr(request.state, "user"),
        )
        raise HTTPException(status_code=401, detail="No autenticado")
    return user


def _forecast_role_key(user: User) -> str:
    role = (getattr(user, "role", "") or getattr(user, "rol", "") or "").strip().lower()
    role = role.removeprefix("role_")
    return re.sub(r"[^a-z0-9]+", "_", role).strip("_")


def _can_view_global_forecast_adjustments(user: User) -> bool:
    """Admin, Auditor y Gerente can read consolidated Forecast overrides from all users.
    Gerente/manager iguala el alcance de datos del Auditor (visibilidad total)."""
    role_key = _forecast_role_key(user)
    return role_key in {
        "admin",
        "administrator",
        "administrador",
        "auditor",
        "audit",
        "aud",
        "auditor_siem",
        "gerente",
        "manager",
    }


# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------

@router.get("", response_class=HTMLResponse, include_in_schema=False)
@router.get("/", response_class=HTMLResponse)
def forecast_home(request: Request, user: User = Depends(require_module("forecast"))):
    return templates.TemplateResponse(
        "forecast/index.html",
        {
            "request": request,
            "user": user,
            "market_context": "forecast",
            # Aprobaciones Forecast: visibilidad/edición por rol (autoridad = policy).
            # El template SOLO refleja; el backend reenforza igual en cada endpoint.
            "can_view_approvals": puede_ver_aprobaciones_forecast(user),
            "can_edit_approvals": puede_editar_aprobaciones_forecast(user),
        },
    )


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@router.get("/api/filter-options")
def api_filter_options(request: Request, _user: User = Depends(require_module("forecast"))):
    started = time.perf_counter()
    try:
        result = svc.get_filter_options()
        _log_api_perf("filter-options", started, result)
        if isinstance(result, bytes):
            return Response(content=result, media_type="application/json")
        return result
    except Exception as exc:
        logger.error("filter-options error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/product-list")
def api_product_list(
    request: Request,
    profiles: Optional[List[str]] = Query(default=None, alias="profiles[]"),
    neg: Optional[List[str]] = Query(default=None, alias="neg[]"),
    _user: User = Depends(require_module("forecast")),
):
    started = time.perf_counter()
    try:
        result = svc.get_product_list(profiles=profiles, neg=neg)
        _log_api_perf("product-list", started, result)
        if isinstance(result, bytes):
            return Response(content=result, media_type="application/json")
        return result
    except Exception as exc:
        logger.error("product-list error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/chart-data")
def api_chart_data(
    request: Request,
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    profiles: Optional[List[str]] = Query(default=None, alias="profiles[]"),
    neg: Optional[List[str]] = Query(default=None, alias="neg[]"),
    subneg: Optional[List[str]] = Query(default=None, alias="subneg[]"),
    products: Optional[List[str]] = Query(default=None, alias="products[]"),
    lab_name: Optional[str] = Query(default=None),
    view_money: bool = Query(default=True),
    growth_pct: float = Query(default=0.0),
    _user: User = Depends(require_module("forecast")),
):
    import traceback as _tb
    import json as _json
    started = time.perf_counter()
    logger.debug(
        "chart-data start_date=%s end_date=%s profiles=%s neg=%s view_money=%s growth_pct=%s",
        start_date, end_date, profiles, neg, view_money, growth_pct,
    )
    try:
        resolved_products = svc.get_lab_product_codes(lab_name) if lab_name else products
        can_view_global = _can_view_global_forecast_adjustments(_user)
        logger.info(
            "[FORECAST API] chart-data user_id=%s role=%r role_key=%s global_overrides=%s",
            getattr(_user, "id", None),
            getattr(_user, "role", None),
            _forecast_role_key(_user),
            can_view_global,
        )
        result = svc.get_chart_data(
            user_id=_user.id,
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=resolved_products,
            view_money=view_money,
            growth_pct=growth_pct,
            is_admin=can_view_global,
        )
        # Cache HIT returns pre-serialized bytes — bypass FastAPI encoding entirely.
        if isinstance(result, bytes):
            _log_api_perf("chart-data", started, result)
            return Response(content=result, media_type="application/json")
        logger.debug(
            "chart-data result history=%s forecast=%s has_overrides=%s",
            len(result.get("history", [])) if isinstance(result, dict) else "?",
            len(result.get("forecast", [])) if isinstance(result, dict) else "?",
            result.get("has_overrides") if isinstance(result, dict) else "?",
        )
        # Validate JSON serializability BEFORE FastAPI tries to serialize it.
        # If this fails, we get the traceback here — not silently in the middleware.
        try:
            _json.dumps(result)
        except Exception as _json_exc:
            _tb_str2 = _tb.format_exc()
            logger.error("chart-data JSON serialization failed: %s\n%s", _json_exc, _tb_str2)
            # Sanitize: replace non-serializable scalars in-place and retry
            import math as _math
            def _sanitize(obj):
                if isinstance(obj, dict):
                    return {k: _sanitize(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [_sanitize(v) for v in obj]
                if isinstance(obj, float):
                    if _math.isnan(obj) or _math.isinf(obj):
                        return 0.0
                    return obj
                try:
                    import numpy as _np
                    if isinstance(obj, _np.floating):
                        v = float(obj)
                        return 0.0 if (_math.isnan(v) or _math.isinf(v)) else v
                    if isinstance(obj, _np.integer):
                        return int(obj)
                except ImportError:
                    pass
                return obj
            result = _sanitize(result)
            logger.debug("chart-data sanitized — retrying JSON")
        _log_api_perf("chart-data", started, result)
        return result
    except Exception as exc:
        logger.error("chart-data error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/client-table")
def api_client_table(
    request: Request,
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    profiles: Optional[List[str]] = Query(default=None, alias="profiles[]"),
    neg: Optional[List[str]] = Query(default=None, alias="neg[]"),
    subneg: Optional[List[str]] = Query(default=None, alias="subneg[]"),
    products: Optional[List[str]] = Query(default=None, alias="products[]"),
    view_money: bool = Query(default=True),
    growth_pct: float = Query(default=0.0),
    lab_products: Optional[List[str]] = Query(default=None, alias="lab_products[]"),
    lab_name: Optional[str] = Query(default=None),
    _user: User = Depends(require_module("forecast")),
):
    import traceback as _tb
    started = time.perf_counter()
    logger.debug("client-table start=%s end=%s profiles=%s neg=%s", start_date, end_date, profiles, neg)
    try:
        resolved_products = svc.get_lab_product_codes(lab_name) if lab_name else products
        can_view_global = _can_view_global_forecast_adjustments(_user)
        result = svc.get_client_table(
            user_id=_user.id,
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=resolved_products,
            view_money=view_money,
            growth_pct=growth_pct,
            lab_products=lab_products,
            is_admin=can_view_global,
        )
        _log_api_perf("client-table", started, result)
        if isinstance(result, bytes):
            return Response(content=result, media_type="application/json")
        return result
    except Exception as exc:
        logger.error("client-table error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/treemap-data")
def api_treemap_data(
    request: Request,
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    profiles: Optional[List[str]] = Query(default=None, alias="profiles[]"),
    neg: Optional[List[str]] = Query(default=None, alias="neg[]"),
    subneg: Optional[List[str]] = Query(default=None, alias="subneg[]"),
    products: Optional[List[str]] = Query(default=None, alias="products[]"),
    view_money: bool = Query(default=True),
    period_date: Optional[str] = Query(default=None),
    lab_name: Optional[str] = Query(default=None),
    _user: User = Depends(require_module("forecast")),
):
    started = time.perf_counter()
    logger.debug("treemap-data start=%s end=%s profiles=%s neg=%s period=%s", start_date, end_date, profiles, neg, period_date)
    try:
        resolved_products = svc.get_lab_product_codes(lab_name) if lab_name else products
        can_view_global = _can_view_global_forecast_adjustments(_user)
        result = svc.get_treemap_data(
            user_id=_user.id,
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=resolved_products,
            view_money=view_money,
            period_date=period_date,
            is_admin=can_view_global,
        )
        _log_api_perf("treemap-data", started, result)
        if isinstance(result, bytes):
            return Response(content=result, media_type="application/json")
        return result
    except Exception as exc:
        logger.error("treemap-data error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/client-detail")
def api_client_detail(
    request: Request,
    client_id: str = Query(...),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    profiles: Optional[List[str]] = Query(default=None, alias="profiles[]"),
    neg: Optional[List[str]] = Query(default=None, alias="neg[]"),
    subneg: Optional[List[str]] = Query(default=None, alias="subneg[]"),
    products: Optional[List[str]] = Query(default=None, alias="products[]"),
    growth_pct: float = Query(default=0.0),
    _user: User = Depends(require_module("forecast")),
):
    started = time.perf_counter()
    try:
        can_view_global = _can_view_global_forecast_adjustments(_user)
        result = svc.get_client_detail(
            user_id=_user.id,
            client_id=client_id,
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            growth_pct=growth_pct,
            is_admin=can_view_global,
        )
        _log_api_perf("client-detail", started, result)
        if isinstance(result, bytes):
            return Response(content=result, media_type="application/json")
        return result
    except Exception as exc:
        import traceback as _tb
        logger.error(
            "client-detail error — client_id=%r start=%s end=%s: %s\n%s",
            client_id, start_date, end_date, exc, _tb.format_exc(),
        )
        raise HTTPException(500, f"Error al cargar detalle del cliente: {exc}")


@router.get("/api/debug-schema")
def api_debug_schema(request: Request, _user: User = Depends(require_module("forecast"))):
    """Return actual column names for all forecast tables from information_schema.
    Use this to verify the real PostgreSQL schema matches what the code expects."""
    try:
        return svc.get_forecast_schema_info()
    except Exception as exc:
        logger.error("debug-schema error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/debug-overrides")
def api_debug_overrides(request: Request, _user: User = Depends(require_module("forecast"))):
    """Inspect ForecastUserOverride records for the current user — for debugging only."""
    from web_comparativas.models import SessionLocal, ForecastUserOverride
    from web_comparativas import forecast_service as _svc
    if SessionLocal is None or ForecastUserOverride is None:
        return {"error": "ORM not available", "records": []}
    try:
        with SessionLocal() as session:
            q = (
                session.query(ForecastUserOverride)
                .filter(ForecastUserOverride.source_module == _svc.FORECAST_OVERRIDE_SOURCE)
            )
            if not _can_view_global_forecast_adjustments(_user):
                q = q.filter(ForecastUserOverride.user_id == int(_user.id))
            
            rows = q.order_by(ForecastUserOverride.updated_at.desc()).limit(50).all()
            records = []
            for r in rows:
                records.append({
                    "id": getattr(r, "id", None),
                    "user_id": getattr(r, "user_id", None),
                    "client_selector": getattr(r, "client_selector", None),
                    "override_scope": getattr(r, "override_scope", None),
                    "subneg": getattr(r, "subneg", None),
                    "codigo_serie": getattr(r, "codigo_serie", None),
                    "forecast_month": getattr(r, "forecast_month", None),
                    "override_growth_pct": getattr(r, "override_growth_pct", None),
                    "effective_monthly_pct": getattr(r, "effective_monthly_pct", None),
                    "effective_from_month": getattr(r, "effective_from_month", None),
                    "is_active": getattr(r, "is_active", None),
                    "updated_at": str(getattr(r, "updated_at", None)),
                })
        efm_now = _svc.get_forecast_effective_month()
        return {
            "user_id": _user.id,
            "effective_from_month_now": efm_now,
            "total_records": len(records),
            "records": records,
        }
    except Exception as exc:
        import traceback as _tb
        return {"error": str(exc), "traceback": _tb.format_exc(), "records": []}


@router.post("/api/reload")
def api_reload(request: Request, _user: User = Depends(require_module("forecast"))):
    if (getattr(_user, "role", "") or "").lower() not in ("admin", "auditor"):
        raise HTTPException(403, "Solo admins pueden recargar los datos de Forecast")
    try:
        svc.reload_data()
        return {"ok": True, "msg": "Datos de Forecast recargados"}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@router.get("/api/diag")
def api_diag(_user: User = Depends(require_module("forecast"))):
    """Diagnostic endpoint: reports which CSV file is loaded, row counts, totals and timestamps.
    Available to all authenticated users for debugging purposes."""
    import pandas as pd
    import datetime as _dt
    import sqlite3

    fact_path = svc.FACT_2026_FILE
    data = svc.get_data()
    df_fact = data.get("df_fact_2026", pd.DataFrame())

    per_month: dict = {}
    total_imp = 0.0
    if not df_fact.empty and "imp_hist" in df_fact.columns and "fecha" in df_fact.columns:
        total_imp = float(df_fact["imp_hist"].sum())
        for m, g in df_fact.groupby(df_fact["fecha"].dt.to_period("M")):
            per_month[str(m)] = {"rows": len(g), "total": float(g["imp_hist"].sum())}

    # SQLite snapshot info (read-only, not the live data source)
    sqlite_path = svc.FORECAST_DIR / "forecast_cache.sqlite"
    sqlite_info: dict = {"path": str(sqlite_path), "exists": sqlite_path.exists()}
    if sqlite_path.exists():
        sqlite_info["size_mb"] = round(sqlite_path.stat().st_size / 1_048_576, 2)
        try:
            conn = sqlite3.connect(str(sqlite_path))
            cur = conn.cursor()
            sqlite_info["df_fact_2026_rows"] = cur.execute("SELECT COUNT(*) FROM df_fact_2026").fetchone()[0]
            sqlite_info["df_fact_2026_total"] = cur.execute("SELECT SUM(imp_hist) FROM df_fact_2026").fetchone()[0]
            date_range = cur.execute("SELECT MIN(fecha), MAX(fecha) FROM df_fact_2026").fetchone()
            sqlite_info["df_fact_2026_date_range"] = list(date_range)
            conn.close()
        except Exception as e:
            sqlite_info["error"] = str(e)

    return {
        "data_source": "CSV (SQLite snapshot is not read by the service)",
        "forecast_service_py": str(Path(svc.__file__).resolve()),
        "forecast_dir": str(svc.FORECAST_DIR.resolve()),
        "fact_2026_file": str(fact_path.resolve()),
        "fact_2026_exists": fact_path.exists(),
        "fact_2026_size_mb": round(fact_path.stat().st_size / 1_048_576, 2) if fact_path.exists() else None,
        "rows_loaded": len(df_fact),
        "total_imp_hist": round(total_imp, 2),
        "per_month": per_month,
        "cache_populated": bool(data),
        "sqlite_snapshot": sqlite_info,
        "server_time": _dt.datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Override endpoints (save / clear client projection edits)
# ---------------------------------------------------------------------------

class _Override(BaseModel):
    articulo: str
    subneg: str | None = None
    date: str      # "YYYY-MM"
    pct: float     # percentage adjustment: nuevo = orig * (1 + pct/100)


class _SubnegOverride(BaseModel):
    subneg: str
    growth_pct: float


class _SavePayload(BaseModel):
    client_id: str
    growth_pct: float = 0.0
    overrides: List[_Override] = Field(default_factory=list)
    subneg_overrides: List[_SubnegOverride] = Field(default_factory=list)


@router.post("/api/save-client")
def api_save_client(
    payload: _SavePayload,
    _request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Persist per-product overrides for a client and reflect changes in the whole dashboard."""
    try:
        svc.save_client_overrides(
            user_id=_user.id,
            client_id=payload.client_id,
            growth_pct=payload.growth_pct,
            user_email=_user.email,
            cell_overrides=[
                {
                    "articulo": o.articulo,
                    "subneg": o.subneg,
                    "date": o.date,
                    "pct": o.pct,
                }
                for o in payload.overrides
            ],
            subneg_overrides=[
                {"subneg": o.subneg, "growth_pct": o.growth_pct}
                for o in payload.subneg_overrides
            ],
        )
        return {
            "ok": True,
            "client_id": payload.client_id,
            "saved_cells": len(payload.overrides),
            "saved_subnegs": len(payload.subneg_overrides),
        }
    except Exception as exc:
        logger.error("save-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


class _GroupSavePayload(BaseModel):
    group_name: str
    client_ids: List[str]
    growth_pct: float
    base_growth_pct: float = 0.0


@router.post("/api/save-group")
def api_save_group(
    payload: _GroupSavePayload,
    _request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Save a uniform growth expectation for all clients in a group."""
    try:
        result = svc.save_group_expectations(
            user_id=_user.id,
            group_name=payload.group_name,
            client_ids=payload.client_ids,
            growth_pct=payload.growth_pct,
            base_growth_pct=payload.base_growth_pct,
            user_email=_user.email,
        )
        saved_ok = result.get("saved_clients", 0) > 0 and result.get("saved_overrides", 0) > 0
        return {"ok": saved_ok, "group_name": payload.group_name, **result}
    except Exception as exc:
        logger.error("save-group error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


class _GroupBatchSavePayload(BaseModel):
    groups: List[dict]   # [{group_name: str, client_ids: [str]}]
    growth_pct: float
    base_growth_pct: float = 0.0


@router.post("/api/save-group-batch")
def api_save_group_batch(
    payload: _GroupBatchSavePayload,
    _request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Save a uniform growth expectation across multiple groups at once."""
    try:
        total_saved = 0
        total_overrides = 0
        total_skipped: list[str] = []
        sample: list[dict] = []
        storage = None
        effective_from_month = None
        for grp in payload.groups:
            result = svc.save_group_expectations(
                user_id=_user.id,
                group_name=str(grp.get("group_name", "")),
                client_ids=list(grp.get("client_ids", [])),
                growth_pct=payload.growth_pct,
                base_growth_pct=payload.base_growth_pct,
                user_email=_user.email,
            )
            total_saved   += result.get("saved_clients", 0)
            total_overrides += result.get("saved_overrides", 0)
            total_skipped += result.get("skipped_clients", [])
            storage = storage or result.get("storage")
            effective_from_month = effective_from_month or result.get("effective_from_month")
            sample.extend(result.get("sample", [])[: max(0, 5 - len(sample))])
        batch_ok = total_saved > 0 and total_overrides > 0
        return {
            "ok": batch_ok,
            "saved_clients": total_saved,
            "saved_overrides": total_overrides,
            "skipped_clients": total_skipped,
            "storage": storage,
            "effective_from_month": effective_from_month,
            "sample": sample[:5],
        }
    except Exception as exc:
        logger.error("save-group-batch error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.delete("/api/clear-client/{client_id}")
def api_clear_client(
    client_id: str,
    _request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Remove all saved overrides for a client, restoring the CSV baseline."""
    try:
        svc.clear_client_overrides(user_id=_user.id, client_id=client_id, user_email=_user.email)
        return {"ok": True, "client_id": client_id}
    except Exception as exc:
        logger.error("clear-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


# ---------------------------------------------------------------------------
# Agregar cliente manual (Forecast > Detalle Operativo)
# ---------------------------------------------------------------------------

@router.get("/api/article-search")
def api_article_search(
    request: Request,
    q: str = Query(default=""),
    limit: int = Query(default=30, ge=1, le=200),
    _user: User = Depends(require_module("forecast")),
):
    """Dynamic article search for the new-client modal."""
    try:
        results = svc.search_articles(q=q, limit=limit)
        return results
    except Exception as exc:
        logger.error("article-search error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/new-client-catalog")
def api_new_client_catalog(
    request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Return catalog data for the new-manual-client form."""
    started = time.perf_counter()
    try:
        result = svc.get_new_client_catalog(user_id=_user.id)
        _log_api_perf("new-client-catalog", started, result)
        return result
    except Exception as exc:
        logger.error("new-client-catalog error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


class _ManualEntry(BaseModel):
    perfil: Optional[str] = None
    neg: str = ""
    subneg: str = ""
    codigo_serie: str
    descripcion: str = ""
    unidad_medida: str = "Unid."
    forecast_month: str
    cantidad: float = 0.0
    costo_unitario: float = 0.0
    monto_total: float = 0.0


class _CreateManualClientPayload(BaseModel):
    nombre_cliente: str
    grupo: Optional[str] = None
    entries: List[_ManualEntry] = Field(default_factory=list)


@router.post("/api/create-manual-client")
def api_create_manual_client(
    payload: _CreateManualClientPayload,
    request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Create a new manual forecast client with article-month entries."""
    started = time.perf_counter()
    try:
        nombre = (payload.nombre_cliente or "").strip()
        if not nombre:
            raise HTTPException(400, "nombre_cliente es obligatorio")
        if not payload.entries:
            raise HTTPException(400, "Debe agregar al menos un artículo")

        result = svc.create_manual_client(
            user_id=_user.id,
            created_by=_user.email or str(_user.id),
            nombre_cliente=nombre,
            grupo=payload.grupo,
            entries=[e.dict() for e in payload.entries],
        )
        # Clear this user's cache so the new client appears immediately
        svc.clear_user_cache(_user.id)
        _log_api_perf("create-manual-client", started, result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("create-manual-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


# ---------------------------------------------------------------------------
# Eliminación lógica de clientes/entries manuales (solo Admin)
# ---------------------------------------------------------------------------

def _require_admin(request: Request, _user: User = Depends(require_module("forecast"))) -> User:
    """Only users with admin role may call delete endpoints."""
    if not _user.is_admin():
        raise HTTPException(status_code=403, detail="Solo administradores pueden eliminar clientes manuales")
    return _user


@router.delete("/api/manual-client/{manual_client_id}")
def api_delete_manual_client(
    manual_client_id: int,
    request: Request,
    _user: User = Depends(_require_admin),
):
    """Logical-delete a manual forecast client (admin only)."""
    started = time.perf_counter()
    try:
        result = svc.delete_manual_client(
            user_id=_user.id,
            manual_client_id=manual_client_id,
            deleted_by=_user.email or str(_user.id),
        )
        if not result.get("ok"):
            raise HTTPException(404, result.get("error", "No encontrado"))
        svc.clear_user_cache(_user.id)
        svc.clear_response_cache()
        _log_api_perf("delete-manual-client", started, result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("delete-manual-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.delete("/api/manual-entry/{manual_entry_id}")
def api_delete_manual_entry(
    manual_entry_id: int,
    request: Request,
    _user: User = Depends(_require_admin),
):
    """Logical-delete a single manual forecast entry (admin only)."""
    started = time.perf_counter()
    try:
        result = svc.delete_manual_entry(
            user_id=_user.id,
            manual_entry_id=manual_entry_id,
            deleted_by=_user.email or str(_user.id),
        )
        if not result.get("ok"):
            raise HTTPException(404, result.get("error", "No encontrado"))
        svc.clear_user_cache(_user.id)
        _log_api_perf("delete-manual-entry", started, result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("delete-manual-entry error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


class _AddArticlesPayload(BaseModel):
    entries: List[_ManualEntry] = Field(default_factory=list)


@router.post("/api/manual-client/{manual_client_id}/add-articles")
def api_add_articles_to_manual_client(
    manual_client_id: int,
    payload: _AddArticlesPayload,
    request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Append new article-month entries to an existing manual client."""
    started = time.perf_counter()
    try:
        if not payload.entries:
            raise HTTPException(400, "Debe agregar al menos un artículo")
        result = svc.add_articles_to_manual_client(
            user_id=_user.id,
            manual_client_id=manual_client_id,
            entries=[e.dict() for e in payload.entries],
        )
        svc.clear_user_cache(_user.id)
        _log_api_perf("add-articles-to-manual-client", started, result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("add-articles-to-manual-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


class _AddArticlesByNamePayload(BaseModel):
    client_name: str
    perfil: Optional[str] = None
    entries: List[_ManualEntry] = Field(default_factory=list)


@router.post("/api/add-articles-to-client")
def api_add_articles_by_client_name(
    payload: _AddArticlesByNamePayload,
    request: Request,
    _user: User = Depends(require_module("forecast")),
):
    """Add articles to any client (base or manual) by client name.

    If no manual record exists for this client+user, one is created automatically.
    Existing records are reused, so no duplication occurs.
    """
    started = time.perf_counter()
    try:
        name = (payload.client_name or "").strip()
        if not name:
            raise HTTPException(400, "client_name es obligatorio")
        if not payload.entries:
            raise HTTPException(400, "Debe agregar al menos un artículo")
        result = svc.add_articles_by_client_name(
            user_id=_user.id,
            created_by=_user.email or str(_user.id),
            client_name=name,
            perfil=payload.perfil or "",
            entries=[e.dict() for e in payload.entries],
        )
        svc.clear_user_cache(_user.id)
        _log_api_perf("add-articles-to-client", started, result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("add-articles-to-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


# ---------------------------------------------------------------------------
# Widget de notas — Mesa de Ayuda integrada con Forecast
# Mismo patrón que sic_router.py / pliego_widget.js, adaptado al módulo Forecast.
# ---------------------------------------------------------------------------

class _ForecastCommentSchema(BaseModel):
    message: str
    empresa: Optional[str] = None
    unidad:  Optional[str] = None


@router.post("/api/comments", response_class=JSONResponse)
def forecast_api_comment(
    request: Request,
    payload: _ForecastCommentSchema,
    user: User = Depends(require_module("forecast")),
):
    """
    Crea o reutiliza un ticket de Mesa de Ayuda para el módulo Forecast.

    Regla de agrupación: si el usuario ya tiene un ticket ABIERTO o PENDIENTE
    en Forecast, el mensaje se agrega a ese ticket existente (evita fragmentar
    la conversación en múltiples tickets). Si no existe ninguno activo, crea uno nuevo.
    """
    # Use request.state.db (middleware session) — avoids holding a separate global
    # scoped_session connection that can exhaust the pool under concurrent load.
    db = request.state.db
    if db is None:
        return JSONResponse({"ok": False, "error": "DB no disponible"}, status_code=503)
    try:
        existing = (
            db.query(Ticket)
            .filter(
                Ticket.modulo_origen == "forecast",
                Ticket.user_id == user.id,
                Ticket.status.in_(["abierto", "pendiente"]),
            )
            .order_by(Ticket.updated_at.desc())
            .first()
        )

        contexto = {
            "empresa": payload.empresa or "",
            "unidad":  payload.unidad  or "",
        }

        is_new = False
        if existing:
            ticket = existing
            ticket.updated_at = dt.datetime.utcnow()
        else:
            title_parts = ["[Forecast]"]
            if payload.empresa:
                title_parts.append(payload.empresa)
            if payload.unidad:
                title_parts.append(payload.unidad[:60])
            auto_title = " – ".join(title_parts)[:200]

            ticket = Ticket(
                user_id=user.id,
                title=auto_title,
                category="forecast",
                priority="media",
                status="abierto",
                modulo_origen="forecast",
                pliego_solicitud_id=None,
                contexto_extra=_json.dumps(contexto, ensure_ascii=False),
            )
            db.add(ticket)
            db.flush()
            is_new = True

        msg = TicketMessage(
            ticket_id=ticket.id,
            user_id=user.id,
            message=payload.message,
        )
        db.add(msg)
        db.flush()  # flush so ticket.id is available; middleware commits at end of request

        # Notificar a admins
        try:
            from web_comparativas.notifications_service import notify_admins
            _nombre = user.name or user.email.split("@")[0]
            _accion = "nueva consulta" if is_new else "nuevo comentario"
            notify_admins(
                db,
                title="Comentario en Forecast",
                message=f"{_nombre} dejó un {_accion} en Forecast",
                category="helpdesk",
                link=f"/sic/helpdesk/{ticket.id}",
            )
        except Exception:
            pass

        return JSONResponse({
            "ok": True,
            "ticket_id": ticket.id,
            "is_new": is_new,
            "message_count": len(ticket.messages),
        })
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.error("forecast-comment error: %s", exc, exc_info=True)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


# ---------------------------------------------------------------------------
# Auditoría de ajustes Forecast (solo admin / auditor)
# ---------------------------------------------------------------------------

def _is_sqlite_db() -> bool:
    try:
        from web_comparativas.models import engine as _engine
        return _engine.url.get_backend_name() == "sqlite"
    except Exception:
        return False


def _require_admin_only(user: User) -> None:
    """Guard admin-only de la sección *Auditoría de ajustes* (/api/audit*).

    NOTA: ya NO gobierna "Aprobaciones Forecast" — esa sección pasó a permisos por
    rol (ver _require_aprobaciones_view / _require_aprobaciones_edit). Este guard
    queda como la regla real de la sección de Auditoría de ajustes, vía
    _require_audit_access, que es admin-only y está fuera del alcance de este cambio.
    """
    try:
        is_admin = bool(user) and user.is_admin()
    except Exception:
        is_admin = False
    if not is_admin:
        raise HTTPException(
            status_code=403,
            detail="Solo usuarios Admin pueden acceder a esta sección.",
        )


def _require_audit_access(user: User) -> None:
    # Sección "Auditoría de ajustes Forecast" (/api/audit*): admin-only.
    # Distinta de "Aprobaciones Forecast". Comportamiento sin cambios.
    _require_admin_only(user)


def _require_aprobaciones_view(user: User) -> None:
    """Aprobaciones Forecast — LECTURA: Admin, Gerente y Auditor. Resto → 403.

    Autoridad server-side de los endpoints GET de la sección. La fuente de verdad
    de la matriz es policy.puede_ver_aprobaciones_forecast.
    """
    if not puede_ver_aprobaciones_forecast(user):
        raise HTTPException(
            status_code=403,
            detail="No tiene acceso a Aprobaciones Forecast.",
        )


def _require_aprobaciones_edit(user: User) -> None:
    """Aprobaciones Forecast — EDICIÓN (aprobar/rechazar): SOLO Admin y Gerente.

    Auditor, Analista y Supervisor → 403, aunque le peguen directo al endpoint.
    La fuente de verdad de la matriz es policy.puede_editar_aprobaciones_forecast.
    """
    if not puede_editar_aprobaciones_forecast(user):
        raise HTTPException(
            status_code=403,
            detail="No tiene permiso para aprobar o rechazar modificaciones de Forecast.",
        )


# ── Textos de limitación incluidos en cada fila del export ──────────────────
_LIM_VALORES_ABSOLUTOS = (
    "Valor absoluto no disponible: forecast_user_overrides almacena solo "
    "porcentajes de ajuste. Para impacto monetario cruzar con fact_forecast_valorizado."
)
_LIM_DESC_ARTICULO = (
    "Descripción no disponible: no se almacena en forecast_user_overrides."
)
_LIM_FECHA_REVERSION = (
    "Fecha de reversión exacta no disponible: se muestra última actualización "
    "(updated_at). No existe tabla de historial de cambios."
)
_LIM_MANUAL_FECHA = (
    "forecast_manual_entries no tiene campo updated_at propio; "
    "se usa created_at del cliente manual."
)
_ORIGEN_PROD  = "Producción PostgreSQL"
_ORIGEN_LOCAL = "Local SQLite"

# Cap de registros por fuente para evitar timeouts en exportaciones completas
_MAX_OVERRIDES_EXPORT = 30000
_MAX_MANUALES_EXPORT  = 10000
# Cap del pool para paginación combinada (fetch-all-then-paginate)
_MAX_POOL = 20000


def _parse_date(s: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


# ── OVERRIDES ────────────────────────────────────────────────────────────────

def _query_overrides(session, filters: dict, cap: int):
    """Query forecast_user_overrides with filters. Returns ORM rows (no ordering)."""
    from web_comparativas.models import ForecastUserOverride, User as UserModel

    q = (
        session.query(ForecastUserOverride, UserModel)
        .outerjoin(UserModel, ForecastUserOverride.user_id == UserModel.id)
        .filter(ForecastUserOverride.source_module == svc.FORECAST_OVERRIDE_SOURCE)
    )

    df_from = _parse_date(filters.get("date_from") or "")
    df_to   = _parse_date(filters.get("date_to") or "")
    comercial = (filters.get("comercial") or "").strip()
    perfil    = (filters.get("perfil") or "").strip()
    subneg    = (filters.get("subneg") or "").strip()
    articulo  = (filters.get("articulo") or "").strip()
    f_month   = (filters.get("forecast_month") or "").strip()
    estado    = (filters.get("estado") or "todos").strip().lower()

    if df_from:
        q = q.filter(ForecastUserOverride.updated_at >= df_from)
    if df_to:
        q = q.filter(ForecastUserOverride.updated_at < df_to + dt.timedelta(days=1))
    if comercial:
        q = q.filter(ForecastUserOverride.created_by.ilike(f"%{comercial}%"))
    if perfil:
        q = q.filter(ForecastUserOverride.perfil.ilike(f"%{perfil}%"))
    if subneg:
        q = q.filter(ForecastUserOverride.subneg.ilike(f"%{subneg}%"))
    if articulo:
        q = q.filter(
            ForecastUserOverride.codigo_serie.ilike(f"%{articulo}%")
            | ForecastUserOverride.client_selector.ilike(f"%{articulo}%")
        )
    if f_month:
        q = q.filter(ForecastUserOverride.forecast_month == f_month)
    if estado == "activo":
        q = q.filter(ForecastUserOverride.is_active.is_(True))
    elif estado == "revertido":
        q = q.filter(ForecastUserOverride.is_active.is_(False))

    return q.limit(cap).all()


def _override_to_dict(override, usr, is_sqlite: bool) -> dict:
    user_email = getattr(usr, "email", None) or override.created_by or "—"
    user_name  = getattr(usr, "full_name", None) or getattr(usr, "name", None) or user_email
    user_role  = getattr(usr, "role", None) or "—"
    user_bu    = getattr(usr, "unit_business", None) or "—"

    ts_act = override.updated_at  # datetime obj for sorting
    base_pct = override.base_growth_pct
    ovr_pct  = override.override_growth_pct
    delta_pct: Optional[float] = None
    if base_pct is not None and ovr_pct is not None:
        delta_pct = round(ovr_pct - base_pct, 4)

    estado_label = "Activo" if override.is_active else "Revertido o desactivado"

    return {
        "_fecha_sort": ts_act or dt.datetime.min,
        "tipo_registro": "Ajuste porcentual",
        "id": override.id,
        "fecha_actividad": ts_act.strftime("%Y-%m-%d %H:%M:%S") if ts_act else "—",
        "fecha_creacion": override.created_at.strftime("%Y-%m-%d %H:%M:%S") if override.created_at else "—",
        "usuario_email": user_email,
        "usuario_nombre": user_name,
        "usuario_rol": user_role,
        "unidad_negocio_usuario": user_bu,
        "creado_por": override.created_by or "—",
        "modificado_por": override.updated_by or "—",
        "cliente": override.client_display or override.client_selector or "—",
        "client_selector": override.client_selector or "—",
        "articulo_codigo": override.codigo_serie or "—",
        "descripcion_articulo": "No disponible",
        "perfil": override.perfil or "—",
        "negocio": override.neg or "—",
        "subnegocio": override.subneg or "—",
        "mes_forecast": override.forecast_month or "—",
        "alcance": override.override_scope or "—",
        "pct_base_anual": base_pct,
        "pct_ajuste_anual": ovr_pct,
        "pct_mensual_efectivo": override.effective_monthly_pct,
        "diferencia_pct_anual": delta_pct,
        "valor_ajustado_final": None,
        "mes_vigencia_desde": override.effective_from_month or "Sin restricción",
        "estado": estado_label,
        "origen": _ORIGEN_PROD if not is_sqlite else _ORIGEN_LOCAL,
        "limitacion_dato": (
            f"{_LIM_VALORES_ABSOLUTOS} | {_LIM_DESC_ARTICULO} | {_LIM_FECHA_REVERSION}"
            if not override.is_active
            else f"{_LIM_VALORES_ABSOLUTOS} | {_LIM_DESC_ARTICULO}"
        ),
    }


# ── CLIENTES MANUALES ────────────────────────────────────────────────────────

def _query_manual_entries(session, filters: dict, cap: int):
    """
    Query forecast_manual_entries joined with manual_clients.
    Filtros aplicados:
      - date_from/date_to → created_at del cliente manual
      - comercial        → created_by del cliente manual
      - articulo         → codigo_serie de la entrada O nombre_cliente
      - forecast_month   → forecast_month de la entrada
      - perfil           → perfil de la entrada (si existe en entry)
      - subneg           → subneg de la entrada (si existe en entry)
      - estado=activo    → solo entradas y clientes activos
      - estado=revertido → solo entradas eliminadas (deleted_at IS NOT NULL)
      - estado=todos     → todos (activos + eliminados)
    Filtros que NO aplican a manuales (se documentan, no filtran):
      - negocio: no hay campo neg en manual_entries como campo de filtro separado
        (sí existe como columna pero no se expone en el filtro UI actual)
    """
    from web_comparativas.models import ForecastManualEntry, ForecastManualClient, User as UserModel

    q = (
        session.query(ForecastManualEntry, ForecastManualClient, UserModel)
        .join(ForecastManualClient, ForecastManualEntry.client_id == ForecastManualClient.id)
        .outerjoin(UserModel, ForecastManualClient.user_id == UserModel.id)
    )

    df_from  = _parse_date(filters.get("date_from") or "")
    df_to    = _parse_date(filters.get("date_to") or "")
    comercial = (filters.get("comercial") or "").strip()
    perfil    = (filters.get("perfil") or "").strip()
    subneg    = (filters.get("subneg") or "").strip()
    articulo  = (filters.get("articulo") or "").strip()
    f_month   = (filters.get("forecast_month") or "").strip()
    estado    = (filters.get("estado") or "todos").strip().lower()

    # Estado: mapeo para manuales
    # "activo"    → entry activa + cliente activo
    # "revertido" → entry eliminada (deleted_at IS NOT NULL) o cliente eliminado
    # "todos"     → sin filtro de estado (incluye eliminados y activos)
    if estado == "activo":
        q = q.filter(ForecastManualEntry.is_active.is_(True))
        q = q.filter(ForecastManualClient.is_active.is_(True))
    elif estado == "revertido":
        # Para manuales "revertido" equivale a "eliminado"
        q = q.filter(
            ForecastManualEntry.is_active.is_(False)
            | ForecastManualClient.is_active.is_(False)
        )
    # estado == "todos": no filtrar is_active

    if df_from:
        q = q.filter(ForecastManualClient.created_at >= df_from)
    if df_to:
        q = q.filter(ForecastManualClient.created_at < df_to + dt.timedelta(days=1))
    if comercial:
        q = q.filter(ForecastManualClient.created_by.ilike(f"%{comercial}%"))
    # perfil: aplica a la entrada si existe
    if perfil:
        q = q.filter(ForecastManualEntry.perfil.ilike(f"%{perfil}%"))
    # subneg: aplica a la entrada
    if subneg:
        q = q.filter(ForecastManualEntry.subneg.ilike(f"%{subneg}%"))
    if articulo:
        q = q.filter(
            ForecastManualEntry.codigo_serie.ilike(f"%{articulo}%")
            | ForecastManualClient.nombre_cliente.ilike(f"%{articulo}%")
        )
    if f_month:
        q = q.filter(ForecastManualEntry.forecast_month == f_month)

    return q.limit(cap).all()


def _manual_entry_to_dict(entry, client, usr, is_sqlite: bool) -> dict:
    user_email = getattr(usr, "email", None) or client.created_by or "—"
    user_name  = getattr(usr, "full_name", None) or getattr(usr, "name", None) or user_email
    user_role  = getattr(usr, "role", None) or "—"
    user_bu    = getattr(usr, "unit_business", None) or "—"

    ts_act = client.created_at  # best available timestamp for manual entries
    entry_deleted = getattr(entry, "deleted_at", None)
    client_deleted = getattr(client, "deleted_at", None)
    is_deleted = (entry_deleted is not None) or (not client.is_active)
    estado_label = "Eliminado" if is_deleted else "Activo"

    return {
        "_fecha_sort": ts_act or dt.datetime.min,
        "tipo_registro": "Carga manual",
        "id": entry.id,
        "fecha_actividad": ts_act.strftime("%Y-%m-%d %H:%M:%S") if ts_act else "—",
        "fecha_creacion": ts_act.strftime("%Y-%m-%d %H:%M:%S") if ts_act else "—",
        "usuario_email": user_email,
        "usuario_nombre": user_name,
        "usuario_rol": user_role,
        "unidad_negocio_usuario": user_bu,
        "creado_por": client.created_by or "—",
        "modificado_por": client.created_by or "—",
        "cliente": client.nombre_cliente or "—",
        "client_selector": f"manual:{client.id}",
        "articulo_codigo": entry.codigo_serie or "—",
        "descripcion_articulo": entry.descripcion or "—",
        "perfil": entry.perfil or "—",
        "negocio": entry.neg or "—",
        "subnegocio": entry.subneg or "—",
        "mes_forecast": entry.forecast_month or "—",
        "alcance": "Carga manual",
        "pct_base_anual": None,
        "pct_ajuste_anual": None,
        "pct_mensual_efectivo": None,
        "diferencia_pct_anual": None,
        "valor_ajustado_final": round(entry.monto_total or 0.0, 2),
        "mes_vigencia_desde": "N/A",
        "estado": estado_label,
        "origen": _ORIGEN_PROD if not is_sqlite else _ORIGEN_LOCAL,
        "limitacion_dato": _LIM_MANUAL_FECHA,
    }


# ── HELPERS COMUNES ──────────────────────────────────────────────────────────

_COL_ORDER_EXPORT = [
    "tipo_registro", "id", "fecha_actividad", "fecha_creacion",
    "usuario_email", "usuario_nombre", "usuario_rol", "unidad_negocio_usuario",
    "creado_por", "modificado_por", "cliente", "client_selector",
    "articulo_codigo", "descripcion_articulo", "perfil", "negocio", "subnegocio",
    "mes_forecast", "alcance", "pct_base_anual", "pct_ajuste_anual",
    "pct_mensual_efectivo", "diferencia_pct_anual", "valor_ajustado_final",
    "mes_vigencia_desde", "estado", "limitacion_dato", "origen",
]

_EXPORT_COL_LABELS = {
    "tipo_registro": "Tipo de Registro",
    "id": "ID",
    "fecha_actividad": "Fecha de Actividad",
    "fecha_creacion": "Fecha Creación",
    "usuario_email": "Email Usuario",
    "usuario_nombre": "Nombre Usuario",
    "usuario_rol": "Rol",
    "unidad_negocio_usuario": "Unidad de Negocio",
    "creado_por": "Creado Por",
    "modificado_por": "Modificado Por",
    "cliente": "Cliente",
    "client_selector": "ID Cliente (interno)",
    "articulo_codigo": "Código Artículo",
    "descripcion_articulo": "Descripción Artículo",
    "perfil": "Perfil",
    "negocio": "Negocio",
    "subnegocio": "Subnegocio",
    "mes_forecast": "Mes Forecast",
    "alcance": "Alcance",
    "pct_base_anual": "% Base Anual",
    "pct_ajuste_anual": "% Ajuste Anual",
    "pct_mensual_efectivo": "% Mensual Efectivo",
    "diferencia_pct_anual": "Diferencia % Anual",
    "valor_ajustado_final": "Valor Ajustado (ARS)",
    "mes_vigencia_desde": "Vigente Desde (Mes)",
    "estado": "Estado",
    "limitacion_dato": "Limitación del Dato",
    "origen": "Origen Datos",
}


def _merge_sort_paginate(all_records: list[dict], page: int, page_size: int) -> list[dict]:
    """Sort combined records by _fecha_sort desc, then slice for pagination."""
    all_records.sort(key=lambda r: r.get("_fecha_sort") or dt.datetime.min, reverse=True)
    start = (page - 1) * page_size
    return all_records[start: start + page_size]


def _strip_sort_key(records: list[dict]) -> list[dict]:
    """Remove internal _fecha_sort key before returning to client."""
    for r in records:
        r.pop("_fecha_sort", None)
    return records


def _build_export_rows(records: list[dict]) -> list[dict]:
    return [{k: r.get(k, "—") for k in _COL_ORDER_EXPORT} for r in records]


# ── ENDPOINTS ────────────────────────────────────────────────────────────────

@router.get("/api/audit", response_class=JSONResponse)
def api_audit(
    request: Request,
    user: User = Depends(require_module("forecast")),
    date_from: Optional[str] = Query(None, description="Fecha desde YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Fecha hasta YYYY-MM-DD"),
    comercial: Optional[str] = Query(None, description="Email del comercial (parcial)"),
    perfil: Optional[str] = Query(None, description="Perfil comercial"),
    subneg: Optional[str] = Query(None, description="Subnegocio"),
    articulo: Optional[str] = Query(None, description="Código artículo o nombre cliente"),
    forecast_month: Optional[str] = Query(None, description="Mes forecast YYYY-MM"),
    estado: Optional[str] = Query("todos", description="activo | revertido | todos"),
    incluir_manuales: bool = Query(True, description="Incluir cargas manuales"),
    page: int = Query(1, ge=1),
    page_size: int = Query(200, ge=1, le=2000),
):
    """
    Informe de auditoría Forecast combinado y ordenado cronológicamente.
    Fuentes: forecast_user_overrides + forecast_manual_entries (opcional).
    Paginación real: merge+sort en Python → luego slice.
    Solo Admin/Auditor. Compatible con SQLite y PostgreSQL.
    """
    _require_audit_access(user)
    from web_comparativas.models import SessionLocal, ForecastManualEntry
    if SessionLocal is None:
        raise HTTPException(503, "ORM no disponible")

    is_sqlite = _is_sqlite_db()
    filters = dict(
        date_from=date_from, date_to=date_to, comercial=comercial, perfil=perfil,
        subneg=subneg, articulo=articulo, forecast_month=forecast_month, estado=estado,
    )

    try:
        with SessionLocal() as session:
            ov_rows = _query_overrides(session, filters, cap=_MAX_POOL)
            all_records = [_override_to_dict(ov, usr, is_sqlite) for ov, usr in ov_rows]
            total_ov = len(all_records)

            total_manual = 0
            if incluir_manuales and ForecastManualEntry is not None:
                man_rows = _query_manual_entries(session, filters, cap=_MAX_POOL // 4)
                manual_recs = [_manual_entry_to_dict(e, c, u, is_sqlite) for e, c, u in man_rows]
                total_manual = len(manual_recs)
                all_records.extend(manual_recs)

        total = len(all_records)
        page_records = _merge_sort_paginate(all_records, page, page_size)
        _strip_sort_key(page_records)

        return JSONResponse({
            "ok": True,
            "total": total,
            "total_overrides": total_ov,
            "total_manual_entries": total_manual,
            "page": page,
            "page_size": page_size,
            "pages": max(1, -(-total // page_size)),
            "records": page_records,
            "origen": "postgresql" if not is_sqlite else "sqlite",
            "pool_capped": total >= _MAX_POOL,
        })
    except Exception as exc:
        logger.error("audit error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al consultar auditoría: {exc}")


@router.get("/api/audit/export")
def api_audit_export(
    request: Request,
    user: User = Depends(require_module("forecast")),
    fmt: str = Query("csv", description="csv | xlsx"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    comercial: Optional[str] = Query(None),
    perfil: Optional[str] = Query(None),
    subneg: Optional[str] = Query(None),
    articulo: Optional[str] = Query(None),
    forecast_month: Optional[str] = Query(None),
    estado: Optional[str] = Query("todos"),
    incluir_manuales: bool = Query(True),
):
    """
    Exporta auditoría (mismos filtros que /api/audit) a CSV o Excel on-demand.
    No genera archivos en disco. Solo Admin/Auditor.
    """
    _require_audit_access(user)
    from web_comparativas.models import SessionLocal, ForecastManualEntry
    if SessionLocal is None:
        raise HTTPException(503, "ORM no disponible")

    is_sqlite = _is_sqlite_db()
    filters = dict(
        date_from=date_from, date_to=date_to, comercial=comercial, perfil=perfil,
        subneg=subneg, articulo=articulo, forecast_month=forecast_month, estado=estado,
    )

    try:
        with SessionLocal() as session:
            ov_rows  = _query_overrides(session, filters, cap=_MAX_OVERRIDES_EXPORT)
            all_recs = [_override_to_dict(ov, usr, is_sqlite) for ov, usr in ov_rows]
            if incluir_manuales and ForecastManualEntry is not None:
                man_rows = _query_manual_entries(session, filters, cap=_MAX_MANUALES_EXPORT)
                all_recs.extend(_manual_entry_to_dict(e, c, u, is_sqlite) for e, c, u in man_rows)
    except Exception as exc:
        logger.error("audit export query error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al consultar datos: {exc}")

    # Sort then strip internal key
    all_recs.sort(key=lambda r: r.get("_fecha_sort") or dt.datetime.min, reverse=True)
    _strip_sort_key(all_recs)
    export_rows = _build_export_rows(all_recs)

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename_base = f"forecast_auditoria_{ts}"

    if fmt == "xlsx":
        import io
        import pandas as _pd
        df = _pd.DataFrame(export_rows)
        df.rename(columns=_EXPORT_COL_LABELS, inplace=True)
        buf = io.BytesIO()
        with _pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Auditoría Forecast")
            glosario = _pd.DataFrame([
                {"Campo": "Tipo de Registro",
                 "Descripción": "'Ajuste porcentual' = override de crecimiento %; 'Carga manual' = cliente manual con valores absolutos."},
                {"Campo": "Fecha de Actividad",
                 "Descripción": "Para ajustes: updated_at de forecast_user_overrides. Para manuales: created_at del cliente manual."},
                {"Campo": "Estado",
                 "Descripción": "Activo / Revertido o desactivado (overrides) | Activo / Eliminado (manuales)."},
                {"Campo": "Limitación del Dato",
                 "Descripción": "Describe qué campos no están disponibles y por qué."},
                {"Campo": "Valor Ajustado (ARS)",
                 "Descripción": "Solo disponible para Cargas Manuales (monto_total). Para ajustes porcentuales: requiere cruce con CSV base."},
                {"Campo": "% Ajuste Anual / % Mensual",
                 "Descripción": "Solo disponible para Ajustes Porcentuales. No aplica a Cargas Manuales."},
                {"Campo": "Origen Datos",
                 "Descripción": f"'{_ORIGEN_PROD}' cuando está desplegado en Render. '{_ORIGEN_LOCAL}' en entorno local."},
            ])
            glosario.to_excel(writer, index=False, sheet_name="Glosario")
        buf.seek(0)
        headers = {
            "Content-Disposition": f'attachment; filename="{filename_base}.xlsx"',
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        return Response(content=buf.read(), headers=headers)

    # Default: CSV UTF-8 BOM (compatible con Excel en español)
    import csv, io as _io
    output = _io.StringIO()
    if export_rows:
        labels = [_EXPORT_COL_LABELS.get(k, k) for k in _COL_ORDER_EXPORT]
        w = csv.writer(output)
        w.writerow(labels)
        for r in export_rows:
            w.writerow([r.get(k, "—") for k in _COL_ORDER_EXPORT])
    else:
        output.write("sin_datos\n")
    csv_bytes = output.getvalue().encode("utf-8-sig")
    headers = {
        "Content-Disposition": f'attachment; filename="{filename_base}.csv"',
        "Content-Type": "text/csv; charset=utf-8-sig",
    }
    return Response(content=csv_bytes, headers=headers)


@router.get("/api/audit/filter-options", response_class=JSONResponse)
def api_audit_filter_options(
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Valores distintos para desplegables de filtro. Solo Admin/Auditor."""
    _require_audit_access(user)
    from web_comparativas.models import SessionLocal, ForecastUserOverride, ForecastManualClient
    if SessionLocal is None or ForecastUserOverride is None:
        return JSONResponse({"ok": False, "perfiles": [], "subneg": [], "comerciales": []})

    try:
        with SessionLocal() as session:
            base = (
                session.query(ForecastUserOverride)
                .filter(ForecastUserOverride.source_module == svc.FORECAST_OVERRIDE_SOURCE)
            )
            perfiles = sorted({
                r.perfil for r in base.with_entities(ForecastUserOverride.perfil).distinct()
                if r.perfil
            })
            subnegs = sorted({
                r.subneg for r in base.with_entities(ForecastUserOverride.subneg).distinct()
                if r.subneg
            })
            comerciales = set(
                r.created_by for r in base.with_entities(ForecastUserOverride.created_by).distinct()
                if r.created_by
            )
            if ForecastManualClient is not None:
                comerciales |= {
                    r.created_by
                    for r in session.query(ForecastManualClient)
                    .with_entities(ForecastManualClient.created_by).distinct()
                    if r.created_by
                }
        return JSONResponse({
            "ok": True,
            "perfiles": perfiles,
            "subneg": subnegs,
            "comerciales": sorted(comerciales),
        })
    except Exception as exc:
        logger.error("audit filter-options error: %s", exc, exc_info=True)
        return JSONResponse({"ok": False, "perfiles": [], "subneg": [], "comerciales": []})


# ---------------------------------------------------------------------------
# Aprobaciones Forecast (EXCLUSIVO Admin)
# Revisión de modificaciones realizadas por cotizadores sobre las
# proyecciones comerciales. Registro de control: aprobar/rechazar NO revierte
# ni bloquea el override; solo deja constancia formal de Dirección.
# ---------------------------------------------------------------------------

_CR_TYPE_LABELS = {
    "suba_pct": "Suba de porcentaje",
    "baja_pct": "Baja de porcentaje",
    "ajuste": "Ajuste de proyección",
    "alta_manual": "Alta manual",
}
_CR_STATUS_LABELS = {
    "pendiente": "Pendiente",
    "aprobado": "Aprobado",
    "rechazado": "Rechazado",
}
_HIGH_IMPACT_THRESHOLD = 1_000_000.0   # ARS — umbral "alto impacto"
_MAX_CR_POOL = 20000


def _lookup_grupo(client_name, client_selector, group_map: dict | None) -> str:
    """Grupo del cliente (misma agrupación que 'Proyección más expectativa').
    '' = sin grupo (cliente suelto)."""
    if not group_map:
        return ""
    for key in (client_name, client_selector):
        if key and key != "—":
            g = group_map.get(str(key).strip().lower())
            if g:
                return g
    return ""


def _cr_to_dict(cr, group_map: dict | None = None) -> dict:
    return {
        "_created_sort": cr.created_at or dt.datetime.min,
        "id": cr.id,
        "override_id": getattr(cr, "override_id", None),
        "grupo": _lookup_grupo(cr.client_name or cr.client_selector, cr.client_selector, group_map),
        "created_at": cr.created_at.strftime("%Y-%m-%d %H:%M:%S") if cr.created_at else "—",
        "source": cr.source or "—",
        "change_type": cr.change_type or "ajuste",
        "change_type_label": _CR_TYPE_LABELS.get(cr.change_type, cr.change_type or "—"),
        "usuario": cr.created_by_username or "—",
        "scope_type": cr.scope_type or "—",
        "client_name": cr.client_name or cr.client_selector or "—",
        "client_selector": cr.client_selector or "",
        "perfil": cr.perfil or "—",
        "negocio": cr.neg or "—",
        "subnegocio": cr.subneg or "—",
        "codigo_serie": cr.codigo_serie or "—",
        "descripcion_articulo": cr.descripcion_articulo or "—",
        "periodo": cr.period or "—",
        "campo": cr.field_changed or "—",
        "valor_anterior": cr.old_value,
        "valor_nuevo": cr.new_value,
        "delta_abs": cr.absolute_delta,
        "delta_pct": cr.percentage_delta,
        "impacto_base": cr.estimated_amount_base,
        "impacto_estimado": cr.estimated_amount_delta,
        "status": cr.status or "pendiente",
        "status_label": _CR_STATUS_LABELS.get(cr.status, cr.status or "—"),
        "revisado_por": cr.reviewed_by_username or "—",
        "revisado_el": cr.reviewed_at.strftime("%Y-%m-%d %H:%M:%S") if cr.reviewed_at else "—",
        "motivo": cr.review_comment or "",
    }


def _query_change_requests(session, filters: dict, cap: int):
    """Filtra forecast_change_requests. Compatible con SQLite y PostgreSQL."""
    from web_comparativas.models import ForecastChangeRequest as CR

    q = session.query(CR)

    estado = (filters.get("estado") or "todos").strip().lower()
    if estado in ("pendiente", "aprobado", "rechazado"):
        q = q.filter(CR.status == estado)

    df_from = _parse_date(filters.get("date_from") or "")
    df_to = _parse_date(filters.get("date_to") or "")
    if df_from:
        q = q.filter(CR.created_at >= df_from)
    if df_to:
        q = q.filter(CR.created_at < df_to + dt.timedelta(days=1))

    comercial = (filters.get("comercial") or "").strip()
    if comercial:
        q = q.filter(CR.created_by_username.ilike(f"%{comercial}%"))

    perfil = (filters.get("perfil") or "").strip()
    if perfil:
        q = q.filter(CR.perfil.ilike(f"%{perfil}%"))

    negocio = (filters.get("negocio") or "").strip()
    if negocio:
        q = q.filter(CR.neg.ilike(f"%{negocio}%"))

    subneg = (filters.get("subneg") or "").strip()
    if subneg:
        q = q.filter(CR.subneg.ilike(f"%{subneg}%"))

    articulo = (filters.get("articulo") or "").strip()
    if articulo:
        q = q.filter(
            CR.codigo_serie.ilike(f"%{articulo}%")
            | CR.client_name.ilike(f"%{articulo}%")
        )

    period = (filters.get("period") or "").strip()
    if period:
        q = q.filter(CR.period == period)

    change_type = (filters.get("change_type") or "").strip()
    if change_type:
        q = q.filter(CR.change_type == change_type)

    impacto = (filters.get("impacto") or "").strip().lower()
    if impacto == "positivo":
        q = q.filter(CR.percentage_delta > 0)
    elif impacto == "negativo":
        q = q.filter(CR.percentage_delta < 0)

    if str(filters.get("alto_impacto") or "").strip().lower() in ("1", "true", "si", "sí"):
        q = q.filter(
            CR.estimated_amount_delta.isnot(None),
            (CR.estimated_amount_delta >= _HIGH_IMPACT_THRESHOLD)
            | (CR.estimated_amount_delta <= -_HIGH_IMPACT_THRESHOLD),
        )

    return q.order_by(CR.created_at.desc()).limit(cap).all()


def _dedupe_and_resolve_overlap(records: list[dict]) -> list[dict]:
    """Devuelve los registros 'netos' para los IMPORTES en $ (matriz e impacto
    por estatus), evitando contar dos veces el mismo dinero. Dos pasos:

    1) DEDUP por celda vigente — identidad de negocio:
       (scope_type, cliente, subnegocio, artículo, período). Entre registros con
       la MISMA clave se conserva SOLO el más reciente (created_at; desempata id).
       Así una celda re-guardada N veces cuenta una sola vez, con su valor vigente.
       OJO: el PERÍODO es parte de la clave → meses distintos NO son duplicados
       (son celdas mensuales legítimas y se mantienen ambas).

    2) SOLAPAMIENTO de alcance — jerarquía cliente-ancla:
            celda / producto  ⊂  subnegocio   (mismo cliente + subnegocio)
       Si para un (cliente, subnegocio) existe un cambio vigente de alcance
       'subnegocio', los cambios 'celda'/'producto' contenidos en ese mismo
       (cliente, subnegocio) se EXCLUYEN: la base anual del subnegocio ya incluye
       económicamente a esas celdas. Se conserva el alcance MÁS GENERAL → el mismo
       dinero del forecast nunca se suma dos veces.

    IMPORTANTE (no romper otras métricas): esto afecta SOLO los importes en $.
    Los CONTEOS de la pantalla (chips subas/bajas/ajustes y conteos por estatus)
    se calculan a propósito sobre el set bruto en _compute_approval_kpis, porque
    miden cuántas modificaciones se hicieron, no el dinero neto resultante.
    """
    def _n(v):
        return str(v or "").strip().lower()

    def _key(r):
        return (
            _n(r.get("scope_type")), _n(r.get("client_name")),
            _n(r.get("subnegocio")), _n(r.get("codigo_serie")), _n(r.get("periodo")),
        )

    # Paso 1 — dedup: gana el registro más reciente por clave de celda.
    best: dict[tuple, dict] = {}
    for r in records:
        k = _key(r)
        cur = best.get(k)
        rank = (r.get("_created_sort") or dt.datetime.min, r.get("id") or 0)
        if cur is None or rank > (cur.get("_created_sort") or dt.datetime.min, cur.get("id") or 0):
            best[k] = r
    survivors = list(best.values())

    # Paso 2 — solapamiento: celda/producto contenida en su subnegocio vigente.
    subneg_scopes = {
        (_n(r.get("client_name")), _n(r.get("subnegocio")))
        for r in survivors if _n(r.get("scope_type")) == "subnegocio"
    }
    return [
        r for r in survivors
        if not (
            _n(r.get("scope_type")) in ("celda", "producto")
            and (_n(r.get("client_name")), _n(r.get("subnegocio"))) in subneg_scopes
        )
    ]


def _vig_norm(v) -> str:
    """Normaliza un componente de clave de vigencia. Trata '—'/'none'/'' como vacío
    (porque _cr_to_dict rellena faltantes con '—' y los overrides con ''/None)."""
    s = str(v or "").strip().lower()
    return "" if s in ("—", "none", "nan") else s


def _compute_approval_kpis(records: list[dict], override_impacts: dict | None = None) -> dict:
    """KPIs ejecutivos sobre el conjunto FILTRADO (antes de paginar).

    Dos vistas del mismo conjunto:
      • CONTEOS (cuántas modificaciones) → set bruto ``records`` (chips subas/
        bajas/ajustes y conteos por estatus).
      • IMPORTES en $ (matriz + impacto por estatus + mayor cuenta) → se calculan
        desde los MISMOS ``records`` que alimentan los nodos del árbol
        (``estimated_amount_delta`` = delta REAL del cambio), deduplicados por celda
        vigente, clasificados por ``change_type`` (baja_pct/suba_pct, espejo exacto de
        ``_agg_amounts``) y segmentados por el ``status`` de cada change request (→ el
        monto migra Pendiente→Aprobado/Rechazado al revisar). Esto reemplaza el cálculo
        contra la curva +25% (que medía distancia al 25%, no el cambio real, y no
        migraba bajo el filtro de Estado).

    ``override_impacts``: parámetro RETIRADO de uso (los call-sites ya no lo pasan).
    La rama que lo consumía —impacto de curva vía ``svc.compute_approval_curve_impacts``—
    queda INACCESIBLE (no se borra; ``compute_approval_curve_impacts`` puede usarse en
    otro lado). Si algún día se vuelve a pasar, ejecutaría esa lógica vieja.
    """
    def _amt(r):
        v = r.get("impacto_estimado")
        return float(v) if isinstance(v, (int, float)) else 0.0

    # ── CONTEOS (cuántas modificaciones) → set BRUTO ─────────────────────────
    pendientes = [r for r in records if r.get("status") == "pendiente"]
    aprobados = [r for r in records if r.get("status") == "aprobado"]
    rechazados = [r for r in records if r.get("status") == "rechazado"]
    usuarios = {r.get("usuario") for r in records if r.get("usuario") and r.get("usuario") != "—"}

    def _direction_pct(r):
        d = r.get("delta_pct")
        if isinstance(d, (int, float)) and d != 0:
            return "baja" if d < 0 else "suba"
        v = r.get("impacto_estimado")
        if isinstance(v, (int, float)) and v != 0:
            return "baja" if v < 0 else "suba"
        return None

    subas = sum(1 for r in records if _direction_pct(r) == "suba")
    bajas = sum(1 for r in records if _direction_pct(r) == "baja")
    ajustes = sum(1 for r in records if _direction_pct(r) is None)
    altas = sum(1 for r in records if r.get("change_type") == "alta_manual")

    matrix = {
        st: {"baja": {"monto": 0.0, "n": 0, "sin_estimar": 0},
             "suba": {"monto": 0.0, "n": 0, "sin_estimar": 0}}
        for st in ("pendiente", "aprobado", "rechazado")
    }
    imp_status = {"pendiente": 0.0, "aprobado": 0.0, "rechazado": 0.0}
    por_cuenta: dict[str, float] = {}
    sel_display: dict[str, str] = {}

    if override_impacts is not None:
        # ── IMPORTES en $ = IMPACTO CURVA (Ajustada − +25%) por override activo,
        #    segmentado por el status de su request vigente. NO usa
        #    estimated_amount_delta persistido. ─────────────────────────────────
        def _ck(scope, sel, sub, cod, month):
            if scope == "subnegocio":
                return (scope, sel, sub, "", "")
            if scope == "producto":
                return (scope, sel, "", cod, "")
            if scope == "celda":
                return (scope, sel, sub, cod, month)
            return None

        # Índice de requests por identidad de alcance → [(sort, status, new_value)]
        cr_index: dict[tuple, list] = {}
        for r in records:
            scope = _vig_norm(r.get("scope_type"))
            sub = _vig_norm(r.get("subnegocio"))
            cod = _vig_norm(r.get("codigo_serie"))
            month = _vig_norm(r.get("periodo"))
            nv = r.get("valor_nuevo")
            nvr = round(float(nv), 2) if isinstance(nv, (int, float)) else None
            st = r.get("status") or "pendiente"
            so = r.get("_created_sort") or dt.datetime.min
            disp = r.get("client_name") or r.get("client_selector") or "—"
            for sel in {_vig_norm(r.get("client_selector")), _vig_norm(r.get("client_name"))}:
                if not sel:
                    continue
                sel_display.setdefault(sel, disp)
                k = _ck(scope, sel, sub, cod, month)
                if k:
                    cr_index.setdefault(k, []).append((so, st, nvr))

        sin_request = 0
        sin_request_monto = 0.0
        considerados = 0
        for key, info in override_impacts.items():
            impact = float(info.get("impact") or 0.0)
            if impact == 0.0:
                continue
            ogp = round(float(info.get("ogp") or 0.0), 2)
            sel = key[1]
            cands = [c for c in cr_index.get(key, []) if c[2] is not None and abs(c[2] - ogp) < 0.01]
            if cands:
                # Hay request vigente coincidente → usa su status (el más reciente).
                cands.sort(key=lambda c: c[0])
                status = cands[-1][1]
                if status not in matrix:
                    status = "pendiente"
            else:
                # Override activo SIN request coincidente → es un ajuste vigente
                # que afecta la curva, así que cuenta como PENDIENTE (pendiente de
                # aprobación), NO se excluye. Se reporta en diagnóstico.
                sin_request += 1
                sin_request_monto += impact
                status = "pendiente"
            direction = "baja" if impact < 0 else "suba"
            cell = matrix[status][direction]
            cell["monto"] += impact
            cell["n"] += 1
            imp_status[status] += impact
            por_cuenta[sel] = por_cuenta.get(sel, 0.0) + abs(impact)
            sel_display.setdefault(sel, info.get("selector") or sel)
            considerados += 1

        for st in matrix:
            for d in ("baja", "suba"):
                matrix[st][d]["monto"] = round(matrix[st][d]["monto"], 2)
        # LOG TEMPORAL (INFO) para validar el primer deploy en producción.
        # TODO: bajar a logger.debug o quitar tras validar (no dejar en INFO).
        _compute_net = round(sum(float(i.get("impact") or 0.0) for i in override_impacts.values()), 2)
        logger.info(
            "approvals impacto curva [TEMP]: compute_net=%.2f considerados=%d | "
            "override_sin_request=%d/%.2f | pendiente baja=%.2f suba=%.2f",
            _compute_net, considerados, sin_request, round(sin_request_monto, 2),
            matrix["pendiente"]["baja"]["monto"], matrix["pendiente"]["suba"]["monto"],
        )
        sin_estimar_total = 0
    else:
        # ── MATRIZ desde los MISMOS records que alimentan los nodos del árbol ──
        # (impacto_estimado = estimated_amount_delta = delta REAL del cambio).
        # Consistencia EXACTA con _agg_amounts de los nodos:
        #   • dedup por celda vigente (_dedupe_and_resolve_overlap) → mismos survivors;
        #   • clasificación por change_type ESTRICTO: baja_pct→baja, suba_pct→suba;
        #     ajuste/alta_manual NO entran a las columnas baja/suba (igual que los
        #     nodos, que solo suman esos dos tipos en sus columnas);
        #   • segmentación por status del CR → el monto MIGRA Pendiente→Aprobado/
        #     Rechazado al revisar (el status sale del propio change request).
        # N/D honesto: si no hay base valorizada (impacto None) se cuenta como
        # sin_estimar de la celda y NO se inventa monto.
        neto = _dedupe_and_resolve_overlap(records)

        def _impact_value(r):
            v = r.get("impacto_estimado")
            return float(v) if isinstance(v, (int, float)) else None

        def _column(r):
            ct = r.get("change_type")
            if ct == "baja_pct":
                return "baja"
            if ct == "suba_pct":
                return "suba"
            return None  # ajuste / alta_manual → fuera de las columnas baja/suba

        for r in neto:
            direction = _column(r)
            if direction is None:
                continue
            st = r.get("status") or "pendiente"
            if st not in matrix:
                st = "pendiente"
            v = _impact_value(r)
            if v is None:
                matrix[st][direction]["sin_estimar"] += 1   # N/D honesto, sin monto
                continue
            if v == 0:
                continue
            matrix[st][direction]["monto"] += v
            matrix[st][direction]["n"] += 1
            imp_status[st] += v
            cta = _vig_norm(r.get("client_selector")) or _vig_norm(r.get("client_name"))
            por_cuenta[cta] = por_cuenta.get(cta, 0.0) + abs(v)
            sel_display.setdefault(cta, r.get("client_name") or "—")
        for st in matrix:
            for d in ("baja", "suba"):
                matrix[st][d]["monto"] = round(matrix[st][d]["monto"], 2)
        sin_estimar_total = sum(
            matrix[st][d]["sin_estimar"] for st in matrix for d in ("baja", "suba")
        )

    mayor_cuenta, mayor_cuenta_monto = ("—", 0.0)
    if por_cuenta:
        _sel, mayor_cuenta_monto = max(por_cuenta.items(), key=lambda kv: kv[1])
        mayor_cuenta = sel_display.get(_sel, _sel) or "—"

    return {
        "pendientes": len(pendientes),
        "aprobados": len(aprobados),
        "rechazados": len(rechazados),
        "impacto_pendiente": round(imp_status["pendiente"], 2),
        "impacto_aprobado": round(imp_status["aprobado"], 2),
        "impacto_rechazado": round(imp_status["rechazado"], 2),
        "usuarios": len(usuarios),
        "mayor_cuenta": mayor_cuenta,
        "mayor_cuenta_monto": round(mayor_cuenta_monto, 2),
        "subas": subas,
        "bajas": bajas,
        "ajustes": ajustes,
        "altas_manuales": altas,
        "matrix": matrix,
        "sin_estimar_total": sin_estimar_total,
        "total": len(records),
    }


@router.get("/api/approvals", response_class=JSONResponse)
def api_approvals(
    request: Request,
    user: User = Depends(require_module("forecast")),
    estado: Optional[str] = Query("todos"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    comercial: Optional[str] = Query(None),
    perfil: Optional[str] = Query(None),
    negocio: Optional[str] = Query(None),
    subneg: Optional[str] = Query(None),
    articulo: Optional[str] = Query(None),
    period: Optional[str] = Query(None),
    change_type: Optional[str] = Query(None),
    impacto: Optional[str] = Query(None),
    alto_impacto: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """Lista de modificaciones para revisión + KPIs ejecutivos.
    Lectura: Admin, Gerente y Auditor."""
    _require_aprobaciones_view(user)
    from web_comparativas.models import SessionLocal
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")

    filters = dict(
        estado=estado, date_from=date_from, date_to=date_to, comercial=comercial,
        perfil=perfil, negocio=negocio, subneg=subneg, articulo=articulo, period=period,
        change_type=change_type, impacto=impacto, alto_impacto=alto_impacto,
    )
    try:
        with SessionLocal() as session:
            rows = _query_change_requests(session, filters, cap=_MAX_CR_POOL)
            records = [_cr_to_dict(r) for r in rows]

        # Matriz/impactos desde los propios change requests (delta real, por status).
        # Ya NO usa la curva +25% (ver _compute_approval_kpis).
        kpis = _compute_approval_kpis(records)
        total = len(records)
        start = (page - 1) * page_size
        page_records = records[start: start + page_size]
        for r in page_records:
            r.pop("_created_sort", None)

        return JSONResponse({
            "ok": True,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max(1, -(-total // page_size)),
            "kpis": kpis,
            "high_impact_threshold": _HIGH_IMPACT_THRESHOLD,
            "records": page_records,
            "pool_capped": total >= _MAX_CR_POOL,
        })
    except Exception as exc:
        logger.error("approvals error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al consultar modificaciones: {exc}")


@router.get("/api/approvals/filter-options", response_class=JSONResponse)
def api_approvals_filter_options(
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Valores distintos para desplegables de filtro. Solo Admin.

    Perfil, Negocio y Subnegocio se alimentan de la MISMA fuente que el filtro
    general superior de Forecast (svc.get_filter_options() → dataset valorizado),
    de modo que el listado coincida exactamente. Usuario (cotizador) y Período
    son específicos de las modificaciones, por lo que se derivan de la propia
    tabla forecast_change_requests. Lectura: Admin, Gerente y Auditor."""
    _require_aprobaciones_view(user)
    from web_comparativas.models import SessionLocal, ForecastChangeRequest as CR
    if SessionLocal is None or CR is None:
        return JSONResponse({"ok": False, "perfiles": [], "negocios": [], "subneg": [], "comerciales": [], "periodos": []})
    try:
        # Perfil / Negocio / Subnegocio: misma fuente que el filtro general.
        perfiles, negocios, subnegs = [], [], []
        try:
            opts = svc.get_filter_options()
            if isinstance(opts, bytes):
                import json as _json
                opts = _json.loads(opts.decode("utf-8"))
            if isinstance(opts, dict):
                perfiles = list(opts.get("profiles") or [])
                negocios = list(opts.get("neg") or [])
                subnegs = list(opts.get("subneg") or [])
        except Exception as fo_exc:
            logger.warning("approvals filter-options: general source failed (%s)", fo_exc)

        # Usuario (cotizador) y Período: propios de las modificaciones.
        with SessionLocal() as session:
            comerciales = sorted({
                r.created_by_username
                for r in session.query(CR.created_by_username).distinct()
                if r.created_by_username
            })
            periodos = sorted({r.period for r in session.query(CR.period).distinct() if r.period})

            # Fallback: si la fuente general no devolvió perfiles/negocios/subneg
            # (p. ej. dataset no disponible), usar los valores presentes en las
            # modificaciones para no dejar el filtro vacío.
            if not perfiles:
                perfiles = sorted({r.perfil for r in session.query(CR.perfil).distinct() if r.perfil})
            if not negocios:
                negocios = sorted({r.neg for r in session.query(CR.neg).distinct() if r.neg})
            if not subnegs:
                subnegs = sorted({r.subneg for r in session.query(CR.subneg).distinct() if r.subneg})

        return JSONResponse({
            "ok": True,
            "perfiles": perfiles,
            "negocios": negocios,
            "subneg": subnegs,
            "comerciales": comerciales,
            "periodos": periodos,
        })
    except Exception as exc:
        logger.error("approvals filter-options error: %s", exc, exc_info=True)
        return JSONResponse({"ok": False, "perfiles": [], "negocios": [], "subneg": [], "comerciales": [], "periodos": []})


class _ReviewPayload(BaseModel):
    motivo: Optional[str] = Field(default=None, max_length=2000)


def _apply_review(session, cr, *, status: str, user: User, motivo: Optional[str]) -> Optional[int]:
    """Aplica la revisión a un change_request. Devuelve el user_id DUEÑO del override
    revertido (para invalidar su caché tras el commit), o None.

    APROBAR  → solo cambia status (el override ya está aplicado, ahora avalado).
    RECHAZAR → además:
      (a) desactiva el override vigente vinculado (cr.override_id) → el alcance vuelve
          a la BASE del modelo. Al grano del override (un subneg revierte sus celdas;
          un override de celda más fino, separado, sobrevive).
      (b) marca TODAS las CR pendientes HERMANAS del mismo override como rechazadas
          con el mismo motivo (sin pendientes fantasma).
    Idempotente: si el override ya está inactivo, o las hermanas ya no están
    pendientes, no rompe ni re-procesa (los loops grupo/by-ids saltan las ya
    decididas por su guard `status != 'pendiente'`)."""
    reviewer_id = getattr(user, "id", None)
    reviewer_email = getattr(user, "email", None) or getattr(user, "display_name", None)
    now = dt.datetime.utcnow()
    comment = (motivo.strip() or None) if motivo is not None else None

    def _stamp(c) -> None:
        c.status = status
        c.reviewed_by_user_id = reviewer_id
        c.reviewed_by_username = reviewer_email
        c.reviewed_at = now
        if motivo is not None:
            c.review_comment = comment

    if status != "rechazado":
        _stamp(cr)
        return None

    # RECHAZAR → revertir override vigente + cascada a hermanas pendientes.
    owner_uid = svc.deactivate_override_by_id(
        session, getattr(cr, "override_id", None), reviewer_email=reviewer_email
    )
    if getattr(cr, "override_id", None) is not None:
        from web_comparativas.models import ForecastChangeRequest as _CR
        siblings = (
            session.query(_CR)
            .filter(_CR.override_id == cr.override_id)
            .filter(_CR.status == "pendiente")
            .all()
        )
        for sib in siblings:
            _stamp(sib)
    _stamp(cr)  # asegura el cr actual (cubre override_id NULL y cualquier borde)
    return owner_uid


@router.post("/api/approvals/{request_id:int}/approve", response_class=JSONResponse)
def api_approvals_approve(
    request_id: int,
    payload: _ReviewPayload,
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Aprueba una modificación pendiente. Solo Admin/Gerente. No revierte el override."""
    _require_aprobaciones_edit(user)
    from web_comparativas.models import SessionLocal, ForecastChangeRequest as CR
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")
    try:
        with SessionLocal() as session:
            cr = session.get(CR, request_id)
            if cr is None:
                raise HTTPException(404, "Modificación no encontrada")
            _apply_review(session, cr, status="aprobado", user=user, motivo=payload.motivo)
            session.commit()
        return JSONResponse({"ok": True, "id": request_id, "status": "aprobado"})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("approve error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al aprobar: {exc}")


@router.post("/api/approvals/{request_id:int}/reject", response_class=JSONResponse)
def api_approvals_reject(
    request_id: int,
    payload: _ReviewPayload,
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Rechaza una modificación con motivo. Solo Admin/Gerente. Revierte el override
    vigente del alcance (vuelve a la base) y cascada a sus CR pendientes hermanas."""
    _require_aprobaciones_edit(user)
    motivo = (payload.motivo or "").strip()
    if not motivo:
        raise HTTPException(400, "Debe indicar un motivo para rechazar la modificación.")
    from web_comparativas.models import SessionLocal, ForecastChangeRequest as CR
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")
    try:
        owner_uid = None
        with SessionLocal() as session:
            cr = session.get(CR, request_id)
            if cr is None:
                raise HTTPException(404, "Modificación no encontrada")
            owner_uid = _apply_review(session, cr, status="rechazado", user=user, motivo=motivo)
            session.commit()
        # Caché del DUEÑO del override (cotizador), no del admin que rechaza.
        if owner_uid is not None:
            svc._clear_cache_for_override_save(owner_uid)
        return JSONResponse({"ok": True, "id": request_id, "status": "rechazado"})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("reject error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al rechazar: {exc}")


# ── Vista AGRUPADA por grupo de clientes (misma agrupación que Forecast) ──────

def _amt_of(r: dict) -> float:
    v = r.get("impacto_estimado")
    return float(v) if isinstance(v, (int, float)) else 0.0


def _build_group_unit(grupo: str, recs: list[dict]) -> dict:
    """Fila padre de grupo con agregados sobre sus registros (ya filtrados)."""
    clientes = len({r.get("client_name") for r in recs if r.get("client_name")})
    baja = round(sum(_amt_of(r) for r in recs if r.get("change_type") == "baja_pct"), 2)
    suba = round(sum(_amt_of(r) for r in recs if r.get("change_type") == "suba_pct"), 2)
    neto = round(sum(_amt_of(r) for r in recs), 2)

    estados = {"pendiente": 0, "aprobado": 0, "rechazado": 0}
    for r in recs:
        st = r.get("status") or "pendiente"
        estados[st] = estados.get(st, 0) + 1
    presentes = [k for k, v in estados.items() if v > 0]
    consolidado = presentes[0] if len(presentes) == 1 else "mixto"

    periodos = sorted({r.get("periodo") for r in recs if r.get("periodo") and r.get("periodo") != "—"})
    for r in recs:
        r.pop("_created_sort", None)

    return {
        "type": "group",
        "grupo": grupo,
        "clientes": clientes,
        "modificaciones": len(recs),
        "impacto_baja": baja,
        "impacto_suba": suba,
        "impacto_neto": neto,
        "estados": estados,
        "estado_consolidado": consolidado,
        "pendientes": estados["pendiente"],
        "periodo_desde": periodos[0] if periodos else None,
        "periodo_hasta": periodos[-1] if periodos else None,
        "records": recs,
    }


# ── Agrupación por dimensión alternativa (perfil / negocio / subnegocio) ──────
_VALID_DIMENSIONS = ("grupo", "perfil", "neg", "subneg")
# Dimensiones aceptadas SOLO por la revisión en bloque (_review_group). Incluye
# "cuenta" (usada por el árbol Perfil→Grupo→Cuenta para aprobar/rechazar una
# cuenta entera). NO se agrega a _VALID_DIMENSIONS para no habilitarla en el
# endpoint /grouped, que no sabe agrupar por cuenta.
_REVIEW_DIMENSIONS = _VALID_DIMENSIONS + ("cuenta",)
# dimension → clave en el dict de _cr_to_dict
_DIM_DICT_KEY = {"perfil": "perfil", "neg": "negocio", "subneg": "subnegocio"}
# dimension → columna del modelo ForecastChangeRequest (para la mutación)
_DIM_MODEL_COL = {"perfil": "perfil", "neg": "neg", "subneg": "subneg"}
# dimension → label del bucket NULL/vacío
_DIM_SIN_LABEL = {"perfil": "Sin perfil", "neg": "Sin negocio", "subneg": "Sin subnegocio"}


def _build_dimension_units(records: list[dict], dimension: str) -> list[dict]:
    """Agrupa por una dimensión del registro (perfil/neg/subneg).

    Clave de bucket = LOWER(TRIM(valor)); el label/value canónico es el primer
    valor original no vacío visto. NULL/vacío/'—' caen en un ÚNICO bucket "Sin X"
    (value canónico ""). Reutiliza _build_group_unit para los agregados (impacto
    baja/suba/neto, estado consolidado, conteos). Orden: por |impacto neto| desc;
    el bucket "Sin X" SIEMPRE al final.
    """
    dkey = _DIM_DICT_KEY[dimension]
    sin_label = _DIM_SIN_LABEL[dimension]
    buckets: dict[str, dict] = {}  # norm -> {"label", "value", "recs"}
    for r in records:
        raw = str(r.get(dkey) or "").strip()
        if raw in ("", "—"):
            norm, label, value = "", sin_label, ""
        else:
            norm, label, value = raw.lower(), raw, raw
        b = buckets.get(norm)
        if b is None:
            b = buckets[norm] = {"label": label, "value": value, "recs": []}
        b["recs"].append(r)

    units: list[dict] = []
    for norm, b in buckets.items():
        unit = _build_group_unit(b["label"], b["recs"])
        unit["value"] = b["value"]       # valor canónico para la mutación server-side
        unit["is_sin"] = (norm == "")
        units.append(unit)

    # |impacto neto| desc; "Sin X" al final (is_sin True ordena después).
    units.sort(key=lambda u: (u["is_sin"], -abs(u.get("impacto_neto") or 0.0)))
    return units


# ── Árbol jerárquico Perfil → Grupo → Cuenta (vista consolidada) ──────────────
# Reutiliza _dedupe_and_resolve_overlap (estado vigente por celda) y la misma
# semántica de subtotales que _build_group_unit, pero anidando 3 niveles y
# mostrando UNA línea por cuenta (no una por evento). Los eventos crudos se
# conservan en cada cuenta (records) para el detalle de nivel 4.

def _agg_amounts(recs: list[dict]) -> tuple[float, float, float]:
    baja = round(sum(_amt_of(r) for r in recs if r.get("change_type") == "baja_pct"), 2)
    suba = round(sum(_amt_of(r) for r in recs if r.get("change_type") == "suba_pct"), 2)
    neto = round(sum(_amt_of(r) for r in recs), 2)
    return baja, suba, neto


def _estados_consolidado(estados: dict) -> str:
    presentes = [k for k, v in estados.items() if v > 0]
    if len(presentes) == 1:
        return presentes[0]
    return "mixto" if presentes else "pendiente"


def _build_account_unit(cuenta: str, recs: list[dict]) -> dict:
    """Línea consolidada de una cuenta (nivel 3).

    El estado VIGENTE de cada celda = el último change request de esa celda
    (vía _dedupe_and_resolve_overlap, que ya resuelve recencia + solapamiento
    subnegocio⊃celda). Los agregados (celdas vigentes, impacto neto, estado
    consolidado) se calculan sobre ese conjunto neto. `records` conserva TODOS
    los eventos crudos para el detalle de nivel 4 (no se pierde nada).
    """
    # IMPORTANTE: deduplicar ANTES de tocar _created_sort (lo usa el dedupe).
    survivors = _dedupe_and_resolve_overlap(recs)
    baja, suba, neto = _agg_amounts(survivors)
    estados = {"pendiente": 0, "aprobado": 0, "rechazado": 0}
    for r in survivors:
        st = r.get("status") or "pendiente"
        estados[st] = estados.get(st, 0) + 1
    consolidado = _estados_consolidado(estados)

    selector = ""
    for r in recs:
        if r.get("client_selector"):
            selector = r.get("client_selector")
            break

    periodos = sorted({r.get("periodo") for r in survivors if r.get("periodo") and r.get("periodo") != "—"})

    # Eventos crudos: ordenados por recencia, luego sin _created_sort.
    # (La cuenta es la HOJA del árbol; estos eventos ya no se muestran en el front,
    #  pero se usan para derivar los IDs pendientes que cuelgan de la cuenta.)
    eventos = sorted(recs, key=lambda r: (r.get("_created_sort") or dt.datetime.min), reverse=True)
    for r in recs:
        r.pop("_created_sort", None)

    # IDs concretos de los CR PENDIENTES (crudos) de esta cuenta en ESTE camino.
    # Base de la aprobación/rechazo por nodo (selección por IDs, no por valor).
    pending_ids = [r.get("id") for r in recs
                   if (r.get("status") or "pendiente") == "pendiente" and r.get("id") is not None]

    return {
        "type": "account",
        "cuenta": cuenta,
        "client_selector": selector,
        "celdas_vigentes": len(survivors),
        "modificaciones": len(survivors),     # vigentes (subtotal de niveles superiores)
        "eventos_totales": len(recs),          # eventos crudos
        "impacto_baja": baja,
        "impacto_suba": suba,
        "impacto_neto": neto,
        "estados": estados,
        "estado_consolidado": consolidado,
        "pendientes": estados["pendiente"],
        "pending_ids": pending_ids,
        "pending_count": len(pending_ids),
        "periodo_desde": periodos[0] if periodos else None,
        "periodo_hasta": periodos[-1] if periodos else None,
        "records": eventos,
    }


# Jerarquía DEFINITIVA por pestaña: cada dimensión arranca por sí misma y baja
# la cadena hasta Cuenta. El ÚLTIMO nivel de la lista deriva en cuentas. El nivel
# "grupo" aparece en las CUATRO dimensiones (por eso el group_map se resuelve para
# todas, no solo dimension=="grupo").
_DIM_LEVELS = {
    "grupo":  ["grupo"],
    "perfil": ["perfil", "grupo"],
    "neg":    ["neg", "perfil", "grupo"],
    "subneg": ["subneg", "perfil", "grupo"],
}
# Por nivel: clave en el dict de _cr_to_dict + label del bucket NULL/vacío.
# "grupo" sale de la resolución de get_client_group_map (no es columna de la tabla).
_LEVEL_DICT_KEY  = {"grupo": "grupo", "perfil": "perfil", "neg": "negocio", "subneg": "subnegocio"}
_LEVEL_SIN_LABEL = {"grupo": "Sin grupo", "perfil": "Sin perfil", "neg": "Sin negocio", "subneg": "Sin subnegocio"}


def _build_node(level: str, label: str, value: str, is_sin: bool, children: list[dict]) -> dict:
    """Nodo intermedio (un nivel de la jerarquía) que agrega sus hijos ya
    construidos. Los hijos pueden ser cuentas (hoja) u otros nodos. La agregación
    es recursiva: suma impacto/estados/modificaciones de los hijos y cuenta las
    cuentas hacia arriba sin asumir profundidad fija."""
    baja = round(sum(c.get("impacto_baja") or 0.0 for c in children), 2)
    suba = round(sum(c.get("impacto_suba") or 0.0 for c in children), 2)
    neto = round(sum(c.get("impacto_neto") or 0.0 for c in children), 2)
    estados = {"pendiente": 0, "aprobado": 0, "rechazado": 0}
    for c in children:
        for k in estados:
            estados[k] += (c.get("estados") or {}).get(k, 0)
    consolidado = _estados_consolidado(estados)
    # cuentas: si los hijos son cuentas, cada uno cuenta 1; si son nodos
    # intermedios, sumamos las suyas (recursión hacia arriba).
    if children and children[0].get("type") == "account":
        cuentas = len(children)
    else:
        cuentas = sum(c.get("cuentas") or 0 for c in children)
    modificaciones = sum(c.get("modificaciones") or 0 for c in children)
    # IDs pendientes que cuelgan de ESTE nodo en ESTE camino = unión recursiva de
    # los de sus hijos (hojas o sub-nodos). Selección por IDs concretos → aprobar
    # este nodo no toca CR de otro camino (otro perfil/grupo bajo otra rama).
    pending_ids: list = []
    for c in children:
        pending_ids.extend(c.get("pending_ids") or [])
    return {
        "type": "node",
        "level": level,         # grupo|perfil|neg|subneg → tag del front
        "label": label,
        "value": value,
        "is_sin": is_sin,
        "cuentas": cuentas,
        "modificaciones": modificaciones,
        "impacto_baja": baja,
        "impacto_suba": suba,
        "impacto_neto": neto,
        "estados": estados,
        "estado_consolidado": consolidado,
        "pendientes": estados["pendiente"],
        "pending_ids": pending_ids,
        "pending_count": len(pending_ids),
        "children": children,
    }


def _build_accounts(records: list[dict]) -> list[dict]:
    """Hoja: agrupa por cuenta → _build_account_unit (estado vigente por celda).
    Orden por |impacto neto| desc."""
    cuentas: dict[str, list[dict]] = {}
    for r in records:
        cname = r.get("client_name") or r.get("client_selector") or "—"
        cuentas.setdefault(cname, []).append(r)
    units = [_build_account_unit(cname, crecs) for cname, crecs in cuentas.items()]
    units.sort(key=lambda u: -abs(u.get("impacto_neto") or 0.0))
    return units


def _build_levels(records: list[dict], levels: list[str], idx: int) -> list[dict]:
    """Construcción RECURSIVA de la jerarquía siguiendo `levels` de afuera hacia
    adentro. Cuando se agotan los niveles, el contenido se resuelve en cuentas.
    Bucketing por nivel: clave LOWER(TRIM); NULL/vacío/'—' → bucket único "Sin X"
    (label según el nivel); orden por |impacto neto| desc, "Sin X" al final."""
    if idx >= len(levels):
        return _build_accounts(records)

    level = levels[idx]
    dkey = _LEVEL_DICT_KEY[level]
    sin_label = _LEVEL_SIN_LABEL[level]

    buckets: dict[str, dict] = {}  # norm -> {"label", "value", "is_sin", "recs"}
    for r in records:
        raw = str(r.get(dkey) or "").strip()
        if raw in ("", "—"):
            norm, label, value = "", sin_label, ""
        else:
            norm, label, value = raw.lower(), raw, raw
        b = buckets.get(norm)
        if b is None:
            b = buckets[norm] = {"label": label, "value": value, "is_sin": norm == "", "recs": []}
        b["recs"].append(r)

    nodes: list[dict] = []
    for norm, b in buckets.items():
        children = _build_levels(b["recs"], levels, idx + 1)
        nodes.append(_build_node(level, b["label"], b["value"], b["is_sin"], children))

    nodes.sort(key=lambda u: (u["is_sin"], -abs(u.get("impacto_neto") or 0.0)))
    return nodes


def _build_tree(records: list[dict], dimension: str) -> list[dict]:
    """Árbol consolidado de profundidad variable (2-4 niveles) según la pestaña:
    Grupo→Cuenta · Perfil→Grupo→Cuenta · Negocio→Perfil→Grupo→Cuenta ·
    Subnegocio→Perfil→Grupo→Cuenta. La hoja es siempre la cuenta consolidada.

    REQUISITO: cada record debe traer 'grupo' resuelto vía _cr_to_dict(r, group_map)
    porque "grupo" es un nivel en las CUATRO dimensiones (si no, ese nivel caería
    todo en "Sin grupo")."""
    levels = _DIM_LEVELS.get(dimension, _DIM_LEVELS["grupo"])
    return _build_levels(records, levels, 0)


@router.get("/api/approvals/grouped", response_class=JSONResponse)
def api_approvals_grouped(
    request: Request,
    user: User = Depends(require_module("forecast")),
    estado: Optional[str] = Query("todos"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    comercial: Optional[str] = Query(None),
    perfil: Optional[str] = Query(None),
    negocio: Optional[str] = Query(None),
    subneg: Optional[str] = Query(None),
    articulo: Optional[str] = Query(None),
    period: Optional[str] = Query(None),
    change_type: Optional[str] = Query(None),
    impacto: Optional[str] = Query(None),
    alto_impacto: Optional[str] = Query(None),
    dimension: Optional[str] = Query("grupo"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
):
    """Modificaciones agrupadas por la DIMENSIÓN elegida (una a la vez):
    grupo de clientes (default) | perfil | negocio | subnegocio.
    Paginación por UNIDAD (una unidad nunca se parte).
    Lectura: Admin, Gerente y Auditor.
    KPIs/matriz se calculan sobre TODO el conjunto filtrado."""
    _require_aprobaciones_view(user)
    from web_comparativas.models import SessionLocal
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")

    dim = (dimension or "grupo").strip().lower()
    if dim not in _VALID_DIMENSIONS:
        dim = "grupo"  # fallback robusto ante valores inválidos

    filters = dict(
        estado=estado, date_from=date_from, date_to=date_to, comercial=comercial,
        perfil=perfil, negocio=negocio, subneg=subneg, articulo=articulo, period=period,
        change_type=change_type, impacto=impacto, alto_impacto=alto_impacto,
    )
    try:
        # El mapa cliente→grupo solo hace falta para la dimensión "grupo".
        group_map = svc.get_client_group_map() if dim == "grupo" else None
        with SessionLocal() as session:
            rows = _query_change_requests(session, filters, cap=_MAX_CR_POOL)
            records = [_cr_to_dict(r, group_map) for r in rows]

        # KPIs/matriz desde los propios change requests (delta real, por status).
        kpis = _compute_approval_kpis(records)

        if dim == "grupo":
            # Comportamiento histórico: grupo → registros ; sin grupo → sueltos.
            grouped: dict[str, list[dict]] = {}
            singles: list[dict] = []
            for r in records:
                g = (r.get("grupo") or "").strip()
                if g:
                    grouped.setdefault(g, []).append(r)
                else:
                    r.pop("_created_sort", None)
                    singles.append(r)

            units: list[dict] = []
            for gname in sorted(grouped.keys(), key=lambda s: s.lower()):
                units.append(_build_group_unit(gname, grouped[gname]))
            for r in singles:
                units.append({"type": "single", "record": r})
            total_groups = len(grouped)
        else:
            # perfil / negocio / subnegocio: buckets por la columna del registro
            # (sin sección de "sueltos"; NULL/vacío → bucket "Sin X" al final).
            units = _build_dimension_units(records, dim)
            total_groups = len(units)

        total_units = len(units)
        total_records = len(records)
        pages = max(1, -(-total_units // page_size))
        start = (page - 1) * page_size
        page_units = units[start: start + page_size]

        return JSONResponse({
            "ok": True,
            "dimension": dim,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "total_units": total_units,
            "total_records": total_records,
            "total_groups": total_groups,
            "kpis": kpis,
            "high_impact_threshold": _HIGH_IMPACT_THRESHOLD,
            "units": page_units,
            "pool_capped": total_records >= _MAX_CR_POOL,
        })
    except Exception as exc:
        logger.error("approvals grouped error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al agrupar modificaciones: {exc}")


# ── Medidor de meta (global, company-wide) ───────────────────────────────────
# Mide el EFECTO REAL de las decisiones sobre la META anual (Total_Adj = base×1.25):
#   • proy. APROBADOS         = meta + Σ delta de overrides cuyo CR está aprobado
#   • proy. APROBADOS+PEND.   = meta + Σ delta de aprobados + pendientes
# Los rechazados NO se suman (camino visual; el override real no se revierte).
# GLOBAL: corre sobre TODOS los CRs (sin los filtros de la tabla) para clasificar
# bien por estado y evitar el bug A′. Reusa compute_approval_curve_impacts (delta
# por override) + el linking impacto→estado del CR (mismo patrón que la matriz).

def _impacts_by_status(records_all: list[dict], impacts: dict) -> dict:
    """Suma los deltas de `impacts` (de compute_approval_curve_impacts) segmentados
    por el estado del change request. Devuelve {"aprobado","pendiente","rechazado"}.

    Clasificación PRIMARIA: vínculo directo impacto→override_id→cr.status (el impacto
    ya trae `override_id` del override efectivo del alcance; se lee el status del CR
    más reciente de ese override). FALLBACK (impactos sin override_id: backfill/manual):
    cruce por valor abs(cr.valor_nuevo − ogp)<0.01 sobre el alcance, como antes.
    Default final → "pendiente" (ajuste vigente aún sin revisar). Solo RECLASIFICA:
    la suma total de impactos entre buckets se conserva."""
    out = {"aprobado": 0.0, "pendiente": 0.0, "rechazado": 0.0}

    # Índice PRIMARIO por override_id → (sort, status) del CR MÁS RECIENTE de ese
    # override (un mismo override re-guardado puede tener varios CRs).
    ovr_status: dict = {}
    for r in records_all:
        oid = r.get("override_id")
        if oid is None:
            continue
        so = r.get("_created_sort") or dt.datetime.min
        st = r.get("status") or "pendiente"
        prev = ovr_status.get(oid)
        if prev is None or so >= prev[0]:
            ovr_status[oid] = (so, st)

    def _ck(scope, sel, sub, cod, month):
        if scope == "subnegocio":
            return (scope, sel, sub, "", "")
        if scope == "producto":
            return (scope, sel, "", cod, "")
        if scope == "celda":
            return (scope, sel, sub, cod, month)
        return None

    # Índice de CRs por identidad de alcance → [(sort, status, new_value)].
    cr_index: dict[tuple, list] = {}
    for r in records_all:
        scope = _vig_norm(r.get("scope_type"))
        sub = _vig_norm(r.get("subnegocio"))
        cod = _vig_norm(r.get("codigo_serie"))
        month = _vig_norm(r.get("periodo"))
        nv = r.get("valor_nuevo")
        nvr = round(float(nv), 2) if isinstance(nv, (int, float)) else None
        st = r.get("status") or "pendiente"
        so = r.get("_created_sort") or dt.datetime.min
        for sel in {_vig_norm(r.get("client_selector")), _vig_norm(r.get("client_name"))}:
            if not sel:
                continue
            k = _ck(scope, sel, sub, cod, month)
            if k:
                cr_index.setdefault(k, []).append((so, st, nvr))

    for key, info in (impacts or {}).items():
        impact = float(info.get("impact") or 0.0)
        if impact == 0.0:
            continue
        status = None
        # PRIMARIO: estado por vínculo directo override_id→cr.status.
        oid = info.get("override_id")
        if oid is not None and oid in ovr_status:
            status = ovr_status[oid][1]
            if status not in out:
                status = "pendiente"
        # FALLBACK (sin override_id vinculado): cruce por valor como antes.
        if status is None:
            ogp = round(float(info.get("ogp") or 0.0), 2)
            cands = [c for c in cr_index.get(key, []) if c[2] is not None and abs(c[2] - ogp) < 0.01]
            if cands:
                cands.sort(key=lambda c: c[0])
                status = cands[-1][1]
                if status not in out:
                    status = "pendiente"
            else:
                status = "pendiente"
        out[status] += impact
    return out


def _compute_meta_gauge(session) -> dict:
    """Números del medidor de meta (global). best-effort: ante cualquier falla
    devuelve {"disponible": False} sin romper el árbol."""
    try:
        # Meta global = misma definición que el KPI "Meta Anual" del resto de la UI
        # (Total_Adj = base × 1.25). Reusa get_chart_data sin filtros; puede volver
        # bytes en cache-hit → json.loads (igual que api_approvals_filter_options).
        cd = svc.get_chart_data(is_admin=True, growth_pct=25.0)
        if isinstance(cd, (bytes, bytearray)):
            cd = _json.loads(cd.decode("utf-8"))
        meta = float(((cd or {}).get("kpis") or {}).get("total_proyeccion_adj") or 0.0)
        if not meta or meta <= 0:
            return {"disponible": False, "efecto_detectado": False}

        base = meta / 1.25

        # Año del forecast: del PERÍODO del forecast (años máximos de la serie de
        # proyección), NO de date.today(). Si seguís viendo el forecast 2026 en 2027,
        # la marca temporal del medidor mide contra 2026.
        forecast_year = None
        try:
            _yrs = [int(str(r.get("fecha"))[:4])
                    for r in ((cd or {}).get("forecast") or []) if r.get("fecha")]
            forecast_year = max(_yrs) if _yrs else None
        except Exception:
            forecast_year = None

        impacts = svc.compute_approval_curve_impacts(growth_pct=25.0, is_admin=True)
        rows_all = _query_change_requests(session, {}, cap=_MAX_CR_POOL)   # TODOS los CRs
        records_all = [_cr_to_dict(r) for r in rows_all]
        by = _impacts_by_status(records_all, impacts)

        proy_aprobados = meta + by["aprobado"]
        proy_aprob_pend = meta + by["aprobado"] + by["pendiente"]
        efecto = (abs(by["aprobado"]) + abs(by["pendiente"]) + abs(by["rechazado"])) > 0.5
        return {
            "disponible": True,
            "efecto_detectado": bool(efecto),
            "forecast_year": forecast_year,
            "base": round(base, 0),
            "meta": round(meta, 0),
            "proy_aprobados": round(proy_aprobados, 0),
            "proy_aprob_pend": round(proy_aprob_pend, 0),
            "pct_aprobados": round(proy_aprobados / meta * 100, 1),
            "pct_si_pendientes": round(proy_aprob_pend / meta * 100, 1),
        }
    except Exception as exc:
        logger.warning("meta gauge failed: %s", exc)
        return {"disponible": False, "efecto_detectado": False}


@router.get("/api/approvals/tree", response_class=JSONResponse)
def api_approvals_tree(
    request: Request,
    user: User = Depends(require_module("forecast")),
    estado: Optional[str] = Query("todos"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    comercial: Optional[str] = Query(None),
    perfil: Optional[str] = Query(None),
    negocio: Optional[str] = Query(None),
    subneg: Optional[str] = Query(None),
    articulo: Optional[str] = Query(None),
    period: Optional[str] = Query(None),
    change_type: Optional[str] = Query(None),
    impacto: Optional[str] = Query(None),
    alto_impacto: Optional[str] = Query(None),
    dimension: Optional[str] = Query("grupo"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
):
    """Aprobaciones en árbol consolidado de 2 niveles: <dimensión> → Cuenta.

    La dimensión de nivel 1 (grupo | perfil | neg | subneg) la elige el selector
    de pestañas. Nivel 2 = una sola línea por CUENTA (consolida sus eventos al
    estado vigente por celda); los eventos crudos quedan en cada cuenta para el
    detalle de nivel 3. Lectura: Admin, Gerente y Auditor. Los KPIs/matriz se
    calculan sobre TODO el conjunto filtrado. Paginación por nodo de nivel 1
    (un bucket de la dimensión nunca se parte)."""
    _require_aprobaciones_view(user)
    from web_comparativas.models import SessionLocal
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")

    dim = (dimension or "grupo").strip().lower()
    if dim not in _VALID_DIMENSIONS:
        dim = "grupo"  # fallback robusto ante valores inválidos

    filters = dict(
        estado=estado, date_from=date_from, date_to=date_to, comercial=comercial,
        perfil=perfil, negocio=negocio, subneg=subneg, articulo=articulo, period=period,
        change_type=change_type, impacto=impacto, alto_impacto=alto_impacto,
    )
    try:
        # CRÍTICO: el nivel "grupo" aparece en las CUATRO dimensiones, así que el
        # mapa cliente→grupo se resuelve SIEMPRE (no solo si dim=="grupo"); cada
        # registro se resuelve vía _cr_to_dict(r, group_map) ANTES de bucketizar.
        # Sin esto, el nivel grupo de Perfil/Negocio/Subnegocio caería en "Sin grupo".
        group_map = svc.get_client_group_map()
        with SessionLocal() as session:
            rows = _query_change_requests(session, filters, cap=_MAX_CR_POOL)
            records = [_cr_to_dict(r, group_map) for r in rows]

            # Medidor de meta: GLOBAL (sobre TODOS los CRs, no `records` filtrado).
            gauge = _compute_meta_gauge(session)

        # KPIs/matriz desde los propios change requests (delta real, por status).
        kpis = _compute_approval_kpis(records)

        # _build_tree consume _created_sort (vía dedupe) y luego lo descarta;
        # por eso se construye DESPUÉS de _compute_approval_kpis.
        tree = _build_tree(records, dim)
        total_units = len(tree)
        total_accounts = sum(n.get("cuentas") or 0 for n in tree)
        total_records = len(records)
        pages = max(1, -(-total_units // page_size))
        start = (page - 1) * page_size
        page_nodes = tree[start: start + page_size]

        return JSONResponse({
            "ok": True,
            "dimension": dim,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "total_units": total_units,
            "total_accounts": total_accounts,
            "total_records": total_records,
            "kpis": kpis,
            "gauge": gauge,
            "high_impact_threshold": _HIGH_IMPACT_THRESHOLD,
            "tree": page_nodes,
            "pool_capped": total_records >= _MAX_CR_POOL,
        })
    except Exception as exc:
        logger.error("approvals tree error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al construir árbol de aprobaciones: {exc}")


class _GroupReviewPayload(BaseModel):
    grupo: Optional[str] = None       # dimension=grupo: nombre del grupo (compat)
    dimension: Optional[str] = "grupo"  # grupo | perfil | neg | subneg
    value: Optional[str] = None       # valor canónico del bucket (dimensiones ≠ grupo; "" = "Sin X")
    motivo: Optional[str] = Field(default=None, max_length=2000)
    # Contexto/filtros actuales: la acción solo afecta lo que el Admin está viendo
    estado: Optional[str] = "todos"
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    comercial: Optional[str] = None
    perfil: Optional[str] = None
    negocio: Optional[str] = None
    subneg: Optional[str] = None
    articulo: Optional[str] = None
    period: Optional[str] = None
    change_type: Optional[str] = None
    impacto: Optional[str] = None
    alto_impacto: Optional[str] = None


def _group_filters(payload: "_GroupReviewPayload") -> dict:
    return dict(
        estado=payload.estado, date_from=payload.date_from, date_to=payload.date_to,
        comercial=payload.comercial, perfil=payload.perfil, negocio=payload.negocio,
        subneg=payload.subneg, articulo=payload.articulo, period=payload.period,
        change_type=payload.change_type, impacto=payload.impacto, alto_impacto=payload.alto_impacto,
    )


def _review_group(payload: "_GroupReviewPayload", *, status: str, user: User) -> int:
    """Aplica revisión (aprobado/rechazado) a las modificaciones PENDIENTES de la
    UNIDAD elegida (grupo de clientes o bucket de perfil/negocio/subnegocio),
    dentro del contexto/filtros actuales. Re-deriva el conjunto en el servidor
    (no recibe IDs del front). No toca registros ya decididos."""
    from web_comparativas.models import SessionLocal
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")

    dim = (payload.dimension or "grupo").strip().lower()
    if dim not in _REVIEW_DIMENSIONS:
        dim = "grupo"
    motivo = (payload.motivo or "").strip() or None
    filters = _group_filters(payload)

    if dim == "grupo":
        gkey = (payload.grupo or payload.value or "").strip().lower()
        if not gkey:
            raise HTTPException(400, "Grupo no especificado.")
        group_map = svc.get_client_group_map()
    elif dim == "cuenta":
        # Aprobar/rechazar una CUENTA entera (árbol Perfil→Grupo→Cuenta): matchea
        # contra client_name O client_selector. value="" no es válido aquí.
        ckey = (payload.value or payload.grupo or "").strip().lower()
        if not ckey:
            raise HTTPException(400, "Cuenta no especificada.")
    else:
        if payload.value is None:
            raise HTTPException(400, "Valor de agrupación no especificado.")
        vkey = (payload.value or "").strip().lower()  # "" → bucket "Sin X" (NULL/vacío)
        col = _DIM_MODEL_COL[dim]

    n = 0
    reverted_owner_ids: set[int] = set()
    with SessionLocal() as session:
        rows = _query_change_requests(session, filters, cap=_MAX_CR_POOL)
        for cr in rows:
            if cr.status != "pendiente":
                continue
            if dim == "grupo":
                g = _lookup_grupo(cr.client_name or cr.client_selector, cr.client_selector, group_map)
                if (g or "").strip().lower() != gkey:
                    continue
            elif dim == "cuenta":
                name = str(cr.client_name or "").strip().lower()
                sel = str(cr.client_selector or "").strip().lower()
                if ckey not in (name, sel):
                    continue
            else:
                colval = str(getattr(cr, col, None) or "").strip().lower()
                if colval != vkey:  # vkey "" matchea NULL/vacío (Sin X)
                    continue
            owner = _apply_review(session, cr, status=status, user=user, motivo=motivo)
            if owner is not None:
                reverted_owner_ids.add(owner)
            n += 1
        session.commit()
    for uid in reverted_owner_ids:
        svc._clear_cache_for_override_save(uid)
    return n


@router.post("/api/approvals/group/approve", response_class=JSONResponse)
def api_approvals_group_approve(
    payload: _GroupReviewPayload,
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Aprueba todas las modificaciones pendientes del grupo (contexto actual). Solo Admin/Gerente."""
    _require_aprobaciones_edit(user)
    try:
        n = _review_group(payload, status="aprobado", user=user)
        return JSONResponse({"ok": True, "grupo": payload.grupo or payload.value, "status": "aprobado", "afectados": n})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("group approve error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al aprobar grupo: {exc}")


@router.post("/api/approvals/group/reject", response_class=JSONResponse)
def api_approvals_group_reject(
    payload: _GroupReviewPayload,
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Rechaza con motivo todas las modificaciones pendientes del grupo (contexto actual). Solo Admin/Gerente."""
    _require_aprobaciones_edit(user)
    if not (payload.motivo or "").strip():
        raise HTTPException(400, "Debe indicar un motivo para rechazar el grupo.")
    try:
        n = _review_group(payload, status="rechazado", user=user)
        return JSONResponse({"ok": True, "grupo": payload.grupo or payload.value, "status": "rechazado", "afectados": n})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("group reject error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al rechazar grupo: {exc}")


# ── Aprobación/rechazo por IDs concretos (árbol: un nodo cualquiera) ──────────
# Cada nodo del árbol expone los IDs pendientes que cuelgan de él en SU camino
# (pending_ids). El cliente manda esos IDs; el server RE-VALIDA que estén dentro
# de los filtros vigentes y pendientes (no confía ciegamente en el cliente).

class _ByIdsReviewPayload(BaseModel):
    ids: List[int] = Field(default_factory=list)
    motivo: Optional[str] = Field(default=None, max_length=2000)
    # Contexto/filtros actuales: la acción solo afecta lo que el usuario ve.
    estado: Optional[str] = "todos"
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    comercial: Optional[str] = None
    perfil: Optional[str] = None
    negocio: Optional[str] = None
    subneg: Optional[str] = None
    articulo: Optional[str] = None
    period: Optional[str] = None
    change_type: Optional[str] = None
    impacto: Optional[str] = None
    alto_impacto: Optional[str] = None


def _ids_filters(payload: "_ByIdsReviewPayload") -> dict:
    return dict(
        estado=payload.estado, date_from=payload.date_from, date_to=payload.date_to,
        comercial=payload.comercial, perfil=payload.perfil, negocio=payload.negocio,
        subneg=payload.subneg, articulo=payload.articulo, period=payload.period,
        change_type=payload.change_type, impacto=payload.impacto, alto_impacto=payload.alto_impacto,
    )


def _review_by_ids(payload: "_ByIdsReviewPayload", *, status: str, user: User) -> int:
    """Aplica revisión (aprobado/rechazado) SOLO a los CR cuyo id viene en
    payload.ids Y que, re-derivados server-side desde los filtros vigentes, sigan
    PENDIENTES. Re-validación: no se confía en los ids del cliente — se intersecan
    con el pool filtrado (mismo que ve la UI) y se exige status pendiente."""
    from web_comparativas.models import SessionLocal
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")
    requested = {int(i) for i in (payload.ids or [])}
    if not requested:
        return 0
    motivo = (payload.motivo or "").strip() or None
    filters = _ids_filters(payload)
    n = 0
    reverted_owner_ids: set[int] = set()
    with SessionLocal() as session:
        rows = _query_change_requests(session, filters, cap=_MAX_CR_POOL)
        for cr in rows:
            if cr.id in requested and cr.status == "pendiente":
                owner = _apply_review(session, cr, status=status, user=user, motivo=motivo)
                if owner is not None:
                    reverted_owner_ids.add(owner)
                n += 1
        session.commit()
    for uid in reverted_owner_ids:
        svc._clear_cache_for_override_save(uid)
    return n


@router.post("/api/approvals/by-ids/approve", response_class=JSONResponse)
def api_approvals_by_ids_approve(
    payload: _ByIdsReviewPayload,
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Aprueba los CR pendientes indicados por id (un nodo del árbol). Solo Admin/Gerente."""
    _require_aprobaciones_edit(user)
    try:
        n = _review_by_ids(payload, status="aprobado", user=user)
        return JSONResponse({"ok": True, "status": "aprobado", "afectados": n})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("by-ids approve error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al aprobar: {exc}")


@router.post("/api/approvals/by-ids/reject", response_class=JSONResponse)
def api_approvals_by_ids_reject(
    payload: _ByIdsReviewPayload,
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """Rechaza con motivo los CR pendientes indicados por id (un nodo del árbol). Solo Admin/Gerente."""
    _require_aprobaciones_edit(user)
    if not (payload.motivo or "").strip():
        raise HTTPException(400, "Debe indicar un motivo para rechazar.")
    try:
        n = _review_by_ids(payload, status="rechazado", user=user)
        return JSONResponse({"ok": True, "status": "rechazado", "afectados": n})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("by-ids reject error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al rechazar: {exc}")


_CR_EXPORT_ORDER = [
    "created_at", "usuario", "grupo", "change_type_label", "campo", "valor_anterior",
    "valor_nuevo", "delta_abs", "delta_pct", "impacto_estimado", "client_name",
    "perfil", "negocio", "subnegocio", "codigo_serie", "periodo",
    "status_label", "revisado_por", "revisado_el", "motivo",
]
_CR_EXPORT_LABELS = {
    "created_at": "Fecha y hora",
    "usuario": "Usuario",
    "grupo": "Grupo",
    "change_type_label": "Tipo de modificación",
    "campo": "Campo modificado",
    "valor_anterior": "Valor anterior (%)",
    "valor_nuevo": "Valor nuevo (%)",
    "delta_abs": "Diferencia (puntos)",
    "delta_pct": "Diferencia %",
    "impacto_estimado": "Impacto estimado (ARS)",
    "client_name": "Cuenta / Cliente",
    "perfil": "Perfil",
    "negocio": "Negocio",
    "subnegocio": "Subnegocio",
    "codigo_serie": "Artículo",
    "periodo": "Período",
    "status_label": "Estado",
    "revisado_por": "Revisado por",
    "revisado_el": "Fecha de revisión",
    "motivo": "Observación / Motivo",
}


@router.get("/api/approvals/export")
def api_approvals_export(
    request: Request,
    user: User = Depends(require_module("forecast")),
    fmt: str = Query("csv", description="csv | xlsx"),
    estado: Optional[str] = Query("todos"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    comercial: Optional[str] = Query(None),
    perfil: Optional[str] = Query(None),
    negocio: Optional[str] = Query(None),
    subneg: Optional[str] = Query(None),
    articulo: Optional[str] = Query(None),
    period: Optional[str] = Query(None),
    change_type: Optional[str] = Query(None),
    impacto: Optional[str] = Query(None),
    alto_impacto: Optional[str] = Query(None),
):
    """Informe ejecutivo de modificaciones (CSV/Excel).
    Lectura: Admin, Gerente y Auditor."""
    _require_aprobaciones_view(user)
    from web_comparativas.models import SessionLocal
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")

    filters = dict(
        estado=estado, date_from=date_from, date_to=date_to, comercial=comercial,
        perfil=perfil, negocio=negocio, subneg=subneg, articulo=articulo, period=period,
        change_type=change_type, impacto=impacto, alto_impacto=alto_impacto,
    )
    try:
        group_map = svc.get_client_group_map()
        with SessionLocal() as session:
            rows = _query_change_requests(session, filters, cap=_MAX_CR_POOL)
            records = [_cr_to_dict(r, group_map) for r in rows]
            for r in records:
                r["grupo"] = r.get("grupo") or "Sin grupo"
    except Exception as exc:
        logger.error("approvals export query error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al consultar datos: {exc}")

    export_rows = [{k: r.get(k, "") for k in _CR_EXPORT_ORDER} for r in records]
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename_base = f"informe_modificaciones_forecast_{ts}"

    if fmt == "xlsx":
        import io
        import pandas as _pd
        df = _pd.DataFrame(export_rows)
        if not df.empty:
            df = df[_CR_EXPORT_ORDER]
        df.rename(columns=_CR_EXPORT_LABELS, inplace=True)
        buf = io.BytesIO()
        with _pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Modificaciones Forecast")
        buf.seek(0)
        headers = {
            "Content-Disposition": f'attachment; filename="{filename_base}.xlsx"',
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        return Response(content=buf.read(), headers=headers)

    import csv, io as _io
    output = _io.StringIO()
    labels = [_CR_EXPORT_LABELS.get(k, k) for k in _CR_EXPORT_ORDER]
    w = csv.writer(output)
    w.writerow(labels)
    for r in export_rows:
        w.writerow([r.get(k, "") for k in _CR_EXPORT_ORDER])
    csv_bytes = output.getvalue().encode("utf-8-sig")
    headers = {
        "Content-Disposition": f'attachment; filename="{filename_base}.csv"',
        "Content-Type": "text/csv; charset=utf-8-sig",
    }
    return Response(content=csv_bytes, headers=headers)


@router.get("/api/comments/summary", response_class=JSONResponse)
def forecast_api_summary(
    request: Request,
    user: User = Depends(require_module("forecast")),
):
    """
    Retorna el resumen de tickets activos de Forecast para el usuario actual.
    Usado por el widget para mostrar el badge y el historial resumido.
    """
    db = request.state.db
    if db is None:
        return JSONResponse({"ok": False, "error": "DB no disponible", "open_count": 0}, status_code=503)
    try:
        tickets = (
            db.query(Ticket)
            .filter(
                Ticket.modulo_origen == "forecast",
                Ticket.user_id == user.id,
            )
            .order_by(Ticket.updated_at.desc())
            .all()
        )

        open_count = sum(1 for t in tickets if t.status in ("abierto", "pendiente"))
        total_msgs = sum(len(t.messages) for t in tickets)

        recent_messages = []
        if tickets:
            latest = tickets[0]
            for m in latest.messages[-10:]:
                sender_name = (
                    "Tú" if m.user_id == user.id
                    else (m.user.name or m.user.email.split("@")[0].capitalize())
                )
                is_admin_role = "admin" in (m.user.role or "").lower() or "supervisor" in (m.user.role or "").lower()
                recent_messages.append({
                    "id": m.id,
                    "message": m.message,
                    "sender": sender_name,
                    "is_admin": is_admin_role,
                    "is_me": m.user_id == user.id,
                    "created_at": m.created_at.strftime("%d/%m %H:%M"),
                })

        active_ticket = tickets[0] if tickets else None

        return JSONResponse({
            "ok": True,
            "open_count": open_count,
            "total_tickets": len(tickets),
            "total_messages": total_msgs,
            "active_ticket_id": active_ticket.id if active_ticket else None,
            "active_ticket_status": active_ticket.status if active_ticket else None,
            "recent_messages": recent_messages,
        })
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.error("forecast-summary error: %s", exc, exc_info=True)
        return JSONResponse({"ok": False, "error": str(exc), "open_count": 0}, status_code=500)
