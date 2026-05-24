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
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path
from pydantic import BaseModel, Field

from web_comparativas.models import User, Ticket, TicketMessage
from web_comparativas import forecast_service as svc

logger = logging.getLogger("wc.forecast.router")
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

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
    """Admin and Auditor can read consolidated Forecast overrides from all users."""
    role_key = _forecast_role_key(user)
    return role_key in {
        "admin",
        "administrator",
        "administrador",
        "auditor",
        "audit",
        "aud",
        "auditor_siem",
    }


# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------

@router.get("", response_class=HTMLResponse, include_in_schema=False)
@router.get("/", response_class=HTMLResponse)
def forecast_home(request: Request, user: User = Depends(_require_user)):
    return templates.TemplateResponse(
        "forecast/index.html",
        {"request": request, "user": user, "market_context": "forecast"},
    )


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@router.get("/api/filter-options")
def api_filter_options(request: Request, _user: User = Depends(_require_user)):
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
def api_debug_schema(request: Request, _user: User = Depends(_require_user)):
    """Return actual column names for all forecast tables from information_schema.
    Use this to verify the real PostgreSQL schema matches what the code expects."""
    try:
        return svc.get_forecast_schema_info()
    except Exception as exc:
        logger.error("debug-schema error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/debug-overrides")
def api_debug_overrides(request: Request, _user: User = Depends(_require_user)):
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
def api_reload(request: Request, _user: User = Depends(_require_user)):
    if (getattr(_user, "role", "") or "").lower() not in ("admin", "auditor"):
        raise HTTPException(403, "Solo admins pueden recargar los datos de Forecast")
    try:
        svc.reload_data()
        return {"ok": True, "msg": "Datos de Forecast recargados"}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@router.get("/api/diag")
def api_diag(_user: User = Depends(_require_user)):
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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

def _require_admin(request: Request, _user: User = Depends(_require_user)) -> User:
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
    _user: User = Depends(_require_user),
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
    _user: User = Depends(_require_user),
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
    user: User = Depends(_require_user),
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


@router.get("/api/comments/summary", response_class=JSONResponse)
def forecast_api_summary(
    request: Request,
    user: User = Depends(_require_user),
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
