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
    """Aprobaciones Forecast: acceso EXCLUSIVO de Admin (frontend + backend)."""
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
    # Defensa en profundidad: la sección pasó a ser exclusiva de Admin.
    _require_admin_only(user)


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
    user: User = Depends(_require_user),
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
    user: User = Depends(_require_user),
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
    user: User = Depends(_require_user),
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
        "grupo": _lookup_grupo(cr.client_name or cr.client_selector, cr.client_selector, group_map),
        "created_at": cr.created_at.strftime("%Y-%m-%d %H:%M:%S") if cr.created_at else "—",
        "source": cr.source or "—",
        "change_type": cr.change_type or "ajuste",
        "change_type_label": _CR_TYPE_LABELS.get(cr.change_type, cr.change_type or "—"),
        "usuario": cr.created_by_username or "—",
        "scope_type": cr.scope_type or "—",
        "client_name": cr.client_name or cr.client_selector or "—",
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


def _compute_approval_kpis(records: list[dict]) -> dict:
    """KPIs ejecutivos sobre el conjunto FILTRADO (antes de paginar)."""
    def _amt(r):
        v = r.get("impacto_estimado")
        return float(v) if isinstance(v, (int, float)) else 0.0

    pendientes = [r for r in records if r.get("status") == "pendiente"]
    aprobados = [r for r in records if r.get("status") == "aprobado"]
    rechazados = [r for r in records if r.get("status") == "rechazado"]

    # Mayor impacto por cuenta/grupo (suma de impacto estimado en valor absoluto)
    por_cuenta: dict[str, float] = {}
    for r in records:
        cta = r.get("client_name") or "—"
        por_cuenta[cta] = por_cuenta.get(cta, 0.0) + abs(_amt(r))
    mayor_cuenta, mayor_cuenta_monto = ("—", 0.0)
    if por_cuenta:
        mayor_cuenta, mayor_cuenta_monto = max(por_cuenta.items(), key=lambda kv: kv[1])

    usuarios = {r.get("usuario") for r in records if r.get("usuario") and r.get("usuario") != "—"}

    # Dirección (Baja/Suba) por SIGNO del impacto; si el impacto no está estimado,
    # se usa el signo de la diferencia % (siempre disponible). Esto evita que un
    # change_type "ajuste" o un impacto NULL excluya el registro de la matriz.
    def _impact_value(r):
        v = r.get("impacto_estimado")
        return float(v) if isinstance(v, (int, float)) else None

    def _direction(r):
        v = _impact_value(r)
        if v is not None and v != 0:
            return "baja" if v < 0 else "suba"
        d = r.get("delta_pct")
        if isinstance(d, (int, float)) and d != 0:
            return "baja" if d < 0 else "suba"
        return None  # sin dirección clara (delta 0 y sin impacto)

    subas = sum(1 for r in records if _direction(r) == "suba")
    bajas = sum(1 for r in records if _direction(r) == "baja")
    ajustes = sum(1 for r in records if _direction(r) is None)
    altas = sum(1 for r in records if r.get("change_type") == "alta_manual")

    # Matriz ejecutiva: por estatus × {baja, suba}.
    # Cada celda lleva: monto (suma de impactos estimados), n (cantidad) y
    # sin_estimar (cuántos no tienen impacto calculable) → permite mostrar N/D
    # honesto en vez de un falso $0.
    def _cell(rows, direction):
        sel = [r for r in rows if _direction(r) == direction]
        monto = round(sum(v for v in (_impact_value(r) for r in sel) if v is not None), 2)
        sin_est = sum(1 for r in sel if _impact_value(r) is None)
        return {"monto": monto, "n": len(sel), "sin_estimar": sin_est}

    matrix = {
        st: {"baja": _cell(rows, "baja"), "suba": _cell(rows, "suba")}
        for st, rows in (("pendiente", pendientes), ("aprobado", aprobados), ("rechazado", rechazados))
    }
    sin_estimar_total = sum(1 for r in records if _impact_value(r) is None)

    return {
        "pendientes": len(pendientes),
        "aprobados": len(aprobados),
        "rechazados": len(rechazados),
        "impacto_pendiente": round(sum(_amt(r) for r in pendientes), 2),
        "impacto_aprobado": round(sum(_amt(r) for r in aprobados), 2),
        "impacto_rechazado": round(sum(_amt(r) for r in rechazados), 2),
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
    user: User = Depends(_require_user),
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
    """Lista de modificaciones para revisión + KPIs ejecutivos. Solo Admin."""
    _require_admin_only(user)
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
    user: User = Depends(_require_user),
):
    """Valores distintos para desplegables de filtro. Solo Admin.

    Perfil, Negocio y Subnegocio se alimentan de la MISMA fuente que el filtro
    general superior de Forecast (svc.get_filter_options() → dataset valorizado),
    de modo que el listado coincida exactamente. Usuario (cotizador) y Período
    son específicos de las modificaciones, por lo que se derivan de la propia
    tabla forecast_change_requests."""
    _require_admin_only(user)
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


def _apply_review(session, cr, *, status: str, user: User, motivo: Optional[str]):
    cr.status = status
    cr.reviewed_by_user_id = getattr(user, "id", None)
    cr.reviewed_by_username = getattr(user, "email", None) or getattr(user, "display_name", None)
    cr.reviewed_at = dt.datetime.utcnow()
    if motivo is not None:
        cr.review_comment = motivo.strip() or None


@router.post("/api/approvals/{request_id:int}/approve", response_class=JSONResponse)
def api_approvals_approve(
    request_id: int,
    payload: _ReviewPayload,
    request: Request,
    user: User = Depends(_require_user),
):
    """Aprueba una modificación pendiente. Solo Admin. No revierte el override."""
    _require_admin_only(user)
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
    user: User = Depends(_require_user),
):
    """Rechaza una modificación con motivo. Solo Admin. No revierte el override."""
    _require_admin_only(user)
    motivo = (payload.motivo or "").strip()
    if not motivo:
        raise HTTPException(400, "Debe indicar un motivo para rechazar la modificación.")
    from web_comparativas.models import SessionLocal, ForecastChangeRequest as CR
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")
    try:
        with SessionLocal() as session:
            cr = session.get(CR, request_id)
            if cr is None:
                raise HTTPException(404, "Modificación no encontrada")
            _apply_review(session, cr, status="rechazado", user=user, motivo=motivo)
            session.commit()
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


@router.get("/api/approvals/grouped", response_class=JSONResponse)
def api_approvals_grouped(
    request: Request,
    user: User = Depends(_require_user),
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
    page_size: int = Query(25, ge=1, le=200),
):
    """Modificaciones agrupadas por grupo de clientes (clientes sueltos al final).
    Paginación por UNIDAD (un grupo entero = 1 unidad → nunca se parte). Solo Admin.
    KPIs/matriz se calculan sobre TODO el conjunto filtrado."""
    _require_admin_only(user)
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

        kpis = _compute_approval_kpis(records)

        # Agrupar: grupo → registros ; sin grupo → sueltos
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

        total_units = len(units)
        total_records = len(records)
        pages = max(1, -(-total_units // page_size))
        start = (page - 1) * page_size
        page_units = units[start: start + page_size]

        return JSONResponse({
            "ok": True,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "total_units": total_units,
            "total_records": total_records,
            "total_groups": len(grouped),
            "kpis": kpis,
            "high_impact_threshold": _HIGH_IMPACT_THRESHOLD,
            "units": page_units,
            "pool_capped": total_records >= _MAX_CR_POOL,
        })
    except Exception as exc:
        logger.error("approvals grouped error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al agrupar modificaciones: {exc}")


class _GroupReviewPayload(BaseModel):
    grupo: str
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
    """Aplica revisión (aprobado/rechazado) a las modificaciones PENDIENTES del grupo,
    dentro del contexto/filtros actuales. No toca registros ya decididos."""
    from web_comparativas.models import SessionLocal
    if SessionLocal is None:
        raise HTTPException(503, "Almacenamiento no disponible")
    gkey = (payload.grupo or "").strip().lower()
    if not gkey:
        raise HTTPException(400, "Grupo no especificado.")
    motivo = (payload.motivo or "").strip() or None
    group_map = svc.get_client_group_map()
    filters = _group_filters(payload)
    n = 0
    with SessionLocal() as session:
        rows = _query_change_requests(session, filters, cap=_MAX_CR_POOL)
        for cr in rows:
            if cr.status != "pendiente":
                continue
            g = _lookup_grupo(cr.client_name or cr.client_selector, cr.client_selector, group_map)
            if (g or "").strip().lower() != gkey:
                continue
            _apply_review(session, cr, status=status, user=user, motivo=motivo)
            n += 1
        session.commit()
    return n


@router.post("/api/approvals/group/approve", response_class=JSONResponse)
def api_approvals_group_approve(
    payload: _GroupReviewPayload,
    request: Request,
    user: User = Depends(_require_user),
):
    """Aprueba todas las modificaciones pendientes del grupo (contexto actual). Solo Admin."""
    _require_admin_only(user)
    try:
        n = _review_group(payload, status="aprobado", user=user)
        return JSONResponse({"ok": True, "grupo": payload.grupo, "status": "aprobado", "afectados": n})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("group approve error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al aprobar grupo: {exc}")


@router.post("/api/approvals/group/reject", response_class=JSONResponse)
def api_approvals_group_reject(
    payload: _GroupReviewPayload,
    request: Request,
    user: User = Depends(_require_user),
):
    """Rechaza con motivo todas las modificaciones pendientes del grupo (contexto actual). Solo Admin."""
    _require_admin_only(user)
    if not (payload.motivo or "").strip():
        raise HTTPException(400, "Debe indicar un motivo para rechazar el grupo.")
    try:
        n = _review_group(payload, status="rechazado", user=user)
        return JSONResponse({"ok": True, "grupo": payload.grupo, "status": "rechazado", "afectados": n})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("group reject error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Error al rechazar grupo: {exc}")


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
    user: User = Depends(_require_user),
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
    """Informe ejecutivo de modificaciones (CSV/Excel). Solo Admin."""
    _require_admin_only(user)
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
