"""
Forecast router — integrates the Forecast module into the SIEM platform.
Routes:
  GET  /forecast/              → renders the dashboard template
  GET  /api/forecast/filter-options
  GET  /api/forecast/product-list
  GET  /api/forecast/chart-data
  GET  /api/forecast/client-table
  POST /api/forecast/reload    → (admin) force re-load CSVs from disk
"""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from pydantic import BaseModel

from web_comparativas.models import User
from web_comparativas import forecast_service as svc

logger = logging.getLogger("wc.forecast.router")

BASE_DIR = Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter(prefix="/forecast", tags=["forecast"])


# ---------------------------------------------------------------------------
# Auth helper (same pattern as other routers)
# ---------------------------------------------------------------------------

def _require_user(request: Request) -> User:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="No autenticado")
    return user


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
    try:
        return svc.get_filter_options()
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
    try:
        return svc.get_product_list(profiles=profiles, neg=neg)
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
    view_money: bool = Query(default=True),
    growth_pct: float = Query(default=0.0),
    _user: User = Depends(_require_user),
):
    try:
        return svc.get_chart_data(
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            view_money=view_money,
            growth_pct=growth_pct,
        )
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
    _user: User = Depends(_require_user),
):
    try:
        return svc.get_client_table(
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            view_money=view_money,
            growth_pct=growth_pct,
            lab_products=lab_products,
        )
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
    _user: User = Depends(_require_user),
):
    try:
        return svc.get_treemap_data(
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            view_money=view_money,
            period_date=period_date,
        )
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
    try:
        return svc.get_client_detail(
            client_id=client_id,
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            growth_pct=growth_pct,
        )
    except Exception as exc:
        logger.error("client-detail error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.get("/api/debug-schema")
def api_debug_schema(request: Request, _user: User = Depends(_require_user)):
    """Return actual column names for all forecast tables from information_schema.
    Use this to verify the real PostgreSQL schema matches what the code expects."""
    try:
        return svc.get_forecast_schema_info()
    except Exception as exc:
        logger.error("debug-schema error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.post("/api/reload")
def api_reload(request: Request, _user: User = Depends(_require_user)):
    if (getattr(_user, "role", "") or "").lower() not in ("admin", "auditor"):
        raise HTTPException(403, "Solo admins pueden recargar los datos de Forecast")
    try:
        svc.reload_data()
        return {"ok": True, "msg": "Datos de Forecast recargados"}
    except Exception as exc:
        raise HTTPException(500, str(exc))


# ---------------------------------------------------------------------------
# Override endpoints (save / clear client projection edits)
# ---------------------------------------------------------------------------

class _Override(BaseModel):
    articulo: str
    date: str      # "YYYY-MM"
    pct: float     # percentage adjustment: nuevo = orig * (1 + pct/100)


class _SavePayload(BaseModel):
    client_id: str
    overrides: List[_Override]   # List from typing — required for Pydantic v1 compatibility


@router.post("/api/save-client")
def api_save_client(
    payload: _SavePayload,
    _request: Request,
    _user: User = Depends(_require_user),
):
    """Persist per-product overrides for a client and reflect changes in the whole dashboard."""
    try:
        svc.save_client_overrides(
            payload.client_id,
            [{"articulo": o.articulo, "date": o.date, "pct": o.pct} for o in payload.overrides],
        )
        return {"ok": True, "client_id": payload.client_id, "saved": len(payload.overrides)}
    except Exception as exc:
        logger.error("save-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))


@router.delete("/api/clear-client/{client_id}")
def api_clear_client(
    client_id: str,
    _request: Request,
    _user: User = Depends(_require_user),
):
    """Remove all saved overrides for a client, restoring the CSV baseline."""
    try:
        svc.clear_client_overrides(client_id)
        return {"ok": True, "client_id": client_id}
    except Exception as exc:
        logger.error("clear-client error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc))
