"""
forecast_router.py — Routes and API endpoints for the Forecast module.
"""
from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent.parent  # web_comparativas/
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter(tags=["forecast"])


# ─── Page route ───────────────────────────────────────────────────────────
@router.get("/forecast", response_class=HTMLResponse)
def forecast_page(request: Request):
    user = getattr(request.state, "user", None)
    if not user:
        from fastapi.responses import RedirectResponse
        return RedirectResponse("/login", 303)
    return templates.TemplateResponse("forecast.html", {
        "request": request,
        "user": user,
        "market_context": "forecast",
        "_display_name": getattr(user, "display_name", user.email if hasattr(user, "email") else "Yo"),
    })


# ─── API: filter options ─────────────────────────────────────────────────
@router.get("/api/forecast/filters")
def api_filters():
    from web_comparativas.forecast_service import get_filter_options, is_available
    if not is_available():
        return JSONResponse({"error": "Forecast data not found"}, status_code=404)
    return get_filter_options()


# ─── API: chart data ─────────────────────────────────────────────────────
@router.post("/api/forecast/chart")
async def api_chart(request: Request):
    body = await request.json()
    from web_comparativas.forecast_service import get_chart_data
    return get_chart_data(
        start_date=body.get("start_date", ""),
        end_date=body.get("end_date", ""),
        profiles=body.get("profiles", []),
        negocios=body.get("negocios", []),
        subnegocios=body.get("subnegocios", []),
        products=body.get("products", []),
        growth_pct=float(body.get("growth_pct", 0)),
        view_money=body.get("view_money", True),
    )


# ─── API: client table ───────────────────────────────────────────────────
@router.post("/api/forecast/clients")
async def api_clients(request: Request):
    body = await request.json()
    from web_comparativas.forecast_service import get_client_table
    return get_client_table(
        start_date=body.get("start_date", ""),
        end_date=body.get("end_date", ""),
        profiles=body.get("profiles", []),
        negocios=body.get("negocios", []),
        subnegocios=body.get("subnegocios", []),
        products=body.get("products", []),
        growth_pct=float(body.get("growth_pct", 0)),
        view_money=body.get("view_money", True),
    )


# ─── API: client detail (product-level for modal) ───────────────────────
@router.post("/api/forecast/client-detail")
async def api_client_detail(request: Request):
    body = await request.json()
    from web_comparativas.forecast_service import get_client_detail
    return get_client_detail(
        cliente_display=body.get("cliente", ""),
        start_date=body.get("start_date", ""),
        end_date=body.get("end_date", ""),
        profiles=body.get("profiles", []),
        negocios=body.get("negocios", []),
        subnegocios=body.get("subnegocios", []),
        products=body.get("products", []),
        growth_pct=float(body.get("growth_pct", 0)),
    )
