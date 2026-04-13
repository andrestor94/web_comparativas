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
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from pydantic import BaseModel

from web_comparativas.models import User, db_session, Ticket, TicketMessage
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
        # Log session state to diagnose 401s in production
        uid = getattr(request.session, "get", lambda k, d=None: d)("uid") if hasattr(request, "session") else None
        logger.warning(
            "forecast 401 — path=%s uid_in_session=%s has_state_user=%s",
            request.url.path, uid, hasattr(request.state, "user"),
        )
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
    import traceback as _tb
    print(
        f"[FORECAST ROUTER] chart-data start_date={start_date} end_date={end_date} "
        f"profiles={profiles} neg={neg} subneg={subneg} products={products} "
        f"view_money={view_money} growth_pct={growth_pct}",
        flush=True,
    )
    try:
        result = svc.get_chart_data(
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            view_money=view_money,
            growth_pct=growth_pct,
        )
        print(f"[FORECAST ROUTER] chart-data OK keys={list(result.keys()) if isinstance(result, dict) else type(result)}", flush=True)
        return result
    except Exception as exc:
        _tb_str = _tb.format_exc()
        print(f"[FORECAST ROUTER] chart-data EXCEPTION: {exc}\n{_tb_str}", flush=True)
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
    import traceback as _tb
    print(
        f"[FORECAST ROUTER] client-table start_date={start_date} end_date={end_date} "
        f"profiles={profiles} neg={neg} subneg={subneg} products={products} "
        f"view_money={view_money} growth_pct={growth_pct} lab_products={lab_products}",
        flush=True,
    )
    try:
        result = svc.get_client_table(
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
        print(f"[FORECAST ROUTER] client-table OK rows={len(result.get('rows', [])) if isinstance(result, dict) else '?'}", flush=True)
        return result
    except Exception as exc:
        _tb_str = _tb.format_exc()
        print(f"[FORECAST ROUTER] client-table EXCEPTION: {exc}\n{_tb_str}", flush=True)
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
    import traceback as _tb
    print(
        f"[FORECAST ROUTER] treemap-data start_date={start_date} end_date={end_date} "
        f"profiles={profiles} neg={neg} subneg={subneg} products={products} "
        f"view_money={view_money} period_date={period_date}",
        flush=True,
    )
    try:
        result = svc.get_treemap_data(
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            view_money=view_money,
            period_date=period_date,
        )
        print(f"[FORECAST ROUTER] treemap-data OK ids={len(result.get('ids', [])) if isinstance(result, dict) else '?'}", flush=True)
        return result
    except Exception as exc:
        _tb_str = _tb.format_exc()
        print(f"[FORECAST ROUTER] treemap-data EXCEPTION: {exc}\n{_tb_str}", flush=True)
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
    try:
        existing = (
            db_session.query(Ticket)
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
            db_session.add(ticket)
            db_session.flush()
            is_new = True

        msg = TicketMessage(
            ticket_id=ticket.id,
            user_id=user.id,
            message=payload.message,
        )
        db_session.add(msg)
        db_session.commit()

        # Notificar a admins
        try:
            from web_comparativas.notifications_service import notify_admins
            _nombre = user.name or user.email.split("@")[0]
            _accion = "nueva consulta" if is_new else "nuevo comentario"
            notify_admins(
                db_session,
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
        db_session.rollback()
        logger.error("forecast-comment error: %s", exc, exc_info=True)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@router.get("/api/comments/summary", response_class=JSONResponse)
def forecast_api_summary(
    _request: Request,
    user: User = Depends(_require_user),
):
    """
    Retorna el resumen de tickets activos de Forecast para el usuario actual.
    Usado por el widget para mostrar el badge y el historial resumido.
    """
    try:
        tickets = (
            db_session.query(Ticket)
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
        db_session.rollback()
        logger.error("forecast-summary error: %s", exc, exc_info=True)
        return JSONResponse({"ok": False, "error": str(exc), "open_count": 0}, status_code=500)
