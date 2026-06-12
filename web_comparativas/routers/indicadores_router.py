"""
Router para el módulo Indicadores Comerciales.
Prefijo: /indicadores-comerciales

Dashboards:
  GET /indicadores-comerciales/rentabilidad-negativa
  GET /indicadores-comerciales/informes-laboratorio
  GET /indicadores-comerciales/inflacion

API compartida:
  GET /indicadores-comerciales/api/health

API Rentabilidad Negativa:
  GET /indicadores-comerciales/api/rentabilidad/metadata
  GET /indicadores-comerciales/api/rentabilidad/resumen
  GET /indicadores-comerciales/api/rentabilidad/detalle

API Informes de Laboratorio:
  GET /indicadores-comerciales/api/laboratorios/metadata
  GET /indicadores-comerciales/api/laboratorios/resumen
  GET /indicadores-comerciales/api/laboratorios/detalle

API Inflación PVP:
  GET /indicadores-comerciales/api/inflacion/resumen
  GET /indicadores-comerciales/api/inflacion/productos
  GET /indicadores-comerciales/api/inflacion/laboratorios
  GET /indicadores-comerciales/api/inflacion/evolucion
  GET /indicadores-comerciales/api/indec/ipc
  GET /indicadores-comerciales/api/indec/ipc/evolucion

Auth: idéntica al resto de SIEM — lee request.state.user.
"""

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from web_comparativas.models import User, RENDER_MODE
from web_comparativas.policy import require_module, can_access as _can_access_tpl, can_switch_market as _can_switch_market_tpl

logger = logging.getLogger("wc.indicadores")

_MSG_SQL_UNAVAILABLE = (
    "No se pudo conectar con la fuente de datos de Indicadores Comerciales. "
    "Verificá la conexión de red, VPN o disponibilidad de SQL Server y volvé a intentar."
)

_PATH_PATTERN = re.compile(r"[A-Za-z]:\\[^\s'\"]+|/[^\s'\"]{5,}")


def _safe_error(exc: Exception) -> str:
    msg = str(exc)
    if "timeout" in msg.lower() or "timed out" in msg.lower():
        return _MSG_SQL_UNAVAILABLE
    if any(k in msg.lower() for k in ("conexión", "connection", "etl_data", "fusion", "sql server", "sqlclient")):
        return _MSG_SQL_UNAVAILABLE
    cleaned = _PATH_PATTERN.sub("[ruta]", msg)
    return cleaned[:300]


_BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(_BASE_DIR / "templates"))
templates.env.globals["can_access"] = _can_access_tpl
templates.env.globals["can_switch_market"] = _can_switch_market_tpl

# ─── Gate por rol en producción ──────────────────────────────────────────────

def _require_admin_en_prod(request: Request):
    """En producción (RENDER_MODE), exige rol admin. En local no restringe (deja
    pasar; los require_module por ruta siguen aplicando)."""
    if RENDER_MODE:
        user = getattr(request.state, "user", None)
        if not user or not user.is_admin():
            raise HTTPException(status_code=403, detail="Indicadores Comerciales está disponible solo para administradores.")
    return None  # no devuelve user; es un guard aditivo, las rutas ya tienen el suyo


# Guard a NIVEL router: cubre las 24 rutas de una sola vez, incluido el redirect
# raíz ("" y "/") que no tiene Depends propio. Se SUMA a los require_module(...)
# por ruta, que conservan la granularidad por dashboard.
router = APIRouter(prefix="/indicadores-comerciales", tags=["indicadores"],
                   dependencies=[Depends(_require_admin_en_prod)])


# ─── Auth helper ─────────────────────────────────────────────────────────────

def _require_user(request: Request) -> User:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="No autenticado")
    return user


# ─── Date helpers ─────────────────────────────────────────────────────────────

def _today_str() -> str:
    return date.today().isoformat()


def _year_start_str() -> str:
    return date(date.today().year, 1, 1).isoformat()


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


# ═══════════════════════════════════════════════════════════════════════════════
# PÁGINAS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("", response_class=RedirectResponse, include_in_schema=False)
@router.get("/", response_class=RedirectResponse, include_in_schema=False)
def indicadores_redirect():
    # La raíz del módulo aterriza en el Home (antes iba directo a rentabilidad-negativa).
    return RedirectResponse("/indicadores-comerciales/home", status_code=302)


@router.get("/home", response_class=HTMLResponse)
def indicadores_home(request: Request, user: User = Depends(require_module("indicadores_comerciales.home"))):
    """Home del módulo: 9 KPIs globales (3 por dashboard), últimos 12 meses.

    Cada bloque de servicio va en su propio try/except: si un resumen falla,
    sus KPIs quedan en None (el template muestra '—') y la página renderiza
    igual con los demás. Solo LEE: no toca corridas ni datos.
    """
    request.session["market_context"] = "indicadores"
    hoy = date.today()
    desde = date(hoy.year - 1, hoy.month, 1)  # últimos 12 meses, desde el 1° del mes

    kpis = {
        "perdida_total": None, "transacciones": None, "renta_promedio": None,
        "inflacion_indice": None, "inflacion_ponderada": None, "productos_comparables": None,
        "total_unidades": None, "promedio_mensual": None, "cantidad_laboratorios": None,
    }

    try:
        from web_comparativas.indicadores_service import get_summary as _renta_summary
        s = _renta_summary(desde=desde, hasta=hoy)
        kpis["perdida_total"] = s.get("utilidad_total")
        kpis["transacciones"] = s.get("total_transacciones")
        kpis["renta_promedio"] = s.get("rentabilidad_promedio")
    except Exception:
        logger.exception("home: resumen de rentabilidad no disponible")

    try:
        from web_comparativas.indicadores_inflacion_service import get_resumen as _infl_resumen
        s = _infl_resumen(desde=desde, hasta=hoy)
        kpis["inflacion_indice"] = s.get("inflacion_pvp_indice")
        kpis["inflacion_ponderada"] = s.get("inflacion_pvp_ponderada_facturacion")
        kpis["productos_comparables"] = s.get("productos_comparables")
    except Exception:
        logger.exception("home: resumen de inflación no disponible")

    try:
        from web_comparativas.indicadores_laboratorios_service import get_summary as _lab_summary
        s = _lab_summary(desde=desde, hasta=hoy)
        kpis["total_unidades"] = s.get("total_unidades")
        kpis["promedio_mensual"] = s.get("promedio_mensual")
        kpis["cantidad_laboratorios"] = s.get("cantidad_laboratorios")
    except Exception:
        logger.exception("home: resumen de laboratorios no disponible")

    return templates.TemplateResponse(
        "indicadores/home.html",
        {
            "request": request,
            "user": user,
            "market_context": "indicadores",
            "kpis": kpis,
            "rango_desde": desde.isoformat(),
            "rango_hasta": hoy.isoformat(),
        },
    )


@router.get("/rentabilidad-negativa", response_class=HTMLResponse)
def indicadores_rentabilidad(request: Request, user: User = Depends(require_module("indicadores_comerciales.rentabilidad_negativa"))):
    request.session["market_context"] = "indicadores"
    from web_comparativas.indicadores_db import is_available
    return templates.TemplateResponse(
        "indicadores/rentabilidad.html",
        {
            "request": request,
            "user": user,
            "market_context": "indicadores",
            "sql_available": is_available(),
            "default_desde": _year_start_str(),
            "default_hasta": _today_str(),
            "active_dashboard": "rentabilidad",
        },
    )


@router.get("/informes-laboratorio", response_class=HTMLResponse)
def indicadores_laboratorios(request: Request, user: User = Depends(require_module("indicadores_comerciales.informes_laboratorio"))):
    request.session["market_context"] = "indicadores"
    from web_comparativas.indicadores_db import is_available
    return templates.TemplateResponse(
        "indicadores/laboratorios.html",
        {
            "request": request,
            "user": user,
            "market_context": "indicadores",
            "sql_available": is_available(),
            "default_desde": _year_start_str(),
            "default_hasta": _today_str(),
            "active_dashboard": "laboratorios",
        },
    )


@router.get("/inflacion", response_class=HTMLResponse)
def indicadores_inflacion(request: Request, user: User = Depends(require_module("indicadores_comerciales.inflacion"))):
    request.session["market_context"] = "indicadores"
    from web_comparativas.indicadores_db import is_available
    return templates.TemplateResponse(
        "indicadores/inflacion.html",
        {
            "request": request,
            "user": user,
            "market_context": "indicadores",
            "sql_available": is_available(),
            "default_desde": _year_start_str(),
            "default_hasta": _today_str(),
            "active_dashboard": "inflacion",
        },
    )


# ═══════════════════════════════════════════════════════════════════════════════
# API SHARED — Health (todos los dashboards)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/api/health")
def api_health(request: Request, _user: User = Depends(require_module("indicadores_comerciales"))):
    try:
        from web_comparativas.indicadores_service import get_health
        return JSONResponse(get_health())
    except Exception as exc:
        logger.error("indicadores health error: %s", exc)
        return JSONResponse({"status": "error", "etl": False, "fusion": False, "error": _safe_error(exc)})


# ═══════════════════════════════════════════════════════════════════════════════
# API RENTABILIDAD NEGATIVA
# ═══════════════════════════════════════════════════════════════════════════════

def _rentabilidad_params(
    desde: Optional[str] = Query(default=None),
    hasta: Optional[str] = Query(default=None),
    laboratorio: Optional[str] = Query(default=None),
    familia: Optional[str] = Query(default=None),
    cliente: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    cadneg: Optional[str] = Query(default=None),
    modo: str = Query(default="detalle"),
) -> dict:
    return {
        "desde": _parse_date(desde or _year_start_str()),
        "hasta": _parse_date(hasta or _today_str()),
        "laboratorio": laboratorio or None,
        "familia": familia or None,
        "cliente": cliente or None,
        "search": search or None,
        "cadneg": cadneg or None,
        "modo": modo,
    }


@router.get("/api/rentabilidad/health")
def api_rentabilidad_health(request: Request, _user: User = Depends(require_module("indicadores_comerciales"))):
    return api_health(request, _user)


@router.get("/api/rentabilidad/metadata")
def api_rentabilidad_metadata(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                               desde: Optional[str] = Query(default=None),
                               hasta: Optional[str] = Query(default=None),
                               laboratorio: Optional[str] = Query(default=None),
                               familia: Optional[str] = Query(default=None),
                               cliente: Optional[str] = Query(default=None),
                               search: Optional[str] = Query(default=None),
                               cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_service import get_metadata
        data = get_metadata(
            desde=_parse_date(desde or _year_start_str()),
            hasta=_parse_date(hasta or _today_str()),
            laboratorio=laboratorio or None,
            familia=familia or None,
            cliente=cliente or None,
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(data)
    except Exception as exc:
        logger.error("rentabilidad metadata error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/rentabilidad/resumen")
def api_rentabilidad_resumen(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                              desde: Optional[str] = Query(default=None),
                              hasta: Optional[str] = Query(default=None),
                              laboratorio: Optional[str] = Query(default=None),
                              familia: Optional[str] = Query(default=None),
                              cliente: Optional[str] = Query(default=None),
                              search: Optional[str] = Query(default=None),
                              cadneg: Optional[str] = Query(default=None),
                              modo: str = Query(default="detalle")):
    try:
        from web_comparativas.indicadores_service import get_summary
        data = get_summary(
            desde=_parse_date(desde or _year_start_str()),
            hasta=_parse_date(hasta or _today_str()),
            laboratorio=laboratorio or None,
            familia=familia or None,
            cliente=cliente or None,
            search=search or None,
            cadneg=cadneg or None,
            modo=modo,
        )
        return JSONResponse(data)
    except Exception as exc:
        logger.error("rentabilidad resumen error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/rentabilidad/detalle")
def api_rentabilidad_detalle(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                              desde: Optional[str] = Query(default=None),
                              hasta: Optional[str] = Query(default=None),
                              laboratorio: Optional[str] = Query(default=None),
                              familia: Optional[str] = Query(default=None),
                              cliente: Optional[str] = Query(default=None),
                              search: Optional[str] = Query(default=None),
                              cadneg: Optional[str] = Query(default=None),
                              modo: str = Query(default="detalle")):
    try:
        from web_comparativas.indicadores_service import get_detail
        rows = get_detail(
            desde=_parse_date(desde or _year_start_str()),
            hasta=_parse_date(hasta or _today_str()),
            laboratorio=laboratorio or None,
            familia=familia or None,
            cliente=cliente or None,
            search=search or None,
            cadneg=cadneg or None,
            modo=modo,
        )
        return JSONResponse(rows[:1200])
    except Exception as exc:
        logger.error("rentabilidad detalle error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


# ═══════════════════════════════════════════════════════════════════════════════
# API INFORMES DE LABORATORIO
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/api/laboratorios/health")
def api_laboratorios_health(request: Request, _user: User = Depends(require_module("indicadores_comerciales"))):
    try:
        from web_comparativas.indicadores_laboratorios_service import get_health
        return JSONResponse(get_health())
    except Exception as exc:
        logger.error("laboratorios health error: %s", exc)
        return JSONResponse({"status": "error", "etl": False, "fusion": False, "error": _safe_error(exc)})


@router.get("/api/laboratorios/metadata")
def api_laboratorios_metadata(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                               desde: Optional[str] = Query(default=None),
                               hasta: Optional[str] = Query(default=None),
                               laboratorio: Optional[str] = Query(default=None),
                               familia: Optional[str] = Query(default=None),
                               cliente: Optional[str] = Query(default=None),
                               search: Optional[str] = Query(default=None),
                               cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_laboratorios_service import get_metadata
        data = get_metadata(
            desde=_parse_date(desde or _year_start_str()),
            hasta=_parse_date(hasta or _today_str()),
            laboratorio=laboratorio or None,
            familia=familia or None,
            cliente=cliente or None,
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(data)
    except Exception as exc:
        logger.error("laboratorios metadata error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/laboratorios/resumen")
def api_laboratorios_resumen(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                              desde: Optional[str] = Query(default=None),
                              hasta: Optional[str] = Query(default=None),
                              laboratorio: Optional[str] = Query(default=None),
                              familia: Optional[str] = Query(default=None),
                              cliente: Optional[str] = Query(default=None),
                              search: Optional[str] = Query(default=None),
                              cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_laboratorios_service import get_summary
        data = get_summary(
            desde=_parse_date(desde or _year_start_str()),
            hasta=_parse_date(hasta or _today_str()),
            laboratorio=laboratorio or None,
            familia=familia or None,
            cliente=cliente or None,
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(data)
    except Exception as exc:
        logger.error("laboratorios resumen error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/laboratorios/detalle")
def api_laboratorios_detalle(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                              desde: Optional[str] = Query(default=None),
                              hasta: Optional[str] = Query(default=None),
                              laboratorio: Optional[str] = Query(default=None),
                              familia: Optional[str] = Query(default=None),
                              cliente: Optional[str] = Query(default=None),
                              search: Optional[str] = Query(default=None),
                              cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_laboratorios_service import get_detail
        rows = get_detail(
            desde=_parse_date(desde or _year_start_str()),
            hasta=_parse_date(hasta or _today_str()),
            laboratorio=laboratorio or None,
            familia=familia or None,
            cliente=cliente or None,
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(rows[:2000])
    except Exception as exc:
        logger.error("laboratorios detalle error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


# ═══════════════════════════════════════════════════════════════════════════════
# API INFLACIÓN PVP
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/api/inflacion/health")
def api_inflacion_health(request: Request, _user: User = Depends(require_module("indicadores_comerciales"))):
    try:
        from web_comparativas.indicadores_inflacion_service import get_health
        return JSONResponse(get_health())
    except Exception as exc:
        logger.error("inflacion health error: %s", exc)
        return JSONResponse({"status": "error", "fusion": False, "etl": False, "error": _safe_error(exc)})


@router.get("/api/inflacion/resumen")
def api_inflacion_resumen(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                           desde: str = Query(...),
                           hasta: str = Query(...),
                           laboratorio: Optional[str] = Query(default=None),
                           search: Optional[str] = Query(default=None),
                           cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_inflacion_service import get_resumen
        data = get_resumen(
            desde=_parse_date(desde),
            hasta=_parse_date(hasta),
            laboratorio=laboratorio or None,
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(data)
    except Exception as exc:
        logger.error("inflacion resumen error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/inflacion/productos")
def api_inflacion_productos(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                             desde: str = Query(...),
                             hasta: str = Query(...),
                             laboratorio: Optional[str] = Query(default=None),
                             search: Optional[str] = Query(default=None),
                             cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_inflacion_service import get_productos
        data = get_productos(
            desde=_parse_date(desde),
            hasta=_parse_date(hasta),
            laboratorio=laboratorio or None,
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(data[:3000])
    except Exception as exc:
        logger.error("inflacion productos error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/inflacion/laboratorios")
def api_inflacion_laboratorios(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                                desde: str = Query(...),
                                hasta: str = Query(...),
                                search: Optional[str] = Query(default=None),
                                cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_inflacion_service import get_laboratorios
        data = get_laboratorios(
            desde=_parse_date(desde),
            hasta=_parse_date(hasta),
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(data)
    except Exception as exc:
        logger.error("inflacion laboratorios error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/inflacion/evolucion")
def api_inflacion_evolucion(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                             desde: str = Query(...),
                             hasta: str = Query(...),
                             laboratorio: Optional[str] = Query(default=None),
                             search: Optional[str] = Query(default=None),
                             cadneg: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_inflacion_service import get_evolucion
        data = get_evolucion(
            desde=_parse_date(desde),
            hasta=_parse_date(hasta),
            laboratorio=laboratorio or None,
            search=search or None,
            cadneg=cadneg or None,
        )
        return JSONResponse(data)
    except Exception as exc:
        logger.error("inflacion evolucion error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=_safe_error(exc))


@router.get("/api/indec/ipc")
def api_indec_ipc(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                  desde: Optional[str] = Query(default=None),
                  hasta: Optional[str] = Query(default=None)):
    try:
        from web_comparativas.indicadores_inflacion_service import get_indec_ipc
        desde_date = _parse_date(desde) if desde else None
        hasta_date = _parse_date(hasta) if hasta else None
        data = get_indec_ipc(desde=desde_date, hasta=hasta_date)
        return JSONResponse(data)
    except Exception as exc:
        logger.error("indec ipc error: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error al consultar INDEC: {str(exc)[:200]}")


@router.get("/api/indec/ipc/evolucion")
def api_indec_ipc_evolucion(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                             desde: str = Query(...),
                             hasta: str = Query(...)):
    try:
        from web_comparativas.indicadores_inflacion_service import get_indec_ipc_evolucion
        data = get_indec_ipc_evolucion(desde=_parse_date(desde), hasta=_parse_date(hasta))
        return JSONResponse(data)
    except Exception as exc:
        logger.error("indec ipc evolucion error: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error al consultar INDEC: {str(exc)[:200]}")


# ─── Backward compat: rutas antiguas redirigen a rentabilidad ────────────────
# (los endpoints /api/metadata, /api/resumen, /api/detalle del router anterior)

@router.get("/api/metadata")
def _compat_metadata(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                     desde: Optional[str] = Query(default=None),
                     hasta: Optional[str] = Query(default=None),
                     laboratorio: Optional[str] = Query(default=None),
                     familia: Optional[str] = Query(default=None),
                     cliente: Optional[str] = Query(default=None),
                     search: Optional[str] = Query(default=None),
                     cadneg: Optional[str] = Query(default=None)):
    return api_rentabilidad_metadata(request, _user, desde, hasta, laboratorio, familia, cliente, search, cadneg)


@router.get("/api/resumen")
def _compat_resumen(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                    desde: Optional[str] = Query(default=None),
                    hasta: Optional[str] = Query(default=None),
                    laboratorio: Optional[str] = Query(default=None),
                    familia: Optional[str] = Query(default=None),
                    cliente: Optional[str] = Query(default=None),
                    search: Optional[str] = Query(default=None),
                    cadneg: Optional[str] = Query(default=None),
                    modo: str = Query(default="detalle")):
    return api_rentabilidad_resumen(request, _user, desde, hasta, laboratorio, familia, cliente, search, cadneg, modo)


@router.get("/api/detalle")
def _compat_detalle(request: Request, _user: User = Depends(require_module("indicadores_comerciales")),
                    desde: Optional[str] = Query(default=None),
                    hasta: Optional[str] = Query(default=None),
                    laboratorio: Optional[str] = Query(default=None),
                    familia: Optional[str] = Query(default=None),
                    cliente: Optional[str] = Query(default=None),
                    search: Optional[str] = Query(default=None),
                    cadneg: Optional[str] = Query(default=None),
                    modo: str = Query(default="detalle")):
    return api_rentabilidad_detalle(request, _user, desde, hasta, laboratorio, familia, cliente, search, cadneg, modo)
