"""
middleware/tracking.py
──────────────────────
Captura automáticamente los eventos de navegación HTTP para todos los usuarios
autenticados. Diseño robusto:
  - Solo extrae primitivos (user_id, role, ip, ua) ANTES de lanzar el background task.
  - No pasa objetos SQLAlchemy ni el objeto `request` al task, evitando
    DetachedInstanceError y accesos a sockets ya cerrados.
  - Usa asyncio.create_task + run_in_threadpool (fire-and-forget no bloqueante).
  - Exclusiones explícitas para rutas estáticas, healthcheck, heartbeat y APIs internas.
"""

import asyncio
import logging
import re
import time

from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from web_comparativas.usage_service import log_usage_event_raw

logger = logging.getLogger("wc.tracking")


# ─── Detección de sección ───────────────────────────────────────────────────

def _detect_section(path: str) -> str:
    if path == "/":
        return "home"

    # ── S.I.C ────────────────────────────────────────────────────────────
    if path.startswith("/sic"):
        if "/password-resets" in path:
            return "sic_password_resets"
        if "/usuarios" in path:
            return "sic_usuarios"
        if "/helpdesk" in path:
            return "sic_helpdesk"
        if "/tracking" in path:
            return "sic_tracking"
        return "sic"

    # ── Mercado Privado ───────────────────────────────────────────────────
    if path.startswith("/mercado-privado"):
        if "dimensiones" in path or "oportunidades" in path:
            return "mercado_privado_dimensiones"
        if "reporte-perfiles" in path:
            return "reporte_perfiles"
        if "helpdesk" in path:
            return "mercado_privado"   # helpdesk privado → módulo privado
        return "mercado_privado"

    # ── Mercado Público ───────────────────────────────────────────────────
    if path.startswith("/mercado-publico"):
        if "web-comparativas" in path or "comparativas" in path:
            return "comparativa"
        if "oportunidades" in path:
            return "mercado_publico_oportunidades"
        if "reporte-perfiles" in path:
            return "reporte_perfiles"
        if "helpdesk" in path:
            return "mercado_publico"   # helpdesk público → módulo público
        return "mercado_publico"

    # ── Oportunidades ─────────────────────────────────────────────────────
    if path.startswith("/oportunidades"):
        if "buscador" in path:
            return "oportunidades_buscador"
        if "dimensiones" in path:
            return "oportunidades_dimensiones"
        return "oportunidades"

    # ── Cargas / Historial ────────────────────────────────────────────────
    if path.startswith("/cargas"):
        if "historial" in path:
            return "cargas_historial"
        return "cargas"

    # ── Otras fuentes externas ────────────────────────────────────────────
    if path.startswith("/otras-fuentes") or path.startswith("/otras-fuentes-externas"):
        return "cargas"

    # ── Tablero / Comparativa ─────────────────────────────────────────────
    if path.startswith("/tablero"):
        return "comparativa"

    # ── Forecast ──────────────────────────────────────────────────────────
    if path.startswith("/forecast"):
        return "forecast"

    # ── Dashboard ─────────────────────────────────────────────────────────
    if path.startswith("/dashboard"):
        return "dashboard"

    # ── Auth ──────────────────────────────────────────────────────────────
    if path.startswith("/login") or path.startswith("/logout"):
        return "auth"
    if path.startswith("/password") or path.startswith("/mi/password"):
        return "auth"

    return "otro"


_TECHNICAL_SEGMENTS = {
    "api", "status", "debug", "bootstrap", "filters", "kpis", "series",
    "results", "top-families", "geo", "process", "upload-csv",
}


def _clean_path(path: str) -> str:
    clean = (path or "/").split("?", 1)[0].split("#", 1)[0].strip().lower()
    if not clean.startswith("/"):
        clean = "/" + clean
    return clean.rstrip("/") or "/"


def _is_id_segment(segment: str) -> bool:
    return bool(
        re.fullmatch(r"\d+", segment)
        or re.fullmatch(r"[0-9a-f]{8,}(-[0-9a-f]{4,})*", segment)
    )


def _fallback_section_from_path(path: str) -> str:
    parts = [
        part.replace("-", "_")
        for part in _clean_path(path).strip("/").split("/")
        if part and not _is_id_segment(part)
    ]
    parts = [part for part in parts if part not in _TECHNICAL_SEGMENTS]
    if not parts:
        return "home"
    return "_".join(parts[:3])


def _detect_section(path: str) -> str:
    path = _clean_path(path)
    if path == "/":
        return "home"

    if path.startswith("/sic"):
        if "/api/usage" in path or "/api/track-event" in path:
            return "sic_tracking_api"
        if "/helpdesk/tickets" in path:
            return "sic_helpdesk_tickets"
        if "/api/tickets" in path or "/helpdesk" in path:
            return "sic_helpdesk"
        if "/password-resets" in path or "/api/password-resets" in path:
            return "sic_password_resets"
        if "/users" in path or "/usuarios" in path:
            return "sic_users"
        if "/tracking" in path:
            return "sic_tracking"
        return "sic_general"

    if path.startswith("/api/mercado-privado/dimensiones"):
        return "dimensionamiento"
    if path.startswith("/mercado-privado"):
        if "reporte-perfiles" in path:
            return "mercado_privado_reporte_perfiles"
        if "dimensiones" in path or "oportunidades" in path:
            return "dimensionamiento"
        if "comentarios" in path:
            return "comentarios"
        if "mi-password" in path:
            return "auth_password"
        if "helpdesk" in path:
            return "mercado_privado_helpdesk"
        if path != "/mercado-privado":
            return _fallback_section_from_path(path)
        return "mercado_privado_home"

    if path.startswith("/api/mercado-publico/perfiles"):
        return "mercado_publico_reporte_perfiles"
    if path.startswith("/api/mercado-publico"):
        if "lectura-pliegos" in path or "/pliegos" in path:
            return "lectura_pliegos"
        if "web-comparativas" in path or "comparativas" in path:
            return "comparativa_mercado"
        if "oportunidades" in path:
            return "mercado_publico_oportunidades"
        return "mercado_publico_home"
    if path.startswith("/mercado-publico"):
        if "lectura-pliegos" in path or "/pliegos" in path:
            return "lectura_pliegos"
        if "reporte-perfiles" in path:
            return "mercado_publico_reporte_perfiles"
        if "web-comparativas" in path or "comparativas" in path:
            return "comparativa_mercado"
        if "oportunidades" in path:
            return "mercado_publico_oportunidades"
        if "helpdesk" in path:
            return "mercado_publico_helpdesk"
        if path != "/mercado-publico":
            return _fallback_section_from_path(path)
        return "mercado_publico_home"

    if path.startswith("/api/oportunidades/dimensiones") or path.startswith("/oportunidades/dimensiones"):
        return "oportunidades_dimensiones"
    if path.startswith("/api/oportunidades/buscador") or path.startswith("/oportunidades/buscador"):
        return "oportunidades_buscador"
    if path.startswith("/oportunidades"):
        return "oportunidades"

    if path.startswith("/api/cargas"):
        return "cargas"
    if path.startswith("/cargas"):
        if "historial" in path:
            return "cargas_historial"
        if "nueva" in path:
            return "cargas_nueva"
        if "editar" in path:
            return "cargas_edicion"
        return "cargas"
    if path.startswith("/otras-fuentes") or path.startswith("/otras-fuentes-externas"):
        return "fuentes_externas"

    if path.startswith("/api/descargar-final"):
        return "descargas"
    if path.startswith("/api/tablero") or path.startswith("/tablero"):
        return "tablero_comparativa"
    if path.startswith("/api/views"):
        return "vistas_guardadas"
    if path.startswith("/api/presets"):
        return "vistas_guardadas"
    if path.startswith("/descargas"):
        return "descargas"
    if path.startswith("/reportes/proceso"):
        return "reporte_proceso"
    if path.startswith("/informes"):
        return "informes"

    if path.startswith("/forecast"):
        return "forecast"
    if path.startswith("/dashboard"):
        return "dashboard"
    if path.startswith("/markets") or path.startswith("/switch-market"):
        return "markets_home"

    if path.startswith("/admin/reset-solicitudes"):
        return "admin_password_resets"
    if path.startswith("/admin"):
        return "administracion"
    if path.startswith("/grupos"):
        return "grupos"
    if path.startswith("/api/notifications") or path.startswith("/notifications"):
        return "notificaciones"
    if path.startswith("/comentarios") or path.startswith("/api/comments"):
        return "comentarios"
    if path.startswith("/api/clientes"):
        return "clientes_api"

    if path.startswith("/login"):
        return "auth_login"
    if path.startswith("/logout"):
        return "auth_logout"
    if path.startswith("/password") or path.startswith("/mi/password"):
        return "auth_password"

    return _fallback_section_from_path(path)


_KNOWN_SECTION_KEYS = {
    "home", "sic_tracking_api", "sic_helpdesk", "sic_password_resets",
    "sic_users", "sic_tracking", "sic_general", "dimensionamiento",
    "reporte_perfiles", "mercado_publico_reporte_perfiles",
    "mercado_privado_reporte_perfiles", "comentarios", "auth_password",
    "mercado_privado_helpdesk", "mercado_privado_home", "lectura_pliegos",
    "comparativa_mercado", "mercado_publico_oportunidades",
    "mercado_publico_helpdesk", "mercado_publico_home",
    "oportunidades_dimensiones", "oportunidades_buscador", "oportunidades",
    "cargas_historial", "cargas_nueva", "cargas_edicion", "cargas",
    "fuentes_externas", "tablero_comparativa", "vistas_guardadas",
    "descargas", "reporte_proceso", "informes", "forecast", "dashboard",
    "markets_home", "admin_password_resets", "administracion", "grupos",
    "notificaciones", "clientes_api", "auth_login", "auth_logout",
}


def _detect_action(method: str, path: str) -> str:
    if method == "POST":
        if "upload" in path:
            return "file_upload"
        return "form_submit"
    return "page_view"


# ─── Rutas excluidas del tracking ───────────────────────────────────────────

_EXCLUDED_PREFIXES = (
    "/static",
    "/favicon.ico",
    "/healthz",
    "/ping",
    "/notifications/unread-count",  # badge poll; no cuenta como page_view
    "/api/heartbeat",          # tiene su propia lógica de tracking
    "/sic/api/track-event",    # se auto-registra
    "/sic/api/usage",          # APIs internas del módulo de tracking
)


class TrackingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        try:
            response = await call_next(request)
        except Exception as exc:
            raise exc

        duration_ms = int((time.time() - start_time) * 1000)
        path = request.url.path

        # Filtrar rutas excluidas
        for prefix in _EXCLUDED_PREFIXES:
            if path.startswith(prefix):
                return response

        user = getattr(request.state, "user", None)
        if not user:
            return response

        # ── Extraer PRIMITIVOS ahora (antes del task asíncrono) ──────────────
        try:
            user_id = int(user.id)
            user_role = (user.role or "").strip().lower()
        except Exception:
            return response

        ip: str | None = None
        try:
            ip = getattr(request.client, "host", None)
        except Exception:
            pass

        ua: str = ""
        try:
            ua = request.headers.get("user-agent", "") or ""
        except Exception:
            pass

        section = _detect_section(path)
        action = _detect_action(request.method, path)
        if section not in _KNOWN_SECTION_KEYS:
            try:
                logger.warning(
                    "Tracking section inferred path=%s method=%s referer=%s user_id=%s role=%s section=%s",
                    path,
                    request.method,
                    request.headers.get("referer", ""),
                    user_id,
                    user_role,
                    section,
                )
            except Exception:
                pass

        # ── Fire-and-forget (no bloquea el event loop principal) ────────────
        asyncio.create_task(
            run_in_threadpool(
                log_usage_event_raw,
                user_id=user_id,
                user_role=user_role,
                action_type=action,
                section=section,
                duration_ms=duration_ms,
                ip=ip,
                user_agent=ua[:1000] if ua else None,
            )
        )

        return response
