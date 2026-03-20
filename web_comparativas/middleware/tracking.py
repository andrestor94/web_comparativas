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
    if path.startswith("/sic"):
        if "/usuarios" in path:
            return "sic_usuarios"
        if "/helpdesk" in path:
            return "sic_helpdesk"
        if "/tracking" in path:
            return "sic_tracking"
        return "sic"
    if path.startswith("/mercado-privado"):
        if "dimensiones" in path:
            return "mercado_privado_dimensiones"
        return "mercado_privado"
    if path.startswith("/mercado-publico"):
        return "mercado_publico"
    if path.startswith("/login") or path.startswith("/logout"):
        return "auth"
    return "otro"


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
