import time
import logging
import asyncio
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.concurrency import run_in_threadpool

from web_comparativas.usage_service import log_usage_event

logger = logging.getLogger("wc.tracking")

class TrackingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Procesar request
        try:
            response = await call_next(request)
        except Exception as exc:
            # Si hay error, igual queremos loguear, pero re-raiseamos
            # (Aunque en un middleware a veces es mejor dejar pasar y que maneje ExceptionMiddleware)
            # Para simplificar, logueamos aqui "error" si podemos, o dejamos pasar.
            # Aqui solo re-lanzo para no interferir.
            raise exc

        # Calcular duración
        duration_ms = int((time.time() - start_time) * 1000)

        # Filtrar estáticos, favicon y endpoints que trackean por sí mismos
        path = request.url.path
        if path.startswith("/static") or path == "/favicon.ico":
            return response
        if path.startswith("/sic/api/track-event") or path.startswith("/sic/api/usage"):
            return response

        # Intentar obtener usuario del state (auth middleware corre antes)
        user = getattr(request.state, "user", None)
        
        # Solo logueamos si hay usuario (actividad autenticada)
        if user:
            # Determinar "sección" a ojo (rule-based)
            section = "otro"
            if path == "/":
                section = "home"
            elif path.startswith("/sic"):
                section = "sic"
                if "/usuarios" in path:
                    section = "sic_usuarios"
                elif "/helpdesk" in path:
                    section = "sic_helpdesk"
                elif "/tracking" in path:
                    section = "sic_tracking"
            elif path.startswith("/mercado-privado"):
                section = "mercado_privado"
                if "dimensiones" in path:
                    section = "mercado_privado_dimensiones"
            elif path.startswith("/mercado-publico"):
                section = "mercado_publico"
            
            # Action type
            action = "page_view"
            if request.method == "POST":
                action = "form_submit" # O "api_call" genérico
                if "upload" in path:
                    action = "file_upload"
            
            # Fire & Forget en threadpool para no bloquear el event loop principal si la BD es lenta o bloquea SQLite
            asyncio.create_task(
                run_in_threadpool(
                    log_usage_event,
                    user=user,
                    action_type=action,
                    section=section,
                    duration_ms=duration_ms,
                    request=request
                )
            )

        return response
