# web_comparativas/usage_service.py
import datetime as dt
from typing import Optional, Dict, Any

from .models import db_session, UsageEvent, User


def log_usage_event(
    *,
    user: Optional[User],
    action_type: str,
    section: Optional[str] = None,
    resource_id: Optional[str] = None,
    duration_ms: Optional[int] = None,
    extra_data: Optional[Dict[str, Any]] = None,
    request=None,
) -> None:
    """
    Registra un evento de uso simple.
    No lanza errores hacia arriba: si algo falla, se ignora silenciosamente.

    Parámetros típicos:
      - action_type: "page_view", "file_upload", "login", etc.
      - section: "home", "oportunidades_buscador", "seguimiento_usuarios", etc.
      - resource_id: id de algo relevante (archivo, proceso, etc.) si aplica
      - extra_data: dict con detalles (filtros, cantidad de filas, etc.)
    """
    if user is None:
        return

    try:
        s = db_session()

        ip = None
        ua = None
        if request is not None:
            try:
                ip = getattr(request.client, "host", None)
            except Exception:
                ip = None
            try:
                ua = request.headers.get("user-agent", "") if hasattr(request, "headers") else ""
            except Exception:
                ua = ""

        ev = UsageEvent(
            timestamp=dt.datetime.utcnow(),
            session_id="legacy",  # más adelante podemos manejar sesiones reales
            user_id=int(user.id),
            user_role=(user.role or "").strip().lower(),
            action_type=action_type,
            section=section,
            resource_id=str(resource_id) if resource_id is not None else None,
            duration_ms=duration_ms,
            extra_data=extra_data or {},
            ip=ip,
            user_agent=(ua[:1000] if ua else None),
        )

        s.add(ev)
        s.commit()
    except Exception:
        # No queremos romper la app por un problema de logging
        try:
            s.rollback()
        except Exception:
            pass
    finally:
        try:
            s.close()
        except Exception:
            pass
