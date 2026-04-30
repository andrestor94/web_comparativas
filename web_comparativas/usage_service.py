# web_comparativas/usage_service.py
import datetime as dt
import logging
import re
from collections import Counter, defaultdict
from typing import Optional, Dict, Any, List, Tuple, Set

from sqlalchemy import func

from .models import db_session, UsageEvent, User, Group, GroupMember
from .visibility_service import get_visible_user_ids as visible_user_ids  # fuente única de verdad

logger = logging.getLogger("wc.usage")
_UNMAPPED_SECTION_LOGGED: Set[str] = set()


# ======================================================================
# Helpers internos
# ======================================================================

def _parse_date_str(s: str | None) -> Optional[dt.date]:
    """
    Acepta formatos:
      - 'YYYY-MM-DD'
      - 'dd/mm/YYYY'
    Devuelve date o None si no matchea.
    """
    if not s:
        return None
    txt = str(s).strip()
    if not txt:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    return None


def _default_date_range(date_from: str | None, date_to: str | None) -> Tuple[dt.date, dt.date]:
    """
    Si el usuario no manda fechas, usamos desde 2024-01-01 hasta hoy.
    """
    today = dt.date.today()
    d_from = _parse_date_str(date_from)
    d_to = _parse_date_str(date_to)

    if d_from and d_to:
        if d_from > d_to:
            d_from, d_to = d_to, d_from
        return d_from, d_to

    if d_from and not d_to:
        d_to = min(today, d_from + dt.timedelta(days=30))
        return d_from, d_to

    if d_to and not d_from:
        d_from = d_to - dt.timedelta(days=30)
        return d_from, d_to

    # ninguno: histórico completo desde inicio del proyecto
    d_to = today
    d_from = dt.date(2024, 1, 1)
    return d_from, d_to


def _map_section_name(raw: str) -> str:
    original = (raw or "").strip()
    mapping = {
        # Inicio / auth
        "home":                         "Inicio",
        "auth":                         "Inicio de Sesión",
        # S.I.C.
        "sic":                          "S.I.C. General",
        "sic_usuarios":                 "Gestión de Usuarios",
        "sic_helpdesk":                 "Mesa de Ayuda",
        "sic_tracking":                 "Seguimiento de Usuarios",
        "sic_tracking_api":             "API Tracking (Interno)",
        "sic_password_resets":          "Gestión de Contraseñas",
        # Mercado Público
        "mercado_publico":              "Mercado Público",
        "mercado_publico_oportunidades":"Oportunidades — Público",
        # Mercado Privado
        "mercado_privado":              "Mercado Privado",
        "mercado_privado_dimensiones":  "Dimensionamiento",
        # Oportunidades
        "oportunidades":                "Oportunidades",
        "oportunidades_buscador":       "Buscador de Oportunidades",
        "oportunidades_dimensiones":    "Análisis de Dimensiones",
        # Cargas / Historial
        "cargas":                       "Cargas",
        "cargas_historial":             "Cargas — Historial",
        # Comparativa
        "comparativa":                  "Comparativa de Mercado",
        # Perfiles
        "reporte_perfiles":             "Reporte de Perfiles",
        # Forecast
        "forecast":                     "Proyecciones y Forecast",
        # Dashboard
        "dashboard":                    "Panel Principal",
        # Misceláneos
        "live_users_dashboard":         "Panel Live",
        "otro":                         "",
        "":                             "Sin Sección",
    }
    if original in mapping:
        return mapping[original]
    return original.replace("_", " ").title()


def _humanize_section_key(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    text = text.split("?", 1)[0].split("#", 1)[0].strip("/")
    text = re.sub(r"\b\d+\b", "", text)
    text = re.sub(r"[0-9a-f]{8,}(-[0-9a-f]{4,})*", "", text, flags=re.I)
    text = text.replace("-", "_").replace("/", "_")
    words = [word.lower() for word in text.split("_") if word and word.lower() not in {"api"}]
    if not words:
        return ""
    phrases = {
        "analisis_dimensiones": "Analisis de Dimensiones",
        "fuentes_externas": "Fuentes Externas",
        "helpdesk_tickets": "Mesa de Ayuda / Tickets",
        "users": "Usuarios",
        "usuarios": "Usuarios",
        "password_resets": "Reseteo de Contrasenas",
        "reporte_perfiles": "Reporte de Perfiles",
        "mercado_publico_reporte_perfiles": "Reporte de Perfiles",
        "mercado_privado_reporte_perfiles": "Reporte de Perfiles",
        "cargas_historial": "Cargas Historial",
        "oportunidades_buscador": "Buscador de Oportunidades",
    }
    joined = "_".join(words)
    if joined in phrases:
        return phrases[joined]
    for size in range(min(3, len(words)), 0, -1):
        suffix = "_".join(words[-size:])
        if suffix in phrases:
            return phrases[suffix]
    acronyms = {"sic": "S.I.C", "siem": "SIEM", "ia": "IA"}
    display_words = words[-2:] if len(words) > 2 else words
    return " ".join(acronyms.get(word, word.capitalize()) for word in display_words)


def _map_section_name(raw: str) -> str:
    original = (raw or "").strip()
    key = original.lower().replace("-", "_").replace("/", "_").strip("_")
    key = re.sub(r"_+", "_", key)

    generic_aliases = {
        "otro", "otros", "sin_identificar", "sin_clasificar", "sin clasificar",
        "sin identificar", "unknown", "undefined", "n/a", "no identificado",
    }
    if key in generic_aliases:
        return ""

    mapping = {
        "home": "Inicio",
        "dashboard": "Dashboard",
        "markets": "Centro de Mercados",
        "markets_home": "Centro de Mercados",
        "sic": "S.I.C General",
        "sic_general": "S.I.C General",
        "sic_home": "S.I.C General",
        "sic_tracking": "Seguimiento de Usuarios",
        "sic_tracking_api": "API Tracking Interno",
        "sic_helpdesk": "Mesa de Ayuda",
        "sic_helpdesk_tickets": "Mesa de Ayuda / Tickets",
        "sic_users": "Usuarios",
        "sic_usuarios": "Usuarios",
        "sic_password_resets": "Reseteo de Contrasenas",
        "admin_password_resets": "Administracion de Reseteos",
        "administracion": "Administracion",
        "grupos": "Grupos",
        "notificaciones": "Notificaciones",
        "mercado_publico": "Mercado Publico Home",
        "mercado_publico_home": "Mercado Publico Home",
        "mercado_publico_helpdesk": "Mesa de Ayuda Mercado Publico",
        "mercado_publico_oportunidades": "Oportunidades",
        "mercado_publico_buscador": "Buscador Mercado Publico",
        "mercado_publico_dimensiones": "Dimensionamiento Mercado Publico",
        "mercado_publico_analisis_dimensiones": "Analisis de Dimensiones",
        "mercado_publico_fuentes_externas": "Fuentes Externas",
        "mercado_privado": "Mercado Privado",
        "mercado_privado_home": "Mercado Privado Home",
        "mercado_privado_helpdesk": "Mesa de Ayuda Mercado Privado",
        "mercado_privado_dimensiones": "Dimensionamiento",
        "dimensionamiento": "Dimensionamiento",
        "oportunidades": "Oportunidades",
        "oportunidades_buscador": "Buscador de Oportunidades",
        "oportunidades_dimensiones": "Dimensiones de Oportunidades",
        "lectura_pliegos": "Lectura de Pliegos",
        "pliegos": "Lectura de Pliegos",
        "pliego_widget": "Visor de Pliegos",
        "pliego_detalle": "Detalle de Pliego",
        "reporte_perfiles": "Reporte de Perfiles",
        "mercado_publico_reporte_perfiles": "Reporte de Perfiles",
        "mercado_privado_reporte_perfiles": "Reporte de Perfiles",
        "perfiles": "Reporte de Perfiles",
        "comparativa": "Comparativa de Mercado",
        "comparativa_mercado": "Comparativa de Mercado",
        "web_comparativas": "Comparativa de Mercado",
        "tablero_comparativa": "Tablero de Comparativa",
        "vistas_guardadas": "Vistas Guardadas",
        "cargas": "Cargas",
        "cargas_nueva": "Nueva Carga",
        "cargas_edicion": "Edicion de Carga",
        "cargas_historial": "Cargas Historial",
        "fuentes_externas": "Fuentes Externas",
        "descargas": "Descargas",
        "reporte_proceso": "Reporte de Proceso",
        "informes": "Informes",
        "forecast": "Forecast",
        "forecast_widget": "Panel de Forecast",
        "auth": "Inicio de Sesion",
        "auth_login": "Inicio de Sesion",
        "auth_logout": "Cierre de Sesion",
        "auth_password": "Gestion de Contrasena",
        "comentarios": "Comentarios",
        "clientes_api": "Consulta de Clientes",
        "live_users_dashboard": "Panel Live",
        "": "Inicio",
    }
    if key in mapping:
        return mapping[key]
    label = _humanize_section_key(original)
    if label and key not in _UNMAPPED_SECTION_LOGGED:
        _UNMAPPED_SECTION_LOGGED.add(key)
        logger.warning("[tracking] ruta sin mapping explicito: %s -> %s", original, label)
    return label


def _map_action_label(action_type: str) -> str:
    """Nombre legible para un tipo de acción."""
    labels = {
        "login": "Inició sesión",
        "page_view": "Navegó a",
        "file_upload": "Cargó archivo",
        "search": "Realizó búsqueda",
        "export": "Exportó resultados",
        "heartbeat": "Señal de presencia",
        "form_submit": "Envió formulario",
        "api_call": "Llamada API",
        "admin_password_reset": "Reset de contraseña",
        "module_visit": "Visitó módulo",
    }
    at = (action_type or "").lower()
    return labels.get(at, at.replace("_", " ").title() if at else "Actividad")


def _get_activity_category(action_type: str) -> str:
    """Categoría de actividad para el badge en monitoreo en vivo."""
    at = (action_type or "").lower()
    if at == "file_upload":
        return "cargando"
    if at == "search":
        return "buscando"
    if at == "export":
        return "exportando"
    if at in ("page_view", "module_visit"):
        return "navegando"
    if at == "form_submit":
        return "editando"
    if at == "login":
        return "ingresando"
    return "activo"


# ======================================================================
# Registro de eventos
# ======================================================================

def log_usage_event_raw(
    *,
    user_id: int,
    user_role: str,
    action_type: str,
    section: Optional[str] = None,
    duration_ms: Optional[int] = None,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> None:
    """
    Versión fire-and-forget segura de log_usage_event: recibe solo primitivos,
    sin objetos SQLAlchemy ni request. Diseñada para ser llamada desde el
    TrackingMiddleware en un background task donde el objeto User/Request
    puede estar detached o fuera de scope.
    """
    try:
        # Actualizar presencia en memoria (no requiere DB)
        update_online_presence(user_id, section or "", True, action_type)

        # Heartbeats: muestrear para no saturar la DB
        if action_type == "heartbeat" and not _should_persist_heartbeat(
            user_id, section or "", True
        ):
            return

        from .models import SessionLocal
        s = SessionLocal()
        ev = UsageEvent(
            timestamp=dt.datetime.utcnow(),
            session_id="legacy",
            user_id=user_id,
            user_role=user_role,
            action_type=action_type,
            section=section,
            duration_ms=duration_ms,
            extra_data={},
            ip=ip,
            user_agent=(user_agent[:1000] if user_agent else None),
        )
        s.add(ev)
        s.commit()
        print(
            f"[TRACKING] uid={user_id} role={user_role} action={action_type} section={section!r}",
            flush=True,
        )
    except Exception as exc:
        print(f"[TRACKING] Error logging event for uid={user_id}: {exc}", flush=True)
        try:
            s.rollback()
        except Exception:
            pass
    finally:
        try:
            s.close()
        except Exception:
            pass


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
    """
    if user is None:
        return

    s = None
    owns_session = False

    try:
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

        # Actualizar presencia en vivo (Memoria)
        is_active = extra_data.get("is_active", True) if extra_data else True
        update_online_presence(int(user.id), section or "", is_active, action_type)

        # Persistimos heartbeats de forma muestreada para no depender solo de memoria.
        if action_type == "heartbeat" and not _should_persist_heartbeat(
            int(user.id),
            section or "",
            is_active,
        ):
            return

        # Para heartbeat reutilizamos la sesión canónica del request cuando existe.
        # En otros eventos mantenemos la sesión aislada para no mezclar commits.
        if action_type == "heartbeat" and request is not None:
            try:
                s = getattr(request.state, "db", None)
            except Exception:
                s = None

        if s is None:
            from .models import SessionLocal
            s = SessionLocal()
            owns_session = True

        ev = UsageEvent(
            timestamp=dt.datetime.utcnow(),
            session_id="legacy",
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
        if owns_session:
            s.commit()
    except Exception:
        if owns_session and s is not None:
            try:
                s.rollback()
            except Exception:
                pass
    finally:
        if owns_session and s is not None:
            try:
                s.close()
            except Exception:
                pass


# ======================================================================
# Live Tracking (En memoria)
# ======================================================================

# Estructura: user_id → {last_heartbeat, last_action_ts, current_section,
#                         is_active, last_action_type, session_start}
ONLINE_USERS: Dict[int, Dict[str, Any]] = {}

# Umbral para considerar que inicia una nueva sesión (minutos sin actividad)
_SESSION_BREAK_MINUTES = 30
_HEARTBEAT_PERSIST_MINUTES = 2
_LAST_PERSISTED_HEARTBEAT: Dict[int, Dict[str, Any]] = {}


def update_online_presence(
    user_id: int,
    section: str,
    is_active: bool,
    action_type: str = "",
) -> None:
    """
    Actualiza el estado de presencia en memoria del usuario.
    - Mantiene session_start a menos que haya un gap > SESSION_BREAK_MINUTES.
    - Actualiza last_action_type y last_action_ts solo para acciones significativas.
    """
    now = dt.datetime.utcnow()
    existing = ONLINE_USERS.get(user_id)

    # Determinar session_start
    if existing:
        gap_min = (now - existing["last_heartbeat"]).total_seconds() / 60
        session_start = now if gap_min > _SESSION_BREAK_MINUTES else existing["session_start"]
    else:
        session_start = now

    # last_action solo se actualiza para acciones significativas (no heartbeat)
    effective_action = action_type if action_type and action_type != "heartbeat" else ""
    if effective_action:
        last_action = effective_action
        last_action_ts = now
    else:
        last_action = existing["last_action_type"] if existing else ""
        last_action_ts = existing.get("last_action_ts", now) if existing else now

    ONLINE_USERS[user_id] = {
        "last_heartbeat": now,
        "last_action_ts": last_action_ts,
        "current_section": section,
        "is_active": is_active,
        "last_action_type": last_action,
        "session_start": session_start,
    }


def _restore_presence_from_db(session, visible_ids: Set[int]) -> None:
    """
    Recupera desde la DB la presencia de usuarios visibles que NO están en memoria.

    Ejecuta siempre (no solo en reinicio), cubriendo dos escenarios:
      1. Reinicio del servidor: ONLINE_USERS vacío → restaura todos los usuarios recientes.
      2. Multi-worker / otro worker tiene usuarios: el worker local los recupera de DB.

    La ventana de búsqueda es 15 min (= _LIVE_ABSENT_THRESHOLD) para que solo
    usuarios realmente recientes sean restaurados.

    NOTA DE ARQUITECTURA: Esta función es suficiente para single-worker con reinicios.
    Para multi-worker estable se recomienda Redis como almacén compartido.
    """
    # Usuarios visibles que NO están en memoria local
    missing_ids = visible_ids - set(ONLINE_USERS.keys())
    if not missing_ids:
        return  # Todos los usuarios visibles ya están en memoria

    cutoff = dt.datetime.utcnow() - dt.timedelta(minutes=15)
    try:
        recent = (
            session.query(UsageEvent)
            .filter(
                UsageEvent.user_id.in_(missing_ids),
                UsageEvent.timestamp >= cutoff,
                func.lower(UsageEvent.action_type) != "api_call",
            )
            .order_by(UsageEvent.user_id, UsageEvent.timestamp.desc())
            .all()
        )

        seen: Set[int] = set()
        restored = 0
        for ev in recent:
            uid = int(ev.user_id)
            if uid in seen:
                continue
            seen.add(uid)
            update_online_presence(uid, ev.section or "", True, ev.action_type or "")
            # Sobreescribir timestamps con los del evento (más precisos que utcnow)
            entry = ONLINE_USERS.get(uid)
            if entry and ev.timestamp:
                entry["last_heartbeat"] = ev.timestamp
                entry["last_action_ts"] = ev.timestamp
            restored += 1

        if restored:
            print(
                f"[PRESENCE] Restaurados {restored} usuario(s) desde DB "
                f"(missing_ids={len(missing_ids)}, visible_ids={len(visible_ids)})",
                flush=True,
            )
    except Exception as exc:
        print(f"[PRESENCE] Error en _restore_presence_from_db: {exc}", flush=True)


_NON_ADOPTION_ROLES = {"admin", "administrator", "administrador"}
_LIVE_ACTIVE_THRESHOLD = dt.timedelta(minutes=2)
_LIVE_CONNECTED_THRESHOLD = dt.timedelta(minutes=5)
_LIVE_ABSENT_THRESHOLD = dt.timedelta(minutes=15)
_ADOPTION_THRESHOLD = 40


def _should_persist_heartbeat(user_id: int, section: str, is_active: bool) -> bool:
    now = dt.datetime.utcnow()
    previous = _LAST_PERSISTED_HEARTBEAT.get(user_id)
    if previous is None:
        _LAST_PERSISTED_HEARTBEAT[user_id] = {"ts": now, "section": section, "is_active": is_active}
        return True

    elapsed_minutes = (now - previous["ts"]).total_seconds() / 60.0
    state_changed = previous["section"] != section or previous["is_active"] != is_active
    if state_changed or elapsed_minutes >= _HEARTBEAT_PERSIST_MINUTES:
        _LAST_PERSISTED_HEARTBEAT[user_id] = {"ts": now, "section": section, "is_active": is_active}
        return True
    return False


def _is_tracked_role(role: str | None) -> bool:
    return bool((role or "").strip().lower())


def _is_adoption_eligible_role(role: str | None) -> bool:
    return (role or "").strip().lower() not in _NON_ADOPTION_ROLES


def _role_label(role: str | None) -> str:
    role_norm = (role or "").strip().lower()
    if role_norm in {"analista", "analyst"}:
        return "Analista"
    if role_norm == "supervisor":
        return "Supervisor"
    if role_norm in {"admin", "administrator"}:
        return "Admin"
    if role_norm == "auditor":
        return "Auditor"
    return role_norm.title() if role_norm else "Sin rol"


def _available_role_options(users: List[User], groups_by_user: Dict[int, List[str]], team_filter: str | None = None) -> List[Dict[str, Any]]:
    team_norm = (team_filter or "").strip().lower()
    counter: Counter[str] = Counter()
    for user in users:
        role_norm = (user.role or "").strip().lower()
        if not role_norm:
            continue
        group_names = groups_by_user.get(int(user.id), [])
        if team_norm and team_norm not in {group.lower() for group in group_names}:
            continue
        counter[role_norm] += 1

    return [
        {"value": role, "label": _role_label(role), "users": count}
        for role, count in sorted(counter.items(), key=lambda item: (_role_label(item[0]).lower(), item[0]))
    ]


def _query_groups_by_user(session, user_ids: List[int]) -> Dict[int, List[str]]:
    if not user_ids:
        return {}
    try:
        rows = (
            session.query(GroupMember.user_id, Group.name)
            .join(Group, GroupMember.group_id == Group.id)
            .filter(GroupMember.user_id.in_(user_ids))
            .all()
        )
    except Exception:
        return {}

    groups_by_user: Dict[int, List[str]] = defaultdict(list)
    for gm_uid, g_name in rows:
        if g_name and g_name not in groups_by_user[int(gm_uid)]:
            groups_by_user[int(gm_uid)].append(g_name)
    return dict(groups_by_user)


def _primary_group(group_names: List[str]) -> str | None:
    return group_names[0] if group_names else None


def _status_from_signal(last_signal: Optional[dt.datetime]) -> Dict[str, Any]:
    now = dt.datetime.utcnow()
    if not last_signal:
        return {"label": "Fuera de monitoreo", "tone": "muted", "connected": False, "minutes_since_signal": None}

    age = now - last_signal
    minutes = round(age.total_seconds() / 60.0, 1)
    if age < _LIVE_ACTIVE_THRESHOLD:
        return {"label": "Activo", "tone": "success", "connected": True, "minutes_since_signal": minutes}
    if age < _LIVE_CONNECTED_THRESHOLD:
        return {"label": "Inactivo", "tone": "warning", "connected": True, "minutes_since_signal": minutes}
    if age < _LIVE_ABSENT_THRESHOLD:
        return {"label": "Ausente", "tone": "secondary", "connected": False, "minutes_since_signal": minutes}
    return {"label": "Fuera de monitoreo", "tone": "muted", "connected": False, "minutes_since_signal": minutes}


def _build_live_presence_map(session, user_ids: Set[int]) -> Dict[int, Dict[str, Any]]:
    if not user_ids:
        return {}

    _restore_presence_from_db(session, user_ids)
    now = dt.datetime.utcnow()
    cutoff = now - dt.timedelta(minutes=_SESSION_BREAK_MINUTES + 15)

    snapshots: Dict[int, Dict[str, Any]] = {}
    try:
        recent_events = (
            session.query(UsageEvent)
            .filter(
                UsageEvent.user_id.in_(user_ids),
                UsageEvent.timestamp >= cutoff,
                func.lower(UsageEvent.action_type) != "api_call",
            )
            .order_by(UsageEvent.user_id.asc(), UsageEvent.timestamp.asc())
            .all()
        )
    except Exception:
        recent_events = []

    for event in recent_events:
        uid = int(event.user_id)
        ts = event.timestamp or now
        snap = snapshots.setdefault(
            uid,
            {
                "last_signal": None,
                "last_action_ts": None,
                "last_action_type": "",
                "current_section": "",
                "session_start": ts,
                "_previous_ts": None,
            },
        )
        previous_ts = snap["_previous_ts"]
        if previous_ts is None or (ts - previous_ts).total_seconds() / 60.0 > _SESSION_BREAK_MINUTES:
            snap["session_start"] = ts
        snap["_previous_ts"] = ts
        snap["last_signal"] = ts
        if event.section:
            snap["current_section"] = event.section
        if (event.action_type or "").lower() != "heartbeat":
            snap["last_action_ts"] = ts
            snap["last_action_type"] = event.action_type or ""

    for uid, data in ONLINE_USERS.items():
        if uid not in user_ids:
            continue
        snap = snapshots.setdefault(
            uid,
            {
                "last_signal": None,
                "last_action_ts": None,
                "last_action_type": "",
                "current_section": "",
                "session_start": data.get("session_start", now),
                "_previous_ts": None,
            },
        )
        memory_signal = data.get("last_heartbeat")
        if memory_signal and (snap["last_signal"] is None or memory_signal > snap["last_signal"]):
            snap["last_signal"] = memory_signal
            snap["current_section"] = data.get("current_section") or snap["current_section"]
            snap["session_start"] = data.get("session_start") or snap["session_start"]

        memory_action_ts = data.get("last_action_ts")
        if memory_action_ts and (snap["last_action_ts"] is None or memory_action_ts > snap["last_action_ts"]):
            snap["last_action_ts"] = memory_action_ts
            snap["last_action_type"] = data.get("last_action_type") or snap["last_action_type"]
            snap["current_section"] = data.get("current_section") or snap["current_section"]

    live_map: Dict[int, Dict[str, Any]] = {}
    for uid, snap in snapshots.items():
        status_info = _status_from_signal(snap.get("last_signal"))
        if status_info["label"] == "Fuera de monitoreo":
            continue

        last_action_ts = snap.get("last_action_ts") or snap.get("last_signal")
        session_start = snap.get("session_start") or last_action_ts or snap.get("last_signal")
        live_map[uid] = {
            "status": status_info["label"],
            "status_tone": status_info["tone"],
            "connected_now": status_info["connected"],
            "last_signal": snap["last_signal"].isoformat() + "Z" if snap.get("last_signal") else None,
            "last_heartbeat": snap["last_signal"].isoformat() + "Z" if snap.get("last_signal") else None,
            "last_action_ts": last_action_ts.isoformat() + "Z" if last_action_ts else None,
            "last_action_type": snap.get("last_action_type", ""),
            "last_action": _map_action_label(snap.get("last_action_type", "")),
            "activity_type": _get_activity_category(snap.get("last_action_type", "")),
            "current_section": _map_section_name(snap.get("current_section", "")),
            "session_start": session_start.isoformat() + "Z" if session_start else None,
            "minutes_since_activity": round((now - last_action_ts).total_seconds() / 60.0, 1) if last_action_ts else None,
            "minutes_since_signal": status_info["minutes_since_signal"],
            "session_minutes": round((now - session_start).total_seconds() / 60.0, 1) if session_start else None,
        }
    return live_map


def get_live_users_data(session, visible_ids: Set[int]) -> List[Dict[str, Any]]:
    print(
        f"[LIVE] get_live_users_data: visible_ids={len(visible_ids)} "
        f"ONLINE_USERS_memory={len(ONLINE_USERS)}",
        flush=True,
    )
    presence_map = _build_live_presence_map(session, visible_ids)
    print(
        f"[LIVE] presence_map={len(presence_map)} usuarios con señal reciente",
        flush=True,
    )
    if not presence_map:
        return []

    candidate_ids = list(presence_map.keys())
    users = session.query(User).filter(User.id.in_(candidate_ids)).all()
    groups_by_user = _query_groups_by_user(session, candidate_ids)

    results = []
    for user in users:
        presence = presence_map.get(int(user.id))
        if not presence:
            continue

        group_names = groups_by_user.get(int(user.id), [])
        results.append({
            "id": user.id,
            "email": user.email,
            "name": user.full_name or user.name or user.email.split("@")[0].title(),
            "role": _role_label(user.role),
            "unit_business": (user.unit_business or "Sin unidad").title(),
            "group": _primary_group(group_names) or "Sin grupo",
            "group_names": group_names,
            "current_section": presence["current_section"],
            "last_action": presence["last_action"],
            "last_action_type": presence["last_action_type"],
            "last_action_ts": presence["last_action_ts"],
            "minutes_since_activity": presence["minutes_since_activity"],
            "status": presence["status"],
            "status_tone": presence["status_tone"],
            "session_start": presence["session_start"],
            "last_signal": presence["last_signal"],
            "last_heartbeat": presence["last_heartbeat"],
            "minutes_since_signal": presence["minutes_since_signal"],
            "activity_type": presence["activity_type"],
            "session_minutes": presence["session_minutes"],
        })

    status_order = {"Activo": 0, "Inactivo": 1, "Ausente": 2}
    results.sort(key=lambda row: (status_order.get(row["status"], 9), row["name"].lower()))
    return results


def _matches_role_filter(role: str | None, role_filter: str | None) -> bool:
    role_norm = (role or "").strip().lower()
    rf = (role_filter or "").strip().lower()
    if rf in {"", "todos", "todos_los_roles"}:
        return bool(role_norm)
    if rf == "analistas_y_supervisores":
        return role_norm in {"analista", "analyst", "supervisor"}
    if rf in {"analista", "analistas", "analyst"}:
        return role_norm in {"analista", "analyst"}
    if rf in {"supervisor", "supervisores"}:
        return role_norm == "supervisor"
    if rf in {"admin", "administrator", "administrador"}:
        return role_norm in {"admin", "administrator", "administrador"}
    return role_norm == rf


def _sessionize_events(events: List[UsageEvent]) -> Dict[str, Any]:
    if not events:
        return {"count": 0, "active_minutes": 0.0, "recent": []}

    sessions: List[Dict[str, Any]] = []
    current = None
    last_ts = None
    for event in sorted(events, key=lambda item: item.timestamp or dt.datetime.utcnow()):
        ts = event.timestamp
        if ts is None:
            continue
        if current is None or last_ts is None or (ts - last_ts).total_seconds() / 60.0 > _SESSION_BREAK_MINUTES:
            if current is not None:
                sessions.append(current)
            current = {"start": ts, "end": ts, "events": 0, "actions": Counter(), "sections": Counter()}
        current["end"] = ts
        current["events"] += 1
        current["actions"][(event.action_type or "").lower()] += 1
        section_name = _map_section_name(event.section or "")
        if section_name:
            current["sections"][section_name] += 1
        last_ts = ts

    if current is not None:
        sessions.append(current)

    recent = []
    total_minutes = 0.0
    for session in sessions:
        active_minutes = max(1.0, round((session["end"] - session["start"]).total_seconds() / 60.0, 1))
        total_minutes += active_minutes
        recent.append({
            "start": session["start"].isoformat() + "Z",
            "end": session["end"].isoformat() + "Z",
            "active_minutes": active_minutes,
            "events": int(session["events"]),
            "uploads": int(session["actions"].get("file_upload", 0)),
            "searches": int(session["actions"].get("search", 0)),
            "exports": int(session["actions"].get("export", 0)),
            "views": int(session["actions"].get("page_view", 0) + session["actions"].get("module_visit", 0)),
            "sections": [name for name, _ in session["sections"].most_common(4) if name],
        })

    return {"count": len(sessions), "active_minutes": round(total_minutes, 1), "recent": list(reversed(recent[-5:]))}


def _session_metric_buckets(events: List[UsageEvent]) -> Dict[str, Dict[Tuple[int, int | None], float]]:
    buckets: Dict[str, Dict[Tuple[int, int | None], float]] = {
        "weekday_sessions": defaultdict(float),
        "weekday_minutes": defaultdict(float),
        "hour_sessions": defaultdict(float),
        "hour_minutes": defaultdict(float),
    }
    if not events:
        return buckets

    current = None
    last_ts = None
    for event in sorted(events, key=lambda item: item.timestamp or dt.datetime.utcnow()):
        ts = event.timestamp
        if ts is None:
            continue
        if current is None or last_ts is None or (ts - last_ts).total_seconds() / 60.0 > _SESSION_BREAK_MINUTES:
            if current is not None:
                start = current["start"]
                minutes = max(1.0, round((current["end"] - start).total_seconds() / 60.0, 1))
                buckets["weekday_sessions"][(start.weekday(), None)] += 1
                buckets["weekday_minutes"][(start.weekday(), None)] += minutes
                buckets["hour_sessions"][(start.weekday(), start.hour)] += 1
                buckets["hour_minutes"][(start.weekday(), start.hour)] += minutes
            current = {"start": ts, "end": ts}
        current["end"] = ts
        last_ts = ts

    if current is not None:
        start = current["start"]
        minutes = max(1.0, round((current["end"] - start).total_seconds() / 60.0, 1))
        buckets["weekday_sessions"][(start.weekday(), None)] += 1
        buckets["weekday_minutes"][(start.weekday(), None)] += minutes
        buckets["hour_sessions"][(start.weekday(), start.hour)] += 1
        buckets["hour_minutes"][(start.weekday(), start.hour)] += minutes

    return buckets


def _engagement_score(sessions: int, active_days: int, uploads: int, searches: int, exports: int, views: int, modules_used: int) -> float:
    return (sessions * 1.5) + (active_days * 2) + (uploads * 4) + (searches * 2.5) + (exports * 2) + (views * 0.5) + (modules_used * 1.5)


def _adoption_score(active_days: int, active_minutes: float, uploads: int, searches: int, exports: int, modules_used: int) -> int:
    return int(round(
        min(active_days / 8.0, 1.0) * 20
        + min(active_minutes / 240.0, 1.0) * 15
        + min(uploads / 5.0, 1.0) * 25
        + min(searches / 8.0, 1.0) * 15
        + min(exports / 4.0, 1.0) * 10
        + min(modules_used / 4.0, 1.0) * 15
    ))


def _usage_frequency(active_days: int, period_days: int, sessions: int) -> str:
    if active_days <= 0 or sessions <= 0:
        return "Sin actividad"
    ratio = active_days / max(period_days, 1)
    if ratio >= 0.6:
        return "Diaria"
    if ratio >= 0.25 or active_days >= 5:
        return "Frecuente"
    if active_days >= 2:
        return "Semanal"
    return "Esporádica"


def _build_usage_alerts(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    severity_rank = {"alta": 3, "media": 2, "baja": 1}
    for row in rows:
        if row.get("last_seen_days", 999) >= 7:
            alerts.append({"type": "danger", "severity": "alta", "severity_rank": 3, "user": row["name"], "user_id": row["user_id"], "reason": "Sin acceso reciente", "message": f"Último acceso hace {row['last_seen_days']} días." if row["last_seen_days"] < 999 else "No registra accesos.", "recommendation": "Validar acceso, credenciales y acompañamiento."})
        if row.get("active_hours", 0) >= 1 and row.get("uploads", 0) == 0 and row.get("searches", 0) == 0:
            alerts.append({"type": "warning", "severity": "media", "severity_rank": 2, "user": row["name"], "user_id": row["user_id"], "reason": "Navegación sin resultado", "message": f"{row.get('active_hours', 0):.1f} h activas sin búsquedas ni cargas.", "recommendation": "Revisar si el usuario entiende el flujo operativo o quedó bloqueado."})
        if row.get("trend_direction") == "down" and row.get("trend_delta_pct", 0) <= -30:
            alerts.append({"type": "warning", "severity": "media", "severity_rank": 2, "user": row["name"], "user_id": row["user_id"], "reason": "Caída vs período anterior", "message": f"Uso {abs(row['trend_delta_pct']):.0f}% menor que el período previo.", "recommendation": "Confirmar cambio de rol, vacaciones o problemas de acceso."})
        if row.get("adoption_score", 0) < 20 and row.get("sessions", 0) >= 2:
            alerts.append({"type": "warning", "severity": "baja", "severity_rank": 1, "user": row["name"], "user_id": row["user_id"], "reason": "Adopción baja", "message": f"Score {row['adoption_score']}/100 con {row['sessions']} sesiones.", "recommendation": "Programar seguimiento corto sobre búsquedas, cargas y exportaciones."})
        if row.get("role", "").lower() == "analista" and not row.get("group") and row.get("sessions", 0) > 0:
            alerts.append({"type": "info", "severity": "baja", "severity_rank": 1, "user": row["name"], "user_id": row["user_id"], "reason": "Sin grupo asignado", "message": "Analista activo sin grupo operativo visible.", "recommendation": "Asignar grupo/equipo para seguimiento consistente."})

    alerts.sort(key=lambda item: (-item["severity_rank"], (item.get("user") or "").lower(), item["reason"]))
    return alerts[:30]


def _eligible_users_scope(session, current_user: User, role_filter: str | None, team_filter: str | None) -> Tuple[List[User], Dict[int, List[str]]]:
    visible_ids = visible_user_ids(session, current_user)
    if not visible_ids:
        return [], {}

    users = session.query(User).filter(User.id.in_(visible_ids)).all()
    groups_by_user = _query_groups_by_user(session, [int(user.id) for user in users])
    team_norm = (team_filter or "").strip().lower()

    eligible = []
    for user in users:
        if not _matches_role_filter(user.role, role_filter):
            continue
        group_names = groups_by_user.get(int(user.id), [])
        if team_norm and team_norm not in {group.lower() for group in group_names}:
            continue
        eligible.append(user)

    return eligible, {int(user.id): groups_by_user.get(int(user.id), []) for user in eligible}


def _timeline_detail(event: UsageEvent) -> str:
    extra = event.extra_data or {}
    action = (event.action_type or "").lower()
    resource = event.resource_id or ""
    if action == "search":
        query = extra.get("query") or extra.get("search") or extra.get("term") or resource
        return f'Busqueda: "{query}"' if query else "Realizo una busqueda"
    if action == "file_upload":
        filename = extra.get("filename") or extra.get("file_name") or resource
        return f"Carga de archivo {filename}" if filename else "Cargo un archivo"
    if action == "export":
        export_name = extra.get("format") or extra.get("export_type") or resource
        return f"Exportacion {export_name}" if export_name else "Exporto resultados"
    if action in {"page_view", "module_visit"}:
        section_name = _map_section_name(event.section or "")
        return f"Visito {section_name}" if section_name else "Visito"
    if action == "form_submit":
        return "Completo una accion de edicion"
    if action == "login":
        return "Inicio sesion en la plataforma"
    return _map_action_label(event.action_type or "")


def _summary_states(eligible_users: int, active_users: int, alerts_count: int, has_filters: bool) -> Dict[str, str]:
    if eligible_users == 0:
        return {
            "kpis": "filter_empty",
            "by_user": "filter_empty",
            "alerts": "filter_empty",
            "charts": "filter_empty",
        }
    if active_users == 0:
        return {
            "kpis": "no_activity",
            "by_user": "ok",
            "alerts": "empty" if alerts_count == 0 else "ok",
            "charts": "no_activity",
        }
    return {
        "kpis": "ok",
        "by_user": "ok",
        "alerts": "ok" if alerts_count > 0 else "empty",
        "charts": "ok",
    }


def _get_usage_summary_impl(
    *,
    current_user: User,
    date_from: str = "",
    date_to: str = "",
    role_filter: str | None = None,
    team_filter: str | None = None,
    view: str = "day",
) -> Dict[str, Any]:
    s = db_session()
    start_date, end_date = _default_date_range(date_from, date_to)
    start_dt = dt.datetime.combine(start_date, dt.time.min)
    end_dt = dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time.min)
    previous_start_dt = start_dt - (end_dt - start_dt)
    period_days = max((end_date - start_date).days + 1, 1)
    has_filters = bool((role_filter or "").strip() or (team_filter or "").strip())

    try:
        visible_ids = visible_user_ids(s, current_user)
        visible_users = s.query(User).filter(User.id.in_(visible_ids)).all() if visible_ids else []
        visible_groups = _query_groups_by_user(s, [int(user.id) for user in visible_users])
        available_roles = _available_role_options(visible_users, visible_groups, team_filter)
        eligible_users, eligible_groups = _eligible_users_scope(
            s, current_user=current_user, role_filter=role_filter, team_filter=team_filter
        )
        eligible_ids = [int(user.id) for user in eligible_users]
        available_groups = Counter((_primary_group(groups) or "Sin grupo") for groups in eligible_groups.values())
        if not eligible_ids:
            return {
                "filters": {"date_from": start_date.isoformat(), "date_to": end_date.isoformat(), "role_filter": role_filter or "", "team_filter": team_filter or "", "view": view},
                "kpis": {"connected_now": 0, "eligible_users": 0, "active_users": 0, "uploads": 0, "active_hours": 0.0, "avg_productivity_index": 0.0, "adoption_rate": 0.0, "inactive_7d_count": 0},
                "meta": {"available_groups": [{"value": name, "label": name, "users": count} for name, count in sorted(available_groups.items())], "available_roles": available_roles, "admins_excluded_by_default": True, "adoption_threshold": _ADOPTION_THRESHOLD, "states": _summary_states(0, 0, 0, has_filters)},
                "charts": {"by_weekday": [], "users_vs_users": [], "heatmap": [], "sections": []},
                "per_user": [],
                "alerts": [],
            }

        last_seen_rows = (
            s.query(UsageEvent.user_id, func.max(UsageEvent.timestamp))
            .filter(UsageEvent.user_id.in_(eligible_ids), func.lower(UsageEvent.action_type) != "api_call")
            .group_by(UsageEvent.user_id)
            .all()
        )
        last_seen_by_user = {int(uid): ts for uid, ts in last_seen_rows if ts}
        current_events = (
            s.query(UsageEvent)
            .filter(UsageEvent.user_id.in_(eligible_ids), UsageEvent.timestamp >= start_dt, UsageEvent.timestamp < end_dt, func.lower(UsageEvent.action_type) != "api_call")
            .order_by(UsageEvent.user_id.asc(), UsageEvent.timestamp.asc())
            .all()
        )
        previous_events = (
            s.query(UsageEvent)
            .filter(UsageEvent.user_id.in_(eligible_ids), UsageEvent.timestamp >= previous_start_dt, UsageEvent.timestamp < start_dt, func.lower(UsageEvent.action_type) != "api_call")
            .order_by(UsageEvent.user_id.asc(), UsageEvent.timestamp.asc())
            .all()
        )
        live_by_user = {row["id"]: row for row in get_live_users_data(s, set(eligible_ids))}

        by_user_current: Dict[int, List[UsageEvent]] = defaultdict(list)
        by_user_previous: Dict[int, List[UsageEvent]] = defaultdict(list)
        weekday_stats = {i: {"events": 0, "user_ids": set(), "uploads": 0, "sessions": 0, "active_minutes": 0.0} for i in range(7)}
        heatmap: Dict[Tuple[int, int], int] = {}
        heatmap_uploads: Dict[Tuple[int, int], int] = {}
        heatmap_sessions: Dict[Tuple[int, int], float] = {}
        heatmap_minutes: Dict[Tuple[int, int], float] = {}
        section_stats: Dict[str, Dict[str, Any]] = {}

        for event in current_events:
            by_user_current[int(event.user_id)].append(event)
            if (event.action_type or "").lower() == "heartbeat":
                continue
            ts = event.timestamp or start_dt
            weekday_stats[ts.weekday()]["events"] += 1
            weekday_stats[ts.weekday()]["user_ids"].add(int(event.user_id))
            heatmap[(ts.weekday(), ts.hour)] = heatmap.get((ts.weekday(), ts.hour), 0) + 1
            if (event.action_type or "").lower() == "file_upload":
                weekday_stats[ts.weekday()]["uploads"] += 1
                heatmap_uploads[(ts.weekday(), ts.hour)] = heatmap_uploads.get((ts.weekday(), ts.hour), 0) + 1
            section_name = _map_section_name(event.section or "")
            if section_name:
                bucket = section_stats.setdefault(section_name, {"section": section_name, "events": 0, "uploads": 0, "active_minutes": 0.0, "sessions": 0, "user_ids": set()})
                bucket["events"] += 1
                if (event.action_type or "").lower() == "file_upload":
                    bucket["uploads"] += 1
                bucket["user_ids"].add(int(event.user_id))

        for event in previous_events:
            by_user_previous[int(event.user_id)].append(event)

        per_user_rows = []
        for user in eligible_users:
            uid = int(user.id)
            current = by_user_current.get(uid, [])
            previous = by_user_previous.get(uid, [])
            current_session = _sessionize_events(current)
            session_buckets = _session_metric_buckets(current)
            for (weekday, _), count in session_buckets["weekday_sessions"].items():
                weekday_stats[weekday]["sessions"] += int(count)
            for (weekday, _), minutes in session_buckets["weekday_minutes"].items():
                weekday_stats[weekday]["active_minutes"] += float(minutes)
            for key, count in session_buckets["hour_sessions"].items():
                heatmap_sessions[key] = heatmap_sessions.get(key, 0.0) + float(count)
            for key, minutes in session_buckets["hour_minutes"].items():
                heatmap_minutes[key] = heatmap_minutes.get(key, 0.0) + float(minutes)
            previous_session = _sessionize_events(previous)
            current_non_heartbeat = [event for event in current if (event.action_type or "").lower() != "heartbeat"]
            previous_non_heartbeat = [event for event in previous if (event.action_type or "").lower() != "heartbeat"]
            active_days = len({event.timestamp.date() for event in current_non_heartbeat if event.timestamp})
            views = sum(1 for event in current_non_heartbeat if (event.action_type or "").lower() in {"page_view", "module_visit"})
            searches = sum(1 for event in current_non_heartbeat if (event.action_type or "").lower() == "search")
            uploads = sum(1 for event in current_non_heartbeat if (event.action_type or "").lower() == "file_upload")
            exports = sum(1 for event in current_non_heartbeat if (event.action_type or "").lower() == "export")
            current_section_names = [_map_section_name(event.section or "") for event in current_non_heartbeat]
            modules_counter = Counter(name for name in current_section_names if name)
            modules_used = len(modules_counter)
            active_minutes = float(current_session["active_minutes"])
            active_hours = round(active_minutes / 60.0, 1)
            sessions = int(current_session["count"])
            if current_non_heartbeat and active_minutes > 0:
                events_count = len(current_non_heartbeat)
                for name, count in modules_counter.items():
                    if name in section_stats:
                        section_stats[name]["active_minutes"] += active_minutes * (count / events_count)
                        section_stats[name]["sessions"] += sessions * (count / events_count)
            adoption_score = _adoption_score(active_days, active_minutes, uploads, searches, exports, modules_used)
            productivity_index = round(uploads / sessions, 2) if sessions > 0 else 0.0

            prev_active_days = len({event.timestamp.date() for event in previous_non_heartbeat if event.timestamp})
            prev_views = sum(1 for event in previous_non_heartbeat if (event.action_type or "").lower() in {"page_view", "module_visit"})
            prev_searches = sum(1 for event in previous_non_heartbeat if (event.action_type or "").lower() == "search")
            prev_uploads = sum(1 for event in previous_non_heartbeat if (event.action_type or "").lower() == "file_upload")
            prev_exports = sum(1 for event in previous_non_heartbeat if (event.action_type or "").lower() == "export")
            previous_section_names = [_map_section_name(event.section or "") for event in previous_non_heartbeat]
            prev_modules = len(Counter(name for name in previous_section_names if name))
            current_engagement = _engagement_score(sessions, active_days, uploads, searches, exports, views, modules_used)
            previous_engagement = _engagement_score(int(previous_session["count"]), prev_active_days, prev_uploads, prev_searches, prev_exports, prev_views, prev_modules)
            if previous_engagement <= 0 and current_engagement <= 0:
                trend_delta_pct = 0.0
            elif previous_engagement <= 0:
                trend_delta_pct = 100.0
            else:
                trend_delta_pct = round(((current_engagement - previous_engagement) / previous_engagement) * 100.0, 1)
            trend_direction = "up" if trend_delta_pct >= 15 else ("down" if trend_delta_pct <= -15 else "stable")

            last_seen = last_seen_by_user.get(uid)
            last_seen_days = (dt.datetime.utcnow() - last_seen).days if last_seen else 999
            live_info = live_by_user.get(uid)
            role_raw = (user.role or "").strip().lower()
            current_status = live_info["status"] if live_info else ("Fuera de monitoreo" if last_seen else "Sin acceso")
            risk_level = "alto" if last_seen_days >= 7 or current_status == "Sin acceso" else ("medio" if adoption_score < _ADOPTION_THRESHOLD or trend_direction == "down" else "bajo")
            groups = eligible_groups.get(uid, [])

            per_user_rows.append({
                "user_id": uid,
                "name": user.full_name or user.name or user.email.split("@")[0].title(),
                "email": user.email,
                "role_raw": role_raw,
                "role": _role_label(user.role),
                "unit_business": (user.unit_business or "Sin unidad").title(),
                "group": _primary_group(groups),
                "group_names": groups,
                "created_at": user.created_at.isoformat() + "Z" if getattr(user, "created_at", None) else None,
                "current_status": current_status,
                "status_tone": live_info["status_tone"] if live_info else "muted",
                "session_start": live_info["session_start"] if live_info else None,
                "last_signal": live_info["last_signal"] if live_info else (last_seen.isoformat() + "Z" if last_seen else None),
                "last_action": live_info["last_action"] if live_info else "Sin actividad reciente",
                "last_action_ts": live_info["last_action_ts"] if live_info else None,
                "activity_type": live_info["activity_type"] if live_info else "fuera de monitoreo",
                "active_days": active_days,
                "sessions": sessions,
                "uploads": uploads,
                "searches": searches,
                "downloads": exports,
                "exports": exports,
                "views": views,
                "module_views": views,
                "modules_used": modules_used,
                "modules_used_list": [name for name, _ in modules_counter.most_common(5)],
                "events": len(current_non_heartbeat),
                "active_minutes": round(active_minutes, 1),
                "active_hours": active_hours,
                "frequency": _usage_frequency(active_days, period_days, sessions),
                "productivity_index": productivity_index,
                "adoption_score": adoption_score,
                "is_inactive_7d": last_seen_days >= 7,
                "last_seen_days": last_seen_days,
                "risk_level": risk_level,
                "trend_direction": trend_direction,
                "trend_delta_pct": trend_delta_pct,
                "trend_label": f"{trend_delta_pct:+.0f}%" if trend_direction != "stable" else "Estable",
                "last_seen": last_seen.isoformat() + "Z" if last_seen else None,
                "recent_sessions": current_session["recent"],
            })

        per_user_rows.sort(key=lambda row: (row["adoption_score"], row["events"], row["active_hours"], row["name"].lower()), reverse=True)
        active_users = sum(1 for row in per_user_rows if row["events"] > 0)
        alerts = _build_usage_alerts(per_user_rows)
        states = _summary_states(len(eligible_ids), active_users, len(alerts), has_filters)
        connected_now = sum(1 for row in live_by_user.values() if row["status"] in {"Activo", "Inactivo"})
        inactive_7d_count = sum(1 for row in per_user_rows if row["is_inactive_7d"])
        uploads_total = sum(row["uploads"] for row in per_user_rows)
        total_active_hours = round(sum(row["active_hours"] for row in per_user_rows), 1)
        adoption_rows = [row for row in per_user_rows if _is_adoption_eligible_role(row.get("role_raw"))]
        productivity_values = [row["productivity_index"] for row in adoption_rows if row["sessions"] > 0]
        avg_productivity = round(sum(productivity_values) / len(productivity_values), 2) if productivity_values else 0.0
        adopters = sum(1 for row in adoption_rows if row["adoption_score"] >= _ADOPTION_THRESHOLD)
        adoption_rate = round((adopters / len(adoption_rows)) * 100.0, 1) if adoption_rows else 0.0

        weekday_labels = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]
        return {
            "filters": {"date_from": start_date.isoformat(), "date_to": end_date.isoformat(), "role_filter": role_filter or "", "team_filter": team_filter or "", "view": view},
            "kpis": {"connected_now": connected_now, "eligible_users": len(eligible_ids), "adoption_eligible_users": len(adoption_rows), "active_users": active_users, "uploads": uploads_total, "active_hours": total_active_hours, "avg_productivity_index": avg_productivity, "adoption_rate": adoption_rate, "inactive_7d_count": inactive_7d_count},
            "meta": {"available_groups": [{"value": name, "label": name, "users": count} for name, count in sorted(available_groups.items())], "available_roles": available_roles, "admins_excluded_by_default": True, "adoption_threshold": _ADOPTION_THRESHOLD, "states": states},
            "charts": {
                "by_weekday": [{"weekday_index": idx, "weekday_label": weekday_labels[idx], "events": int(weekday_stats[idx]["events"]), "users": len(weekday_stats[idx]["user_ids"]), "uploads": int(weekday_stats[idx]["uploads"]), "sessions": int(weekday_stats[idx]["sessions"]), "active_hours": round(float(weekday_stats[idx]["active_minutes"]) / 60.0, 1)} for idx in range(7)],
                "users_vs_users": [{"user_label": row["name"], "events": row["events"], "active_hours": row["active_hours"], "sessions": row["sessions"], "uploads": row["uploads"]} for row in [r for r in per_user_rows if r["events"] > 0][:15]],
                "heatmap": [{"weekday": weekday, "hour": hour, "events": int(count), "uploads": int(heatmap_uploads.get((weekday, hour), 0)), "sessions": int(heatmap_sessions.get((weekday, hour), 0)), "active_hours": round(float(heatmap_minutes.get((weekday, hour), 0.0)) / 60.0, 1)} for (weekday, hour), count in sorted(heatmap.items())],
                "sections": [{"section": sec, "events": int(data["events"]), "users": len(data["user_ids"]), "uploads": int(data["uploads"]), "sessions": int(round(data["sessions"])), "active_hours": round(float(data["active_minutes"]) / 60.0, 1)} for sec, data in sorted(section_stats.items(), key=lambda item: item[1]["events"], reverse=True)[:20]],
            },
            "per_user": per_user_rows,
            "alerts": alerts,
        }
    finally:
        try:
            s.close()
        except Exception:
            pass


def _get_user_profile_timeline_impl(session, user_id: int) -> Dict[str, Any]:
    user = session.query(User).filter(User.id == user_id).first()
    if not user:
        return {"error": "Usuario no encontrado"}

    groups = _query_groups_by_user(session, [int(user_id)]).get(int(user_id), [])
    base_q = session.query(UsageEvent).filter(UsageEvent.user_id == user_id, func.lower(UsageEvent.action_type) != "api_call")
    history = base_q.filter(UsageEvent.timestamp >= dt.datetime.utcnow() - dt.timedelta(days=90)).order_by(UsageEvent.timestamp.asc()).all()
    session_data = _sessionize_events(history)
    last_seen = max((event.timestamp for event in history if event.timestamp), default=None)
    live_info = _build_live_presence_map(session, {int(user_id)}).get(int(user_id))
    current_status = live_info["status"] if live_info else ("Fuera de monitoreo" if last_seen else "Sin acceso")
    non_heartbeat = [event for event in history if (event.action_type or "").lower() != "heartbeat"]
    views = sum(1 for event in non_heartbeat if (event.action_type or "").lower() in {"page_view", "module_visit"})
    searches = sum(1 for event in non_heartbeat if (event.action_type or "").lower() == "search")
    uploads = sum(1 for event in non_heartbeat if (event.action_type or "").lower() == "file_upload")
    exports = sum(1 for event in non_heartbeat if (event.action_type or "").lower() == "export")
    section_names = [_map_section_name(event.section or "") for event in non_heartbeat]
    modules_counter = Counter(name for name in section_names if name)
    active_days = len({event.timestamp.date() for event in non_heartbeat if event.timestamp})
    active_minutes = float(session_data["active_minutes"])
    adoption_score = _adoption_score(active_days, active_minutes, uploads, searches, exports, len(modules_counter))
    risk_level = "alto" if last_seen is None or (dt.datetime.utcnow() - last_seen).days >= 7 else ("medio" if adoption_score < _ADOPTION_THRESHOLD else "bajo")
    timeline_events = base_q.filter(func.lower(UsageEvent.action_type) != "heartbeat").order_by(UsageEvent.timestamp.desc()).limit(150).all()

    return {
        "user_id": user.id,
        "name": user.full_name or user.name or user.email.split("@")[0].title(),
        "email": user.email,
        "role": _role_label(user.role),
        "unit_business": (user.unit_business or "Sin unidad").title(),
        "group": _primary_group(groups),
        "group_names": groups,
        "created_at": user.created_at.isoformat() + "Z" if getattr(user, "created_at", None) else None,
        "adoption_score": adoption_score,
        "risk_level": risk_level,
        "current_status": {"status": current_status, "status_tone": live_info["status_tone"] if live_info else "muted", "current_section": live_info["current_section"] if live_info else None, "last_action": live_info["last_action"] if live_info else "Sin actividad reciente", "last_action_ts": live_info["last_action_ts"] if live_info else None, "last_signal": live_info["last_signal"] if live_info else (last_seen.isoformat() + "Z" if last_seen else None), "activity_type": live_info["activity_type"] if live_info else "fuera de monitoreo", "session_start": live_info["session_start"] if live_info else None},
        "stats": {"sessions": int(session_data["count"]), "active_hours": round(active_minutes / 60.0, 1), "active_days": active_days, "views": views, "searches": searches, "downloads": exports, "uploads": uploads, "exports": exports, "modules_used": len(modules_counter), "modules_used_list": [name for name, _ in modules_counter.most_common(8)], "frequency": _usage_frequency(active_days, 90, int(session_data["count"])), "last_seen": last_seen.isoformat() + "Z" if last_seen else None},
        "recent_sessions": session_data["recent"],
        "modules": [{"section": name, "events": count} for name, count in modules_counter.most_common(8)],
        "alerts": _build_usage_alerts([{"user_id": user.id, "name": user.full_name or user.name or user.email.split("@")[0].title(), "role": _role_label(user.role), "group": _primary_group(groups), "adoption_score": adoption_score, "sessions": int(session_data["count"]), "active_hours": round(active_minutes / 60.0, 1), "uploads": uploads, "searches": searches, "last_seen_days": (dt.datetime.utcnow() - last_seen).days if last_seen else 999, "trend_direction": "stable", "trend_delta_pct": 0.0}]),
        "timeline": [{"id": event.id, "timestamp": event.timestamp.isoformat() + "Z" if event.timestamp else None, "action_type": event.action_type, "action_label": _map_action_label(event.action_type or ""), "activity_type": _get_activity_category(event.action_type or ""), "section": _map_section_name(event.section or ""), "description": _timeline_detail(event), "duration_ms": event.duration_ms, "extra_data": event.extra_data or {}} for event in timeline_events],
    }


# ======================================================================
# Consultas agregadas para "Seguimiento de usuarios"
# ======================================================================

def get_usage_summary(
    *,
    current_user: User,
    date_from: str = "",
    date_to: str = "",
    role_filter: str | None = None,
    team_filter: str | None = None,
    view: str = "day",
) -> Dict[str, Any]:
    """
    Devuelve un resumen completo para alimentar el dashboard de
    Seguimiento de usuarios.

    - Respeta la visibilidad del usuario (visible_user_ids).
    - Se centra en roles de Analista + Supervisor.
    - Incluye: KPIs, gráficos, per_user (con grupo, riesgo, tendencia),
      y alertas de uso detalladas por usuario.
    """
    return _get_usage_summary_impl(
        current_user=current_user,
        date_from=date_from,
        date_to=date_to,
        role_filter=role_filter,
        team_filter=team_filter,
        view=view,
    )

    s = db_session()

    def _empty(start: dt.date, end: dt.date) -> Dict[str, Any]:
        return {
            "filters": {
                "date_from": start.isoformat(),
                "date_to": end.isoformat(),
                "role_filter": role_filter or "analistas_y_supervisores",
                "team_filter": team_filter or "",
                "view": view,
            },
            "kpis": {
                "connected_now": 0,
                "eligible_users": 0,
                "active_users": 0,
                "uploads": 0,
                "active_hours": 0.0,
                "avg_productivity_index": 0.0,
                "adoption_rate": 0.0,
                "inactive_7d_count": 0,
            },
            "meta": {
                "available_groups": [],
                "admins_excluded_by_default": True,
                "adoption_threshold": _ADOPTION_THRESHOLD,
            },
            "charts": {
                "by_weekday": [],
                "users_vs_users": [],
                "heatmap": [],
                "sections": [],
            },
            "per_user": [],
            "alerts": [],
        }

    start_date, end_date = _default_date_range(date_from, date_to)

    try:
        eligible_users, eligible_groups = _eligible_users_scope(
            s,
            current_user=current_user,
            role_filter=role_filter,
            team_filter=team_filter,
        )
        eligible_ids = [int(user.id) for user in eligible_users]
        if not eligible_ids:
            return _empty(start_date, end_date)

        base_q = (
            s.query(UsageEvent)
            .filter(
                UsageEvent.user_id.in_(eligible_ids),
                func.lower(UsageEvent.action_type) != "heartbeat",
                func.lower(UsageEvent.action_type) != "api_call",
            )
        )

        start_dt = dt.datetime.combine(start_date, dt.time.min)
        end_dt = dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time.min)
        base_q = base_q.filter(
            UsageEvent.timestamp >= start_dt,
            UsageEvent.timestamp < end_dt,
        )

        events: List[UsageEvent] = (
            base_q.order_by(UsageEvent.user_id.asc(), UsageEvent.timestamp.asc()).all()
        )

        if not events:
            events = []

        # Punto medio del periodo para calcular tendencia
        period_midpoint = start_dt + (end_dt - start_dt) / 2
        cutoff_7d = dt.datetime.utcnow() - dt.timedelta(days=7)

        # ------------------------------------------------------------------
        # 3) Recorrido principal
        # ------------------------------------------------------------------
        SESSION_GAP_MIN = 30

        user_stats: Dict[int, Dict[str, Any]] = {}
        weekday_stats: Dict[int, Dict[str, Any]] = {
            i: {"weekday": i, "events": 0, "user_ids": set()} for i in range(7)
        }
        heatmap: Dict[Tuple[int, int], int] = {}
        section_stats: Dict[str, Dict[str, Any]] = {}
        role_stats: Dict[str, Dict[str, Any]] = {}

        for eligible_user in eligible_users:
            uid = int(eligible_user.id)
            user_stats[uid] = {
                "user_id": uid,
                "role": (eligible_user.role or "").lower(),
                "unit_business": getattr(eligible_user, "unit_business", "Sin unidad"),
                "days": set(),
                "events": 0,
                "uploads": 0,
                "searches": 0,
                "exports": 0,
                "module_views": 0,
                "sections_used": set(),
                "duration_ms": 0,
                "sessions": 0,
                "active_minutes": 0.0,
                "last_seen": last_seen_by_user.get(uid),
                "_last_ts": None,
                "_session_start": None,
                "h1_events": 0,
                "h2_events": 0,
                "recent_7d_events": 0,
                "created_at": eligible_user.created_at.isoformat() + "Z" if getattr(eligible_user, "created_at", None) else None,
                "name": eligible_user.full_name or eligible_user.name or eligible_user.email,
                "email": eligible_user.email,
            }

        for ev in events:
            uid = int(ev.user_id)
            role = (ev.user_role or "").lower() or "otro"

            st = user_stats.setdefault(
                uid,
                {
                    "user_id": uid,
                    "role": role,
                    "unit_business": "Sin unidad",
                    "days": set(),
                    "events": 0,
                    "uploads": 0,
                    "searches": 0,
                    "exports": 0,
                    "module_views": 0,
                    "sections_used": set(),
                    "duration_ms": 0,
                    "sessions": 0,
                    "active_minutes": 0.0,
                    "last_seen": None,
                    "_last_ts": None,
                    "_session_start": None,
                    # Para tendencia: primera vs segunda mitad del periodo
                    "h1_events": 0,
                    "h2_events": 0,
                    # Para alertas: recientes
                    "recent_7d_events": 0,
                    "created_at": None,
                },
            )

            ts = ev.timestamp or dt.datetime.utcnow()
            day = ts.date()
            st["days"].add(day)
            st["events"] += 1

            # Tendencia
            if ts < period_midpoint:
                st["h1_events"] += 1
            else:
                st["h2_events"] += 1

            # Eventos recientes (últimos 7 días)
            if ts >= cutoff_7d:
                st["recent_7d_events"] += 1

            if ev.action_type == "file_upload":
                st["uploads"] += 1
            elif ev.action_type == "search":
                st["searches"] += 1
            elif ev.action_type == "export":
                st["exports"] += 1
            elif ev.action_type in ("page_view", "module_visit"):
                st["module_views"] += 1

            if ev.duration_ms:
                st["duration_ms"] += int(ev.duration_ms)

            if st["last_seen"] is None or ts > st["last_seen"]:
                st["last_seen"] = ts

            # Sesiones (por gap)
            last_ts = st["_last_ts"]
            if last_ts is None:
                st["sessions"] = 1
                st["_session_start"] = ts
            else:
                gap_min = (ts - last_ts).total_seconds() / 60.0
                if gap_min > SESSION_GAP_MIN:
                    start_s = st["_session_start"] or last_ts
                    dur_prev = max(1.0, (last_ts - start_s).total_seconds() / 60.0)
                    st["active_minutes"] += dur_prev
                    st["sessions"] += 1
                    st["_session_start"] = ts

            st["_last_ts"] = ts

            # Actividad por día de la semana
            dow = ts.weekday()
            wd = weekday_stats[dow]
            wd["events"] += 1
            wd["user_ids"].add(uid)

            # Heatmap día/hora
            key = (dow, ts.hour)
            heatmap[key] = heatmap.get(key, 0) + 1

            # Secciones
            raw_sec = (ev.section or "").strip()
            sec_name = _map_section_name(raw_sec)
            if not sec_name:
                continue
            st["sections_used"].add(sec_name)
            sec_st = section_stats.setdefault(
                sec_name, {"section": sec_name, "events": 0, "user_ids": set()}
            )
            sec_st["events"] += 1
            sec_st["user_ids"].add(uid)

            # Stats por rol
            rs = role_stats.setdefault(
                role,
                {"role": role, "events": 0, "uploads": 0, "active_minutes": 0.0},
            )
            rs["events"] += 1
            if ev.action_type == "file_upload":
                rs["uploads"] += 1

        # Cerrar sesiones abiertas
        for st in user_stats.values():
            last_ts = st["_last_ts"]
            start_s = st["_session_start"] or last_ts
            if last_ts is not None and start_s is not None:
                dur = max(1.0, (last_ts - start_s).total_seconds() / 60.0)
                st["active_minutes"] += dur

        # ------------------------------------------------------------------
        # 4) Enriquecer con datos del usuario
        # ------------------------------------------------------------------
        user_ids = list(user_stats.keys())
        users = s.query(User).filter(User.id.in_(user_ids)).all()
        users_by_id = {int(u.id): u for u in users}

        for uid, st in user_stats.items():
            u = users_by_id.get(uid)
            if u is not None:
                st["name"] = u.full_name or u.name or u.email
                st["email"] = u.email
                st["role"] = (u.role or st["role"] or "").lower()
                st["unit_business"] = getattr(u, "unit_business", "Sin unidad")
                created = getattr(u, "created_at", None)
                st["created_at"] = created.isoformat() + "Z" if created else None

        # Acumular minutos activos en stats por rol
        for st in user_stats.values():
            r = (st["role"] or "otro").lower()
            rs = role_stats.setdefault(
                r,
                {"role": r, "events": 0, "uploads": 0, "active_minutes": 0.0},
            )
            rs["active_minutes"] += float(st["active_minutes"])

        # ------------------------------------------------------------------
        # 4b) Grupos por usuario (un solo query)
        # ------------------------------------------------------------------
        try:
            group_members = (
                s.query(GroupMember.user_id, Group.name)
                .join(Group, GroupMember.group_id == Group.id)
                .filter(GroupMember.user_id.in_(user_ids))
                .all()
            )
            groups_by_user: Dict[int, List[str]] = {}
            for gm_uid, g_name in group_members:
                groups_by_user.setdefault(int(gm_uid), []).append(g_name)
        except Exception:
            groups_by_user = {}

        for uid, names in eligible_groups.items():
            if uid not in groups_by_user:
                groups_by_user[uid] = list(names)

        live_by_user = {
            row["id"]: row
            for row in get_live_users_data(s, set(eligible_ids))
        }
        connected_now = sum(1 for row in live_by_user.values() if row["status"] in {"Activo", "Inactivo"})

        # ------------------------------------------------------------------
        # 5) KPIs globales
        # ------------------------------------------------------------------
        active_users = sum(1 for st in user_stats.values() if st["events"] > 0)
        uploads_total = sum(st["uploads"] for st in user_stats.values())
        total_active_minutes = sum(st["active_minutes"] for st in user_stats.values())
        total_active_hours = round(total_active_minutes / 60.0, 1)

        user_indices: List[float] = []
        for st in user_stats.values():
            if st["sessions"] > 0:
                idx = st["uploads"] / float(st["sessions"])
                user_indices.append(idx)

        avg_prod = round(
            sum(user_indices) / len(user_indices), 2
        ) if user_indices else 0.0

        # ------------------------------------------------------------------
        # 6) Datos para gráficos
        # ------------------------------------------------------------------
        weekday_chart = []
        weekday_labels = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
        for i in range(7):
            wd = weekday_stats[i]
            weekday_chart.append({
                "weekday_index": i,
                "weekday_label": weekday_labels[i],
                "events": int(wd["events"]),
                "users": len(wd["user_ids"]),
            })

        user_list_for_chart = []
        for uid, st in user_stats.items():
            u_name = st.get("name") or str(uid)
            if "@" in u_name:
                u_name = u_name.split("@")[0]
            user_list_for_chart.append({
                "user_label": u_name,
                "events": st["events"],
                "active_hours": round(st["active_minutes"] / 60.0, 1),
            })

        user_list_for_chart.sort(key=lambda x: x["events"], reverse=True)
        users_vs_users_chart = user_list_for_chart[:15]

        heatmap_list = []
        for (dow, hour), count in heatmap.items():
            heatmap_list.append({"weekday": dow, "hour": hour, "events": int(count)})

        sections_list = []
        for sec, st in section_stats.items():
            sections_list.append({
                "section": sec,
                "events": int(st["events"]),
                "users": len(st["user_ids"]),
            })
        sections_list.sort(key=lambda r: r["events"], reverse=True)
        sections_list = sections_list[:20]

        # ------------------------------------------------------------------
        # 7) Resumen por usuario
        # ------------------------------------------------------------------
        per_user_rows = []
        today_utc = dt.datetime.utcnow()

        for uid, st in user_stats.items():
            u = users_by_id.get(uid)
            role_norm = (st["role"] or "").lower()
            role_label = "Analista" if role_norm in ("analista", "analyst") else (
                "Supervisor" if role_norm == "supervisor" else role_norm.capitalize()
            )
            active_days = len(st["days"])
            active_hours = st["active_minutes"] / 60.0

            prod_idx = (
                round(st["uploads"] / float(st["sessions"]), 2)
                if st["sessions"] > 0 else 0.0
            )

            adoption_score = _adoption_score(
                active_days,
                float(st["active_minutes"]),
                int(st["uploads"]),
                int(st["searches"]),
                int(st["exports"]),
                len([name for name in st["sections_used"] if name and name != "Sin Sección"]),
            )

            # Días desde último acceso
            last_seen_days = (today_utc - st["last_seen"]).days if st["last_seen"] else 999
            is_inactive_7d = last_seen_days >= 7

            # Nivel de riesgo
            if is_inactive_7d:
                risk_level = "alto"
            elif last_seen_days >= 3 or (active_hours >= 1.0 and adoption_score < 15):
                risk_level = "medio"
            elif adoption_score >= 30:
                risk_level = "bajo"
            else:
                risk_level = "medio" if (active_hours < 0.5 and adoption_score < 10) else "bajo"

            # Tendencia (1ra vs 2da mitad del periodo)
            h1 = st["h1_events"]
            h2 = st["h2_events"]
            if h1 == 0 and h2 > 0:
                trend = "up"
            elif h1 > 0 and h2 == 0:
                trend = "down"
            elif h1 == 0 and h2 == 0:
                trend = "stable"
            elif h2 > h1 * 1.3:
                trend = "up"
            elif h2 < h1 * 0.7:
                trend = "down"
            else:
                trend = "stable"

            group_names = groups_by_user.get(uid, [])
            live_info = live_by_user.get(uid)
            modules_used = len([name for name in st["sections_used"] if name and name != "Sin Sección"])
            trend_delta_pct = 30.0 if trend == "up" else (-30.0 if trend == "down" else 0.0)
            current_status = live_info["status"] if live_info else ("Sin señal" if st["last_seen"] else "Sin acceso")

            per_user_rows.append({
                "user_id": uid,
                "name": st.get("name") or (u.email if u else f"User {uid}"),
                "email": st.get("email") or (u.email if u else None),
                "role": role_label,
                "unit_business": (st.get("unit_business") or "Sin unidad").title(),
                "group": group_names[0] if group_names else None,
                "group_names": group_names,
                "created_at": st.get("created_at"),
                "current_status": current_status,
                "status_tone": live_info["status_tone"] if live_info else "muted",
                "session_start": live_info["session_start"] if live_info else None,
                "last_signal": live_info["last_signal"] if live_info else (st["last_seen"].isoformat() + "Z" if st["last_seen"] else None),
                "last_action": live_info["last_action"] if live_info else "Sin actividad reciente",
                "last_action_ts": live_info["last_action_ts"] if live_info else None,
                "activity_type": live_info["activity_type"] if live_info else "sin señal",
                "active_days": active_days,
                "sessions": int(st["sessions"]),
                "uploads": int(st["uploads"]),
                "searches": int(st["searches"]),
                "downloads": int(st["exports"]),
                "exports": int(st["exports"]),
                "views": int(st["module_views"]),
                "module_views": int(st["module_views"]),
                "modules_used": modules_used,
                "modules_used_list": [name for name in sorted(st["sections_used"]) if name and name != "Sin Sección"][:5],
                "events": int(st["events"]),
                "active_minutes": round(float(st["active_minutes"]), 1),
                "active_hours": round(active_hours, 1),
                "frequency": _usage_frequency(active_days, period_days, int(st["sessions"])),
                "productivity_index": prod_idx,
                "adoption_score": adoption_score,
                "is_inactive_7d": is_inactive_7d,
                "last_seen_days": last_seen_days,
                "risk_level": risk_level,
                "trend": trend,
                "trend_direction": trend,
                "trend_delta_pct": trend_delta_pct,
                "trend_label": f"{trend_delta_pct:+.0f}%" if trend != "stable" else "Estable",
                "last_seen": st["last_seen"].isoformat() + "Z" if st["last_seen"] else None,
            })

        per_user_rows.sort(
            key=lambda r: (r["adoption_score"], r["uploads"], r["active_minutes"]),
            reverse=True,
        )

        # KPIs adicionales
        inactive_7d_count = sum(1 for r in per_user_rows if r["is_inactive_7d"])
        adoption_rate = 0.0
        if per_user_rows:
            adopters = sum(1 for r in per_user_rows if r["adoption_score"] >= _ADOPTION_THRESHOLD)
            adoption_rate = round((adopters / len(per_user_rows)) * 100, 1)

        usage_alerts = _build_usage_alerts(per_user_rows)

        # ------------------------------------------------------------------
        # Respuesta final
        # ------------------------------------------------------------------
        return {
            "filters": {
                "date_from": start_date.isoformat(),
                "date_to": end_date.isoformat(),
                "role_filter": role_filter or "analistas_y_supervisores",
                "team_filter": team_filter or "",
                "view": view,
            },
            "kpis": {
                "connected_now": connected_now,
                "eligible_users": len(eligible_ids),
                "active_users": active_users,
                "uploads": uploads_total,
                "active_hours": total_active_hours,
                "avg_productivity_index": avg_prod,
                "adoption_rate": adoption_rate,
                "inactive_7d_count": inactive_7d_count,
            },
            "meta": {
                "available_groups": [
                    {"value": name, "label": name, "users": count}
                    for name, count in sorted(Counter((row["group"] or "Sin grupo") for row in per_user_rows).items())
                ],
                "admins_excluded_by_default": True,
                "adoption_threshold": _ADOPTION_THRESHOLD,
            },
            "alerts": usage_alerts,
            "charts": {
                "by_weekday": weekday_chart,
                "users_vs_users": users_vs_users_chart,
                "heatmap": heatmap_list,
                "sections": sections_list,
            },
            "per_user": per_user_rows,
        }

    finally:
        try:
            s.close()
        except Exception:
            pass


# ======================================================================
# Perfil individual
# ======================================================================

def get_user_profile_timeline(session, user_id: int) -> Dict[str, Any]:
    """
    Devuelve el perfil completo de un usuario con:
    - Datos básicos + grupo + fecha de alta
    - Stats consolidados (sesiones, horas, cargas, búsquedas, exportaciones)
    - Score de adopción
    - Estado online actual
    - Timeline de las últimas 200 acciones (con labels legibles)
    """
    return _get_user_profile_timeline_impl(session, user_id)

    u = session.query(User).filter(User.id == user_id).first()
    if not u:
        return {"error": "Usuario no encontrado"}

    all_events_q = session.query(UsageEvent).filter(
        UsageEvent.user_id == user_id,
        func.lower(UsageEvent.action_type) != "api_call",
    )

    # Counts totales históricos
    try:
        uploads = all_events_q.filter(UsageEvent.action_type == "file_upload").count()
        searches = all_events_q.filter(UsageEvent.action_type == "search").count()
        exports = all_events_q.filter(UsageEvent.action_type == "export").count()
        views = all_events_q.filter(UsageEvent.action_type.in_(("page_view", "module_visit"))).count()
        total_events = all_events_q.count()
    except Exception:
        uploads = searches = exports = views = total_events = 0

    # Sesiones y horas activas (últimos 90 días para performance)
    cutoff_90d = dt.datetime.utcnow() - dt.timedelta(days=90)
    try:
        recent_events = (
            all_events_q
            .filter(UsageEvent.timestamp >= cutoff_90d)
            .order_by(UsageEvent.timestamp.asc())
            .all()
        )
    except Exception:
        recent_events = []

    last_seen = None
    active_days_set: set = set()
    for ev in recent_events:
        if ev.timestamp is None:
            continue
        if last_seen is None or ev.timestamp > last_seen:
            last_seen = ev.timestamp
        active_days_set.add(ev.timestamp.date())

    session_data = _sessionize_events(recent_events)
    sessions_count = int(session_data["count"])
    active_minutes = float(session_data["active_minutes"])
    active_days = len(active_days_set)

    # Grupos del usuario
    try:
        group_data = (
            session.query(Group.name)
            .join(GroupMember, GroupMember.group_id == Group.id)
            .filter(GroupMember.user_id == user_id)
            .all()
        )
        group_names = [g[0] for g in group_data]
    except Exception:
        group_names = []

    modules_counter: Counter = Counter()
    for ev in recent_events:
        if (ev.action_type or "").lower() == "heartbeat":
            continue
        section_name = _map_section_name(ev.section or "")
        if section_name and section_name != "Sin Sección":
            modules_counter[section_name] += 1

    adoption_score = _adoption_score(
        active_days,
        active_minutes,
        uploads,
        searches,
        exports,
        len(modules_counter),
    )

    live_info = _build_live_presence_map(session, {int(user_id)}).get(int(user_id))
    current_status = live_info["status"] if live_info else ("Sin señal" if last_seen else "Sin acceso")
    risk_level = "alto" if last_seen is None or (dt.datetime.utcnow() - last_seen).days >= 7 else (
        "medio" if adoption_score < _ADOPTION_THRESHOLD else "bajo"
    )

    # Timeline: últimas 200 acciones
    try:
        timeline_events = (
            all_events_q
            .filter(func.lower(UsageEvent.action_type) != "heartbeat")
            .order_by(UsageEvent.timestamp.desc())
            .limit(200)
            .all()
        )
    except Exception:
        timeline_events = []

    timeline = []
    for ev in timeline_events:
        timeline.append({
            "id": ev.id,
            "timestamp": ev.timestamp.isoformat() + "Z" if ev.timestamp else None,
            "action_type": ev.action_type,
            "action_label": _map_action_label(ev.action_type or ""),
            "activity_type": _get_activity_category(ev.action_type or ""),
            "section": _map_section_name(ev.section or ""),
            "description": _timeline_detail(ev),
            "duration_ms": ev.duration_ms,
            "extra_data": ev.extra_data or {},
        })

    evolution_map: Dict[str, Dict[str, Any]] = {}
    for ev in recent_events:
        if ev.timestamp is None or (ev.action_type or "").lower() == "heartbeat":
            continue
        week_start = ev.timestamp.date() - dt.timedelta(days=ev.timestamp.weekday())
        key = week_start.isoformat()
        bucket = evolution_map.setdefault(
            key,
            {"week_start": week_start.isoformat(), "label": week_start.strftime("%d/%m"), "events": 0, "uploads": 0, "searches": 0, "exports": 0},
        )
        bucket["events"] += 1
        action = (ev.action_type or "").lower()
        if action == "file_upload":
            bucket["uploads"] += 1
        elif action == "search":
            bucket["searches"] += 1
        elif action == "export":
            bucket["exports"] += 1

    profile_alerts = _build_usage_alerts([{
        "user_id": u.id,
        "name": u.full_name or u.name or u.email.split("@")[0].title(),
        "role": _role_label(u.role),
        "group": _primary_group(group_names),
        "adoption_score": adoption_score,
        "sessions": sessions_count,
        "active_hours": round(active_minutes / 60.0, 1),
        "uploads": uploads,
        "searches": searches,
        "last_seen_days": (dt.datetime.utcnow() - last_seen).days if last_seen else 999,
        "trend_direction": "stable",
        "trend_delta_pct": 0.0,
    }])

    return {
        "user_id": u.id,
        "name": u.full_name or u.name or u.email.split("@")[0].title(),
        "email": u.email,
        "role": _role_label(u.role),
        "unit_business": (getattr(u, "unit_business", None) or "Sin unidad").title(),
        "group_names": group_names,
        "created_at": u.created_at.isoformat() + "Z" if getattr(u, "created_at", None) else None,
        "adoption_score": adoption_score,
        "risk_level": risk_level,
        "current_status": {
            "status": current_status,
            "status_tone": live_info["status_tone"] if live_info else "muted",
            "current_section": live_info["current_section"] if live_info else None,
            "last_action": live_info["last_action"] if live_info else "Sin actividad reciente",
            "last_action_ts": live_info["last_action_ts"] if live_info else None,
            "last_signal": live_info["last_signal"] if live_info else (last_seen.isoformat() + "Z" if last_seen else None),
            "activity_type": live_info["activity_type"] if live_info else "sin señal",
            "session_start": live_info["session_start"] if live_info else None,
        },
        "stats": {
            "sessions": sessions_count,
            "active_hours": round(active_minutes / 60.0, 1),
            "active_days": active_days,
            "views": views,
            "downloads": exports,
            "uploads": uploads,
            "searches": searches,
            "exports": exports,
            "modules_used": len(modules_counter),
            "modules_used_list": [name for name, _ in modules_counter.most_common(8)],
            "frequency": _usage_frequency(active_days, 90, sessions_count),
            "total_events": total_events,
            "last_seen": last_seen.isoformat() + "Z" if last_seen else None,
        },
        "recent_sessions": session_data["recent"],
        "modules": [{"section": name, "events": count} for name, count in modules_counter.most_common(8)],
        "usage_evolution": [bucket for _, bucket in sorted(evolution_map.items())][-8:],
        "alerts": profile_alerts,
        "timeline": timeline,
    }
