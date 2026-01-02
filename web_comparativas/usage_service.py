# web_comparativas/usage_service.py
import datetime as dt
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy import func

from .models import db_session, UsageEvent, User, visible_user_ids


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
    Si el usuario no manda fechas, usamos los últimos 30 días.
    Si manda solo una, usamos esa como límite y completamos con 30 días.
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

    # ninguno: últimos 30 días
    # ninguno: todo el histórico (o desde inicio 2024)
    d_to = today
    # Fecha "histórica" fija para traer todo por defecto, o podríamos buscar min() en DB. 
    # Para ser prácticos y veloces, ponemos una fecha arbitraria de inicio de proyecto.
    d_from = dt.date(2024, 1, 1)
    return d_from, d_to


# ======================================================================
# Registro de eventos
# ======================================================================

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
        try:
            s.rollback()
        except Exception:
            pass
    finally:
        try:
            s.close()
        except Exception:
            pass


def _map_section_name(raw: str) -> str:
    original = raw.strip()
    # Mapa de nombres amigables
    mapping = {
        "home": "Inicio",
        "sic": "S.I.C. General",
        "sic_usuarios": "Gestión de Usuarios",
        "sic_helpdesk": "Mesa de Ayuda",
        "sic_tracking": "Seguimiento de Usuarios",
        "sic_tracking_api": "API Tracking (Interno)",
        "mercado_privado": "Mercado Privado (Landing)",
        "mercado_privado_dimensiones": "Dimensionamiento",
        "mercado_publico": "Mercado Público",
        "auth": "Inicio de Sesión",
        "": "Sin Sección"
    }
    # Si esta exacto en el mapa
    if original in mapping:
        return mapping[original]
    
    # Fallback: Quitar guiones bajos y Capitalizar
    return original.replace("_", " ").title()

# ======================================================================
# Consultas agregadas para "Seguimiento de usuarios"
# ======================================================================

def get_usage_summary(
    *,
    current_user: User,
    date_from: str = "",
    date_to: str = "",
    role_filter: str | None = None,
    view: str = "day",
) -> Dict[str, Any]:
    """
    Devuelve un resumen completo para alimentar el dashboard de
    Seguimiento de usuarios.

    - Respeta la visibilidad del usuario (visible_user_ids).
    - Se centra en roles de Analista + Supervisor.
    """
    s = db_session()

    # Estructura vacía por si no hay datos o no ve a nadie
    def _empty(start: dt.date, end: dt.date) -> Dict[str, Any]:
        return {
            "filters": {
                "date_from": start.isoformat(),
                "date_to": end.isoformat(),
                "role_filter": role_filter or "analistas_y_supervisores",
                "view": view,
            },
            "kpis": {
                "active_users": 0,
                "uploads": 0,
                "active_hours": 0.0,
                "avg_productivity_index": 0.0,
            },
            "charts": {
                "by_weekday": [],
                "analysts_vs_supervisors": [],
                "heatmap": [],
                "sections": [],
            },
            "per_user": [],
        }

    start_date, end_date = _default_date_range(date_from, date_to)

    try:
        # 1) Qué usuarios puede ver el usuario actual
        visible_ids = visible_user_ids(s, current_user)
        if not visible_ids:
            return _empty(start_date, end_date)

        # 2) Base query: solo eventos de Analistas y Supervisores visibles
        allowed_roles = {"analista", "analyst", "supervisor"}
        base_q = (
            s.query(UsageEvent)
            .filter(
                UsageEvent.user_id.in_(visible_ids),
                func.lower(UsageEvent.user_role).in_(tuple(allowed_roles)),
            )
        )

        # Filtro de fechas (UTC) [start, end + 1 día)
        start_dt = dt.datetime.combine(start_date, dt.time.min)
        end_dt = dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time.min)
        base_q = base_q.filter(
            UsageEvent.timestamp >= start_dt,
            UsageEvent.timestamp < end_dt,
        )

        # Filtro de rol explícito (selector de la UI)
        rf = (role_filter or "").strip().lower()
        if rf == "analistas":
            base_q = base_q.filter(
                func.lower(UsageEvent.user_role).in_(("analista", "analyst"))
            )
        elif rf == "supervisores":
            base_q = base_q.filter(
                func.lower(UsageEvent.user_role).in_(("supervisor",))
            )
        # si es vacío o "analistas_y_supervisores", quedan ambos

        # Ordenamos por user_id + timestamp para armar sesiones luego
        events: List[UsageEvent] = (
            base_q.order_by(UsageEvent.user_id.asc(), UsageEvent.timestamp.asc()).all()
        )

        if not events:
            return _empty(start_date, end_date)

        # ------------------------------------------------------------------
        # 3) Recorrido principal: agregamos todo en memoria
        # ------------------------------------------------------------------
        SESSION_GAP_MIN = 30  # separación para cortar sesiones

        user_stats: Dict[int, Dict[str, Any]] = {}
        weekday_stats: Dict[int, Dict[str, Any]] = {
            i: {"weekday": i, "events": 0, "user_ids": set()} for i in range(7)
        }
        heatmap: Dict[Tuple[int, int], int] = {}  # (dow, hour) -> count
        section_stats: Dict[str, Dict[str, Any]] = {}
        role_stats: Dict[str, Dict[str, Any]] = {}

        for ev in events:
            uid = int(ev.user_id)
            role = (ev.user_role or "").lower() or "otro"

            st = user_stats.setdefault(
                uid,
                {
                    "user_id": uid,
                    "role": role,
                    "days": set(),
                    "events": 0,
                    "uploads": 0,
                    "duration_ms": 0,
                    "sessions": 0,
                    "active_minutes": 0.0,
                    "last_seen": None,
                    "_last_ts": None,
                    "_session_start": None,
                },
            )

            ts = ev.timestamp or dt.datetime.utcnow()
            day = ts.date()
            st["days"].add(day)
            st["events"] += 1
            if ev.action_type == "file_upload":
                st["uploads"] += 1
            if ev.duration_ms:
                st["duration_ms"] += int(ev.duration_ms)

            if st["last_seen"] is None or ts > st["last_seen"]:
                st["last_seen"] = ts

            # --- sesiones (por usuario) ---
            last_ts = st["_last_ts"]
            if last_ts is None:
                st["sessions"] = 1
                st["_session_start"] = ts
            else:
                gap_min = (ts - last_ts).total_seconds() / 60.0
                if gap_min > SESSION_GAP_MIN:
                    # cerramos sesión previa
                    start_s = st["_session_start"] or last_ts
                    dur_prev = max(
                        1.0, (last_ts - start_s).total_seconds() / 60.0
                    )
                    st["active_minutes"] += dur_prev
                    # nueva sesión
                    st["sessions"] += 1
                    st["_session_start"] = ts

            st["_last_ts"] = ts

            # --- actividad por día de la semana ---
            dow = ts.weekday()  # 0=lunes .. 6=domingo
            wd = weekday_stats[dow]
            wd["events"] += 1
            wd["user_ids"].add(uid)

            # --- heatmap día/hora ---
            key = (dow, ts.hour)
            heatmap[key] = heatmap.get(key, 0) + 1

            # --- secciones más usadas ---
            raw_sec = (ev.section or "").strip() 
            sec_name = _map_section_name(raw_sec) or "Otras"
            sec_st = section_stats.setdefault(
                sec_name, {"section": sec_name, "events": 0, "user_ids": set()}
            )
            sec_st["events"] += 1
            sec_st["user_ids"].add(uid)

            # --- stats por rol ---
            rs = role_stats.setdefault(
                role,
                {"role": role, "events": 0, "uploads": 0, "active_minutes": 0.0},
            )
            rs["events"] += 1
            if ev.action_type == "file_upload":
                rs["uploads"] += 1

        # Cerrar sesiones abiertas (último evento de cada usuario)
        for st in user_stats.values():
            last_ts = st["_last_ts"]
            start_s = st["_session_start"] or last_ts
            if last_ts is not None and start_s is not None:
                dur = max(1.0, (last_ts - start_s).total_seconds() / 60.0)
                st["active_minutes"] += dur

        # ------------------------------------------------------------------
        # 4) Enriquecer con datos del usuario (nombre, email)
        # ------------------------------------------------------------------
        user_ids = list(user_stats.keys())
        users = (
            s.query(User)
            .filter(User.id.in_(user_ids))
            .all()
        )
        users_by_id = {int(u.id): u for u in users}

        for uid, st in user_stats.items():
            u = users_by_id.get(uid)
            if u is not None:
                st["name"] = u.full_name or u.name or u.email
                st["email"] = u.email
                st["role"] = (u.role or st["role"] or "").lower()

        # Volcamos minutos activos a stats por rol
        for st in user_stats.values():
            r = (st["role"] or "otro").lower()
            rs = role_stats.setdefault(
                r,
                {"role": r, "events": 0, "uploads": 0, "active_minutes": 0.0},
            )
            rs["active_minutes"] += float(st["active_minutes"])

        # ------------------------------------------------------------------
        # 5) KPIs globales
        # ------------------------------------------------------------------
        active_users = len(user_stats)
        uploads_total = sum(st["uploads"] for st in user_stats.values())
        total_active_minutes = sum(st["active_minutes"] for st in user_stats.values())
        total_active_hours = round(total_active_minutes / 60.0, 1)

        # índice de productividad (uploads / sesiones)
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
        # a) Actividad por día de la semana
        weekday_chart = []
        weekday_labels = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
        for i in range(7):
            wd = weekday_stats[i]
            weekday_chart.append(
                {
                    "weekday_index": i,
                    "weekday_label": weekday_labels[i],
                    "events": int(wd["events"]),
                    "users": len(wd["user_ids"]),
                }
            )

        # b) Usuario vs Usuario (Top 15 por eventos)
        # Reemplazamos la lógica de roles por lógica de usuarios individuales
        # Usamos per_user_rows que ya calculamos abajo? No, lo calculamos recién en paso 7.
        # Lo calculamos acá on-the-fly usando user_stats
        
        # Convertimos user_stats a lista para ordenar
        user_list_for_chart = []
        for uid, st in user_stats.items():
             u_name = st.get("name") or str(uid)
             # Limpiar nombre si es email muy largo
             if "@" in u_name:
                 u_name = u_name.split("@")[0]
             
             user_list_for_chart.append({
                 "user_label": u_name,
                 "events": st["events"],
                 "active_hours": round(st["active_minutes"] / 60.0, 1)
             })
        
        # Ordenar por eventos descendente
        user_list_for_chart.sort(key=lambda x: x["events"], reverse=True)
        # Top 15
        user_list_for_chart = user_list_for_chart[:15]
        
        users_vs_users_chart = user_list_for_chart

        # c) Heatmap día/hora
        heatmap_list = []
        for (dow, hour), count in heatmap.items():
            heatmap_list.append(
                {
                    "weekday": dow,
                    "hour": hour,
                    "events": int(count),
                }
            )

        # d) Secciones más usadas
        sections_list = []
        for sec, st in section_stats.items():
            sections_list.append(
                {
                    "section": sec,
                    "events": int(st["events"]),
                    "users": len(st["user_ids"]),
                }
            )
        sections_list.sort(key=lambda r: r["events"], reverse=True)
        sections_list = sections_list[:20]

        # ------------------------------------------------------------------
        # 7) Resumen por usuario (tabla)
        # ------------------------------------------------------------------
        per_user_rows = []
        for uid, st in user_stats.items():
            u = users_by_id.get(uid)
            role_norm = (st["role"] or "").lower()
            role_label = "Analista" if role_norm in ("analista", "analyst") else (
                "Supervisor" if role_norm == "supervisor" else role_norm.capitalize()
            )
            active_days = len(st["days"])
            prod_idx = (
                round(st["uploads"] / float(st["sessions"]), 2)
                if st["sessions"] > 0
                else 0.0
            )

            per_user_rows.append(
                {
                    "user_id": uid,
                    "name": st.get("name") or (u.email if u else f"User {uid}"),
                    "email": st.get("email") or (u.email if u else None),
                    "role": role_label,
                    "active_days": active_days,
                    "sessions": int(st["sessions"]),
                    "uploads": int(st["uploads"]),
                    "events": int(st["events"]),
                    "active_minutes": round(float(st["active_minutes"]), 1),
                    "productivity_index": prod_idx,
                    "last_seen": st["last_seen"].isoformat() if st["last_seen"] else None,
                }
            )

        per_user_rows.sort(
            key=lambda r: (r["uploads"], r["active_minutes"], r["events"]),
            reverse=True,
        )

        # ------------------------------------------------------------------
        # Respuesta final
        # ------------------------------------------------------------------
        return {
            "filters": {
                "date_from": start_date.isoformat(),
                "date_to": end_date.isoformat(),
                "role_filter": role_filter or "analistas_y_supervisores",
                "view": view,
            },
            "kpis": {
                "active_users": active_users,
                "uploads": uploads_total,
                "active_hours": total_active_hours,
                "avg_productivity_index": avg_prod,
            },
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
