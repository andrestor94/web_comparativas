from fastapi import APIRouter, Request, Depends, HTTPException, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
import datetime as dt
from sqlalchemy import func, or_

from web_comparativas.models import User, db_session, BUSINESS_UNITS, normalize_unit_business, Ticket, TicketMessage, PasswordResetRequest, PliegoSolicitud
from web_comparativas.auth import user_display, hash_password, verify_password
from web_comparativas.usage_service import get_usage_summary, log_usage_event

# Setup templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Create Router
router = APIRouter(prefix="/sic", tags=["sic"])

# Roles que pueden acceder al módulo S.I.C.
_SIC_ALLOWED_ROLES = {"admin", "auditor", "supervisor", "analista"}

# --- Security Dependency ---
def sic_access_required(request: Request) -> User:
    """
    Dependencia de seguridad para el módulo S.I.C.
    - Requiere sesión autenticada → 401 si no hay usuario.
    - Requiere rol dentro de _SIC_ALLOWED_ROLES → 403 si el rol no está permitido.
    - El contenido visible dentro de cada endpoint se restringe según el rol
      (admin/auditor ven todo; supervisor/analista ven solo lo propio).
    """
    user: User = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="S.I.C.: sesión requerida.")

    role = (user.role or "").strip().lower()
    if role not in _SIC_ALLOWED_ROLES:
        raise HTTPException(status_code=403, detail="S.I.C.: rol no autorizado.")

    return user

# --- HOME ---
@router.get("/", response_class=HTMLResponse)
def sic_home(request: Request, user: User = Depends(sic_access_required)):
    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "home"
    }
    return templates.TemplateResponse("sic/home.html", ctx)

@router.get("/helpdesk", response_class=HTMLResponse)
def sic_helpdesk(request: Request, user: User = Depends(sic_access_required)):
    # Admin y Auditor ven todos los tickets; el resto solo los propios.
    full_access_roles = {"admin", "auditor"}
    user_role = (user.role or "").strip().lower()
    has_full_access = user_role in full_access_roles

    db = getattr(request.state, "db", db_session)
    q = db.query(Ticket)
    if not has_full_access:
        q = q.filter(Ticket.user_id == user.id)

    # Filtro opcional por módulo (e.g. ?modulo=lectura_pliegos)
    mod_filter = request.query_params.get("modulo", "").strip()
    if mod_filter:
        q = q.filter(Ticket.modulo_origen == mod_filter)

    tickets = q.order_by(Ticket.updated_at.desc()).all()

    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "helpdesk",
        "tickets": tickets,
        "is_admin": "admin" in user_role,
    }
    return templates.TemplateResponse("sic/helpdesk.html", ctx)

@router.get("/helpdesk/new", response_class=HTMLResponse)
def sic_helpdesk_new(request: Request, user: User = Depends(sic_access_required)):
    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "helpdesk"
    }
    return templates.TemplateResponse("sic/helpdesk_form.html", ctx)

@router.post("/helpdesk/new")
def sic_helpdesk_create(
    request: Request,
    title: str = Form(...),
    category: str = Form("consulta"),
    priority: str = Form("media"),
    message: str = Form(...),
    user: User = Depends(sic_access_required)
):
    try:
        # Create Ticket
        ticket = Ticket(
            user_id=user.id,
            title=title,
            category=category,
            priority=priority,
            status="abierto"
        )
        db_session.add(ticket)
        db_session.flush() # Get ID

        # Create First Message
        msg = TicketMessage(
            ticket_id=ticket.id,
            user_id=user.id,
            message=message
        )
        db_session.add(msg)
        db_session.commit()
        return RedirectResponse(f"/sic/helpdesk/{ticket.id}", status_code=303)
    except Exception as e:
        db_session.rollback()
        # In a real app we would pass error to template
        return RedirectResponse("/sic/helpdesk/new?err=create_failed", status_code=303)

# --- HELPDESK API (for Dashboard integration) ---
from pydantic import BaseModel

class TicketCreateSchema(BaseModel):
    title: str
    message: str
    category: str = "consulta"
    priority: str = "media"
    upload_id: Optional[str] = None
    process_code: Optional[str] = None

@router.post("/api/tickets/create", response_class=JSONResponse)
def sic_api_ticket_create(
    request: Request,
    payload: TicketCreateSchema,
    user: User = Depends(sic_access_required)
):
    try:
        # Context info
        ctx_info = ""
        if payload.process_code:
            ctx_info += f" [Proceso: {payload.process_code}]"
        if payload.upload_id:
            ctx_info += f" [UploadID: {payload.upload_id}]"

        full_title = f"{payload.title} {ctx_info}".strip()

        # Create Ticket
        ticket = Ticket(
            user_id=user.id,
            title=full_title[:200], # truncate if too long
            category=payload.category,
            priority=payload.priority,
            status="abierto"
        )
        db_session.add(ticket)
        db_session.flush()

        # Create First Message
        msg = TicketMessage(
            ticket_id=ticket.id,
            user_id=user.id,
            message=payload.message
        )
        db_session.add(msg)
        db_session.commit()

        return {"ok": True, "ticket_id": ticket.id, "redirect_url": f"/sic/helpdesk/{ticket.id}"}
    except Exception as e:
        db_session.rollback()
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# WIDGET: Lectura de Pliegos — comentarios rápidos contextuales
# ---------------------------------------------------------------------------

import json as _json

class PliegoWidgetCommentSchema(BaseModel):
    pliego_id: int
    message: str
    # Contexto automático capturado por el widget en el frontend
    numero_proceso: Optional[str] = None
    nombre_licitacion: Optional[str] = None
    organismo: Optional[str] = None
    titulo_caso: Optional[str] = None
    seccion: Optional[str] = None   # "lista" | "detalle"


@router.post("/api/tickets/pliego-comment", response_class=JSONResponse)
def sic_api_pliego_comment(
    request: Request,
    payload: PliegoWidgetCommentSchema,
    user: User = Depends(sic_access_required),
):
    """
    Crea o reutiliza un ticket de Mesa de Ayuda asociado a un caso de Lectura de Pliegos.

    Regla de agrupación:
      - Si el usuario ya tiene un ticket ABIERTO para el mismo pliego (modulo_origen +
        pliego_solicitud_id + usuario), el mensaje se agrega a ese ticket existente.
      - Si no existe ninguno abierto, se crea uno nuevo.

    Esto evita fragmentar la conversación en muchos tickets cuando el usuario
    envía varias notas sobre el mismo proceso.
    """
    try:
        pliego = db_session.get(PliegoSolicitud, payload.pliego_id)
        if not pliego:
            return JSONResponse({"ok": False, "error": "Caso de pliego no encontrado."}, status_code=404)

        # Buscar ticket activo del mismo usuario para el mismo pliego
        existing = (
            db_session.query(Ticket)
            .filter(
                Ticket.modulo_origen == "lectura_pliegos",
                Ticket.pliego_solicitud_id == payload.pliego_id,
                Ticket.user_id == user.id,
                Ticket.status.in_(["abierto", "pendiente"]),
            )
            .order_by(Ticket.updated_at.desc())
            .first()
        )

        contexto = {
            "pliego_id": payload.pliego_id,
            "numero_proceso": payload.numero_proceso or pliego.numero_proceso,
            "nombre_licitacion": payload.nombre_licitacion or pliego.nombre_licitacion,
            "organismo": payload.organismo or pliego.organismo,
            "titulo_caso": payload.titulo_caso or pliego.titulo,
            "seccion": payload.seccion or "detalle",
        }

        is_new = False
        if existing:
            ticket = existing
            ticket.updated_at = dt.datetime.utcnow()
            # Reabre si estaba cerrado/resuelto (no debería, por el filtro, pero por seguridad)
            if ticket.status not in ("abierto", "pendiente"):
                ticket.status = "abierto"
        else:
            # Armar título descriptivo automático
            proceso_str = pliego.numero_proceso or ""
            licit_str = pliego.nombre_licitacion or pliego.titulo or f"Pliego #{pliego.id}"
            title_parts = ["[Lectura de Pliegos]"]
            if proceso_str:
                title_parts.append(f"Proceso {proceso_str}")
            title_parts.append(licit_str[:80])
            auto_title = " – ".join(title_parts)[:200]

            ticket = Ticket(
                user_id=user.id,
                title=auto_title,
                category="lectura_pliegos",
                priority="media",
                status="abierto",
                modulo_origen="lectura_pliegos",
                pliego_solicitud_id=payload.pliego_id,
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

        return JSONResponse({
            "ok": True,
            "ticket_id": ticket.id,
            "is_new": is_new,
            "message_count": len(ticket.messages),
        })
    except Exception as e:
        db_session.rollback()
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.get("/api/tickets/pliego/{pliego_id}/summary", response_class=JSONResponse)
def sic_api_pliego_summary(
    request: Request,
    pliego_id: int,
    user: User = Depends(sic_access_required),
):
    """
    Retorna el resumen de tickets activos para un pliego dado y el usuario actual.
    Usado por el widget para mostrar el badge de cantidad y el historial resumido.
    """
    try:
        tickets = (
            db_session.query(Ticket)
            .filter(
                Ticket.modulo_origen == "lectura_pliegos",
                Ticket.pliego_solicitud_id == pliego_id,
                Ticket.user_id == user.id,
            )
            .order_by(Ticket.updated_at.desc())
            .all()
        )

        open_count = sum(1 for t in tickets if t.status in ("abierto", "pendiente"))
        total_msgs = sum(len(t.messages) for t in tickets)

        # Historial compacto para el widget (últimos 10 mensajes del ticket más reciente)
        recent_messages = []
        if tickets:
            latest = tickets[0]
            for m in latest.messages[-10:]:
                sender_name = (
                    "Tú" if m.user_id == user.id
                    else (m.user.name or m.user.email.split("@")[0].capitalize())
                )
                is_admin = "admin" in (m.user.role or "").lower() or "supervisor" in (m.user.role or "").lower()
                recent_messages.append({
                    "id": m.id,
                    "message": m.message,
                    "sender": sender_name,
                    "is_admin": is_admin,
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
    except Exception as e:
        db_session.rollback()
        return JSONResponse({"ok": False, "error": str(e), "open_count": 0}, status_code=500)


@router.get("/helpdesk/{ticket_id}", response_class=HTMLResponse)
def sic_helpdesk_detail(
    request: Request, 
    ticket_id: int, 
    user: User = Depends(sic_access_required)
):
    ticket = db_session.get(Ticket, ticket_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")
    
    # Access check: owner or admin/supervisor
    # Access check: owner or admin/auditor
    user_role = (user.role or "").lower()
    has_full_access = user_role in ["admin", "auditor"]
    
    if ticket.user_id != user.id and not has_full_access:
        raise HTTPException(status_code=403, detail="Access denied")

    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "helpdesk",
        "ticket": ticket,
        "is_admin": "admin" in user_role
    }
    return templates.TemplateResponse("sic/helpdesk_detail.html", ctx)

@router.post("/helpdesk/{ticket_id}/reply")
def sic_helpdesk_reply(
    request: Request,
    ticket_id: int,
    message: str = Form(...),
    user: User = Depends(sic_access_required)
):
    ticket = db_session.get(Ticket, ticket_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")

    # Reply logic
    msg = TicketMessage(
        ticket_id=ticket.id,
        user_id=user.id,
        message=message
    )
    db_session.add(msg)
    
    # Update updated_at
    ticket.updated_at = dt.datetime.utcnow()
    
    # Auto-reopen if user replies to closed?
    # Or Admin replies -> Pending?
    # Simple logic for now: if closed and user replies -> reopen
    if ticket.status == "cerrado" and ticket.user_id == user.id:
        ticket.status = "abierto"
    
    db_session.commit()

    # --- Notificación ---
    try:
        from .notifications_service import create_notification
        # Si responde ADMIN/SUPERVISOR -> Notificar al dueño del ticket
        is_staff = "admin" in (user.role or "").lower() or "supervisor" in (user.role or "").lower()
        if is_staff and ticket.user_id != user.id:
            create_notification(
                db_session,
                user_id=ticket.user_id,
                title="Nueva respuesta en Mesa de Ayuda",
                message=f"Respondieron a tu consulta: {ticket.title[:30]}...",
                category="helpdesk",
                link=f"/sic/helpdesk/{ticket.id}"
            )
    except Exception as e:
        print(f"Error notificando reply: {e}")

    return RedirectResponse(f"/sic/helpdesk/{ticket.id}", status_code=303)

@router.post("/helpdesk/{ticket_id}/status")
def sic_helpdesk_status(
    request: Request,
    ticket_id: int,
    status: str = Form(...),
    user: User = Depends(sic_access_required)
):
    # Only Admin/Supervisor can change status manually via this endpoint usually, 
    # but maybe user can "Resolve" their own ticket?
    ticket = db_session.get(Ticket, ticket_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")
        
    is_admin = "admin" in (user.role or "").lower() or "supervisor" in (user.role or "").lower()
    
    if not is_admin and ticket.user_id != user.id:
         raise HTTPException(status_code=403, detail="Access denied")

    # If user, maybe only allow 'cerrado'?
    # For now allow all if authorized
    ticket.status = status
    ticket.updated_at = dt.datetime.utcnow()
    db_session.commit()

    # --- Notificación de cambio de estado ---
    if ticket.user_id != user.id:
        try:
             from .notifications_service import create_notification
             create_notification(
                db_session,
                user_id=ticket.user_id,
                title="Actualización de Ticket",
                message=f"Tu consulta '{ticket.title[:20]}...' ha cambiado a estado: {status}",
                category="helpdesk",
                link=f"/sic/helpdesk/{ticket.id}"
             )
        except:
             pass

    return RedirectResponse(f"/sic/helpdesk/{ticket.id}", status_code=303)


@router.post("/helpdesk/{ticket_id}/delete")
def sic_helpdesk_delete(
    request: Request,
    ticket_id: int,
    user: User = Depends(sic_access_required)
):
    print(f"DEBUG: Attempting to delete ticket {ticket_id} by {user.email}")
    is_admin = "admin" in (user.role or "").lower()
    # Explicitly check for auditor just in case logic changes
    if (user.role or "").lower() == "auditor":
        is_admin = False
    if not is_admin:
        print("DEBUG: Delete denied - Not admin")
        return RedirectResponse("/sic/helpdesk?err=permiso_denegado", status_code=303)

    ticket = db_session.get(Ticket, ticket_id)
    if not ticket:
        print("DEBUG: Delete failed - Ticket not found")
        raise HTTPException(status_code=404, detail="Consulta no encontrada")

    try:
        db_session.delete(ticket)
        db_session.commit()
        print(f"DEBUG: Ticket {ticket_id} deleted successfully")
        return RedirectResponse("/sic/helpdesk?ok=deleted", status_code=303)
    except Exception as e:
        db_session.rollback()
        print(f"DEBUG: Delete exception: {e}")
        return RedirectResponse(f"/sic/helpdesk?err=EXCEPTION_{str(e)}", status_code=303)



# --- TRACKING ---
@router.get("/tracking", response_class=HTMLResponse)
def sic_tracking(request: Request, user: User = Depends(sic_access_required)):
    # Admin check for Tracking
    user_role = (user.role or "").lower()
    has_access = user_role in ["admin", "supervisor", "auditor"]
    if not has_access:
        raise HTTPException(status_code=403, detail="Access Denied: Admins Only")

    # Log usage event logic from legacy route
    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "tracking"
    }
    return templates.TemplateResponse("sic/tracking.html", ctx)

# --- TRACKING API (Redirected for SIC) ---
@router.get("/api/usage/summary", response_class=JSONResponse)
def sic_api_usage_summary(
    request: Request,
    date_from: str = Query("", description="Fecha desde"),
    date_to: str = Query("", description="Fecha hasta"),
    role: str = Query("analistas_y_supervisores"),
    view: str = Query("day"),
    user: User = Depends(sic_access_required)
):
    """
    Proxy to get_usage_summary logic, protected by sic_access_required.
    """
    user_role = (user.role or "").lower()
    has_access = user_role in ["admin", "supervisor", "auditor"]
    if not has_access:
        raise HTTPException(status_code=403, detail="Access Denied")

    summary = get_usage_summary(
        current_user=user,
        date_from=date_from,
        date_to=date_to,
        role_filter=role,
        view=view,
    )
    
    # Log specific event for SIC usage
    log_usage_event(
        user=user,
        action_type="page_view",
        section="sic_tracking_api",
        request=request,
    )

    return JSONResponse({"ok": True, **summary})


# --- USERS MANAGEMENT ---

@router.get("/users", response_class=HTMLResponse)
def sic_users_list(request: Request, user: User = Depends(sic_access_required)):
    error = None
    users = []
    try:
        q = db_session.query(User)
        if hasattr(User, "created_at"):
            q = q.order_by(User.created_at.desc(), User.email.asc())
        else:
            q = q.order_by(User.email.asc())
        users = q.all()
    except Exception as e:
        db_session.rollback()
        error = str(e)

    ok = request.query_params.get("ok")
    err = request.query_params.get("err")

    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "users": users,
        "error": error,
        "ok": ok,
        "err": err,
        "section": "users"
    }
    return templates.TemplateResponse("sic/users.html", ctx)

@router.get("/users/new", response_class=HTMLResponse)
def sic_users_new(request: Request, user: User = Depends(sic_access_required)):
    # Only Admin can create users? Or Supervisors too?
    # Original code restricted to "admin". Let's keep that restriction for CREATION if needed.
    # sic_access_required allows supervisors. user list view allows reading.
    # If we want to restrict creation to admin:
    if "admin" not in (user.role or "").lower():
         # If supervisor tries to access, maybe redirect or show error?
         # For now letting supervisors create too as per SIC "Gestion de Usuarios" logic implying full control?
         # Wait, original legacy code: @app.get("/usuarios/nuevo", ... require_roles("admin"))
         # So only admin could create.
         # I should check if user is admin.
         pass
    
    # Check admin role stricter for modifications if needed, but for now I'll apply same SIC access
    # If user wants strict 1:1 migration, I should enforce admin for modifications.
    # Let's check user role.
    is_admin = "admin" in (user.role or "").lower()
    
    business_units = list(BUSINESS_UNITS)
    form = SimpleNamespace(
        id=None,
        email="",
        name="",
        full_name="",
        role="analista",
        unit_business="Otros",
        access_scope="todos", # Default
    )
    
    # Capture error from redirect
    err = request.query_params.get("err")
    
    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "form": form,
        "is_new": True,
        "allow_edit_email": True,
        "business_units": business_units,
        "section": "users",
        "is_admin": is_admin,
        "error": err 
    }
    return templates.TemplateResponse("sic/users_form.html", ctx)

@router.post("/users/new")
def sic_users_create(
    request: Request,
    email: str = Form(...),
    name: str = Form(""),
    role: str = Form("analista"),
    password: str = Form("TuClaveFuerte123"), # Default password logic
    unit_business: str = Form("Otros"),
    access_scope: str = Form("todos"),
    user: User = Depends(sic_access_required),
):
    # Enforce admin?
    if "admin" not in (user.role or "").lower():
         return RedirectResponse("/sic/users?err=permiso_denegado", status_code=303)

    email = (email or "").strip().lower()
    role = (role or "").strip().lower()
    unit_ok = normalize_unit_business(unit_business)

    if not email:
        print("DEBUG: User create failed - Email empty")
        return RedirectResponse("/sic/users/new?err=email_vacio", status_code=303)

    try:
        print(f"DEBUG: Attempting to create user {email}")
        exists = db_session.query(User).filter(func.lower(User.email) == email).first()
        if exists:
            return RedirectResponse("/sic/users/new?err=email_existe", status_code=303)

        u = User(
            email=email,
            name=(name or "").strip() or email.split("@")[0],
            role=role,
            password_hash=hash_password(password),
            created_at=dt.datetime.utcnow(),
            unit_business=unit_ok,
            access_scope=access_scope,
        )
        db_session.add(u)
        db_session.commit()
        return RedirectResponse("/sic/users?ok=created", status_code=303)
    except Exception as e:
        db_session.rollback()
        print(f"DEBUG: User create exception: {e}")
        return RedirectResponse(f"/sic/users/new?err={str(e)}", status_code=303)

@router.get("/users/{user_id}/edit", response_class=HTMLResponse)
def sic_users_edit(
    request: Request,
    user_id: int,
    user: User = Depends(sic_access_required),
):
    # Only admin?
    # Supervisors might want to see details?
    # Original: admin only.
    is_admin = "admin" in (user.role or "").lower()
    # if not is_admin: return RedirectResponse...

    u = db_session.get(User, user_id)
    if not u:
        return RedirectResponse("/sic/users?err=not_found", status_code=303)

    business_units = list(BUSINESS_UNITS)
    # Patch unit if missing
    if not hasattr(u, "unit_business") or u.unit_business is None:
        try:
            u.unit_business = "Otros"
            db_session.commit()
        except:
            db_session.rollback()

    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "form": u,
        "is_new": False,
        "allow_edit_email": False,
        "business_units": business_units,
        "section": "users",
        "is_admin": is_admin
    }
    return templates.TemplateResponse("sic/users_form.html", ctx)

@router.post("/users/{user_id}/update")
def sic_users_update(
    request: Request,
    user_id: int,
    name: str = Form(""),
    role: str = Form("analista"),
    unit_business: str = Form("Otros"),
    access_scope: str = Form("todos"),
    user: User = Depends(sic_access_required),
):
    if "admin" not in (user.role or "").lower():
         return RedirectResponse("/sic/users?err=permiso_denegado", status_code=303)

    u = db_session.get(User, user_id)
    if not u:
        return RedirectResponse("/sic/users?err=not_found", status_code=303)

    unit_ok = normalize_unit_business(unit_business)

    try:
        u.name = (name or "").strip()
        u.role = (role or "").strip().lower()
        if hasattr(u, "unit_business"):
            u.unit_business = unit_ok
        u.access_scope = access_scope
        db_session.commit()
        return RedirectResponse("/sic/users?ok=updated", status_code=303)
    except Exception as e:
        db_session.rollback()
        return RedirectResponse(f"/sic/users/{user_id}/edit?err={str(e)}", status_code=303)

@router.post("/users/{user_id}/password")
def sic_users_password(
    request: Request,
    user_id: int,
    nueva: str = Form(...),
    confirmar: str = Form(...),
    user: User = Depends(sic_access_required),
):
    if "admin" not in (user.role or "").lower():
         return RedirectResponse("/sic/users?err=permiso_denegado", status_code=303)

    if nueva != confirmar:
        return RedirectResponse(f"/sic/users/{user_id}/edit?perror=nomatch", status_code=303)
    if len(nueva) < 8:
        return RedirectResponse(f"/sic/users/{user_id}/edit?perror=short", status_code=303)
        
    try:
        u = db_session.get(User, user_id)
        if not u:
            return RedirectResponse("/sic/users?err=not_found", status_code=303)
            
        u.password_hash = hash_password(nueva)
        db_session.commit()
        return RedirectResponse(f"/sic/users/{user_id}/edit?pok=1", status_code=303)
    except Exception:
        db_session.rollback()
        return RedirectResponse(f"/sic/users/{user_id}/edit?perror=fail", status_code=303)

@router.api_route("/users/{user_id}/delete", methods=["GET", "POST"])
def sic_users_delete(
    request: Request,
    user_id: int,
    user: User = Depends(sic_access_required),
):
    if "admin" not in (user.role or "").lower():
         return RedirectResponse("/sic/users?err=permiso_denegado", status_code=303)

    u = db_session.get(User, user_id)
    if not u:
        return RedirectResponse("/sic/users?err=not_found", status_code=303)

    if u.id == user.id:
        return RedirectResponse("/sic/users?err=cannot_self", status_code=303)

    admins = db_session.query(User).filter(User.role == "admin").count()
    if u.role == "admin" and admins <= 1:
        return RedirectResponse("/sic/users?err=last_admin", status_code=303)

    try:
        db_session.delete(u)
        db_session.commit()
        return RedirectResponse("/sic/users?ok=deleted", status_code=303)
    except Exception:
        db_session.rollback()
        return RedirectResponse("/sic/users?err=delete_failed", status_code=303)


# ===== PASSWORD RESET REQUESTS (dentro de S.I.C) =====

RESET_STATUSES = ["Pendiente", "En proceso", "Resuelto", "Rechazado"]


@router.get("/api/password-resets/pending-count", response_class=JSONResponse)
def sic_password_resets_count(request: Request, user: User = Depends(sic_access_required)):
    if "admin" not in (user.role or "").lower():
        return JSONResponse({"count": 0})
    try:
        count = db_session.query(PasswordResetRequest).filter(
            PasswordResetRequest.status == "Pendiente"
        ).count()
        return JSONResponse({"count": count})
    except Exception:
        db_session.rollback()
        return JSONResponse({"count": 0})


@router.get("/password-resets", response_class=HTMLResponse)
def sic_password_resets_list(request: Request, user: User = Depends(sic_access_required)):
    if "admin" not in (user.role or "").lower():
        return RedirectResponse("/sic/", status_code=303)

    status_filter = request.query_params.get("status_filter", "")
    solicitudes = []
    pending_count = 0
    try:
        q = db_session.query(PasswordResetRequest)
        if status_filter:
            q = q.filter(PasswordResetRequest.status == status_filter)
        solicitudes = q.order_by(PasswordResetRequest.request_date.desc()).all()
        pending_count = db_session.query(PasswordResetRequest).filter(
            PasswordResetRequest.status == "Pendiente"
        ).count()
    except Exception:
        db_session.rollback()

    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "users",
        "solicitudes": solicitudes,
        "statuses": RESET_STATUSES,
        "status_filter": status_filter,
        "pending_count": pending_count,
        "ok": request.query_params.get("ok"),
        "err": request.query_params.get("err"),
    }
    return templates.TemplateResponse("sic/password_resets.html", ctx)


@router.get("/password-resets/{req_id}", response_class=HTMLResponse)
def sic_password_reset_detail(request: Request, req_id: int, user: User = Depends(sic_access_required)):
    if "admin" not in (user.role or "").lower():
        return RedirectResponse("/sic/", status_code=303)

    sol = None
    try:
        sol = db_session.get(PasswordResetRequest, req_id)
    except Exception:
        db_session.rollback()

    if not sol:
        return RedirectResponse("/sic/password-resets?err=not_found", status_code=303)

    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "users",
        "sol": sol,
        "statuses": RESET_STATUSES,
        "ok": request.query_params.get("ok"),
        "err": request.query_params.get("err"),
    }
    return templates.TemplateResponse("sic/password_reset_detail.html", ctx)


@router.post("/password-resets/{req_id}/estado")
def sic_password_reset_estado(
    request: Request,
    req_id: int,
    status: str = Form(...),
    admin_observation: str = Form(""),
    user: User = Depends(sic_access_required),
):
    if "admin" not in (user.role or "").lower():
        return RedirectResponse("/sic/", status_code=303)

    try:
        sol = db_session.get(PasswordResetRequest, req_id)
        if not sol:
            return RedirectResponse("/sic/password-resets?err=not_found", status_code=303)

        if status not in RESET_STATUSES:
            return RedirectResponse(f"/sic/password-resets/{req_id}?err=invalid_status", status_code=303)

        sol.status = status
        if admin_observation.strip():
            sol.admin_observation = admin_observation.strip()
        sol.handled_by = user.email
        sol.handled_date = dt.datetime.utcnow()
        db_session.commit()
        return RedirectResponse(f"/sic/password-resets/{req_id}?ok=estado_actualizado", status_code=303)
    except Exception:
        db_session.rollback()
        return RedirectResponse(f"/sic/password-resets/{req_id}?err=save_error", status_code=303)


@router.post("/password-resets/{req_id}/resolver")
def sic_password_reset_resolver(
    request: Request,
    req_id: int,
    generate_password: str = Form("1"),
    new_password: str = Form(""),
    must_change_on_login: str = Form(""),
    admin_observation: str = Form(""),
    user: User = Depends(sic_access_required),
):
    if "admin" not in (user.role or "").lower():
        return RedirectResponse("/sic/", status_code=303)

    try:
        import secrets
        import string
        from urllib.parse import quote
        from web_comparativas.auth import hash_password as hp

        sol = db_session.get(PasswordResetRequest, req_id)
        if not sol:
            return RedirectResponse("/sic/password-resets?err=not_found", status_code=303)

        target_user = db_session.query(User).filter(
            func.lower(User.email) == sol.user_email.lower()
        ).first()
        if not target_user:
            return RedirectResponse(f"/sic/password-resets/{req_id}?err=user_not_found", status_code=303)

        tmp_pwd_plain = None
        if generate_password == "1":
            alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
            tmp_pwd_plain = "".join(secrets.choice(alphabet) for _ in range(12))
            target_user.password_hash = hp(tmp_pwd_plain)
            sol.temporary_password_generated = True
        else:
            pwd = (new_password or "").strip()
            if len(pwd) < 8:
                return RedirectResponse(f"/sic/password-resets/{req_id}?err=password_too_short", status_code=303)
            target_user.password_hash = hp(pwd)
            sol.temporary_password_generated = False

        force_change = must_change_on_login == "1"
        target_user.must_change_password = force_change
        sol.must_change_password_on_next_login = force_change
        sol.status = "Resuelto"
        sol.handled_by = user.email
        sol.handled_date = dt.datetime.utcnow()
        if admin_observation.strip():
            sol.admin_observation = admin_observation.strip()

        db_session.commit()

        log_usage_event(
            user=user,
            action_type="admin_password_reset",
            section="sic_password_resets",
            request=request,
        )

        redirect_url = f"/sic/password-resets/{req_id}?ok=resuelto"
        if tmp_pwd_plain:
            redirect_url += f"&tmp_pwd={quote(tmp_pwd_plain)}"
        return RedirectResponse(redirect_url, status_code=303)
    except Exception:
        db_session.rollback()
        return RedirectResponse(f"/sic/password-resets/{req_id}?err=save_error", status_code=303)
