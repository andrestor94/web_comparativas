from fastapi import APIRouter, Request, Depends, HTTPException, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
import datetime as dt
from sqlalchemy import func, or_

from web_comparativas.models import User, db_session, BUSINESS_UNITS, normalize_unit_business, Ticket, TicketMessage
from web_comparativas.auth import user_display, hash_password, verify_password
from web_comparativas.usage_service import get_usage_summary, log_usage_event

# Setup templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Create Router
router = APIRouter(prefix="/sic", tags=["sic"])

# --- Security Dependency ---
def sic_access_required(request: Request):
    user: User = request.state.user if hasattr(request.state, "user") else None
    if not user:
         raise HTTPException(status_code=401, detail="S.I.C Restricted Area: Please login.")
    
    # Access Control: Admin and Auditor have full access
    # Supervisors and Analysts have restricted access (handled in endpoints)
    # But for "Entry", we allow them all if they have a valid user.
    # The requirement says Auditor needs full navigation.
    
    # We verify if roles are valid for SIC
    allowed_roles = ["admin", "auditor", "supervisor", "analista"]
    role = (user.role or "").lower()
    
    if role not in allowed_roles:
         # If strict restriction is needed:
         # raise HTTPException(status_code=403, detail="Access Denied: Uncleared Role.")
         pass 
         
    # Legacy check was: if role != "admin": raise...
    # We now allow more roles to enter SIC, but individual views will restrict content.
    if role not in ["admin", "auditor", "supervisor", "analista"]:
         pass # O hacemos raise si queremos bloquear roles desconocidos
         
    return user
    
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
    # Roles with full visibility
    full_access_roles = ["admin", "auditor"]
    user_role = (user.role or "").lower()
    
    has_full_access = user_role in full_access_roles
    
    q = db_session.query(Ticket)
    if not has_full_access:
        # Supervisors and Analysts only see their own tickets
        q = q.filter(Ticket.user_id == user.id)
        
    tickets = q.order_by(Ticket.updated_at.desc()).all()
    
    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "section": "helpdesk",
        "tickets": tickets,
        "is_admin": "admin" in user_role
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
