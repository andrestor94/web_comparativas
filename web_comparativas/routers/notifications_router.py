from fastapi import APIRouter, Request, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from sqlalchemy.orm import Session

from web_comparativas.models import User, db_session
from web_comparativas.notifications_service import (
    get_unread_count,
    get_user_notifications,
    mark_as_read,
    mark_all_as_read,
    delete_notification
)
from web_comparativas.auth import user_display

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter(prefix="/notifications", tags=["notifications"])

def login_required(request: Request):
    user = request.state.user if hasattr(request.state, "user") else None
    if not user:
        raise HTTPException(status_code=401, detail="Login required")
    return user

@router.get("/unread-count")
def api_unread_count(user: User = Depends(login_required)):
    count = get_unread_count(db_session, user.id)
    return {"count": count}

@router.get("/", response_class=HTMLResponse)
def page_notifications(request: Request, user: User = Depends(login_required)):
    # Renderizamos la página de notificaciones
    # Obtenemos las primeras 50 (o paginamos)
    notifs = get_user_notifications(db_session, user.id, limit=50)
    
    ctx = {
        "request": request,
        "user": user,
        "user_display": user_display,
        "notifications": notifs,
        "section": "notifications"
    }
    return templates.TemplateResponse("notifications.html", ctx)

@router.post("/{notif_id}/read")
def api_mark_read(notif_id: int, user: User = Depends(login_required)):
    success = mark_as_read(db_session, notif_id, user.id)
    if not success:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"ok": True}

@router.post("/read-all")
def api_mark_all_read(user: User = Depends(login_required)):
    mark_all_as_read(db_session, user.id)
    return {"ok": True}

@router.delete("/{notif_id}")
def api_delete_notif(notif_id: int, user: User = Depends(login_required)):
    success = delete_notification(db_session, notif_id, user.id)
    if not success:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"ok": True}
