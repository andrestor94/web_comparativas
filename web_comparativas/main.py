
from pathlib import Path
import logging
import os
import re
from typing import Optional
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session
from .models import SessionLocal, db_session, User
from .auth import hash_password, verify_password

# Setup
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app = FastAPI()

# ======================================================================
# LOGGING (Console Only)
# ======================================================================
logger = logging.getLogger("wc.main")
logger.setLevel(logging.INFO)

# ======================================================================
# AUTH HELPERS
# ======================================================================
def _reset_session():
    try:
        if hasattr(db_session, "remove"):
            db_session.remove()
        else:
            db_session.close()
    except Exception:
        pass

def get_current_user(request: Request) -> Optional[User]:
    uid = request.session.get("uid")
    if not uid:
        return None
    try:
        return db_session.get(User, uid)
    except Exception:
        _reset_session()
        try:
            return db_session.get(User, uid)
        except Exception:
            return None

def user_display(u: Optional[User]) -> str:
    if not u:
        return ""
    for attr in ("name", "full_name", "nombre"):
        v = getattr(u, attr, None)
        if v and str(v).strip():
            return str(v).strip()
    email = getattr(u, "email", "") or ""
    alias = email.split("@")[0] if "@" in email else email
    alias = re.sub(r"[._-]+", " ", alias).strip().title()
    return alias or ""

templates.env.globals["user_display"] = user_display

# ======================================================================
# MIDDLEWARES
# ======================================================================

@app.middleware("http")
async def db_session_lifecycle(request: Request, call_next):
    # Reset session before request to avoid stale state
    _reset_session()
    
    request.state.db = SessionLocal()
    try:
        response = await call_next(request)
        request.state.db.commit()
        return response
    except Exception:
        request.state.db.rollback()
        raise
    finally:
        request.state.db.close()

@app.middleware("http")
async def attach_user_to_state(request: Request, call_next):
    u = get_current_user(request)
    request.state.user = u
    request.state.user_display = user_display(u) if u else ""
    response = await call_next(request)
    return response

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("APP_SECRET", "dev-secret-123"),
    session_cookie="wc_session",
    https_only=False,
    max_age=60 * 60 * 24 * 7,
)

# ======================================================================
# ROUTERS
# ======================================================================

# Importar y conectar SIC
from .routers.sic_router import router as sic_router
app.include_router(sic_router)

# ======================================================================
# BASIC ROUTES
# ======================================================================

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # Si tenemos usuario, mostramos la home "real" (o redireccionamos)
    # Por ahora, landing simple
    return templates.TemplateResponse("home.html", {
        "request": request, 
        "user": request.state.user,
        "bg_class": "bg-gray-100"
    })

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_post(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    password = str(form.get("password", "")).strip()

    user = db_session.query(User).filter(User.email == email).first()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {
            "request": request, 
            "error": "Credenciales inválidas"
        })
    
    # Login OK
    request.session["uid"] = user.id
    return RedirectResponse("/", status_code=303)

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

@app.get("/ping")
def ping():
    return {"status": "ok", "stage": "sic_restored"}

# Static
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

print("DEBUG: App Initialized with SIC Router + Auth", flush=True)

