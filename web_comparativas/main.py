
from pathlib import Path
import logging
from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse
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

# Middleware DB
@app.middleware("http")
async def db_session_lifecycle(request: Request, call_next):
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

# Middleware Session
app.add_middleware(
    SessionMiddleware,
    secret_key="dev-secret-123",
    session_cookie="wc_session",
    https_only=False,
)

# Dummy Auth Middleware helper
def get_current_user(request: Request):
    return None # Simplified

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request, "user": None})

@app.get("/ping")
def ping():
    return {"status": "ok", "stage": "nuclear_v2"}

