from __future__ import annotations
from pathlib import Path
import os
import sys

# DEBUG PRINT
print("DEBUG: STAGE 2 - Imports starting...", flush=True)

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware

# Database and Models
print("DEBUG: Importing models...", flush=True)
try:
    from web_comparativas import models
    print("[SUCCESS] Models imported.", flush=True)
except Exception as e:
    print(f"[ERROR] Models import failed: {e}", flush=True)

# Authentication Middleware (but NO router yet)
print("DEBUG: Importing Auth middleware...", flush=True)
from starlette.middleware.authentication import AuthenticationMiddleware
# We need a backend for Auth. Let's define a simple one or import if safe.
# Assuming web_comparativas.auth.BasicAuthBackend exists or similar.
# In the original main.py, it was likely using a Custom Backend or BasicAuth.
# Let's check how it was implemented in original code.
# Based on context, it likely uses a custom backend class.
# I will define a DUMMY backend for now to test the middleware infrastructure
# without risking auth.py dependencies if they are heavy.

from starlette.authentication import (
    AuthenticationBackend, AuthCredentials, SimpleUser, UnauthenticatedUser
)

class DummyBackend(AuthenticationBackend):
    async def authenticate(self, conn):
        return AuthCredentials(["authenticated"]), SimpleUser("test_user")

# BASE DIR
BASE_DIR = Path(__file__).resolve().parent
print(f"DEBUG: BASE_DIR={BASE_DIR}", flush=True)

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

print("DEBUG: Creating FastAPI app...", flush=True)
app = FastAPI()

# 1. CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
print("DEBUG: CORS Middleware added", flush=True)

# 2. Session
SECRET_KEY = os.getenv("SECRET_KEY", "super_secret_key_12345")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, https_only=False)
print("DEBUG: Session Middleware added", flush=True)

# 3. Auth (Dummy for now to test middleware impact)
app.add_middleware(AuthenticationMiddleware, backend=DummyBackend())
print("DEBUG: Auth Middleware (Dummy) added", flush=True)

# 4. Tracking - SKIPPED FOR NOW
# from web_comparativas.middleware.tracking import TrackingMiddleware
# app.add_middleware(TrackingMiddleware)
print("DEBUG: Tracking Middleware SKIPPED", flush=True)

# 5. Static
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# 6. Startup Event
@app.on_event("startup")
def run_startup_migrations():
    print("[MIGRATION] Startup event triggered.", flush=True)
    print("[MIGRATION] SKIPPED (Stage 2 Testing)", flush=True)

@app.get("/ping")
def ping():
    return {"status": "ok", "stage": "2_middleware_dummy_auth"}

@app.get("/")
def root():
    return {"message": "Stage 2: Models + Middleware (Session/CORS/Auth-Dummy) Loaded."}

print("DEBUG: STAGE 2 - Ready to run", flush=True)
