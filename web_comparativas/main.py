from __future__ import annotations
from pathlib import Path
import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

print("DEBUG: STAGE 1 - Imports starting...", flush=True)

# 1. Setup Base Utils
BASE_DIR = Path(__file__).resolve().parent
print("DEBUG: BASE_DIR calculated", flush=True)

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
print("DEBUG: Templates initialized", flush=True)

app = FastAPI()
print("DEBUG: FastAPI app instance created", flush=True)

# 2. Mount Static
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
print("DEBUG: Static files mounted", flush=True)

# 3. Import Models (Test DB connection at module level)
print("DEBUG: Importing models (DB connection)...", flush=True)
try:
    from web_comparativas import models
    print("[SUCCESS] Models imported.", flush=True)
except Exception as e:
    print(f"[ERROR] Models import failed: {e}", flush=True)

# 4. Define minimal routes
@app.get("/ping")
def ping():
    return {"status": "ok", "stage": "1_models_only"}

@app.get("/")
def root():
    return {"message": "Stage 1: Models loaded, no middleware, no routers."}

print("DEBUG: STAGE 1 - Ready to run", flush=True)
