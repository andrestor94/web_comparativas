from fastapi import FastAPI
import os

print("DEBUG: Nuclear option started", flush=True)

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Nuclear Hello World"}

@app.get("/ping")
def ping():
    return {"status": "ok", "mode": "nuclear"}

print("DEBUG: App created, ready to fly", flush=True)
