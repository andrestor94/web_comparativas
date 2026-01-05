
import re

file_path = "web_comparativas/legacy_routes.py"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# 1. Replace app instantiation with APIRouter
if "from fastapi import APIRouter" not in content:
    content = content.replace("from fastapi import (", "from fastapi import (\n    APIRouter,")
    
content = re.sub(r'app\s*=\s*FastAPI\(\)', 'router = APIRouter() # Replaced FastAPI app', content)

# 2. Comment out Startup Events
content = re.sub(r'@app\.on_event\("startup"\)', '# @router.on_event("startup") [DISABLED]', content)

# 3. Comment out Middlewares (Global middlewares stay in main.py)
content = re.sub(r'@app\.middleware\(', '# @app.middleware(', content)

# 4. Replace Route Decorators
content = re.sub(r'@app\.get\(', '@router.get(', content)
content = re.sub(r'@app\.post\(', '@router.post(', content)
content = re.sub(r'@app\.put\(', '@router.put(', content)
content = re.sub(r'@app\.delete\(', '@router.delete(', content)
content = re.sub(r'@app\.options\(', '@router.options(', content)
content = re.sub(r'@app\.head\(', '@router.head(', content)
content = re.sub(r'@app\.patch\(', '@router.patch(', content)

# 5. Comment out include_router calls
content = re.sub(r'app\.include_router\(', '# app.include_router(', content)

# 6. Disable CONFLICTING routes (already in main.py)
duplicates = [
    r'@router\.get\("/"\)',
    r'@router\.get\("/login"\)',
    r'@router\.get\("/logout"\)',
    r'@router\.get\("/mercado-privado"\)',
    r'@router\.get\("/mercado-privado/dimensiones"\)',
    r'@router\.get\("/mercado-publico"\)',
    r'@router\.get\("/mercado-publico/web-comparativas"\)',
]
for p in duplicates:
    clean_p = p.replace(r"\\", "")
    content = re.sub(p, f'# [CONFLICT] {clean_p} DISABLED', content)

# 7. Fix logging setup


with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Sanitized legacy_routes.py successfully.")
