from fastapi import APIRouter, Request, UploadFile, File, Depends
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import List
import pandas as pd
from pathlib import Path
import io
import traceback
import math
import uuid
import shutil

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _require_admin(request: Request):
    """Block non-admin users from pliegos routes."""
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse("/login", 303)
    if (user.role or "").lower() != "admin":
        return RedirectResponse("/mercado-publico", 303)
    return None

def peso(n):
    try:
        if n is None: return "No informado"
        return f"$ {float(n):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(n)

def pct(n):
    try:
        if n is None: return "—"
        # If it's a dict (ExtractedField), extract the value
        if isinstance(n, dict):
            n = n.get("value", n)
        if n is None: return "—"
        s = str(n).strip()
        # If it already contains %, return as-is
        if '%' in s:
            return s
        # Try to interpret as a decimal (0.05 = 5%)
        val = float(s)
        if val < 1:
            return f"{val*100:.0f}%"
        else:
            return f"{val:.0f}%"
    except (ValueError, TypeError):
        return str(n)

templates.env.filters["peso"] = peso
templates.env.filters["pct"] = pct

def clean_key(col_name: str) -> str:
    """Removes the category prefix from column names."""
    if "|" in col_name:
        return col_name.split("|", 1)[1].strip()
    return col_name

def parse_flat_excel(content: bytes):
    df = pd.read_excel(io.BytesIO(content))
    
    # === 1. EXTRACT ALL INFO (General Data) ===
    info = {}
    
    exclude_substrings = ["Renglón", "Ítem", "Detalle de productos", "Requisitos"]
    
    for col in df.columns:
        # Skip internal
        if col in ['TipoFila', 'Subsección']:
            continue
            
        # If it looks like Item/Req data, skip for General Info
        if any(ex in col for ex in exclude_substrings):
            continue

        # FIND VALUE: Scan entire column for a non-null value
        # This fixes the issue where Row 0 might be empty for some metadata
        valid_values = df[col].dropna()
        val = None
        if not valid_values.empty:
            # Take the first valid value found
            # Ideally they are all same, or we take the most common? 
            # First valid is usually safe for denormalized metadata
            val = valid_values.iloc[0]
            
        # Store if valid
        if val is not None and str(val).strip().lower() not in ["", "nan", "none"]:
             key = clean_key(col)
             info[key] = val

    # === 2. EXTRACT ITEMS (Renglones) ===
    items = []
    columns_items = []
    if 'TipoFila' in df.columns:
        # Filter for rows that are Renglones
        df_renglones = df[df['TipoFila'].astype(str).str.lower().str.contains('rengl', na=False)]
        
        target_cols = [c for c in df.columns if "Detalle de productos" in c or "Renglón" in c or "Ítem" in c]
        
        if not df_renglones.empty:
             for _, row in df_renglones.iterrows():
                item = {}
                for col in target_cols:
                    key = clean_key(col)
                    val = row[col]
                    if pd.notna(val) and str(val).lower() != "nan":
                         item[key] = val
                    else:
                         item[key] = "-"
                items.append(item)
             if items:
                columns_items = list(items[0].keys())

    # === 3. EXTRACT REQUIREMENTS ===
    requirements = []
    if 'TipoFila' in df.columns:
        df_reqs = df[df['TipoFila'].astype(str).str.lower().str.contains('requisito', na=False)]
        req_cols = [c for c in df.columns if "Requisitos" in c]
        
        if not df_reqs.empty:
            for _, row in df_reqs.iterrows():
                req = {}
                for col in req_cols:
                    key = clean_key(col)
                    val = row[col]
                    if pd.isna(val) or str(val).lower() == 'nan':
                        req[key] = None
                    else:
                        req[key] = val
                requirements.append(req)

    return info, items, requirements, columns_items


@router.get("/mercado-publico/pliegos", response_class=HTMLResponse)
def pliegos_form(request: Request):
    blocked = _require_admin(request)
    if blocked:
        return blocked
    user = request.state.user
    return templates.TemplateResponse("pliegos/form.html", {
        "request": request,
        "user": user
    })

@router.post("/mercado-publico/pliegos/upload", response_class=HTMLResponse)
async def pliegos_upload(request: Request, files: List[UploadFile] = File(...)):
    blocked = _require_admin(request)
    if blocked:
        return blocked
    user = request.state.user
    
    # Pre-check extensions
    allowed_exts = [".json", ".xlsx", ".pdf", ".jpg", ".jpeg", ".png", ".bmp", ".tiff"]
    for file in files:
        if not any(file.filename.lower().endswith(ext) for ext in allowed_exts):
             return templates.TemplateResponse("pliegos/form.html", {
                "request": request,
                "user": user,
                "error": f"El archivo '{file.filename}' no tiene una extensión válida."
            })

    try:
        # Build Files Map
        files_map = {}
        json_data = None
        main_filename = files[0].filename # Fallback name for view
        
        for file in files:
            content = await file.read()
            fname = file.filename.lower()
            
            # If ANY file is JSON, we prioritize it as Pre-Processed Result
            if fname.endswith(".json"):
                import json
                json_data = json.loads(content.decode("utf-8"))
                main_filename = file.filename
                
            files_map[file.filename] = content
            
        # 2. Case JSON (Prioritize if present)
        if json_data:
             items_list = json_data.get("items", [])
             return templates.TemplateResponse("pliegos/process_view.html", {
                 "request": request,
                 "user": user,
                 "data": json_data,
                 "items": items_list,
                 "filename": main_filename
             })

        # 3. Case PDF (Multi-Agent Processing)
        # We process ONLY if we have gathered PDFs
        from web_comparativas.tender_processor import scan_and_process_pdf
        
        data = await scan_and_process_pdf(files_map)
        
        # Guardar archivos localmente en 'static' para poder servirlos en el visor de la UI
        tender_id = data.get("tender_id", str(uuid.uuid4().hex[:12]))
        data["tender_id"] = tender_id  # Inject explicitly for the frontend
        
        upload_dir = BASE_DIR / "static" / "uploads" / "pliegos_ia" / tender_id
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        # Build documents metadata for the sidebar
        source_files = []
        for fn, content_bytes in files_map.items():
            # Save file
            save_path = upload_dir / fn
            try:
                with open(save_path, "wb") as f:
                    f.write(content_bytes)
                url_path = f"/static/uploads/pliegos_ia/{tender_id}/{fn}"
            except Exception as e:
                url_path = ""
                
            source_files.append({
                "filename": fn, 
                "doc_type": "Archivo cargado",
                "url": url_path
            })
            
        if "documents" not in data:
            data["documents"] = {"source_files": source_files}
        else:
            # Update existing documents if orchestrator returned something
            data["documents"]["source_files"] = source_files
        
        # If the orchestrator returned just an error (no data at all), show form 
        if "error" in data and "summary" not in data:
            raise Exception(data["error"])
                
        items_list = data.get("items", [])
        return templates.TemplateResponse("pliegos/process_view.html", {
                "request": request,
                "user": user,
                "data": data,
                "items": items_list,
                "filename": f"Lote de {len(files)} archivos"
            })

    except Exception as e:
        traceback.print_exc()
        return templates.TemplateResponse("pliegos/form.html", {
            "request": request,
            "user": user,
            "error": f"Error al procesar el lote: {str(e)}"
        })


from pydantic import BaseModel
from sqlalchemy.orm import Session
from web_comparativas.models import RevisionSession

class RevisionPayload(BaseModel):
    field_path: str
    original_value: str = None
    corrected_value: str = None
    confidence_at_revision: float = None

@router.post("/mercado-publico/pliegos/{tender_id}/revisions")
async def save_revision(tender_id: str, payload: RevisionPayload, request: Request):
    """ Endpoint for saving user corrections to inferred/extracted data """
    blocked = _require_admin(request)
    if blocked:
        return blocked
    db = request.state.db
    user = request.state.user
    
    try:
        new_rev = RevisionSession(
            tender_id=tender_id,
            user_id=user.id,
            field_path=payload.field_path,
            original_value=payload.original_value,
            corrected_value=payload.corrected_value,
            confidence_at_revision=payload.confidence_at_revision
        )
        db.add(new_rev)
        db.commit()
        return {"status": "success", "message": "Revisión guardada correctamente"}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
