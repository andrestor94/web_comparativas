
import pandas as pd
import json
import io
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from typing import List, Dict, Any
from web_comparativas.auth import get_current_user
from web_comparativas.models import User

router = APIRouter(prefix="/api/mercado-privado/dimensiones", tags=["dimensiones"])

# Normalization Maps
PROVINCE_MAP = {
    "caba": "CABA",
    "ciudad autonoma de buenos aires": "CABA",
    "capital federal": "CABA",
    "buenos aires": "Buenos Aires",
    "pba": "Buenos Aires",
    "catamarca": "Catamarca",
    "chaco": "Chaco",
    "chubut": "Chubut",
    "cordoba": "Córdoba",
    "corrientes": "Corrientes",
    "entren rios": "Entre Ríos",
    "entre rios": "Entre Ríos",
    "formosa": "Formosa",
    "jujuy": "Jujuy",
    "la pampa": "La Pampa",
    "la rioja": "La Rioja",
    "mendoza": "Mendoza",
    "misiones": "Misiones",
    "neuquen": "Neuquén",
    "rio negro": "Río Negro",
    "salta": "Salta",
    "san juan": "San Juan",
    "sj": "San Juan",
    "san luis": "San Luis",
    "santa cruz": "Santa Cruz",
    "santa fe": "Santa Fe",
    "santiago del estero": "Santiago del Estero",
    "tierra del fuego": "Tierra del Fuego",
    "tucuman": "Tucumán",
    # Abbreviations
    "ba": "Buenos Aires",
    "cat": "Catamarca",
    "cba": "Córdoba",
    "cf": "CABA",
    "cht": "Chubut",
    "er": "Entre Ríos",
    "lp": "La Pampa",
    "mza": "Mendoza",
    "neu": "Neuquén",
    "rn": "Río Negro",
    "sde": "Santiago del Estero",
    "sfe": "Santa Fe",
    "slt": "Salta",
    "tuc": "Tucumán"
}

def normalize_province(val):
    if not val:
        return "Desconocido"
    s = str(val).lower().strip()
    return PROVINCE_MAP.get(s, val) # Fallback to original if not found

def parse_number(val):
    if pd.isna(val): return 0
    if isinstance(val, (int, float)): return val
    s = str(val).strip()
    s = s.replace('$', '').replace(' ', '')
    # Handle European/Latam formats (1.234,56 vs 1,234.56)
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'): # 1.234,56
             s = s.replace('.', '').replace(',', '.')
        else: # 1,234.56
             s = s.replace(',', '')
    elif ',' in s:
        s = s.replace(',', '.')
    
    try:
        return float(s)
    except:
        return 0

@router.post("/process")
async def process_dimensiones(
    json_file: UploadFile = File(...),
    excel_file: UploadFile = File(...),
    user: User = Depends(get_current_user)
):
    try:
        # 1. Read JSON
        json_content = await json_file.read()
        try:
            json_data = json.loads(json_content)
        except json.JSONDecodeError:
            raise HTTPException(400, "El archivo JSON no es válido.")
        
        # Handle various JSON wrappers
        if isinstance(json_data, dict):
            for k in ['data', 'rows', 'results', 'items']:
                if k in json_data and isinstance(json_data[k], list):
                    json_data = json_data[k]
                    break
        
        if not isinstance(json_data, list):
             raise HTTPException(400, "Formato JSON no reconocido (se espera una lista de objetos).")

        df_json = pd.DataFrame(json_data)
        
        # Normalize columns to lowercase
        df_json.columns = [c.lower().strip() for c in df_json.columns]

        # 2. Excel -> DataFrame
        excel_content = await excel_file.read()
        df_excel = pd.read_excel(io.BytesIO(excel_content))
        
        # Helper to normalize column names strings (No external deps)
        def clean_col_name(n):
            s = str(n).lower().strip().replace(' ','_').replace('.', '')
            replacements = {'á':'a', 'é':'e', 'í':'i', 'ó':'o', 'ú':'u', 'ñ':'n', 'ü':'u'}
            for old, new in replacements.items():
                s = s.replace(old, new)
            return s

        df_json.columns = [clean_col_name(c) for c in df_json.columns]
        df_excel.columns = [clean_col_name(c) for c in df_excel.columns]

        print(f"DEBUG: Normalized JSON Columns: {df_json.columns.tolist()}")
        print(f"DEBUG: Normalized Excel Columns: {df_excel.columns.tolist()}")

        # --- JSON Key Normalization (Strict) ---
        # Explicitly use 'codigo_suizo' as verified from file inspection
        # This handles cases where values are loaded as floats (e.g., 8026567.0 -> '8026567')
        target_json_col = 'codigo_suizo'
        if target_json_col in df_json.columns:
            df_json['codigo_suizo_key'] = (
                df_json[target_json_col]
                .astype(str)
                .str.replace(r'\.0$', '', regex=True)
                .str.strip()
                .str.lower()
            )
        else:
             # Fallback only if strict column is missing
             print(f"WARNING: '{target_json_col}' column missing in JSON. Available: {df_json.columns.tolist()}")
             df_json['codigo_suizo_key'] = 'sin_dato'

        # --- Excel Key Normalization (Strict) ---
        target_excel_col = 'codigo'
        if target_excel_col in df_excel.columns:
            df_excel['codigo_key'] = (
                df_excel[target_excel_col]
                .astype(str)
                .str.replace(r'\.0$', '', regex=True)
                .str.strip()
                .str.lower()
            )
        else:
            print(f"WARNING: '{target_excel_col}' column missing in Excel. Available: {df_excel.columns.tolist()}")
            df_excel['codigo_key'] = 'sin_dato'

        # Deduplicate Excel briefly to avoid huge explosion, but keep it simple
        if 'codigo_key' in df_excel.columns:
            df_excel = df_excel.drop_duplicates(subset=['codigo_key'])

        # 4. Merge
        merged = pd.merge(
            df_json,
            df_excel,
            left_on='codigo_suizo_key',
            right_on='codigo_key',
            how='left'
        )
        
        # --- DEBUG LOGGING ---
        try:
             total_input = len(df_json)
             merged_len = len(merged)
             # Check if we have valid excel matches (non-null 'codigo_key')
             matched = merged['codigo_key'].notna().sum()
             print(f"--- DIMENSIONES DEBUG ---")
             print(f"JSON Rows: {total_input}")
             print(f"Merge Result: {merged_len}")
             print(f"Matches Found: {matched} ({matched/merged_len*100:.2f}%)")
             print(f"Cols in Merged: {merged.columns.tolist()[:20]}...")
             print(f"-------------------------")
        except Exception as e:
             print(f"Debug Log Error: {e}")

        # 5. Process
        final_rows = []
        for _, row in merged.iterrows():
            # Basic Extraction (Strict Mapping)
            raw_date = row.get('apertura') or row.get('fecha')
            client = row.get('hospital') or 'Desconocido'
            proceso = row.get('proceso') or str(row.get('id', ''))
            
            # Map Families (Fix: Check both 'familia_y' and 'familia')
            # If no collision, pandas uses 'familia'. If collision (unlikely here), 'familia_y'.
            familia = row.get('familia_y') or row.get('familia')
            if pd.isna(familia) or str(familia).strip() == '':
                 familia = 'Sin Familia'

            # Sub-Business Unit / Category (Check 'desneg' or 'desneg_y')
            categoria = row.get('desneg_y') or row.get('desneg')
            if pd.isna(categoria):
                 categoria = 'Sin Categoría'

            # Product (new field from JSON)
            producto = row.get('producto') or 'Sin Producto'

            # Quantities
            qty = parse_number(row.get('cantidad'))
            price = parse_number(row.get('precio_unitario') or 0)

            # Excel Data (Price & Date)
            excel_price = parse_number(row.get('predrog'))
            excel_date_raw = row.get('fchpre')
            excel_date = str(excel_date_raw) if pd.notna(excel_date_raw) else None

            # Province (JSON 'provincia')
            prov_raw = row.get('provincia') 
            prov_norm = normalize_province(prov_raw)

            # Result (JSON 'resultado')
            res = row.get('resultado') or 'Desconocido'
            
            # Relation (Logic based on CUIT or explicit 'relación' field if exists)
            # User didn't specify strict logic for relation, but 'relación' key exists in JSON.
            # Fallback to CUIT logic if 'relación' is missing or ambiguous.
            relation_val = row.get('relación')
            if relation_val and isinstance(relation_val, str) and len(relation_val) > 2:
                 relation = relation_val.title()
                 is_client = (relation.lower() == 'cliente')
            else:
                # CUIT Fallback
                cuit_val = row.get('cuit')
                is_client = False
                if pd.notna(cuit_val):
                    s_cuit = str(cuit_val).replace('.','').replace('-','').strip()
                    if len(s_cuit) >= 10 and s_cuit.isdigit() and s_cuit != '0':
                        is_client = True
                relation = "Cliente" if is_client else "No Cliente"          

            # Identified Status
            # We check if 'codigo_suizo_key' from the merge is not 'sin_dato'
            # Note: During merge we used 'codigo_suizo_key'. 
            # We can check row['codigo_suizo_key'] if preserved, but pandas merge might rename it.
            # safer: check if 'codigo_key' (from excel) is not null/nan.
            # In the merge step:
            # merged = pd.merge(df_json, df_excel, left_on='codigo_suizo_key', right_on='codigo_key', how='left')
            # If match found, 'codigo_key' will be present.
            is_identified = pd.notna(row.get('codigo_key'))

            final_rows.append({
                "date": str(raw_date),
                "client": client,
                "process_id": str(proceso),
                "family": str(familia),
                "category": str(categoria),
                "product": str(producto), # NEW
                "province": prov_norm,
                "result": str(res),
                "quantity": qty,
                "price": price,
                "relation": relation,
                "is_client_bool": is_client,
                "excel_price": excel_price, # NEW
                "excel_date": excel_date,   # NEW
                "identified": bool(is_identified) # NEW
            })

        return {
            "ok": True,
            "count": len(final_rows),
            "data": final_rows
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error processing dimensions: {str(e)}")
        # Return structured error to frontend for alert
        raise HTTPException(status_code=400, detail=f"Error interno: {str(e)}")
