import pandas as pd
import json
import io

JSON_PATH = r"c:\Users\andre\OneDrive\Escritorio\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\data\archivos dimensionamiento\bionexo_estandarizado.json"
EXCEL_PATH = r"c:\Users\andre\OneDrive\Escritorio\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\data\archivos dimensionamiento\listas_hospitalarios.xlsx"

print("--- LOADING DATA ---")

# Load simplified JSON (only keys) to save time/memory, if possible? 
# JSON is struct, we can't easily partial load with pandas efficiently without parsing.
# We'll load regular way but maybe limit rows if too slow? 
# Using chunksize or just loading it (user said it is heavy). 350MB is fine for modern RAM.
with open(JSON_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

df_json = pd.DataFrame(data)
print(f"JSON Loaded. Shape: {df_json.shape}")
print(f"JSON Cols: {df_json.columns.tolist()}")

df_excel = pd.read_excel(EXCEL_PATH)
print(f"Excel Loaded. Shape: {df_excel.shape}")
print(f"Excel Cols: {df_excel.columns.tolist()}")

# --- LOGIC UNDER TEST ---

target_json = 'codigo_suizo'
target_excel = 'codigo'

print(f"\n--- NORMALIZING KEYS ---")
print(f"Using JSON Col: {target_json}")
print(f"Using Excel Col: {target_excel}")

# 1. JSON Norm
if target_json in df_json.columns:
    df_json['key'] = (
        df_json[target_json]
        .astype(str)
        .str.replace(r'\.0$', '', regex=True)
        .str.strip()
        .str.lower()
    )
    print("Sample JSON Keys (Normalized):", df_json['key'].head(10).tolist())
else:
    print("MISSING JSON COL")

# 2. Excel Norm
if target_excel in df_excel.columns:
    df_excel['key'] = (
        df_excel[target_excel]
        .astype(str)
        .str.replace(r'\.0$', '', regex=True)
        .str.strip()
        .str.lower()
    )
    print("Sample Excel Keys (Normalized):", df_excel['key'].head(10).tolist())
else:
    print("MISSING EXCEL COL")

# --- CHECK INTERSECTION ---
json_keys = set(df_json['key'].unique())
excel_keys = set(df_excel['key'].unique())

intersection = json_keys.intersection(excel_keys)
print(f"\nUnique JSON Keys: {len(json_keys)}")
print(f"Unique Excel Keys: {len(excel_keys)}")
print(f"INTERSECTION SIZE: {len(intersection)}")

print("\nSample Matching Keys:", list(intersection)[:5])
print("Sample MISSING Keys (In JSON but not Excel):", list(json_keys - excel_keys)[:5])

# Check specific value seen in debug
val = '8026567'
print(f"\nCheck '{val}': In JSON? {val in json_keys}. In Excel? {val in excel_keys}")
