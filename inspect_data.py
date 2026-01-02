import pandas as pd
import json
import itertools

JSON_PATH = r"c:\Users\andre\OneDrive\Escritorio\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\data\archivos dimensionamiento\bionexo_estandarizado.json"
EXCEL_PATH = r"c:\Users\andre\OneDrive\Escritorio\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\data\archivos dimensionamiento\listas_hospitalarios.xlsx"

print("--- INSPECTING EXCEL HEADERS ---")
try:
    df_excel = pd.read_excel(EXCEL_PATH, nrows=5)
    print("Excel Columns:", df_excel.columns.tolist())
    print("Sample 'codigo' values:", df_excel['codigo'].astype(str).tolist())
except Exception as e:
    print("Error reading Excel:", e)

print("\n--- INSPECTING JSON KEYS ---")
try:
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        # Read streaming or first chunk
        data = json.load(f)
        if isinstance(data, list) and len(data) > 0:
            print("JSON Keys:", list(data[0].keys()))
            sample_ids = [str(x.get('codigo_suizo')) for x in data[:5]]
            print("Sample 'codigo_suizo' values:", sample_ids)
        elif isinstance(data, dict):
            pass # ...
except Exception as e:
    print("Error reading JSON:", e)
