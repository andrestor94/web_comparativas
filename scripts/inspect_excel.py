
import pandas as pd
from pathlib import Path

# Path to the specific upload
base_path = Path(r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2\data\uploads\f7814f49-f6e8-4267-8aab-459cc9717009")
norm_path = base_path / "processed" / "normalized.xlsx"
dash_path = base_path / "processed" / "dashboard.json"

print(f"Checking {norm_path}...")
if norm_path.exists():
    try:
        df = pd.read_excel(norm_path)
        print("COLUMNS FOUND:", list(df.columns))
        print("-" * 40)
        # Check key columns
        cols_to_check = ["Precio unitario", "Cantidad ofertada", "Total por renglón", "Proveedor"]
        present_cols = [c for c in cols_to_check if c in df.columns]
        
        if present_cols:
            print(df[present_cols].head(5).to_string())
        else:
            print("KEY COLUMNS MISSING!", cols_to_check)
            
        print("-" * 40)
        # Check dtypes
        print("DTYPES:")
        print(df.dtypes)
    except Exception as e:
        print("ERROR READING EXCEL:", e)
else:
    print("NORMALIZED.XLSX NOT FOUND")

print("-" * 40)
if dash_path.exists():
    try:
        print("DASHBOARD.JSON content:")
        # Read a bit to print safely
        with open(dash_path, "r", encoding="utf-8") as f:
            print(f.read()[:500]) # First 500 chars
    except Exception as e:
        print("ERROR READING JSON:", e)
else:
    print("DASHBOARD.JSON NOT FOUND")
