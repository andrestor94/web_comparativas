
import sys
from pathlib import Path
import pandas as pd

# Path setup
root = Path(r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2")
sys.path.append(str(root))

from web_comparativas.adapters.portales import normalize_bac

# File path
file_path = root / "data/uploads/f7814f49-f6e8-4267-8aab-459cc9717009/Cuadro_Comparativo_23-0015-LPU24.xlsx"
out_dir = root / "scripts/tmp_out"
out_dir.mkdir(exist_ok=True)

print(f"Testing normalize_bac on {file_path}")

try:
    result = normalize_bac(file_path, metadata={}, out_dir=out_dir)
    df = result["df"]
    summary = result["summary"]
    
    print("\nSUMMARY:")
    print(summary)
    
    print("\nDATAFRAME COLUMNS:", list(df.columns))
    
    if not df.empty:
        print("\nFIRST 5 ROWS (Selected Columns):")
        cols = ["Descripción", "Precio unitario", "Cantidad ofertada", "Total por renglón", "Proveedor"]
        present = [c for c in cols if c in df.columns]
        print(df[present].head(5).to_string())
        
        # Check totals
        print("\nChecking Totals:")
        print("Sum of Total por renglón:", df["Total por renglón"].sum())
        
    else:
        print("DATAFRAME IS EMPTY")

except Exception as e:
    print("ERROR:", e)
    import traceback
    traceback.print_exc()
