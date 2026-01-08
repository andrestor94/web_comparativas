
import sys
import os
from pathlib import Path

# Add project root to sys.path
root = Path(r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2")
sys.path.append(str(root))

log_file = root / "debug_output.txt"

def log(msg):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(str(msg) + "\n")
    print(msg)

log("STARTING DEBUG")

# Set DB Path
db_path = root / "web_comparativas" / "app.db"
os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
log(f"DB Path Set: {os.environ['DATABASE_URL']}")

try:
    from web_comparativas.models import SessionLocal, Upload as UploadModel
    log("Models imported")
    from web_comparativas import services
    log("Services imported")

    session = SessionLocal()
    try:
        # Get latest upload
        # Order by id desc to include potentially unsaved ones? No, id is key.
        up = session.query(UploadModel).order_by(UploadModel.id.desc()).first()
        if not up:
            log("NO UPLOADS FOUND")
            sys.exit(1)
            
        log(f"DEBUG: Processing Upload ID: {up.id}, Created: {up.created_at}")
        log(f"DEBUG: Process Number: {up.proceso_nro}")
        log(f"DEBUG: Original Path: {up.original_path}")
        log(f"DEBUG: Base Dir: {up.base_dir}")
        
        # Check integrity
        if not os.path.exists(up.original_path):
            log(f"ERROR: Original file missing at {up.original_path}")
        else:
            log("DEBUG: Original file exists.")
            
        # Run processing
        log("DEBUG: Starting classify_and_process...")
        try:
            result = services.classify_and_process(up.id)
            log(f"DEBUG: Result: {result}")
        except Exception as proc_exc:
            log(f"ERROR IN PROCESS: {proc_exc}")
            import traceback
            with open(log_file, "a", encoding="utf-8") as f:
                traceback.print_exc(file=f)

        # Check output dir
        base_dir_abs = services._abs_path(up.base_dir) or (services.PROJECT_ROOT / "data" / "uploads")
        out_dir = base_dir_abs / "processed"
        log(f"DEBUG: Expected Output Dir: {out_dir}")
        
        if out_dir.exists():
            log("DEBUG: Output dir exists.")
            log(f"DEBUG: Contents: {[p.name for p in out_dir.iterdir()]}")
            
            # Check dashboard.json
            dash = out_dir / "dashboard.json"
            if dash.exists():
                txt = dash.read_text(encoding="utf-8")
                log(f"DASHBOARD JSON (First 200chars): {txt[:200]}")
            else:
                log("DASHBOARD JSON MISSING")
        else:
            log("DEBUG: Output dir DOES NOT EXIST.")

    except Exception as e:
        log(f"CRITICAL ERROR: {e}")
        import traceback
        with open(log_file, "a", encoding="utf-8") as f:
            traceback.print_exc(file=f)
    finally:
        session.close()

except Exception as import_err:
    log(f"IMPORT ERROR: {import_err}")
    import traceback
    with open(log_file, "a", encoding="utf-8") as f:
        traceback.print_exc(file=f)
