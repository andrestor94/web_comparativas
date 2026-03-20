from pathlib import Path
import os
import json, re, importlib, logging, unicodedata  # <-- NUEVO: unicodedata
from dataclasses import dataclass
import yaml
import pandas as pd

from .models import db_session, Upload as UploadModel, SavedView, User  # <-- NUEVO: User

# ------------------------------------------------------------
# Logger
# ------------------------------------------------------------
logger = logging.getLogger("web_comp.services")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[services] %(levelname)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

# ------------------------------------------------------------
# Pasos estándar del proceso (los usa el stepper en main.py)
# ------------------------------------------------------------
PROCESS_STEPS = [
    ("pending",     "Pendiente"),
    ("classifying", "Validando y clasif."),
    ("processing",  "Procesando"),
    ("reviewing",   "En revisión"),
    ("dashboard",   "Tablero"),
    ("done",        "Finalizado"),
]
STEP_KEYS = [k for k, _ in PROCESS_STEPS]

def step_index(status_key: str) -> int:
    try:
        return STEP_KEYS.index(status_key)
    except Exception:
        return -1

class PreconditionFailed(Exception):
    """Lanzada cuando se intenta avanzar sin cumplir una regla de negocio."""
    pass

# ------------------------------------------------------------
# Registry de adapters
# ------------------------------------------------------------
REG_PATH = Path(__file__).resolve().parent / "registry.yml"
if not REG_PATH.exists():
    REG_PATH = Path(__file__).resolve().parent / "web_comparativas" / "registry.yml"

@dataclass
class HandlerRef:
    module: str
    func: str

def _load_registry():
    if not REG_PATH.exists():
        return {"sources": []}
    data = yaml.safe_load(REG_PATH.read_text(encoding="utf-8")) or {}
    return data.get("sources", [])

def _str_keyed(o):
    """Convierte claves numpy/int en str para JSON."""
    if isinstance(o, dict):
        return {str(k): _str_keyed(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_str_keyed(x) for x in o]
    return o

def _pick_handler(meta: dict):
    # --- DEBUG LOGGING START ---
    try:
        debug_log = PROJECT_ROOT / "debug_services.log"
        with open(debug_log, "a", encoding="utf-8") as f:
            f.write(f"[PICK_HANDLER] Meta: {meta}\n")
    except: pass
    # --- DEBUG LOGGING END ---

    sources = _load_registry()
    fname = meta.get("filename", "") or ""
    platform = (meta.get("platform", "") or "").upper()
    for s in sources:
        w = s.get("when", {}) or {}
        ok = True
        if "platform" in w and w["platform"]:
            ok &= platform in [str(p).upper() for p in w["platform"]]
        if w.get("filename_regex"):
            if not re.match(w["filename_regex"], fname, flags=re.I):
                ok = False
        
        # --- DEBUG LOGGING LOOP ---
        try:
            with open(debug_log, "a", encoding="utf-8") as f:
                f.write(f"  Checking {s.get('id')}: OK={ok} (regex={w.get('filename_regex')} vs fname={fname}, plat={w.get('platform')} vs {platform})\n")
        except: pass
        # --- DEBUG LOGGING LOOP END ---

        if ok:
            mod, func = s["handler"].split(":")
            return HandlerRef(mod, func), s.get("id")
            
    s = [x for x in sources if x.get("id") == "DEFAULT"]
    if s:
        mod, func = s[0]["handler"].split(":")
        return HandlerRef(mod, func), "DEFAULT"
    raise RuntimeError("No hay handler en registry.yml")

# ------------------------------------------------------------
# Helpers de rutas/DB
# ------------------------------------------------------------
def _get_upload(upload_id: int) -> UploadModel:
    up = db_session.get(UploadModel, int(upload_id))
    if not up:
        raise RuntimeError(f"Upload {upload_id} no existe")
    return up

def _commit_safe():
    try:
        db_session.commit()
    except Exception as exc:
        try:
            db_session.rollback()
        except Exception as rollback_exc:
            logger.error("Fallo al hacer rollback de la sesión: %s", rollback_exc)
        raise exc

def _set_status_by_id(upload_id: int, status_key: str):
    if status_key not in STEP_KEYS:
        status_key = "processing"
    up = _get_upload(upload_id)
    up.status = status_key
    db_session.add(up)
    _commit_safe()
    logger.info(f"Upload {upload_id}: status → {status_key}")

# === Helpers de rutas absolutas y detecciones ================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # raíz del proyecto

# Directorio raíz de uploads: respeta UPLOADS_PATH si está configurado
# (apunta al Render Persistent Disk en producción, p.ej. /opt/render/project/data/uploads).
# En local, usa {PROJECT_ROOT}/data/uploads como antes.
_uploads_path_env = os.environ.get("UPLOADS_PATH", "").strip()
if _uploads_path_env:
    UPLOADS_ROOT = Path(_uploads_path_env)
else:
    UPLOADS_ROOT = PROJECT_ROOT / "data" / "uploads"

def _abs_path(p):
    if not p:
        return None
    p = Path(p)
    return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()

def _resolve_original_path(up: UploadModel, base_dir_abs: Path) -> Path | None:
    op = getattr(up, "original_path", None)
    op_abs = _abs_path(op)
    if op_abs and op_abs.exists():
        return op_abs
    for pattern in ("*.xlsx", "*.xls", "*.csv", "*.pdf"):
        candidates = list(base_dir_abs.glob(pattern))
        if candidates:
            return candidates[0]
    return None

# --- NUEVO: helpers para snapshot de uploader --------------------------------
def _ensure_uploader_snapshot(up: UploadModel) -> None:
    """
    Garantiza uploaded_by_name / uploaded_by_email.
    Si faltan y existe user_id -> busca el User y completa.
    Idempotente y seguro para ejecutar muchas veces.
    """
    need_name = not getattr(up, "uploaded_by_name", None)
    need_mail = not getattr(up, "uploaded_by_email", None)
    if not (need_name or need_mail):
        return

    user = getattr(up, "user", None)
    if not user and getattr(up, "user_id", None):
        try:
            user = db_session.get(User, int(up.user_id))
        except Exception:
            user = None

    if user:
        name_pref = (user.full_name or user.name or user.email or "").strip()
        if need_name and name_pref:
            up.uploaded_by_name = name_pref
        if need_mail and user.email:
            up.uploaded_by_email = user.email

        db_session.add(up)
        try:
            _commit_safe()
            logger.info(f"Snapshot de uploader completado para upload {up.id}")
        except Exception as e:
            logger.warning(f"No se pudo guardar snapshot para upload {up.id}: {e}")

def set_upload_uploader_snapshot(upload_id: int) -> bool:
    """
    Utilitario público: fuerza completar el snapshot de un upload puntual.
    Devuelve True si quedó con nombre o email seteado.
    """
    up = _get_upload(upload_id)
    _ensure_uploader_snapshot(up)
    return bool(up.uploaded_by_name or up.uploaded_by_email)

def backfill_uploader_snapshots(limit: int | None = None) -> int:
    """
    Rellena snapshots faltantes en uploads históricos.
    Si 'limit' se indica, procesa hasta esa cantidad.
    Devuelve la cantidad de filas actualizadas.
    """
    q = db_session.query(UploadModel).filter(
        (UploadModel.uploaded_by_name.is_(None)) | (UploadModel.uploaded_by_email.is_(None)),
        UploadModel.user_id.isnot(None),
    ).order_by(UploadModel.id.asc())

    if limit:
        q = q.limit(int(limit))

    updated = 0
    for up in q.all():
        before = (up.uploaded_by_name, up.uploaded_by_email)
        _ensure_uploader_snapshot(up)
        after = (up.uploaded_by_name, up.uploaded_by_email)
        if before != after:
            updated += 1
    return updated

# --- NUEVO: helpers para normalized.xlsx -------------------------------------
def get_normalized_path(upload: UploadModel) -> Path | None:
    p = getattr(upload, "normalized_path", None)
    if p:
        return Path(p)
    
    base_dir = getattr(upload, "base_dir", None)
    base_dir_abs = _abs_path(base_dir) or (PROJECT_ROOT / "data" / "uploads")

    # Match isolation logic from classify_and_process
    if base_dir_abs == UPLOADS_ROOT:
         return base_dir_abs / f"iso_{upload.id}" / "processed" / "normalized.xlsx"
    
    return base_dir_abs / "processed" / "normalized.xlsx"

def normalized_exists(upload: UploadModel) -> bool:
    p = get_normalized_path(upload)
    try:
        if p and p.exists():
            return True
    except Exception:
        pass
    # Fallback: verificar contenido guardado en DB (sobrevive redespliegues)
    return bool(getattr(upload, "normalized_content", None))


def get_normalized_bytes(upload: UploadModel) -> bytes | None:
    """
    Devuelve los bytes del normalized.xlsx.
    Prioridad: disco local → contenido en DB.
    Así funciona aunque el filesystem de Render se haya reiniciado.
    """
    try:
        p = get_normalized_path(upload)
        if p and p.exists():
            return p.read_bytes()
    except Exception:
        pass
    content = getattr(upload, "normalized_content", None)
    return bytes(content) if content else None


def get_dashboard_data(upload: UploadModel) -> dict:
    """
    Devuelve el dict del dashboard.json.
    Prioridad: archivo en disco → campo dashboard_json en DB.
    """
    try:
        p = get_normalized_path(upload)
        if p and p.exists():
            dash_path = p.parent / "dashboard.json"
            if dash_path.exists():
                return json.loads(dash_path.read_text(encoding="utf-8"))
    except Exception:
        pass
    stored = getattr(upload, "dashboard_json", None)
    if stored:
        try:
            return json.loads(stored)
        except Exception:
            pass
    return {}

def refresh_flags(upload: UploadModel) -> dict:
    return {
        "normalized_ready": normalized_exists(upload),
        "can_open_dashboard": normalized_exists(upload),
        "adapter": getattr(upload, "detected_source", None) or getattr(upload, "script_key", None),
    }
# -----------------------------------------------------------------------------

def _find_normalized_in(out_dir: Path) -> bool:
    try:
        return (out_dir / "normalized.xlsx").exists()
    except Exception:
        return False

# --- NUEVO: utilidades para contar renglones (ítems) únicos ------------------
def _norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", s.lower())

_RENGLON_KEYS = {
    "renglon", "nrenglon", "nrorenglon", "numerorenglon",
    "n", "nitem", "item", "itemid", "codigoitem", "codigorig",
}

def _pick_renglon_col(df: pd.DataFrame) -> str | None:
    idx = {_norm_text(c): c for c in df.columns}
    for k, original in idx.items():
        if k in _RENGLON_KEYS or ("renglon" in k):  # tolera “renglón”, “N° Renglón”, etc.
            return original
    for c in df.columns:
        if "rengl" in _norm_text(c):
            return c
    return None

def _canon_item(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    s = s.replace(",", ".")
    # 001, 1.0, 1.00 -> 1
    m = re.match(r"^\s*0*(\d+)(?:[.](?:0)+)?\s*$", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    # texto: quitar tildes/espacios múltiples; bajar a minúsculas
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s or None

def _count_renglones_unicos(df: pd.DataFrame) -> int:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return 0
    col = _pick_renglon_col(df)
    if col and col in df.columns:
        series = df[col].map(_canon_item)
        count = int(series.dropna().nunique())
        logger.info(f"Conteo de renglones únicos usando columna '{col}': {count}")
        return count
    # Fallback por descripción si no existiese la columna de renglón
    for cand in df.columns:
        if _norm_text(cand) in {"descripcion", "description", "desc"}:
            series = df[cand].map(lambda x: str(x).strip().lower())
            count = int(series.dropna().nunique())
            logger.info(f"Conteo de renglones únicos fallback por '{cand}': {count}")
            return count
    # Último recurso
    count = int(df.drop_duplicates().shape[0])
    logger.info(f"Conteo de renglones únicos por filas únicas (último recurso): {count}")
    return count
# -----------------------------------------------------------------------------

# ------------------------------------------------------------
# Estado enriquecido (para /api/cargas/{id}/status)
# ------------------------------------------------------------
def get_status(upload_id: int) -> dict:
    up = _get_upload(upload_id)
    # Asegura snapshot por si el registro es viejo y nunca se llenó
    _ensure_uploader_snapshot(up)
    flags = refresh_flags(up)
    return {
        "status": up.status,
        "adapter": flags["adapter"],
        "normalized_ready": bool(flags["normalized_ready"]),
        "can_open_dashboard": bool(flags["can_open_dashboard"]),
        "normalized_path": str(get_normalized_path(up)) if get_normalized_path(up) else None,
    }

# ------------------------------------------------------------
# Reglas de avance
# ------------------------------------------------------------
def advance_status(upload_id: int, *, force: bool = False) -> str:
    up = _get_upload(upload_id)
    idx = step_index(up.status)
    if idx < 0 or idx >= len(STEP_KEYS) - 1:
        return up.status
    target = STEP_KEYS[idx + 1]
    if target == "dashboard" and not (force or normalized_exists(up)):
        raise PreconditionFailed("No se puede avanzar a 'Tablero' porque no está listo el normalized.xlsx.")
    _set_status_by_id(upload_id, target)
    return target

# ------------------------------------------------------------
# Pipeline principal (ID-safe)
# ------------------------------------------------------------
def classify_and_process(upload_id: int, metadata: dict, *, touch_status: bool = False):
    up = _get_upload(upload_id)
    
    # --- DEBUG LOG ---
    try:
        debug_log = PROJECT_ROOT / "debug_services.log"
        with open(debug_log, "a", encoding="utf-8") as f:
            f.write(f"\n--- [START] classify_and_process id={upload_id} ---\n")
            f.write(f"Upload: {up}\n")
            f.write(f"Original: {up.original_path}\n")
            f.write(f"BaseDir (DB): {up.base_dir}\n")
    except:
        pass
    # -----------------

    # NUEVO: asegurar snapshot del cargador al entrar al pipeline
    _ensure_uploader_snapshot(up)

    base_dir_abs = _abs_path(getattr(up, "base_dir", None)) or UPLOADS_ROOT
    
    # --- FIX: enforce isolation even if base_dir is shared ---
    # If base_dir is the root uploads folder, force a subdirectory based on ID or UUID
    # to prevents data leakage between processes.
    if base_dir_abs == UPLOADS_ROOT:
        # Fallback isolation
        out_dir = base_dir_abs / f"iso_{upload_id}" / "processed"
    else:
        out_dir = base_dir_abs / "processed"
    
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        if touch_status:
            _set_status_by_id(upload_id, "classifying")

        # === Validación temprana del archivo ===
        file_path = _resolve_original_path(up, base_dir_abs)
        if not file_path or not file_path.exists():
            # Fallback: si el archivo no está en disco (p.ej. tras un redeploy en Render),
            # intentar restaurarlo desde original_content guardado en DB.
            orig_bytes = getattr(up, "original_content", None)
            if orig_bytes:
                orig_fname = getattr(up, "original_filename", None) or "restored_file"
                restored_path = base_dir_abs / orig_fname
                base_dir_abs.mkdir(parents=True, exist_ok=True)
                restored_path.write_bytes(bytes(orig_bytes))
                logger.info(
                    f"Upload {upload_id}: archivo restaurado desde DB → {restored_path}"
                )
                file_path = restored_path
            else:
                raise RuntimeError(
                    f"No se encontró archivo fuente en {base_dir_abs} "
                    "y no hay contenido guardado en DB. "
                    "El archivo se perdió posiblemente por un redespliegue."
                )

        valid_ext = {".xlsx", ".xls", ".csv", ".pdf", ".zip"}
        if file_path.suffix.lower() not in valid_ext:
            raise RuntimeError(f"Formato de archivo no admitido: {file_path.suffix}. Solo se aceptan {', '.join(valid_ext)}")

        # --- validación estructural mínima si es Excel ---
        if file_path.suffix.lower() in {".xlsx", ".xls"}:
            try:
                df_preview = pd.read_excel(file_path, nrows=5)
                if df_preview.empty or len(df_preview.columns) < 2:
                    raise ValueError("El archivo Excel parece vacío o sin columnas suficientes.")
            except Exception as e:
                raise RuntimeError(f"No se pudo leer correctamente el Excel ({e})")

        meta_eff = dict(metadata or {})
        meta_eff.setdefault("filename", file_path.name)
        plat_hint = (getattr(up, "platform_hint", None) or "").strip()
        if plat_hint and not meta_eff.get("platform"):
            meta_eff["platform"] = plat_hint

        href, script_id = _pick_handler(meta_eff)

        try:
            with open(debug_log, "a", encoding="utf-8") as f:
                f.write(f"Selected handler: {script_id} -> {href.module}:{href.func}\n")
        except: pass
        
        if touch_status:
            _set_status_by_id(upload_id, "processing")
        handler = getattr(importlib.import_module(href.module), href.func)

        result = handler(Path(file_path), meta_eff, out_dir)
        
        try:
            with open(debug_log, "a", encoding="utf-8") as f:
                if isinstance(result, dict):
                    summ = result.get("summary", {})
                elif isinstance(result, tuple) and len(result) > 1 and isinstance(result[1], dict):
                    summ = result[1]
                else:
                    summ = {}
                f.write(f"Function result summary keys: {list(summ.keys())}\n")
                f.write(f"Total Offers: {summ.get('total_offers')}\n")
        except: pass

        if isinstance(result, tuple):
            df, summary = result
        elif isinstance(result, dict):
            df = result.get("df")
            summary = result.get("summary", {})
        else:
            df, summary = result, {}

        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            raise RuntimeError("El resultado del procesamiento está vacío o no es un DataFrame válido.")

        normalized_path = out_dir / "normalized.xlsx"
        df.to_excel(normalized_path, index=False, engine="openpyxl")

        # KPI mínimos si el adapter no los genera
        if not summary or not isinstance(summary, dict):
            total = 0.0
            if "Total por renglón" in df.columns:
                total = float(pd.to_numeric(df["Total por renglón"], errors="coerce").fillna(0).sum())
            summary = {"total_offers": total, "awarded": 0.0, "pct_over_awarded": 0.0}

        # --- Renglones únicos ---
        try:
            renglones_unicos = _count_renglones_unicos(df)
        except Exception:
            renglones_unicos = 0
        summary["renglones"] = int(renglones_unicos)

        # Guardar dashboard.json en disco
        dashboard_json_str = json.dumps(_str_keyed(summary), ensure_ascii=False)
        (out_dir / "dashboard.json").write_text(dashboard_json_str, encoding="utf-8")

        # --- PERSISTENCIA EN DB: guarda contenido en PostgreSQL para sobrevivir redespliegues ---
        # Esto asegura que la comparativa siga siendo accesible aunque Render reinicie el filesystem.
        try:
            up.normalized_content = normalized_path.read_bytes()
            up.dashboard_json = dashboard_json_str
            logger.info(f"Upload {upload_id}: contenido guardado en DB ({len(up.normalized_content)} bytes).")
        except Exception as persist_err:
            logger.warning(f"Upload {upload_id}: no se pudo guardar en DB – {persist_err}")

        # --- NUEVO: Persistir metadatos ricos en la DB para el Header del Dashboard ---
        try:
            # Apertura
            ap_val = summary.get("basic_info", {}).get("opening_date", {}).get("value")
            if ap_val:
                 up.apertura_fecha = str(ap_val)

            # Comprador / Organismo (UOA)
            org_val = summary.get("summary", {}).get("uoa", {}).get("value")
            if org_val:
                 up.buyer_hint = str(org_val)
            
            # Lugar / Provincia
            place_val = summary.get("basic_info", {}).get("place", {}).get("value")
            if place_val:
                 up.province_hint = str(place_val)

            # Cuenta (si viniese en summary)
            # exp_val = summary.get("summary", {}).get("expediente", {}).get("value")
            # if exp_val: ...
                 
        except Exception as e:
            logger.warning(f"Error mapeando metadatos extraídos a DB: {e}")


        up.detected_source = script_id
        up.script_key = f"{href.module}:{href.func}"
        db_session.add(up)
        _commit_safe()

        if touch_status:
            _set_status_by_id(upload_id, "reviewing")
            # REMOVED: Auto-advance to dashboard/done
            # The Admin must manually click "Avanzar" to go to dashboard.
        
        # --- Notificación al usuario ---
        if up.user_id:
            try:
                from .notifications_service import create_notification
                create_notification(
                    db_session,
                    user_id=up.user_id,
                    title="Procesamiento completado",
                    message=f"El archivo {up.original_filename} ha sido procesado correctamente (Proceso: {up.proceso_nro}).",
                    category="processing",
                    link=f"/tablero/{up.id}"
                )
            except Exception as e:
                logger.warning(f"No se pudo crear notificación: {e}")

        return {"normalized_path": str(normalized_path), "summary": summary}

    except Exception as e:
        # --- BLOQUE DE ALERTA ---
        err_msg = f"Error al procesar upload {upload_id}: {e}"
        try:
            (out_dir / "error.log").write_text(err_msg, encoding="utf-8")
        except Exception:
            pass
        logger.error(err_msg)

        # Notificación in-app a admins + log de respaldo
        try:
            from .notifications_service import notify_admins
            _nombre_archivo = getattr(up, "original_filename", None) or f"Upload #{upload_id}"
            notify_admins(
                db_session,
                title="Error en procesamiento de archivo",
                message=f"Falló el procesamiento de «{_nombre_archivo}» (Upload #{upload_id}). Revisá el caso para reprocesar.",
                category="processing",
                link=f"/tablero/{upload_id}",
            )
        except Exception as notif_err:
            logger.warning(f"No se pudo crear notificación de error: {notif_err}")
            # Fallback: escribir en log de admin
            try:
                admin_log = PROJECT_ROOT / "data" / "alerts_admin.log"
                admin_log.parent.mkdir(parents=True, exist_ok=True)
                with open(admin_log, "a", encoding="utf-8") as f:
                    f.write(f"[ALERTA] {err_msg}\n")
            except Exception:
                pass

        return {"error": str(e)}


# ============================================================================
#                         Saved Views API (personalización)
# ============================================================================

# Claves admitidas en el payload (podemos ampliar luego)
_ALLOWED_KEYS = {
    "supplier_filter",   # "suizo" | "otros" | "todos"
    "fit_mode",          # "100" | "ajustar"
    "density",           # "normal" | "compacto"
    "column_order",      # list[str]
    "hidden_columns",    # list[str]
    "date_range",        # dict|str|null
    "search_query",      # str
}

def _clean_payload(payload: dict) -> dict:
    try:
        payload = dict(payload or {})
    except Exception:
        return {}
    cleaned = {}
    for k, v in payload.items():
        if k in _ALLOWED_KEYS:
            cleaned[k] = v
    return cleaned

def _serialize_sv(sv: SavedView) -> dict:
    return {
        "id": sv.id,
        "user_id": sv.user_id,
        "view_id": sv.view_id,
        "name": sv.name,
        "is_default": bool(sv.is_default),
        "payload": dict(sv.payload or {}),
        "created_at": sv.created_at.isoformat() if sv.created_at else None,
        "updated_at": sv.updated_at.isoformat() if sv.updated_at else None,
    }

def list_views(user_id: int, view_id: str = "dashboard") -> list[dict]:
    """Devuelve todas las vistas del usuario para una view determinada."""
    q = db_session.query(SavedView).filter(
        SavedView.user_id == int(user_id),
        SavedView.view_id == str(view_id),
    ).order_by(SavedView.is_default.desc(), SavedView.updated_at.desc())
    return [_serialize_sv(x) for x in q.all()]

def get_default_view(user_id: int, view_id: str = "dashboard") -> dict | None:
    """Obtiene la vista por defecto; si no hay, devuelve None."""
    sv = (
        db_session.query(SavedView)
        .filter(SavedView.user_id == int(user_id), SavedView.view_id == str(view_id), SavedView.is_default.is_(True))
        .order_by(SavedView.updated_at.desc())
        .first()
    )
    return _serialize_sv(sv) if sv else None

def get_view(user_id: int, *, view_id: str = "dashboard", view_pk: int | None = None, name: str | None = None) -> dict | None:
    """Obtiene una vista específica por id o nombre."""
    q = db_session.query(SavedView).filter(SavedView.user_id == int(user_id), SavedView.view_id == str(view_id))
    if view_pk is not None:
        sv = q.filter(SavedView.id == int(view_pk)).first()
    elif name:
        sv = q.filter(SavedView.name == str(name)).first()
    else:
        sv = None
    return _serialize_sv(sv) if sv else None

def _unset_others_default(user_id: int, view_id: str, keep_id: int):
    """Pone en False el is_default de todas las demás vistas del mismo usuario/view."""
    q = db_session.query(SavedView).filter(
        SavedView.user_id == int(user_id),
        SavedView.view_id == str(view_id),
        SavedView.id != int(keep_id),
        SavedView.is_default.is_(True),
    )
    count = 0
    for sv in q.all():
        sv.is_default = False
        db_session.add(sv)
        count += 1
    if count:
        _commit_safe()

def save_view(
    user_id: int,
    *,
    view_id: str = "dashboard",
    name: str,
    payload: dict,
    is_default: bool = False,
    replace_existing: bool = True,
) -> dict:
    """
    Crea o actualiza una vista:
      - Si existe (user_id, view_id, name) y replace_existing=True, actualiza.
      - Si no, crea una nueva.
    """
    name = (name or "").strip()
    if not name:
        raise ValueError("El nombre de la vista no puede estar vacío.")
    if len(name) > 120:
        raise ValueError("El nombre de la vista no debe superar 120 caracteres.")

    payload = _clean_payload(payload)

    q = db_session.query(SavedView).filter(
        SavedView.user_id == int(user_id),
        SavedView.view_id == str(view_id),
        SavedView.name == name,
    )
    sv = q.first()

    if sv and not replace_existing:
        raise ValueError("Ya existe una vista con ese nombre.")

    if not sv:
        sv = SavedView(user_id=int(user_id), view_id=str(view_id), name=name, payload=payload, is_default=bool(is_default))
        db_session.add(sv)
    else:
        sv.payload = payload
        sv.is_default = bool(is_default) or bool(sv.is_default)

    _commit_safe()
    if sv.is_default:
        _unset_others_default(user_id=int(user_id), view_id=str(view_id), keep_id=sv.id)

    logger.info(f"Vista guardada: user={user_id} view={view_id!r} name={name!r} default={sv.is_default}")
    return _serialize_sv(sv)

def set_default_view(user_id: int, *, view_id: str = "dashboard", view_pk: int | None = None, name: str | None = None) -> dict:
    """Marca una vista como default y desmarca las demás."""
    q = db_session.query(SavedView).filter(SavedView.user_id == int(user_id), SavedView.view_id == str(view_id))
    if view_pk is not None:
        sv = q.filter(SavedView.id == int(view_pk)).first()
    elif name:
        sv = q.filter(SavedView.name == str(name)).first()
    else:
        raise ValueError("Debe indicar view_pk o name para establecer por defecto.")

    if not sv:
        raise RuntimeError("La vista indicada no existe.")

    sv.is_default = True
    db_session.add(sv)
    _commit_safe()
    _unset_others_default(user_id=int(user_id), view_id=str(view_id), keep_id=sv.id)

    logger.info(f"Vista por defecto establecida: user={user_id} view={view_id!r} name={sv.name!r}")
    return _serialize_sv(sv)

def delete_view(user_id: int, *, view_id: str = "dashboard", view_pk: int | None = None, name: str | None = None) -> bool:
    """Elimina una vista. Si era default, no asigna otra automáticamente (lo decide la UI)."""
    q = db_session.query(SavedView).filter(SavedView.user_id == int(user_id), SavedView.view_id == str(view_id))
    if view_pk is not None:
        sv = q.filter(SavedView.id == int(view_pk)).first()
    elif name:
        sv = q.filter(SavedView.name == str(name)).first()
    else:
        raise ValueError("Debe indicar view_pk o name para eliminar.")

    if not sv:
        return False

    db_session.delete(sv)
    _commit_safe()

    logger.info(f"Vista eliminada: user={user_id} view={view_id!r} name={sv.name!r}")
    return True
