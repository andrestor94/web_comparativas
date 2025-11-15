# === main.py ==================================================
from __future__ import annotations

from pathlib import Path
import os
import shutil
import uuid
import json
import datetime as dt
import unicodedata
import re
import logging
from types import SimpleNamespace
from typing import Any, Optional, List, Dict
from dotenv import load_dotenv
load_dotenv()

# === LIBRERÍAS DE TERCEROS / FRAMEWORKS ===
import numpy as np
import pandas as pd

# === [PDF reportes] ===
import io

try:
    # si está instalado pypdf (nombre nuevo)
    from pypdf import PdfReader, PdfWriter
except ImportError:
    # fallback si tu entorno tiene PyPDF2
    from PyPDF2 import PdfReader, PdfWriter

from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import Color

from fastapi import (
    FastAPI,
    Request,
    UploadFile,
    Form,
    Depends,
    HTTPException,
    Query,
    File,
    BackgroundTasks,
)
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,  # 👈 agregado para devolver el PDF en memoria
    Response,          # 👈 NECESARIO para devolver bytes del PDF
)
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from sqlalchemy import func, or_, select
from starlette.middleware.sessions import SessionMiddleware
from passlib.context import CryptContext
from sqlalchemy.orm import Session

# Modelos
from .models import (
    init_db,
    Upload as UploadModel,
    db_session,
    User,
    BUSINESS_UNITS,
    Group,
    GroupMember,
    normalize_proceso_nro,
)

# Visibilidad por grupos
from .visibility_service import (
    uploads_visible_query,
    visible_user_ids,
    kpis_for_home,
    recent_done as vis_recent_done,
)

# === Visibilidad extendida para AUDITOR ================================
def _is_auditor(user: User) -> bool:
    return (user.role or "").strip().lower() == "auditor"


def uploads_visible_ext(session, user: User):
    """
    Si es auditor → ve TODAS las cargas.
    Si no → aplica la visibilidad por grupos.
    """
    if _is_auditor(user):
        return session.query(UploadModel)
    return uploads_visible_query(session, user)


def visible_user_ids_ext(session, user: User):
    """
    Si es auditor → ve TODOS los usuarios.
    Caso contrario → usa la lógica de grupos PERO SIEMPRE incluye al propio usuario.
    Esto garantiza que el Analista vea sus propios procesos aunque no esté
    todavía asignado a un grupo o falte configurar membresías.
    """
    if _is_auditor(user):
        ids = session.query(User.id).all()  # [(1,), (2,), ...]
        return {int(row[0]) for row in ids}

    base = set(visible_user_ids(session, user))
    base.add(int(user.id))  # <- cinturón y tirantes: el propio usuario siempre
    return base


# Servicios de procesamiento
from .services import (
    classify_and_process,
    PROCESS_STEPS,
    get_status as svc_get_status,
)
from . import services

# 👉 NUEVO: servicios de “vistas guardadas”
from .services import (
    list_views as sv_list_views,
    get_default_view as sv_get_default_view,
    get_view as sv_get_view,
    save_view as sv_save_view,
    set_default_view as sv_set_default_view,
    delete_view as sv_delete_view,
)

# Transformación de ranking para el Tablero (podemos usarla o no según DF)
from .rankings import build_ranked_positions  # (se deja import para compatibilidad)

# Comentarios / Feedback (API)
from .api_comments import router as comments_router

# 👉 NUEVO: capa de correo (opcional, no rompe si no existe)
try:
    from . import email_service as _email_svc
except Exception:
    # si no existe el archivo, no rompemos la app
    _email_svc = None


# ======================================================================
# DEPENDENCIAS / SESIÓN
# ======================================================================
def get_db():
    """
    Dependencia de SQLAlchemy. Usa el db_session global del proyecto y lo cierra al final.
    """
    db = db_session
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception:
            pass


# ======================================================================
# LOGGING
# ======================================================================
logger = logging.getLogger("wc.auth")
logger.setLevel(logging.INFO)


# ======================================================================
# CONFIGURACIÓN BASE DE LA APP
# ======================================================================
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app = FastAPI()

# === [Oportunidades - Buscador] ===
OPP_DIR  = BASE_DIR / "data" / "oportunidades"
OPP_DIR.mkdir(parents=True, exist_ok=True)
OPP_FILE = OPP_DIR / "reporte_oportunidades.xlsx"

def _save_oportunidades_excel(file: UploadFile) -> int:
    name = (file.filename or "").lower()
    if not name.endswith(".xlsx"):
        raise HTTPException(
            status_code=400,
            detail="Formato no permitido. Subí un Excel .xlsx (no .xls)"
        )
    tmp_path = OPP_DIR / f"tmp_{uuid.uuid4().hex}.xlsx"
    with tmp_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    tmp_path.replace(OPP_FILE)
    try:
        df = pd.read_excel(OPP_FILE, dtype=str, engine="openpyxl")
        return int(len(df))
    except Exception:
        return -1


def _oportunidades_status() -> dict:
    """
    Devuelve metadata para la vista (si hay archivo, filas, última actualización).
    """
    info = {"has_file": False, "rows": None, "last_updated_str": None}
    if OPP_FILE.exists():
        info["has_file"] = True
        try:
            df = pd.read_excel(OPP_FILE, dtype=str)
            info["rows"] = int(len(df))
        except Exception:
            info["rows"] = None

        mtime = dt.datetime.fromtimestamp(OPP_FILE.stat().st_mtime)
        info["last_updated_str"] = mtime.strftime("%d/%m/%Y %H:%M")
    return info

# Estados que consideramos “finalizados” para habilitar tablero/descarga a no-admin
FINAL_STATES = {"done", "finalizado", "dashboard", "tablero"}

# === [Oportunidades - Helpers de dashboard] ==============================
def _opp_norm(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s or ""))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return re.sub(r"[\s._/\-]+", "", s.strip().lower())

def _opp_pick(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {_opp_norm(c): c for c in df.columns}
    for cand in candidates:
        c = cols.get(_opp_norm(cand))
        if c:
            return c
    return None

def _opp_parse_number(x) -> float:
    if x is None:
        return 0.0
    s = str(x).strip()
    if not s:
        return 0.0
    # quita miles con punto, acepta coma como decimal
    s = re.sub(r"\.(?=\d{3}(\D|$))", "", s)
    s = s.replace(",", ".")
    try:
        v = float(s)
        return v if np.isfinite(v) else 0.0
    except Exception:
        return 0.0

def _opp_parse_date(s) -> dt.date | None:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(t, fmt).date()
        except Exception:
            pass
    try:
        return dt.datetime.fromisoformat(t).date()
    except Exception:
        return None

def _opp_load_df() -> pd.DataFrame | None:
    if not OPP_FILE.exists():
        return None
    try:
        df = pd.read_excel(OPP_FILE, dtype=str, engine="openpyxl").fillna("")
        # normaliza encabezados visuales (conservamos los originales)
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        print("[_opp_load_df] Error:", e)
        return None

def _opp_apply_filters(df: pd.DataFrame, q: str, buyer: str, platform: str,
                       province: str, date_from: str, date_to: str) -> pd.DataFrame:
    out = df.copy()

    # columnas candidatas
    buyer_col = _opp_pick(out, ["Comprador", "Repartición", "Entidad", "Organismo", "Unidad Compradora", "Buyer"])
    platf_col = _opp_pick(out, ["Plataforma", "Portal", "Origen", "Sistema", "Platform"])
    prov_col  = _opp_pick(out, ["Provincia", "Provincia/Municipio", "Municipio", "Jurisdicción", "Localidad", "Departamento"])
    fecha_col = _opp_pick(out, ["Fecha Apertura", "Apertura", "Fecha", "Fecha de Publicación", "Publicación"])
    desc_col  = _opp_pick(out, ["Descripción", "Descripcion", "Objeto", "Detalle", "Renglón", "Renglon"])
    proc_col  = _opp_pick(out, ["N° Proceso", "Nro Proceso", "Proceso", "Expediente"])
    cuenta_col = _opp_pick(out, ["Cuenta", "N° Cuenta", "Nro Cuenta", "Cuenta Nro"])

    # búsqueda libre
    if q.strip():
        like = q.strip().lower()
        cols_buscar = [c for c in [desc_col, buyer_col, platf_col, prov_col, proc_col, cuenta_col] if c]
        if cols_buscar:
            m = False
            for c in cols_buscar:
                m = m | out[c].astype(str).str.lower().str.contains(like, na=False)
            out = out[m]

    # filtros por campo
    if buyer.strip() and buyer_col:
        out = out[out[buyer_col].astype(str).str.contains(buyer.strip(), case=False, na=False)]
    if platform.strip() and platf_col:
        out = out[out[platf_col].astype(str).str.contains(platform.strip(), case=False, na=False)]
    if province.strip() and prov_col:
        out = out[out[prov_col].astype(str).str.contains(province.strip(), case=False, na=False)]

    # rango de fechas
    if fecha_col and (date_from.strip() or date_to.strip()):
        dates = out[fecha_col].apply(_opp_parse_date)
        if date_from.strip():
            dfm = _opp_parse_date(date_from.strip())
            if dfm:
                out = out[dates >= dfm]
        if date_to.strip():
            dtm = _opp_parse_date(date_to.strip())
            if dtm:
                out = out[dates <= dtm]

    return out

def _opp_compute_kpis(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "total_rows": 0,
            "buyers": 0,
            "platforms": 0,
            "provinces": 0,
            "budget_total": 0.0,
            "date_min": "",
            "date_max": "",
        }

    buyer_col = _opp_pick(df, ["Comprador", "Repartición", "Entidad", "Organismo", "Unidad Compradora", "Buyer"])
    platf_col = _opp_pick(df, ["Plataforma", "Portal", "Origen", "Sistema", "Platform"])
    prov_col  = _opp_pick(df, ["Provincia", "Provincia/Municipio", "Municipio", "Jurisdicción", "Localidad", "Departamento"])
    fecha_col = _opp_pick(df, ["Fecha Apertura", "Apertura", "Fecha", "Fecha de Publicación", "Publicación"])
    presu_col = _opp_pick(df, ["Presupuesto oficial", "Presupuesto", "Monto", "Importe Total", "Total Presupuesto", "Monto Total", "Importe"])

    k = {
        "total_rows": int(len(df)),
        "buyers": int(df[buyer_col].astype(str).str.strip().str.lower().nunique()) if buyer_col else 0,
        "platforms": int(df[platf_col].astype(str).str.strip().str.lower().nunique()) if platf_col else 0,
        "provinces": int(df[prov_col].astype(str).str.strip().str.lower().nunique()) if prov_col else 0,
        "budget_total": 0.0,
        "date_min": "",
        "date_max": "",
    }

    if presu_col:
        k["budget_total"] = float(pd.Series(df[presu_col]).map(_opp_parse_number).sum())

    if fecha_col:
        fechas = pd.Series(df[fecha_col]).map(_opp_parse_date)
        try:
            fmin = fechas.dropna().min()
            fmax = fechas.dropna().max()
            k["date_min"] = fmin.strftime("%d/%m/%Y") if isinstance(fmin, dt.date) else ""
            k["date_max"] = fmax.strftime("%d/%m/%Y") if isinstance(fmax, dt.date) else ""
        except Exception:
            pass

    return k


# === Helper de render: usa plantilla si existe, sino cae a HTML simple ===
def _render_or_fallback(template_name: str, ctx: dict, fallback_html: str):
    tpath = BASE_DIR / "templates" / template_name
    if tpath.exists():
        return templates.TemplateResponse(template_name, ctx)
    return HTMLResponse(fallback_html)

# ======================================================================
# PDF / REPORTES – CONFIG
# ======================================================================
# carpeta donde vamos a guardar los PDF generados
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# tu plantilla real (la que está en static/reports)
PDF_TEMPLATE_PATH = BASE_DIR / "static" / "reports" / "Informe Comparativas.pdf"

# tu PDF de diseño es 960 x 540 (landscape)
PDF_PAGE_WIDTH = 960
PDF_PAGE_HEIGHT = 540
PDF_PAGE_SIZE = (PDF_PAGE_WIDTH, PDF_PAGE_HEIGHT)


def _make_overlay(draw_fn):
    """
    Crea un PDF en memoria del mismo tamaño que la plantilla y deja que draw_fn(canvas) dibuje lo que corresponda.
    """
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=PDF_PAGE_SIZE)
    draw_fn(c)
    c.save()
    buf.seek(0)

    try:
        overlay_reader = PdfReader(buf)
    except Exception:
        from PyPDF2 import PdfReader as _PdfReader

        overlay_reader = _PdfReader(buf)

    return overlay_reader


def render_informe_comparativas(data_pdf: dict) -> bytes:
    """
    Genera el PDF final tomando como base la plantilla estática
    'static/reports/Informe Comparativas.pdf' y escribiendo encima los datos del proceso
    en 5 páginas.
    """
    if not PDF_TEMPLATE_PATH.exists():
        # fallback A4 si no está la plantilla
        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=(595, 842))
        c.setFont("Helvetica-Bold", 14)
        c.drawString(40, 800, "Informe de licitación")
        c.setFont("Helvetica", 11)
        c.drawString(40, 780, f"Proceso: {data_pdf.get('proceso_nro','')}")
        c.save()
        buf.seek(0)
        return buf.read()

    # abrir plantilla base
    try:
        base_reader = PdfReader(str(PDF_TEMPLATE_PATH))
    except Exception:
        from PyPDF2 import PdfReader as _PdfReader

        base_reader = _PdfReader(str(PDF_TEMPLATE_PATH))

    # writer final
    try:
        writer = PdfWriter()
    except Exception:
        from PyPDF2 import PdfWriter as _PdfWriter

        writer = _PdfWriter()

    # ---------------- datos ----------------
    nro_proceso = (
        data_pdf.get("proceso_nro")
        or data_pdf.get("proceso")
        or data_pdf.get("nro")
        or ""
    )
    comprador = (
        data_pdf.get("comprador")
        or data_pdf.get("reparticion")
        or data_pdf.get("buyer")
        or data_pdf.get("origen")
        or ""
    )
    apertura = data_pdf.get("apertura") or ""
    cuenta = data_pdf.get("cuenta") or ""
    plataforma = data_pdf.get("plataforma") or data_pdf.get("platform") or ""
    provincia = data_pdf.get("provincia") or data_pdf.get("municipio") or ""
    kpis = data_pdf.get("kpis") or {}
    presu_adj = kpis.get("presupuesto_adjudicado", "")
    renglones = kpis.get("renglones", "")
    competidores = kpis.get("competidores", "")

    suizo = data_pdf.get("suizo") or {}
    suiz_monto_ofert = suizo.get("monto_ofertado", "")
    suiz_monto_adj = suizo.get("monto_adjudicado", "")
    suiz_eff_monto = suizo.get("efectividad_por_monto", "")
    suiz_reng_ofert = suizo.get("renglones_ofertados", "")
    suiz_reng_adj = suizo.get("renglones_adjudicados", "")
    suiz_eff_reng = suizo.get("efectividad_por_renglones", "")

    posiciones = data_pdf.get("posiciones") or []

    hoy = dt.datetime.now().strftime("%d/%m/%Y %H:%M")

    # ---------------- PÁGINAS ----------------
    def _draw_p1(c):
        c.setFont("Helvetica-Bold", 20)
        c.drawString(60, 500, "Informe preliminar – Comparativa de licitación")

        c.setFont("Helvetica", 12)
        c.drawString(60, 470, f"N° de proceso: {nro_proceso}")
        c.drawString(60, 450, f"Repartición / Comprador: {comprador}")
        if apertura:
            c.drawString(60, 430, f"Fecha de apertura: {apertura}")
        c.drawString(60, 410, f"Generado: {hoy}")
        if plataforma:
            c.drawString(60, 390, f"Plataforma: {plataforma}")
        if provincia:
            c.drawString(60, 370, f"Provincia / Municipio: {provincia}")

    def _draw_p2(c):
        c.setFont("Helvetica-Bold", 14)
        c.drawString(40, 500, "Datos del proceso")

        c.setFont("Helvetica", 11)
        c.drawString(40, 470, f"Proceso: {nro_proceso}")
        c.drawString(260, 470, f"Apertura: {apertura}")
        c.drawString(420, 470, f"Cuenta: {cuenta}")
        c.drawString(580, 470, f"Comprador: {comprador}")

        c.setFont("Helvetica-Bold", 12)
        c.drawString(40, 440, "Datos generales de licitación")
        c.setFont("Helvetica", 11)
        c.drawString(40, 420, f"Presupuesto adjudicado: {presu_adj}")
        c.drawString(320, 420, f"Renglones: {renglones}")
        c.drawString(540, 420, f"Competidores: {competidores}")

        c.setFont("Helvetica-Bold", 12)
        c.drawString(40, 390, "Efectividad Suizo")

        c.setFont("Helvetica", 10)
        c.drawString(40, 370, f"Monto ofertado: {suiz_monto_ofert}")
        c.drawString(40, 355, f"Monto adjudicado: {suiz_monto_adj}")
        c.drawString(40, 340, f"Efectividad por monto: {suiz_eff_monto}")

        c.drawString(300, 370, f"Renglones ofertados: {suiz_reng_ofert}")
        c.drawString(300, 355, f"Renglones adjudicados: {suiz_reng_adj}")
        c.drawString(300, 340, f"Efectividad por renglones: {suiz_eff_reng}")

    def _draw_pos_table(c, title, rows, start_y=500):
        c.setFont("Helvetica-Bold", 12)
        c.drawString(40, start_y, title)
        y = start_y - 20

        c.setFont("Helvetica-Bold", 9)
        c.drawString(40, y, "Pos.")
        c.drawString(70, y, "Descripción / Renglón")
        c.drawString(350, y, "Proveedor")
        c.drawString(540, y, "Monto")
        c.drawString(630, y, "Tipo")

        y -= 15
        c.setFont("Helvetica", 8)

        for row in rows:
            if y < 50:
                break

            pos = row.get("posicion") or row.get("pos") or ""
            desc = row.get("descripcion") or row.get("detalle") or ""
            prov = row.get("proveedor") or row.get("oferente") or ""
            monto = row.get("monto") or row.get("importe") or ""
            tipo = row.get("tipo") or row.get("clase") or ""

            c.drawString(40, y, str(pos))
            c.drawString(
                70,
                y,
                (desc[:55] + "...") if len(desc) > 55 else desc,
            )
            c.drawString(
                350,
                y,
                (prov[:25] + "...") if len(prov) > 25 else prov,
            )
            c.drawString(540, y, str(monto))
            c.drawString(630, y, str(tipo))

            y -= 14

    def _draw_p3(c):
        ganadores = [
            p
            for p in posiciones
            if str(p.get("posicion") or p.get("pos") or "1") == "1"
        ]
        if not ganadores:
            ganadores = posiciones[:25]

        _draw_pos_table(c, "Adjudicaciones – 1° lugar", ganadores, start_y=500)

    def _draw_p4(c):
        segundos = [p for p in posiciones if str(p.get("posicion") or "") == "2"]
        if not segundos:
            segundos = posiciones[25:50]

        _draw_pos_table(c, "Posiciones alternativas – 2° lugar", segundos, start_y=500)

    def _draw_p5(c):
        c.setFont("Helvetica-Bold", 18)
        c.drawString(60, 500, "Suizo Argentina")

        c.setFont("Helvetica", 11)
        c.drawString(
            60,
            470,
            "Informe generado automáticamente desde Web Comparativas.",
        )

        if nro_proceso:
            c.drawString(60, 450, f"Proceso: {nro_proceso}")

        c.drawString(60, 430, f"Fecha: {hoy}")

    drawers = [_draw_p1, _draw_p2, _draw_p3, _draw_p4, _draw_p5]

    for i, page in enumerate(base_reader.pages):
        if i < len(drawers):
            overlay_reader = _make_overlay(drawers[i])
            page.merge_page(overlay_reader.pages[0])
        writer.add_page(page)

    out_buf = io.BytesIO()
    writer.write(out_buf)
    out_buf.seek(0)
    return out_buf.getvalue()


# ================== CLIENTES (autocompletar por N° de cuenta) ==================
CLIENTES_PATH = Path(__file__).with_name("data") / "BASE_CLIENTES_SUIZO.xlsx"

# Cargamos el Excel una sola vez al iniciar la app
_clientes_index: dict[str, dict] = {}

if CLIENTES_PATH.exists():
    df_clientes = pd.read_excel(CLIENTES_PATH).fillna("")
    # Ojo: en el Excel la columna viene como "Nombre Fantasia " (con espacio)
    for _, row in df_clientes.iterrows():
        nro = str(row.get("N° Cuenta", "")).strip()
        if not nro:
            continue

        _clientes_index[nro] = {
            # Esto lo vamos a usar para el campo "Comprador"
            "comprador": str(row.get("Nombre Fantasia ", "")).strip().strip('"'),
            # Esto lo vamos a usar para el campo "Provincia/Municipio"
            "provincia": str(row.get("Provincia", "")).strip(),
            # Lo dejo preparado por si después agregamos "plataforma" en el Excel
            "plataforma": "",
        }
else:
    print(f"[WARN] No se encontró el archivo de clientes en {CLIENTES_PATH}")


@app.get("/api/clientes/{n_cuenta}")
def api_get_cliente_por_cuenta(n_cuenta: str):
    """Devuelve los datos del cliente para autocompletar el formulario."""
    key = n_cuenta.strip()
    data = _clientes_index.get(key)
    if not data:
        return {"ok": False, "msg": "Cliente no encontrado"}
    return {"ok": True, "data": data}


# ======================================================================
# MIDDLEWARE: inyectar usuario en request.state
# ======================================================================
@app.middleware("http")
async def attach_user_to_state(request: Request, call_next):
    """
    El backend de comentarios lee request.state.user_display si existe.
    Por eso dejamos el usuario "a mano" en el state.
    """
    u = get_current_user(request)
    request.state.user = u
    request.state.user_display = user_display(u) if u else ""

    response = await call_next(request)
    return response


# ======================================================================
# SESIONES (cookies)
# ======================================================================
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("APP_SECRET", "dev-secret-123"),
    session_cookie="wc_session",
    same_site="lax",
    https_only=False,
    max_age=60 * 60 * 24 * 7,  # 7 días
)

# Archivos estáticos
app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "static")),
    name="static",
)

# Rutas de Comentarios (REST + SSE)
app.include_router(comments_router)


# ======================================================================
# CONTROL DE CICLO DE VIDA DE LA SESIÓN SQLALCHEMY
# ======================================================================
def _reset_session():
    """
    Hay varios puntos donde SQLAlchemy puede quedar "sucio".
    Esta función intenta devolverlo a un estado limpio.
    """
    try:
        if hasattr(db_session, "remove"):
            db_session.remove()
        else:
            db_session.close()
    except Exception:
        pass


@app.middleware("http")
async def db_session_lifecycle(request: Request, call_next):
    """
    Antes de cada request forzamos un reset.
    Si hay una excepción, hacemos rollback.
    """
    _reset_session()
    try:
        response = await call_next(request)
        return response
    except Exception:
        try:
            db_session.rollback()
        except Exception:
            pass
        raise


# ======================================================================
# AUTENTICACIÓN Y ROLES
# ======================================================================
pwd_context = CryptContext(
    schemes=["bcrypt", "pbkdf2_sha256"],
    deprecated="auto",
)


def hash_password(p: str) -> str:
    return pwd_context.hash(p)


def verify_password(p: str, h) -> bool:
    """
    Verifica una contraseña. Soporta hashes viejos (texto plano) por compatibilidad.
    """
    if not h:
        return False

    hs = h.decode() if isinstance(h, (bytes, bytearray)) else str(h)

    try:
        if hs.startswith("$"):
            return pwd_context.verify(p, hs)
        # fallback: contraseña en texto plano guardada en DB
        return hs == p
    except Exception:
        return hs == p

def verify_reset_password(user: User, password: str) -> bool:
    """
    Verificación extra para acciones DESTRUCTIVAS (reset de procesos).
    - Si existe la variable de entorno ADMIN_RESET_SECRET o RESET_UPLOADS_SECRET,
      se usa como clave maestra.
    - Si no existe, se usa la contraseña del propio usuario admin.
    """
    pwd = (password or "").strip()
    if not pwd:
        return False

    # 1) clave maestra opcional por env (más segura para entornos compartidos)
    master = os.getenv("ADMIN_RESET_SECRET") or os.getenv("RESET_UPLOADS_SECRET")
    if master:
        return pwd == master

    # 2) fallback: validamos contra la contraseña del usuario actual
    return verify_password(pwd, getattr(user, "password_hash", ""))

def get_current_user(request: Request) -> Optional[User]:
    """
    Obtiene el usuario logueado a partir de la sesión.
    Intenta recuperar la sesión si falló la primera vez.
    """
    uid = request.session.get("uid")
    if not uid:
        return None

    try:
        return db_session.get(User, uid)
    except Exception:
        _reset_session()
        try:
            return db_session.get(User, uid)
        except Exception:
            return None


def require_roles(*roles: str):
    """
    Dependencia que exige uno de los roles indicados.
    Ejemplo:

    @app.get("/ruta")
    def vista(user: User = Depends(require_roles("admin", "analista"))):
        ...
    """
    roles_norm = {r.lower() for r in roles} if roles else set()

    def _dep(request: Request) -> User:
        user = get_current_user(request)
        if user is None:
            raise HTTPException(status_code=401, detail="Inicie sesión")

        if roles_norm and (user.role or "").lower() not in roles_norm:
            raise HTTPException(status_code=403, detail="No autorizado")

        return user

    return _dep


# ======================================================================
# MANEJO GLOBAL DE HTTPException
# ======================================================================
@app.exception_handler(HTTPException)
async def _http_exc_redirect_login(request: Request, exc: HTTPException):
    """
    Si es 401, vamos al login.
    Para el resto, devolvemos el texto plano.
    """
    if exc.status_code == 401:
        return RedirectResponse("/login", status_code=303)
    return PlainTextResponse(str(exc.detail), status_code=exc.status_code)


# ======================================================================
# FILTROS / HELPERS PARA JINJA
# ======================================================================
def peso(n: Any):
    try:
        return (
            "$ {:,.2f}".format(float(n))
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
    except Exception:
        return n


def pct(n: Any):
    try:
        return (
            "{:,.2f}%".format(float(n))
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
    except Exception:
        return n


templates.env.filters["peso"] = peso
templates.env.filters["pct"] = pct


# ======================================================================
# NOMBRE A MOSTRAR DEL USUARIO
# ======================================================================
def user_display(u: Optional[User]) -> str:
    """
    Intenta construir un nombre legible a partir de los campos disponibles.
    """
    if not u:
        return ""

    for attr in ("name", "full_name", "nombre"):
        v = getattr(u, attr, None)
        if v and str(v).strip():
            return str(v).strip()

    email = getattr(u, "email", "") or ""
    alias = email.split("@")[0] if "@" in email else email
    alias = re.sub(r"[._-]+", " ", alias).strip().title()
    return alias or ""


# que Jinja pueda llamarlo
templates.env.globals["user_display"] = user_display


# ======================================================================
# ALIAS LEGIBLE PARA LA BANDEJA DE COMENTARIOS
# ======================================================================
@app.get("/comentarios", response_class=HTMLResponse)
def comentarios_alias(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    return RedirectResponse("/api/comments/ui", status_code=307)


# ======================================================================
# INICIALIZACIÓN / STARTUP
# ======================================================================
def _ensure_seed_user():
    """
    Crea/actualiza un usuario admin de seed si está configurado por env.
    """
    email = (os.getenv("ADMIN_SEED_EMAIL") or "").strip().lower()
    pwd = os.getenv("ADMIN_SEED_PASSWORD")
    if not email or not pwd:
        return

    name = os.getenv("ADMIN_SEED_NAME", "Admin")
    role = os.getenv("ADMIN_SEED_ROLE", "admin")

    user = (
        db_session.query(User)
        .filter(func.lower(func.trim(User.email)) == email)
        .first()
    )

    if user is None:
        user = User(
            email=email,
            name=name,
            role=role,
            password_hash=hash_password(pwd),
        )
        db_session.add(user)
        db_session.commit()
        return

    # actualizar si cambió el password o el nombre
    stored_hash = getattr(user, "password_hash", None)
    if not stored_hash or pwd_context.needs_update(stored_hash):
        user.password_hash = hash_password(pwd)

    if user.name != name:
        user.name = name

    if (user.role or "").lower() != role.lower():
        user.role = role

    db_session.commit()
    logger.info("Seed actualizado: %s", email)


def _backfill_names():
    """
    Rellena name / full_name / nombre para usuarios viejos.
    """
    try:
        users = db_session.query(User).all()

        for u in users:
            n = getattr(u, "name", None)
            nf = getattr(u, "full_name", None)
            nn = getattr(u, "nombre", None)

            ok = lambda x: bool(str(x or "").strip())

            if hasattr(User, "name") and not ok(n) and (ok(nf) or ok(nn)):
                u.name = nf or nn

            if hasattr(User, "full_name") and not ok(nf) and (ok(n) or ok(nn)):
                u.full_name = n or nn

        db_session.commit()
    except Exception:
        db_session.rollback()


@app.on_event("startup")
def _boot():
    # 1) inicializar DB
    init_db()

    # 2) asegurar carpeta data/
    (BASE_DIR / "data").mkdir(exist_ok=True)

    # 3) seed de admin
    _ensure_seed_user()

    # 4) backfill de nombres
    _backfill_names()

    # 5) inicializar archivo de notificaciones si no existe
    notif_file = BASE_DIR / "data" / "email_sent.json"
    if not notif_file.exists():
        try:
            notif_file.write_text("{}", encoding="utf-8")
        except Exception:
            pass

    # 6) inicializar archivo de reseteo de contraseña si no existe
    pass_reset_file = BASE_DIR / "data" / "password_resets.json"
    if not pass_reset_file.exists():
        try:
            pass_reset_file.write_text("{}", encoding="utf-8")
        except Exception:
            pass


# ======================================================================
# 👉 UTILIDADES DE NOTIFICACIONES POR MAIL
# ======================================================================
_EMAIL_LOG_PATH = BASE_DIR / "data" / "email_sent.json"


def _mail_is_ready() -> bool:
    """
    Devuelve True si hay un módulo email_service con algún método de envío.
    """
    if _email_svc is None:
        return False

    return any(
        hasattr(_email_svc, attr)
        for attr in (
            "send_email",
            "send_mail",
            "send_html_email",
            "send_upload_mail",
        )
    )


def _load_email_log() -> Dict[str, List[int]]:
    """
    Estructura en disco:
    {
      "123": [1, 5, 9],  # upload_id=123 => notificado a users 1,5,9
      "124": [1]
    }
    """
    try:
        if _EMAIL_LOG_PATH.exists():
            data = json.loads(_EMAIL_LOG_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_email_log(data: Dict[str, List[int]]):
    try:
        _EMAIL_LOG_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning("No se pudo guardar email_sent.json: %s", e)


def _was_email_sent(upload_id: int, user_id: int) -> bool:
    data = _load_email_log()
    arr = data.get(str(upload_id)) or []
    return int(user_id) in arr


def _mark_email_sent(upload_id: int, user_id: int):
    data = _load_email_log()
    arr = data.get(str(upload_id)) or []
    if int(user_id) not in arr:
        arr.append(int(user_id))
    data[str(upload_id)] = arr
    _save_email_log(data)


def _collect_followers(upload: UploadModel) -> List[User]:
    """
    Por ahora:
    - siempre el que cargó el proceso (upload.user_id)
    - en el futuro: acá se agregan "seguidores" sin tocar el resto
    """
    res: List[User] = []
    if upload.user_id:
        u = db_session.get(User, upload.user_id)
        if u:
            res.append(u)
    return res


def _render_finalized_mail(upload: UploadModel, user: User):
    """
    Construye asunto, html y texto plano.
    """
    num = upload.proceso_nro or f"ID {upload.id}"

    subject = f"[Web Comparativas] Proceso {num} finalizado"
    nombre = user_display(user) or "Usuario"
    link = f"/tablero/{upload.id}" if upload.id else "/"

    html = f"""
        <p>Hola {nombre},</p>
        <p>El proceso <b>{num}</b> que cargaste ya se encuentra <b>FINALIZADO</b> y el tablero está disponible.</p>
        <p>Puedes verlo aquí: <a href="{link}">{link}</a></p>
        <p>— Sistema Web Comparativas</p>
    """
    text = (
        f"Hola {nombre},\n\n"
        f"El proceso {num} que cargaste ya se encuentra FINALIZADO y el tablero está disponible.\n"
        f"Ver: {link}\n\n"
        f"— Sistema Web Comparativas"
    )
    return subject, html, text


def _send_finalized_mail(upload: UploadModel, user: User):
    """
    Envía el mail de finalización al usuario.
    Si falla el envío, igual lo marca como enviado para no saturarlo.
    """
    if _was_email_sent(upload.id, user.id):
        return

    subject, html, text = _render_finalized_mail(upload, user)

    if _mail_is_ready():
        try:
            if hasattr(_email_svc, "send_upload_mail"):
                _email_svc.send_upload_mail(
                    to=user.email,
                    subject=subject,
                    html=html,
                    text=text,
                )
            elif hasattr(_email_svc, "send_html_email"):
                _email_svc.send_html_email(
                    to=user.email,
                    subject=subject,
                    html=html,
                    text=text,
                )
            elif hasattr(_email_svc, "send_email"):
                _email_svc.send_email(
                    to=user.email,
                    subject=subject,
                    html=html,
                    text=text,
                )
            elif hasattr(_email_svc, "send_mail"):
                _email_svc.send_mail(
                    to=user.email,
                    subject=subject,
                    html=html,
                    text=text,
                )

            logger.info(
                "Notificación enviada a %s por upload %s",
                user.email,
                upload.id,
            )
        except Exception as e:
            logger.warning(
                "No se pudo enviar mail a %s: %s",
                user.email,
                e,
            )

    # marcar igual como enviado (no repetimos)
    _mark_email_sent(upload.id, user.id)


def _maybe_notify_finalized(upload: UploadModel):
    """
    Si el upload está en estado finalizado/dashboard/tablero, notifica a todos los
    usuarios relevantes solo una vez.
    """
    st = (upload.status or "").lower().strip()
    if st not in ("done", "finalizado", "dashboard", "tablero"):
        return

    followers = _collect_followers(upload)
    for u in followers:
        if not u.email:
            # no hay mail pero igual marcamos
            _mark_email_sent(upload.id, u.id)
            continue
        _send_finalized_mail(upload, u)
# ======================================================================
# SOPORTE PARA RESETEO DE CONTRASEÑA POR TOKEN
# ======================================================================
_PASSWORD_RESET_PATH = BASE_DIR / "data" / "password_resets.json"


def _load_password_resets() -> dict:
    """
    Estructura esperada:
    {
        "token1": {"email": "user@dominio", "created_at": "..."},
        ...
    }
    """
    try:
        if _PASSWORD_RESET_PATH.exists():
            data = json.loads(_PASSWORD_RESET_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_password_resets(data: dict):
    try:
        _PASSWORD_RESET_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning("No se pudo guardar password_resets.json: %s", e)


def _create_password_reset_token(email: str) -> str:
    """
    Crea un token nuevo y lo asocia al email (no revela si existe o no).
    """
    data = _load_password_resets()
    tok = uuid.uuid4().hex
    data[tok] = {
        "email": (email or "").strip().lower(),
        "created_at": dt.datetime.utcnow().isoformat(),
    }
    _save_password_resets(data)
    return tok


def _get_password_reset_email(token: str) -> Optional[str]:
    data = _load_password_resets()
    item = data.get(token)
    if not item:
        return None

    # vencimiento 24h
    try:
        created_at = dt.datetime.fromisoformat(item.get("created_at"))
        if (dt.datetime.utcnow() - created_at).total_seconds() > 86400:
            return None
    except Exception:
        pass

    return item.get("email")


def _consume_password_reset_token(token: str):
    data = _load_password_resets()
    if token in data:
        data.pop(token, None)
    _save_password_resets(data)


def _send_password_reset_email(user: User, token: str):
    """
    Si hay servicio de mail, lo usa. Si no, igual deja el token generado.
    """
    link = f"/password/restablecer?token={token}"
    nombre = user_display(user) or "usuario"

    subject = "[Web Comparativas] Restablecer contraseña"
    html = f"""
        <p>Hola {nombre},</p>
        <p>Recibimos una solicitud para restablecer tu contraseña.</p>
        <p>Podes hacerlo desde aquí: <a href="{link}">{link}</a></p>
        <p>Si no pediste esto, podes ignorar este mensaje.</p>
    """
    text = (
        f"Hola {nombre},\n\n"
        f"Recibimos una solicitud para restablecer tu contraseña.\n"
        f"Link: {link}\n\n"
        f"Si no fuiste vos, ignorá este mensaje."
    )

    if _mail_is_ready():
        try:
            if hasattr(_email_svc, "send_html_email"):
                _email_svc.send_html_email(
                    to=user.email,
                    subject=subject,
                    html=html,
                    text=text,
                )
            elif hasattr(_email_svc, "send_email"):
                _email_svc.send_email(
                    to=user.email,
                    subject=subject,
                    html=html,
                    text=text,
                )
            elif hasattr(_email_svc, "send_mail"):
                _email_svc.send_mail(
                    to=user.email,
                    subject=subject,
                    html=html,
                    text=text,
                )
            logger.info("Mail de reset enviado a %s", user.email)
        except Exception as e:
            logger.warning(
                "No se pudo enviar mail de reset a %s: %s",
                user.email,
                e,
            )


# ======================================================================
# VISIBILIDAD (OBSOLETA, PERO DEJADA POR COMPATIBILIDAD)
# ======================================================================
def _apply_visibility(qry, user: User):
    """
    **OBSOLETA**: mantenida por compatibilidad.
    La visibilidad real ahora se logra con uploads_visible_query(...).
    """
    role = (getattr(user, "role", "") or getattr(user, "rol", "") or "").lower()
    if role in ("admin", "supervisor", "auditor"):
        return qry
    return qry.filter(UploadModel.user_id == user.id)


# ======================================================================
# HOME: recopilación de datos
# ======================================================================
def _home_collect(user: User):
    """
    Compila los datos del panel con visibilidad por grupos.
    """
    # KPIs según visibilidad
    kpis = kpis_for_home(db_session, user)

    # cargas visibles (auditor ve todas)
    q_all = uploads_visible_ext(db_session, user).order_by(
        UploadModel.created_at.desc()
    )
    uploads = q_all.all()

    def _is_pending(st: str) -> bool:
        s = (st or "").lower()
        return s in ("pending", "classifying", "processing", "reviewing")

    def _is_done(st: str) -> bool:
        s = (st or "").lower()
        return s in ("done", "dashboard", "finalizado", "tablero")

    pending = [u for u in uploads if _is_pending(u.status)]
    done = [u for u in uploads if _is_done(u.status)]

    # últimos finalizados (visibles)
    last_done = vis_recent_done(db_session, user, limit=3)

    # “recent_done” en últimas 24h
    now = dt.datetime.utcnow()
    recent_done_24h = [
        u
        for u in done
        if u.updated_at and (now - u.updated_at).total_seconds() < 86400
    ]

    return {
        "uploads": uploads,
        "pending": pending,
        "done": done,
        "last_done": list(last_done),
        "recent_done": recent_done_24h,
        "total_all": int(kpis.get("total", 0)),
        "total_pending": int(kpis.get("pending", 0)),
        "total_done": int(kpis.get("done", 0)),
    }

# ======================================================================
# HOME: selección de Mercado (Menú principal)
# ======================================================================
@app.get("/", response_class=HTMLResponse)
def markets_home(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Pantalla principal: permite elegir Mercado Público o Mercado Privado.
    """
    ctx = {
        "request": request,
        "user": user,
    }
    return templates.TemplateResponse("markets_home.html", ctx)


# ======================================================================
# MERCADO PRIVADO (placeholder)
# ======================================================================
@app.get("/mercado-privado", response_class=HTMLResponse)
def mercado_privado_placeholder(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Placeholder temporal para Mercado Privado.
    Más adelante se reemplaza por el módulo real.
    """
    return HTMLResponse(
        "<div style='font-family:system-ui;padding:32px;'>"
        "<h2>Módulo Mercado Privado en construcción</h2>"
        "<p>Próximamente vas a poder acceder a las herramientas del mercado privado desde aquí.</p>"
        "</div>"
    )

# ======================================================================
# MERCADO PÚBLICO: helpers comunes
# ======================================================================
def _render_mercado_publico_home(request: Request, user: User):
    """
    Panel principal del Mercado Público.
    Por ahora muestra el resumen de Web Comparativas.
    """
    data = _home_collect(user)

    step_labels = {
        "pending": "Pendiente",
        "classifying": "Validado y clasificado",
        "processing": "Procesando",
        "reviewing": "En revisión",
        "dashboard": "Tablero",
        "done": "Finalizado",
        "error": "Error",
    }

    ctx = {
        "request": request,
        "user": user,
        "step_labels": step_labels,
        **data,
    }
    return templates.TemplateResponse("home.html", ctx)


# ======================================================================
# MERCADO PÚBLICO: rutas principales
# ======================================================================
@app.get("/mercado-publico", response_class=HTMLResponse)
def mercado_publico_home(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Home (panel principal) del Mercado Público.
    """
    return _render_mercado_publico_home(request, user)


@app.get("/mercado-publico/web-comparativas", response_class=HTMLResponse)
def mercado_publico_web_comparativas(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Home específico de Web Comparativas dentro de Mercado Público.
    Reutiliza el mismo panel principal.
    """
    return _render_mercado_publico_home(request, user)


@app.get("/mercado-publico/oportunidades", response_class=HTMLResponse)
def mercado_publico_oportunidades(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Módulo Oportunidades (placeholder).
    """
    ctx = {
        "request": request,
        "user": user,
    }
    return templates.TemplateResponse("oportunidades.html", ctx)


@app.get("/mercado-publico/reporte-perfiles", response_class=HTMLResponse)
def mercado_publico_reporte_perfiles(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Módulo Reporte de Perfiles (placeholder).
    """
    ctx = {
        "request": request,
        "user": user,
    }
    return templates.TemplateResponse("reporte_perfiles.html", ctx)

# ======================================================================
# OPORTUNIDADES: Buscador & Dimensiones
# ======================================================================

@app.get("/oportunidades/buscador", response_class=HTMLResponse)
def oportunidades_buscador(
    request: Request,
    q: str = Query(""),
    buyer: str = Query(""),
    platform: str = Query(""),
    province: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=10, le=200),
    uploaded: int = Query(0),  # <--- NUEVO: para mostrar toast tras subir
    user: User = Depends(require_roles("admin", "analista", "supervisor", "auditor")),
):
    opp_info = _oportunidades_status()
    toast_msg = None
    if uploaded:
        filas = opp_info.get("rows")
        toast_msg = f"Archivo cargado correctamente. Filas: {filas}" if filas is not None else "Archivo cargado correctamente."

    df_all = _opp_load_df()

    if df_all is None or df_all.empty:
        ctx = {
            "request": request,
            "user": user,
            "opp": opp_info,
            "toast": toast_msg,   # <--- usar el toast aquí
            "kpis": {"total_rows": 0, "buyers": 0, "platforms": 0, "provinces": 0, "budget_total": 0.0, "date_min": "", "date_max": ""},
            "filters": {"q": q, "buyer": buyer, "platform": platform, "province": province, "date_from": date_from, "date_to": date_to},
            "table_cols": [],
            "table_rows": [],
            "total": 0,
            "page": 1,
            "pages": 1,
            "page_size": page_size,
            "showing_from": 0,
            "showing_to": 0,
        }
        fallback_html = (
            "<div style='font-family:system-ui;padding:32px;'>"
            "<h2>Oportunidades · Buscador</h2>"
            "<p>No hay archivo maestro aún. Subí un Excel para habilitar el dashboard.</p>"
            "</div>"
        )
        return _render_or_fallback("oportunidades_buscador.html", ctx, fallback_html)

    df_filtered = _opp_apply_filters(df_all, q, buyer, platform, province, date_from, date_to)
    kpis = _opp_compute_kpis(df_filtered)

    proc_col   = _opp_pick(df_filtered, ["N° Proceso", "Nro Proceso", "Proceso", "Expediente"])
    fecha_col  = _opp_pick(df_filtered, ["Fecha Apertura", "Apertura", "Fecha", "Fecha de Publicación", "Publicación"])
    buyer_col  = _opp_pick(df_filtered, ["Comprador", "Repartición", "Entidad", "Organismo", "Unidad Compradora", "Buyer"])
    prov_col   = _opp_pick(df_filtered, ["Provincia", "Provincia/Municipio", "Municipio", "Jurisdicción", "Localidad", "Departamento"])
    platf_col  = _opp_pick(df_filtered, ["Plataforma", "Portal", "Origen", "Sistema", "Platform"])
    presu_col  = _opp_pick(df_filtered, ["Presupuesto oficial", "Presupuesto", "Monto", "Importe Total", "Total Presupuesto", "Monto Total", "Importe"])
    desc_col   = _opp_pick(df_filtered, ["Descripción", "Descripcion", "Objeto", "Detalle"])

    colmap = []
    for label, col in [
        ("Proceso", proc_col),
        ("Fecha", fecha_col),
        ("Comprador", buyer_col),
        ("Provincia/Municipio", prov_col),
        ("Plataforma", platf_col),
        ("Presupuesto", presu_col),
        ("Objeto / Descripción", desc_col),
    ]:
        if col:
            colmap.append((label, col))

    total = int(len(df_filtered))
    pages = max(1, int(np.ceil(total / page_size)))
    if page > pages:
        page = pages
    start = (page - 1) * page_size
    end = min(total, start + page_size)
    df_page = df_filtered.iloc[start:end].copy()

    def _san(v):
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        s = str(v)
        return s.replace("<", "&lt;").replace(">", "&gt;")

    table_cols = [lbl for (lbl, _) in colmap]
    table_rows = []
    for _, rec in df_page.iterrows():
        row = {}
        for (lbl, col) in colmap:
            val = rec.get(col, "")
            if _opp_norm(lbl).startswith("presupuesto"):
                val = _opp_parse_number(val)
                row[lbl] = f"$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            elif _opp_norm(lbl) == "fecha":
                d = _opp_parse_date(val)
                row[lbl] = d.strftime("%d/%m/%Y") if d else _san(val)
            else:
                row[lbl] = _san(val)
        table_rows.append(row)

    ctx = {
        "request": request,
        "user": user,
        "opp": opp_info,
        "toast": toast_msg,  # <--- usar el toast aquí también
        "kpis": kpis,
        "filters": {
            "q": q or "",
            "buyer": buyer or "",
            "platform": platform or "",
            "province": province or "",
            "date_from": date_from or "",
            "date_to": date_to or "",
        },
        "table_cols": table_cols,
        "table_rows": table_rows,
        "total": total,
        "page": page,
        "pages": pages,
        "page_size": page_size,
        "showing_from": 0 if total == 0 else (start + 1),
        "showing_to": end,
    }
    fallback_html = (
        "<div style='font-family:system-ui;padding:32px;'>"
        "<h2>Oportunidades · Buscador</h2>"
        "<p>Dashboard cargado.</p>"
        "</div>"
    )
    return _render_or_fallback("oportunidades_buscador.html", ctx, fallback_html)

@app.get("/api/oportunidades/buscador", response_class=JSONResponse)
def api_oportunidades_buscador(
    q: str = Query(""),
    buyer: str = Query(""),
    platform: str = Query(""),
    province: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=10, le=200),
    user: User = Depends(require_roles("admin", "analista", "supervisor", "auditor")),
):
    """
    API JSON para el Buscador de Oportunidades.
    Devuelve KPIs + filas paginadas en formato lista de dicts.
    """
    def _san_str(v):
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        s = str(v or "")
        return s.replace("<", "&lt;").replace(">", "&gt;")

    opp_info = _oportunidades_status()

    df_all = _opp_load_df()
    if df_all is None or df_all.empty:
        return JSONResponse(
            {
                "ok": True,
                "has_file": False,
                "opp": opp_info,
                "kpis": {
                    "total_rows": 0,
                    "buyers": 0,
                    "platforms": 0,
                    "provinces": 0,
                    "budget_total": 0.0,
                    "date_min": "",
                    "date_max": "",
                },
                "filters": {
                    "q": q or "",
                    "buyer": buyer or "",
                    "platform": platform or "",
                    "province": province or "",
                    "date_from": date_from or "",
                    "date_to": date_to or "",
                },
                "total": 0,
                "page": 1,
                "pages": 1,
                "page_size": page_size,
                "from": 0,
                "to": 0,
                "rows": [],
            }
        )

    # aplicar filtros
    df_filtered = _opp_apply_filters(df_all, q, buyer, platform, province, date_from, date_to)
    kpis = _opp_compute_kpis(df_filtered)

    # columnas candidatas (igual que en la vista HTML)
    proc_col   = _opp_pick(df_filtered, ["N° Proceso", "Nro Proceso", "Proceso", "Expediente"])
    fecha_col  = _opp_pick(df_filtered, ["Fecha Apertura", "Apertura", "Fecha", "Fecha de Publicación", "Publicación"])
    buyer_col  = _opp_pick(df_filtered, ["Comprador", "Repartición", "Entidad", "Organismo", "Unidad Compradora", "Buyer"])
    prov_col   = _opp_pick(df_filtered, ["Provincia", "Provincia/Municipio", "Municipio", "Jurisdicción", "Localidad", "Departamento"])
    platf_col  = _opp_pick(df_filtered, ["Plataforma", "Portal", "Origen", "Sistema", "Platform"])
    presu_col  = _opp_pick(df_filtered, ["Presupuesto oficial", "Presupuesto", "Monto", "Importe Total", "Total Presupuesto", "Monto Total", "Importe"])
    desc_col   = _opp_pick(df_filtered, ["Descripción", "Descripcion", "Objeto", "Detalle"])

    total = int(len(df_filtered))
    pages = max(1, int(np.ceil(total / page_size)))
    if page > pages:
        page = pages
    start = (page - 1) * page_size
    end = min(total, start + page_size)
    df_page = df_filtered.iloc[start:end].copy()

    rows_json = []

    for _, rec in df_page.iterrows():
        # Proceso
        v_proc = _san_str(rec.get(proc_col, "")) if proc_col else ""

        # Fecha -> siempre dd/mm/YYYY si se puede
        raw_fecha = rec.get(fecha_col, "") if fecha_col else ""
        d = _opp_parse_date(raw_fecha)
        v_fecha = d.strftime("%d/%m/%Y") if d else _san_str(raw_fecha)

        # Comprador
        v_buyer = _san_str(rec.get(buyer_col, "")) if buyer_col else ""

        # Provincia / Municipio
        v_prov = _san_str(rec.get(prov_col, "")) if prov_col else ""

        # Plataforma
        v_platf = _san_str(rec.get(platf_col, "")) if platf_col else ""

        # Presupuesto: numérico + string formateado
        if presu_col:
            raw_presu = rec.get(presu_col, "")
            presu_num = _opp_parse_number(raw_presu)
            presu_fmt = (
                f"$ {presu_num:,.2f}"
                .replace(",", "X")
                .replace(".", ",")
                .replace("X", ".")
            )
        else:
            presu_num = 0.0
            presu_fmt = ""

        # Descripción / Objeto
        v_desc = _san_str(rec.get(desc_col, "")) if desc_col else ""

        rows_json.append(
            {
                "proceso": v_proc,
                "fecha": v_fecha,
                "comprador": v_buyer,
                "provincia": v_prov,
                "plataforma": v_platf,
                "presupuesto": presu_num,
                "presupuesto_fmt": presu_fmt,
                "descripcion": v_desc,
            }
        )

    showing_from = 0 if total == 0 else (start + 1)
    showing_to = end

    return JSONResponse(
        {
            "ok": True,
            "has_file": True,
            "opp": opp_info,
            "kpis": kpis,
            "filters": {
                "q": q or "",
                "buyer": buyer or "",
                "platform": platform or "",
                "province": province or "",
                "date_from": date_from or "",
                "date_to": date_to or "",
            },
            "total": total,
            "page": page,
            "pages": pages,
            "page_size": page_size,
            "from": showing_from,
            "to": showing_to,
            "rows": rows_json,
        }
    )

@app.post("/oportunidades/buscador/upload")
async def oportunidades_buscador_upload(
    request: Request,
    file: UploadFile = File(...),
    user: User = Depends(require_roles("admin", "analista", "supervisor", "auditor")),
):
    # guarda y calcula filas
    rows = _save_oportunidades_excel(file)
    # redirige al GET que arma KPIs/tabla (evita 'kpis undefined')
    url = request.url_for("oportunidades_buscador")
    url = str(url) + "?uploaded=1"
    return RedirectResponse(url, status_code=303)


@app.get("/oportunidades/dimensiones", response_class=HTMLResponse)
def oportunidades_dimensiones(
    request: Request,
    user: User = Depends(require_roles("admin", "analista", "supervisor", "auditor")),
):
    """
    Vista de Dimensiones (análisis por ejes: región, comprador, cuenta, plataforma, etc.).
    """
    ctx = {"request": request, "user": user}
    fallback_html = (
        "<div style='font-family:system-ui;padding:32px;'>"
        "<h2>Oportunidades · Dimensiones</h2>"
        "<p>Pantalla en construcción. Aquí armamos tableros por ejes/dimensiones (comprador, provincia, plataforma, etc.).</p>"
        "</div>"
    )
    return _render_or_fallback("oportunidades_dimensiones.html", ctx, fallback_html)


# ======================================================================
# CARGAS – NUEVA / CREAR
# ======================================================================
@app.get("/cargas/nueva", response_class=HTMLResponse)
def nueva_carga(
    request: Request,
    user: User = Depends(require_roles("admin", "analista", "supervisor")),
):
    """
    Muestra el formulario para cargar un nuevo archivo.
    """
    return templates.TemplateResponse(
        "upload_form.html",
        {"request": request, "user": user},
    )


@app.post("/cargas", response_class=HTMLResponse)
async def crear_carga(
    request: Request,
    background_tasks: BackgroundTasks,
    # Campos visibles
    proceso_nro: str = Form(""),
    apertura_fecha: str = Form(""),
    cuenta_nro: str = Form(""),
    # Hints (ocultos) + visibles (fallback)
    platform_hint: str = Form(""),
    buyer_hint: str = Form(""),
    province_hint: str = Form(""),
    plataforma: str = Form(""),
    comprador: str = Form(""),
    provincia: str = Form(""),
    # Archivo
    file: UploadFile = File(...),
    user: User = Depends(require_roles("admin", "analista", "supervisor")),
):
    """
    Crea una carga y dispara el procesamiento en segundo plano.
    También evita duplicados usando el proceso_nro normalizado.
    """
    # Carpeta base donde se guardan los uploads
    base_dir = Path("data/uploads")
    base_dir.mkdir(parents=True, exist_ok=True)

    # Carpeta única para esta carga
    uid = str(uuid.uuid4())
    upload_dir = base_dir / uid
    upload_dir.mkdir(parents=True, exist_ok=True)

    # Leer el archivo en memoria (bytes)
    file_bytes = await file.read()

    # Guardar archivo subido en disco (como hasta ahora)
    file_path = upload_dir / file.filename
    with open(file_path, "wb") as f:
        f.write(file_bytes)

    # Normalizar fecha de apertura
    def _norm_date(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        # formato AAAA-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return s
        # formato DD/MM/AAAA o DD-MM-AAAA
        m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$", s)
        if m:
            d, mm, y = map(int, m.groups())
            return f"{y:04d}-{mm:02d}-{d:02d}"
        return s

    # Fallback: si los hints ocultos vienen vacíos, usar los visibles
    platform_hint = (platform_hint or plataforma).strip()
    buyer_hint = (buyer_hint or comprador).strip()
    province_hint = (province_hint or provincia).strip()

    # Normalizamos el número de proceso para detectar duplicados
    proceso_nro_clean = (proceso_nro or "").strip() or None
    proceso_key = normalize_proceso_nro(proceso_nro_clean)

    # Si hay clave normalizada, buscamos si ya existe una carga con ese proceso
    existing = None
    if proceso_key:
        existing = (
            db_session.query(UploadModel)
            .filter(func.upper(func.trim(UploadModel.proceso_key)) == proceso_key)
            .order_by(UploadModel.created_at.desc())
            .first()
        )

    if existing:
        # No creamos nada nuevo: redirigimos al proceso existente
        return RedirectResponse(
            f"/cargas/{existing.id}?dup=1",
            status_code=303,
        )

    # Crear la carga nueva
    up = UploadModel(
        user_id=user.id,
        proceso_nro=proceso_nro_clean,
        proceso_key=proceso_key,  # puede ser None si no hay proceso cargado
        apertura_fecha=_norm_date(apertura_fecha) or None,
        cuenta_nro=(cuenta_nro or "").strip() or None,
        platform_hint=platform_hint or None,
        buyer_hint=buyer_hint or None,
        province_hint=province_hint or None,
        original_filename=file.filename,
        original_path=str(file_path),
        base_dir=str(upload_dir),
        status="pending",
        created_at=dt.datetime.utcnow(),
        updated_at=dt.datetime.utcnow(),
    )
    db_session.add(up)
    db_session.commit()

    # Procesar en segundo plano
    background_tasks.add_task(classify_and_process, up.id, {})

    return RedirectResponse(f"/cargas/{up.id}", status_code=303)


# ======================================================================
# HISTORIAL DE CARGAS (con filtros + paginación + visibilidad)
# ======================================================================
from math import ceil
from sqlalchemy import and_  # noqa: F401 (puede usarse en filtros futuros)


def _parse_date_like(s: str) -> Optional[dt.date]:
    """
    Acepta 'AAAA-MM-DD' o 'DD/MM/AAAA' y devuelve date. Tolera varios formatos.
    """
    s = (s or "").strip()
    if not s:
        return None
    try:
        # AAAA-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            y, m, d = map(int, s.split("-"))
            return dt.date(y, m, d)
        # DD/MM/AAAA
        if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
            d, m, y = map(int, s.split("/"))
            return dt.date(y, m, d)
        # fallback ISO
        return dt.datetime.fromisoformat(s).date()
    except Exception:
        return None


_STATUS_LABELS = {
    "pending": "Pendiente",
    "classifying": "Validado y clasificado",
    "processing": "Procesando",
    "reviewing": "En revisión",
    "dashboard": "Tablero",
    "done": "Finalizado",
    "error": "Error",
}
_STATUS_NORMALIZE = {
    **{k: k for k in _STATUS_LABELS.keys()},
    "pendiente": "pending",
    "validado y clasificado": "classifying",
    "procesando": "processing",
    "en revisión": "reviewing",
    "revision": "reviewing",
    "tablero": "dashboard",
    "finalizado": "done",
}


@app.get("/cargas/historial", response_class=HTMLResponse)
def historial_cargas(
    request: Request,
    # existentes
    q: str = Query("", description="búsqueda"),
    status: str = Query("", description="estado (clave o etiqueta)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=10, le=100),
    # nuevos filtros
    created_from: str = Query("", description="AAAA-MM-DD o DD/MM/AAAA"),
    created_to: str = Query("", description="AAAA-MM-DD o DD/MM/AAAA"),
    uploader_id: str = Query("", description="ID de usuario (Subido por)"),
    proceso: str = Query("", description="N° de proceso"),
    cuenta: str = Query("", description="Cuenta"),
    platform: str = Query("", description="Plataforma"),
    buyer: str = Query("", description="Comprador"),
    province: str = Query("", description="Provincia/Municipio"),
    filename: str = Query("", description="Nombre de archivo"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Listado del historial de cargas, con visibilidad por grupos.
    Permite filtrar por casi todos los campos relevantes.
    """
    error = None
    rows = []
    users_by_id: Dict[int, User] = {}
    total = 0
    pages = 1

    try:
        # 👇 👇 👇 AQUÍ USAMOS LA VERSIÓN QUE DEJA VER TODO AL AUDITOR
        base_qry = uploads_visible_ext(db_session, user)

        # búsqueda general
        if q and q.strip():
            like = f"%{q.strip()}%"
            base_qry = base_qry.filter(
                or_(
                    UploadModel.proceso_nro.ilike(like),
                    UploadModel.cuenta_nro.ilike(like),
                    UploadModel.platform_hint.ilike(like),
                    UploadModel.buyer_hint.ilike(like),
                    UploadModel.province_hint.ilike(like),
                    UploadModel.original_filename.ilike(like),
                    UploadModel.status.ilike(like),
                )
            )

        # campos específicos
        if proceso.strip():
            base_qry = base_qry.filter(
                UploadModel.proceso_nro.ilike(f"%{proceso.strip()}%")
            )
        if cuenta.strip():
            base_qry = base_qry.filter(
                UploadModel.cuenta_nro.ilike(f"%{cuenta.strip()}%")
            )
        if platform.strip():
            base_qry = base_qry.filter(
                UploadModel.platform_hint.ilike(f"%{platform.strip()}%")
            )
        if buyer.strip():
            base_qry = base_qry.filter(
                UploadModel.buyer_hint.ilike(f"%{buyer.strip()}%")
            )
        if province.strip():
            base_qry = base_qry.filter(
                UploadModel.province_hint.ilike(f"%{province.strip()}%")
            )
        if filename.strip():
            base_qry = base_qry.filter(
                UploadModel.original_filename.ilike(f"%{filename.strip()}%")
            )

        # estado
        if status and status.strip():
            st_key = _STATUS_NORMALIZE.get(
                status.strip().lower(), status.strip().lower()
            )
            base_qry = base_qry.filter(
                func.lower(func.trim(UploadModel.status)) == st_key
            )

        # rango de fechas
        d_from = _parse_date_like(created_from)
        d_to = _parse_date_like(created_to)
        if d_from:
            base_qry = base_qry.filter(
                UploadModel.created_at
                >= dt.datetime.combine(d_from, dt.time.min)
            )
        if d_to:
            base_qry = base_qry.filter(
                UploadModel.created_at
                < (dt.datetime.combine(d_to, dt.time.min) + dt.timedelta(days=1))
            )

        # subido por (validando visibilidad)
        try:
            _uploader_id = (
                int(str(uploader_id).strip())
                if str(uploader_id).strip()
                else None
            )
        except Exception:
            _uploader_id = None

        if _uploader_id is not None:
            vis_ids = visible_user_ids_ext(db_session, user)
            if _uploader_id in vis_ids:
                base_qry = base_qry.filter(UploadModel.user_id == _uploader_id)
            else:
                # fuerza resultado vacío
                base_qry = base_qry.filter(UploadModel.user_id == -999999)

        # paginación
        total = base_qry.count()
        pages = max(1, ceil(total / page_size))
        if page > pages:
            page = pages

        order_by = (
            UploadModel.created_at.desc()
            if hasattr(UploadModel, "created_at")
            else UploadModel.id.desc()
        )
        rows = (
            base_qry.order_by(order_by)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )

        # mapa de usuarios (para mostrar quien subió cada una)
        uids = {r.user_id for r in rows if getattr(r, "user_id", None)}
        if uids:
            users = db_session.query(User).filter(User.id.in_(uids)).all()
            users_by_id = {u.id: u for u in users}

        # lista de usuarios visibles (para el filtro "Subido por")
        vis_ids_all = list(visible_user_ids_ext(db_session, user))
        vis_users = (
            db_session.query(User).filter(User.id.in_(vis_ids_all)).all()
        )
        users_options = sorted(
            [
                {"id": u.id, "label": user_display(u)}
                for u in vis_users
            ],
            key=lambda x: (x["label"] or "").lower(),
        )
    except Exception as e:
        db_session.rollback()
        error = str(e)
        users_options = []

    # info de paginación
    can_view_all = True
    showing_from = 0 if total == 0 else (page - 1) * page_size + 1
    showing_to = min(total, page * page_size)

    # filtros usados (para re-render)
    flt = {
        "q": q or "",
        "status": status or "",
        "created_from": created_from or "",
        "created_to": created_to or "",
        "uploader_id": str(_uploader_id or ""),
        "proceso": proceso or "",
        "cuenta": cuenta or "",
        "platform": platform or "",
        "buyer": buyer or "",
        "province": province or "",
        "filename": filename or "",
    }

    ctx = {
        "request": request,
        "user": user,
        "rows": rows,
        "q": q or "",
        "status": status or "",
        "error": error,
        "can_view_all": can_view_all,
        "users_by_id": users_by_id,
        "users_options": users_options,
        "status_labels": _STATUS_LABELS,
        "total": total,
        "page": page,
        "pages": pages,
        "page_size": page_size,
        "showing_from": showing_from,
        "showing_to": showing_to,
        "filters": flt,
    }
    return templates.TemplateResponse("uploads_list.html", ctx)


# ======================================================================
# SEGUIMIENTO: VISTA DE DETALLE
# ======================================================================
@app.get("/cargas/{upload_id}", response_class=HTMLResponse)
def view_upload(
    request: Request,
    upload_id: int,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Vista de seguimiento de proceso individual (detalle).
    """
    upload = db_session.get(UploadModel, upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Carga no encontrada")

    PROCESS_STEPS_LOC = {
        "pending": "Pendiente",
        "classifying": "Validado y clasificado",
        "processing": "Procesando",
        "reviewing": "En revisión",
        "dashboard": "Tablero",
        "done": "Finalizado",
        "error": "Error",
    }

    # 👉 si ya está finalizado, lanzamos aviso (idempotente)
    try:
        _maybe_notify_finalized(upload)
    except Exception as e:
        logger.warning("No se pudo disparar notificación en /cargas/{id}: %s", e)

    ctx = {
        "request": request,
        "user": user,
        "upload": upload,
        "upload_steps": PROCESS_STEPS_LOC,
        "dup": request.query_params.get("dup") == "1",  # <-- 👈 NUEVO
    }
    return templates.TemplateResponse("upload_show.html", ctx)


# ======================================================================
# API: ESTADO Y PROGRESO DE LA CARGA
# ======================================================================
@app.get("/api/cargas/{upload_id}/status", response_class=JSONResponse)
def api_carga_status(
    upload_id: int,
    user: User = Depends(
        require_roles("admin", "analista", "supervisor", "auditor")
    ),
):
    """
    Devuelve info de la carga para la UI dinámica (stepper, botones, etc.).
    También dispara la notificación de finalizado de forma segura.
    """
    up = db_session.get(UploadModel, upload_id)
    if not up:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)

    # Enriquecido desde services
    try:
        enriched = svc_get_status(upload_id) or {}
    except Exception:
        enriched = {}

    # armar steps
    steps = [{"key": k, "label": lbl} for (k, lbl) in PROCESS_STEPS]

    st_raw = (up.status or "").strip()
    st = st_raw.lower()

    idx_map = {k: i for i, (k, _) in enumerate(PROCESS_STEPS)}
    cur_idx = idx_map.get(st, 0)

    if cur_idx == 0:
        # heurística por label
        for i, (k, lbl) in enumerate(PROCESS_STEPS):
            if lbl.lower() in st:
                cur_idx = i
                break

    for i, s in enumerate(steps):
        s["done"] = i < cur_idx
        s["current"] = i == cur_idx

    normalized_ready = bool(enriched.get("normalized_ready"))
    role = (user.role or "").lower()
    is_admin = role == "admin"

    # Para no-admin: alcanza con que el proceso esté en estado “final”
    require_ready = st in FINAL_STATES

    # Reglas de apertura de tablero / descarga
    can_open_dashboard = (normalized_ready if is_admin else require_ready)
    can_download_normalized = (normalized_ready if is_admin else require_ready)

    # 👉 Notificación idempotente cuando entra a estado final
    if st in FINAL_STATES:
        try:
            _maybe_notify_finalized(up)
        except Exception as e:
            logger.warning("No se pudo notificar finalización (status API): %s", e)

    data = {
        "ok": True,
        "id": up.id,
        "status": st or "pending",
        "steps": steps,
        "index": cur_idx,
        "total": len(PROCESS_STEPS),
        "adapter": enriched.get("adapter") or getattr(up, "adapter_name", None),
        "normalized_ready": normalized_ready,
        "can_open_dashboard": can_open_dashboard,
        "can_download_normalized": can_download_normalized,
        "dashboard_url": f"/tablero/{up.id}",
    }
    return JSONResponse(data)


# ======================================================================
# API PARA “VISTAS GUARDADAS”
# ======================================================================
def _parse_bool_like(v) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in {"1", "true", "t", "si", "sí", "yes", "y", "on"}


def _parse_payload_any(p) -> dict:
    if isinstance(p, dict):
        return p
    if p is None:
        return {}
    try:
        return json.loads(p)
    except Exception:
        return {}


@app.get("/api/views", response_class=JSONResponse)
def api_views_list(
    view_id: str = Query("dashboard"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Lista todas las vistas del usuario para una vista dada.
    """
    try:
        views = sv_list_views(user.id, view_id=view_id)
        return JSONResponse({"ok": True, "views": views})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.get("/api/views/default", response_class=JSONResponse)
def api_views_default(
    view_id: str = Query("dashboard"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Obtiene la vista por defecto del usuario (si existe).
    """
    try:
        v = sv_get_default_view(user.id, view_id=view_id)
        return JSONResponse({"ok": True, "view": v})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.get("/api/views/get", response_class=JSONResponse)
def api_views_get(
    view_id: str = Query("dashboard"),
    id: Optional[int] = Query(None),
    name: str = Query(""),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Obtiene una vista por id o por nombre.
    """
    try:
        v = sv_get_view(
            user.id,
            view_id=view_id,
            view_pk=id,
            name=(name or None),
        )
        return JSONResponse({"ok": True, "view": v})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/views", response_class=JSONResponse)
async def api_views_save(
    request: Request,
    # soporte también para form-data
    view_id: str = Form("dashboard"),
    name: str = Form(None),
    payload: str = Form(None),
    is_default: str = Form("0"),
    replace_existing: str = Form("1"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Crea/actualiza una vista.
    """
    try:
        # si vino JSON en el body
        if request.headers.get("content-type", "").lower().startswith(
            "application/json"
        ):
            data = await request.json()
            view_id = str(data.get("view_id", view_id) or "dashboard")
            name = data.get("name", name)
            payload = data.get("payload", payload)
            is_default = data.get("is_default", is_default)
            replace_existing = data.get("replace_existing", replace_existing)

        name = (name or "").strip()
        if not name:
            return JSONResponse(
                {"ok": False, "error": "name_required"},
                status_code=400,
            )

        pv = _parse_payload_any(payload)
        res = sv_save_view(
            user.id,
            view_id=view_id,
            name=name,
            payload=pv,
            is_default=_parse_bool_like(is_default),
            replace_existing=_parse_bool_like(replace_existing),
        )
        return JSONResponse({"ok": True, "view": res})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/views/default", response_class=JSONResponse)
async def api_views_set_default(
    request: Request,
    view_id: str = Form("dashboard"),
    id: Optional[int] = Form(None),
    name: str = Form(""),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Marca una vista como 'por defecto' (por id o por nombre).
    """
    try:
        if request.headers.get("content-type", "").lower().startswith(
            "application/json"
        ):
            data = await request.json()
            view_id = data.get("view_id", view_id)
            id = data.get("id", id)
            name = data.get("name", name)

        if id is None and not (name or "").strip():
            return JSONResponse(
                {"ok": False, "error": "id_or_name_required"},
                status_code=400,
            )

        res = sv_set_default_view(
            user.id,
            view_id=view_id,
            view_pk=id,
            name=(name or None),
        )
        return JSONResponse({"ok": True, "view": res})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.delete("/api/views/{view_pk}", response_class=JSONResponse)
def api_views_delete(
    view_pk: int,
    view_id: str = Query("dashboard"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Elimina una vista por su id.
    """
    try:
        ok = sv_delete_view(
            user.id,
            view_id=view_id,
            view_pk=int(view_pk),
        )
        return JSONResponse({"ok": bool(ok)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


# ======================================================================
# HELPERS DE ESTADO / ARCHIVOS
# ======================================================================
def _norm_status(s: str) -> str:
    """
    Normaliza un estado posible (en español/variantes) a la clave estándar.
    """
    s = (s or "").strip().lower()
    mapping = {
        "pendiente": "pending",
        "validado y clasificado": "classifying",
        "validando y clasif.": "classifying",
        "val. y clasif.": "classifying",
        "procesando": "processing",
        "en revisión": "reviewing",
        "revision": "reviewing",
        "tablero": "dashboard",
        "finalizado": "done",
        "error": "error",
    }
    return mapping.get(s, s)


def _needs_processing(up: UploadModel) -> bool:
    """
    True si NO existe processed/normalized.xlsx todavía.
    """
    try:
        return not services.normalized_exists(up)
    except Exception:
        # si hay algún error, forzamos a procesar
        return True


# ======================================================================
# ENDPOINT: AVANZAR / RETROCEDER ESTADO (ADMIN)
# ======================================================================
@app.post("/api/cargas/{upload_id}/avance", response_class=JSONResponse)
def api_carga_avance(
    upload_id: int,
    action: str = Form(...),  # "next" | "prev" | "goto"
    target: str = Form(""),  # cuando action="goto"
    force: int = Form(0),  # 1 para forzar salto a dashboard/done
    background_tasks: BackgroundTasks = None,
    user: User = Depends(require_roles("admin")),
):
    """
    Cambia manualmente el estado de una carga SIN romper el flujo original.

    Reglas:
    - Orden canónico: pending → classifying → processing → reviewing → dashboard → done
    - Al entrar a 'processing' y si falta normalized.xlsx → se dispara classify_and_process.
    - Para ir a 'dashboard'/'done' se exige normalized.xlsx (a menos que force=1).
    - Al llegar a estado finalizado/dashboard/tablero → dispara notificación única.
    """
    up = db_session.get(UploadModel, upload_id)
    if not up:
        raise HTTPException(status_code=404, detail="Proceso no encontrado")

    orden = list(services.STEP_KEYS)  # fuente de verdad
    st = _norm_status(getattr(up, "status", "") or "pending")
    if st not in orden:
        st = "pending"
    idx = orden.index(st)

    # determinar destino
    if action == "next":
        if idx >= len(orden) - 1:
            return JSONResponse(
                {"ok": False, "msg": "Ya está en el último estado"},
                status_code=400,
            )
        new_status = orden[idx + 1]
    elif action == "prev":
        if idx <= 0:
            return JSONResponse(
                {"ok": False, "msg": "Ya está en el primer estado"},
                status_code=400,
            )
        new_status = orden[idx - 1]
    elif action == "goto":
        t = _norm_status(target)
        if t not in orden:
            return JSONResponse(
                {"ok": False, "msg": "Destino inválido"},
                status_code=400,
            )
        new_status = t
    else:
        return JSONResponse(
            {"ok": False, "msg": "Acción inválida"},
            status_code=400,
        )

    force_bool = bool(int(force or 0))

    # si el destino requiere normalized.xlsx
    if (
        new_status in ("dashboard", "done")
        and _needs_processing(up)
        and not force_bool
    ):
        # si estamos en un estado que permite procesar, intentamos generarlo YA
        if st in ("processing", "reviewing"):
            try:
                classify_and_process(upload_id, {})
                db_session.refresh(up)
                if _needs_processing(up):
                    return JSONResponse(
                        {
                            "ok": False,
                            "msg": "No se pudo preparar el normalized.xlsx automáticamente.",
                        },
                        status_code=409,
                    )
            except Exception as e:
                return JSONResponse(
                    {
                        "ok": False,
                        "msg": f"No se pudo preparar el normalized.xlsx: {e}",
                    },
                    status_code=409,
                )
        else:
            return JSONResponse(
                {
                    "ok": False,
                    "msg": (
                        "Aún no existe processed/normalized.xlsx. Pase por 'processing' "
                        "para correr el pipeline (o use force=1)."
                    ),
                },
                status_code=409,
            )

    # persistir cambio de estado
    up.status = new_status
    up.updated_at = dt.datetime.utcnow()
    db_session.commit()
    db_session.refresh(up)

    # si quedó finalizado → notificar
    if new_status in ("dashboard", "done", "finalizado", "tablero"):
        try:
            _maybe_notify_finalized(up)
        except Exception as e:
            logger.warning(
                "No se pudo enviar notificación en avance manual: %s", e
            )

    # si entramos a 'processing' y hace falta procesar → disparar pipeline
    try:
        if new_status == "processing" and _needs_processing(up):
            if background_tasks is not None:
                background_tasks.add_task(classify_and_process, up.id, {})
            else:
                import threading

                threading.Thread(
                    target=classify_and_process,
                    args=(up.id, {}),
                    daemon=True,
                ).start()
    except Exception as e:
        logger.exception("No se pudo disparar classify_and_process: %s", e)

    # respuesta coherente con el stepper
    label_map = dict(services.PROCESS_STEPS)
    cur_idx = services.step_index(up.status)
    steps_json = []
    for i, key in enumerate(services.STEP_KEYS):
        steps_json.append(
            {
                "key": key,
                "label": label_map.get(key, key.title()),
                "done": i < cur_idx,
                "current": i == cur_idx,
            }
        )

    f = services.refresh_flags(up)
    status_code = (
        202 if (new_status == "processing" and _needs_processing(up)) else 200
    )

    return JSONResponse(
        {
            "ok": True,
            "id": up.id,
            "status": up.status,
            "steps": steps_json,
            "index": cur_idx,
            "total": len(services.STEP_KEYS),
            "adapter": f["adapter"],
            "normalized_ready": f["normalized_ready"],
            "can_open_dashboard": f["can_open_dashboard"],
            "dashboard_url": f"/tablero/{up.id}",
        },
        status_code=status_code,
    )
# ======================================================================
# DF PROCESADO (normalized.xlsx)
# ======================================================================
def _load_processed_df(upload: UploadModel) -> Optional[pd.DataFrame]:
    """
    Carga el normalized.xlsx asociado al upload.
    Devuelve un DataFrame o None si no existe.
    """
    try:
        norm_path = services.get_normalized_path(upload)
        if not norm_path or not norm_path.exists():
            return None
        df = pd.read_excel(norm_path)
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        print("[_load_processed_df] Error:", e)
        return None


# ======================================================================
# API JSON PARA RANKING DEL TABLERO
# ======================================================================
def _san(v):
    """
    Sanea texto simple para JSON.
    """
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (int, float)):
        return float(v) if isinstance(v, float) else int(v)
    s = str(v)
    return s.replace("<", "&lt;").replace(">", "&gt;")


def _norm_key(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return re.sub(r"[\s._/\-]+", "", s.strip().lower())


def _pick(df: pd.DataFrame, candidates) -> Optional[str]:
    cols = {_norm_key(c): c for c in df.columns}
    for cand in candidates:
        c = cols.get(_norm_key(cand))
        if c:
            return c
    return None


def _to_num(x) -> float:
    s = str(x or "").strip()
    if not s:
        return 0.0
    # quita separador de miles "." y usa punto como decimal
    s = re.sub(r"\.(?=\d{3}(\D|$))", "", s)
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


@app.get("/api/tablero/{upload_id}/ranking", response_class=JSONResponse)
def api_tablero_ranking(
    upload_id: int,
    max_positions: Optional[int] = Query(
        None, description="Limitar cantidad de puestos por renglón"
    ),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    """
    Ranking robusto para el tablero.
    """
    up = db_session.get(UploadModel, upload_id)
    if not up:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)

    # 🔒 visibilidad por grupos (pero auditor ve todo)
    if up.user_id not in visible_user_ids_ext(db_session, user):
        raise HTTPException(status_code=403, detail="No autorizado")

    df = _load_processed_df(up)
    if df is None or df.empty:
        return JSONResponse(
            {"ok": True, "rows": [], "max_pos": 0, "total_rows": 0}
        )

    cols = [str(c).strip() for c in df.columns]

    def _find(pred):
        for c in cols:
            if pred(c.lower()):
                return c
        return None

    item_col = _find(
        lambda s: s
        in {
            "n°",
            "nro",
            "item",
            "numero",
            "número",
            "renglón",
            "renglon",
        }
    )
    desc_col = _find(lambda s: s.startswith("descrip"))
    prov_col = _find(
        lambda s: "proveedor" in s or "oferente" in s or "empresa" in s
    )
    price_col = (
        _find(
            lambda s: ("precio" in s and "unit" in s)
            or "unitario" in s
        )
        or _find(
            lambda s: (
                "precio" in s
                and "total" not in s
                and "importe" not in s
            )
        )
    )
    qty_req_col = _find(
        lambda s: "cantidad" in s and "solicit" in s
    )
    qty_off_col = _find(
        lambda s: "cantidad" in s and ("ofertad" in s or "ofrec" in s)
    )

    def _to_num_local(x):
        try:
            if isinstance(x, str):
                s = x.strip()
                s = s.replace(".", "").replace(",", ".")
                v = float(s)
            else:
                v = float(x)
            return v if np.isfinite(v) else 0.0
        except Exception:
            return 0.0

    if not (item_col and desc_col and prov_col and price_col):
        return JSONResponse(
            {"ok": True, "rows": [], "max_pos": 0, "total_rows": 0}
        )

    # recorrer filas
    rows_map: Dict[str, Dict[str, Any]] = {}  # item -> datos

    for rec in df.to_dict(orient="records"):
        nro = str(rec.get(item_col, "")).strip()
        if not nro:
            continue
        desc = str(rec.get(desc_col, "")).strip()
        prov = str(rec.get(prov_col, "")).strip()
        if not prov:
            continue
        unit = _to_num_local(rec.get(price_col))
        if unit <= 0:
            continue
        qty_req = _to_num_local(rec.get(qty_req_col)) if qty_req_col else 0.0
        qty_off = _to_num_local(rec.get(qty_off_col)) if qty_off_col else 0.0
        if qty_off <= 0 and qty_req > 0:
            qty_off = qty_req

        r = rows_map.setdefault(
            nro,
            {
                "nro": nro,
                "descripcion": "",
                "cantidad": 0.0,
                "offers": {},
            },
        )
        if desc and not r["descripcion"]:
            r["descripcion"] = desc
        if (qty_req > 0) and (r["cantidad"] <= 0):
            r["cantidad"] = float(qty_req)

        cur = r["offers"].get(prov)
        if (cur is None) or (unit < cur["precio"]):
            r["offers"][prov] = {
                "precio": float(unit),
                "cantidad": float(
                    qty_off if qty_off > 0 else r["cantidad"] or 0.0
                ),
            }

    # salida ordenada
    rows_out: List[Dict[str, Any]] = []
    max_pos_found = 0

    for _, r in rows_map.items():
        offers = [
            {
                "proveedor": p,
                "precio": v["precio"],
                "cantidad": v["cantidad"],
            }
            for p, v in r["offers"].items()
        ]
        offers.sort(key=lambda x: (x["precio"], x["proveedor"]))

        if max_positions is not None:
            offers = offers[: int(max_positions)]

        positions = [{"pos": i + 1, **o} for i, o in enumerate(offers)]
        max_pos_found = max(max_pos_found, len(positions))

        rows_out.append(
            {
                "nro": r["nro"],
                "descripcion": r["descripcion"],
                "cantidad": float(
                    r["cantidad"] if r["cantidad"] > 0 else 1.0
                ),
                "positions": positions,
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "rows": rows_out,
            "max_pos": int(max_pos_found),
            "total_rows": int(len(rows_out)),
        }
    )


# ======================================================================
# KPI Y GRÁFICOS DEL TABLERO
# ======================================================================
def compute_kpis_and_charts(df: pd.DataFrame):
    """
    Calcula KPIs y gráficos base del tablero.
    """
    kpis: Dict[str, Any] = {}
    chart_sup = {"labels": [], "values": []}
    chart_pos = {"labels": [], "values": []}

    if df is None or df.empty:
        kpis.update(
            {
                "total_offers": 0.0,
                "awarded": 0.0,
                "pct_over_awarded": 0.0,
                "bidders": 0,
                "items": 0,
                "offers": 0,
            }
        )
        return kpis, chart_sup, chart_pos

    try:
        prov_col = _pick(
            df,
            ["Proveedor", "Empresa", "Oferente", "Supplier", "Provider"],
        )
        pos_col = _pick(
            df,
            ["Posición", "Posicion", "Rank", "Orden"],
        )
        desc_col = next(
            (c for c in df.columns if str(c).lower().startswith("descrip")),
            None,
        )
        item_col = _pick(
            df,
            [
                "N°",
                "Nro",
                "Item",
                "Número",
                "Numero",
                "Renglón",
                "Renglon",
            ],
        )
        price_col = _pick(
            df,
            [
                "Precio unitario",
                "Precio Unitario",
                "Precio",
                "PU",
                "Unit Price",
            ],
        )
        qty_off_col = _pick(
            df,
            [
                "Cantidad ofertada",
                "Cant. ofertada",
                "Cantidad ofrecida",
                "Qty Offered",
            ],
        )
        qty_req_col = _pick(
            df,
            [
                "Cantidad solicitada",
                "Cant. solicitada",
                "Cantidad requerida",
                "Cantidad",
                "Cant",
                "Qty",
                "Unidades",
            ],
        )
        total_col = _pick(
            df,
            [
                "Total por renglón",
                "Total por renglon",
                "Total",
                "Importe",
                "Amount",
            ],
        )

        if price_col and (qty_off_col or qty_req_col):
            qcol = qty_off_col or qty_req_col
            tot_series = (
                pd.to_numeric(df[price_col], errors="coerce").fillna(0)
                * pd.to_numeric(df[qcol], errors="coerce").fillna(0)
            )
        elif total_col:
            tot_series = (
                pd.to_numeric(df[total_col], errors="coerce")
                .fillna(0)
            )
        else:
            tot_series = pd.Series(
                [0] * len(df),
                index=df.index,
                dtype="float64",
            )

        # total ofertas
        kpis["total_offers"] = float(tot_series.sum())

        # ganadores
        adj_cols = [
            c
            for c in df.columns
            if ("adjud" in c.lower()) or ("ganador" in c.lower())
        ]
        if pos_col is not None:
            winners = pd.to_numeric(df[pos_col], errors="coerce") == 1
        elif adj_cols:
            winners = df[adj_cols[0]].astype(str).str.lower().isin(
                ["si", "sí", "true", "1", "x"]
            )
        else:
            winners = pd.Series([False] * len(df), index=df.index)

        kpis["awarded"] = float(tot_series[winners].sum())
        kpis["pct_over_awarded"] = (
            kpis["awarded"] / kpis["total_offers"] * 100.0
            if kpis["total_offers"] > 0
            else 0.0
        )

        # proveedores únicos
        kpis["bidders"] = (
            int(df[prov_col].astype(str).str.strip().str.lower().nunique())
            if prov_col
            else 0
        )

        # ítems únicos
        if item_col:
            kpis["items"] = int(pd.Series(df[item_col]).dropna().nunique())
        elif desc_col:
            kpis["items"] = int(pd.Series(df[desc_col]).dropna().nunique())
        else:
            kpis["items"] = int(len(df))

        # top proveedores
        if prov_col:
            top_sup = (
                tot_series.groupby(df[prov_col])
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
            chart_sup["labels"] = list(top_sup.index)
            chart_sup["values"] = [float(x) for x in top_sup.values]

        # distribución de posiciones
        if pos_col:
            pos_counts = df[pos_col].astype(str).value_counts().head(10)
            chart_pos["labels"] = list(map(str, pos_counts.index))
            chart_pos["values"] = [int(x) for x in pos_counts.values]

        kpis["offers"] = int(len(df))
    except Exception as e:
        print("[compute_kpis_and_charts] Error:", e)
        kpis.setdefault("items", 0)
        kpis.setdefault("offers", int(len(df)))

    return kpis, chart_sup, chart_pos


def enrich_df_for_dashboard(df: pd.DataFrame):
    """
    Normaliza nombres de columnas.
    """
    if df is None or df.empty:
        return df, {}
    df_en = df.copy()
    df_en.columns = [c.strip().title() for c in df.columns]
    col_map = dict(zip(df.columns, df_en.columns))
    return df_en, col_map


def compute_extra_charts(df: pd.DataFrame, col_map: dict):
    """
    Gráficos complementarios: top proveedores ajustado, donut posiciones, donut suizo.
    """
    chart_sup_adj = {"labels": [], "values": []}
    donut_pos = {"labels": ["1", "2", "Otras"], "values": [0, 0, 0]}
    donut_suizo = {"labels": ["Suizo", "Otros"], "values": [0.0, 0.0]}

    if df is None or df.empty:
        return chart_sup_adj, donut_pos, donut_suizo

    try:
        prov_col = next(
            (
                c
                for c in df.columns
                if "proveedor" in c.lower() or "empresa" in c.lower()
            ),
            None,
        )
        total_col = next(
            (
                c
                for c in df.columns
                if "total" in c.lower() or "importe" in c.lower()
            ),
            None,
        )
        pos_col = next(
            (
                c
                for c in df.columns
                if "posición" in c.lower() or "posicion" in c.lower()
            ),
            None,
        )

        # top 10 proveedores ajustado
        if prov_col and total_col:
            top_sup = (
                df.groupby(prov_col)[total_col]
                .sum(numeric_only=True)
                .sort_values(ascending=False)
                .head(10)
            )
            chart_sup_adj["labels"] = list(top_sup.index)
            chart_sup_adj["values"] = [float(x) for x in top_sup.values]

        # donut posiciones
        if pos_col:
            pos_counts = df[pos_col].astype(str).value_counts()
            v1 = int(pos_counts.get("1", 0))
            v2 = int(pos_counts.get("2", 0))
            otras = int(int(pos_counts.sum()) - v1 - v2)
            donut_pos["values"] = [v1, v2, otras]

        # donut suizo vs otros
        if prov_col and total_col:
            df["_prov_lower"] = df[prov_col].astype(str).str.lower()
            total_suizo = (
                pd.to_numeric(
                    df.loc[
                        df["_prov_lower"].str.contains("suizo", na=False),
                        total_col,
                    ],
                    errors="coerce",
                )
                .fillna(0)
                .sum()
            )
            total_otros = (
                pd.to_numeric(df[total_col], errors="coerce")
                .fillna(0)
                .sum()
                - total_suizo
            )
            donut_suizo["values"] = [
                float(total_suizo),
                float(total_otros),
            ]
    except Exception as e:
        print("[compute_extra_charts] Error:", e)

    return chart_sup_adj, donut_pos, donut_suizo


# ======================================================================
# TABLERO (HTML)
# ======================================================================
def summarize_general(
    up: Any, df: Optional[pd.DataFrame] = None
) -> Dict[str, float | int]:
    """
    KPIs mínimos para el tablero.
    Tolera DFs con columnas distintas; si no hay DF usa hints de 'up'.
    """
    out = {"awarded": 0.0, "items": 0, "bidders": 0}

    if df is not None and not df.empty:
        item_col = _pick(
            df,
            [
                "N°",
                "Nro",
                "Item",
                "Número",
                "Numero",
                "Renglón",
                "Renglon",
            ],
        )
        desc_col = next(
            (c for c in df.columns if str(c).lower().startswith("descrip")),
            None,
        )
        if item_col:
            out["items"] = int(pd.Series(df[item_col]).dropna().nunique())
        elif desc_col:
            out["items"] = int(pd.Series(df[desc_col]).dropna().nunique())
        else:
            out["items"] = int(len(df))

        prov_col = _pick(
            df,
            [
                "Proveedor",
                "Oferente",
                "Empresa",
                "Supplier",
                "Provider",
            ],
        )
        out["bidders"] = (
            int(
                df[prov_col]
                .astype(str)
                .str.strip()
                .str.lower()
                .nunique()
            )
            if prov_col
            else 0
        )

        price_col = _pick(
            df,
            [
                "Precio unitario",
                "Precio Unitario",
                "Precio",
                "PU",
                "Unit Price",
                "Price",
            ],
        )
        qty_off_col = _pick(
            df,
            [
                "Cantidad ofertada",
                "Cant. ofertada",
                "Cantidad ofrecida",
                "Qty Offered",
            ],
        )
        qty_req_col = _pick(
            df,
            [
                "Cantidad solicitada",
                "Cant. solicitada",
                "Cantidad requerida",
                "Cantidad",
                "Cant",
                "Qty",
                "Unidades",
            ],
        )
        total_col = _pick(
            df,
            [
                "Total por renglón",
                "Total por renglon",
                "Total",
                "Importe",
                "Amount",
            ],
        )

        if price_col and (qty_off_col or qty_req_col):
            qcol = qty_off_col or qty_req_col
            tot_series = (
                pd.to_numeric(df[price_col], errors="coerce").fillna(0)
                * pd.to_numeric(df[qcol], errors="coerce").fillna(0)
            )
        elif total_col:
            tot_series = pd.to_numeric(
                df[total_col], errors="coerce"
            ).fillna(0)
        else:
            tot_series = pd.Series(
                [0] * len(df),
                index=df.index,
                dtype="float64",
            )

        pos_col = _pick(
            df,
            [
                "Posición",
                "Posicion",
                "Rank",
                "Orden",
            ],
        )
        adj_cols = [
            c
            for c in df.columns
            if ("adjud" in c.lower()) or ("ganador" in c.lower())
        ]
        if pos_col:
            winners = pd.to_numeric(df[pos_col], errors="coerce") == 1
        elif adj_cols:
            winners = df[adj_cols[0]].astype(str).str.lower().isin(
                ["si", "sí", "true", "1", "x"]
            )
        else:
            winners = pd.Series([False] * len(df), index=df.index)

        out["awarded"] = float(tot_series[winners].sum())

    # fallbacks desde el upload
    if out["items"] == 0:
        for attr in ("items", "renglones", "cant_items"):
            v = getattr(up, attr, None)
            if v:
                try:
                    out["items"] = int(v)
                    break
                except Exception:
                    pass

    if out["bidders"] == 0:
        for attr in ("bidders", "proveedores", "competidores"):
            v = getattr(up, attr, None)
            if v:
                try:
                    out["bidders"] = int(v)
                    break
                except Exception:
                    pass

    if out["awarded"] == 0:
        for attr in (
            "awarded",
            "monto_adjudicado_total",
            "presupuesto_adjudicado",
        ):
            v = getattr(up, attr, None)
            if v:
                try:
                    out["awarded"] = float(v)
                    break
                except Exception:
                    pass

    return out


def compute_suizo_effectiveness(df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """
    Efectividad Suizo.
    """
    out = {
        "monto_ofertado": 0.0,
        "monto_adjudicado": 0.0,
        "efectividad_monto": 0.0,
        "renglones_ofertados": 0,
        "renglones_adjudicados": 0,
        "efectividad_renglones": 0.0,
    }

    if df is None or df.empty:
        return out

    prov_col = _pick(
        df,
        ["Proveedor", "Oferente", "Empresa", "Supplier", "Provider"],
    )
    pos_col = _pick(df, ["Posición", "Posicion", "Rank", "Orden"])
    item_col = _pick(
        df,
        [
            "N°",
            "Nro",
            "Item",
            "Número",
            "Numero",
            "Renglón",
            "Renglon",
        ],
    )
    desc_col = next(
        (c for c in df.columns if str(c).lower().startswith("descrip")),
        None,
    )
    price_col = _pick(
        df,
        [
            "Precio unitario",
            "Precio Unitario",
            "Precio",
            "PU",
            "Unit Price",
            "Price",
        ],
    )
    qty_off_col = _pick(
        df,
        [
            "Cantidad ofertada",
            "Cant. ofertada",
            "Cantidad ofrecida",
            "Qty Offered",
        ],
    )
    qty_req_col = _pick(
        df,
        [
            "Cantidad solicitada",
            "Cant. solicitada",
            "Cantidad requerida",
            "Cantidad",
            "Cant",
            "Qty",
            "Unidades",
        ],
    )
    total_col = _pick(
        df,
        ["Total por renglón", "Total por renglon", "Total", "Importe", "Amount"],
    )

    if price_col and (qty_off_col or qty_req_col):
        qcol = qty_off_col or qty_req_col
        tot_series = (
            pd.to_numeric(df[price_col], errors="coerce").fillna(0)
            * pd.to_numeric(df[qcol], errors="coerce").fillna(0)
        )
    elif total_col:
        tot_series = (
            pd.to_numeric(df[total_col], errors="coerce").fillna(0)
        )
    else:
        tot_series = pd.Series(
            [0] * len(df),
            index=df.index,
            dtype="float64",
        )

    if prov_col is None:
        return out

    mask_suizo = df[prov_col].astype(str).str.contains(
        "suizo", case=False, na=False
    )
    adj_cols = [
        c
        for c in df.columns
        if ("adjud" in c.lower()) or ("ganador" in c.lower())
    ]
    if pos_col:
        winners = pd.to_numeric(df[pos_col], errors="coerce") == 1
    elif adj_cols:
        winners = df[adj_cols[0]].astype(str).str.lower().isin(
            ["si", "sí", "true", "1", "x"]
        )
    else:
        winners = pd.Series([False] * len(df), index=df.index)

    out["monto_ofertado"] = float(tot_series[mask_suizo].sum())
    out["monto_adjudicado"] = float(
        tot_series[mask_suizo & winners].sum()
    )
    out["efectividad_monto"] = (
        out["monto_adjudicado"] / out["monto_ofertado"]
        if out["monto_ofertado"] > 0
        else 0.0
    )

    key_col = item_col or desc_col
    if key_col:
        offered_items = (
            df.loc[mask_suizo, key_col]
            .astype(str)
            .dropna()
            .unique()
        )
        awarded_items = (
            df.loc[mask_suizo & winners, key_col]
            .astype(str)
            .dropna()
            .unique()
        )
        out["renglones_ofertados"] = int(len(offered_items))
        out["renglones_adjudicados"] = int(len(awarded_items))
        out["efectividad_renglones"] = (
            out["renglones_adjudicados"]
            / out["renglones_ofertados"]
            if out["renglones_ofertados"] > 0
            else 0.0
        )
    else:
        out["renglones_ofertados"] = int(mask_suizo.sum())
        out["renglones_adjudicados"] = int((mask_suizo & winners).sum())
        out["efectividad_renglones"] = (
            out["renglones_adjudicados"]
            / out["renglones_ofertados"]
            if out["renglones_ofertados"] > 0
            else 0.0
        )

    return out


@app.get("/tablero/{upload_id}", response_class=HTMLResponse)
def tablero_show(
    request: Request,
    upload_id: int,
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    up = db_session.get(UploadModel, upload_id)
    if not up:
        return HTMLResponse("Carga no encontrada", status_code=404)

        # 🔒 Verificar visibilidad por grupos (auditor ve todo).
    # Si el proceso es del propio usuario, SIEMPRE permitir.
    vis_ids = visible_user_ids_ext(db_session, user)
    if (up.user_id != user.id) and (up.user_id not in vis_ids):
        raise HTTPException(
            status_code=403,
            detail="No autorizado para ver este proceso.",
        )

    role = (user.role or "").lower()
    st = (up.status or "").lower()
    is_admin = role == "admin"
    is_finalized = st in ("done", "finalizado")

    norm_path = services.get_normalized_path(up)
    if not norm_path or not norm_path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    # 🔒 Regla: solo ADMIN puede ver antes de finalizado
    if not is_admin and not is_finalized:
        raise HTTPException(
            status_code=403,
            detail="Disponible cuando el admin finalice el proceso.",
        )

    processed_dir = (
        norm_path.parent
        if norm_path and norm_path.exists()
        else Path(getattr(up, "base_dir", "")) / "processed"
    )

    # intentar leer dashboard.json generado por pipeline
    summary = {}
    dash_json = processed_dir / "dashboard.json"
    if dash_json.exists():
        try:
            summary = json.loads(dash_json.read_text(encoding="utf-8"))
        except Exception:
            summary = {}

    df = _load_processed_df(up)

    if df is not None and not df.empty:
        kpis, chart_sup, chart_pos = compute_kpis_and_charts(df)
        df_en, col_map = enrich_df_for_dashboard(df)
        chart_sup_adj, donut_pos, donut_suizo = compute_extra_charts(
            df_en, col_map
        )
    else:
        # usar lo que venga de dashboard.json
        kpis = summary.get(
            "kpis",
            {
                "total_offers": 0,
                "awarded": 0,
                "pct_over_awarded": 0,
                "bidders": 0,
                "items": 0,
                "offers": 0,
            },
        )
        chart_sup = summary.get(
            "chart_suppliers",
            {"labels": [], "values": []},
        )
        chart_pos = summary.get(
            "chart_positions",
            {"labels": [], "values": []},
        )
        df_en, col_map = pd.DataFrame(), {}
        chart_sup_adj = {"labels": [], "values": []}
        donut_pos = {"labels": ["1", "2", "Otras"], "values": [0, 0, 0]}
        donut_suizo = {"labels": ["Suizo", "Otros"], "values": [0.0, 0.0]}

    lic_gen = summarize_general(
        up,
        df if df is not None and not df.empty else None,
    )
    suizo_eff = compute_suizo_effectiveness(
        df_en if df is not None and not df.empty else None
    )

    def _as_list_str(xs):
        try:
            return [str(x) for x in (xs or [])]
        except Exception:
            return []

    def _as_list_float(xs):
        out = []
        for x in (xs or []):
            try:
                out.append(float(x))
            except Exception:
                out.append(0.0)
        return out

    def _as_list_int(xs):
        out = []
        for x in (xs or []):
            try:
                out.append(int(x))
            except Exception:
                out.append(0)
        return out

    try:
        kpis = {
            "total_offers": float(kpis.get("total_offers", 0) or 0),
            "awarded": float(kpis.get("awarded", 0) or 0),
            "pct_over_awarded": float(
                kpis.get("pct_over_awarded", 0) or 0
            ),
            "bidders": int(kpis.get("bidders", 0) or 0),
            "items": int(kpis.get("items", 0) or 0),
            "offers": int(kpis.get("offers", 0) or 0),
        }
    except Exception:
        kpis = {
            "total_offers": 0,
            "awarded": 0,
            "pct_over_awarded": 0,
            "bidders": 0,
            "items": 0,
            "offers": 0,
        }

    # tabla "preview" del normalized
    if df is not None and not df.empty:
        df_tab = df.fillna("")
        table_cols = [str(c) for c in df_tab.columns]

        def _sanitize_cell(v):
            if isinstance(v, pd.Timestamp):
                return v.isoformat()
            try:
                if pd.isna(v):
                    return ""
            except Exception:
                pass
            s = str(v)
            return s.replace("<", "&lt;").replace(">", "&gt;")

        table_rows = [
            [_sanitize_cell(v) for v in row]
            for row in df_tab.itertuples(index=False, name=None)
        ]
        rows_total = int(len(df_tab))
    else:
        table_cols, table_rows, rows_total = [], [], 0

    chart_suppliers_labels = _as_list_str(chart_sup.get("labels", []))
    chart_suppliers_values = _as_list_float(chart_sup.get("values", []))
    chart_positions_labels = _as_list_str(chart_pos.get("labels", []))
    chart_positions_values = _as_list_int(chart_pos.get("values", []))

    labels_adj = _as_list_str(chart_sup_adj.get("labels", []))
    values_adj = _as_list_float(chart_sup_adj.get("values", []))

    chart_prov = [
        {"label": l, "value": v}
        for l, v in zip(labels_adj, values_adj)
    ] or [
        {
            "label": l,
            "value": v,
        }
        for l, v in zip(
            chart_suppliers_labels, chart_suppliers_values
        )
    ]

    # URL JSON que consume dashboard.html para la tabla de ranking
    rank_api_url = f"/api/tablero/{upload_id}/ranking"

    # ✅ NUEVO: URL para descargar el PDF del proceso
    pdf_url = f"/reportes/proceso/{upload_id}"

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "upload": up,
            "upload_id": upload_id,
            "kpis": kpis,
            "chart_suppliers_labels": chart_suppliers_labels,
            "chart_suppliers_values": chart_suppliers_values,
            "chart_positions_labels": chart_positions_labels,
            "chart_positions_values": chart_positions_values,
            "chart_suppliers_adj_labels": labels_adj,
            "chart_suppliers_adj_values": values_adj,
            "donut_pos_labels": _as_list_str(donut_pos.get("labels", [])),
            "donut_pos_values": _as_list_int(donut_pos.get("values", [])),
            "donut_suizo_labels": _as_list_str(
                donut_suizo.get("labels", [])
            ),
            "donut_suizo_values": _as_list_float(
                donut_suizo.get("values", [])
            ),
            "chart_prov": chart_prov,
            "lic_gen": lic_gen,
            "suizo_eff": suizo_eff,
            "table_cols": table_cols,
            "table_rows": table_rows,
            "rows_total": rows_total,
            "rank_api_url": rank_api_url,
            "pdf_url": pdf_url,  # 👈 ahora SÍ existe
        },
    )

# ======================================================================
# ADMIN: RESET DE PROCESOS / CARGAS
# ======================================================================
@app.post("/admin/reset_uploads")
def admin_reset_uploads(
    request: Request,
    password: str = Form(...),
    user: User = Depends(require_roles("admin")),
):
    """
    Elimina TODAS las cargas (UploadModel) y borra la carpeta data/uploads.
    SOLO para admin y pidiendo una contraseña especial (verify_reset_password).
    Se pensó para usarse desde un botón + modal en la interfaz.
    """
    # 1) Validar contraseña de seguridad
    if not verify_reset_password(user, password):
        # Podés leer estos flags en home.html para mostrar un toast/mensaje
        return RedirectResponse(
            "/?reset_err=bad_password",
            status_code=303,
        )

    base_dir = Path("data/uploads")

    try:
        # 2) Borrar registros de UploadModel
        db_session.query(UploadModel).delete()
        db_session.commit()

        # 3) Borrar los archivos físicos
        if base_dir.exists():
            shutil.rmtree(base_dir)
        base_dir.mkdir(parents=True, exist_ok=True)

        # 4) Volver al home con flag de OK
        return RedirectResponse(
            "/?reset_ok=1",
            status_code=303,
        )
    except Exception as e:
        db_session.rollback()
        # Limitamos el mensaje de error para que no rompa la URL
        msg = str(e).replace(" ", "_")[:180]
        return RedirectResponse(
            f"/?reset_err={msg}",
            status_code=303,
        )

# ======================================================================
# LOGIN / AUTENTICACIÓN
# ======================================================================
@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request):
    ctx = {
        "request": request,
        "error": request.query_params.get("error"),
    }
    return templates.TemplateResponse("login.html", ctx)


@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    email = (email or "").strip().lower()
    password = password or ""
    u = (
        db_session.query(User)
        .filter(func.lower(func.trim(User.email)) == email)
        .first()
    )
    if not u or not verify_password(password, u.password_hash):
        return RedirectResponse(
            "/login?error=Credenciales%20inv%C3%A1lidas",
            status_code=303,
        )

    request.session["uid"] = u.id
    request.session["role"] = (u.role or "").lower()
    request.session["name"] = user_display(u)

    return RedirectResponse("/", status_code=303)


@app.get("/logout", include_in_schema=False)
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


# ======================================================================
# MI CONTRASEÑA (USUARIO LOGUEADO)
# ======================================================================
@app.get("/mi/password", response_class=HTMLResponse)
def my_password_form(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    ctx = {
        "request": request,
        "user": user,
        "error": request.query_params.get("error") or "",
        "ok": request.query_params.get("ok") or "",
    }
    return templates.TemplateResponse("account_password.html", ctx)


@app.post("/mi/password")
def my_password_submit(
    request: Request,
    actual: str = Form(...),
    nueva: str = Form(...),
    confirmar: str = Form(...),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    if not verify_password(actual, user.password_hash):
        return RedirectResponse(
            "/mi/password?error=Contraseña%20actual%20incorrecta",
            status_code=303,
        )
    if nueva != confirmar:
        return RedirectResponse(
            "/mi/password?error=Las%20contrase%C3%B1as%20no%20coinciden",
            status_code=303,
        )
    if len(nueva) < 8:
        return RedirectResponse(
            "/mi/password?error=La%20nueva%20contrase%C3%B1a%20es%20muy%20corta",
            status_code=303,
        )
    try:
        user.password_hash = hash_password(nueva)
        db_session.add(user)
        db_session.commit()
        return RedirectResponse("/mi/password?ok=1", status_code=303)
    except Exception:
        db_session.rollback()
        return RedirectResponse(
            "/mi/password?error=No%20se%20pudo%20actualizar",
            status_code=303,
        )


# ======================================================================
# OLVIDÉ MI CONTRASEÑA
# ======================================================================
@app.get("/password/olvido", response_class=HTMLResponse)
def password_forgot_form(request: Request):
    ctx = {
        "request": request,
        "sent": request.query_params.get("sent") or "",
    }
    return templates.TemplateResponse("password_forgot.html", ctx)


@app.post("/password/olvido")
def password_forgot_submit(
    request: Request,
    email: str = Form(...),
):
    email = (email or "").strip().lower()
    u = (
        db_session.query(User)
        .filter(func.lower(func.trim(User.email)) == email)
        .first()
    )

    tok = _create_password_reset_token(email)
    if u:
        _send_password_reset_email(u, tok)

    return RedirectResponse("/password/olvido?sent=1", status_code=303)


@app.get("/password/restablecer", response_class=HTMLResponse)
def password_reset_form(
    request: Request,
    token: str = Query(""),
):
    if not token:
        return RedirectResponse("/password/olvido", status_code=303)

    email = _get_password_reset_email(token)
    if not email:
        return templates.TemplateResponse(
            "password_reset.html",
            {
                "request": request,
                "token": "",
                "error": "El enlace no es válido o ya venció.",
            },
        )

    return templates.TemplateResponse(
        "password_reset.html",
        {
            "request": request,
            "token": token,
            "error": "",
        },
    )


@app.post("/password/restablecer")
def password_reset_submit(
    request: Request,
    token: str = Form(...),
    password: str = Form(...),
    confirmar: str = Form(...),
):
    if not token:
        return RedirectResponse("/password/olvido", status_code=303)

    email = _get_password_reset_email(token)
    if not email:
        return RedirectResponse(
            "/password/restablecer?token=&error=1",
            status_code=303,
        )

    if password != confirmar:
        return RedirectResponse(
            f"/password/restablecer?token={token}&error=1",
            status_code=303,
        )

    if len(password) < 8:
        return RedirectResponse(
            f"/password/restablecer?token={token}&error=1",
            status_code=303,
        )

    u = (
        db_session.query(User)
        .filter(func.lower(func.trim(User.email)) == email.lower())
        .first()
    )
    if not u:
        _consume_password_reset_token(token)
        return RedirectResponse("/login", status_code=303)

    try:
        u.password_hash = hash_password(password)
        db_session.add(u)
        db_session.commit()
        _consume_password_reset_token(token)
        return RedirectResponse("/login", status_code=303)
    except Exception:
        db_session.rollback()
        return RedirectResponse(
            f"/password/restablecer?token={token}&error=1",
            status_code=303,
        )

# ======================================================================
# DESCARGA DEL ARCHIVO ORIGINAL SUBIDO
# ======================================================================
@app.get("/cargas/{upload_id}/original")
def descargar_archivo_original(
    upload_id: int,
    user: User = Depends(require_roles("admin")),  # solo admin revisa
):
    """
    Permite al administrador descargar el archivo ORIGINAL que subió el usuario,
    antes de procesarlo.
    """
    up = db_session.get(UploadModel, upload_id)
    if not up:
        raise HTTPException(status_code=404, detail="Carga no encontrada")

    # 🔒 Verificar visibilidad por grupos (auditor vería todo, pero acá solo admin)
    vis_ids = visible_user_ids_ext(db_session, user)
    if (up.user_id != user.id) and (up.user_id not in vis_ids):
        raise HTTPException(
            status_code=403,
            detail="No autorizado para ver este archivo.",
        )

    # Ruta física del archivo original
    orig_path = getattr(up, "original_path", None)
    if not orig_path:
        raise HTTPException(
            status_code=404,
            detail="No hay archivo original registrado para esta carga.",
        )

    p = Path(orig_path)
    if not p.exists():
        raise HTTPException(
            status_code=404,
            detail="El archivo original no se encuentra en el servidor.",
        )

    # Nombre de archivo para la descarga
    filename = p.name

    # Tipo genérico; Excel lo abre igual
    return FileResponse(
        str(p),
        filename=filename,
        media_type="application/octet-stream",
    )

# ======================================================================
# DESCARGA normalized.xlsx
# ======================================================================
@app.get("/descargas/{upload_id}")
def descargar_normalizado(
    upload_id: int,
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    up = db_session.get(UploadModel, upload_id)
    if not up:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    # 🔒 Verificar visibilidad por grupos (auditor ve todo)
    vis_ids = visible_user_ids_ext(db_session, user)
    if up.user_id not in vis_ids:
        raise HTTPException(
            status_code=403,
            detail="No autorizado para descargar este archivo.",
        )

    role = (user.role or "").lower()
    st = (up.status or "").lower()
    is_admin = role == "admin"
    is_finalized = st in ("done", "finalizado")

    if not is_admin and not is_finalized:
        raise HTTPException(
            status_code=403,
            detail="Disponible cuando el admin finalice el proceso.",
        )

    norm_path = services.get_normalized_path(up)
    if not norm_path or not norm_path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(
        str(norm_path),
        filename=f"normalized_{upload_id}.xlsx",
        media_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
    )


def _fmt_fecha(value):
    """
    Devuelve la fecha en formato dd/mm/YYYY sin explotar si viene como str.
    Acepta: None, str, datetime.date, datetime.datetime
    """
    if not value:
        return ""
    if isinstance(value, (dt.datetime, dt.date)):
        return value.strftime("%d/%m/%Y")
    # si ya es un string (por ej. "2025-11-01" o "01/11/2025"), lo devolvemos así
    return str(value)


@app.get("/reportes/proceso/{upload_id}")
def descargar_reporte_proceso(
    upload_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "analista", "auditor", "supervisor")),
):
    # 1) Traer el proceso / upload (usar UploadModel, no Upload)
    up = (
        db.query(UploadModel)
        .filter(UploadModel.id == upload_id)
        .first()
    )
    if not up:
        raise HTTPException(status_code=404, detail="Proceso no encontrado")

    # 2) 🔒 Verificar visibilidad por grupos (auditor ve todo).
    #    Si el proceso es del propio usuario, SIEMPRE permitir.
    vis_ids = visible_user_ids_ext(db_session, user)
    if (up.user_id != user.id) and (up.user_id not in vis_ids):
        raise HTTPException(
            status_code=403,
            detail="No autorizado para ver este proceso.",
        )

    # 3) 🔒 Regla: solo ADMIN puede ver antes de finalizado
    role = (user.role or "").lower()
    is_admin = role == "admin"
    st = (up.status or "").strip().lower()
    is_finalized = st in ("done", "finalizado", "dashboard", "tablero")
    if not is_admin and not is_finalized:
        raise HTTPException(
            status_code=403,
            detail="Disponible cuando el admin finalice el proceso.",
        )

    # 4) Normalizar fechas del modelo (acepta str/date/datetime)
    apertura_raw = (
        getattr(up, "apertura_fecha", None)
        or getattr(up, "apertura", None)
        or getattr(up, "fecha_apertura", None)
    )
    apertura_str = _fmt_fecha(apertura_raw)

    # 5) Otros campos que solemos tener
    proceso_nro = (
        getattr(up, "proceso_nro", None)
        or getattr(up, "nro_proceso", None)
        or getattr(up, "proceso", None)
        or ""
    )
    comprador = (
        getattr(up, "comprador", None)
        or getattr(up, "reparticion", None)
        or getattr(up, "entidad", None)
        or getattr(up, "buyer_hint", None)
        or ""
    )
    cuenta = (
        getattr(up, "cuenta", None)
        or getattr(up, "n_cuenta", None)
        or getattr(up, "cuenta_nro", None)
        or ""
    )
    plataforma = (
        getattr(up, "plataforma", None)
        or getattr(up, "origen", None)
        or getattr(up, "portal", None)
        or getattr(up, "platform_hint", None)
        or ""
    )
    provincia = (
        getattr(up, "provincia", None)
        or getattr(up, "municipio", None)
        or getattr(up, "jurisdiccion", None)
        or getattr(up, "province_hint", None)
        or ""
    )

    # 6) KPIs (si los guardaste como JSON en DB)
    kpis = {}
    if getattr(up, "kpis_json", None):
        try:
            kpis = json.loads(up.kpis_json)
        except Exception:
            kpis = {}
    elif getattr(up, "kpis", None) and isinstance(up.kpis, dict):
        kpis = up.kpis

    # 7) Posiciones (si están serializadas en el modelo)
    posiciones = []
    if hasattr(up, "posiciones_json") and up.posiciones_json:
        try:
            posiciones = json.loads(up.posiciones_json)
        except Exception:
            posiciones = []

    # 8) Payload para el generador de PDF
    data_pdf = {
        "proceso_nro": proceso_nro,
        "comprador": comprador,
        "apertura": apertura_str,
        "cuenta": cuenta,
        "plataforma": plataforma,
        "provincia": provincia,
        "kpis": kpis,
        "posiciones": posiciones,
        "suizo": {
            "monto_ofertado": getattr(up, "monto_ofertado_suizo", ""),
            "monto_adjudicado": getattr(up, "monto_adjudicado_suizo", ""),
            "efectividad_por_monto": getattr(up, "efectividad_monto_suizo", ""),
            "renglones_ofertados": getattr(up, "renglones_ofertados_suizo", ""),
            "renglones_adjudicados": getattr(up, "renglones_adjudicados_suizo", ""),
            "efectividad_por_renglones": getattr(up, "efectividad_renglones_suizo", ""),
        },
    }

    # 9) Generar el PDF en memoria
    pdf_bytes = render_informe_comparativas(data_pdf)

    # 10) Devolverlo como descarga
    filename = f"informe_proceso_{upload_id}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )


# === Informe PDF ============================================================
@app.get("/informes/{upload_id}/pdf", name="informe_pdf")
def informe_pdf(
    upload_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    up = db.query(UploadModel).filter(UploadModel.id == upload_id).first()
    if not up:
        raise HTTPException(status_code=404, detail="Proceso no encontrado")

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=(595, 842))  # A4

    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, 800, "Informe de licitación")
    c.setFont("Helvetica", 11)
    c.drawString(
        40,
        780,
        f"Proceso: {up.proceso_nro or up.proceso_key or upload_id}",
    )
    c.drawString(40, 765, f"Plataforma: {up.platform_hint or '-'}")
    c.drawString(40, 750, f"Comprador: {up.buyer_hint or '-'}")
    c.drawString(40, 735, f"Provincia/Municipio: {up.province_hint or '-'}")
    c.drawString(40, 710, f"Generado por: {user_display(user)}")
    c.drawString(
        40,
        695,
        f"Fecha: {dt.datetime.now().strftime('%d/%m/%Y %H:%M')}",
    )
    c.showPage()
    c.save()

    buffer.seek(0)
    filename = f"informe_{upload_id}.pdf"
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{filename}"'
        },
    )


# ======================================================================
# USUARIOS
# ======================================================================
def normalize_unit_business(val: str) -> str:
    """
    Devuelve un valor válido dentro de BUSINESS_UNITS.
    """
    if not val:
        return "Otros"
    v = str(val).strip()
    alias = {
        "Hospitalario Publico": "Productos Hospitalarios",
        "Hospitalario Público": "Productos Hospitalarios",
        "Hospitalario Privado": "Productos Hospitalarios",
    }
    v = alias.get(v, v)
    return v if v in BUSINESS_UNITS else "Otros"


@app.get("/usuarios", response_class=HTMLResponse)
def usuarios_list_view(
    request: Request,
    user: User = Depends(require_roles("admin")),
):
    error = None
    users = []
    try:
        q = db_session.query(User)
        if hasattr(User, "created_at"):
            q = q.order_by(User.created_at.desc(), User.email.asc())
        else:
            q = q.order_by(User.email.asc())
        users = q.all()
    except Exception as e:
        db_session.rollback()
        error = str(e)

    ok = request.query_params.get("ok")
    err = request.query_params.get("err")

    return templates.TemplateResponse(
        "users_list.html",
        {
            "request": request,
            "user": user,
            "users": users,
            "error": error,
            "ok": ok,
            "err": err,
        },
    )


@app.get("/usuarios/nuevo", response_class=HTMLResponse)
def usuarios_nuevo_view(
    request: Request,
    user: User = Depends(require_roles("admin")),
):
    business_units = list(BUSINESS_UNITS)
    form = SimpleNamespace(
        id=None,
        email="",
        name="",
        full_name="",
        role="analista",
        unit_business="Otros",
    )
    return templates.TemplateResponse(
        "users_form.html",
        {
            "request": request,
            "user": user,
            "form": form,
            "is_new": True,
            "allow_edit_email": True,
            "business_units": business_units,
        },
    )


@app.post("/usuarios/nuevo/actualizar")
def usuarios_crear(
    request: Request,
    email: str = Form(...),
    name: str = Form(""),
    role: str = Form("analista"),
    password: str = Form("TuClaveFuerte123"),
    unit_business: str = Form("Otros"),
    user: User = Depends(require_roles("admin")),
):
    email = (email or "").strip().lower()
    role = (role or "").strip().lower()
    unit_ok = normalize_unit_business(unit_business)

    if not email:
        return RedirectResponse("/usuarios?err=email_vacio", status_code=303)

    try:
        exists = (
            db_session.query(User)
            .filter(func.lower(User.email) == email)
            .first()
        )
        if exists:
            return RedirectResponse("/usuarios?err=email_existe", status_code=303)

        u = User(
            email=email,
            name=(name or "").strip() or email.split("@")[0],
            role=role,
            password_hash=hash_password(password),
            created_at=dt.datetime.utcnow(),
            unit_business=unit_ok,
        )
        db_session.add(u)
        db_session.commit()
        return RedirectResponse("/usuarios?ok=created", status_code=303)
    except Exception as e:
        db_session.rollback()
        return RedirectResponse(f"/usuarios?err={str(e)}", status_code=303)


@app.get("/usuarios/{user_id}/editar", response_class=HTMLResponse)
def usuarios_editar_view(
    request: Request,
    user_id: int,
    user: User = Depends(require_roles("admin")),
):
    u = db_session.get(User, user_id)
    if not u:
        return RedirectResponse("/usuarios?err=not_found", status_code=303)

    business_units = list(BUSINESS_UNITS)
    if not hasattr(u, "unit_business") or u.unit_business is None:
        try:
            u.unit_business = "Otros"
            db_session.commit()
        except Exception:
            db_session.rollback()

    return templates.TemplateResponse(
        "users_form.html",
        {
            "request": request,
            "user": user,
            "form": u,
            "is_new": False,
            "allow_edit_email": False,
            "business_units": business_units,
        },
    )


@app.post("/usuarios/{user_id}/actualizar")
def usuarios_actualizar(
    request: Request,
    user_id: int,
    name: str = Form(""),
    role: str = Form("analista"),
    unit_business: str = Form("Otros"),
    user: User = Depends(require_roles("admin")),
):
    u = db_session.get(User, user_id)
    if not u:
        return RedirectResponse("/usuarios?err=not_found", status_code=303)

    unit_ok = normalize_unit_business(unit_business)

    try:
        u.name = (name or "").strip()
        u.role = (role or "").strip().lower()
        if hasattr(u, "unit_business"):
            u.unit_business = unit_ok
        db_session.commit()
        return RedirectResponse("/usuarios?ok=updated", status_code=303)
    except Exception as e:
        db_session.rollback()
        return RedirectResponse(f"/usuarios?err={str(e)}", status_code=303)


@app.post("/usuarios/{user_id}/password")
def usuarios_cambiar_password(
    request: Request,
    user_id: int,
    nueva: str = Form(...),
    confirmar: str = Form(...),
    user: User = Depends(require_roles("admin")),
):
    if nueva != confirmar:
        return RedirectResponse(
            f"/usuarios/{user_id}/editar?perror=nomatch",
            status_code=303,
        )
    if len(nueva) < 8:
        return RedirectResponse(
            f"/usuarios/{user_id}/editar?perror=short",
            status_code=303,
        )
    try:
        u = db_session.get(User, user_id)
        if not u:
            return RedirectResponse("/usuarios?err=not_found", status_code=303)
        u.password_hash = hash_password(nueva)
        db_session.commit()
        return RedirectResponse(
            f"/usuarios/{user_id}/editar?pok=1",
            status_code=303,
        )
    except Exception:
        db_session.rollback()
        return RedirectResponse(
            f"/usuarios/{user_id}/editar?perror=fail",
            status_code=303,
        )


@app.get("/usuarios/{user_id}/borrar")
def usuarios_borrar(
    request: Request,
    user_id: int,
    user: User = Depends(require_roles("admin")),
):
    u = db_session.get(User, user_id)
    if not u:
        return RedirectResponse("/usuarios?err=not_found", status_code=303)

    if u.id == user.id:
        return RedirectResponse("/usuarios?err=cannot_self", status_code=303)

    admins = db_session.query(User).filter(User.role == "admin").count()
    if u.role == "admin" and admins <= 1:
        return RedirectResponse("/usuarios?err=last_admin", status_code=303)

    try:
        db_session.delete(u)
        db_session.commit()
        return RedirectResponse("/usuarios?ok=deleted", status_code=303)
    except Exception:
        db_session.rollback()
        return RedirectResponse("/usuarios?err=delete_failed", status_code=303)


# ======================================================================
# GRUPOS
# ======================================================================
GROUP_ROLE_CHOICES = ["coordinador", "supervisor", "analista", "visor"]
GROUP_ROLE_LABELS = {
    "coordinador": "Coordinador",
    "supervisor": "Supervisor",
    "analista": "Analista",
    "visor": "Visor",
}


def _norm_group_role(x: str) -> str:
    r = (x or "").strip().lower()
    return r if r in GROUP_ROLE_CHOICES else "analista"


def _get_group_or_404(group_id: int) -> Group:
    g = db_session.get(Group, group_id)
    if not g:
        raise HTTPException(status_code=404, detail="Grupo no encontrado")
    return g


def _my_membership(user: User, group: Group):
    return (
        db_session.query(GroupMember)
        .filter(
            GroupMember.group_id == group.id,
            GroupMember.user_id == user.id,
        )
        .first()
    )


def _can_manage_group(user: User, group: Group) -> bool:
    role = (user.role or "").lower()
    if role == "admin":
        return True
    if role == "supervisor":
        m = _my_membership(user, group)
        return bool(
            m and m.role_in_group in ("owner", "coordinador", "supervisor")
        )
    return False


def _allowed_users_for_group(user: User, group: Group):
    q = db_session.query(User).order_by(User.email.asc())
    actor_role = (user.role or "").lower()
    if actor_role == "admin":
        pass
    else:
        bu_ref = user.unit_business
        if bu_ref:
            q = q.filter(User.unit_business == bu_ref)
        q = q.filter(func.lower(func.trim(User.role)) != "admin")

    in_group_ids = [m.user_id for m in group.memberships]
    if in_group_ids:
        q = q.filter(~User.id.in_(in_group_ids))
    return q.all()


@app.get("/grupos", response_class=HTMLResponse)
def grupos_list_view(
    request: Request,
    user: User = Depends(require_roles("admin", "supervisor")),
):
    role = (user.role or "").lower()
    if role == "admin":
        groups = (
            db_session.query(Group)
            .order_by(Group.created_at.desc())
            .all()
        )
    else:
        groups = (
            db_session.query(Group)
            .join(GroupMember, GroupMember.group_id == Group.id)
            .filter(GroupMember.user_id == user.id)
            .order_by(Group.created_at.desc())
            .all()
        )

    ok = request.query_params.get("ok")
    err = request.query_params.get("err")

    return templates.TemplateResponse(
        "groups_list.html",
        {
            "request": request,
            "user": user,
            "groups": groups,
            "ok": ok,
            "err": err,
        },
    )


@app.get("/grupos/nuevo", response_class=HTMLResponse)
def grupos_new_view(
    request: Request,
    user: User = Depends(require_roles("admin", "supervisor")),
):
    role = (user.role or "").lower()
    is_admin = role == "admin"
    business_units = list(BUSINESS_UNITS) if is_admin else [
        user.unit_business or "Otros"
    ]
    return templates.TemplateResponse(
        "group_form.html",
        {
            "request": request,
            "user": user,
            "business_units": business_units,
            "is_admin": is_admin,
            "default_bu": (user.unit_business or "Otros"),
        },
    )


@app.post("/grupos/nuevo")
def grupos_create(
    request: Request,
    name: str = Form(...),
    business_unit: str = Form(""),
    user: User = Depends(require_roles("admin", "supervisor")),
):
    name = (name or "").strip()
    if not name:
        return RedirectResponse("/grupos?err=nombre_vacio", status_code=303)

    role = (user.role or "").lower()
    is_admin = role == "admin"
    if is_admin:
        bu = (business_unit or "").strip() or None
        if bu and bu not in BUSINESS_UNITS:
            return RedirectResponse("/grupos?err=bu_invalida", status_code=303)
    else:
        bu = (user.unit_business or "Otros")

    exists = (
        db_session.query(Group)
        .filter(func.lower(func.trim(Group.name)) == name.lower())
        .first()
    )
    if exists:
        return RedirectResponse(
            "/grupos?err=nombre_ya_existe",
            status_code=303,
        )

    try:
        g = Group(
            name=name,
            business_unit=bu,
            created_by_user_id=user.id,
            created_at=dt.datetime.utcnow(),
        )
        db_session.add(g)
        db_session.flush()

        gm = GroupMember(
            group_id=g.id,
            user_id=user.id,
            role_in_group="owner",
            added_by_user_id=user.id,
        )
        db_session.add(gm)

        db_session.commit()
        return RedirectResponse("/grupos?ok=created", status_code=303)
    except Exception as e:
        db_session.rollback()
        return RedirectResponse(f"/grupos?err={str(e)}", status_code=303)


@app.get("/grupos/{group_id}/editar", response_class=HTMLResponse)
def grupos_edit_view(
    request: Request,
    group_id: int,
    user: User = Depends(require_roles("admin", "supervisor")),
):
    g = _get_group_or_404(group_id)
    if not _can_manage_group(user, g):
        raise HTTPException(status_code=403, detail="No autorizado")

    available_users = _allowed_users_for_group(user, g)
    ok = request.query_params.get("ok")
    err = request.query_params.get("err")

    return templates.TemplateResponse(
        "group_edit.html",
        {
            "request": request,
            "user": user,
            "group": g,
            "memberships": g.memberships,
            "available_users": available_users,
            "roles": GROUP_ROLE_CHOICES,
            "role_labels": GROUP_ROLE_LABELS,
            "business_units": list(BUSINESS_UNITS),
            "ok": ok,
            "err": err,
        },
    )


@app.post("/grupos/{group_id}/actualizar")
def grupos_update(
    request: Request,
    group_id: int,
    name: str = Form(...),
    business_unit: str = Form(""),
    user: User = Depends(require_roles("admin", "supervisor")),
):
    g = _get_group_or_404(group_id)
    if not _can_manage_group(user, g):
        raise HTTPException(status_code=403, detail="No autorizado")

    if (user.role or "").lower() == "admin":
        bu = (business_unit or "").strip() or None
        if bu and bu not in BUSINESS_UNITS:
            return RedirectResponse(
                f"/grupos/{g.id}/editar?err=BU%20inv%C3%A1lida",
                status_code=303,
            )
        g.business_unit = bu

    g.name = (name or "").strip()
    if not g.name:
        return RedirectResponse(
            f"/grupos/{g.id}/editar?err=Nombre%20vac%C3%ío",
            status_code=303,
        )

    exists = (
        db_session.query(Group)
        .filter(
            func.lower(func.trim(Group.name)) == g.name.lower(),
            Group.id != g.id,
        )
        .first()
    )
    if exists:
        return RedirectResponse(
            f"/grupos/{g.id}/editar?err=Ya%20existe%20un%20grupo%20con%20ese%20nombre",
            status_code=303,
        )

    db_session.commit()
    return RedirectResponse(
        f"/grupos/{g.id}/editar?ok=updated", status_code=303
    )


@app.post("/grupos/{group_id}/miembros/agregar")
def grupos_members_add(
    request: Request,
    group_id: int,
    user_ids: List[int] = Form([]),
    role_in_group: str = Form("analista"),
    user: User = Depends(require_roles("admin", "supervisor")),
):
    g = _get_group_or_404(group_id)
    if not _can_manage_group(user, g):
        raise HTTPException(status_code=403, detail="No autorizado")

    role_in_group = _norm_group_role(role_in_group)
    allowed = {u.id for u in _allowed_users_for_group(user, g)}
    added = 0
    try:
        for uid in (user_ids or []):
            if uid not in allowed:
                continue
            exists = (
                db_session.query(GroupMember)
                .filter(
                    GroupMember.group_id == g.id,
                    GroupMember.user_id == uid,
                )
                .first()
            )
            if exists:
                continue
            db_session.add(
                GroupMember(
                    group_id=g.id,
                    user_id=uid,
                    role_in_group=role_in_group,
                    added_by_user_id=user.id,
                )
            )
            added += 1
        db_session.commit()
        db_session.expire_all()
    except Exception as e:
        db_session.rollback()
        return RedirectResponse(
            f"/grupos/{g.id}/editar?err={str(e)}",
            status_code=303,
        )

    return RedirectResponse(
        f"/grupos/{g.id}/editar?ok=added={added}", status_code=303
    )


@app.post("/grupos/{group_id}/miembros/{member_id}/rol")
def grupos_member_update_role(
    request: Request,
    group_id: int,
    member_id: int,
    role_in_group: str = Form(...),
    user: User = Depends(require_roles("admin", "supervisor")),
):
    g = _get_group_or_404(group_id)
    if not _can_manage_group(user, g):
        raise HTTPException(status_code=403, detail="No autorizado")

    m = db_session.get(GroupMember, member_id)
    if not m or m.group_id != g.id:
        return RedirectResponse(
            f"/grupos/{g.id}/editar?err=Miembro%20no%20encontrado",
            status_code=303,
        )

    try:
        m.role_in_group = _norm_group_role(role_in_group)
        db_session.add(m)
        db_session.commit()
        db_session.expire_all()
    except Exception as e:
        db_session.rollback()
        return RedirectResponse(
            f"/grupos/{g.id}/editar?err={str(e)}",
            status_code=303,
        )

    return RedirectResponse(
        f"/grupos/{g.id}/editar?ok=role_updated", status_code=303
    )


@app.get("/grupos/{group_id}/miembros/{member_id}/eliminar")
def grupos_member_remove(
    request: Request,
    group_id: int,
    member_id: int,
    user: User = Depends(require_roles("admin", "supervisor")),
):
    g = _get_group_or_404(group_id)
    if not _can_manage_group(user, g):
        raise HTTPException(status_code=403, detail="No autorizado")

    m = db_session.get(GroupMember, member_id)
    if not m or m.group_id != g.id:
        return RedirectResponse(
            f"/grupos/{g.id}/editar?err=Miembro%20no%20encontrado",
            status_code=303,
        )

    # evitar dejar sin owner
    if m.role_in_group == "owner":
        owners = [x for x in g.memberships if x.role_in_group == "owner"]
        if len(owners) <= 1:
            return RedirectResponse(
                f"/grupos/{g.id}/editar?err=No%20puede%20quedar%20sin%20propietario",
                status_code=303,
            )

    try:
        db_session.delete(m)
        db_session.commit()
        db_session.expire_all()
    except Exception as e:
        db_session.rollback()
        return RedirectResponse(
            f"/grupos/{g.id}/editar?err={str(e)}",
            status_code=303,
        )

    return RedirectResponse(
        f"/grupos/{g.id}/editar?ok=member_removed", status_code=303
    )


@app.get("/grupos/{group_id}/eliminar")
def grupos_delete(
    request: Request,
    group_id: int,
    user: User = Depends(require_roles("admin", "supervisor")),
):
    g = _get_group_or_404(group_id)

    if (user.role or "").lower() == "supervisor":
        if not _can_manage_group(user, g):
            raise HTTPException(status_code=403, detail="No autorizado")
        m = _my_membership(user, g)
        if not m or m.role_in_group not in (
            "owner",
            "coordinador",
            "supervisor",
        ):
            raise HTTPException(
                status_code=403,
                detail="Se requieren permisos elevados en el grupo",
            )

    db_session.query(GroupMember).filter(
        GroupMember.group_id == g.id
    ).delete()
    db_session.delete(g)
    db_session.commit()
    return RedirectResponse("/grupos?ok=deleted", status_code=303)


# ======================================================================
# PANEL DE PRESERVACIÓN DE FILTROS / PRESETS
# ======================================================================
def _presets_dir() -> Path:
    p = BASE_DIR / "data" / "presets"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _preset_file_for(user_id: int) -> Path:
    return _presets_dir() / f"{int(user_id)}.json"


def _load_user_presets(user_id: int) -> List[Dict[str, Any]]:
    f = _preset_file_for(user_id)
    if not f.exists():
        return []
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_user_presets(user_id: int, presets: List[Dict[str, Any]]):
    f = _preset_file_for(user_id)
    f.write_text(
        json.dumps(presets, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _sanitize_filters_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "q",
        "status",
        "created_from",
        "created_to",
        "uploader_id",
        "proceso",
        "cuenta",
        "platform",
        "buyer",
        "province",
        "filename",
        "page",
        "page_size",
    }
    out = {}
    for k, v in (d or {}).items():
        if k in allowed:
            out[k] = "" if v is None else str(v)
    return out


@app.get("/api/presets", response_class=JSONResponse)
def api_presets_list(
    view: str = Query("historial"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    items = [
        p for p in _load_user_presets(user.id) if (p.get("view") == view)
    ]
    items.sort(key=lambda x: x.get("updated_at") or "", reverse=True)
    return {"ok": True, "items": items}


@app.post("/api/presets", response_class=JSONResponse)
async def api_presets_save(
    request: Request,
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
    name: str = Form(...),
    view: str = Form("historial"),
    filters_json: str = Form("{}"),
    preset_id: str = Form(
        "",
        description="Si viene, actualiza ese preset",
    ),
):
    try:
        filters = json.loads(filters_json) if filters_json else {}
    except Exception:
        filters = {}

    filters = _sanitize_filters_dict(filters)
    presets = _load_user_presets(user.id)
    now = dt.datetime.utcnow().isoformat()

    if preset_id:
        found = False
        for p in presets:
            if p.get("id") == preset_id and p.get("view") == view:
                p["name"] = name.strip() or "Sin título"
                p["filters"] = filters
                p["updated_at"] = now
                found = True
                break
        if not found:
            return JSONResponse(
                {"ok": False, "error": "not_found"},
                status_code=404,
            )
    else:
        presets.append(
            {
                "id": str(uuid.uuid4()),
                "view": view,
                "name": name.strip() or "Sin título",
                "filters": filters,
                "created_at": now,
                "updated_at": now,
            }
        )

    _save_user_presets(user.id, presets)
    return {
        "ok": True,
        "items": [p for p in presets if p.get("view") == view],
    }


@app.delete("/api/presets/{pid}", response_class=JSONResponse)
def api_presets_delete(
    pid: str,
    view: str = Query("historial"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    presets = _load_user_presets(user.id)
    new_list = [
        p
        for p in presets
        if not (p.get("id") == pid and p.get("view") == view)
    ]
    if len(new_list) == len(presets):
        return JSONResponse(
            {"ok": False, "error": "not_found"},
            status_code=404,
        )
    _save_user_presets(user.id, new_list)
    return {"ok": True}


@app.get("/api/presets/{pid}/apply")
def api_presets_apply_redirect(
    pid: str,
    view: str = Query("historial"),
    user: User = Depends(
        require_roles("admin", "analista", "auditor", "supervisor")
    ),
):
    presets = _load_user_presets(user.id)
    p = next(
        (x for x in presets if x.get("id") == pid and x.get("view") == view),
        None,
    )
    if not p:
        raise HTTPException(status_code=404, detail="Preset no encontrado")

    base = "/cargas/historial" if view == "historial" else "/"
    flt = p.get("filters") or {}
    ordered_keys = [
        "q",
        "status",
        "created_from",
        "created_to",
        "uploader_id",
        "proceso",
        "cuenta",
        "platform",
        "buyer",
        "province",
        "filename",
        "page",
        "page_size",
    ]
    parts = []
    for k in ordered_keys:
        v = flt.get(k)
        if v not in (None, ""):
            parts.append(f"{k}={str(v)}")
    qs = ("?" + "&".join(parts)) if parts else ""
    return RedirectResponse(f"{base}{qs}", status_code=303)
