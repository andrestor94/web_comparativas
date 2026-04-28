"""
Router: Lectura de Pliegos
Reemplaza completamente el módulo anterior "Lectura de Pliego IA".
Flujo semimanual: usuario carga archivos → admin gestiona → GPT externo → Excel → SIEM muestra.
"""
from __future__ import annotations

import io
import uuid
import datetime as dt
import traceback
import unicodedata
import re as _re
from pathlib import Path
from typing import List

import pandas as pd
from fastapi import APIRouter, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from web_comparativas.models import (
    User,
    PliegoSolicitud,
    PliegoArchivo,
    PliegoHistorial,
    PliegoExcelCarga,
    PliegoProceso,
    PliegoCronograma,
    PliegoRequisito,
    PliegoGarantia,
    PliegoRenglon,
    PliegoDocumento,
    PliegoActoAdmin,
    PliegoHallazgo,
    PliegoFaltante,
    PliegoTrazabilidad,
    PliegoFusionCabecera,
    PliegoFusionRenglon,
    PliegoAnalitica,
    PliegoControlCarga,
    PLIEGO_ESTADOS,
    PLIEGO_ESTADO_LABELS,
)
from web_comparativas.pliegos_rp import (
    build_rp_output,
    build_canonical_output,
)
from web_comparativas.pliegos_summary import build_debug_matrix, build_resumen_licitacion
from web_comparativas.pliegos_fusion import (
    calcular_estado_fusion,
    export_fusion_excel_bytes,
    FUSION_CAMPOS_OBLIGATORIOS,
    FUSION_CAMPOS_COMPLEMENTARIOS,
)

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

UPLOAD_DIR = BASE_DIR / "static" / "uploads" / "pliegos_solicitudes"
EXCEL_DIR = BASE_DIR / "static" / "uploads" / "pliegos_excel"

# Extensiones permitidas para archivos del usuario
ALLOWED_USER_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".zip"}

# Hojas mínimas requeridas en el Excel del GPT
REQUIRED_SHEETS = [
    "Proceso",
    "Cronograma",
    "Requisitos",
    "Garantias",
    "Renglones",
    "Documentos",
    "Actos_Administrativos",
    "Hallazgos_Extra",
    "Faltantes_y_Dudas",
    "Trazabilidad",
]

# Hojas opcionales en el Excel del GPT (soporte dual v2)
OPTIONAL_SHEETS = [
    "Fusion_Cabecera",
    "Fusion_Renglones",
    "SIEM_Analitica",
    "Control_Carga",
]

# Estados que habilitan la carga de Excel
ESTADOS_CARGA_EXCEL = {"pendiente_revision", "en_revision", "pendiente_procesamiento", "procesado_externamente", "reprocesar"}

# Transiciones de estado permitidas por rol
TRANSICIONES_ADMIN = {
    "borrador":                ["pendiente_revision", "rechazado", "archivado"],
    "pendiente_revision":      ["en_revision", "observado", "rechazado", "archivado"],
    "en_revision":             ["pendiente_procesamiento", "observado", "rechazado"],
    "pendiente_procesamiento": ["procesado_externamente", "en_revision", "reprocesar"],
    "procesado_externamente":  ["excel_cargado", "reprocesar", "en_revision"],
    "excel_cargado":           ["en_validacion", "reprocesar"],
    "en_validacion":           ["listo", "reprocesar", "observado"],
    "listo":                   ["archivado", "reprocesar"],
    "observado":               ["en_revision", "pendiente_revision", "archivado"],
    "rechazado":               ["archivado", "pendiente_revision"],
    "reprocesar":              ["en_revision", "pendiente_procesamiento"],
    "archivado":               ["pendiente_revision"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_user(request: Request):
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse("/login", 303)
    return None


def _require_admin(request: Request):
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse("/login", 303)
    if (user.role or "").lower() != "admin":
        return RedirectResponse("/mercado-publico/lectura-pliegos", 303)
    return None


def _is_admin(request: Request) -> bool:
    user = getattr(request.state, "user", None)
    return bool(user and (user.role or "").lower() == "admin")


def _registrar_historial(db: Session, solicitud_id: int, estado_anterior: str,
                         estado_nuevo: str, comentario: str, usuario_id: int):
    h = PliegoHistorial(
        solicitud_id=solicitud_id,
        estado_anterior=estado_anterior,
        estado_nuevo=estado_nuevo,
        comentario=comentario,
        usuario_id=usuario_id,
    )
    db.add(h)


def _cambiar_estado(db: Session, caso: PliegoSolicitud, nuevo_estado: str,
                    comentario: str, usuario_id: int):
    _registrar_historial(db, caso.id, caso.estado, nuevo_estado, comentario, usuario_id)
    caso.estado = nuevo_estado
    caso.actualizado_en = dt.datetime.now(dt.timezone.utc)


def _safe_str(val) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("nan", "none", "") else s


def _slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", _safe_str(value)).encode("ascii", "ignore").decode("ascii")
    return _re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()


def _buscar_proceso(datos: dict, *claves) -> str:
    """Busca un valor en el dict de proceso con múltiples claves candidatas (case-insensitive)."""
    if not datos:
        return ""
    idx = {k.lower().strip(): v for k, v in datos.items()}
    for clave in claves:
        val = idx.get(clave.lower().strip(), "")
        if val and str(val).strip() and str(val).strip().lower() not in ("nan", "none"):
            return str(val).strip()
    return ""


def _buscar_trazabilidad(trazabilidad, *campos) -> str:
    """Busca valor en registros de trazabilidad para un campo dado (coincidencia parcial)."""
    campos_lower = [c.lower().strip() for c in campos]
    for t in trazabilidad:
        campo_t = (t.campo or "").lower().strip()
        if any(c in campo_t or campo_t in c for c in campos_lower):
            val = (t.valor_extraido or "").strip()
            if val and val.lower() not in ("nan", "none", "—", "-"):
                return val
    return ""


def _completitud(caso: PliegoSolicitud) -> int:
    """Calcula nivel de completitud real 0-100, penalizando faltantes críticos."""
    puntaje = 0
    if caso.datos_proceso and caso.datos_proceso.datos:
        puntaje += 30
    if caso.cronograma:
        puntaje += 10
    if caso.requisitos:
        puntaje += 15
    if caso.garantias:
        puntaje += 10
    if caso.renglones:
        puntaje += 20
    if caso.documentos_pliego:
        puntaje += 5
    if caso.trazabilidad:
        puntaje += 10
    # Penalizar por faltantes críticos: nunca mostrar 100% si hay críticos
    _CRIT_VALS = {"alta", "crítica", "critica", "alto", "high"}
    criticos = [f for f in caso.faltantes if (f.criticidad or "").lower() in _CRIT_VALS]
    if criticos:
        puntaje = min(puntaje, 85)
        penalizacion = min(len(criticos) * 3, 20)
        puntaje = max(puntaje - penalizacion, 30)
    return puntaje


def _build_resumen_licitacion(caso: PliegoSolicitud) -> dict:
    """
    Construye un dict normalizado de resumen del proceso licitatorio.
    Busca datos en múltiples hojas con claves flexibles (case-insensitive).
    """
    datos = caso.datos_proceso.datos if caso.datos_proceso else {}
    traz = caso.trazabilidad or []
    _CRIT_VALS = {"alta", "crítica", "critica", "alto", "high"}

    def _p(*claves):
        return _buscar_proceso(datos, *claves)

    def _t(*campos):
        return _buscar_trazabilidad(traz, *campos)

    def _pt(*claves_proceso, campos_trazabilidad=None):
        val = _p(*claves_proceso)
        if not val and campos_trazabilidad:
            val = _t(*campos_trazabilidad)
        return val

    criticos_list = [f for f in caso.faltantes if (f.criticidad or "").lower() in _CRIT_VALS]

    resumen = {
        # Identidad
        "titulo": caso.titulo or "",
        "nombre_proceso": _p("nombre del proceso", "nombre proceso", "denominación", "denominacion") or caso.nombre_licitacion or caso.titulo or "",
        "numero_proceso": _pt("número de proceso", "numero de proceso", "nro proceso", "número proceso", "nro. proceso", campos_trazabilidad=["numero_proceso", "nro_proceso"]) or caso.numero_proceso or "",
        "expediente": _pt("expediente", "nro expediente", "número expediente", campos_trazabilidad=["expediente"]) or caso.expediente or "",
        "organismo": _p("organismo contratante", "organismo", "entidad contratante", "repartición") or caso.organismo or "",
        "tipo_proceso": _p("tipo de proceso", "tipo proceso", "tipo de licitación", "tipo licitacion", "tipo de compra"),
        "objeto": _p("objeto de contratación", "objeto contratacion", "objeto de la contratación", "objeto"),
        "unidad_operativa": _p("unidad operativa", "unidad contratante", "repartición contratante"),
        "rubro": _p("rubro", "categoría", "categoria", "clase"),
        "moneda": _p("moneda", "tipo de moneda", "currency"),
        "modalidad": _p("modalidad", "modalidad de contratación"),
        "etapa": _p("etapa", "etapa del proceso", "fase"),
        "alcance": _p("alcance", "alcance territorial"),
        # Condiciones comerciales
        "presupuesto_oficial": _pt(
            "presupuesto oficial", "monto estimado", "monto oficial", "presupuesto",
            "monto total", "valor total estimado",
            campos_trazabilidad=["presupuesto_oficial", "monto_estimado", "presupuesto"]
        ),
        "valor_pliego": _pt(
            "valor del pliego", "precio pliego", "valor pliego", "costo pliego",
            "requiere pago pliego",
            campos_trazabilidad=["valor_pliego", "precio_pliego"]
        ),
        "plazo_mantenimiento_oferta": _pt(
            "plazo mantenimiento oferta", "mantenimiento oferta", "plazo mantenimiento",
            "mantenimiento de oferta",
            campos_trazabilidad=["plazo_mantenimiento_oferta", "mantenimiento_oferta"]
        ),
        "duracion_contrato": _pt(
            "duración", "duracion", "plazo contrato", "duración contrato", "duración del contrato",
            "plazo de contrato",
            campos_trazabilidad=["duracion_contrato", "plazo_contrato"]
        ),
        "pago": _p("pago", "condiciones de pago", "forma de pago", "condición de pago"),
        "anticipo": _p("anticipo financiero", "anticipo"),
        "contragarantia": _p("contragarantía", "contragarantia", "contra garantía"),
        "tipo_adjudicacion": _p("tipo de adjudicación", "tipo adjudicacion", "forma de adjudicación", "adjudicacion parcial", "adjudicacion total"),
        "tipo_cotizacion": _p("tipo de cotización", "tipo cotizacion", "forma de cotización", "cotizacion parcial", "cotizacion total"),
        "fecha_inicio": _p("fecha estimada de inicio", "fecha inicio", "fecha probable inicio", "inicio del contrato"),
        # Entrega y responsables
        "lugar_entrega": _p("lugar de entrega", "lugar entrega", "domicilio de entrega"),
        "plazo_entrega": _p("plazo de entrega", "plazo entrega", "tiempo de entrega"),
        "periodicidad": _p("periodicidad", "periodicidad de entrega"),
        "supervisor": _p("supervisor", "responsable", "responsable técnico"),
        "contacto": _p("contactos", "contacto", "teléfono", "telefono", "correo", "email"),
        "observaciones": _p("observaciones", "observaciones generales", "notas"),
        # Conteos
        "cantidad_renglones": len(caso.renglones),
        "cantidad_requisitos": len(caso.requisitos),
        "cantidad_hallazgos": len(caso.hallazgos),
        "cantidad_criticos": len(criticos_list),
        # Meta
        "fecha_carga": caso.publicado_en or caso.actualizado_en,
        "estado": caso.estado,
        # Para campos no mapeados (extras del Excel)
        "_campos_conocidos": {
            "nombre del proceso", "numero de proceso", "número de proceso", "nro proceso",
            "expediente", "organismo contratante", "organismo", "tipo de proceso",
            "objeto de contratación", "objeto contratacion", "objeto", "unidad operativa",
            "rubro", "moneda", "modalidad", "etapa", "alcance", "presupuesto oficial",
            "monto estimado", "valor del pliego", "precio pliego", "plazo mantenimiento oferta",
            "mantenimiento oferta", "duración", "duracion", "plazo contrato", "pago",
            "anticipo financiero", "anticipo", "contragarantía", "contragarantia",
            "tipo de adjudicación", "tipo adjudicacion", "tipo de cotización",
            "fecha estimada de inicio", "fecha inicio", "lugar de entrega", "lugar entrega",
            "plazo de entrega", "supervisor", "contactos", "contacto", "observaciones",
            "periodicidad", "entidad contratante", "repartición",
        },
        "_datos_raw": datos,
    }

    def _mf(label: str, value: str) -> dict:
        return {
            "label": label,
            "display_value": value,
            "state": "Encontrado" if value else "No encontrado",
        }

    resumen["grupos"] = {
        "informacion_basica": [
            _mf("Nombre del Proceso", resumen["nombre_proceso"]),
            _mf("Nro. Proceso", resumen["numero_proceso"]),
            _mf("Expediente", resumen["expediente"]),
            _mf("Organismo", resumen["organismo"]),
            _mf("Tipo de Proceso", resumen["tipo_proceso"]),
            _mf("Objeto", resumen["objeto"]),
            _mf("Unidad Operativa", resumen["unidad_operativa"]),
            _mf("Rubro", resumen["rubro"]),
            _mf("Moneda", resumen["moneda"]),
            _mf("Modalidad", resumen["modalidad"]),
            _mf("Etapa", resumen["etapa"]),
            _mf("Alcance", resumen["alcance"]),
        ],
        "condiciones_comerciales": [
            _mf("Presupuesto Oficial", resumen["presupuesto_oficial"]),
            _mf("Valor del Pliego", resumen["valor_pliego"]),
            _mf("Plazo Mantenimiento Oferta", resumen["plazo_mantenimiento_oferta"]),
            _mf("Duración Contrato", resumen["duracion_contrato"]),
            _mf("Condiciones de Pago", resumen["pago"]),
            _mf("Anticipo", resumen["anticipo"]),
            _mf("Contragarantía", resumen["contragarantia"]),
            _mf("Tipo de Adjudicación", resumen["tipo_adjudicacion"]),
            _mf("Tipo de Cotización", resumen["tipo_cotizacion"]),
            _mf("Fecha de Inicio", resumen["fecha_inicio"]),
        ],
        "entrega_responsables": [
            _mf("Lugar de Entrega", resumen["lugar_entrega"]),
            _mf("Plazo de Entrega", resumen["plazo_entrega"]),
            _mf("Periodicidad", resumen["periodicidad"]),
            _mf("Supervisor / Responsable", resumen["supervisor"]),
            _mf("Contacto", resumen["contacto"]),
            _mf("Observaciones", resumen["observaciones"]),
        ],
    }
    return resumen


# ---------------------------------------------------------------------------
# Ruta legacy: redirige al nuevo módulo
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/pliegos")
def pliegos_legacy_redirect():
    return RedirectResponse("/mercado-publico/lectura-pliegos", 301)


@router.get("/mercado-publico/pliegos/upload")
def pliegos_legacy_upload_redirect():
    return RedirectResponse("/mercado-publico/lectura-pliegos", 301)


# ---------------------------------------------------------------------------
# LISTADO DE CASOS
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos", response_class=HTMLResponse)
def lectura_pliegos_lista(request: Request, estado: str = "", q: str = ""):
    blocked = _require_user(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db
    es_admin = _is_admin(request)

    query = db.query(PliegoSolicitud)

    if not es_admin:
        query = query.filter(PliegoSolicitud.creado_por_id == user.id)

    if estado:
        query = query.filter(PliegoSolicitud.estado == estado)

    if q:
        like = f"%{q}%"
        query = query.filter(
            PliegoSolicitud.titulo.ilike(like) |
            PliegoSolicitud.organismo.ilike(like) |
            PliegoSolicitud.nombre_licitacion.ilike(like) |
            PliegoSolicitud.numero_proceso.ilike(like)
        )

    casos = query.order_by(PliegoSolicitud.actualizado_en.desc()).all()

    # Conteo por estado para el admin
    conteos = {}
    if es_admin:
        for est in PLIEGO_ESTADOS:
            conteos[est] = db.query(PliegoSolicitud).filter(
                PliegoSolicitud.estado == est).count()

    return templates.TemplateResponse("pliegos/lista.html", {
        "request": request,
        "user": user,
        "casos": casos,
        "es_admin": es_admin,
        "estado_filtro": estado,
        "q": q,
        "conteos": conteos,
        "PLIEGO_ESTADOS": PLIEGO_ESTADOS,
        "PLIEGO_ESTADO_LABELS": PLIEGO_ESTADO_LABELS,
    })


# ---------------------------------------------------------------------------
# NUEVA SOLICITUD
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/nueva", response_class=HTMLResponse)
def lectura_pliegos_nueva_form(request: Request):
    blocked = _require_user(request)
    if blocked:
        return blocked
    return templates.TemplateResponse("pliegos/nueva_solicitud.html", {
        "request": request,
        "user": request.state.user,
    })


@router.post("/mercado-publico/lectura-pliegos/nueva", response_class=HTMLResponse)
async def lectura_pliegos_nueva_submit(
    request: Request,
    accion: str = Form("enviar"),
    titulo: str = Form(...),
    organismo: str = Form(""),
    nombre_licitacion: str = Form(""),
    numero_proceso: str = Form(""),
    expediente: str = Form(""),
    observaciones_usuario: str = Form(""),
    archivos: List[UploadFile] = File(default=[]),
):
    blocked = _require_user(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    if not titulo.strip():
        return templates.TemplateResponse("pliegos/nueva_solicitud.html", {
            "request": request,
            "user": user,
            "error": "El título es obligatorio.",
            "form": {"titulo": titulo, "organismo": organismo,
                     "nombre_licitacion": nombre_licitacion,
                     "numero_proceso": numero_proceso, "expediente": expediente,
                     "observaciones_usuario": observaciones_usuario},
        })

    # Validar extensiones
    archivos_validos = [f for f in archivos if f.filename]
    for f in archivos_validos:
        ext = Path(f.filename).suffix.lower()
        if ext not in ALLOWED_USER_EXTS:
            return templates.TemplateResponse("pliegos/nueva_solicitud.html", {
                "request": request,
                "user": user,
                "error": f"Extensión no permitida: {f.filename}. Permitidas: {', '.join(ALLOWED_USER_EXTS)}",
                "form": {"titulo": titulo},
            })

    estado_inicial = "borrador" if accion == "borrador" else "pendiente_revision"

    caso = PliegoSolicitud(
        titulo=titulo.strip(),
        organismo=organismo.strip() or None,
        nombre_licitacion=nombre_licitacion.strip() or None,
        numero_proceso=numero_proceso.strip() or None,
        expediente=expediente.strip() or None,
        observaciones_usuario=observaciones_usuario.strip() or None,
        estado=estado_inicial,
        creado_por_id=user.id,
    )
    db.add(caso)
    db.flush()  # Get ID

    _registrar_historial(db, caso.id, None, estado_inicial,
                         "Solicitud creada", user.id)

    # Guardar archivos
    if archivos_validos:
        caso_dir = UPLOAD_DIR / str(caso.id)
        caso_dir.mkdir(parents=True, exist_ok=True)

        for f in archivos_validos:
            content = await f.read()
            if not content:
                continue
            ext = Path(f.filename).suffix.lower()
            nombre_guardado = f"{uuid.uuid4().hex}{ext}"
            path = caso_dir / nombre_guardado
            path.write_bytes(content)

            archivo = PliegoArchivo(
                solicitud_id=caso.id,
                nombre_original=f.filename,
                nombre_guardado=nombre_guardado,
                tipo_mime=f.content_type,
                tamano_bytes=len(content),
                url_path=f"/static/uploads/pliegos_solicitudes/{caso.id}/{nombre_guardado}",
            )
            db.add(archivo)

    db.commit()

    # --- Notificación a admins solo si la solicitud fue enviada (no borrador) ---
    if estado_inicial == "pendiente_revision":
        try:
            from web_comparativas.notifications_service import notify_admins
            nombre_remitente = user.name or user.email.split("@")[0]
            notify_admins(
                db,
                title="Nueva solicitud de Lectura de Pliegos",
                message=f"{nombre_remitente} envió una nueva solicitud: «{titulo[:60]}»",
                category="pliegos",
                link=f"/mercado-publico/lectura-pliegos/{caso.id}/admin",
            )
        except Exception:
            pass

    return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso.id}", 303)


# ---------------------------------------------------------------------------
# DETALLE DEL CASO (usuario ve su propio; admin ve cualquiera)
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/{caso_id}", response_class=HTMLResponse)
def lectura_pliegos_detalle(request: Request, caso_id: int):
    blocked = _require_user(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db
    es_admin = _is_admin(request)

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404, "Caso no encontrado")

    # Permiso: el usuario solo puede ver sus propios casos
    if not es_admin and caso.creado_por_id != user.id:
        raise HTTPException(403, "No tenés acceso a este caso")

    # Admin es redirigido a vista admin
    if es_admin:
        return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}/admin", 302)

    return templates.TemplateResponse("pliegos/detalle_usuario.html", {
        "request": request,
        "user": user,
        "caso": caso,
        "PLIEGO_ESTADO_LABELS": PLIEGO_ESTADO_LABELS,
    })


# ---------------------------------------------------------------------------
# VISTA ADMIN DEL CASO
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/{caso_id}/admin", response_class=HTMLResponse)
def lectura_pliegos_admin_caso(request: Request, caso_id: int):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404, "Caso no encontrado")

    transiciones_posibles = TRANSICIONES_ADMIN.get(caso.estado, [])
    puede_cargar_excel = caso.estado in ESTADOS_CARGA_EXCEL

    # Versión activa del Excel
    excel_activo = (
        db.query(PliegoExcelCarga)
        .filter(PliegoExcelCarga.solicitud_id == caso_id, PliegoExcelCarga.es_activa == True)
        .order_by(PliegoExcelCarga.version.desc())
        .first()
    )

    return templates.TemplateResponse("pliegos/admin_caso.html", {
        "request": request,
        "user": user,
        "caso": caso,
        "PLIEGO_ESTADO_LABELS": PLIEGO_ESTADO_LABELS,
        "PLIEGO_ESTADOS": PLIEGO_ESTADOS,
        "transiciones": transiciones_posibles,
        "puede_cargar_excel": puede_cargar_excel,
        "excel_activo": excel_activo,
        "ESTADOS_CARGA_EXCEL": list(ESTADOS_CARGA_EXCEL),
    })


# ---------------------------------------------------------------------------
# CAMBIO DE ESTADO (admin)
# ---------------------------------------------------------------------------

@router.post("/mercado-publico/lectura-pliegos/{caso_id}/estado")
async def lectura_pliegos_cambiar_estado(
    request: Request,
    caso_id: int,
    nuevo_estado: str = Form(...),
    comentario: str = Form(""),
):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    transiciones_validas = TRANSICIONES_ADMIN.get(caso.estado, [])
    if nuevo_estado not in transiciones_validas:
        return JSONResponse({"ok": False, "msg": f"Transición inválida: {caso.estado} → {nuevo_estado}"}, 400)

    if nuevo_estado == "listo" and not caso.datos_proceso:
        return JSONResponse({"ok": False, "msg": "No se puede publicar sin Excel válido cargado."}, 400)

    if nuevo_estado == "listo":
        caso.publicado_en = dt.datetime.now(dt.timezone.utc)

    _cambiar_estado(db, caso, nuevo_estado, comentario or f"Estado cambiado a: {nuevo_estado}", user.id)

    # Asignar admin responsable si no lo tiene
    if not caso.admin_responsable_id:
        caso.admin_responsable_id = user.id

    db.commit()

    # --- Notificación al usuario creador según el nuevo estado ---
    _MENSAJES_ESTADO = {
        "en_revision": (
            "Tu solicitud está siendo revisada",
            "El administrador tomó tu solicitud «{titulo}» y está en revisión.",
        ),
        "observado": (
            "Tu solicitud tiene una observación — requiere tu atención",
            "El administrador dejó una observación en «{titulo}». Ingresá para ver el detalle.",
        ),
        "rechazado": (
            "Tu solicitud fue rechazada",
            "La solicitud «{titulo}» fue rechazada. Revisá los comentarios del administrador.",
        ),
        "listo": (
            "Tu solicitud está lista ✓",
            "La solicitud «{titulo}» fue procesada y ya está disponible para consultar.",
        ),
        "pendiente_procesamiento": (
            "Tu solicitud está en procesamiento externo",
            "La solicitud «{titulo}» fue enviada a procesamiento. Te avisaremos cuando haya novedades.",
        ),
        "archivado": (
            "Tu solicitud fue archivada",
            "La solicitud «{titulo}» fue archivada por el administrador.",
        ),
    }
    if nuevo_estado in _MENSAJES_ESTADO and caso.creado_por_id:
        try:
            from web_comparativas.notifications_service import create_notification
            titulo_notif, msg_tpl = _MENSAJES_ESTADO[nuevo_estado]
            create_notification(
                db,
                user_id=caso.creado_por_id,
                title=titulo_notif,
                message=msg_tpl.format(titulo=caso.titulo[:55]),
                category="pliegos",
                link=f"/mercado-publico/lectura-pliegos/{caso_id}",
            )
        except Exception:
            pass

    return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}/admin", 303)


# ---------------------------------------------------------------------------
# OBSERVACIÓN INTERNA (admin)
# ---------------------------------------------------------------------------

@router.post("/mercado-publico/lectura-pliegos/{caso_id}/observacion")
async def lectura_pliegos_observacion(
    request: Request,
    caso_id: int,
    observacion: str = Form(...),
):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    caso.observaciones_admin = observacion.strip()
    caso.actualizado_en = dt.datetime.now(dt.timezone.utc)

    _registrar_historial(db, caso.id, caso.estado, caso.estado,
                         f"Observación interna actualizada", user.id)
    db.commit()

    # --- Notificar al usuario que el admin dejó una observación ---
    if caso.creado_por_id:
        try:
            from web_comparativas.notifications_service import create_notification
            create_notification(
                db,
                user_id=caso.creado_por_id,
                title="El administrador dejó una observación en tu solicitud",
                message=f"Revisá la observación en «{caso.titulo[:55]}» para continuar con el proceso.",
                category="pliegos",
                link=f"/mercado-publico/lectura-pliegos/{caso_id}",
            )
        except Exception:
            pass

    return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}/admin", 303)


# ---------------------------------------------------------------------------
# MARCAR ENVIADO A GPT (admin)
# ---------------------------------------------------------------------------

@router.post("/mercado-publico/lectura-pliegos/{caso_id}/marcar-gpt")
async def lectura_pliegos_marcar_gpt(
    request: Request,
    caso_id: int,
    obs_proc: str = Form(""),
):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    caso.enviado_a_gpt_en = dt.datetime.now(dt.timezone.utc)
    caso.enviado_a_gpt_por_id = user.id
    if obs_proc:
        caso.observaciones_procesamiento = obs_proc.strip()

    if caso.estado == "en_revision":
        _cambiar_estado(db, caso, "pendiente_procesamiento",
                        "Archivos enviados a procesamiento externo (GPT)", user.id)

    db.commit()
    return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}/admin", 303)


# ---------------------------------------------------------------------------
# CARGA DEL EXCEL (admin)
# ---------------------------------------------------------------------------

@router.post("/mercado-publico/lectura-pliegos/{caso_id}/excel")
async def lectura_pliegos_cargar_excel(
    request: Request,
    caso_id: int,
    excel_file: UploadFile = File(...),
    obs_excel: str = Form(""),
):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    if caso.estado not in ESTADOS_CARGA_EXCEL:
        return JSONResponse(
            {"ok": False, "msg": f"El estado actual ({caso.estado}) no permite cargar Excel."},
            400
        )

    if not excel_file.filename.lower().endswith(".xlsx"):
        return RedirectResponse(
            f"/mercado-publico/lectura-pliegos/{caso_id}/admin?error=solo_xlsx", 303)

    content = await excel_file.read()
    if not content:
        return RedirectResponse(
            f"/mercado-publico/lectura-pliegos/{caso_id}/admin?error=archivo_vacio", 303)

    # Validar estructura del Excel
    errores, advertencias = _validar_excel(content)

    if errores:
        # Guardar igual para que admin pueda ver el reporte de errores
        return RedirectResponse(
            f"/mercado-publico/lectura-pliegos/{caso_id}/validacion?errores={_encode_msgs(errores)}&advertencias={_encode_msgs(advertencias)}&pendiente=1",
            303
        )

    # Guardar Excel en disco
    excel_dir = EXCEL_DIR / str(caso_id)
    excel_dir.mkdir(parents=True, exist_ok=True)

    # Calcular versión
    ultima_version = (
        db.query(PliegoExcelCarga)
        .filter(PliegoExcelCarga.solicitud_id == caso_id)
        .count()
    )
    nueva_version = ultima_version + 1
    nombre_guardado = f"excel_v{nueva_version}_{uuid.uuid4().hex[:6]}.xlsx"
    (excel_dir / nombre_guardado).write_bytes(content)

    # Desactivar versiones previas
    db.query(PliegoExcelCarga).filter(
        PliegoExcelCarga.solicitud_id == caso_id,
        PliegoExcelCarga.es_activa == True
    ).update({"es_activa": False})

    carga = PliegoExcelCarga(
        solicitud_id=caso_id,
        nombre_archivo=excel_file.filename,
        version=nueva_version,
        url_path=f"/static/uploads/pliegos_excel/{caso_id}/{nombre_guardado}",
        cargado_por_id=user.id,
        es_activa=True,
        observaciones=obs_excel.strip() or None,
    )
    db.add(carga)

    # Importar datos del Excel a las tablas
    try:
        _importar_excel(db, caso_id, content)
    except Exception as e:
        traceback.print_exc()
        db.rollback()
        return RedirectResponse(
            f"/mercado-publico/lectura-pliegos/{caso_id}/admin?error=import_{str(e)[:50]}", 303)

    _cambiar_estado(db, caso, "excel_cargado",
                    f"Excel cargado (v{nueva_version}): {excel_file.filename}", user.id)
    db.commit()

    return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}/validacion", 303)


# ---------------------------------------------------------------------------
# DESCARGA DE EXCEL (admin)
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/{caso_id}/excel/download")
def lectura_pliegos_descargar_excel(request: Request, caso_id: int):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    db: Session = request.state.db
    carga = (
        db.query(PliegoExcelCarga)
        .filter(PliegoExcelCarga.solicitud_id == caso_id, PliegoExcelCarga.es_activa == True)
        .order_by(PliegoExcelCarga.version.desc())
        .first()
    )
    if not carga or not carga.url_path:
        raise HTTPException(404, "No hay Excel disponible")

    path = BASE_DIR / carga.url_path.lstrip("/")
    if not path.exists():
        raise HTTPException(404, "Archivo no encontrado en disco")

    return FileResponse(str(path), filename=carga.nombre_archivo,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ---------------------------------------------------------------------------
# DESCARGA DE ARCHIVO DEL USUARIO
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/{caso_id}/archivo/{archivo_id}")
def lectura_pliegos_descargar_archivo(request: Request, caso_id: int, archivo_id: int):
    blocked = _require_user(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db
    es_admin = _is_admin(request)

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)
    if not es_admin and caso.creado_por_id != user.id:
        raise HTTPException(403)

    archivo = db.get(PliegoArchivo, archivo_id)
    if not archivo or archivo.solicitud_id != caso_id:
        raise HTTPException(404)

    path = BASE_DIR / archivo.url_path.lstrip("/")
    if not path.exists():
        raise HTTPException(404, "Archivo no encontrado en disco")

    return FileResponse(str(path), filename=archivo.nombre_original)


def _build_ampliada_context(caso: PliegoSolicitud, rp_output: dict) -> dict:
    proceso = caso.datos_proceso.datos if caso.datos_proceso else {}

    req_por_categoria: dict = {}
    for requisito in caso.requisitos:
        categoria = (requisito.categoria or "Otros").strip()
        req_por_categoria.setdefault(categoria, []).append(requisito)

    impacto_order = {"alto": 0, "high": 0, "medio": 1, "medium": 1, "bajo": 2, "low": 2, "baja": 2}
    hallazgos_ordenados = sorted(
        caso.hallazgos,
        key=lambda hallazgo: (impacto_order.get((hallazgo.impacto or "").lower(), 5), hallazgo.categoria or "")
    )

    return {
        "proceso": proceso,
        "proceso_extras": rp_output.get("process_extra_fields", {}),
        "req_por_cat": req_por_categoria,
        "hallazgos_ordenados": hallazgos_ordenados,
    }


# ---------------------------------------------------------------------------
# VALIDACIÓN POST-CARGA EXCEL (admin)
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/{caso_id}/validacion", response_class=HTMLResponse)
def lectura_pliegos_validacion(
    request: Request,
    caso_id: int,
    errores: str = "",
    advertencias: str = "",
    pendiente: str = "0",
):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    # Resumen de registros cargados
    resumen = {
        "cronograma": len(caso.cronograma),
        "requisitos": len(caso.requisitos),
        "garantias": len(caso.garantias),
        "renglones": len(caso.renglones),
        "documentos": len(caso.documentos_pliego),
        "actos_admin": len(caso.actos_admin),
        "hallazgos": len(caso.hallazgos),
        "faltantes": len(caso.faltantes),
        "trazabilidad": len(caso.trazabilidad),
        "proceso": bool(caso.datos_proceso and caso.datos_proceso.datos),
    }

    criticos = [f for f in caso.faltantes if (f.criticidad or "").lower() in {"alta", "critica", "crítica", "alto"}]
    rp_output = build_rp_output(caso)
    resumen_summary = build_resumen_licitacion(caso)
    completitud = resumen_summary["completitud_ejecutiva"]["porcentaje"]
    completitud_ejecutiva = resumen_summary["completitud_ejecutiva"]
    completitud_tecnica = resumen_summary["completitud_tecnica"]

    # Calcular estado Fusion independientemente de LicIA
    fusion_ctx = calcular_estado_fusion(caso)

    errores_list = _decode_msgs(errores) if errores else []
    adv_list = _decode_msgs(advertencias) if advertencias else []

    ctrl_datos = caso.control_carga.datos if caso.control_carga else {}
    ctrl_row = ctrl_datos[0] if isinstance(ctrl_datos, list) and ctrl_datos else ctrl_datos

    return templates.TemplateResponse("pliegos/validacion_excel.html", {
        "request": request,
        "user": user,
        "caso": caso,
        "resumen": resumen,
        "criticos": criticos,
        "completitud": completitud,
        "completitud_ejecutiva": completitud_ejecutiva,
        "completitud_tecnica": completitud_tecnica,
        "rp_output": rp_output,
        "rp_validation": rp_output["validation"],
        "rp_validation_by_group": rp_output["validation_by_group"],
        "rp_mapping_matrix": rp_output["mapping_matrix"],
        "errores": errores_list,
        "advertencias": adv_list,
        "pendiente": pendiente == "1",
        "PLIEGO_ESTADO_LABELS": PLIEGO_ESTADO_LABELS,
        "control_carga": ctrl_row,
        "fusion_ctx": fusion_ctx,
        "fusion_campos_obligatorios": FUSION_CAMPOS_OBLIGATORIOS,
    })


@router.post("/mercado-publico/lectura-pliegos/{caso_id}/publicar")
async def lectura_pliegos_publicar(
    request: Request,
    caso_id: int,
    comentario: str = Form(""),
):
    blocked = _require_admin(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    if not caso.datos_proceso:
        return RedirectResponse(
            f"/mercado-publico/lectura-pliegos/{caso_id}/validacion?errores=Sin_datos_de_proceso",
            303)

    _cambiar_estado(db, caso, "listo",
                    comentario or "Caso publicado para visualización", user.id)
    caso.publicado_en = dt.datetime.now(dt.timezone.utc)
    db.commit()

    return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}/vista", 303)


# ---------------------------------------------------------------------------
# VISTA FINAL ESTRUCTURADA
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/{caso_id}/vista", response_class=HTMLResponse)
def lectura_pliegos_vista_final(request: Request, caso_id: int):
    blocked = _require_user(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db
    es_admin = _is_admin(request)

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    if not es_admin and caso.creado_por_id != user.id:
        raise HTTPException(403)

    if caso.estado != "listo" and not es_admin:
        return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}", 302)

    canonical_output = build_canonical_output(caso)

    # RECONSTRUIR RESUMEN Y VARIABLES NECESARIAS PARA visualizacion_rp.html
    resumen = _build_resumen_licitacion(caso)
    resumen_summary = build_resumen_licitacion(caso)
    completitud = resumen_summary["completitud_ejecutiva"]["porcentaje"]
    completitud_ejecutiva = resumen_summary["completitud_ejecutiva"]
    completitud_tecnica = resumen_summary["completitud_tecnica"]
    proceso = caso.datos_proceso.datos if caso.datos_proceso else {}

    # Calcular estado Fusion independientemente de LicIA
    fusion_ctx = calcular_estado_fusion(caso)

    req_por_cat: dict = {}
    for r in caso.requisitos:
        cat = (r.categoria or "Otros").strip()
        req_por_cat.setdefault(cat, []).append(r)

    _IMPACTO_ORDER = {"alto": 0, "high": 0, "medio": 1, "medium": 1, "bajo": 2, "low": 2, "baja": 2}
    hallazgos_ordenados = sorted(
        caso.hallazgos,
        key=lambda h: (_IMPACTO_ORDER.get((h.impacto or "").lower(), 5), h.categoria or "")
    )

    campos_conocidos_lower = {k.lower() for k in resumen.get("_campos_conocidos", set())}
    proceso_extras = {
        k: v for k, v in proceso.items()
        if type(v) in (str, int, float, bool) and k.lower().strip() not in campos_conocidos_lower
    }

    # Versión activa del Excel cargado
    excel_activo = next(
        (c for c in sorted(caso.cargas_excel, key=lambda x: x.version, reverse=True) if c.es_activa),
        None
    )

    response = templates.TemplateResponse("pliegos/visualizacion_rp.html", {
        "request": request,
        "user": user,
        "caso": caso,
        "canonical_output": canonical_output,
        "proceso": proceso,
        "resumen": resumen,
        "completitud": completitud,
        "completitud_ejecutiva": completitud_ejecutiva,
        "completitud_tecnica": completitud_tecnica,
        "req_por_cat": req_por_cat,
        "hallazgos_ordenados": hallazgos_ordenados,
        "proceso_extras": proceso_extras,
        "es_admin": es_admin,
        "PLIEGO_ESTADO_LABELS": PLIEGO_ESTADO_LABELS,
        "control_carga": caso.control_carga.datos if caso.control_carga else {},
        "fusion_ctx": fusion_ctx,
        "fusion_campos_obligatorios": FUSION_CAMPOS_OBLIGATORIOS,
        "fusion_campos_complementarios": FUSION_CAMPOS_COMPLEMENTARIOS,
        "excel_activo": excel_activo,
    })
    response.headers["Cache-Control"] = "private, max-age=45"
    return response


# ---------------------------------------------------------------------------
# VISTA AMPLIADA Y EXPORTACION RP
# ---------------------------------------------------------------------------

@router.get("/mercado-publico/lectura-pliegos/{caso_id}/vista-ampliada", response_class=HTMLResponse)
def lectura_pliegos_vista_ampliada(request: Request, caso_id: int):
    blocked = _require_user(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db
    es_admin = _is_admin(request)

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    if not es_admin and caso.creado_por_id != user.id:
        raise HTTPException(403)

    if caso.estado != "listo" and not es_admin:
        return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}", 302)

    rp_output = build_rp_output(caso)
    contexto_ampliado = _build_ampliada_context(caso, rp_output)
    debug_matrix = build_debug_matrix(caso)
    resumen_summary = build_resumen_licitacion(caso)
    completitud_ejecutiva = resumen_summary["completitud_ejecutiva"]
    completitud_tecnica = resumen_summary["completitud_tecnica"]

    # Calcular estado Fusion independientemente de LicIA
    fusion_ctx = calcular_estado_fusion(caso)

    # Parsear SIEM_Analitica para la vista ampliada (lista de filas)
    analitica_raw = caso.analitica.datos if caso.analitica else []
    analitica_rows = analitica_raw if isinstance(analitica_raw, list) else []

    # Control_Carga: normalizar para la vista
    ctrl_datos = caso.control_carga.datos if caso.control_carga else {}
    ctrl_row = ctrl_datos[0] if isinstance(ctrl_datos, list) and ctrl_datos else ctrl_datos

    excel_activo = next(
        (c for c in sorted(caso.cargas_excel, key=lambda x: x.version, reverse=True) if c.es_activa),
        None
    )

    response = templates.TemplateResponse("pliegos/visualizacion_ampliada.html", {
        "request": request,
        "user": user,
        "caso": caso,
        "rp_output": rp_output,
        "debug_matrix": debug_matrix,
        "completitud_ejecutiva": completitud_ejecutiva,
        "completitud_tecnica": completitud_tecnica,
        "es_admin": es_admin,
        "PLIEGO_ESTADO_LABELS": PLIEGO_ESTADO_LABELS,
        "control_carga": ctrl_row,
        "analitica": caso.analitica.datos if caso.analitica else {},
        "analitica_rows": analitica_rows,
        "fusion_ctx": fusion_ctx,
        "fusion_campos_obligatorios": FUSION_CAMPOS_OBLIGATORIOS,
        "excel_activo": excel_activo,
        **contexto_ampliado,
    })
    response.headers["Cache-Control"] = "private, max-age=45"
    return response


@router.get("/mercado-publico/lectura-pliegos/{caso_id}/rp/export")
def lectura_pliegos_exportar_rp(request: Request, caso_id: int):
    blocked = _require_user(request)
    if blocked:
        return blocked

    user: User = request.state.user
    db: Session = request.state.db
    es_admin = _is_admin(request)

    caso = db.get(PliegoSolicitud, caso_id)
    if not caso:
        raise HTTPException(404)

    if not es_admin and caso.creado_por_id != user.id:
        raise HTTPException(403)

    if caso.estado != "listo" and not es_admin:
        return RedirectResponse(f"/mercado-publico/lectura-pliegos/{caso_id}", 302)

    payload = export_fusion_excel_bytes(caso)
    safe_name = _slugify(caso.numero_proceso or caso.titulo or f"caso_{caso.id}")
    filename = f"SIEM_Fusion_{safe_name or caso.id}.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        io.BytesIO(payload),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# ---------------------------------------------------------------------------
# EXCEL PARSING & IMPORT
# ---------------------------------------------------------------------------

def _build_sheet_map(xl) -> dict:
    """
    Mapea nombre canónico → nombre real en el Excel.
    Maneja prefijos numéricos (ej: '01_Proceso'), diferencias de mayúsculas
    y nombres abreviados (ej: '07_Administrativo' → 'Actos_Administrativos').
    """
    actual_names = xl.sheet_names
    result = {}
    
    all_sheets = REQUIRED_SHEETS + OPTIONAL_SHEETS
    for canonical in all_sheets:
        c_low = canonical.lower()
        # 1. Coincidencia exacta
        if canonical in actual_names:
            result[canonical] = canonical
            continue
        # 2. Coincidencia exacta sin distinción de mayúsculas
        found = next((a for a in actual_names if a.lower() == c_low), None)
        if found:
            result[canonical] = found
            continue
        # 3. Quitar prefijo numérico (ej: '01_', '07_') y comparar sin mayúsculas
        found = next(
            (a for a in actual_names if _re.sub(r'^\d+_', '', a).lower() == c_low),
            None
        )
        if found:
            result[canonical] = found
            continue
        # 4. Coincidencia parcial: uno contiene al otro tras quitar prefijo
        for a in actual_names:
            stripped = _re.sub(r'^\d+_', '', a).lower()
            if stripped in c_low or c_low in stripped:
                result[canonical] = a
                break
    return result


def _looks_like_template_row(row) -> bool:
    values = [_safe_str(value) for value in row.tolist()]
    non_empty = [value for value in values if value]
    if not non_empty:
        return True
    normalized = [_re.sub(r"\s+", " ", value.lower()) for value in non_empty]
    placeholder_hits = 0
    placeholder_patterns = (
        "id proceso",
        "id único",
        "id unico",
        "sí/no",
        "si/no",
        "aaaa-mm-dd",
        "hh:mm",
        "archivo origen",
        "página/sección",
        "pagina/seccion",
        "encontrado/ambiguo",
        "abierto/cerrado",
        "literal/inferido/calculado",
        "texto/tabla/imagen",
    )
    for value in normalized:
        if value in {
            "campo", "valor", "fecha", "organismo", "resumen", "notas",
            "monto", "importe", "plazo", "objeto", "rubro", "uoa", "0-100",
        }:
            placeholder_hits += 1
            continue
        if any(pattern in value for pattern in placeholder_patterns):
            placeholder_hits += 1
    return placeholder_hits >= max(2, len(non_empty) // 3)


def _iter_data_rows(df):
    for _, row in df.iterrows():
        if _looks_like_template_row(row):
            continue
        yield row


def _extract_proceso_data(df) -> dict:
    if df.empty:
        return {}
    if len(df.columns) > 2:
        rows = list(_iter_data_rows(df))
        if not rows:
            return {}
        row = rows[0]
        return {str(col): _safe_str(row.get(col, "")) for col in df.columns}
    datos = {}
    for row in _iter_data_rows(df):
        key = _safe_str(row.iloc[0])
        value = _safe_str(row.iloc[1]) if len(row) > 1 else ""
        if key:
            datos[key] = value
    return datos


def _validar_excel(content: bytes) -> tuple[list, list]:
    """Valida la estructura del Excel. Retorna (errores_criticos, advertencias)."""
    errores = []
    advertencias = []
    try:
        xl = pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        return [f"No se pudo leer el archivo Excel: {e}"], []

    sheet_map = _build_sheet_map(xl)

    for hoja in REQUIRED_SHEETS:
        if hoja not in sheet_map:
            errores.append(f"Hoja faltante: '{hoja}'")

    # Verificar hojas no vacías
    for hoja in REQUIRED_SHEETS:
        actual = sheet_map.get(hoja)
        if actual:
            try:
                df = xl.parse(actual)
                if df.empty:
                    advertencias.append(f"Hoja vacía: '{hoja}'")
            except Exception:
                advertencias.append(f"No se pudo leer la hoja: '{hoja}'")

    # Verificar hoja Proceso no vacía
    proc_actual = sheet_map.get("Proceso")
    if proc_actual:
        try:
            df_proc = xl.parse(proc_actual)
            if df_proc.empty:
                errores.append("La hoja 'Proceso' está vacía")
        except Exception:
            pass

    return errores, advertencias


def _importar_excel(db: Session, solicitud_id: int, content: bytes):
    """Importa los datos del Excel a las tablas correspondientes. Reemplaza datos anteriores."""

    # Limpiar datos anteriores
    for Model in [PliegoCronograma, PliegoRequisito, PliegoGarantia, PliegoRenglon,
                  PliegoDocumento, PliegoActoAdmin, PliegoHallazgo, PliegoFaltante,
                  PliegoTrazabilidad, PliegoFusionRenglon]:
        db.query(Model).filter(
            getattr(Model, "solicitud_id") == solicitud_id
        ).delete()
    
    for ModelSingle in [PliegoFusionCabecera, PliegoAnalitica, PliegoControlCarga]:
        db.query(ModelSingle).filter(
            getattr(ModelSingle, "solicitud_id") == solicitud_id
        ).delete()

    proc_existente = db.query(PliegoProceso).filter(
        PliegoProceso.solicitud_id == solicitud_id).first()

    xl = pd.ExcelFile(io.BytesIO(content))
    sheet_map = _build_sheet_map(xl)

    # ── Proceso ──────────────────────────────────────────────────────────────
    if "Proceso" in sheet_map:
        df = xl.parse(sheet_map["Proceso"], dtype=str).fillna("")
        datos = _extract_proceso_data(df)
        if False and not df.empty:
            if len(df.columns) >= 2:
                # Formato clave-valor: columna 0 = campo, columna 1 = valor
                for _, row in df.iterrows():
                    k = _safe_str(row.iloc[0])
                    v = _safe_str(row.iloc[1]) if len(row) > 1 else ""
                    if k:
                        datos[k] = v
            else:
                # Formato de fila única
                datos = {col: _safe_str(df[col].iloc[0]) for col in df.columns if _safe_str(df[col].iloc[0])}
        if proc_existente:
            proc_existente.datos = datos
        else:
            db.add(PliegoProceso(solicitud_id=solicitud_id, datos=datos))

    # ── Cronograma ────────────────────────────────────────────────────────────
    if "Cronograma" in sheet_map:
        df = xl.parse(sheet_map["Cronograma"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "hito": ["hito", "evento", "item"],
            "fecha": ["fecha"],
            "hora": ["hora"],
            "lugar_medio": ["lugar", "medio", "lugar/medio", "lugar_medio", "observaciones"],
            "estado_dato": ["estado", "estado_dato"],
            "fuente": ["fuente"],
        })
        for row in _iter_data_rows(df):
            hito = _safe_str(row.get(col_map.get("hito", ""), ""))
            if not hito:
                continue
            db.add(PliegoCronograma(
                solicitud_id=solicitud_id,
                hito=hito,
                fecha=_safe_str(row.get(col_map.get("fecha", ""), "")),
                hora=_safe_str(row.get(col_map.get("hora", ""), "")),
                lugar_medio=_safe_str(row.get(col_map.get("lugar_medio", ""), "")),
                estado_dato=_safe_str(row.get(col_map.get("estado_dato", ""), "")),
                fuente=_safe_str(row.get(col_map.get("fuente", ""), "")),
            ))

    # ── Requisitos ────────────────────────────────────────────────────────────
    if "Requisitos" in sheet_map:
        df = xl.parse(sheet_map["Requisitos"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "categoria": ["categoria", "categoría", "tipo"],
            "descripcion": ["requisito", "descripcion", "descripción", "detalle"],
            "obligatorio": ["obligatoriedad", "obligatorio", "requerido"],
            "momento_presentacion": ["momento", "momento_presentacion"],
            "medio_presentacion": ["medio", "medio_presentacion"],
            "vigencia": ["vigencia"],
            "estado_dato": ["estado", "estado_dato"],
            "fuente": ["fuente"],
        })
        for row in _iter_data_rows(df):
            desc = _safe_str(row.get(col_map.get("descripcion", ""), ""))
            if not desc:
                continue
            db.add(PliegoRequisito(
                solicitud_id=solicitud_id,
                categoria=_safe_str(row.get(col_map.get("categoria", ""), "")),
                descripcion=desc,
                obligatorio=_safe_str(row.get(col_map.get("obligatorio", ""), "")),
                momento_presentacion=_safe_str(row.get(col_map.get("momento_presentacion", ""), "")),
                medio_presentacion=_safe_str(row.get(col_map.get("medio_presentacion", ""), "")),
                vigencia=_safe_str(row.get(col_map.get("vigencia", ""), "")),
                estado_dato=_safe_str(row.get(col_map.get("estado_dato", ""), "")),
                fuente=_safe_str(row.get(col_map.get("fuente", ""), "")),
            ))

    # ── Garantias ─────────────────────────────────────────────────────────────
    if "Garantias" in sheet_map:
        df = xl.parse(sheet_map["Garantias"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "tipo": ["tipo", "tipo_garantia"],
            "requerida": ["requerida", "requerido"],
            "porcentaje": ["porcentaje_o_monto", "porcentaje", "porcentaje_%", "%"],
            "base_calculo": ["base", "base_calculo"],
            "plazo": ["momento_exigibilidad", "plazo"],
            "formas_admitidas": ["formas", "formas_admitidas"],
            "estado_dato": ["estado", "estado_dato"],
            "fuente": ["fuente"],
        })
        for row in _iter_data_rows(df):
            tipo = _safe_str(row.get(col_map.get("tipo", ""), ""))
            if not tipo:
                continue
            db.add(PliegoGarantia(
                solicitud_id=solicitud_id,
                tipo=tipo,
                requerida=_safe_str(row.get(col_map.get("requerida", ""), "")),
                porcentaje=_safe_str(row.get(col_map.get("porcentaje", ""), "")),
                base_calculo=_safe_str(row.get(col_map.get("base_calculo", ""), "")),
                plazo=_safe_str(row.get(col_map.get("plazo", ""), "")),
                formas_admitidas=_safe_str(row.get(col_map.get("formas_admitidas", ""), "")),
                estado_dato=_safe_str(row.get(col_map.get("estado_dato", ""), "")),
                fuente=_safe_str(row.get(col_map.get("fuente", ""), "")),
            ))

    # ── Renglones ─────────────────────────────────────────────────────────────
    if "Renglones" in sheet_map:
        df = xl.parse(sheet_map["Renglones"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "orden": ["renglon_orden", "orden", "n°", "nro", "#"],
            "numero_renglon": ["numero_renglon", "renglón", "renglon", "nro_renglon"],
            "codigo_item": ["codigo_item", "codigo", "código", "cod"],
            "descripcion": ["descripcion", "descripción", "detalle"],
            "cantidad": ["cantidad", "cant"],
            "unidad": ["unidad_medida", "unidad", "ud", "u.m."],
            "destino_efector": ["destino_efector", "destino", "efector"],
            "entrega_parcial": ["entrega_parcial", "entrega parcial"],
            "obs_tecnicas": ["observaciones", "obs", "obs_tecnicas"],
            "estado": ["estado"],
            "fuente": ["fuente"],
            "marca": ["marca"],
            "precio_unitario": ["precio_unitario", "precio unitario"],
            "importe_total": ["importe_total", "importe total"],
            "lugar_entrega": ["lugar_entrega", "lugar entrega"],
            "plazo_entrega": ["plazo_entrega", "plazo entrega"],
            "periodicidad": ["periodicidad"],
        })
        for idx, row in enumerate(_iter_data_rows(df)):
            desc = _safe_str(row.get(col_map.get("descripcion", ""), ""))
            if not desc:
                continue
            orden_val = _safe_str(row.get(col_map.get("orden", ""), ""))
            try:
                orden_int = int(float(orden_val)) if orden_val else idx + 1
            except (ValueError, TypeError):
                orden_int = idx + 1
            # Capturar todas las columnas del Excel de LicIA
            extra_data = {
                "lugar_entrega": _safe_str(row.get(col_map.get("lugar_entrega", "lugar_entrega"), "")),
                "plazo_entrega": _safe_str(row.get(col_map.get("plazo_entrega", "plazo_entrega"), "")),
                "periodicidad": _safe_str(row.get(col_map.get("periodicidad", "periodicidad"), "")),
                "marca": _safe_str(row.get(col_map.get("marca", "marca"), "")),
                "precio_unitario": _safe_str(row.get(col_map.get("precio_unitario", "precio_unitario"), "")),
                "importe_total": _safe_str(row.get(col_map.get("importe_total", "importe_total"), "")),
                "fuente": _safe_str(row.get(col_map.get("fuente", "fuente"), "")),
                "especificaciones_tecnicas": _safe_str(row.get("especificaciones_tecnicas", "")),
                "muestras": _safe_str(row.get("muestras", "")),
            }
            db.add(PliegoRenglon(
                solicitud_id=solicitud_id,
                orden=orden_int,
                numero_renglon=_safe_str(row.get(col_map.get("numero_renglon", ""), "")),
                codigo_item=_safe_str(row.get(col_map.get("codigo_item", ""), "")),
                descripcion=desc,
                cantidad=_safe_str(row.get(col_map.get("cantidad", ""), "")),
                unidad=_safe_str(row.get(col_map.get("unidad", ""), "")),
                destino_efector=_safe_str(row.get(col_map.get("destino_efector", ""), "")),
                entrega_parcial=_safe_str(row.get(col_map.get("entrega_parcial", ""), "")),
                obs_tecnicas=_safe_str(row.get(col_map.get("obs_tecnicas", ""), "")),
                estado=_safe_str(row.get(col_map.get("estado", ""), "")),
                datos_extra={k: v for k, v in extra_data.items() if v},
            ))

    # ── Documentos ────────────────────────────────────────────────────────────
    if "Documentos" in sheet_map:
        df = xl.parse(sheet_map["Documentos"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "documento_id": ["documento_id", "id", "id_documento"],
            "nombre": ["nombre_documento", "nombre", "documento"],
            "tipo": ["tipo_documento", "tipo"],
            "rol": ["prioridad_jerarquica", "rol", "prioridad"],
            "obligatorio": ["obligatorio"],
            "estado_lectura": ["estado", "estado_lectura"],
            "fecha": ["fecha"],
            "observaciones": ["observaciones", "observacion"],
            "fuente": ["fuente"],
        })
        for row in _iter_data_rows(df):
            nombre = _safe_str(row.get(col_map.get("nombre", ""), ""))
            if not nombre:
                continue
            # Guardar observaciones y fuente en fecha (campo disponible) o en un campo extra
            # El modelo PliegoDocumento usa fecha para data adicional; usamos el campo existente
            obs_val = _safe_str(row.get(col_map.get("observaciones", ""), ""))
            fuente_val = _safe_str(row.get(col_map.get("fuente", ""), ""))
            doc_id_val = _safe_str(row.get(col_map.get("documento_id", ""), ""))
            db.add(PliegoDocumento(
                solicitud_id=solicitud_id,
                nombre=nombre,
                tipo=_safe_str(row.get(col_map.get("tipo", ""), "")),
                rol=_safe_str(row.get(col_map.get("rol", ""), "")),
                obligatorio=doc_id_val or _safe_str(row.get(col_map.get("obligatorio", ""), "")),
                estado_lectura=_safe_str(row.get(col_map.get("estado_lectura", ""), "")),
                fecha=fuente_val or obs_val or _safe_str(row.get(col_map.get("fecha", ""), "")),
            ))

    # ── Actos_Administrativos ─────────────────────────────────────────────────
    if "Actos_Administrativos" in sheet_map:
        df = xl.parse(sheet_map["Actos_Administrativos"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "tipo_acto": ["tipo", "tipo_acto"],
            "numero": ["numero", "número", "nro"],
            "numero_especial": ["numero_especial", "nro_especial"],
            "fecha": ["fecha"],
            "organismo_emisor": ["organismo", "organismo_emisor"],
            "descripcion": ["descripcion", "descripción", "detalle"],
        })
        for row in _iter_data_rows(df):
            tipo = _safe_str(row.get(col_map.get("tipo_acto", ""), ""))
            if not tipo:
                continue
            db.add(PliegoActoAdmin(
                solicitud_id=solicitud_id,
                tipo_acto=tipo,
                numero=_safe_str(row.get(col_map.get("numero", ""), "")),
                numero_especial=_safe_str(row.get(col_map.get("numero_especial", ""), "")),
                fecha=_safe_str(row.get(col_map.get("fecha", ""), "")),
                organismo_emisor=_safe_str(row.get(col_map.get("organismo_emisor", ""), "")),
                descripcion=_safe_str(row.get(col_map.get("descripcion", ""), "")),
            ))

    # ── Hallazgos_Extra ───────────────────────────────────────────────────────
    if "Hallazgos_Extra" in sheet_map:
        df = xl.parse(sheet_map["Hallazgos_Extra"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "categoria": ["categoria", "categoría"],
            "titulo": ["titulo", "título"],
            "hallazgo": ["descripcion", "descripción", "hallazgo", "detalle"],
            "impacto": ["impacto"],
            "accion_sugerida": ["accion_sugerida", "accion", "acción"],
            "fuente": ["fuente"],
            "campo_dashboard_sugerido": ["campo_dashboard_sugerido", "campo_dashboard"],
            "valor_sugerido": ["valor_sugerido", "valor sugerido"],
            "estado": ["estado"],
        })
        for row in _iter_data_rows(df):
            titulo_val = _safe_str(row.get(col_map.get("titulo", ""), ""))
            hallazgo_val = _safe_str(row.get(col_map.get("hallazgo", ""), ""))
            # El campo hallazgo del modelo recibe el texto principal; priorizamos título + descripción
            texto_principal = titulo_val or hallazgo_val
            if not texto_principal:
                continue
            # Enriquecer: concatenar titulo + descripcion si ambos existen
            if titulo_val and hallazgo_val and titulo_val != hallazgo_val:
                texto_principal = f"{titulo_val}: {hallazgo_val}"
            extra = {
                "campo_dashboard_sugerido": _safe_str(row.get(col_map.get("campo_dashboard_sugerido", ""), "")),
                "valor_sugerido": _safe_str(row.get(col_map.get("valor_sugerido", ""), "")),
                "estado": _safe_str(row.get(col_map.get("estado", ""), "")),
            }
            db.add(PliegoHallazgo(
                solicitud_id=solicitud_id,
                categoria=_safe_str(row.get(col_map.get("categoria", ""), "")),
                hallazgo=texto_principal,
                impacto=_safe_str(row.get(col_map.get("impacto", ""), "")),
                accion_sugerida=_safe_str(row.get(col_map.get("accion_sugerida", ""), "")),
                fuente=_safe_str(row.get(col_map.get("fuente", ""), "")),
                datos_extra={k: v for k, v in extra.items() if v} if extra else None,
            ))

    # ── Faltantes_y_Dudas ─────────────────────────────────────────────────────
    if "Faltantes_y_Dudas" in sheet_map:
        df = xl.parse(sheet_map["Faltantes_y_Dudas"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "campo_objetivo": ["campo_o_tema", "campo", "campo_objetivo"],
            "motivo": ["situacion", "situación", "motivo"],
            "detalle": ["detalle", "descripcion", "descripción"],
            "criticidad": ["criticidad", "prioridad"],
            "accion_recomendada": ["accion_recomendada", "accion_sugerida", "accion", "acción"],
            "fuente": ["fuente"],
            "estado": ["estado"],
        })
        for row in _iter_data_rows(df):
            campo = _safe_str(row.get(col_map.get("campo_objetivo", ""), ""))
            if not campo:
                continue
            db.add(PliegoFaltante(
                solicitud_id=solicitud_id,
                campo_objetivo=campo,
                motivo=_safe_str(row.get(col_map.get("motivo", ""), "")),
                detalle=_safe_str(row.get(col_map.get("detalle", ""), "")),
                criticidad=_safe_str(row.get(col_map.get("criticidad", ""), "")),
                accion_recomendada=_safe_str(row.get(col_map.get("accion_recomendada", ""), "")),
                fuente=_safe_str(row.get(col_map.get("fuente", ""), "")),
                estado=_safe_str(row.get(col_map.get("estado", ""), "")),
            ))

    # ── Trazabilidad ──────────────────────────────────────────────────────────
    if "Trazabilidad" in sheet_map:
        df = xl.parse(sheet_map["Trazabilidad"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "campo": ["campo", "campo_objetivo"],
            "valor_extraido": ["valor", "valor_extraido"],
            "documento_fuente": ["documento_origen", "documento", "documento_fuente", "fuente"],
            "pagina_seccion": ["pagina", "página", "seccion", "sección", "pagina_seccion"],
            "tipo_evidencia": ["tipo_documento", "tipo_evidencia", "tipo"],
            "observacion": ["observaciones", "observacion", "observación"],
            "estado_extraccion": ["estado"],
            "metodo_extraccion": ["metodo_extraccion"],
            "texto_evidencia": ["evidencia_breve", "texto_evidencia"],
            "normalizacion_aplicada": ["normalizacion_aplicada"],
        })
        for row in _iter_data_rows(df):
            campo = _safe_str(row.get(col_map.get("campo", ""), ""))
            if not campo:
                continue
            observaciones = [
                _safe_str(row.get(col_map.get("observacion", ""), "")),
                f"Estado: {_safe_str(row.get(col_map.get('estado_extraccion', ''), ''))}" if col_map.get("estado_extraccion") and _safe_str(row.get(col_map.get("estado_extraccion", ""), "")) else "",
                f"Metodo: {_safe_str(row.get(col_map.get('metodo_extraccion', ''), ''))}" if col_map.get("metodo_extraccion") and _safe_str(row.get(col_map.get("metodo_extraccion", ""), "")) else "",
                f"Texto evidencia: {_safe_str(row.get(col_map.get('texto_evidencia', ''), ''))}" if col_map.get("texto_evidencia") and _safe_str(row.get(col_map.get("texto_evidencia", ""), "")) else "",
                f"Normalizacion: {_safe_str(row.get(col_map.get('normalizacion_aplicada', ''), ''))}" if col_map.get("normalizacion_aplicada") and _safe_str(row.get(col_map.get("normalizacion_aplicada", ""), "")) else "",
            ]
            db.add(PliegoTrazabilidad(
                solicitud_id=solicitud_id,
                campo=campo,
                valor_extraido=_safe_str(row.get(col_map.get("valor_extraido", ""), "")),
                documento_fuente=_safe_str(row.get(col_map.get("documento_fuente", ""), "")),
                pagina_seccion=_safe_str(row.get(col_map.get("pagina_seccion", ""), "")),
                tipo_evidencia=_safe_str(row.get(col_map.get("tipo_evidencia", ""), "")),
                observacion="\n".join(part for part in observaciones if part),
            ))

    # ── Fusion_Cabecera ───────────────────────────────────────────────────────
    if "Fusion_Cabecera" in sheet_map:
        df = xl.parse(sheet_map["Fusion_Cabecera"], dtype=str).fillna("")
        datos = _extract_proceso_data(df)
        db.add(PliegoFusionCabecera(solicitud_id=solicitud_id, datos=datos))

    # ── Fusion_Renglones ──────────────────────────────────────────────────────
    if "Fusion_Renglones" in sheet_map:
        df = xl.parse(sheet_map["Fusion_Renglones"], dtype=str).fillna("")
        col_map = _col_map(df.columns, {
            "numero_renglon": ["renglon_orden", "renglón", "renglon", "nro_renglon", "numero_renglon", "n°", "#"],
            "codigo_item": ["codigo_item", "codigo", "código", "item"],
            "descripcion": ["descripcion", "descripción", "detalle"],
            "cantidad": ["cantidad", "cant"],
            "unidad": ["unidad_medida", "unidad", "ud"],
            "precio_unitario_estimado": ["precio_unitario", "precio", "estimado", "monto"],
            "lugar_entrega": ["lugar_entrega", "lugar entrega"],
            "plazo_entrega": ["plazo_entrega", "plazo entrega"],
            "periodicidad": ["periodicidad"],
            "marca": ["marca"],
            "importe_total": ["importe_total", "importe total"],
            "destino_efector": ["destino_efector", "destino"],
            "observaciones": ["observaciones", "obs"],
            "fuente": ["fuente"],
            "estado": ["estado"],
        })
        for idx, row in enumerate(_iter_data_rows(df)):
            desc = _safe_str(row.get(col_map.get("descripcion", ""), ""))
            num_ren = _safe_str(row.get(col_map.get("numero_renglon", ""), "")) or str(idx + 1)
            if not desc and not num_ren:
                continue

            # Capturar todos los campos del Excel LicIA en datos_extra
            extra_data = {
                "lugar_entrega": _safe_str(row.get(col_map.get("lugar_entrega", ""), "")),
                "plazo_entrega": _safe_str(row.get(col_map.get("plazo_entrega", ""), "")),
                "periodicidad": _safe_str(row.get(col_map.get("periodicidad", ""), "")),
                "marca": _safe_str(row.get(col_map.get("marca", ""), "")),
                "importe_total": _safe_str(row.get(col_map.get("importe_total", ""), "")),
                "destino_efector": _safe_str(row.get(col_map.get("destino_efector", ""), "")),
                "observaciones": _safe_str(row.get(col_map.get("observaciones", ""), "")),
                "fuente": _safe_str(row.get(col_map.get("fuente", ""), "")),
                "estado": _safe_str(row.get(col_map.get("estado", ""), "")),
            }

            db.add(PliegoFusionRenglon(
                solicitud_id=solicitud_id,
                numero_renglon=num_ren,
                codigo_item=_safe_str(row.get(col_map.get("codigo_item", ""), "")),
                descripcion=desc,
                cantidad=_safe_str(row.get(col_map.get("cantidad", ""), "")),
                unidad=_safe_str(row.get(col_map.get("unidad", ""), "")),
                precio_unitario_estimado=_safe_str(row.get(col_map.get("precio_unitario_estimado", ""), "")),
                datos_extra={k: v for k, v in extra_data.items() if v} or None,
            ))

    # ── SIEM_Analitica ────────────────────────────────────────────────────────
    # Parsear como lista de filas (no clave-valor) para que la vista ampliada pueda iterar
    if "SIEM_Analitica" in sheet_map:
        df = xl.parse(sheet_map["SIEM_Analitica"], dtype=str).fillna("")
        analitica_rows = []
        for row in _iter_data_rows(df):
            row_dict = {str(col): _safe_str(row.get(col, "")) for col in df.columns}
            if any(v for v in row_dict.values()):
                analitica_rows.append(row_dict)
        # Si tiene formato clave-valor (menos de 3 columnas) usar dict; sino lista de filas
        datos_analitica = analitica_rows if len(df.columns) >= 3 else _extract_proceso_data(df)
        db.add(PliegoAnalitica(solicitud_id=solicitud_id, datos=datos_analitica))

    # ── Control_Carga ─────────────────────────────────────────────────────────
    # Parsear como lista de filas para acceso directo; el primer registro es el de control
    if "Control_Carga" in sheet_map:
        df = xl.parse(sheet_map["Control_Carga"], dtype=str).fillna("")
        ctrl_rows = []
        for row in _iter_data_rows(df):
            row_dict = {str(col): _safe_str(row.get(col, "")) for col in df.columns}
            if any(v for v in row_dict.values()):
                ctrl_rows.append(row_dict)
        # Si hay exactamente 1 fila, guardar como dict para compatibilidad; sino lista
        if len(ctrl_rows) == 1:
            datos_ctrl = {k.lower().strip(): v for k, v in ctrl_rows[0].items()}
        elif ctrl_rows:
            datos_ctrl = [{k.lower().strip(): v for k, v in r.items()} for r in ctrl_rows]
        else:
            datos_ctrl = _extract_proceso_data(df)
        db.add(PliegoControlCarga(solicitud_id=solicitud_id, datos=datos_ctrl))


def _col_map(columns, mapping: dict) -> dict:
    """Mapea nombres canónicos a nombres reales de columnas del DataFrame."""
    cols_lower = {str(c).lower().strip(): str(c) for c in columns}
    result = {}
    for canonical, candidates in mapping.items():
        for candidate in candidates:
            if candidate.lower() in cols_lower:
                result[canonical] = cols_lower[candidate.lower()]
                break
        else:
            # Intentar coincidencia parcial
            for candidate in candidates:
                for col_lower, col_real in cols_lower.items():
                    if candidate.lower() in col_lower:
                        result[canonical] = col_real
                        break
                if canonical in result:
                    break
    return result


def _encode_msgs(msgs: list) -> str:
    return ",".join(m.replace(",", ";") for m in msgs)


def _decode_msgs(s: str) -> list:
    return [m for m in s.split(",") if m.strip()]
