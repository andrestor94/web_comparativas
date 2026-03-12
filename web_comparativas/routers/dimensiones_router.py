import datetime as dt
import logging
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

import shutil
import tempfile

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, Request, UploadFile
from sqlalchemy.orm import Session

from web_comparativas.auth import require_roles
from web_comparativas.dimensionamiento.query_service import (
    build_filters,
    get_clients_by_result,
    get_debug_snapshot,
    get_family_consumption_table,
    get_filter_options,
    get_geography_distribution,
    get_kpis,
    get_results_breakdown,
    get_series,
    get_status,
    get_top_families,
)
from web_comparativas.models import User

router = APIRouter(prefix="/api/mercado-privado/dimensiones", tags=["dimensiones"])
logger = logging.getLogger("wc.dimensionamiento.api")

# Ruta al archivo de mapeo de negocios (relativa al package web_comparativas)
_NEGOCIOS_PATH = Path(__file__).resolve().parent.parent / "data" / "Negocios.xlsx"


@lru_cache(maxsize=1)
def _load_negocio_labels() -> dict[str, Any]:
    """Lee Negocios.xlsx y construye un mapeo anidado de código → descripción.

    Estructura retornada:
        {
            "unidades": {"4": "Insumos medico - hospitalarios", ...},
            "subunidades": {"4|1": "Insumos medico - hospitalarios", ...}
        }

    Las claves son strings para ser JSON-safe. El frontend usa
    value (código original) como filtro pero muestra el label descriptivo.
    Retorna vacío si el archivo no existe o no puede leerse.
    """
    result: dict[str, Any] = {"unidades": {}, "subunidades": {}}
    if not _NEGOCIOS_PATH.exists():
        logger.warning(
            "[NEGOCIO] Negocios.xlsx no encontrado en %s. "
            "Los filtros de unidad/subunidad no tendrán etiquetas descriptivas.",
            _NEGOCIOS_PATH,
        )
        return result
    try:
        import openpyxl
        wb = openpyxl.load_workbook(_NEGOCIOS_PATH, data_only=True, read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
        if not rows:
            return result

        # Detectar columnas por header (primera fila)
        header = [str(c).strip().lower() if c is not None else "" for c in rows[0]]
        try:
            idx_unidad = header.index("unidad")
            idx_subunidad = header.index("subunidad")
            idx_descrip = header.index("descrip")
        except ValueError:
            # Fallback: asumir orden unidad, subunidad, descrip
            idx_unidad, idx_subunidad, idx_descrip = 0, 1, 2

        unidades: dict[str, str] = {}
        subunidades: dict[str, str] = {}

        for row in rows[1:]:
            try:
                raw_u = row[idx_unidad]
                raw_s = row[idx_subunidad]
                raw_d = row[idx_descrip]
                if raw_u is None or raw_d is None:
                    continue
                descrip = str(raw_d).strip()
                if not descrip:
                    continue
                # Normalizar códigos a string entero (sin decimales)
                u_key = str(int(float(str(raw_u).strip())))
                s_key = str(int(float(str(raw_s).strip()))) if raw_s is not None else "0"
                # Mapeo de unidad: solo guardar la primera descripcion encontrada
                # (row con subunidad=0 suele ser el nombre de la unidad)
                if u_key not in unidades:
                    # Preferir la fila con subunidad=0 como nombre de la unidad
                    if s_key == "0":
                        unidades[u_key] = descrip
                elif s_key == "0":
                    unidades[u_key] = descrip
                # Mapeo de subunidad: clave compuesta "unidad|subunidad"
                compound = f"{u_key}|{s_key}"
                subunidades[compound] = descrip
            except (TypeError, ValueError):
                continue

        result["unidades"] = unidades
        result["subunidades"] = subunidades
        logger.info(
            "[NEGOCIO] Negocios.xlsx cargado: %d unidades, %d subunidades desde %s",
            len(unidades),
            len(subunidades),
            _NEGOCIOS_PATH,
        )
    except Exception:
        logger.exception("[NEGOCIO] Error leyendo Negocios.xlsx en %s", _NEGOCIOS_PATH)
    return result

AllowedUser = Annotated[
    User,
    Depends(require_roles("admin", "analista", "supervisor", "auditor")),
]


def get_db(request: Request) -> Session:
    db = getattr(request.state, "db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="No hay sesión de base de datos disponible.")
    return db


def _request_debug_payload(request: Request) -> dict[str, Any]:
    return {
        "path": request.url.path,
        "query_params": dict(request.query_params),
    }


def _safe_dashboard_response(request: Request, endpoint_name: str, fn, fallback_data):
    payload = _request_debug_payload(request)
    logger.info("[DIM][API] %s start payload=%s", endpoint_name, payload)
    try:
        data = fn()
        result_count = len(data) if isinstance(data, list) else (len(data.get("rows", [])) if isinstance(data, dict) and "rows" in data else None)
        logger.info("[DIM][API] %s success path=%s rows=%s", endpoint_name, request.url.path, result_count)
        return {"ok": True, "has_data": bool(data), "data": data}
    except Exception as exc:
        exc_str = str(exc).lower()
        exc_type = type(exc).__name__.lower()
        if (
            "statement timeout" in exc_str
            or "canceling statement" in exc_str
            or "querycanceled" in exc_type
            or "querycancelled" in exc_type
        ):
            error_code = "timeout"
            log_msg = "[DIM][API] %s TIMEOUT path=%s exc=%s"
        else:
            error_code = "backend_error"
            log_msg = "[DIM][API] %s BACKEND_ERROR path=%s exc=%s"
        logger.exception(log_msg, endpoint_name, request.url.path, exc)
        return {
            "ok": False,
            "has_data": False,
            "data": fallback_data,
            "error": True,
            "error_code": error_code,
            "message": f"Widget '{endpoint_name}' no disponible temporalmente.",
        }


def _filters_from_query(
    cliente: list[str] | None = Query(default=None),
    provincia: list[str] | None = Query(default=None),
    familia: list[str] | None = Query(default=None),
    plataforma: list[str] | None = Query(default=None),
    unidad_negocio: list[str] | None = Query(default=None),
    subunidad_negocio: list[str] | None = Query(default=None),
    resultado: list[str] | None = Query(default=None),
    fecha_desde: dt.date | None = Query(default=None),
    fecha_hasta: dt.date | None = Query(default=None),
    identified: bool | None = Query(default=None),
    is_client: bool | None = Query(default=None),
    search: str | None = Query(default=None),
):
    return build_filters(
        clientes=cliente,
        provincias=provincia,
        familias=familia,
        plataformas=plataforma,
        unidades_negocio=unidad_negocio,
        subunidades_negocio=subunidad_negocio,
        resultados=resultado,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        identified=identified,
        is_client=is_client,
        search=search,
    )


@router.get("/status")
def dimensionamiento_status(
    _: AllowedUser,
    db: Session = Depends(get_db),
):
    logger.info("[DIM][API] GET /status start")
    try:
        data = get_status(db)
        logger.info(
            "[DIM][API] GET /status success has_data=%s total_rows=%s",
            data.get("has_data"),
            data.get("total_rows"),
        )
        return {"ok": True, "data": data}
    except Exception:
        logger.exception("[DIM][API] GET /status failed")
        raise


@router.get("/debug-snapshot")
def dimensionamiento_debug_snapshot(
    request: Request,
    _: AllowedUser,
    db: Session = Depends(get_db),
):
    payload = _request_debug_payload(request)
    logger.info("[DIM][API] debug_snapshot start payload=%s", payload)
    try:
        data = get_debug_snapshot(db)
        logger.info(
            "[DIM][API] debug_snapshot success total_registros=%s table=%s",
            data.get("total_registros"),
            data.get("table"),
        )
        return {"ok": True, "data": data}
    except Exception:
        logger.exception("[DIM][API] debug_snapshot failed payload=%s", payload)
        raise


@router.get("/negocio-labels")
def dimensionamiento_negocio_labels(_: AllowedUser):
    """Devuelve el mapeo de códigos de unidad/subunidad negocio a nombres descriptivos
    leídos desde Negocios.xlsx. Cacheado en memoria.

    Respuesta:
        {
            "unidades": {"1": "AMBULATORIO", "4": "Insumos medico...", ...},
            "subunidades": {"1|0": "AMBULATORIO", "4|1": "...", ...}
        }
    """
    return {"ok": True, "data": _load_negocio_labels()}


@router.get("/filters")
def dimensionamiento_filters(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    logger.info("[DIM][API] GET /filters start filters=%s", filters)
    try:
        data = get_filter_options(db, filters)
        logger.info(
            "[DIM][API] GET /filters success clientes=%s provincias=%s familias=%s plataformas=%s",
            len(data.get("clientes", [])),
            len(data.get("provincias", [])),
            len(data.get("familias", [])),
            len(data.get("plataformas", [])),
        )
        return {"ok": True, "data": data}
    except Exception as exc:
        exc_str = str(exc).lower()
        error_code = "timeout" if ("statement timeout" in exc_str or "canceling statement" in exc_str) else "backend_error"
        logger.exception("[DIM][API] GET /filters failed filters=%s", filters)
        return {
            "ok": False,
            "has_data": False,
            "data": {
                "clientes": [],
                "provincias": [],
                "familias": [],
                "plataformas": [],
                "unidades_negocio": [],
                "subunidades_negocio": [],
                "resultados": [],
                "date_range": {"min": None, "max": None},
            },
            "error": True,
            "error_code": error_code,
            "message": "Filtros no disponibles temporalmente.",
        }


@router.get("/kpis")
def dimensionamiento_kpis(
    request: Request,
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return _safe_dashboard_response(
        request,
        "kpis",
        lambda: get_kpis(db, filters),
        {
            "total_rows": 0,
            "clientes": 0,
            "renglones": 0,
            "familias": 0,
            "cantidad_demandada": 0.0,
        },
    )


@router.get("/series")
def dimensionamiento_series(
    request: Request,
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return _safe_dashboard_response(
        request,
        "series",
        lambda: get_series(db, filters),
        {"months": [], "datasets": []},
    )


@router.get("/results")
def dimensionamiento_results(
    request: Request,
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return _safe_dashboard_response(
        request,
        "results",
        lambda: get_results_breakdown(db, filters),
        [],
    )


@router.get("/top-families")
def dimensionamiento_top_families(
    request: Request,
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=50),
):
    return _safe_dashboard_response(
        request,
        "top_families",
        lambda: get_top_families(db, filters, limit=limit),
        [],
    )


@router.get("/geo")
def dimensionamiento_geo(
    request: Request,
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return _safe_dashboard_response(
        request,
        "geo",
        lambda: get_geography_distribution(db, filters),
        [],
    )


@router.get("/clients-by-result")
def dimensionamiento_clients_by_result(
    request: Request,
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=30),
):
    return _safe_dashboard_response(
        request,
        "clients_by_result",
        lambda: get_clients_by_result(db, filters, limit=limit),
        [],
    )


@router.get("/family-consumption")
def dimensionamiento_family_consumption(
    request: Request,
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
    limit: int = Query(default=20, ge=1, le=50),
):
    return _safe_dashboard_response(
        request,
        "family_consumption",
        lambda: get_family_consumption_table(db, filters, limit=limit),
        {"months": [], "rows": []},
    )


@router.post("/process")
def deprecated_manual_process(_: AllowedUser):
    raise HTTPException(
        status_code=410,
        detail=(
            "La carga manual fue removida. Actualice el CSV unificado y ejecute la ingesta backend "
            "para refrescar el módulo Dimensionamiento."
        ),
    )


AdminUser = Annotated[
    User,
    Depends(require_roles("admin")),
]


def _run_ingestion_background(tmp_path: str) -> None:
    """Ejecuta la ingesta del CSV y luego elimina el archivo temporal."""
    from web_comparativas.dimensionamiento.ingestion import ingest_dimensionamiento_csv
    try:
        result = ingest_dimensionamiento_csv(csv_path=tmp_path, mode="replace", force=True)
        print(f"[DIMENSIONAMIENTO] Ingesta completada: {result}", flush=True)
    except Exception as exc:
        print(f"[DIMENSIONAMIENTO] Error en ingesta background: {exc}", flush=True)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


@router.post("/upload-csv", status_code=202)
async def upload_dimensionamiento_csv(
    background_tasks: BackgroundTasks,
    _: AdminUser,
    file: UploadFile = File(...),
):
    """
    (Solo admin) Recibe el CSV unificado, lo guarda en un archivo temporal
    y dispara la ingesta en background. Devuelve 202 inmediatamente.
    El progreso se puede consultar con GET /status.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV (.csv).")

    # Guardar en /tmp (único directorio writable en Render)
    tmp_dir = tempfile.mkdtemp()
    tmp_path = str(Path(tmp_dir) / "dataset_unificado.csv")
    try:
        with open(tmp_path, "wb") as out:
            shutil.copyfileobj(file.file, out)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error al guardar el archivo: {exc}")
    finally:
        await file.close()

    background_tasks.add_task(_run_ingestion_background, tmp_path)
    return {
        "ok": True,
        "message": "Archivo recibido. La ingesta está corriendo en background.",
        "hint": "Consultá GET /api/mercado-privado/dimensiones/status para ver el progreso.",
    }
