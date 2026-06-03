import datetime as dt
import logging
import os
import shutil
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Body, Depends, File, HTTPException, Query, Request, UploadFile, Header
from fastapi.responses import JSONResponse
from sqlalchemy import func, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from web_comparativas.auth import require_roles
from web_comparativas.dimensionamiento.query_service import (
    build_filters,
    get_clients_by_result,
    get_dashboard_bootstrap,
    get_debug_snapshot,
    get_family_consumption_table,
    get_filter_options,
    get_geography_distribution,
    get_kpis,
    get_results_breakdown,
    get_series,
    get_status,
    get_top_families,
    invalidate_query_cache,
    DEFAULT_DASHBOARD_SNAPSHOT_KEY,
)
from web_comparativas.models import User, IS_SQLITE
from web_comparativas.dimensionamiento.models import (
    DimensionamientoImportRun,
    DimensionamientoRecord,
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoDashboardSnapshot,
    DimensionamientoImportError,
)

router = APIRouter(prefix="/api/mercado-privado/dimensiones", tags=["dimensiones"])
logger = logging.getLogger("wc.dimensionamiento.api")


def verify_import_token(x_import_token: str = Header(..., alias="X-Import-Token")) -> str:
    """Verifica el token de importación en producción o local."""
    expected_token = os.getenv("DIMENSIONAMIENTO_IMPORT_TOKEN")
    if not expected_token:
        if IS_SQLITE:
            expected_token = "local_dev_token"
        else:
            logger.error("[DIM][IMPORT] DIMENSIONAMIENTO_IMPORT_TOKEN environment variable is not configured.")
            raise HTTPException(
                status_code=500,
                detail="El token de importación no está configurado en el servidor.",
            )
    if x_import_token != expected_token:
        logger.warning("[DIM][IMPORT] Invalid X-Import-Token header received.")
        raise HTTPException(
            status_code=403,
            detail="Token de importación inválido.",
        )
    return x_import_token


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
        status_code = 503 if error_code == "timeout" else 500
        return JSONResponse(status_code=status_code, content={
            "ok": False,
            "has_data": False,
            "data": fallback_data if error_code == "timeout" else None,
            "error": True,
            "error_code": error_code,
            "message": f"Widget '{endpoint_name}' no disponible temporalmente.",
            "detail": (
                f"Widget '{endpoint_name}' excedió el tiempo de respuesta."
                if error_code == "timeout"
                else f"Error interno en el backend de Dimensionamiento ({endpoint_name})."
            ),
        })


def _filters_from_query(
    cliente: list[str] | None = Query(default=None),
    provincia: list[str] | None = Query(default=None),
    familia: list[str] | None = Query(default=None),
    plataforma: list[str] | None = Query(default=None),
    unidad_negocio: list[str] | None = Query(default=None),
    unidad_negocio_excluir: list[str] | None = Query(default=None),
    subunidad_negocio: list[str] | None = Query(default=None),
    resultado: list[str] | None = Query(default=None),
    fecha_desde: dt.date | None = Query(default=None),
    fecha_hasta: dt.date | None = Query(default=None),
    is_client: bool | None = Query(default=None),
):
    return build_filters(
        clientes=cliente,
        provincias=provincia,
        familias=familia,
        plataformas=plataforma,
        unidades_negocio=unidad_negocio,
        unidades_negocio_excluir=unidad_negocio_excluir,
        subunidades_negocio=subunidad_negocio,
        resultados=resultado,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        is_client=is_client,
    )


def _payload_list(payload: dict[str, Any] | None, *keys: str) -> list[str] | None:
    if not payload:
        return None
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value in (None, ""):
            return None
        if isinstance(value, list):
            return value
        return [value]
    return None


def _payload_date(payload: dict[str, Any] | None, key: str) -> dt.date | None:
    if not payload or not payload.get(key):
        return None
    value = payload.get(key)
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _payload_bool(payload: dict[str, Any] | None, key: str, default: bool | None = None) -> bool | None:
    if not payload or key not in payload:
        return default
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "si", "s"}


def _payload_int(payload: dict[str, Any] | None, key: str, default: int) -> int:
    if not payload or key not in payload:
        return default
    try:
        return int(payload.get(key))
    except (TypeError, ValueError):
        return default


def _filters_from_payload(payload: dict[str, Any] | None):
    return build_filters(
        clientes=_payload_list(payload, "cliente", "clientes"),
        provincias=_payload_list(payload, "provincia", "provincias"),
        familias=_payload_list(payload, "familia", "familias"),
        plataformas=_payload_list(payload, "plataforma", "plataformas"),
        unidades_negocio=_payload_list(payload, "unidad_negocio", "unidades_negocio", "unidadNegocio"),
        unidades_negocio_excluir=_payload_list(payload, "unidad_negocio_excluir", "unidades_negocio_excluir"),
        subunidades_negocio=_payload_list(payload, "subunidad_negocio", "subunidades_negocio", "subunidad"),
        resultados=_payload_list(payload, "resultado", "resultados"),
        fecha_desde=_payload_date(payload, "fecha_desde"),
        fecha_hasta=_payload_date(payload, "fecha_hasta"),
        is_client=_payload_bool(payload, "is_client"),
    )


def _filters_for_request(request: Request, query_filters, payload: dict[str, Any] | None):
    return _filters_from_payload(payload) if request.method.upper() == "POST" else query_filters


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


@router.api_route("/bootstrap", methods=["GET", "POST"])
def dimensionamiento_bootstrap(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    include_status: bool = Query(default=True),
    bypass_snapshot: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    active_filters = _filters_for_request(request, filters, payload)
    active_include_status = _payload_bool(payload, "include_status", include_status)
    active_bypass_snapshot = _payload_bool(payload, "bypass_snapshot", bypass_snapshot)
    return _safe_dashboard_response(
        request,
        "bootstrap",
        lambda: get_dashboard_bootstrap(
            db,
            active_filters,
            include_status=active_include_status,
            bypass_snapshot=active_bypass_snapshot,
        ),
        {
            "status": {"has_data": False, "total_rows": 0, "platforms": [], "last_import": None},
            "filters": {
                "clientes": [],
                "provincias": [],
                "familias": [],
                "plataformas": [],
                "unidades_negocio": [],
                "subunidades_negocio": [],
                "resultados": [],
                "date_range": {"min": None, "max": None},
            },
            "kpis": {
                "total_rows": 0,
                "clientes": 0,
                "renglones": 0,
                "familias": 0,
                "provincias": 0,
                "valorizacion": 0,
            },
            "series": {"months": [], "datasets": []},
            "results": [],
            "top_families": [],
            "geo": [],
            "clients_by_result": [],
            "family_consumption": {
                "months": [],
                "rows": [],
                "total": 0,
            },
            "meta": {"source": "fallback", "stale": False},
        },
    )


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


@router.api_route("/filters", methods=["GET", "POST"])
def dimensionamiento_filters(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    filters = _filters_for_request(request, filters, payload)
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


@router.api_route("/kpis", methods=["GET", "POST"])
def dimensionamiento_kpis(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    filters = _filters_for_request(request, filters, payload)
    return _safe_dashboard_response(
        request,
        "kpis",
        lambda: get_kpis(db, filters),
        {
            "total_rows": 0,
            "clientes": 0,
            "renglones": 0,
            "familias": 0,
            "provincias": 0,
            "valorizacion": 0,
        },
    )


@router.api_route("/series", methods=["GET", "POST"])
def dimensionamiento_series(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    filters = _filters_for_request(request, filters, payload)
    return _safe_dashboard_response(
        request,
        "series",
        lambda: get_series(db, filters),
        {"months": [], "datasets": []},
    )


@router.api_route("/results", methods=["GET", "POST"])
def dimensionamiento_results(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    filters = _filters_for_request(request, filters, payload)
    return _safe_dashboard_response(
        request,
        "results",
        lambda: get_results_breakdown(db, filters),
        [],
    )


@router.api_route("/top-families", methods=["GET", "POST"])
def dimensionamiento_top_families(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    filters = _filters_for_request(request, filters, payload)
    return _safe_dashboard_response(
        request,
        "top_families",
        lambda: get_top_families(db, filters),
        [],
    )


@router.api_route("/geo", methods=["GET", "POST"])
def dimensionamiento_geo(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    filters = _filters_for_request(request, filters, payload)
    return _safe_dashboard_response(
        request,
        "geo",
        lambda: get_geography_distribution(db, filters),
        [],
    )


@router.api_route("/clients-by-result", methods=["GET", "POST"])
def dimensionamiento_clients_by_result(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=30),
):
    filters = _filters_for_request(request, filters, payload)
    limit = max(1, min(30, _payload_int(payload, "limit", limit)))
    return _safe_dashboard_response(
        request,
        "clients_by_result",
        lambda: get_clients_by_result(db, filters, limit=limit),
        [],
    )


@router.api_route("/family-consumption", methods=["GET", "POST"])
def dimensionamiento_family_consumption(
    request: Request,
    _: AllowedUser,
    payload: dict[str, Any] | None = Body(default=None),
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    filters = _filters_for_request(request, filters, payload)
    return _safe_dashboard_response(
        request,
        "family_consumption",
        lambda: get_family_consumption_table(db, filters),
        {"months": [], "rows": [], "total": 0},
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


def _run_local_reload_background(
    csv_path: str,
    chunk_size: int,
    mode: str,
    force: bool,
) -> None:
    """Dispara la ingestión desde el CSV local del servidor en background."""
    from web_comparativas.dimensionamiento.ingestion import ingest_dimensionamiento_csv
    try:
        result = ingest_dimensionamiento_csv(
            csv_path=csv_path,
            chunk_size=chunk_size,
            mode=mode,
            force=force,
        )
        rows = result.get("rows_processed", 0)
        status = result.get("status")
        print(
            f"[DIMENSIONAMIENTO] reload-local completado: status={status} rows={rows:,}",
            flush=True,
        )
    except Exception as exc:
        print(f"[DIMENSIONAMIENTO] reload-local ERROR: {exc}", flush=True)


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


@router.post("/admin/reload-local", status_code=202)
def reload_local_csv(
    background_tasks: BackgroundTasks,
    _: AdminUser,
    chunk_size: int = Query(default=10000, ge=1000, le=50000, description="Filas por batch"),
    mode: str = Query(default="replace", pattern="^(replace|upsert)$"),
    force: bool = Query(default=True, description="Forzar aunque el hash no haya cambiado"),
):
    """
    (Solo admin) Dispara la reingestión del dataset_unificado.csv que ya está
    presente en el servidor, procesando por chunks sin cargar todo en memoria.

    Estrategia:
    - Lee el CSV local en batches de `chunk_size` filas (default 10 000)
    - PostgreSQL: staging table UNLOGGED + COPY FROM STDIN por chunk
    - mode=replace: carga nueva corrida, luego borra la anterior
    - Reconstruye dimensionamiento_family_monthly_summary al final
    - Invalida caché en memoria y refresca dashboard snapshot
    - No toca datos productivos hasta que la carga está 100% completa

    Devuelve 202 inmediatamente. Consultá GET /status para ver el progreso.
    """
    from web_comparativas.dimensionamiento.ingestion import DEFAULT_CSV_PATH

    csv_path = Path(
        os.getenv("DIMENSIONAMIENTO_CSV_PATH") or DEFAULT_CSV_PATH
    ).resolve()

    if not csv_path.exists():
        raise HTTPException(
            status_code=422,
            detail=(
                f"CSV no encontrado en el servidor: {csv_path}. "
                "Asegurate de que DIMENSIONAMIENTO_CSV_PATH apunte al archivo correcto "
                "o subilo vía POST /upload-csv."
            ),
        )

    size_mb = csv_path.stat().st_size / (1024 ** 2)
    logger.info(
        "[DIMENSIONAMIENTO] reload-local enqueued: path=%s size=%.1fMB "
        "chunk_size=%s mode=%s force=%s",
        csv_path,
        size_mb,
        chunk_size,
        mode,
        force,
    )

    background_tasks.add_task(
        _run_local_reload_background,
        str(csv_path),
        chunk_size,
        mode,
        force,
    )

    return {
        "ok": True,
        "message": "Recarga iniciada en background.",
        "csv_path": str(csv_path),
        "csv_size_mb": round(size_mb, 2),
        "chunk_size": chunk_size,
        "mode": mode,
        "force": force,
        "hint": "Consultá GET /api/mercado-privado/dimensiones/status para ver el progreso.",
    }


# ===========================================================================
# Endpoints Administrativos para Carga por Chunks desde PC Cliente
# ===========================================================================

class ImportStartPayload(BaseModel):
    source_path: str
    source_hash: str | None = None
    source_mtime: str | None = None
    mode: str = "replace"
    chunk_size: int = 20000


class RecordItem(BaseModel):
    id_registro_unico: str
    fecha: str
    plataforma: str
    cliente_nombre_homologado: str | None = None
    cliente_nombre_original: str | None = None
    cliente_visible: str | None = None
    cuit: str | None = None
    provincia: str | None = None
    cuenta_interna: str | None = None
    codigo_articulo: str | None = None
    descripcion: str | None = None
    clasificacion_suizo: str | None = None
    descripcion_articulo: str | None = None
    familia: str | None = None
    unidad_negocio: str | None = None
    subunidad_negocio: str | None = None
    cantidad_demandada: float
    valorizacion_estimada: float | None = 0.0
    resultado_participacion: str | None = None
    producto_nombre_original: str | None = None
    fecha_procesamiento: str | None = None
    is_identified: bool = False
    is_client: bool = False


class RecordChunkPayload(BaseModel):
    import_run_id: int
    records: list[RecordItem]


class SummaryItem(BaseModel):
    month: str
    plataforma: str
    cliente_nombre_homologado: str | None = None
    cliente_visible: str | None = None
    provincia: str | None = None
    familia: str | None = None
    unidad_negocio: str | None = None
    subunidad_negocio: str | None = None
    resultado_participacion: str | None = None
    is_identified: bool = False
    is_client: bool = False
    total_cantidad: float
    total_valorizacion: float
    total_registros: int
    clientes_unicos: int


class SummaryChunkPayload(BaseModel):
    import_run_id: int
    summaries: list[SummaryItem]


class FinalizePayload(BaseModel):
    import_run_id: int
    snapshot: dict[str, Any] | None = None
    summary_metadata: dict[str, Any] | None = None


class RollbackPayload(BaseModel):
    import_run_id: int
    error_message: str | None = None


class CleanupPayload(BaseModel):
    keep_runs: int = 2


def _parse_date(val: str | None) -> dt.date | None:
    if not val:
        return None
    val = val.strip()[:10]
    try:
        return dt.date.fromisoformat(val)
    except ValueError:
        return None


def _parse_datetime(val: str | None) -> dt.datetime | None:
    if not val:
        return None
    val = val.strip()
    if val.endswith("Z"):
        val = val[:-1]
    val = val.replace("T", " ")
    if len(val) >= 19:
        val = val[:19]
    try:
        return dt.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return dt.datetime.strptime(val[:10], "%Y-%m-%d")
        except ValueError:
            return None


@router.post("/admin/import/start")
def admin_import_start(
    payload: ImportStartPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """
    Inicia una nueva corrida de importación de dimensionamiento, poniéndola en
    estado 'running'. Retorna el ID generado.
    """
    try:
        mtime = _parse_datetime(payload.source_mtime)
        run = DimensionamientoImportRun(
            source_path=payload.source_path,
            source_hash=payload.source_hash,
            source_mtime=mtime,
            mode=payload.mode,
            status="running",
            chunk_size=payload.chunk_size,
            started_at=dt.datetime.utcnow(),
            rows_processed=0,
            rows_inserted=0,
            rows_updated=0,
            rows_rejected=0,
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        logger.info("[DIM][IMPORT] Started run_id=%d mode=%s", run.id, run.mode)
        return {"ok": True, "import_run_id": run.id}
    except Exception as e:
        db.rollback()
        logger.exception("[DIM][IMPORT] Error starting import run")
        raise HTTPException(status_code=500, detail=f"Error starting import run: {e}")


@router.post("/admin/import/chunk/records")
def admin_import_chunk_records(
    payload: RecordChunkPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """
    Recibe un lote de records (hasta 20.000) y los inserta de forma masiva en base de datos.
    """
    run = db.query(DimensionamientoImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        mappings = []
        for r in payload.records:
            fecha_val = _parse_date(r.fecha)
            if not fecha_val:
                raise ValueError(f"Invalid date format: {r.fecha}")
            mappings.append({
                "id_registro_unico": r.id_registro_unico,
                "fecha": fecha_val,
                "plataforma": r.plataforma,
                "cliente_nombre_homologado": r.cliente_nombre_homologado,
                "cliente_nombre_original": r.cliente_nombre_original,
                "cliente_visible": r.cliente_visible,
                "cuit": r.cuit,
                "provincia": r.provincia,
                "cuenta_interna": r.cuenta_interna,
                "codigo_articulo": r.codigo_articulo,
                "descripcion": r.descripcion,
                "clasificacion_suizo": r.clasificacion_suizo,
                "descripcion_articulo": r.descripcion_articulo,
                "familia": r.familia,
                "unidad_negocio": r.unidad_negocio,
                "subunidad_negocio": r.subunidad_negocio,
                "cantidad_demandada": r.cantidad_demandada,
                "valorizacion_estimada": r.valorizacion_estimada or 0.0,
                "resultado_participacion": r.resultado_participacion,
                "producto_nombre_original": r.producto_nombre_original,
                "fecha_procesamiento": _parse_datetime(r.fecha_procesamiento),
                "is_identified": r.is_identified,
                "is_client": r.is_client,
                "import_run_id": payload.import_run_id,
            })
        
        # Idempotente: un reintento tras un timeout de red (donde el servidor
        # ya habia insertado el lote) no debe romper con UniqueViolation. Se
        # mantiene executemany (no .values()) para no exceder el limite de
        # parametros de PostgreSQL con lotes grandes.
        if IS_SQLITE:
            stmt = sqlite_insert(DimensionamientoRecord).on_conflict_do_nothing()
        else:
            stmt = pg_insert(DimensionamientoRecord).on_conflict_do_nothing(
                constraint="uq_dim_records_id_run"
            )
        db.execute(stmt, mappings)

        run.rows_processed += len(mappings)
        run.rows_inserted += len(mappings)
        db.commit()
        
        logger.info("[DIM][IMPORT] Run %d: inserted %d records (total: %d)", 
                    run.id, len(mappings), run.rows_processed)
        return {"ok": True, "count": len(mappings)}
    except Exception as e:
        db.rollback()
        logger.exception("[DIM][IMPORT] Error inserting records chunk")
        raise HTTPException(status_code=500, detail=f"Error inserting records chunk: {e}")


@router.post("/admin/import/chunk/summaries")
def admin_import_chunk_summaries(
    payload: SummaryChunkPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """
    Recibe un lote de resúmenes mensuales y los inserta de forma masiva en base de datos.
    """
    run = db.query(DimensionamientoImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        mappings = []
        for s in payload.summaries:
            month_val = _parse_date(s.month)
            if not month_val:
                raise ValueError(f"Invalid month format: {s.month}")
            mappings.append({
                "month": month_val,
                "plataforma": s.plataforma,
                "cliente_nombre_homologado": s.cliente_nombre_homologado,
                "cliente_visible": s.cliente_visible,
                "provincia": s.provincia,
                "familia": s.familia,
                "unidad_negocio": s.unidad_negocio,
                "subunidad_negocio": s.subunidad_negocio,
                "resultado_participacion": s.resultado_participacion,
                "is_identified": s.is_identified,
                "is_client": s.is_client,
                "total_cantidad": s.total_cantidad,
                "total_valorizacion": s.total_valorizacion,
                "total_registros": s.total_registros,
                "clientes_unicos": s.clientes_unicos,
                "import_run_id": payload.import_run_id,
            })
        
        # Idempotente (ver chunk/records): reintentos tras timeout no deben fallar.
        if IS_SQLITE:
            stmt = sqlite_insert(DimensionamientoFamilyMonthlySummary).on_conflict_do_nothing()
        else:
            stmt = pg_insert(DimensionamientoFamilyMonthlySummary).on_conflict_do_nothing(
                constraint="uq_dim_family_monthly_summary"
            )
        db.execute(stmt, mappings)
        db.commit()
        
        logger.info("[DIM][IMPORT] Run %d: inserted %d summaries", run.id, len(mappings))
        return {"ok": True, "count": len(mappings)}
    except Exception as e:
        db.rollback()
        logger.exception("[DIM][IMPORT] Error inserting summaries chunk")
        raise HTTPException(status_code=500, detail=f"Error inserting summaries chunk: {e}")


@router.post("/admin/import/finalize")
def admin_import_finalize(
    payload: FinalizePayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """
    Finaliza la importación, guarda el snapshot precalculado localmente y activa
    la corrida al poner su estado en 'success'. Además, limpia los cachés del servidor.
    """
    run = db.query(DimensionamientoImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        if payload.snapshot:
            snap = db.query(DimensionamientoDashboardSnapshot).filter_by(
                snapshot_key=DEFAULT_DASHBOARD_SNAPSHOT_KEY,
                import_run_id=payload.import_run_id
            ).first()
            if not snap:
                snap = DimensionamientoDashboardSnapshot(
                    snapshot_key=DEFAULT_DASHBOARD_SNAPSHOT_KEY,
                    import_run_id=payload.import_run_id,
                    payload=payload.snapshot,
                    generated_at=dt.datetime.utcnow()
                )
                db.add(snap)
            else:
                snap.payload = payload.snapshot
                snap.generated_at = dt.datetime.utcnow()
        
        if payload.summary_metadata:
            run_sum = dict(run.summary or {})
            run_sum.update(payload.summary_metadata)
            run.summary = run_sum
            
        run.status = "success"
        run.finished_at = dt.datetime.utcnow()
        db.commit()
        
        invalidate_query_cache()
        logger.info("[DIM][IMPORT] Finalized run_id=%d successfully.", run.id)
        return {"ok": True, "message": "Import run finalized successfully."}
    except Exception as e:
        db.rollback()
        logger.exception("[DIM][IMPORT] Error finalizing import run")
        raise HTTPException(status_code=500, detail=f"Error finalizing import run: {e}")


@router.post("/admin/import/rollback")
def admin_import_rollback(
    payload: RollbackPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """
    Cancela la corrida actual y la marca como 'failed'.
    """
    run = db.query(DimensionamientoImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    
    try:
        run.status = "failed"
        if payload.error_message:
            run.error_message = payload.error_message
        run.finished_at = dt.datetime.utcnow()
        db.commit()
        
        invalidate_query_cache()
        logger.warning("[DIM][IMPORT] Rolled back run_id=%d (marked as failed).", run.id)
        return {"ok": True, "message": f"Run {run.id} marked as failed."}
    except Exception as e:
        db.rollback()
        logger.exception("[DIM][IMPORT] Error rolling back import run")
        raise HTTPException(status_code=500, detail=f"Error rolling back: {e}")


@router.post("/admin/import/cleanup")
def admin_import_cleanup(
    payload: CleanupPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """
    Elimina físicamente los registros y resúmenes de corridas viejas o fallidas
    para liberar espacio de base de datos en producción.
    """
    try:
        success_runs = db.query(DimensionamientoImportRun).filter_by(status="success").order_by(
            DimensionamientoImportRun.finished_at.desc(),
            DimensionamientoImportRun.id.desc()
        ).all()
        
        keep_ids = set()
        for r in success_runs[:payload.keep_runs]:
            keep_ids.add(r.id)
            
        running_runs = db.query(DimensionamientoImportRun).filter_by(status="running").all()
        for r in running_runs:
            keep_ids.add(r.id)
            
        latest_success = db.query(DimensionamientoImportRun).filter_by(status="success").order_by(
            DimensionamientoImportRun.finished_at.desc(),
            DimensionamientoImportRun.id.desc()
        ).first()
        if latest_success:
            keep_ids.add(latest_success.id)
            
        if not keep_ids:
            return {"ok": True, "deleted_runs_count": 0, "message": "No runs to protect, skipping cleanup."}
            
        runs_to_delete = db.query(DimensionamientoImportRun).filter(
            ~DimensionamientoImportRun.id.in_(list(keep_ids))
        ).all()
        
        delete_ids = [r.id for r in runs_to_delete]
        if not delete_ids:
            return {"ok": True, "deleted_runs_count": 0, "message": "No runs to clean up."}
            
        logger.info("[DIM][IMPORT] Cleaning up runs: %s", delete_ids)
        
        db.query(DimensionamientoRecord).filter(DimensionamientoRecord.import_run_id.in_(delete_ids)).delete(synchronize_session=False)
        db.query(DimensionamientoFamilyMonthlySummary).filter(DimensionamientoFamilyMonthlySummary.import_run_id.in_(delete_ids)).delete(synchronize_session=False)
        db.query(DimensionamientoDashboardSnapshot).filter(DimensionamientoDashboardSnapshot.import_run_id.in_(delete_ids)).delete(synchronize_session=False)
        db.query(DimensionamientoImportError).filter(DimensionamientoImportError.import_run_id.in_(delete_ids)).delete(synchronize_session=False)
        db.query(DimensionamientoImportRun).filter(DimensionamientoImportRun.id.in_(delete_ids)).delete(synchronize_session=False)
        
        db.commit()
        logger.info("[DIM][IMPORT] Cleanup completed successfully. Deleted runs: %d", len(delete_ids))
        return {
            "ok": True,
            "deleted_runs_count": len(delete_ids),
            "deleted_run_ids": delete_ids,
            "message": f"Successfully deleted {len(delete_ids)} old runs and their records.",
        }
    except Exception as e:
        db.rollback()
        logger.exception("[DIM][IMPORT] Error performing cleanup")
        raise HTTPException(status_code=500, detail=f"Error performing cleanup: {e}")


@router.get("/admin/import/verify")
def admin_import_verify(
    run_id: int | None = Query(default=None),
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """
    Verificación read-only (protegida por import token) para validar la carga en
    producción sin necesidad de sesión de usuario. Devuelve conteos y totales del
    run indicado (o del último 'success' si no se especifica).
    """
    latest_success = db.query(DimensionamientoImportRun).filter_by(status="success").order_by(
        DimensionamientoImportRun.finished_at.desc(),
        DimensionamientoImportRun.id.desc(),
    ).first()

    if run_id is not None:
        run = db.query(DimensionamientoImportRun).filter_by(id=run_id).first()
    else:
        run = latest_success

    if not run:
        raise HTTPException(status_code=404, detail="No import run found")

    rid = run.id
    rec_q = db.query(DimensionamientoRecord).filter_by(import_run_id=rid)
    records = rec_q.with_entities(func.count(DimensionamientoRecord.id)).scalar() or 0
    summaries = db.query(func.count(DimensionamientoFamilyMonthlySummary.id)).filter_by(import_run_id=rid).scalar() or 0
    total_val = db.query(func.coalesce(func.sum(DimensionamientoRecord.valorizacion_estimada), 0)).filter_by(import_run_id=rid).scalar() or 0
    total_cant = db.query(func.coalesce(func.sum(DimensionamientoRecord.cantidad_demandada), 0)).filter_by(import_run_id=rid).scalar() or 0
    fecha_min = db.query(func.min(DimensionamientoRecord.fecha)).filter_by(import_run_id=rid).scalar()
    fecha_max = db.query(func.max(DimensionamientoRecord.fecha)).filter_by(import_run_id=rid).scalar()
    snapshots = db.query(func.count(DimensionamientoDashboardSnapshot.id)).filter_by(import_run_id=rid).scalar() or 0
    plat_rows = (
        db.query(DimensionamientoRecord.plataforma, func.count(DimensionamientoRecord.id))
        .filter_by(import_run_id=rid)
        .group_by(DimensionamientoRecord.plataforma)
        .all()
    )

    return {
        "ok": True,
        "data": {
            "run_id": rid,
            "run_status": run.status,
            "is_latest_success": bool(latest_success and latest_success.id == rid),
            "records": int(records),
            "summaries": int(summaries),
            "total_valorizacion": float(total_val),
            "total_cantidad": float(total_cant),
            "fecha_min": str(fecha_min) if fecha_min else None,
            "fecha_max": str(fecha_max) if fecha_max else None,
            "snapshots": int(snapshots),
            "platforms": {p: int(c) for p, c in plat_rows},
        },
    }

