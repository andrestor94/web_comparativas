import datetime as dt
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from web_comparativas.auth import require_roles
from web_comparativas.dimensionamiento.query_service import (
    build_filters,
    get_clients_by_result,
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
    except Exception:
        pass
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
    return {"ok": True, "data": get_status(db)}


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
    return {"ok": True, "data": get_filter_options(db, filters)}


@router.get("/kpis")
def dimensionamiento_kpis(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return {"ok": True, "data": get_kpis(db, filters)}


@router.get("/series")
def dimensionamiento_series(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return {"ok": True, "data": get_series(db, filters)}


@router.get("/results")
def dimensionamiento_results(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return {"ok": True, "data": get_results_breakdown(db, filters)}


@router.get("/top-families")
def dimensionamiento_top_families(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=50),
):
    return {"ok": True, "data": get_top_families(db, filters, limit=limit)}


@router.get("/geo")
def dimensionamiento_geo(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
):
    return {"ok": True, "data": get_geography_distribution(db, filters)}


@router.get("/clients-by-result")
def dimensionamiento_clients_by_result(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=30),
):
    return {"ok": True, "data": get_clients_by_result(db, filters, limit=limit)}


@router.get("/family-consumption")
def dimensionamiento_family_consumption(
    _: AllowedUser,
    filters=Depends(_filters_from_query),
    db: Session = Depends(get_db),
    limit: int = Query(default=20, ge=1, le=50),
):
    return {"ok": True, "data": get_family_consumption_table(db, filters, limit=limit)}


@router.post("/process")
def deprecated_manual_process(_: AllowedUser):
    raise HTTPException(
        status_code=410,
        detail=(
            "La carga manual fue removida. Actualice el CSV unificado y ejecute la ingesta backend "
            "para refrescar el módulo Dimensionamiento."
        ),
    )
