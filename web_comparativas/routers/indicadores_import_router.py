"""Router de importación + aprobación del módulo Indicadores Comerciales.

Prefijo: /api/indicadores
Se registra SIEMPRE en producción (como dimensiones_router), protegido por token.
NO confundir con indicadores_router.py (router de consulta, local-only).

FASE 2a: verify_import_token + /start + chunk de rentabilidad (molde validado).
Las otras 3 rutas de chunk, finalize/rollback y las rutas de aprobación llegan
en fases siguientes.
"""

import datetime as dt
import json
import logging
import os
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Header, Request
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
from pydantic import BaseModel

from web_comparativas.models import IS_SQLITE, User
from web_comparativas.policy import require_module
from web_comparativas.indicadores_summary_models import (
    IndArticulos,
    IndImportRun,
    IndInflacionFacturacionMensual,
    IndInflacionPvpMensual,
    IndRentabilidadLineas,
)

router = APIRouter(prefix="/api/indicadores", tags=["indicadores-import"])
logger = logging.getLogger("wc.indicadores.import")


# ── Token (calca de dimensiones_router.verify_import_token, env propia) ──────────
def verify_import_token(x_import_token: str = Header(..., alias="X-Import-Token")) -> str:
    """Verifica el token de importación de Indicadores (prod o local)."""
    expected_token = os.getenv("INDICADORES_IMPORT_TOKEN")
    if not expected_token:
        if IS_SQLITE:
            expected_token = "local_dev_token"
        else:
            logger.error("[IND][IMPORT] INDICADORES_IMPORT_TOKEN no está configurada.")
            raise HTTPException(
                status_code=500,
                detail="El token de importación no está configurado en el servidor.",
            )
    if x_import_token != expected_token:
        logger.warning("[IND][IMPORT] X-Import-Token inválido.")
        raise HTTPException(status_code=403, detail="Token de importación inválido.")
    return x_import_token


# ── Sesión de DB (calca de dimensiones_router.get_db) ────────────────────────────
def get_db(request: Request) -> Session:
    db = getattr(request.state, "db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="No hay sesión de base de datos disponible.")
    return db


# ── Guard admin para aprobación (calca de forecast_router._require_admin) ────────
def _require_admin(request: Request, _user: User = Depends(require_module("indicadores_comerciales"))) -> User:
    """Doble capa: acceso al módulo (sesión) + rol admin, como los deletes de forecast."""
    if not _user.is_admin():
        raise HTTPException(status_code=403, detail="Solo administradores pueden aprobar o descartar corridas de Indicadores.")
    return _user


# ── Helpers de parseo ────────────────────────────────────────────────────────────
def _to_decimal(v: "str | None"):
    """money llega como string para no perder precisión en el JSON; a Decimal acá."""
    if v is None or v == "":
        return None
    return Decimal(v)


def _parse_dt(v: "str | None"):
    """fecha llega ISO 8601 (datetime completo, NO se baja a date)."""
    if v is None or v == "":
        return None
    s = v.strip()
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return dt.datetime.fromisoformat(s + "T00:00:00")


def _parse_date(v: "str | None"):
    """fecha_snapshot llega ISO 8601 date-only ('YYYY-MM-DD'); a date (NO datetime)."""
    if v is None or v == "":
        return None
    return dt.date.fromisoformat(v.strip())


# ── Payloads ─────────────────────────────────────────────────────────────────────
class IndImportStartPayload(BaseModel):
    nota: "str | None" = None


class RentabilidadLineaItem(BaseModel):
    line_seq: int                       # ordinal estable en la corrida -> idempotencia
    ctacte: "int | None" = None
    cliente_grupo: "int | None" = None
    nombre_cliente: "str | None" = None
    articulo: int
    cadneg: "str | None" = None
    fecha: "str | None" = None          # ISO 8601 datetime
    cant: "int | None" = None
    importe: "str | None" = None        # money -> Decimal server-side
    renta1: "str | None" = None         # money -> Decimal server-side
    comprob: "str | None" = None


class RentabilidadChunkPayload(BaseModel):
    import_run_id: int
    lineas: list[RentabilidadLineaItem]


class PvpItem(BaseModel):
    articulo: int
    mes: str                            # 'YYYY-MM'
    fecha_snapshot: "str | None" = None  # ISO 8601 date-only -> date
    pvp: "str | None" = None            # money -> Decimal server-side


class PvpChunkPayload(BaseModel):
    import_run_id: int
    lineas: list[PvpItem]


class FacturacionItem(BaseModel):
    articulo: int
    cadneg: str
    mes: str                            # 'YYYY-MM'
    unidades: "float | None" = None     # SUM(CAST(cant AS FLOAT)) — queda float
    facturacion: "str | None" = None    # money -> Decimal server-side


class FacturacionChunkPayload(BaseModel):
    import_run_id: int
    lineas: list[FacturacionItem]


class ArticuloItem(BaseModel):
    articulo: int
    marca: "str | None" = None
    descripcion: "str | None" = None
    laboratorio: "str | None" = None
    familia: "str | None" = None
    principio_activo: "str | None" = None
    unineg: "int | None" = None


class ArticulosChunkPayload(BaseModel):
    import_run_id: int
    lineas: list[ArticuloItem]


class FinalizePayload(BaseModel):
    import_run_id: int


class RollbackPayload(BaseModel):
    import_run_id: int
    error_message: "str | None" = None


class ApprovePayload(BaseModel):
    import_run_id: int


class DiscardPayload(BaseModel):
    import_run_id: int


# ── /start ───────────────────────────────────────────────────────────────────────
@router.post("/admin/import/start")
def admin_import_start(
    payload: IndImportStartPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Crea una corrida en estado 'running' y devuelve su id."""
    try:
        run = IndImportRun(status="running", nota=payload.nota)
        db.add(run)
        db.commit()
        db.refresh(run)
        logger.info("[IND][IMPORT] Started run_id=%d", run.id)
        return {"ok": True, "import_run_id": run.id}
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error starting import run")
        raise HTTPException(status_code=500, detail=f"Error starting import run: {e}")


# ── /chunk/rentabilidad (MOLDE) ──────────────────────────────────────────────────
@router.post("/admin/import/chunk/rentabilidad")
def admin_import_chunk_rentabilidad(
    payload: RentabilidadChunkPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Inserta un lote de líneas de rentabilidad etiquetadas con la corrida.

    Idempotente por (import_run_id, line_seq): un reintento tras timeout no duplica.
    NOTA: IndImportRun no tiene contadores de filas (a diferencia de Dimensionamiento),
    así que NO se actualiza ningún contador acá; el conteo se calcula en /finalize.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        mappings = []
        for ln in payload.lineas:
            mappings.append({
                "ctacte": ln.ctacte,
                "cliente_grupo": ln.cliente_grupo,
                "nombre_cliente": ln.nombre_cliente,
                "articulo": ln.articulo,
                "cadneg": ln.cadneg,
                "fecha": _parse_dt(ln.fecha),
                "cant": ln.cant,
                "importe": _to_decimal(ln.importe),
                "renta1": _to_decimal(ln.renta1),
                "comprob": ln.comprob,
                "import_run_id": payload.import_run_id,
                "line_seq": ln.line_seq,
            })

        # executemany (no .values()) para no exceder el límite de parámetros de PG.
        if IS_SQLITE:
            stmt = sqlite_insert(IndRentabilidadLineas).on_conflict_do_nothing()
        else:
            stmt = pg_insert(IndRentabilidadLineas).on_conflict_do_nothing(
                constraint="uq_ind_rentab_run_seq"
            )
        db.execute(stmt, mappings)
        db.commit()

        logger.info("[IND][IMPORT] Run %d: rentabilidad chunk %d filas", run.id, len(mappings))
        return {"ok": True, "count": len(mappings)}
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error inserting rentabilidad chunk")
        raise HTTPException(status_code=500, detail=f"Error inserting rentabilidad chunk: {e}")


# ── /chunk/pvp ───────────────────────────────────────────────────────────────────
@router.post("/admin/import/chunk/pvp")
def admin_import_chunk_pvp(
    payload: PvpChunkPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Inserta un lote de PVP articulo×mes etiquetado con la corrida.

    Idempotente por (articulo, mes, import_run_id): un reintento tras timeout no duplica.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        mappings = []
        for ln in payload.lineas:
            mappings.append({
                "articulo": ln.articulo,
                "mes": ln.mes,
                "fecha_snapshot": _parse_date(ln.fecha_snapshot),
                "pvp": _to_decimal(ln.pvp),
                "import_run_id": payload.import_run_id,
            })

        # executemany (no .values()) para no exceder el límite de parámetros de PG.
        if IS_SQLITE:
            stmt = sqlite_insert(IndInflacionPvpMensual).on_conflict_do_nothing()
        else:
            stmt = pg_insert(IndInflacionPvpMensual).on_conflict_do_nothing(
                constraint="uq_ind_pvp_articulo_mes_run"
            )
        db.execute(stmt, mappings)
        db.commit()

        logger.info("[IND][IMPORT] Run %d: pvp chunk %d filas", run.id, len(mappings))
        return {"ok": True, "count": len(mappings)}
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error inserting pvp chunk")
        raise HTTPException(status_code=500, detail=f"Error inserting pvp chunk: {e}")


# ── /chunk/facturacion ───────────────────────────────────────────────────────────
@router.post("/admin/import/chunk/facturacion")
def admin_import_chunk_facturacion(
    payload: FacturacionChunkPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Inserta un lote de facturación articulo×cadneg×mes etiquetado con la corrida.

    Idempotente por (articulo, cadneg, mes, import_run_id): un reintento no duplica.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        mappings = []
        for ln in payload.lineas:
            mappings.append({
                "articulo": ln.articulo,
                "cadneg": ln.cadneg,
                "mes": ln.mes,
                "unidades": ln.unidades,
                "facturacion": _to_decimal(ln.facturacion),
                "import_run_id": payload.import_run_id,
            })

        # executemany (no .values()) para no exceder el límite de parámetros de PG.
        if IS_SQLITE:
            stmt = sqlite_insert(IndInflacionFacturacionMensual).on_conflict_do_nothing()
        else:
            stmt = pg_insert(IndInflacionFacturacionMensual).on_conflict_do_nothing(
                constraint="uq_ind_fact_articulo_cadneg_mes_run"
            )
        db.execute(stmt, mappings)
        db.commit()

        logger.info("[IND][IMPORT] Run %d: facturacion chunk %d filas", run.id, len(mappings))
        return {"ok": True, "count": len(mappings)}
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error inserting facturacion chunk")
        raise HTTPException(status_code=500, detail=f"Error inserting facturacion chunk: {e}")


# ── /chunk/articulos ─────────────────────────────────────────────────────────────
@router.post("/admin/import/chunk/articulos")
def admin_import_chunk_articulos(
    payload: ArticulosChunkPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Inserta un lote de la dimensión de artículos etiquetado con la corrida.

    Idempotente por (articulo, import_run_id): un reintento tras timeout no duplica.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        mappings = []
        for ln in payload.lineas:
            mappings.append({
                "articulo": ln.articulo,
                "marca": ln.marca,
                "descripcion": ln.descripcion,
                "laboratorio": ln.laboratorio,
                "familia": ln.familia,
                "principio_activo": ln.principio_activo,
                "unineg": ln.unineg,
                "import_run_id": payload.import_run_id,
            })

        # executemany (no .values()) para no exceder el límite de parámetros de PG.
        if IS_SQLITE:
            stmt = sqlite_insert(IndArticulos).on_conflict_do_nothing()
        else:
            stmt = pg_insert(IndArticulos).on_conflict_do_nothing(
                constraint="uq_ind_articulos_articulo_run"
            )
        db.execute(stmt, mappings)
        db.commit()

        logger.info("[IND][IMPORT] Run %d: articulos chunk %d filas", run.id, len(mappings))
        return {"ok": True, "count": len(mappings)}
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error inserting articulos chunk")
        raise HTTPException(status_code=500, detail=f"Error inserting articulos chunk: {e}")


# ── /finalize ────────────────────────────────────────────────────────────────────
@router.post("/admin/import/finalize")
def admin_import_finalize(
    payload: FinalizePayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Cierra la carga: la corrida queda en 'pending_approval', LISTA para aprobación.

    NO publica: la corrida approved activa sigue intacta y las lecturas no cambian.
    La publicación es el switch de aprobación (Fase 4). Acá solo se calculan los
    conteos por tabla de ESTA corrida (rows_por_tabla, JSON) para que el aprobador
    los compare contra la corrida activa antes de aprobar.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "running":
        raise HTTPException(status_code=400, detail=f"Import run is not running (status={run.status})")

    try:
        rows_por_tabla = {}
        for model in (IndRentabilidadLineas, IndInflacionPvpMensual,
                      IndInflacionFacturacionMensual, IndArticulos):
            rows_por_tabla[model.__tablename__] = (
                db.query(model).filter_by(import_run_id=run.id).count()
            )

        run.rows_por_tabla = json.dumps(rows_por_tabla)
        run.status = "pending_approval"
        run.finalized_at = dt.datetime.utcnow()
        db.commit()

        logger.info("[IND][IMPORT] Run %d finalized: %s", run.id, rows_por_tabla)

        # 5ª tabla summary (sparkline "Evolución 12M" del Home): se RECALCULA acá, en el
        # server, a partir de las tablas recién recibidas (pvp/facturación/artículos),
        # keyed por ESTA corrida. El upload por chunks transporta solo 4 tablas y el ETL
        # no corre en prod, así que sin este paso ind_inflacion_evolucion_mensual nunca se
        # puebla allá (sparkline "sin datos aún"). El servicio lo soporta pre-aprobación
        # (import_run_id fuerza la rama summary scoped a esa corrida). Best-effort:
        #   - usa su propia conexión (engine) sobre datos YA commiteados de esta corrida,
        #   - es idempotente (DELETE+INSERT de esta corrida, no duplica en reintentos),
        #   - si falla NO corta el finalize ni la aprobación (el Home degrada al fallback).
        evolucion_filas = None
        try:
            from web_comparativas.indicadores_inflacion_service import poblar_evolucion_precalc
            hoy = dt.date.today()
            desde = dt.date(hoy.year - 1, hoy.month, 1)  # últimos 12 meses, espeja Home/ETL
            evolucion_filas = poblar_evolucion_precalc(run.id, desde, hoy)
            logger.info("[IND][IMPORT] Run %d: evolución de inflación precalculada (%s filas)",
                        run.id, evolucion_filas)
        except Exception:
            logger.exception("[IND][IMPORT] Run %d: no se pudo precalcular la evolución de "
                             "inflación (no bloquea el finalize ni la aprobación)", run.id)

        return {
            "ok": True,
            "import_run_id": run.id,
            "status": "pending_approval",
            "rows_por_tabla": rows_por_tabla,
            "evolucion_filas": evolucion_filas,
        }
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error finalizing import run")
        raise HTTPException(status_code=500, detail=f"Error finalizing import run: {e}")


# ── /rollback ────────────────────────────────────────────────────────────────────
@router.post("/admin/import/rollback")
def admin_import_rollback(
    payload: RollbackPayload,
    _: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Marca la corrida como 'failed' (abortada por el cliente de carga).

    NO borra las filas ya cargadas: quedan huérfanas de una corrida failed y las
    limpia el cleanup de retención más adelante (igual que Dimensionamiento). Las
    lecturas nunca las ven porque filtran por la corrida approved.
    Si vino error_message se CONCATENA a run.nota (separador ' | ERROR: ') para no
    pisar la nota original de /start.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status not in ("running", "pending_approval"):
        raise HTTPException(
            status_code=400,
            detail=f"Import run cannot be rolled back (status={run.status})",
        )

    try:
        run.status = "failed"
        run.finalized_at = dt.datetime.utcnow()
        if payload.error_message:
            run.nota = (
                f"{run.nota} | ERROR: {payload.error_message}"
                if run.nota else f"ERROR: {payload.error_message}"
            )
        db.commit()

        logger.info("[IND][IMPORT] Run %d rolled back (failed)", run.id)
        return {"ok": True, "import_run_id": run.id, "status": "failed"}
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error rolling back import run")
        raise HTTPException(status_code=500, detail=f"Error rolling back import run: {e}")


# ── Aprobación (guard admin de sesión, NO token) ─────────────────────────────────
_SUMMARY_MODELS = (IndRentabilidadLineas, IndInflacionPvpMensual,
                   IndInflacionFacturacionMensual, IndArticulos)


def _conteos_por_tabla(db: Session, run_id: int) -> dict:
    return {
        model.__tablename__: db.query(model).filter_by(import_run_id=run_id).count()
        for model in _SUMMARY_MODELS
    }


@router.get("/import/pending")
def admin_import_pending(
    _user: User = Depends(_require_admin),
    db: Session = Depends(get_db),
):
    """Corrida en 'pending_approval' (la más reciente) + conteos de la activa, para el modal.

    Solo lectura: no cambia ningún estado.
    """
    pending = (
        db.query(IndImportRun)
        .filter_by(status="pending_approval")
        .order_by(IndImportRun.id.desc())
        .first()
    )
    pending_dict = None
    if pending:
        try:
            rows_por_tabla = json.loads(pending.rows_por_tabla) if pending.rows_por_tabla else None
        except Exception:
            rows_por_tabla = None
        pending_dict = {
            "import_run_id": pending.id,
            "created_at": pending.created_at.isoformat() if pending.created_at else None,
            "finalized_at": pending.finalized_at.isoformat() if pending.finalized_at else None,
            "rows_por_tabla": rows_por_tabla,
            "nota": pending.nota,
        }

    activa = (
        db.query(IndImportRun)
        .filter_by(status="approved")
        .order_by(IndImportRun.approved_at.desc(), IndImportRun.id.desc())
        .first()
    )
    activa_dict = {
        "import_run_id": activa.id if activa else None,
        "rows_por_tabla": _conteos_por_tabla(db, activa.id) if activa else {},
    }

    return {"pending": pending_dict, "activa": activa_dict}


@router.post("/import/approve")
def admin_import_approve(
    payload: ApprovePayload,
    _user: User = Depends(_require_admin),
    db: Session = Depends(get_db),
):
    """Aprueba la corrida: pasa a 'approved' y se vuelve la ACTIVA para las lecturas.

    NO toca la corrida approved anterior (queda superseded): _corrida_activa hace el
    switch sola porque ordena por approved_at DESC.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "pending_approval":
        raise HTTPException(status_code=400, detail=f"Import run is not pending approval (status={run.status})")

    try:
        run.status = "approved"
        run.approved_at = dt.datetime.utcnow()
        run.approved_by = _user.email or str(_user.id)
        db.commit()

        logger.info("[IND][IMPORT] Run %d approved by %s", run.id, run.approved_by)
        return {"ok": True, "import_run_id": run.id, "status": "approved"}
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error approving import run")
        raise HTTPException(status_code=500, detail=f"Error approving import run: {e}")


@router.post("/import/discard")
def admin_import_discard(
    payload: DiscardPayload,
    _user: User = Depends(_require_admin),
    db: Session = Depends(get_db),
):
    """Descarta la corrida: BORRA sus filas de las 4 tablas y la marca 'discarded'.

    Diferencia con /rollback: discard SÍ borra, porque es una decisión deliberada
    del admin de tirar esa corrida; rollback (aborto del cliente de carga) deja las
    filas huérfanas para el cleanup de retención.
    """
    run = db.query(IndImportRun).filter_by(id=payload.import_run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    if run.status != "pending_approval":
        raise HTTPException(status_code=400, detail=f"Import run is not pending approval (status={run.status})")

    try:
        filas_borradas = 0
        for model in _SUMMARY_MODELS:
            filas_borradas += (
                db.query(model)
                .filter_by(import_run_id=run.id)
                .delete(synchronize_session=False)
            )

        run.status = "discarded"
        if run.finalized_at is None:
            run.finalized_at = dt.datetime.utcnow()
        db.commit()

        logger.info("[IND][IMPORT] Run %d discarded by %s (%d filas borradas)",
                    run.id, _user.email or str(_user.id), filas_borradas)
        return {
            "ok": True,
            "import_run_id": run.id,
            "status": "discarded",
            "filas_borradas": filas_borradas,
        }
    except Exception as e:
        db.rollback()
        logger.exception("[IND][IMPORT] Error discarding import run")
        raise HTTPException(status_code=500, detail=f"Error discarding import run: {e}")
