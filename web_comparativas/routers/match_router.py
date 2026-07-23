"""API del módulo Match (Mercado Privado) — homologación asistida.

Sirve el maestro de artículos y el detalle de candidatas SIEMPRE paginado desde la
tabla compacta `match_propuestas` (corrida vigente). Persiste decisiones de
homologación en `match_homologaciones` (upsert last-wins) + bitácora
`match_homologacion_eventos`.

Seguridad:
  - Acceso gobernado por `require_perm("mercado_privado.match")` (mismo sistema de
    permisos declarativo que el resto de Mercado Privado).
  - Kill-switch `MATCH_ENABLED`: si está OFF, la API responde 404.
  - El `usuario` de cada decisión se SELLA server-side desde la sesión; NUNCA del body.
"""
from __future__ import annotations

import datetime as dt
import logging

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from web_comparativas.match import MATCH_ENABLED
from web_comparativas.match.models import (
    DECISION_DESCARTADO,
    DECISION_HOMOLOGADO,
    DECISION_REVERTIDO,
    EVENTO_PAPELERA_ELIMINADO,
    MatchHomologacion,
    MatchHomologacionEvento,
)
from web_comparativas.match.service import (
    DEFAULT_PAGE_SIZE,
    detalle_articulo,
    exportar_reporte_bytes,
    latest_approved_run,
    listar_articulos,
    match_desempeno,
    match_negocios,
    match_papelera,
    match_resumen,
)
from web_comparativas.policy import require_perm

router = APIRouter(prefix="/api/mercado-privado/match", tags=["match"])
logger = logging.getLogger("wc.match.api")

# Misma key de permiso que la pestaña del sidebar (gobierna acceso vía can_access).
_perm_match = require_perm("mercado_privado.match")
AllowedUser = Depends(_perm_match)

# Gate de ESCRITURA: regla vigente del proyecto — en visualización Gerente se iguala a
# Auditor, pero en escritura Gerente queda AFUERA (igual que auditor/visor, solo lectura).
# Escriben decisiones: admin, analista y supervisor.
_WRITE_ROLES = {"admin", "administrator", "administrador", "analista", "analyst", "supervisor"}


def _require_match_write(request: Request):
    user = _perm_match(request)
    role = (getattr(user, "role", "") or "").strip().lower()
    if role not in _WRITE_ROLES:
        raise HTTPException(
            status_code=403,
            detail="Solo lectura: tu rol no puede registrar decisiones de Match.",
        )
    return user


AllowedWriter = Depends(_require_match_write)


def get_db(request: Request) -> Session:
    db = getattr(request.state, "db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="No hay sesión de base de datos disponible.")
    return db


def _require_enabled() -> None:
    if not MATCH_ENABLED():
        raise HTTPException(status_code=404, detail="Módulo Match deshabilitado.")


def _sello_usuario(user) -> str:
    """Sello server-side del usuario logueado. NUNCA se toma del body."""
    usuario = getattr(user, "email", None) or getattr(user, "username", None)
    if not usuario:
        raise HTTPException(status_code=401, detail="Usuario sin identidad en la sesión.")
    return usuario


# ──────────────────────────────────────────────────────────────────────────────
# Lecturas (paginadas)
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/articulos")
def match_articulos(
    _user=AllowedUser,
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=200),
    q: str | None = Query(default=None),
    nivel: str | None = Query(default=None),
    negocio: str | None = Query(default=None),
    subnegocio: str | None = Query(default=None),
):
    """Maestro de artículos Suizo (paginado) de la corrida vigente."""
    _require_enabled()
    data = listar_articulos(
        db, page=page, page_size=page_size, q=q, nivel=nivel,
        negocio=negocio, subnegocio=subnegocio,
    )
    return {"ok": True, "data": data}


@router.get("/negocios")
def match_negocios_endpoint(
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Árbol {negocio: [subnegocios]} para los desplegables en cascada."""
    _require_enabled()
    return {"ok": True, "data": match_negocios(db)}


@router.get("/articulo")
def match_articulo_detalle(
    _user=AllowedUser,
    db: Session = Depends(get_db),
    codigo: str = Query(..., min_length=1, description="candidato_codigo (código Suizo)"),
):
    """Detalle de un artículo: sus descripciones de portal con estado de homologación."""
    _require_enabled()
    data = detalle_articulo(db, candidato_codigo=codigo.strip())
    return {"ok": True, "data": data}


@router.get("/resumen")
def match_resumen_endpoint(
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Resumen AGREGADO de la corrida vigente (encabezado del módulo)."""
    _require_enabled()
    data = match_resumen(db)
    return {"ok": True, "data": data}


@router.get("/exportar")
def match_exportar(
    user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Descarga un .xlsx (una hoja) con TODAS las propuestas de la corrida vigente:
    mismas columnas que el Excel de entrada + 'homologado'/'descartado' al final.
    SOLO LECTURA: genera el archivo en memoria, no escribe en app.db (corre con el server
    vivo). Identidad del usuario tomada server-side (auditoría)."""
    _require_enabled()
    usuario = _sello_usuario(user)
    bio, rid, filas = exportar_reporte_bytes(db)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M")
    fname = f"match_reporte_{ts}.xlsx"
    logger.info("[MATCH][API] exportar run=%s filas=%s por=%s", rid, filas, usuario)
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/desempeno")
def match_desempeno_endpoint(
    user=AllowedUser,
    db: Session = Depends(get_db),
    limite: int = Query(default=8, ge=1, le=50),
):
    """Tablero de DESEMPEÑO de la corrida vigente (datos reales): posición del
    usuario, homologadas/hoy/semana, avance del equipo y ranking por conteo.
    Identidad del usuario tomada server-side, nunca del body."""
    _require_enabled()
    actual = getattr(user, "email", None) or getattr(user, "username", None)
    data = match_desempeno(db, usuario=actual, limite=limite)
    return {"ok": True, "data": data}


# ──────────────────────────────────────────────────────────────────────────────
# Decisiones (upsert last-wins + bitácora). Usuario sellado server-side.
# ──────────────────────────────────────────────────────────────────────────────

class HomologarBody(BaseModel):
    producto_plataforma: str = Field(..., min_length=1)
    codigo_elegido: str | None = None       # normalmente = candidato_codigo, editable
    descripcion_elegida: str | None = None
    # NB: NO existe campo `usuario` a propósito: se sella server-side desde la sesión.


class DescartarBody(BaseModel):
    producto_plataforma: str = Field(..., min_length=1)
    codigo_elegido: str | None = None       # opcional (qué candidato se rechazó)


def _upsert_homologacion(
    db: Session,
    *,
    producto_plataforma: str,
    decision: str,
    usuario: str,
    codigo_elegido: str | None,
    descripcion_elegida: str | None,
    run_id: int | None,
) -> MatchHomologacion:
    """Upsert last-wins por `producto_plataforma` (portable SQLite/PG) + evento append-only.

    Idempotente: reenviar la misma decisión actualiza la fila vigente (updated_at) y deja
    otro registro en la bitácora, sin romper nada.
    """
    now = dt.datetime.utcnow()
    existente = db.execute(
        select(MatchHomologacion).where(
            MatchHomologacion.producto_plataforma == producto_plataforma
        )
    ).scalar_one_or_none()

    if existente is None:
        existente = MatchHomologacion(
            producto_plataforma=producto_plataforma,
            codigo_elegido=codigo_elegido,
            descripcion_elegida=descripcion_elegida,
            decision=decision,
            usuario=usuario,
            import_run_id=run_id,
            created_at=now,
            updated_at=now,
        )
        db.add(existente)
    else:
        existente.codigo_elegido = codigo_elegido
        existente.descripcion_elegida = descripcion_elegida
        existente.decision = decision
        existente.usuario = usuario
        existente.import_run_id = run_id
        existente.updated_at = now

    db.add(MatchHomologacionEvento(
        producto_plataforma=producto_plataforma,
        decision=decision,
        codigo_elegido=codigo_elegido,
        usuario=usuario,
        created_at=now,
    ))
    db.commit()
    db.refresh(existente)
    return existente


def _run_id_vigente(db: Session) -> int | None:
    latest = latest_approved_run(db)
    return latest.id if latest else None


@router.post("/homologar")
def match_homologar(
    request: Request,
    user=AllowedWriter,
    db: Session = Depends(get_db),
    body: HomologarBody = Body(...),
):
    """Marca una descripción de portal como HOMOLOGADA con el código elegido.
    Usuario sellado server-side. Upsert last-wins + bitácora. Idempotente."""
    _require_enabled()
    usuario = _sello_usuario(user)
    producto = body.producto_plataforma.strip()
    if not producto:
        raise HTTPException(status_code=422, detail="producto_plataforma vacío.")

    row = _upsert_homologacion(
        db,
        producto_plataforma=producto,
        decision=DECISION_HOMOLOGADO,
        usuario=usuario,
        codigo_elegido=(body.codigo_elegido or "").strip() or None,
        descripcion_elegida=body.descripcion_elegida,
        run_id=_run_id_vigente(db),
    )
    logger.info("[MATCH][API] homologar producto=%r por=%s codigo=%s",
                producto[:80], usuario, row.codigo_elegido)
    return {
        "ok": True,
        "decision": row.decision,
        "producto_plataforma": row.producto_plataforma,
        "codigo_elegido": row.codigo_elegido,
        "usuario": row.usuario,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.post("/descartar")
def match_descartar(
    request: Request,
    user=AllowedWriter,
    db: Session = Depends(get_db),
    body: DescartarBody = Body(...),
):
    """Marca una descripción de portal como DESCARTADA (sin match válido).
    Usuario sellado server-side. Upsert last-wins + bitácora. Idempotente."""
    _require_enabled()
    usuario = _sello_usuario(user)
    producto = body.producto_plataforma.strip()
    if not producto:
        raise HTTPException(status_code=422, detail="producto_plataforma vacío.")

    row = _upsert_homologacion(
        db,
        producto_plataforma=producto,
        decision=DECISION_DESCARTADO,
        usuario=usuario,
        codigo_elegido=(body.codigo_elegido or "").strip() or None,
        descripcion_elegida=None,
        run_id=_run_id_vigente(db),
    )
    logger.info("[MATCH][API] descartar producto=%r por=%s", producto[:80], usuario)
    return {
        "ok": True,
        "decision": row.decision,
        "producto_plataforma": row.producto_plataforma,
        "usuario": row.usuario,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


class RevertirBody(BaseModel):
    producto_plataforma: str = Field(..., min_length=1)


@router.post("/revertir")
def match_revertir(
    user=AllowedWriter,
    db: Session = Depends(get_db),
    body: RevertirBody = Body(...),
):
    """Revierte la decisión vigente de una descripción de portal: ELIMINA la fila de
    `match_homologaciones` (sea 'homologado' o 'descartado') y deja un evento
    'revertido' en la bitácora. Idempotente: si ya no hay decisión, responde OK.
    Usuario sellado server-side."""
    _require_enabled()
    usuario = _sello_usuario(user)
    producto = body.producto_plataforma.strip()
    if not producto:
        raise HTTPException(status_code=422, detail="producto_plataforma vacío.")

    existente = db.execute(
        select(MatchHomologacion).where(
            MatchHomologacion.producto_plataforma == producto
        )
    ).scalar_one_or_none()

    decision_previa = existente.decision if existente is not None else None
    revertido = False
    if existente is not None:
        db.execute(
            delete(MatchHomologacion).where(
                MatchHomologacion.producto_plataforma == producto
            )
        )
        db.add(MatchHomologacionEvento(
            producto_plataforma=producto,
            decision=DECISION_REVERTIDO,
            codigo_elegido=existente.codigo_elegido,
            usuario=usuario,
            created_at=dt.datetime.utcnow(),
        ))
        revertido = True
    db.commit()

    logger.info("[MATCH][API] revertir producto=%r por=%s previa=%s revertido=%s",
                producto[:80], usuario, decision_previa, revertido)
    return {
        "ok": True,
        "revertido": revertido,
        "decision_previa": decision_previa,
        "producto_plataforma": producto,
        "usuario": usuario,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Papelera de descartados (recuperable 24h). Calculada, sin esquema nuevo.
# ──────────────────────────────────────────────────────────────────────────────

class PapeleraEliminarBody(BaseModel):
    producto_plataforma: str = Field(..., min_length=1)


@router.get("/papelera")
def match_papelera_endpoint(
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Descartados en papelera (últimas 24h, sin 'papelera_eliminado')."""
    _require_enabled()
    return {"ok": True, "data": match_papelera(db)}


def _marcar_eliminado(db: Session, producto: str, usuario: str) -> bool:
    """Registra evento 'papelera_eliminado' si el producto está hoy en papelera y aún
    no fue eliminado. Idempotente: si ya no figura en papelera, no agrega nada."""
    en_papelera = {it["producto_plataforma"] for it in match_papelera(db)["items"]}
    if producto not in en_papelera:
        return False
    db.add(MatchHomologacionEvento(
        producto_plataforma=producto,
        decision=EVENTO_PAPELERA_ELIMINADO,
        usuario=usuario,
        created_at=dt.datetime.utcnow(),
    ))
    return True


@router.post("/papelera/eliminar")
def match_papelera_eliminar(
    user=AllowedWriter,
    db: Session = Depends(get_db),
    body: PapeleraEliminarBody = Body(...),
):
    """Elimina definitivamente de la papelera: deja evento 'papelera_eliminado'. La fila
    'descartado' QUEDA. Usuario server-side. Idempotente."""
    _require_enabled()
    usuario = _sello_usuario(user)
    producto = body.producto_plataforma.strip()
    if not producto:
        raise HTTPException(status_code=422, detail="producto_plataforma vacío.")
    eliminado = _marcar_eliminado(db, producto, usuario)
    db.commit()
    logger.info("[MATCH][API] papelera/eliminar producto=%r por=%s eliminado=%s",
                producto[:80], usuario, eliminado)
    return {"ok": True, "eliminado": eliminado, "producto_plataforma": producto}


@router.post("/papelera/vaciar")
def match_papelera_vaciar(
    user=AllowedWriter,
    db: Session = Depends(get_db),
):
    """Vacía la papelera: 'papelera_eliminado' para todo lo que hoy figura en ella.
    Usuario server-side. Idempotente."""
    _require_enabled()
    usuario = _sello_usuario(user)
    items = match_papelera(db)["items"]
    n = 0
    for it in items:
        if _marcar_eliminado(db, it["producto_plataforma"], usuario):
            n += 1
    db.commit()
    logger.info("[MATCH][API] papelera/vaciar por=%s eliminados=%s", usuario, n)
    return {"ok": True, "eliminados": n}


# ──────────────────────────────────────────────────────────────────────────────
# Push de datos desde local (patrón push_identity / import por chunks de
# Dimensionamiento): TODO se calcula/importa en la PC local y viaja como DATO
# por lotes chicos; acá SOLO se aplica y commitea por lote. Protegido por el
# MISMO token de import de Dimensionamiento (X-Import-Token). NO se gatea por
# MATCH_ENABLED: el push debe poder correr ANTES de encender el módulo.
# ──────────────────────────────────────────────────────────────────────────────

from typing import Any  # noqa: E402

from sqlalchemy import func, text  # noqa: E402
from sqlalchemy.dialects.postgresql import insert as pg_insert  # noqa: E402
from sqlalchemy.dialects.sqlite import insert as sqlite_insert  # noqa: E402

from web_comparativas.models import IS_SQLITE  # noqa: E402
from web_comparativas.match.models import (  # noqa: E402
    MATCH_RUN_APPROVED,
    MATCH_RUN_PENDING,
    MatchDemandaDesc,
    MatchImportRun,
    MatchNegocioMap,
    MatchPropuesta,
)
from web_comparativas.routers.dimensiones_router import verify_import_token  # noqa: E402


def _insert_ignore(model, constraint: str | None = None):
    """INSERT idempotente portable: reintentos tras timeout no rompen con UniqueViolation."""
    if IS_SQLITE:
        return sqlite_insert(model).on_conflict_do_nothing()
    if constraint:
        return pg_insert(model).on_conflict_do_nothing(constraint=constraint)
    return pg_insert(model).on_conflict_do_nothing()


@router.get("/admin/estado")
def match_admin_estado(
    _token: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Estado verificable por curl (runbook): corridas, conteos de propuestas de la
    corrida vigente y tamaño de las tablas precalculadas."""
    vigente = latest_approved_run(db)
    pendientes = db.execute(
        select(MatchImportRun.id, MatchImportRun.status, MatchImportRun.rows_inserted)
        .where(MatchImportRun.status == MATCH_RUN_PENDING)
        .order_by(MatchImportRun.id.desc()).limit(5)
    ).all()
    propuestas_vigente = 0
    if vigente is not None:
        propuestas_vigente = int(db.execute(
            select(func.count(MatchPropuesta.id))
            .where(MatchPropuesta.import_run_id == vigente.id)
        ).scalar_one() or 0)
    negocio_map = int(db.execute(select(func.count(MatchNegocioMap.codigo))).scalar_one() or 0)
    demanda_desc = int(db.execute(select(func.count(MatchDemandaDesc.desc_norm))).scalar_one() or 0)
    return {
        "ok": True,
        "match_enabled": MATCH_ENABLED(),
        "run_vigente": vigente.id if vigente else None,
        "run_vigente_filas": propuestas_vigente,
        "runs_pendientes": [{"id": r.id, "status": r.status, "rows": r.rows_inserted} for r in pendientes],
        "match_negocio_map": negocio_map,
        "match_demanda_desc": demanda_desc,
    }


@router.post("/admin/apply-data-chunk")
def match_admin_apply_data_chunk(
    payload: dict[str, Any] = Body(...),
    _token: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Aplica UN LOTE de datos de Match calculados en local. Síncrono, commit por lote,
    reanudable (inserts idempotentes por UNIQUE/PK). kind:
      - 'start'        : crea corrida pending_approval -> {run_id}. Con payload.resume_run
                         devuelve esa corrida sin crear una nueva (reanudación).
      - 'propuestas'   : payload.run_id + payload.rows [dicts de match_propuestas].
      - 'negocio-map'  : payload.rows [[codigo, negocio, subnegocio],...]. payload.reset
                         (solo en el 1er lote) vacía la tabla antes (es chica).
      - 'demanda-desc' : payload.rows [[desc_norm, renglones, clientes],...]. Ídem reset.
      - 'finalize'     : payload.run_id + counts -> cierra la corrida y la APRUEBA
                         (pasa a vigente). Devuelve el estado.
    """
    kind = payload.get("kind")
    try:
        if kind == "start":
            resume = payload.get("resume_run")
            if resume:
                run = db.get(MatchImportRun, int(resume))
                if run is None:
                    raise HTTPException(status_code=404, detail=f"resume_run {resume} no existe.")
                return {"ok": True, "kind": kind, "run_id": run.id, "resumed": True}
            run = MatchImportRun(
                source_path=str(payload.get("source_path") or "push:local"),
                status=MATCH_RUN_PENDING,
                started_at=dt.datetime.utcnow(),
            )
            db.add(run)
            db.commit()
            db.refresh(run)
            logger.info("[MATCH][PUSH] start run_id=%s", run.id)
            return {"ok": True, "kind": kind, "run_id": run.id, "resumed": False}

        elif kind == "propuestas":
            run_id = int(payload.get("run_id"))
            rows = payload.get("rows") or []
            now = dt.datetime.utcnow()
            mappings = [{
                "import_run_id": run_id,
                "producto_plataforma": r.get("producto_plataforma"),
                "nivel_confianza": r.get("nivel_confianza"),
                "score_mejor": r.get("score_mejor"),
                "candidato_codigo": r.get("candidato_codigo"),
                "candidato_descripcion": r.get("candidato_descripcion"),
                "score_tfidf": r.get("score_tfidf"),
                "score_fuzzy": r.get("score_fuzzy"),
                "score_pharma": r.get("score_pharma"),
                "created_at": now,
            } for r in rows if r.get("producto_plataforma")]
            if mappings:
                db.execute(_insert_ignore(MatchPropuesta, "uq_match_propuestas_run_prod_cand"), mappings)
            db.commit()
            total = int(db.execute(
                select(func.count(MatchPropuesta.id))
                .where(MatchPropuesta.import_run_id == run_id)
            ).scalar_one() or 0)
            return {"ok": True, "kind": kind, "run_id": run_id, "insertadas": len(mappings), "total_run": total}

        elif kind == "negocio-map":
            rows = payload.get("rows") or []
            if payload.get("reset"):
                borrados = db.execute(text("DELETE FROM match_negocio_map")).rowcount
                logger.info("[MATCH][PUSH] negocio-map reset (borrados=%s)", borrados)
            mappings = [{"codigo": str(c).strip(), "negocio": n, "subnegocio": s}
                        for c, n, s in rows if c is not None and str(c).strip()]
            if mappings:
                db.execute(_insert_ignore(MatchNegocioMap), mappings)
            db.commit()
            total = int(db.execute(select(func.count(MatchNegocioMap.codigo))).scalar_one() or 0)
            return {"ok": True, "kind": kind, "insertadas": len(mappings), "total": total}

        elif kind == "demanda-desc":
            rows = payload.get("rows") or []
            if payload.get("reset"):
                borrados = db.execute(text("DELETE FROM match_demanda_desc")).rowcount
                logger.info("[MATCH][PUSH] demanda-desc reset (borrados=%s)", borrados)
            mappings = [{"desc_norm": k, "renglones": int(r or 0), "clientes": int(c or 0)}
                        for k, r, c in rows if k]
            if mappings:
                db.execute(_insert_ignore(MatchDemandaDesc), mappings)
            db.commit()
            total = int(db.execute(select(func.count(MatchDemandaDesc.desc_norm))).scalar_one() or 0)
            return {"ok": True, "kind": kind, "insertadas": len(mappings), "total": total}

        elif kind == "finalize":
            run_id = int(payload.get("run_id"))
            run = db.get(MatchImportRun, run_id)
            if run is None:
                raise HTTPException(status_code=404, detail=f"run {run_id} no existe.")
            total = int(db.execute(
                select(func.count(MatchPropuesta.id))
                .where(MatchPropuesta.import_run_id == run_id)
            ).scalar_one() or 0)
            esperadas = payload.get("rows_esperadas")
            if esperadas is not None and int(esperadas) != total:
                raise HTTPException(
                    status_code=409,
                    detail=f"Conteo no coincide: prod tiene {total} filas y local esperaba {esperadas}. "
                           f"Reanudá el push (mismo comando con --resume-run {run_id}) antes de aprobar.",
                )
            run.rows_inserted = total
            run.articulos_distintos = int(db.execute(
                select(func.count(func.distinct(MatchPropuesta.candidato_codigo)))
                .where(MatchPropuesta.import_run_id == run_id)
            ).scalar_one() or 0)
            if payload.get("counts_by_nivel"):
                run.counts_by_nivel = payload["counts_by_nivel"]
            run.finished_at = dt.datetime.utcnow()
            run.status = MATCH_RUN_APPROVED
            run.approved_at = dt.datetime.utcnow()
            run.approved_by = "push:push_match_data"
            db.commit()
            logger.info("[MATCH][PUSH] finalize run=%s filas=%s (APPROVED)", run_id, total)
            return {"ok": True, "kind": kind, "run_id": run_id, "filas": total,
                    "articulos": run.articulos_distintos, "status": run.status}

        else:
            raise HTTPException(status_code=400, detail=f"kind inválido: {kind!r}")
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("[MATCH][PUSH] apply-data-chunk kind=%s FALLO", kind)
        raise HTTPException(status_code=500, detail=f"Error en lote {kind}: {exc}")
