from __future__ import annotations

import datetime as dt
import os
import sys

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.dimensionamiento.models import (
    Base, DimensionamientoImportRun, DimensionamientoRecord, OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import rebuild_oportunidades_for_run
from web_comparativas.routers import oportunidades_router as router


@pytest.fixture()
def db(monkeypatch):
    monkeypatch.setenv("OPORTUNIDADES_ENABLED", "1")
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        DimensionamientoImportRun.__table__, DimensionamientoRecord.__table__,
        OportunidadSummary.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.add(DimensionamientoImportRun(id=1, source_path="x.csv", status="success"))
        session.commit()
        # 13 meses de NO_PARTICIPO para un mismo par cliente/artículo — alcanza para
        # que _detectar_ultimo_mes_completo encuentre un "último mes completo" real.
        rid = 0
        base = dt.date(2025, 1, 15)
        for m in range(13):
            fecha = dt.date(base.year + (base.month - 1 + m) // 12, (base.month - 1 + m) % 12 + 1, 15)
            for _ in range(5):
                rid += 1
                session.add(DimensionamientoRecord(
                    id_registro_unico=f"REG{rid}", fecha=fecha, plataforma="P",
                    cliente_visible="CLIENTE X", codigo_articulo="ART1",
                    resultado_participacion="NO_PARTICIPO", cantidad_demandada=10,
                    valorizacion_estimada=100, import_run_id=1,
                    cuenta_interna="AAA", cuit="20111111112",
                    is_identified=True, is_client=True,
                ))
        session.commit()
        yield session, engine


def _count_queries(engine, fn):
    queries = []

    def _before(conn, cursor, statement, parameters, context, executemany):
        queries.append(statement)

    event.listen(engine, "before_cursor_execute", _before)
    try:
        resultado = fn()
    finally:
        event.remove(engine, "before_cursor_execute", _before)
    return resultado, queries


def test_rebuild_persiste_oportunidades_ref_month(db):
    session, engine = db
    run = session.get(DimensionamientoImportRun, 1)
    assert run.oportunidades_ref_month is None

    result = rebuild_oportunidades_for_run(session, 1)
    assert result["status"] == "ok"
    assert result["stats"].get("ref_month") is not None

    session.refresh(run)
    assert run.oportunidades_ref_month is not None
    assert run.oportunidades_ref_month.isoformat() == result["stats"]["ref_month"]


def test_window_meta_con_ancla_poblada_no_consulta_la_base(db):
    """El punto entero del cambio: con la columna poblada, _window_meta no dispara
    NINGUNA query — antes agregaba sobre todo dimensionamiento_records del run."""
    session, engine = db
    rebuild_oportunidades_for_run(session, 1)
    run = session.get(DimensionamientoImportRun, 1)
    assert run.oportunidades_ref_month is not None

    meta, queries = _count_queries(engine, lambda: router._window_meta(session, run))
    assert queries == []
    assert meta["ref_month"] == run.oportunidades_ref_month.isoformat()
    assert meta["label"] is not None
    assert meta["window_meses"] == 12


def test_window_meta_con_ancla_null_cae_al_calculo_de_siempre_y_da_lo_mismo(db):
    """Fallback para runs de antes del cambio (columna NULL, sin backfill forzado):
    tiene que dar EXACTAMENTE el mismo resultado que con la columna poblada — la
    optimización no puede cambiar el valor, solo el costo de conseguirlo."""
    session, engine = db
    rebuild_oportunidades_for_run(session, 1)
    run = session.get(DimensionamientoImportRun, 1)
    meta_con_ancla = router._window_meta(session, run)

    run.oportunidades_ref_month = None
    session.commit()
    session.refresh(run)

    meta_fallback, queries = _count_queries(engine, lambda: router._window_meta(session, run))
    assert len(queries) >= 1  # sí vuelve a tocar la base — es justamente el camino viejo
    assert meta_fallback == meta_con_ancla


def test_run_sin_datos_no_rompe_el_rebuild_ni_deja_ancla_falsa(db):
    session, engine = db
    run_vacio = DimensionamientoImportRun(id=2, source_path="vacio.csv", status="success")
    session.add(run_vacio)
    session.commit()

    result = rebuild_oportunidades_for_run(session, 2)
    assert result["status"] == "ok"

    session.refresh(run_vacio)
    assert run_vacio.oportunidades_ref_month is None
