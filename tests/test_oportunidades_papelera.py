from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base
from web_comparativas.dimensionamiento.models import (
    CrmEnvio,
    DimensionamientoImportRun,
    OportunidadRechazo,
    OportunidadRechazoEvento,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id
from web_comparativas.routers import oportunidades_router as router


def make_opportunity(summary_id: int, run_id: int, client: str, article: str, amount: float):
    return OportunidadSummary(
        id=summary_id,
        import_run_id=run_id,
        codigo_articulo=article,
        cliente_visible=client,
        cuit="30123456789",
        cuenta_interna="100",
        provincia="Buenos Aires",
        producto_nombre="Producto " + article,
        familia="Familia",
        unidad_negocio="Medicamentos",
        subunidad_negocio="Genericos",
        plataforma="Portal",
        tipo_oportunidad="RECURRENTE",
        estado_actividad="ACTIVA",
        meses_demanda_cliente_12m=8,
        meses_no_participo_12m=4,
        ventana_meses=12,
        consumo_tipico_mensual=10,
        consumo_min_mensual=8,
        consumo_max_mensual=12,
        ultima_demanda=dt.date(2026, 8, 1),
        meses_desde_ultima_demanda=1,
        precio_unitario_estimado=amount / 10,
        monto_oportunidad=amount,
        efectividad=0.4,
        ganados=3,
        comprado_otra=1,
        en_espera=0,
        clientes_distintos=5,
        tipo_multiplicador=1,
        multiplicador_actividad=1,
        score=amount,
    )


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        engine,
        tables=[
            DimensionamientoImportRun.__table__,
            OportunidadSummary.__table__,
            CrmEnvio.__table__,
            OportunidadRechazo.__table__,
            OportunidadRechazoEvento.__table__,
        ],
    )
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.add(DimensionamientoImportRun(
            id=1,
            source_path="run-1.csv",
            status="success",
            finished_at=dt.datetime(2026, 9, 1, 12, 0),
            oportunidades_ref_month=dt.date(2026, 8, 1),
        ))
        session.add_all([
            make_opportunity(1, 1, "Cliente Uno", "A1", 1_000),
            make_opportunity(2, 1, "Cliente Dos", "A2", 2_000),
        ])
        session.commit()
        yield session


def make_client(db, monkeypatch, *, user=None):
    actor = user or SimpleNamespace(id=7, email="admin@suizo.com", role="admin")
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._perm_oportunidades] = lambda: actor
    app.dependency_overrides[router._require_oportunidades_write] = lambda: actor
    app.dependency_overrides[router.get_db] = lambda: db
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "OPORTUNIDADES_CARTERA_ENABLED", lambda: False)
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "simulado")
    monkeypatch.setattr(router, "_contexto_asignacion_seguro", lambda *_args: {
        "match": None,
        "usuarios": [],
        "error": None,
        "puede_elegir": True,
        "email": actor.email,
    })
    return TestClient(app)


def test_browser_trash_url_is_registered_and_returns_http_200(db, monkeypatch):
    exact_url = "/api/mercado-privado/oportunidades/papelera"
    registered = [
        route
        for route in router.router.routes
        if getattr(route, "path", None) == exact_url
        and "GET" in (getattr(route, "methods", set()) or set())
    ]
    assert len(registered) == 1

    route_paths = [getattr(route, "path", "") for route in router.router.routes]
    dynamic_paths = [
        index
        for index, path in enumerate(route_paths)
        if "{" in path and path.startswith("/api/mercado-privado/oportunidades/")
    ]
    assert route_paths.index(exact_url) < min(dynamic_paths)

    response = make_client(db, monkeypatch).get(exact_url)
    assert response.status_code == 200
    assert response.json()["data"]["total"] == 0


def test_main_app_and_frontend_use_the_exact_same_trash_url():
    from web_comparativas.main import app

    exact_url = "/api/mercado-privado/oportunidades/papelera"
    matching_routes = [
        route
        for route in app.routes
        if getattr(route, "path", None) == exact_url
        and "GET" in (getattr(route, "methods", set()) or set())
    ]
    assert len(matching_routes) == 1
    assert exact_url in app.openapi()["paths"]

    javascript = Path(
        "web_comparativas/static/js/mercado_privado_oportunidades.js"
    ).read_text(encoding="utf-8")
    assert f'const TRASH_API = "{exact_url}";' in javascript
    assert "window.confirm" not in javascript


def test_reject_refresh_trash_restore_keeps_data_and_totals(db, monkeypatch):
    client = make_client(db, monkeypatch)
    stable_id = opportunity_stable_id("Cliente Uno", "A1")

    initial = client.get("/api/mercado-privado/oportunidades/list").json()["data"]
    assert initial["total"] == 2
    assert sum(row["monto_oportunidad"] for row in initial["rows"]) == 3_000

    rejected = client.post("/api/mercado-privado/oportunidades/rechazar/1")
    assert rejected.status_code == 200
    assert rejected.json()["rechazada"] is True
    repeated = client.post("/api/mercado-privado/oportunidades/rechazar/1")
    assert repeated.status_code == 200
    assert repeated.json()["ya_rechazada"] is True

    refreshed = client.get("/api/mercado-privado/oportunidades/list").json()["data"]
    assert refreshed["total"] == 1
    assert [row["oportunidad_id"] for row in refreshed["rows"]] == [
        opportunity_stable_id("Cliente Dos", "A2")
    ]
    assert sum(row["monto_oportunidad"] for row in refreshed["rows"]) == 2_000
    assert client.get("/api/mercado-privado/oportunidades/cuentas/1").status_code == 409
    assert client.post("/api/mercado-privado/oportunidades/enviar/1").status_code == 409

    trash = client.get("/api/mercado-privado/oportunidades/papelera").json()["data"]
    assert trash["total"] == 1
    assert trash["ventana_horas"] == 24
    assert trash["items"][0]["oportunidad_id"] == stable_id
    assert trash["items"][0]["producto_nombre"] == "Producto A1"
    assert trash["items"][0]["monto_oportunidad"] == 1_000

    restored = client.post(
        "/api/mercado-privado/oportunidades/papelera/recuperar/" + stable_id
    )
    assert restored.status_code == 200
    assert restored.json()["recuperada"] is True
    repeated_restore = client.post(
        "/api/mercado-privado/oportunidades/papelera/recuperar/" + stable_id
    )
    assert repeated_restore.status_code == 200
    assert repeated_restore.json()["recuperada"] is False

    final = client.get("/api/mercado-privado/oportunidades/list").json()["data"]
    assert final["total"] == 2
    assert len({row["oportunidad_id"] for row in final["rows"]}) == 2
    restored_row = next(row for row in final["rows"] if row["oportunidad_id"] == stable_id)
    assert restored_row["producto_nombre"] == "Producto A1"
    assert restored_row["monto_oportunidad"] == 1_000
    assert client.get("/api/mercado-privado/oportunidades/papelera").json()["data"]["total"] == 0

    events = db.execute(
        select(OportunidadRechazoEvento.decision)
        .where(OportunidadRechazoEvento.oportunidad_id == stable_id)
        .order_by(OportunidadRechazoEvento.id)
    ).scalars().all()
    assert events == ["rechazada", "recuperada"]


def test_expired_rejection_stays_hidden_and_cannot_be_restored(db, monkeypatch):
    client = make_client(db, monkeypatch)
    stable_id = opportunity_stable_id("Cliente Uno", "A1")
    assert client.post("/api/mercado-privado/oportunidades/rechazar/1").status_code == 200

    rejection = db.execute(
        select(OportunidadRechazo).where(OportunidadRechazo.oportunidad_id == stable_id)
    ).scalar_one()
    rejection.updated_at = dt.datetime.utcnow() - dt.timedelta(hours=25)
    db.commit()

    assert client.get("/api/mercado-privado/oportunidades/papelera").json()["data"]["total"] == 0
    active = client.get("/api/mercado-privado/oportunidades/list").json()["data"]
    assert stable_id not in {row["oportunidad_id"] for row in active["rows"]}
    restore = client.post(
        "/api/mercado-privado/oportunidades/papelera/recuperar/" + stable_id
    )
    assert restore.status_code == 409
    assert db.execute(
        select(OportunidadRechazo).where(OportunidadRechazo.oportunidad_id == stable_id)
    ).scalar_one_or_none() is not None


def test_delete_from_trash_keeps_permanent_rejection(db, monkeypatch):
    client = make_client(db, monkeypatch)
    stable_id = opportunity_stable_id("Cliente Uno", "A1")
    assert client.post("/api/mercado-privado/oportunidades/rechazar/1").status_code == 200

    deleted = client.post(
        "/api/mercado-privado/oportunidades/papelera/eliminar/" + stable_id
    )
    assert deleted.status_code == 200
    assert deleted.json()["eliminada"] is True
    assert client.get("/api/mercado-privado/oportunidades/papelera").json()["data"]["total"] == 0
    active = client.get("/api/mercado-privado/oportunidades/list").json()["data"]
    assert stable_id not in {row["oportunidad_id"] for row in active["rows"]}
    assert db.execute(
        select(OportunidadRechazo).where(OportunidadRechazo.oportunidad_id == stable_id)
    ).scalar_one_or_none() is not None


def test_rejection_survives_summary_rebuild_by_stable_identity(db, monkeypatch):
    client = make_client(db, monkeypatch)
    stable_id = opportunity_stable_id("Cliente Uno", "A1")
    assert client.post("/api/mercado-privado/oportunidades/rechazar/1").status_code == 200

    db.add(DimensionamientoImportRun(
        id=2,
        source_path="run-2.csv",
        status="success",
        finished_at=dt.datetime(2026, 9, 10, 12, 0),
        oportunidades_ref_month=dt.date(2026, 8, 1),
    ))
    db.add_all([
        make_opportunity(101, 2, "Cliente Uno", "A1", 1_500),
        make_opportunity(102, 2, "Cliente Tres", "A3", 3_000),
    ])
    db.commit()

    active = client.get("/api/mercado-privado/oportunidades/list").json()["data"]
    assert active["run_id"] == 2
    assert stable_id not in {row["oportunidad_id"] for row in active["rows"]}
    trash = client.get("/api/mercado-privado/oportunidades/papelera").json()["data"]
    assert trash["items"][0]["id"] == 101


def test_trash_and_mutations_reuse_current_visibility(db, monkeypatch):
    admin_client = make_client(db, monkeypatch)
    stable_id = opportunity_stable_id("Cliente Uno", "A1")
    assert admin_client.post("/api/mercado-privado/oportunidades/rechazar/1").status_code == 200

    monkeypatch.setattr(router, "OPORTUNIDADES_CARTERA_ENABLED", lambda: True)
    monkeypatch.setattr(router, "oportunidades_visibles_para", lambda *_args: [])
    restricted = make_client(
        db,
        monkeypatch,
        user=SimpleNamespace(id=8, email="analista@suizo.com", role="analista"),
    )
    monkeypatch.setattr(router, "OPORTUNIDADES_CARTERA_ENABLED", lambda: True)
    monkeypatch.setattr(router, "oportunidades_visibles_para", lambda *_args: [])

    assert restricted.get("/api/mercado-privado/oportunidades/papelera").json()["data"]["total"] == 0
    restore = restricted.post(
        "/api/mercado-privado/oportunidades/papelera/recuperar/" + stable_id
    )
    assert restore.status_code == 403
