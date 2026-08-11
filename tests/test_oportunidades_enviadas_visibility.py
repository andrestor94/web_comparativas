from __future__ import annotations

import datetime as dt
import json
import os
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, User
from web_comparativas.dimensionamiento.models import CrmEnvio
from web_comparativas.routers import oportunidades_router as router


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[User.__table__, CrmEnvio.__table__])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        yield session


def make_user(db, email, role, *, manager=None):
    user = User(
        email=email, name=email.split("@")[0], role=role, password_hash="x",
        reporta_a_id=manager.id if manager else None,
    )
    db.add(user)
    db.flush()
    return user


def add_envio(db, sender: User, suffix: str, *, with_user_id=True):
    envio = CrmEnvio(
        oportunidad_id=f"opp-{suffix}", cliente_visible=f"Cliente {suffix}",
        codigo_articulo=f"ART-{suffix}", unidad_negocio="Unidad",
        enviado_por=sender.email, enviado_por_id=sender.id if with_user_id else None,
        enviado_at=dt.datetime(2026, 8, 1, 12, 0), crm_status="ENVIADO_TEST",
        crm_id=f"crm-{suffix}", crm_modo="test", crm_assigned_usuario=sender.name,
        crm_assigned_origen="match", payload_snapshot=json.dumps({"amount": 100}),
    )
    db.add(envio)
    return envio


@pytest.fixture()
def escenario(db, monkeypatch):
    gerente = make_user(db, "gerente@example.com", "gerente")
    supervisor = make_user(db, "supervisor@example.com", "supervisor", manager=gerente)
    analista = make_user(db, "analista@example.com", "analista", manager=supervisor)
    otro_supervisor = make_user(db, "otro.supervisor@example.com", "supervisor")
    otro_analista = make_user(db, "otro.analista@example.com", "analista", manager=otro_supervisor)
    admin = make_user(db, "admin@example.com", "admin")
    auditor = make_user(db, "auditor@example.com", "auditor")

    add_envio(db, gerente, "gerente")
    add_envio(db, supervisor, "supervisor")
    # Historico sin enviado_por_id: debe resolverse por el email sellado.
    add_envio(db, analista, "analista", with_user_id=False)
    add_envio(db, otro_supervisor, "otro-supervisor")
    add_envio(db, otro_analista, "otro-analista")
    db.commit()

    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "_latest_success_import_run", lambda _db: None)
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "test")
    return {
        "gerente": gerente, "supervisor": supervisor, "analista": analista,
        "otro_supervisor": otro_supervisor, "otro_analista": otro_analista,
        "admin": admin, "auditor": auditor,
    }


def visible_ids(db, actor):
    response = router.oportunidades_enviadas(object(), actor, db)
    return {row["oportunidad_id"] for row in response["data"]["rows"]}


def test_analista_ve_solo_sus_envios(db, escenario):
    assert visible_ids(db, escenario["analista"]) == {"opp-analista"}


def test_supervisor_ve_propios_y_analistas_asignados(db, escenario):
    assert visible_ids(db, escenario["supervisor"]) == {
        "opp-supervisor", "opp-analista",
    }


def test_gerente_ve_propios_y_estructura_transitiva(db, escenario):
    assert visible_ids(db, escenario["gerente"]) == {
        "opp-gerente", "opp-supervisor", "opp-analista",
    }


@pytest.mark.parametrize("role_key", ["admin", "auditor"])
def test_admin_y_auditor_ven_todos_los_envios(db, escenario, role_key):
    assert visible_ids(db, escenario[role_key]) == {
        "opp-gerente", "opp-supervisor", "opp-analista",
        "opp-otro-supervisor", "opp-otro-analista",
    }


def test_analista_no_resuelve_por_url_un_envio_ajeno(db, escenario):
    own = router.oportunidad_enviada_detalle(
        object(), "opp-analista", escenario["analista"], db,
    )
    foreign = router.oportunidad_enviada_detalle(
        object(), "opp-supervisor", escenario["analista"], db,
    )
    assert own["data"]["found"] is True
    assert foreign["data"]["found"] is False
    assert foreign["data"]["row"] is None

    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._perm_oportunidades] = lambda: escenario["analista"]
    app.dependency_overrides[router.get_db] = lambda: db
    response = TestClient(app).get(
        "/api/mercado-privado/oportunidades/enviadas/detalle",
        params={"oportunidad_id": "opp-supervisor"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["found"] is False
