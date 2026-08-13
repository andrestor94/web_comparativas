from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.middleware.sessions import SessionMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, User, VendedorFusion
from web_comparativas.auth import hash_password
from web_comparativas.routers import sic_router as router


STATIC_DIR = Path(__file__).resolve().parent.parent / "web_comparativas" / "static"


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[User.__table__, VendedorFusion.__table__])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        yield session


@pytest.fixture()
def client(db, monkeypatch):
    monkeypatch.setattr(router, "db_session", db)
    app = FastAPI()
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.add_middleware(SessionMiddleware, secret_key="test")
    app.include_router(router.router)
    with TestClient(app, follow_redirects=False) as c:
        yield c


def make_user(db, *, email, role, name=None, reporta_a_id=None) -> User:
    u = User(
        email=email, name=name or email.split("@")[0], role=role,
        password_hash=hash_password("x" * 12), unit_business="Otros",
        access_scope="todos", module_access=None, reporta_a_id=reporta_a_id,
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


def make_vendedor(db, *, codigo, nombre="VENDEDOR TEST", user_id=None) -> VendedorFusion:
    v = VendedorFusion(codigo_vendedor=codigo, nombre_fusion=nombre, activo=True, user_id=user_id)
    db.add(v)
    db.commit()
    db.refresh(v)
    return v


def override_actor(test_client: TestClient, actor: User):
    test_client.app.dependency_overrides[router.sic_access_required] = lambda: actor


def create_analista_form(vendedor_id: str = "", **overrides):
    data = {
        "email": "nuevo.analista@suizo.com",
        "name": "Nuevo Analista",
        "role": "analista",
        "password": "x" * 12,
        "password_confirm": "x" * 12,
        "unit_business": "Otros",
        "vendedor_fusion_id": vendedor_id,
    }
    data.update(overrides)
    return data


def test_analista_alta_con_vendedor_libre(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    vendedor = make_vendedor(db, codigo="4071", nombre="AYELEN PILUSO")
    override_actor(client, admin)

    r = client.post("/sic/users/new", data=create_analista_form(vendedor_id=str(vendedor.id)))
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=created"

    nuevo = db.query(User).filter(User.email == "nuevo.analista@suizo.com").first()
    assert nuevo is not None
    db.refresh(vendedor)
    assert vendedor.user_id == nuevo.id


def test_analista_no_puede_tomar_vendedor_ocupado(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    titular = make_user(db, email="titular@suizo.com", role="analista")
    vendedor = make_vendedor(db, codigo="4071", nombre="AYELEN PILUSO", user_id=titular.id)
    override_actor(client, admin)

    r = client.post("/sic/users/new", data=create_analista_form(vendedor_id=str(vendedor.id)))
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users/new?err=vendedor_ocupado"

    # Nada se persistió: ni el usuario nuevo, ni se movió el vendedor.
    assert db.query(User).filter(User.email == "nuevo.analista@suizo.com").first() is None
    db.refresh(vendedor)
    assert vendedor.user_id == titular.id


def test_analista_puede_liberar_y_tomar_otro_vendedor_al_editar(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    analista = make_user(db, email="analista@suizo.com", role="analista")
    vendedor_a = make_vendedor(db, codigo="4071", nombre="AYELEN PILUSO", user_id=analista.id)
    vendedor_b = make_vendedor(db, codigo="2731", nombre="YANINA SASSONE")
    override_actor(client, admin)

    r = client.post(
        f"/sic/users/{analista.id}/update",
        data={
            "name": "Analista", "role": "analista", "unit_business": "Otros",
            "vendedor_fusion_id": str(vendedor_b.id),
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=updated"

    db.refresh(vendedor_a)
    db.refresh(vendedor_b)
    assert vendedor_a.user_id is None
    assert vendedor_b.user_id == analista.id


def test_supervisor_alta_con_analistas_libres(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    a1 = make_user(db, email="a1@suizo.com", role="analista")
    a2 = make_user(db, email="a2@suizo.com", role="analista")
    override_actor(client, admin)

    r = client.post(
        "/sic/users/new",
        data={
            "email": "nuevo.supervisor@suizo.com", "name": "Nuevo Supervisor",
            "role": "supervisor", "password": "x" * 12, "password_confirm": "x" * 12,
            "unit_business": "Otros", "analista_ids": [str(a1.id), str(a2.id)],
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=created"

    supervisor = db.query(User).filter(User.email == "nuevo.supervisor@suizo.com").first()
    assert supervisor is not None
    db.refresh(a1)
    db.refresh(a2)
    assert a1.reporta_a_id == supervisor.id
    assert a2.reporta_a_id == supervisor.id


def test_supervisor_no_puede_tomar_analista_ya_asignado(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    sup1 = make_user(db, email="sup1@suizo.com", role="supervisor")
    analista = make_user(db, email="analista@suizo.com", role="analista", reporta_a_id=sup1.id)
    override_actor(client, admin)

    r = client.post(
        "/sic/users/new",
        data={
            "email": "sup2@suizo.com", "name": "Supervisor 2", "role": "supervisor",
            "password": "x" * 12, "password_confirm": "x" * 12, "unit_business": "Otros",
            "analista_ids": [str(analista.id)],
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users/new?err=analista_ocupado"

    assert db.query(User).filter(User.email == "sup2@suizo.com").first() is None
    db.refresh(analista)
    assert analista.reporta_a_id == sup1.id


def test_gerente_alta_con_supervisores(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    sup = make_user(db, email="sup@suizo.com", role="supervisor")
    override_actor(client, admin)

    r = client.post(
        "/sic/users/new",
        data={
            "email": "gerente@suizo.com", "name": "Gerente Uno", "role": "gerente",
            "password": "x" * 12, "password_confirm": "x" * 12, "unit_business": "Otros",
            "supervisor_ids": [str(sup.id)],
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=created"

    gerente = db.query(User).filter(User.email == "gerente@suizo.com").first()
    db.refresh(sup)
    assert sup.reporta_a_id == gerente.id


def test_cambio_de_rol_libera_vinculos(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    supervisor = make_user(db, email="supervisor@suizo.com", role="supervisor")
    a1 = make_user(db, email="a1@suizo.com", role="analista", reporta_a_id=supervisor.id)
    a2 = make_user(db, email="a2@suizo.com", role="analista", reporta_a_id=supervisor.id)
    override_actor(client, admin)

    # El supervisor pasa a Analista: sus 2 analistas quedan huérfanos (reporta_a_id=None).
    r = client.post(
        f"/sic/users/{supervisor.id}/update",
        data={"name": "Ex Supervisor", "role": "analista", "unit_business": "Otros"},
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=updated"

    db.refresh(a1)
    db.refresh(a2)
    assert a1.reporta_a_id is None
    assert a2.reporta_a_id is None


def test_cambio_de_rol_libera_vendedor(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    analista = make_user(db, email="analista@suizo.com", role="analista")
    vendedor = make_vendedor(db, codigo="4071", nombre="AYELEN PILUSO", user_id=analista.id)
    override_actor(client, admin)

    r = client.post(
        f"/sic/users/{analista.id}/update",
        data={"name": "Ex Analista", "role": "auditor", "unit_business": "Otros"},
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=updated"

    db.refresh(vendedor)
    assert vendedor.user_id is None


def test_form_renderiza_bloques_segun_rol(client, db):
    admin = make_user(db, email="admin@suizo.com", role="admin")
    make_vendedor(db, codigo="4071", nombre="AYELEN PILUSO")
    override_actor(client, admin)

    r = client.get("/sic/users/new")
    assert r.status_code == 200
    assert 'data-role-blocks="analista supervisor"' in r.text
    assert 'data-role-blocks="supervisor"' in r.text
    assert 'data-role-blocks="gerente"' in r.text
    assert "AYELEN PILUSO" in r.text
    assert "Identidad en Fusión (ERP)" in r.text
    assert "— Sin vincular —" in r.text
    assert 'id="role-change-warning"' in r.text


def test_supervisor_puede_tener_identidad_fusion_propia_ademas_del_equipo(client, db):
    """Un Supervisor NO es solo jerarquía: los 16 de Operadores.xlsx incluyen
    supervisores con cartera propia (ej. Daniela Armillo). El form tiene que dejarlo
    vincular su propia identidad en Fusión Y elegir su equipo en la misma alta."""
    admin = make_user(db, email="admin@suizo.com", role="admin")
    analista = make_user(db, email="analista@suizo.com", role="analista")
    vendedor = make_vendedor(db, codigo="3162", nombre="DANIELA ARMILLO")
    override_actor(client, admin)

    r = client.post(
        "/sic/users/new",
        data={
            "email": "supervisora@suizo.com", "name": "Daniela Armillo", "role": "supervisor",
            "password": "x" * 12, "password_confirm": "x" * 12, "unit_business": "Otros",
            "vendedor_fusion_id": str(vendedor.id), "analista_ids": [str(analista.id)],
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=created"

    supervisora = db.query(User).filter(User.email == "supervisora@suizo.com").first()
    assert supervisora is not None
    db.refresh(vendedor)
    db.refresh(analista)
    assert vendedor.user_id == supervisora.id
    assert analista.reporta_a_id == supervisora.id


def test_cambio_de_rol_supervisor_a_auditor_libera_identidad_y_equipo_juntos(client, db):
    """Un supervisor con cartera propia Y equipo que pasa a un rol sin ninguno de los
    dos (Auditor) tiene que liberar ambos vínculos en la misma operación."""
    admin = make_user(db, email="admin@suizo.com", role="admin")
    supervisora = make_user(db, email="supervisora@suizo.com", role="supervisor")
    vendedor = make_vendedor(db, codigo="3162", nombre="DANIELA ARMILLO", user_id=supervisora.id)
    analista = make_user(db, email="analista@suizo.com", role="analista", reporta_a_id=supervisora.id)
    override_actor(client, admin)

    r = client.post(
        f"/sic/users/{supervisora.id}/update",
        data={"name": "Ex Supervisora", "role": "auditor", "unit_business": "Otros"},
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/sic/users?ok=updated"

    db.refresh(vendedor)
    db.refresh(analista)
    assert vendedor.user_id is None
    assert analista.reporta_a_id is None
