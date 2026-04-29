from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from web_comparativas.models import Base, PliegoSolicitud, User
from web_comparativas.routers.pliegos_router import lectura_pliegos_nueva_submit


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def user(db_session):
    user = User(
        email="pliegos-test@example.com",
        name="Pliegos Test",
        role="analista",
        password_hash="secret",
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


def _request(user, db_session):
    return SimpleNamespace(state=SimpleNamespace(user=user, db=db_session))


async def _submit(request, *, token="", title="Lectura duplicada", numero="LP-001"):
    response = await lectura_pliegos_nueva_submit(
        request,
        accion="enviar",
        client_request_id=token,
        titulo=title,
        organismo="Hospital Central",
        nombre_licitacion="Suturas",
        numero_proceso=numero,
        expediente="EXP-001",
        observaciones_usuario="",
        archivos=[],
    )
    request.state.db.commit()
    return response


@pytest.mark.asyncio
async def test_same_client_request_id_is_idempotent(db_session, user):
    request = _request(user, db_session)

    first = await _submit(request, token="fixed-token-1")
    second = await _submit(request, token="fixed-token-1")

    solicitudes = db_session.query(PliegoSolicitud).all()
    assert len(solicitudes) == 1
    assert first.status_code == 303
    assert second.status_code == 303
    assert first.headers["location"] == second.headers["location"]


@pytest.mark.asyncio
async def test_recent_same_payload_without_token_is_idempotent(db_session, user):
    request = _request(user, db_session)

    first = await _submit(request)
    second = await _submit(request)

    solicitudes = db_session.query(PliegoSolicitud).all()
    assert len(solicitudes) == 1
    assert first.headers["location"] == second.headers["location"]


@pytest.mark.asyncio
async def test_distinct_payloads_are_allowed(db_session, user):
    request = _request(user, db_session)

    await _submit(request, token="fixed-token-1", title="Lectura A", numero="LP-001")
    await _submit(request, token="fixed-token-2", title="Lectura B", numero="LP-002")

    solicitudes = db_session.query(PliegoSolicitud).order_by(PliegoSolicitud.id).all()
    assert len(solicitudes) == 2
    assert [s.numero_proceso for s in solicitudes] == ["LP-001", "LP-002"]
