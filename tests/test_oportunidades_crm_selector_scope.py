from __future__ import annotations

import os
import sys

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, User, UserReporte
from web_comparativas.dimensionamiento import crm_client
from web_comparativas.routers import oportunidades_router as router


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[User.__table__, UserReporte.__table__])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        yield session


def make_user(db, *, email, role, superior_id=None) -> User:
    """`superior_id`: reemplaza el viejo `reporta_a_id` (congelada) — se traduce
    a una fila en `user_reportes`."""
    u = User(email=email, name=email.split("@")[0], role=role, password_hash="x")
    db.add(u)
    db.commit()
    db.refresh(u)
    if superior_id is not None:
        db.add(UserReporte(subordinado_id=u.id, superior_id=superior_id))
        db.commit()
    return u


# Simula los 82 usuarios del CRM: uno por cada persona SIEM que nos interesa, más
# "ruido" (gente del CRM que no tiene ningún usuario de SIEM correspondiente).
CRM_USUARIOS_FAKE = [
    {"id": "crm-1", "usuario": "daniela.armilio"},
    {"id": "crm-2", "usuario": "ayelen.piluso"},
    {"id": "crm-3", "usuario": "myriam.fernandez"},
    {"id": "crm-4", "usuario": "ariel.gurvich"},
    {"id": "crm-5", "usuario": "otro.supervisor"},
    {"id": "crm-6", "usuario": "otro.analista"},
    {"id": "crm-7", "usuario": "gerente.test"},
] + [{"id": f"crm-ruido-{i}", "usuario": f"nadie.relacionado{i}"} for i in range(75)]


@pytest.fixture()
def escenario(db, monkeypatch):
    gerente = make_user(db, email="gerente.test@suizoargentina.com", role="gerente")
    supervisor = make_user(db, email="daniela.armilio@suizoargentina.com", role="supervisor", superior_id=gerente.id)
    otro_supervisor = make_user(db, email="otro.supervisor@suizoargentina.com", role="supervisor")
    ayelen = make_user(db, email="ayelen.piluso@suizoargentina.com", role="analista", superior_id=supervisor.id)
    myriam = make_user(db, email="myriam.fernandez@suizoargentina.com", role="analista", superior_id=supervisor.id)
    ariel = make_user(db, email="ariel.gurvich@suizoargentina.com", role="analista", superior_id=supervisor.id)
    otro_analista = make_user(db, email="otro.analista@suizoargentina.com", role="analista", superior_id=otro_supervisor.id)
    admin = make_user(db, email="admin@suizo.com", role="admin")
    auditor = make_user(db, email="auditor@suizo.com", role="auditor")

    monkeypatch.setattr(crm_client, "crm_config", lambda: {"base_url": "http://fake", "usuario_fallback_id": None})
    monkeypatch.setattr(
        crm_client, "contexto_asignacion",
        lambda email_siem: {
            "match": crm_client.buscar_match_usuario(CRM_USUARIOS_FAKE, email_siem),
            "usuarios": list(CRM_USUARIOS_FAKE),
            "sugerido_id": None,
        },
    )

    return {
        "gerente": gerente, "supervisor": supervisor, "otro_supervisor": otro_supervisor,
        "ayelen": ayelen, "myriam": myriam, "ariel": ariel, "otro_analista": otro_analista,
        "admin": admin, "auditor": auditor,
    }


def _usuarios_ids(ctx) -> set[str]:
    return {u["id"] for u in ctx["usuarios"]}


def test_analista_no_ve_ningun_usuario(db, escenario):
    ctx = router._contexto_asignacion_seguro(escenario["ayelen"], db)
    assert ctx["usuarios"] == []
    assert ctx["puede_elegir"] is False


def test_supervisor_ve_solo_a_si_mismo_y_su_equipo(db, escenario):
    ctx = router._contexto_asignacion_seguro(escenario["supervisor"], db)
    assert _usuarios_ids(ctx) == {"crm-1", "crm-2", "crm-3", "crm-4"}  # daniela + ayelen + myriam + ariel
    # Ni el otro supervisor ni su analista (equipo ajeno) aparecen.
    assert "crm-5" not in _usuarios_ids(ctx)
    assert "crm-6" not in _usuarios_ids(ctx)


def test_gerente_ve_su_estructura_transitiva(db, escenario):
    ctx = router._contexto_asignacion_seguro(escenario["gerente"], db)
    # gerente.test (el propio) + daniela (su supervisora) + los 3 analistas de daniela.
    assert _usuarios_ids(ctx) == {"crm-7", "crm-1", "crm-2", "crm-3", "crm-4"}
    assert "crm-5" not in _usuarios_ids(ctx)  # otro supervisor, no es suyo
    assert "crm-6" not in _usuarios_ids(ctx)  # analista de otro supervisor


@pytest.mark.parametrize("actor_key", ["admin", "auditor"])
def test_admin_y_auditor_ven_los_82(db, escenario, actor_key):
    ctx = router._contexto_asignacion_seguro(escenario[actor_key], db)
    assert len(ctx["usuarios"]) == len(CRM_USUARIOS_FAKE) == 82


def test_supervisor_no_puede_forzar_un_assigned_user_id_fuera_de_su_equipo(db, escenario):
    """La validación server-side: si igual llega un assigned_user_id que no está en
    la lista acotada (ej. alguien manipulando el POST a mano), _decidir_asignado
    tiene que rechazarlo — no solo faltar en el <select>."""
    ctx = router._contexto_asignacion_seguro(escenario["supervisor"], db)
    with pytest.raises(HTTPException) as exc_info:
        router._decidir_asignado(ctx, "crm-5")  # el usuario del CRM de "otro.supervisor"
    assert exc_info.value.status_code == 422


def test_supervisor_puede_asignar_a_alguien_de_su_propio_equipo(db, escenario):
    ctx = router._contexto_asignacion_seguro(escenario["supervisor"], db)
    resultado = router._decidir_asignado(ctx, "crm-2")  # ayelen, de su equipo
    assert resultado == {"id": "crm-2", "usuario": "ayelen.piluso", "origen": "manual"}


def test_supervisor_no_ve_opcion_usuario_siem(db, escenario, monkeypatch):
    """CRM_USUARIO_SIEM_ID está restringida a Admin puro (ago-2026): un Supervisor
    (que sí puede elegir asignado) no tiene que ver la opción "Usuario SIEM (sistema)"
    en su lista, aunque la variable esté seteada."""
    monkeypatch.setattr(router, "CRM_USUARIO_SIEM_ID", lambda: "crm-siem-1")
    ctx_supervisor = router._contexto_asignacion_seguro(escenario["supervisor"], db)
    assert "crm-siem-1" not in _usuarios_ids(ctx_supervisor)

    # Control positivo en el mismo test: con la variable seteada, Admin SÍ la ve.
    ctx_admin = router._contexto_asignacion_seguro(escenario["admin"], db)
    assert "crm-siem-1" in _usuarios_ids(ctx_admin)


def test_supervisor_no_puede_forzar_usuario_siem(db, escenario, monkeypatch):
    """La validación server-side (no solo el <select>): un Supervisor que mande el id
    del usuario de sistema a mano en el request tiene que ser rechazado igual, aunque
    no lo vea en su propia lista."""
    monkeypatch.setattr(router, "CRM_USUARIO_SIEM_ID", lambda: "crm-siem-1")
    ctx_supervisor = router._contexto_asignacion_seguro(escenario["supervisor"], db)
    with pytest.raises(HTTPException) as exc_info:
        router._decidir_asignado(ctx_supervisor, "crm-siem-1")
    assert exc_info.value.status_code == 422


def test_sugerido_id_se_limpia_si_queda_fuera_del_scope(db, escenario, monkeypatch):
    monkeypatch.setattr(
        crm_client, "contexto_asignacion",
        lambda email_siem: {
            "match": crm_client.buscar_match_usuario(CRM_USUARIOS_FAKE, email_siem),
            "usuarios": list(CRM_USUARIOS_FAKE),
            "sugerido_id": "crm-5",  # el fallback configurado es alguien fuera del equipo
        },
    )
    ctx = router._contexto_asignacion_seguro(escenario["supervisor"], db)
    assert ctx["sugerido_id"] is None
