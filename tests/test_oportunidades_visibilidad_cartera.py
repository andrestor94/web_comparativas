from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, User, VendedorFusion
from web_comparativas.dimensionamiento.models import (
    DimensionamientoImportRun, OportunidadAsignacionManual, OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id
from web_comparativas.dimensionamiento import oportunidades_visibilidad as visibilidad


class _FakeMaster:
    """Stand-in de MasterIndex: evita depender de Operadores.xlsx/clientes.csv reales
    en el test (mismo criterio de aislamiento que ya usan los tests de oportunidades_router:
    monkeypatch de la función que trae datos externos, no del cálculo)."""

    def __init__(self, operators_by_code: dict[str, dict[str, str]]):
        self.operators_by_code = operators_by_code


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        User.__table__, VendedorFusion.__table__,
        DimensionamientoImportRun.__table__, OportunidadSummary.__table__,
        OportunidadAsignacionManual.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.add(DimensionamientoImportRun(id=1, source_path="test.csv", status="success"))
        session.commit()
        yield session


def make_user(db, *, email, role, reporta_a_id=None) -> User:
    u = User(email=email, name=email.split("@")[0], role=role, password_hash="x", reporta_a_id=reporta_a_id)
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


def make_vendedor(db, *, codigo, user_id=None) -> VendedorFusion:
    v = VendedorFusion(codigo_vendedor=codigo, nombre_fusion=f"VENDEDOR {codigo}", activo=True, user_id=user_id)
    db.add(v)
    db.commit()
    return v


def make_oportunidad(db, *, oid, cuenta_interna) -> OportunidadSummary:
    o = OportunidadSummary(
        id=oid, import_run_id=1, codigo_articulo=f"ART{oid}", cliente_visible=f"CLIENTE {oid}",
        cuenta_interna=cuenta_interna,
    )
    db.add(o)
    db.commit()
    db.refresh(o)
    return o


@pytest.fixture()
def escenario(db, monkeypatch):
    """Fuerza de venta con cartera propia en dos niveles (Analista y Supervisor),
    jerarquía de 3 niveles, y un buffer con dos casos distintos de "sin vincular":
    una cuenta cuyo vendedor de Fusión existe pero todavía no tiene usuario (DDD/5304),
    y una cuenta que directamente no matchea Operadores.xlsx (ZZZ)."""
    admin = make_user(db, email="admin@suizo.com", role="admin")
    auditor = make_user(db, email="auditor@suizo.com", role="auditor")

    gerente1 = make_user(db, email="gerente1@suizo.com", role="gerente")
    supervisor1 = make_user(db, email="supervisor1@suizo.com", role="supervisor", reporta_a_id=gerente1.id)
    supervisor2 = make_user(db, email="supervisor2@suizo.com", role="supervisor", reporta_a_id=gerente1.id)
    analista1 = make_user(db, email="analista1@suizo.com", role="analista", reporta_a_id=supervisor1.id)
    analista2 = make_user(db, email="analista2@suizo.com", role="analista", reporta_a_id=supervisor2.id)

    # 4071 -> analista1 | 2731 -> supervisor1 (cartera propia) | 3162 -> analista2
    # 5304 existe en Operadores.xlsx pero todavía nadie lo vinculó -> va al buffer.
    make_vendedor(db, codigo="4071", user_id=analista1.id)
    make_vendedor(db, codigo="2731", user_id=supervisor1.id)
    make_vendedor(db, codigo="3162", user_id=analista2.id)
    make_vendedor(db, codigo="5304", user_id=None)

    opp_analista1 = make_oportunidad(db, oid=1, cuenta_interna="AAA")
    opp_supervisor1 = make_oportunidad(db, oid=2, cuenta_interna="BBB")
    opp_analista2 = make_oportunidad(db, oid=3, cuenta_interna="CCC")
    opp_buffer_vendedor_sin_vincular = make_oportunidad(db, oid=4, cuenta_interna="DDD")
    opp_buffer_sin_match = make_oportunidad(db, oid=5, cuenta_interna="ZZZ")

    fake_master = _FakeMaster({
        "AAA": {"vendedor_codigo": "4071"},
        "BBB": {"vendedor_codigo": "2731"},
        "CCC": {"vendedor_codigo": "3162"},
        "DDD": {"vendedor_codigo": "5304"},
        # "ZZZ" deliberadamente ausente: simula una cuenta que no matchea Operadores.xlsx.
    })
    monkeypatch.setattr(visibilidad, "get_master_index", lambda: fake_master)

    todas = [
        opp_analista1, opp_supervisor1, opp_analista2,
        opp_buffer_vendedor_sin_vincular, opp_buffer_sin_match,
    ]
    return {
        "admin": admin, "auditor": auditor, "gerente1": gerente1,
        "supervisor1": supervisor1, "supervisor2": supervisor2,
        "analista1": analista1, "analista2": analista2,
        "opp_analista1": opp_analista1, "opp_supervisor1": opp_supervisor1,
        "opp_analista2": opp_analista2,
        "opp_buffer_vendedor_sin_vincular": opp_buffer_vendedor_sin_vincular,
        "opp_buffer_sin_match": opp_buffer_sin_match,
        "todas": todas,
    }


def _ids(rows) -> set[int]:
    return {r.id for r in rows}


def test_analista_ve_solo_su_propia_cartera(db, escenario):
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["analista1"], escenario["todas"])
    assert _ids(vistas) == {escenario["opp_analista1"].id}


def test_supervisor_ve_propia_cartera_mas_equipo_mas_buffer(db, escenario):
    """La fórmula pedida: SU cartera propia + la de sus analistas + el buffer sin vincular."""
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["supervisor1"], escenario["todas"])
    esperado = {
        escenario["opp_supervisor1"].id,                    # su propia cartera (2731)
        escenario["opp_analista1"].id,                       # analista1 está a su cargo
        escenario["opp_buffer_vendedor_sin_vincular"].id,    # buffer: 5304 sin vincular
        escenario["opp_buffer_sin_match"].id,                # buffer: ZZZ sin match
    }
    assert _ids(vistas) == esperado
    # Lo de analista2 (a cargo de supervisor2, no de supervisor1) NO debe aparecer.
    assert escenario["opp_analista2"].id not in _ids(vistas)


def test_supervisor_sin_cartera_propia_ve_igual_equipo_y_buffer(db, escenario):
    """supervisor2 no está vinculado a ningún vendedor: ve su equipo y el buffer, sin cartera propia."""
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["supervisor2"], escenario["todas"])
    esperado = {
        escenario["opp_analista2"].id,
        escenario["opp_buffer_vendedor_sin_vincular"].id,
        escenario["opp_buffer_sin_match"].id,
    }
    assert _ids(vistas) == esperado


def test_gerente_ve_supervisores_y_analistas_transitivamente_sin_buffer(db, escenario):
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["gerente1"], escenario["todas"])
    esperado = {
        escenario["opp_supervisor1"].id,   # cartera propia de supervisor1 (a su cargo)
        escenario["opp_analista1"].id,     # analista1, transitivo via supervisor1
        escenario["opp_analista2"].id,     # analista2, transitivo via supervisor2
    }
    assert _ids(vistas) == esperado
    # El buffer es turf del Supervisor, no del Gerente.
    assert escenario["opp_buffer_vendedor_sin_vincular"].id not in _ids(vistas)
    assert escenario["opp_buffer_sin_match"].id not in _ids(vistas)


@pytest.mark.parametrize("actor_key", ["admin", "auditor"])
def test_admin_y_auditor_ven_todo(db, escenario, actor_key):
    vistas = visibilidad.oportunidades_visibles_para(db, escenario[actor_key], escenario["todas"])
    assert _ids(vistas) == _ids(escenario["todas"])


def test_asignacion_manual_pisa_la_cartera_para_el_analista(db, escenario):
    """La oportunidad de analista2 (cuenta CCC, vendedor 3162) se asigna a mano a
    analista1: analista1 tiene que verla ADEMÁS de su propia cartera, sin que
    analista2 pierda la suya (aditivo, no resta)."""
    opp_ajena = escenario["opp_analista2"]
    db.add(OportunidadAsignacionManual(
        oportunidad_id=opportunity_stable_id(opp_ajena.cliente_visible, opp_ajena.codigo_articulo),
        analista_user_id=escenario["analista1"].id,
        asignado_por_user_id=escenario["supervisor1"].id,
    ))
    db.commit()

    vistas_analista1 = visibilidad.oportunidades_visibles_para(db, escenario["analista1"], escenario["todas"])
    assert _ids(vistas_analista1) == {escenario["opp_analista1"].id, opp_ajena.id}

    # analista2 sigue viendo la suya igual: la asignación manual no le sacó nada.
    vistas_analista2 = visibilidad.oportunidades_visibles_para(db, escenario["analista2"], escenario["todas"])
    assert _ids(vistas_analista2) == {opp_ajena.id}
