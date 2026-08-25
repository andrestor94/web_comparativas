from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, CarteraOperador, CarteraVendedor, User
from web_comparativas.dimensionamiento.models import (
    DimensionamientoImportRun, OportunidadAsignacionManual, OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id
from web_comparativas.dimensionamiento import oportunidades_visibilidad as visibilidad


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        User.__table__, CarteraOperador.__table__, CarteraVendedor.__table__,
        DimensionamientoImportRun.__table__, OportunidadSummary.__table__,
        OportunidadAsignacionManual.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.add(DimensionamientoImportRun(id=1, source_path="test.csv", status="success"))
        session.commit()
        yield session


def make_user(db, *, email, role, reporta_a_id=None, operador_codigos=None, vendedor_codigos=None) -> User:
    u = User(
        email=email, name=email.split("@")[0], role=role, password_hash="x",
        reporta_a_id=reporta_a_id,
        cartera_operador_codigos=operador_codigos or [],
        cartera_vendedor_codigos=vendedor_codigos or [],
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


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
def escenario(db):
    """Fuerza de venta con cartera propia en dos niveles (Analista y Supervisor,
    resuelta vía `cartera_operadores`/`cartera_vendedores`, no `VendedorFusion`),
    jerarquía de 3 niveles, y dos cuentas huérfanas (DDD/ZZZ: no están en la cartera
    de NADIE) para probar el buffer de Gerente."""
    admin = make_user(db, email="admin@suizo.com", role="admin")
    auditor = make_user(db, email="auditor@suizo.com", role="auditor")

    gerente1 = make_user(db, email="gerente1@suizo.com", role="gerente")
    # Cartera propia de supervisor1 vía OPERADOR (no se topa con el fail-closed de
    # unineg_scope, que solo aplica al lado vendedor — ver cartera_visibilidad.py).
    supervisor1 = make_user(
        db, email="supervisor1@suizo.com", role="supervisor", reporta_a_id=gerente1.id,
        operador_codigos=["OP-S1"],
    )
    supervisor2 = make_user(db, email="supervisor2@suizo.com", role="supervisor", reporta_a_id=gerente1.id)
    analista1 = make_user(
        db, email="analista1@suizo.com", role="analista", reporta_a_id=supervisor1.id,
        vendedor_codigos=["V-A1"],
    )
    analista2 = make_user(
        db, email="analista2@suizo.com", role="analista", reporta_a_id=supervisor2.id,
        vendedor_codigos=["V-A2"],
    )

    db.add_all([
        CarteraVendedor(codigo_cliente="AAA", vendedor_codigo="V-A1", unineg="0"),
        CarteraOperador(codigo_cliente="BBB", operador_codigo="OP-S1"),
        CarteraVendedor(codigo_cliente="CCC", vendedor_codigo="V-A2", unineg="0"),
        # DDD/ZZZ deliberadamente ausentes de cartera_operadores/cartera_vendedores:
        # son las cuentas huérfanas del buffer.
    ])
    db.commit()

    opp_analista1 = make_oportunidad(db, oid=1, cuenta_interna="AAA")
    opp_supervisor1 = make_oportunidad(db, oid=2, cuenta_interna="BBB")
    opp_analista2 = make_oportunidad(db, oid=3, cuenta_interna="CCC")
    opp_buffer_1 = make_oportunidad(db, oid=4, cuenta_interna="DDD")
    opp_buffer_2 = make_oportunidad(db, oid=5, cuenta_interna="ZZZ")

    todas = [opp_analista1, opp_supervisor1, opp_analista2, opp_buffer_1, opp_buffer_2]
    return {
        "admin": admin, "auditor": auditor, "gerente1": gerente1,
        "supervisor1": supervisor1, "supervisor2": supervisor2,
        "analista1": analista1, "analista2": analista2,
        "opp_analista1": opp_analista1, "opp_supervisor1": opp_supervisor1,
        "opp_analista2": opp_analista2,
        "opp_buffer_1": opp_buffer_1, "opp_buffer_2": opp_buffer_2,
        "todas": todas,
    }


def _ids(rows) -> set[int]:
    return {r.id for r in rows}


def test_analista_ve_solo_su_propia_cartera(db, escenario):
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["analista1"], escenario["todas"])
    assert _ids(vistas) == {escenario["opp_analista1"].id}


def test_supervisor_ve_propia_cartera_mas_equipo_sin_buffer(db, escenario):
    """Fórmula nueva: SU cartera propia + la de sus analistas a cargo — SIN buffer
    (el buffer de cuentas huérfanas ahora es turf de Gerente, no de Supervisor)."""
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["supervisor1"], escenario["todas"])
    esperado = {
        escenario["opp_supervisor1"].id,   # su propia cartera (OP-S1 -> BBB)
        escenario["opp_analista1"].id,     # analista1 está a su cargo
    }
    assert _ids(vistas) == esperado
    assert escenario["opp_buffer_1"].id not in _ids(vistas)
    assert escenario["opp_buffer_2"].id not in _ids(vistas)
    assert escenario["opp_analista2"].id not in _ids(vistas)


def test_supervisor_sin_cartera_propia_ve_solo_su_equipo(db, escenario):
    """supervisor2 no tiene operador/vendedor propio: ve solo a su equipo, sin
    cartera propia y sin buffer."""
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["supervisor2"], escenario["todas"])
    assert _ids(vistas) == {escenario["opp_analista2"].id}


def test_gerente_ve_supervisores_y_analistas_transitivamente_mas_buffer(db, escenario):
    """Fórmula nueva: cartera de su equipo (transitiva) MÁS el buffer de cuentas que
    no son de la cartera de NADIE — decisión de negocio 2026-08-25, "nada se pierde"."""
    vistas = visibilidad.oportunidades_visibles_para(db, escenario["gerente1"], escenario["todas"])
    assert _ids(vistas) == _ids(escenario["todas"])  # en este escenario, todo cae bajo su árbol o el buffer


def test_gerente_no_ve_cuentas_de_otro_arbol_que_si_tienen_dueno(db, escenario):
    """El buffer es SOLO lo huérfano, no todo lo que esté fuera del árbol propio del
    Gerente: una cuenta cubierta por otro usuario (aunque no sea de su equipo) NO
    entra por el buffer."""
    otro_gerente = make_user(db, email="gerente2@suizo.com", role="gerente")
    otro_analista = make_user(
        db, email="analista3@suizo.com", role="analista", reporta_a_id=otro_gerente.id,
        vendedor_codigos=["V-A3"],
    )
    db.add(CarteraVendedor(codigo_cliente="FFF", vendedor_codigo="V-A3", unineg="0"))
    db.commit()
    opp_otro_arbol = make_oportunidad(db, oid=6, cuenta_interna="FFF")
    todas = escenario["todas"] + [opp_otro_arbol]

    vistas_gerente1 = visibilidad.oportunidades_visibles_para(db, escenario["gerente1"], todas)
    assert opp_otro_arbol.id not in _ids(vistas_gerente1)

    vistas_analista3 = visibilidad.oportunidades_visibles_para(db, otro_analista, todas)
    assert _ids(vistas_analista3) == {opp_otro_arbol.id}


@pytest.mark.parametrize("actor_key", ["admin", "auditor"])
def test_admin_y_auditor_ven_todo(db, escenario, actor_key):
    vistas = visibilidad.oportunidades_visibles_para(db, escenario[actor_key], escenario["todas"])
    assert _ids(vistas) == _ids(escenario["todas"])


def test_asignacion_manual_pisa_la_cartera_para_el_analista(db, escenario):
    """La oportunidad de analista2 (cuenta CCC) se asigna a mano a analista1:
    analista1 tiene que verla ADEMÁS de su propia cartera, sin que analista2 pierda
    la suya (aditivo, no resta)."""
    opp_ajena = escenario["opp_analista2"]
    db.add(OportunidadAsignacionManual(
        oportunidad_id=opportunity_stable_id(opp_ajena.cliente_visible, opp_ajena.codigo_articulo),
        analista_user_id=escenario["analista1"].id,
        asignado_por_user_id=escenario["supervisor1"].id,
    ))
    db.commit()

    vistas_analista1 = visibilidad.oportunidades_visibles_para(db, escenario["analista1"], escenario["todas"])
    assert _ids(vistas_analista1) == {escenario["opp_analista1"].id, opp_ajena.id}

    vistas_analista2 = visibilidad.oportunidades_visibles_para(db, escenario["analista2"], escenario["todas"])
    assert _ids(vistas_analista2) == {opp_ajena.id}


@pytest.mark.parametrize("rol_desconocido", ["desconocido", ""])
def test_rol_no_canonico_y_no_full_read_es_fail_closed(db, escenario, rol_desconocido):
    """Un rol que no es ni canónico ni un alias de lectura completa/analista/
    supervisor/gerente cae fail-closed (clientes_visibles_para -> NONE_): ve 0.
    ('visor'/'viewer' NO entran acá: son alias de lectura completa en
    _ROLES_FULL_READ y ven todo, igual que admin/auditor.)"""
    user = make_user(db, email="rol-raro@suizo.com", role=rol_desconocido)
    vistas = visibilidad.oportunidades_visibles_para(db, user, escenario["todas"])
    assert vistas == []
