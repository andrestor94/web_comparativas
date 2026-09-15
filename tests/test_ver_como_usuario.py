from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, CarteraOperador, CarteraVendedor, User, UserReporte
from web_comparativas.cartera_visibilidad import (
    clientes_visibles_para,
    clientes_visibles_para_usuarios,
    invalidate_cartera_scope_cache,
    resolve_effective_scope,
    usuarios_con_cartera,
)


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        User.__table__, UserReporte.__table__, CarteraOperador.__table__, CarteraVendedor.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        invalidate_cartera_scope_cache()
        yield session
        # Limpieza al salir: el caché de cartera_visibilidad.py es un dict GLOBAL
        # de proceso, cacheado por user.id — sin este invalidate, los ids (1, 2, 3…)
        # de esta DB en memoria quedan cacheados y contaminan al próximo archivo de
        # test que corra en la misma sesión de pytest, que crea sus propios usuarios
        # con esos mismos ids autoincrementales pero en OTRA DB en memoria.
        invalidate_cartera_scope_cache()


def make_user(
    db, *, email, role, operador_codigos=None, vendedor_codigos=None,
    unineg_scope=None, superior_ids=None,
) -> User:
    u = User(
        email=email, name=email.split("@")[0], role=role, password_hash="x",
        cartera_operador_codigos=operador_codigos or [],
        cartera_vendedor_codigos=vendedor_codigos or [],
        cartera_unineg_scope=unineg_scope if unineg_scope is not None else [],
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    for superior_id in (superior_ids or []):
        db.add(UserReporte(subordinado_id=u.id, superior_id=superior_id))
    if superior_ids:
        db.commit()
    return u


@pytest.fixture()
def escenario(db):
    """(a) Un analista con cartera de VENDEDOR (cuenta AAA), (b) un analista con
    cartera de OPERADOR (cuenta BBB, Mercado Público) — casos de prueba pedidos
    para "Ver como usuario"."""
    admin = make_user(db, email="admin@suizo.com", role="admin")
    otro_admin = make_user(db, email="admin2@suizo.com", role="admin")
    auditor = make_user(db, email="auditor@suizo.com", role="auditor")
    vendedor_user = make_user(
        db, email="vendedor@suizo.com", role="analista", vendedor_codigos=["V-1"],
    )
    operador_user = make_user(
        db, email="operador@suizo.com", role="analista", operador_codigos=["OP-1"],
    )
    sin_cartera = make_user(db, email="sincartera@suizo.com", role="analista")

    db.add_all([
        CarteraVendedor(codigo_cliente="AAA", vendedor_codigo="V-1", unineg="0"),
        CarteraOperador(codigo_cliente="BBB", operador_codigo="OP-1"),
    ])
    db.commit()

    return {
        "admin": admin, "otro_admin": otro_admin, "auditor": auditor,
        "vendedor_user": vendedor_user, "operador_user": operador_user,
        "sin_cartera": sin_cartera,
    }


def test_admin_sin_seleccion_ve_todo(db, escenario):
    scope = resolve_effective_scope(db, escenario["admin"], None)
    assert scope.unrestricted is True


def test_admin_con_lista_vacia_ve_todo(db, escenario):
    scope = resolve_effective_scope(db, escenario["admin"], [])
    assert scope.unrestricted is True


def test_no_admin_ignora_la_seleccion_server_side(db, escenario):
    """Seguridad: un usuario NO admin que mande ver_como_user_ids (front manipulado
    o bug) obtiene exactamente su propio scope, como si no hubiera mandado nada."""
    propio = clientes_visibles_para(db, escenario["vendedor_user"])
    con_seleccion = resolve_effective_scope(
        db, escenario["vendedor_user"], [escenario["operador_user"].id]
    )
    assert con_seleccion.unrestricted == propio.unrestricted
    assert con_seleccion.codigos_cliente == propio.codigos_cliente
    assert con_seleccion.codigos_cliente == frozenset({"AAA"})


def test_admin_ver_como_un_usuario_con_cartera_de_vendedor(db, escenario):
    """Caso (a): admin selecciona un usuario con cartera de VENDEDOR."""
    scope = resolve_effective_scope(db, escenario["admin"], [escenario["vendedor_user"].id])
    assert scope.unrestricted is False
    assert scope.codigos_cliente == frozenset({"AAA"})


def test_admin_ver_como_un_usuario_con_cartera_de_operador(db, escenario):
    """Caso (b): admin selecciona un usuario con cartera de OPERADOR (Mercado
    Público)."""
    scope = resolve_effective_scope(db, escenario["admin"], [escenario["operador_user"].id])
    assert scope.unrestricted is False
    assert scope.codigos_cliente == frozenset({"BBB"})


def test_admin_ver_como_dos_usuarios_a_la_vez_es_la_union(db, escenario):
    """Caso (c): dos usuarios seleccionados a la vez -> unión de sus carteras."""
    scope = resolve_effective_scope(
        db, escenario["admin"],
        [escenario["vendedor_user"].id, escenario["operador_user"].id],
    )
    assert scope.unrestricted is False
    assert scope.codigos_cliente == frozenset({"AAA", "BBB"})
    assert len(scope.branches) == 2


def test_admin_ver_como_usuario_sin_cartera_es_fail_closed(db, escenario):
    scope = resolve_effective_scope(db, escenario["admin"], [escenario["sin_cartera"].id])
    assert scope.unrestricted is False
    assert scope.codigos_cliente == frozenset()


def test_admin_ver_como_incluye_otro_admin_cae_a_unrestricted(db, escenario):
    """Si dentro de la selección hay un usuario que YA es unrestricted por sí mismo
    (admin/auditor), la unión es unrestricted -- mismo criterio que si ese usuario
    mirara el módulo por su cuenta."""
    scope = resolve_effective_scope(
        db, escenario["admin"],
        [escenario["vendedor_user"].id, escenario["otro_admin"].id],
    )
    assert scope.unrestricted is True


def test_auditor_no_puede_usar_ver_como_aunque_tenga_lectura_total(db, escenario):
    """Solo el rol admin puede usar "Ver como usuario" (spec: visible SOLO para
    admin) -- auditor, aunque también tenga lectura total, no lo activa."""
    scope = resolve_effective_scope(db, escenario["auditor"], [escenario["vendedor_user"].id])
    assert scope.unrestricted is True  # su propio scope de auditor, no la selección


def test_clientes_visibles_para_usuarios_vacio_es_fail_closed(db, escenario):
    assert clientes_visibles_para_usuarios(db, []).unrestricted is False
    assert clientes_visibles_para_usuarios(db, []).codigos_cliente == frozenset()


def test_ver_como_ids_no_iterables_se_ignoran_sin_explotar(db, escenario):
    """Defensivo: un valor no-lista (p.ej. el propio objeto fastapi.Depends(...) que
    Python usa como default cuando el endpoint se llama directo, fuera del ciclo de
    request) se trata como "sin selección" en vez de romper."""
    class NotAList:
        pass

    scope = resolve_effective_scope(db, escenario["admin"], NotAList())
    assert scope.unrestricted is True


# ─────────────────────────────────────────────────────────────────────────────
# usuarios_con_cartera — padrón acotado a quien tiene >=1 cuenta (sep-2026).
# ─────────────────────────────────────────────────────────────────────────────

def test_usuarios_con_cartera_excluye_admin_auditor_y_sin_cartera(db, escenario):
    incluidos = usuarios_con_cartera(db)
    assert incluidos == frozenset({escenario["vendedor_user"].id, escenario["operador_user"].id})
    assert escenario["admin"].id not in incluidos
    assert escenario["otro_admin"].id not in incluidos
    assert escenario["auditor"].id not in incluidos
    assert escenario["sin_cartera"].id not in incluidos


@pytest.fixture()
def escenario_jerarquia(db):
    """Supervisor/Gerente con distintas combinaciones de cartera propia vs. de
    equipo, para probar que `usuarios_con_cartera` sigue la MISMA regla que
    `clientes_visibles_para` (jerarquía manda; unineg fail-closed solo del lado
    vendedor y solo para la cartera PROPIA)."""
    gerente_con_rama = make_user(db, email="gerente-con@suizo.com", role="gerente")
    gerente_vacio = make_user(db, email="gerente-vacio@suizo.com", role="gerente")

    # Supervisor con cartera PROPIA vía operador (no depende de unineg).
    supervisor_propia = make_user(
        db, email="sup-propia@suizo.com", role="supervisor",
        operador_codigos=["OP-SUP"], superior_ids=[gerente_con_rama.id],
    )
    # Supervisor SIN cartera propia (vendedor propio pero unineg_scope vacío ->
    # fail-closed del lado vendedor) pero con un analista a cargo que SÍ tiene.
    supervisor_solo_equipo = make_user(
        db, email="sup-equipo@suizo.com", role="supervisor",
        vendedor_codigos=["V-SUP-SIN-UN"], unineg_scope=[],
        superior_ids=[gerente_con_rama.id],
    )
    analista_del_equipo = make_user(
        db, email="an-equipo@suizo.com", role="analista",
        vendedor_codigos=["V-AN-EQUIPO"], superior_ids=[supervisor_solo_equipo.id],
    )
    # Supervisor totalmente vacío (ni propia ni equipo) -> excluido, y el gerente
    # que solo tiene a ESTE supervisor a cargo también queda excluido.
    supervisor_vacio = make_user(
        db, email="sup-vacio@suizo.com", role="supervisor",
        superior_ids=[gerente_vacio.id],
    )
    # Supervisor con vendedor propio Y unineg_scope que matchea -> tiene cartera propia.
    supervisor_con_un = make_user(
        db, email="sup-con-un@suizo.com", role="supervisor",
        vendedor_codigos=["V-SUP-UN"], unineg_scope=["4"],
    )

    db.add_all([
        CarteraOperador(codigo_cliente="OPCTA", operador_codigo="OP-SUP"),
        # V-SUP-SIN-UN SÍ tiene cuentas en el padrón, pero con un unineg que el
        # supervisor no tiene asignado (unineg_scope=[]) -> fail-closed del lado
        # propio; su inclusión depende ENTERAMENTE del analista de su equipo.
        CarteraVendedor(codigo_cliente="ZZZ", vendedor_codigo="V-SUP-SIN-UN", unineg="9"),
        CarteraVendedor(codigo_cliente="EQ1", vendedor_codigo="V-AN-EQUIPO", unineg="0"),
        CarteraVendedor(codigo_cliente="UNM", vendedor_codigo="V-SUP-UN", unineg="4"),
    ])
    db.commit()

    return {
        "gerente_con_rama": gerente_con_rama, "gerente_vacio": gerente_vacio,
        "supervisor_propia": supervisor_propia, "supervisor_solo_equipo": supervisor_solo_equipo,
        "analista_del_equipo": analista_del_equipo, "supervisor_vacio": supervisor_vacio,
        "supervisor_con_un": supervisor_con_un,
    }


def test_usuarios_con_cartera_matches_ground_truth_en_escenario_jerarquia(db, escenario_jerarquia):
    """Cross-check contra `clientes_visibles_para` (ground truth, lenta) para TODOS
    los usuarios del escenario — no solo casos puntuales."""
    todos = db.query(User).all()
    invalidate_cartera_scope_cache()
    ground_truth = {
        u.id for u in todos
        if not clientes_visibles_para(db, u).unrestricted and clientes_visibles_para(db, u).codigos_cliente
    }
    assert usuarios_con_cartera(db) == ground_truth


def test_usuarios_con_cartera_supervisor_por_cartera_propia(db, escenario_jerarquia):
    assert escenario_jerarquia["supervisor_propia"].id in usuarios_con_cartera(db)


def test_usuarios_con_cartera_supervisor_sin_propia_pero_con_equipo(db, escenario_jerarquia):
    incluidos = usuarios_con_cartera(db)
    assert escenario_jerarquia["supervisor_solo_equipo"].id in incluidos
    assert escenario_jerarquia["analista_del_equipo"].id in incluidos


def test_usuarios_con_cartera_supervisor_vacio_queda_afuera(db, escenario_jerarquia):
    assert escenario_jerarquia["supervisor_vacio"].id not in usuarios_con_cartera(db)


def test_usuarios_con_cartera_supervisor_con_unineg_propio_matchea(db, escenario_jerarquia):
    assert escenario_jerarquia["supervisor_con_un"].id in usuarios_con_cartera(db)


def test_usuarios_con_cartera_gerente_incluido_via_rama_de_equipo(db, escenario_jerarquia):
    """El Gerente no tiene cartera propia — entra solo si ALGUNA rama (Supervisor
    propio o su equipo) tiene cuentas."""
    assert escenario_jerarquia["gerente_con_rama"].id in usuarios_con_cartera(db)


def test_usuarios_con_cartera_gerente_con_ramas_vacias_queda_afuera(db, escenario_jerarquia):
    assert escenario_jerarquia["gerente_vacio"].id not in usuarios_con_cartera(db)
