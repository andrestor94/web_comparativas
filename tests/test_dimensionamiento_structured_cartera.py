import datetime as dt

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from web_comparativas.cartera_visibilidad import clientes_visibles_para
from web_comparativas.dimensionamiento.models import (
    DimensionamientoImportRun,
    DimensionamientoRecord,
)
from web_comparativas.dimensionamiento.query_service import (
    DimensionamientoFilters,
    _apply_common_filters,
    get_status,
    invalidate_query_cache,
)
from web_comparativas.models import (
    Base,
    CarteraImportRun,
    CarteraOperador,
    CarteraVendedor,
    User,
    UserReporte,
)
from web_comparativas.routers.sic_router import (
    _CarteraConflictError,
    _assert_hierarchy_link_safe,
    _clear_incompatible_parent,
)


@pytest.fixture()
def db():
    engine = create_engine(
        'sqlite:///:memory:',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        CarteraImportRun.__table__,
        User.__table__,
        UserReporte.__table__,
        CarteraOperador.__table__,
        CarteraVendedor.__table__,
        DimensionamientoImportRun.__table__,
        DimensionamientoRecord.__table__,
    ])
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _user(db, email, role, parent=None, parents=None, sellers=None, units=None):
    """`parent` (uno solo, retrocompatible) o `parents` (varios, M:N 2026-08-25) —
    ambos se traducen a filas en `user_reportes`, no a `reporta_a_id` (congelada)."""
    user = User(
        email=email,
        name=email.split('@')[0],
        role=role,
        password_hash='x',
        cartera_vendedor_codigos=sellers or [],
        cartera_operador_codigos=[],
        cartera_unineg_scope=units or [],
    )
    db.add(user)
    db.flush()
    for p in list(parents or []) + ([parent] if parent else []):
        db.add(UserReporte(subordinado_id=user.id, superior_id=p.id))
    db.flush()
    return user


def test_manager_two_supervisor_branches_do_not_cross_units(db):
    manager = _user(db, 'manager@test', 'gerente')
    sup_a = _user(db, 'sup-a@test', 'supervisor', manager, sellers=['SA'], units=['A'])
    sup_b = _user(db, 'sup-b@test', 'supervisor', manager, sellers=['SB'], units=['B'])
    _user(db, 'analyst-a@test', 'analista', sup_a, sellers=['AA'])
    _user(db, 'analyst-b@test', 'analista', sup_b, sellers=['AB'])
    db.add_all([
        CarteraVendedor(codigo_cliente='C1', vendedor_codigo='SA', unineg='A'),
        CarteraVendedor(codigo_cliente='C2', vendedor_codigo='AA', unineg='A'),
        CarteraVendedor(codigo_cliente='C3', vendedor_codigo='SB', unineg='B'),
        CarteraVendedor(codigo_cliente='C4', vendedor_codigo='AB', unineg='B'),
    ])
    db.commit()

    scope = clientes_visibles_para(db, manager)
    assert scope.unrestricted is False
    assert len(scope.branches) == 2
    by_root = {branch.root_user_id: branch for branch in scope.branches}
    assert by_root[sup_a.id].unidades_negocio == frozenset({'A'})
    assert by_root[sup_a.id].codigos_cliente == frozenset({'C1', 'C2'})
    assert by_root[sup_b.id].unidades_negocio == frozenset({'B'})
    assert by_root[sup_b.id].codigos_cliente == frozenset({'C3', 'C4'})

    run = DimensionamientoImportRun(source_path='test.csv', mode='replace', status='success')
    db.add(run)
    db.flush()
    for row_id, entity_id, unit in [
        ('allowed-a', 1, 'UN-A'),
        ('cross-a', 1, 'UN-B'),
        ('allowed-b', 2, 'UN-B'),
        ('cross-b', 2, 'UN-A'),
    ]:
        db.add(DimensionamientoRecord(
            id_registro_unico=row_id,
            fecha=dt.date(2026, 1, 1),
            plataforma='P',
            cliente_entidad_id=entity_id,
            cuenta_interna='C1' if entity_id == 1 else 'C3',
            unidad_negocio=unit,
            import_run_id=run.id,
        ))
    db.commit()

    filters = DimensionamientoFilters(
        import_run_id=run.id,
        cartera_unrestricted=False,
        cartera_branches=((frozenset({'C1'}), frozenset({'UN-A'})),
                           (frozenset({'C3'}), frozenset({'UN-B'}))),
        cartera_entidad_branches=((frozenset({1}), frozenset({'UN-A'})),
                                  (frozenset({2}), frozenset({'UN-B'}))),
    )
    stmt = _apply_common_filters(select(DimensionamientoRecord.id_registro_unico), DimensionamientoRecord, filters)
    assert set(db.execute(stmt).scalars()) == {'allowed-a', 'allowed-b'}

    invalidate_query_cache()
    status = get_status(
        db,
        run.id,
        allowed_cartera_branches=(
            (frozenset({'C1'}), frozenset({'UN-A'})),
            (frozenset({'C3'}), frozenset({'UN-B'})),
        ),
    )
    assert status['total_rows'] == 2


def test_structured_scope_empty_is_fail_closed(db):
    manager = _user(db, 'empty-manager@test', 'gerente')
    supervisor = _user(db, 'empty-supervisor@test', 'supervisor', sellers=['S'], units=[])
    db.add(CarteraVendedor(codigo_cliente='C9', vendedor_codigo='S', unineg='A'))
    db.commit()
    assert clientes_visibles_para(db, manager).branches == ()
    supervisor_scope = clientes_visibles_para(db, supervisor)
    assert supervisor_scope.codigos_cliente == frozenset()
    assert supervisor_scope.branches[0].unidades_negocio == frozenset()

    filters = DimensionamientoFilters(
        cartera_unrestricted=False,
        cartera_branches=(),
        cartera_entidad_branches=(),
    )
    stmt = _apply_common_filters(select(DimensionamientoRecord.id), DimensionamientoRecord, filters)
    assert db.execute(stmt).scalars().all() == []


def test_sic_rejects_cycles_and_detaches_incompatible_parent(db):
    manager = _user(db, 'manager@test', 'gerente')
    supervisor = _user(db, 'supervisor@test', 'supervisor', manager)
    analyst = _user(db, 'analyst@test', 'analista', supervisor)
    db.commit()

    with pytest.raises(_CarteraConflictError) as error:
        _assert_hierarchy_link_safe(db, analyst, manager)
    assert error.value.err_code == 'jerarquia_ciclo'

    analyst.role = 'gerente'
    _clear_incompatible_parent(db, analyst, 'gerente')
    assert db.query(UserReporte).filter(UserReporte.subordinado_id == analyst.id).count() == 0


def test_shared_analyst_under_two_supervisors_unions_without_duplication(db):
    """Caso real (Daniela y Yanina comparten analistas, ago-2026): un mismo
    analista con DOS supervisores a cargo aporta su cartera a las DOS ramas, sin
    romper nada ni duplicarse en el total agregado (branches en frozenset)."""
    gerente = _user(db, 'gerente@test', 'gerente')
    # units=['0']: la UN acota SOLO la cartera propia del lado vendedor (ver
    # cartera_visibilidad.py) — sin esto, Daniela/Yanina quedarían fail-closed en
    # su propia cartera aunque la del analista compartido igual les llegue bien.
    daniela = _user(db, 'daniela@test', 'supervisor', gerente, sellers=['VD'], units=['0'])
    yanina = _user(db, 'yanina@test', 'supervisor', gerente, sellers=['VY'], units=['0'])
    compartido = _user(db, 'compartido@test', 'analista', parents=[daniela, yanina], sellers=['VC'])
    db.add_all([
        CarteraVendedor(codigo_cliente='CD', vendedor_codigo='VD', unineg='0'),
        CarteraVendedor(codigo_cliente='CY', vendedor_codigo='VY', unineg='0'),
        CarteraVendedor(codigo_cliente='CC', vendedor_codigo='VC', unineg='0'),
    ])
    db.commit()

    scope_daniela = clientes_visibles_para(db, daniela)
    scope_yanina = clientes_visibles_para(db, yanina)
    assert scope_daniela.codigos_cliente == frozenset({'CD', 'CC'})
    assert scope_yanina.codigos_cliente == frozenset({'CY', 'CC'})

    gerente_scope = clientes_visibles_para(db, gerente)
    assert len(gerente_scope.branches) == 2  # una rama por supervisor, CC aparece en las dos
    by_root = {b.root_user_id: b for b in gerente_scope.branches}
    assert by_root[daniela.id].codigos_cliente == frozenset({'CD', 'CC'})
    assert by_root[yanina.id].codigos_cliente == frozenset({'CY', 'CC'})
    # El agregado del gerente NO duplica CC (frozenset union) — 3 cuentas, no 4.
    assert gerente_scope.codigos_cliente == frozenset({'CD', 'CY', 'CC'})
    assert len(gerente_scope.codigos_cliente) == 3


def test_cartera_accepts_only_the_five_canonical_siem_roles(db):
    gerente = _user(db, 'gerente@test', 'gerente')
    supervisor = _user(
        db, 'supervisor@test', 'supervisor', gerente, sellers=['VS'], units=['U1']
    )
    analista = _user(db, 'analista@test', 'analista', supervisor, sellers=['VA'])
    admin = _user(db, 'admin@test', 'admin')
    auditor = _user(db, 'auditor@test', 'auditor')
    db.add_all([
        CarteraVendedor(codigo_cliente='CA', vendedor_codigo='VA', unineg='U1'),
        CarteraVendedor(codigo_cliente='CS', vendedor_codigo='VS', unineg='U1'),
    ])
    db.commit()

    assert clientes_visibles_para(db, analista).codigos_cliente == frozenset({'CA'})
    assert clientes_visibles_para(db, supervisor).codigos_cliente == frozenset({'CA', 'CS'})
    gerente_scope = clientes_visibles_para(db, gerente)
    assert gerente_scope.unrestricted is False
    assert len(gerente_scope.branches) == 1
    assert gerente_scope.branches[0].codigos_cliente == frozenset({'CA', 'CS'})
    assert gerente_scope.branches[0].unidades_negocio == frozenset({'U1'})
    assert clientes_visibles_para(db, auditor).unrestricted is True
    assert clientes_visibles_para(db, admin).unrestricted is True


@pytest.mark.parametrize(
    'legacy_role',
    ['visor', 'viewer', 'manager', 'analyst', 'administrator', 'administrador', 'desconocido'],
)
def test_legacy_or_unknown_role_is_fail_closed(db, legacy_role):
    user = _user(db, f'{legacy_role}@test', legacy_role, sellers=['VA'], units=['U1'])
    db.add(CarteraVendedor(codigo_cliente='CA', vendedor_codigo='VA', unineg='U1'))
    db.commit()

    scope = clientes_visibles_para(db, user)
    assert scope.unrestricted is False
    assert scope.codigos_cliente == frozenset()
    assert scope.branches == ()
