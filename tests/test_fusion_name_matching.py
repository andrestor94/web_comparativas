import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from web_comparativas.models import (
    Base,
    CarteraImportRun,
    CarteraOperador,
    CarteraVendedor,
    User,
)
from web_comparativas.fusion_name_matching import resolve_fusion_identity
from web_comparativas.cartera_visibilidad import clientes_visibles_para
from web_comparativas.routers.sic_router import _CarteraConflictError, _validated_fusion_link


@pytest.fixture()
def db():
    engine = create_engine(
        'sqlite:///:memory:',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        engine,
        tables=[CarteraImportRun.__table__, User.__table__, CarteraOperador.__table__, CarteraVendedor.__table__],
    )
    session = sessionmaker(bind=engine)()
    session.add_all([
        CarteraOperador(codigo_cliente='A1', operador_codigo='4071', operador_nombre='AYELEN PILUSO'),
        CarteraOperador(codigo_cliente='D1', operador_codigo='3162', operador_nombre='DANIELA ARMILLO'),
        CarteraVendedor(codigo_cliente='D2', vendedor_codigo='30300', vendedor_nombre='DANIELA ARMILLIO', unineg='6'),
        CarteraVendedor(codigo_cliente='J1', vendedor_codigo='21103', vendedor_nombre='JUAN PEREZ', unineg='6'),
        CarteraVendedor(codigo_cliente='J2', vendedor_codigo='30259', vendedor_nombre='PEREZ, JUAN', unineg='7'),
        CarteraVendedor(codigo_cliente='G1', vendedor_codigo='10', vendedor_nombre='GONZALEZ OMAR', unineg='6'),
        CarteraVendedor(codigo_cliente='G2', vendedor_codigo='11', vendedor_nombre='ROMAN GONZALEZ', unineg='6'),
    ])
    session.commit()
    yield session
    session.close()


def test_exact_inverted_and_accents(db):
    assert resolve_fusion_identity(db, 'Ayelén Piluso')['status'] == 'exact'
    inverted = resolve_fusion_identity(db, 'PILUSO, AYELEN')
    assert inverted['status'] == 'exact'
    assert inverted['match'].operator_codes == {'4071'}


def test_cross_source_typo_requires_confirmation_before_combining(db):
    result = resolve_fusion_identity(db, 'Daniela Armilio')
    assert result['status'] == 'merge_confirmation_required'
    assert result['match'] is None
    assert {item['name'] for item in result['candidates']} == {'DANIELA ARMILLO', 'DANIELA ARMILLIO'}

    confirmed = resolve_fusion_identity(db, 'Daniela Armilio', result['merge_key'])
    assert confirmed['status'] == 'merge_confirmed'
    assert confirmed['match'].operator_codes == {'3162'}
    assert confirmed['match'].seller_codes == {'30300'}

    with pytest.raises(_CarteraConflictError) as blocked:
        _validated_fusion_link(db, 'Daniela Armilio', True, '')
    assert blocked.value.err_code == 'fusion_confirmacion_requerida'
    assert _validated_fusion_link(db, 'Daniela Armilio', True, result['merge_key']) == (
        True, result['merge_key']
    )


def test_same_person_multiple_codes_are_deduplicated(db):
    result = resolve_fusion_identity(db, 'Juan Perez')
    assert result['status'] == 'exact'
    assert result['match'].seller_codes == {'21103', '30259'}


def test_ambiguous_typo_requires_admin_choice(db):
    result = resolve_fusion_identity(db, 'Gonzalez Oma')
    assert result['status'] == 'ambiguous'
    assert result['match'] is None
    assert len(result['candidates']) >= 2
    selected = resolve_fusion_identity(db, 'Gonzalez Oma', result['candidates'][0]['key'])
    assert selected['status'] == 'selected'


def test_low_confidence_is_fail_closed(db):
    result = resolve_fusion_identity(db, 'Persona Inexistente')
    assert result['status'] == 'not_found'
    user = User(
        email='none@example.com', name='Persona Inexistente', role='analista',
        cartera_fusion_enabled=True, cartera_operador_codigos=[], cartera_vendedor_codigos=[],
    )
    db.add(user)
    db.commit()
    scope = clientes_visibles_para(db, user)
    assert scope.unrestricted is False
    assert scope.codigos_cliente == frozenset()


def test_reload_changes_codes_without_editing_user(db):
    user = User(
        email='reload@example.com', name='Juan Perez', role='analista',
        cartera_fusion_enabled=True, cartera_operador_codigos=[], cartera_vendedor_codigos=[],
    )
    db.add(user)
    db.commit()
    assert clientes_visibles_para(db, user).codigos_cliente == frozenset({'J1', 'J2'})

    db.query(CarteraVendedor).filter(CarteraVendedor.vendedor_codigo == '21103').delete()
    db.add(CarteraVendedor(
        codigo_cliente='J3', vendedor_codigo='30999', vendedor_nombre='JUAN PEREZ', unineg='6'
    ))
    db.commit()
    assert clientes_visibles_para(db, user).codigos_cliente == frozenset({'J2', 'J3'})


def test_runtime_link_manual_exception_supervisor_and_admin(db):
    merge_key = resolve_fusion_identity(db, 'Daniela Armilio')['merge_key']
    unconfirmed = User(
        email='unconfirmed@example.com', name='Daniela Armilio', role='analista',
        cartera_fusion_enabled=True, cartera_operador_codigos=[], cartera_vendedor_codigos=[],
    )
    analyst = User(
        email='daniela@example.com', name='Daniela Armilio', role='analista',
        cartera_fusion_enabled=True, cartera_operador_codigos=[], cartera_vendedor_codigos=['21103'],
        cartera_fusion_identidad=merge_key,
    )
    supervisor = User(
        email='sup@example.com', name='Daniela Armilio', role='supervisor',
        cartera_fusion_enabled=True, cartera_operador_codigos=[], cartera_vendedor_codigos=[],
        cartera_unineg_scope=['6'], cartera_fusion_identidad=merge_key,
    )
    admin = User(
        email='admin@example.com', name='Persona Inexistente', role='admin',
        cartera_fusion_enabled=True,
    )
    db.add_all([unconfirmed, analyst, supervisor, admin])
    db.commit()

    assert clientes_visibles_para(db, unconfirmed).codigos_cliente == frozenset()
    assert clientes_visibles_para(db, analyst).codigos_cliente == frozenset({'D1', 'D2', 'J1'})
    # D1 llega por operador y entra completo aunque no tenga relación en unidad 6:
    # cartera_unineg_scope acota EXCLUSIVAMENTE el lado vendedor (operadores_comerciales.csv
    # no trae unidad de negocio, así que ese lado nunca se filtra por esto — fix 2026-08-24).
    assert clientes_visibles_para(db, supervisor).codigos_cliente == frozenset({'D1', 'D2'})
    assert clientes_visibles_para(db, admin).unrestricted is True


def test_confirmed_merge_keeps_accounts_dynamic_after_reload(db):
    merge_key = resolve_fusion_identity(db, 'Daniela Armilio')['merge_key']
    user = User(
        email='confirmed-reload@example.com', name='Daniela Armilio', role='analista',
        cartera_fusion_enabled=True, cartera_fusion_identidad=merge_key,
        cartera_operador_codigos=[], cartera_vendedor_codigos=[],
    )
    db.add(user)
    db.commit()
    assert clientes_visibles_para(db, user).codigos_cliente == frozenset({'D1', 'D2'})

    db.add(CarteraVendedor(
        codigo_cliente='D3', vendedor_codigo='30300', vendedor_nombre='DANIELA ARMILLIO', unineg='6'
    ))
    db.commit()
    assert clientes_visibles_para(db, user).codigos_cliente == frozenset({'D1', 'D2', 'D3'})

    user.name = 'Persona Diferente'
    db.commit()
    assert clientes_visibles_para(db, user).codigos_cliente == frozenset()
