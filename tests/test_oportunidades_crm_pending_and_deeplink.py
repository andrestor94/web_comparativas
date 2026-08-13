from __future__ import annotations

import datetime as dt
import json
import os
import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base
from web_comparativas.dimensionamiento.models import (
    CrmEnvio,
    DimensionamientoImportRun,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id
from web_comparativas.routers import oportunidades_router as router


@pytest.fixture()
def db():
    engine = create_engine(
        'sqlite:///:memory:', future=True,
        connect_args={'check_same_thread': False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        DimensionamientoImportRun.__table__,
        OportunidadSummary.__table__,
        CrmEnvio.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        yield session


def add_opportunity(db, row_id: int, client: str, code: str) -> OportunidadSummary:
    row = OportunidadSummary(
        id=row_id, import_run_id=1, codigo_articulo=code, cliente_visible=client,
        cuenta_interna=f'CTA-{row_id}', producto_nombre=f'Producto {row_id}',
        familia='Familia', unidad_negocio='Unidad', plataforma='Portal',
        tipo_oportunidad='ESTABLE', estado_actividad='ACTIVA',
        meses_demanda_cliente_12m=3, meses_no_participo_12m=3, ventana_meses=12,
        consumo_tipico_mensual=10, consumo_min_mensual=5, consumo_max_mensual=15,
        meses_desde_ultima_demanda=1, precio_unitario_estimado=100,
        monto_oportunidad=1000, efectividad=0.5, ganados=2, comprado_otra=1,
        en_espera=0, clientes_distintos=2, tipo_multiplicador=1,
        multiplicador_actividad=1, score=1000,
    )
    db.add(row)
    return row


def add_sent(db, opportunity: OportunidadSummary, mode: str, *, raw_client=None, raw_code=None):
    oid = opportunity_stable_id(opportunity.cliente_visible, opportunity.codigo_articulo)
    db.add(CrmEnvio(
        oportunidad_id=oid,
        cliente_visible=raw_client if raw_client is not None else opportunity.cliente_visible,
        codigo_articulo=raw_code if raw_code is not None else opportunity.codigo_articulo,
        unidad_negocio=opportunity.unidad_negocio, enviado_por='sender@example.com',
        enviado_at=dt.datetime(2026, 8, 1, 12, 0), crm_status='CREADA',
        crm_id=f'crm-{mode}-{opportunity.id}', crm_modo=mode,
        crm_assigned_usuario='Asignado', crm_assigned_origen='match',
        payload_snapshot=json.dumps({'amount': opportunity.monto_oportunidad}),
    ))
    return oid


def configure_list(monkeypatch, mode: str):
    monkeypatch.setattr(router, 'OPORTUNIDADES_ENABLED', lambda: True)
    monkeypatch.setattr(router, '_modo_envio_actual', lambda: mode)
    monkeypatch.setattr(router, '_latest_success_import_run', lambda _db: SimpleNamespace(id=1))
    monkeypatch.setattr(router, '_window_meta', lambda _db, _run_id: {'label': 'periodo'})
    monkeypatch.setattr(router, '_contexto_asignacion_seguro', lambda *_args: {
        'match': None, 'usuarios': [], 'sugerido_id': None, 'error': None,
        'puede_elegir': True, 'email': 'viewer@example.com',
    })


def test_main_list_excludes_only_current_environment_and_kpis_use_pending(db, monkeypatch):
    never = add_opportunity(db, 1, 'Nunca', 'A')
    only_test = add_opportunity(db, 2, 'Solo Test', 'B')
    only_prod = add_opportunity(db, 3, 'Solo Prod', 'C')
    both = add_opportunity(db, 4, 'Ambos', 'D')
    add_sent(db, only_test, 'test', raw_client='  SOLO TEST ', raw_code=' b ')
    add_sent(db, only_prod, 'prod')
    add_sent(db, both, 'test')
    add_sent(db, both, 'prod')
    db.commit()

    expected = {
        'test': {opportunity_stable_id('Nunca', 'A'), opportunity_stable_id('Solo Prod', 'C')},
        'prod': {opportunity_stable_id('Nunca', 'A'), opportunity_stable_id('Solo Test', 'B')},
    }
    user = SimpleNamespace(email='viewer@example.com', role='supervisor')
    for mode in ('test', 'prod'):
        configure_list(monkeypatch, mode)
        response = router.oportunidades_list(request=object(), _user=user, db=db)
        rows = response['data']['rows']
        assert {row['oportunidad_id'] for row in rows} == expected[mode]
        assert response['data']['total'] == len(rows) == 2
        assert response['data']['completeness']['total'] == len(rows)
        assert all(row['envio'] == {'enviado': False} for row in rows)

def test_main_list_http_endpoint_returns_only_pending_rows_and_consistent_totals(db, monkeypatch):
    never = add_opportunity(db, 1, 'Nunca', 'A')
    only_test = add_opportunity(db, 2, 'Solo Test', 'B')
    only_prod = add_opportunity(db, 3, 'Solo Prod', 'C')
    both = add_opportunity(db, 4, 'Ambos', 'D')
    add_sent(db, only_test, 'test')
    add_sent(db, only_prod, 'prod')
    add_sent(db, both, 'test')
    add_sent(db, both, 'prod')
    db.commit()

    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._perm_oportunidades] = lambda: SimpleNamespace(
        email='viewer@example.com', role='supervisor'
    )
    app.dependency_overrides[router.get_db] = lambda: db

    expected = {
        'test': {opportunity_stable_id('Nunca', 'A'), opportunity_stable_id('Solo Prod', 'C')},
        'prod': {opportunity_stable_id('Nunca', 'A'), opportunity_stable_id('Solo Test', 'B')},
    }
    with TestClient(app) as client:
        for mode in ('test', 'prod'):
            configure_list(monkeypatch, mode)
            response = client.get('/api/mercado-privado/oportunidades/list')
            assert response.status_code == 200
            body = response.json()
            assert body['ok'] is True
            data = body['data']
            rows = data['rows']
            assert data['crm_modo'] == mode
            assert {row['oportunidad_id'] for row in rows} == expected[mode]
            assert all(
                row['oportunidad_id'] == row['crm']['payload']['id_sistema_origen_c']
                for row in rows
            )
            assert data['total'] == data['completeness']['total'] == len(rows) == 2
            assert sum(row['monto_oportunidad'] for row in rows) == 2000
            assert all(row['envio'] == {'enviado': False} for row in rows)


def test_main_list_frontend_filters_cannot_reintroduce_sent_rows(db, monkeypatch):
    pending = add_opportunity(db, 1, 'Pendiente buscable', 'A')
    sent = add_opportunity(db, 2, 'Enviada buscable', 'B')
    sent_id = add_sent(db, sent, 'test')
    db.commit()
    configure_list(monkeypatch, 'test')

    response = router.oportunidades_list(
        request=object(),
        _user=SimpleNamespace(email='viewer@example.com', role='supervisor'),
        db=db,
    )
    returned_ids = {row['oportunidad_id'] for row in response['data']['rows']}
    assert opportunity_stable_id(pending.cliente_visible, pending.codigo_articulo) in returned_ids
    assert sent_id not in returned_ids


def test_deep_link_is_exact_environment_scoped_and_re_resolves(db, monkeypatch):
    monkeypatch.setenv('CRM_BASE_URL', 'https://crm.example.test')
    opportunity = add_opportunity(db, 1, 'Cliente', 'ART-1')
    oid = add_sent(db, opportunity, 'test')
    db.commit()
    monkeypatch.setattr(router, 'OPORTUNIDADES_ENABLED', lambda: True)

    monkeypatch.setattr(router, '_modo_envio_actual', lambda: 'test')
    actor = SimpleNamespace(id=900, email='admin@example.com', role='admin')
    found = router.oportunidad_enviada_detalle(object(), oid, actor, db)
    assert found['data']['found'] is True
    assert found['data']['row']['oportunidad_id'] == oid
    assert found['data']['row']['crm_modo'] == 'test'
    assert found['data']['row']['crm_url'].endswith(f'record=crm-test-{opportunity.id}')

    different_case = router.oportunidad_enviada_detalle(object(), oid.upper(), actor, db)
    assert different_case['data']['found'] is False
    hostile = router.oportunidad_enviada_detalle(object(), chr(39) + ' OR 1=1 --', actor, db)
    assert hostile['data']['found'] is False

    monkeypatch.setattr(router, '_modo_envio_actual', lambda: 'prod')
    other_environment = router.oportunidad_enviada_detalle(object(), oid, actor, db)
    assert other_environment['data']['found'] is False
    assert other_environment['data']['crm_modo'] == 'prod'


def test_repository_frontend_navigates_to_dedicated_detail_page():
    path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'web_comparativas', 'static', 'js',
        'mercado_privado_oportunidades_enviadas.js',
    )
    with open(path, encoding='utf-8') as source_file:
        source = source_file.read()
    assert 'params.has(' + chr(39) + 'oportunidad_id' + chr(39) + ')' in source
    assert 'detailPageUrl(row)' in source
    assert 'encodeURIComponent(row.oportunidad_id)' in source
    assert 'window.location.href = detailPageUrl(row)' in source
    assert "data-crm-modo='${esc(r.crm_modo || \"\")}'" in source
    assert "(item.crm_modo || '') === tr.dataset.crmModo" in source
    assert 'window.location.replace(detailPageUrl({ oportunidad_id: requestedId }))' in source
    assert 'bootstrap.Modal' not in source
    assert 'envDetailModal' not in source

    detail_template = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'web_comparativas', 'templates',
        'mercado_privado_oportunidad_enviada_detalle.html',
    )
    with open(detail_template, encoding='utf-8') as template_file:
        template = template_file.read()
    for heading in (
        'Cliente', 'Producto', 'Demanda', 'Desempe&ntilde;o comercial',
        'Valorizaci&oacute;n', 'Env&iacute;o y CRM',
    ):
        assert heading in template
    for internal_label in (
        'Identificador SIEM', 'Confianza', 'Criterio', 'Fuente',
        'Cuentas candidatas', 'Cuentas evaluadas',
    ):
        assert internal_label not in template
