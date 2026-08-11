from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, User, VendedorFusion
from web_comparativas.dimensionamiento import crm_client
from web_comparativas.dimensionamiento.models import (
    CrmEnvio, CrmEnvioEvento, DimensionamientoImportRun, OportunidadSummary,
)
from web_comparativas.routers import oportunidades_router as router


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        DimensionamientoImportRun.__table__, OportunidadSummary.__table__,
        CrmEnvio.__table__, CrmEnvioEvento.__table__,
        User.__table__, VendedorFusion.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.add(DimensionamientoImportRun(id=1, source_path="test.csv", status="success"))
        session.add(OportunidadSummary(
            id=371, import_run_id=1, codigo_articulo="8111612",
            cliente_visible="SANATORIO JUAN XXIII S.R.L.", cuit="30595201027",
            cuenta_interna="32059", producto_nombre="CEFTAZIDIMA", familia="FAMILIA",
            unidad_negocio="MEDICAMENTOS", plataforma="BIONEXO",
            tipo_oportunidad="INTERMITENTE", estado_actividad="ACTIVA",
            meses_demanda_cliente_12m=3, meses_no_participo_12m=3, ventana_meses=12,
            consumo_tipico_mensual=100, consumo_min_mensual=100, consumo_max_mensual=100,
            meses_desde_ultima_demanda=4, precio_unitario_estimado=100,
            monto_oportunidad=10000, efectividad=0.37, ganados=46, comprado_otra=1,
            en_espera=0, clientes_distintos=25, tipo_multiplicador=1,
            multiplicador_actividad=1, score=10000,
        ))
        session.commit()
        yield session


def selected_resolution(account="8519", *, operador_codigo="4071", operador_nombre="AYELEN PILUSO"):
    selected = {
        "cuenta": account, "crm_account_id": f"crm-account-{account}",
        "operador_codigo": operador_codigo, "operador_nombre": operador_nombre,
        "crm_cuit": None, "crm_razon_social": "SANATORIO JUAN XXIII S.R.L.",
    }
    return {
        "cuit": "30595201027", "cuenta_original": "32059",
        "cuentas_candidatas": [{"cuenta": "32059"}, selected],
        "cantidad_candidatas_total": 2, "cantidad_evaluadas_crm": 2,
        "cuentas_encontradas_en_crm": [selected],
        "cuenta_seleccionada": selected, "criterio_seleccion": "unica_alternativa_valida",
        "estado_confianza": "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO",
        "confianza_label": "verificar antes de enviar",
        "confirmacion_fiscal": False, "seleccion_origen": "automatica_unica_alternativa",
        "fuente_relacion": "test", "trazabilidad_texto": None,
        "bloqueado": False, "requiere_seleccion": False, "crm_modo": "test",
    }


def link_vendedor(db, *, codigo, email, role="analista") -> User:
    u = User(email=email, name=email.split("@")[0], role=role, password_hash="x")
    db.add(u)
    db.flush()
    db.add(VendedorFusion(codigo_vendedor=codigo, nombre_fusion="AYELEN PILUSO", user_id=u.id))
    db.commit()
    return u


FAKE_CRM_USUARIOS = [
    {"id": "crm-ayelen", "usuario": "ayelen.piluso"},
    {"id": "crm-otro", "usuario": "otro.usuario"},
]


def patch_crm_usuarios(monkeypatch):
    monkeypatch.setattr(
        crm_client, "contexto_asignacion",
        lambda email: {
            "match": crm_client.buscar_match_usuario(FAKE_CRM_USUARIOS, email),
            "usuarios": list(FAKE_CRM_USUARIOS), "sugerido_id": None,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# _operador_crm_matches (unidad)
# ─────────────────────────────────────────────────────────────────────────────

def test_operador_crm_matches_encuentra_el_usuario_vinculado(db, monkeypatch):
    link_vendedor(db, codigo="4071", email="ayelen.piluso@suizoargentina.com")
    patch_crm_usuarios(monkeypatch)
    resultado = router._operador_crm_matches(db, {"4071"})
    assert resultado["4071"] == {"id": "crm-ayelen", "usuario": "ayelen.piluso", "origen": "match"}


def test_operador_crm_matches_vacio_si_codigo_no_vinculado(db, monkeypatch):
    patch_crm_usuarios(monkeypatch)
    resultado = router._operador_crm_matches(db, {"9999"})
    assert resultado == {}


def test_operador_crm_matches_traga_error_de_crm(db, monkeypatch):
    link_vendedor(db, codigo="4071", email="ayelen.piluso@suizoargentina.com")
    monkeypatch.setattr(
        crm_client, "contexto_asignacion",
        lambda email: (_ for _ in ()).throw(crm_client.CrmError("caído", kind="red", reintentable=True)),
    )
    resultado = router._operador_crm_matches(db, {"4071"})
    assert resultado == {}


# ─────────────────────────────────────────────────────────────────────────────
# GET /cuentas/{id}: enriquecimiento con operador_asignado_crm
# ─────────────────────────────────────────────────────────────────────────────

def make_client(db, monkeypatch, *, actor_role="supervisor"):
    user = SimpleNamespace(id=99, email="quien.envia@suizoargentina.com", role=actor_role)
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._perm_oportunidades] = lambda: user
    app.dependency_overrides[router._require_oportunidades_write] = lambda: user
    app.dependency_overrides[router.get_db] = lambda: db
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "_latest_success_import_run", lambda _db: SimpleNamespace(id=1))
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "test")
    return TestClient(app)


def test_cuentas_incluye_operador_asignado_crm_cuando_hay_match(db, monkeypatch):
    link_vendedor(db, codigo="4071", email="ayelen.piluso@suizoargentina.com")
    patch_crm_usuarios(monkeypatch)
    client = make_client(db, monkeypatch)
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_a, **_k: selected_resolution())
    r = client.get("/api/mercado-privado/oportunidades/cuentas/371")
    assert r.status_code == 200
    data = r.json()["data"]
    assert data["cuenta_seleccionada"]["operador_asignado_crm"] == {
        "id": "crm-ayelen", "usuario": "ayelen.piluso", "origen": "match",
    }
    assert data["cuentas_encontradas_en_crm"][0]["operador_asignado_crm"]["id"] == "crm-ayelen"


def test_cuentas_operador_asignado_crm_none_si_operador_sin_usuario_crm(db, monkeypatch):
    patch_crm_usuarios(monkeypatch)  # nadie vinculado en VendedorFusion
    client = make_client(db, monkeypatch)
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_a, **_k: selected_resolution())
    r = client.get("/api/mercado-privado/oportunidades/cuentas/371")
    data = r.json()["data"]
    assert data["cuenta_seleccionada"]["operador_asignado_crm"] is None


def test_cuentas_no_calcula_operador_para_analista(db, monkeypatch):
    link_vendedor(db, codigo="4071", email="ayelen.piluso@suizoargentina.com")
    patch_crm_usuarios(monkeypatch)
    client = make_client(db, monkeypatch, actor_role="analista")
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_a, **_k: selected_resolution())
    r = client.get("/api/mercado-privado/oportunidades/cuentas/371")
    data = r.json()["data"]
    # Ni se agrega la clave: un analista nunca ve ni usa esto.
    assert "operador_asignado_crm" not in data["cuenta_seleccionada"]


# ─────────────────────────────────────────────────────────────────────────────
# POST /enviar/{id}: el operador es válido aunque quede fuera de la estructura
# ─────────────────────────────────────────────────────────────────────────────

def test_enviar_acepta_operador_fuera_de_la_estructura_del_supervisor(db, monkeypatch):
    """El supervisor que envía no tiene a ayelen.piluso en su equipo (ctx.usuarios
    acotado no la incluye), pero la cuenta SÍ es de ella (operador 4071) — tiene que
    poder enviarse igual, asignada a ella."""
    link_vendedor(db, codigo="4071", email="ayelen.piluso@suizoargentina.com")
    patch_crm_usuarios(monkeypatch)
    client = make_client(db, monkeypatch, actor_role="supervisor")
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_a, **_k: selected_resolution())
    monkeypatch.setattr(router, "_contexto_asignacion_seguro", lambda *_a: {
        "match": None, "usuarios": [{"id": "crm-otro", "usuario": "otro.usuario"}],  # sin ayelen
        "sugerido_id": None, "error": None, "puede_elegir": True,
    })
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda payload, assigned, account_id: {
        "ok": True, "crm_status": "ENVIADO_TEST", "crm_id": "opp-1", "crm_account_id": account_id,
        "crm_modo": "test", "assigned_user_id": assigned["id"], "assigned_user": assigned["usuario"],
        "usuario_origen": assigned["origen"], "bitacora_error": None,
    })
    response = client.post(
        "/api/mercado-privado/oportunidades/enviar/371",
        params={"cuenta_seleccionada": "8519", "assigned_user_id": "crm-ayelen"},
    )
    assert response.status_code == 200
    envio = db.execute(select(CrmEnvio)).scalar_one()
    assert envio.crm_assigned_user_id == "crm-ayelen"
    assert envio.crm_assigned_usuario == "ayelen.piluso"


def test_enviar_sigue_rechazando_ids_que_no_son_ni_equipo_ni_operador(db, monkeypatch):
    link_vendedor(db, codigo="4071", email="ayelen.piluso@suizoargentina.com")
    patch_crm_usuarios(monkeypatch)
    client = make_client(db, monkeypatch, actor_role="supervisor")
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_a, **_k: selected_resolution())
    monkeypatch.setattr(router, "_contexto_asignacion_seguro", lambda *_a: {
        "match": None, "usuarios": [{"id": "crm-otro", "usuario": "otro.usuario"}],
        "sugerido_id": None, "error": None, "puede_elegir": True,
    })
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda *_a: pytest.fail("no debe enviar"))
    response = client.post(
        "/api/mercado-privado/oportunidades/enviar/371",
        params={"cuenta_seleccionada": "8519", "assigned_user_id": "crm-inventado"},
    )
    assert response.status_code == 422
    assert db.scalar(select(func.count()).select_from(CrmEnvio)) == 0
