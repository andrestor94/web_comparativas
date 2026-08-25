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

from web_comparativas.models import Base, CarteraOperador, CarteraVendedor, User, UserReporte, VendedorFusion
from web_comparativas.dimensionamiento.models import (
    CrmEnvio, CrmEnvioEvento, DimensionamientoImportRun,
    OportunidadAsignacionManual, OportunidadSummary,
)
from web_comparativas.routers import oportunidades_router as router

# Dos oportunidades del mismo run: PROPIA (cuenta_interna="AAA", en la cartera del
# analista vía cartera_vendedores) y AJENA (cuenta_interna="ZZZ", sin ninguna fila
# en cartera_operadores/cartera_vendedores — fuera de la cartera de cualquiera).
PROPIA_ID = 501
AJENA_ID = 502


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        User.__table__, UserReporte.__table__, VendedorFusion.__table__,
        CarteraOperador.__table__, CarteraVendedor.__table__,
        DimensionamientoImportRun.__table__, OportunidadSummary.__table__,
        OportunidadAsignacionManual.__table__,
        CrmEnvio.__table__, CrmEnvioEvento.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        analista = User(
            email="analista@suizo.com", name="Analista", role="analista", password_hash="x",
            cartera_vendedor_codigos=["V-AN"],
        )
        admin = User(email="admin@suizo.com", name="Admin", role="admin", password_hash="x")
        session.add_all([analista, admin])
        session.flush()

        session.add(CarteraVendedor(codigo_cliente="AAA", vendedor_codigo="V-AN", unineg="0"))
        # "ZZZ" deliberadamente ausente de cartera_operadores/cartera_vendedores.

        session.add(DimensionamientoImportRun(id=1, source_path="test.csv", status="success"))

        def _summary(summary_id: int, cuenta_interna: str) -> OportunidadSummary:
            return OportunidadSummary(
                id=summary_id, import_run_id=1, codigo_articulo=f"ART-{summary_id}",
                cliente_visible=f"Cliente {summary_id}", cuenta_interna=cuenta_interna,
                producto_nombre="Producto", familia="Familia", unidad_negocio="Unidad",
                plataforma="Portal", tipo_oportunidad="ESTABLE", estado_actividad="ACTIVA",
                meses_demanda_cliente_12m=3, meses_no_participo_12m=3, ventana_meses=12,
                consumo_tipico_mensual=10, consumo_min_mensual=5, consumo_max_mensual=15,
                meses_desde_ultima_demanda=1, precio_unitario_estimado=100,
                monto_oportunidad=1000, efectividad=0.5, ganados=2, comprado_otra=1,
                en_espera=0, clientes_distintos=2, tipo_multiplicador=1,
                multiplicador_actividad=1, score=1000,
            )

        session.add(_summary(PROPIA_ID, "AAA"))
        session.add(_summary(AJENA_ID, "ZZZ"))
        session.commit()

        session.info["analista_id"] = analista.id
        session.info["admin_id"] = admin.id
        yield session


def selected_resolution(cuenta="AAA"):
    selected = {
        "cuenta": cuenta, "crm_account_id": f"crm-account-{cuenta}",
        "operador_codigo": None, "operador_nombre": None,
        "crm_cuit": None, "crm_razon_social": "Cliente test",
    }
    return {
        "cuit": None, "cuenta_original": cuenta,
        "cuentas_candidatas": [selected],
        "cantidad_candidatas_total": 1, "cantidad_evaluadas_crm": 1,
        "cuentas_encontradas_en_crm": [selected],
        "cuenta_seleccionada": selected, "criterio_seleccion": "unica_alternativa_valida",
        "estado_confianza": "COINCIDENCIA_EXACTA", "confianza_label": "test",
        "confirmacion_fiscal": False, "seleccion_origen": "automatica_unica_alternativa",
        "fuente_relacion": "test", "trazabilidad_texto": None,
        "bloqueado": False, "requiere_seleccion": False, "crm_modo": "test",
    }


def make_client(db, monkeypatch, *, user, cartera_enabled: bool):
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._require_oportunidades_write] = lambda: user
    app.dependency_overrides[router.get_db] = lambda: db
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "OPORTUNIDADES_CARTERA_ENABLED", lambda: cartera_enabled)
    monkeypatch.setattr(router, "_latest_success_import_run", lambda _db: SimpleNamespace(id=1))
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "test")
    monkeypatch.setattr(router, "_contexto_asignacion_seguro", lambda *_args: {
        "match": {"id": "user-1", "usuario": user.role, "origen": "match"},
        "usuarios": [{"id": "user-1", "usuario": user.role}],
        "error": None, "puede_elegir": True,
    })
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_a, **_k: selected_resolution())
    return TestClient(app)


def _n_envios(db) -> int:
    return db.scalar(select(func.count()).select_from(CrmEnvio))


def _fake_ok_send(payload, assigned, account_id):
    return {
        "ok": True, "crm_status": "ENVIADO_TEST", "crm_id": "opportunity-1",
        "crm_account_id": account_id, "crm_modo": "test", "assigned_user_id": assigned["id"],
        "assigned_user": assigned["usuario"], "usuario_origen": assigned["origen"],
        "bitacora_error": None,
    }


def test_analista_no_puede_enviar_summary_id_fuera_de_su_cartera(db, monkeypatch):
    analista = db.get(User, db.info["analista_id"])
    client = make_client(db, monkeypatch, user=analista, cartera_enabled=True)
    monkeypatch.setattr(
        router, "_enviar_real_a_crm",
        lambda *_a: pytest.fail("no debe llamar al CRM: la oportunidad está fuera de su cartera"),
    )

    response = client.post(f"/api/mercado-privado/oportunidades/enviar/{AJENA_ID}?cuenta_seleccionada=ZZZ")

    assert response.status_code == 403
    assert "cartera" in response.json()["detail"].lower()
    assert _n_envios(db) == 0
    assert db.scalar(select(func.count()).select_from(CrmEnvioEvento)) == 0


def test_analista_si_puede_enviar_su_propia_cartera(db, monkeypatch):
    analista = db.get(User, db.info["analista_id"])
    client = make_client(db, monkeypatch, user=analista, cartera_enabled=True)
    monkeypatch.setattr(router, "_enviar_real_a_crm", _fake_ok_send)

    response = client.post(f"/api/mercado-privado/oportunidades/enviar/{PROPIA_ID}?cuenta_seleccionada=AAA")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert _n_envios(db) == 1


def test_switch_apagado_no_filtra_por_cartera_rollback_completo(db, monkeypatch):
    """Con OPORTUNIDADES_CARTERA_ENABLED apagado, /enviar tiene que comportarse
    exactamente como antes de este cambio — ni siquiera evalúa cartera."""
    analista = db.get(User, db.info["analista_id"])
    client = make_client(db, monkeypatch, user=analista, cartera_enabled=False)
    monkeypatch.setattr(router, "_enviar_real_a_crm", _fake_ok_send)

    response = client.post(f"/api/mercado-privado/oportunidades/enviar/{AJENA_ID}?cuenta_seleccionada=ZZZ")

    assert response.status_code == 200
    assert response.json()["ok"] is True


@pytest.mark.parametrize("cartera_enabled", [True, False])
def test_admin_no_cambia_envia_cualquier_summary_id(db, monkeypatch, cartera_enabled):
    admin = db.get(User, db.info["admin_id"])
    client = make_client(db, monkeypatch, user=admin, cartera_enabled=cartera_enabled)
    monkeypatch.setattr(router, "_enviar_real_a_crm", _fake_ok_send)

    response = client.post(f"/api/mercado-privado/oportunidades/enviar/{AJENA_ID}?cuenta_seleccionada=ZZZ")

    assert response.status_code == 200
    assert response.json()["ok"] is True
