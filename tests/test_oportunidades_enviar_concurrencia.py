from __future__ import annotations

import datetime as dt
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
from web_comparativas.dimensionamiento.crm_client import CrmError
from web_comparativas.dimensionamiento.models import (
    CrmEnvio, CrmEnvioEvento, DimensionamientoImportRun, OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id
from web_comparativas.routers import oportunidades_router as router


CLIENTE = "SANATORIO JUAN XXIII S.R.L."
ARTICULO = "8111612"
OPORTUNIDAD_ID = opportunity_stable_id(CLIENTE, ARTICULO)


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
            id=371, import_run_id=1, codigo_articulo=ARTICULO,
            cliente_visible=CLIENTE, cuit="30595201027",
            cuenta_interna="32059", producto_nombre="CEFTAZIDIMA", familia="FAMILIA",
            unidad_negocio="MEDICAMENTOS", plataforma="BIONEXO",
            tipo_oportunidad="INTERMITENTE", estado_actividad="ACTIVA",
            meses_demanda_cliente_12m=3, meses_no_participo_12m=3, ventana_meses=12,
            consumo_tipico_mensual=100, consumo_min_mensual=100, consumo_max_mensual=100,
            ultima_demanda=dt.date(2026, 4, 15), meses_desde_ultima_demanda=4,
            precio_unitario_estimado=100,
            monto_oportunidad=10000, efectividad=0.37, ganados=46, comprado_otra=1,
            en_espera=0, clientes_distintos=25, tipo_multiplicador=1,
            multiplicador_actividad=1, score=10000,
        ))
        session.commit()
        yield session


def selected_resolution():
    selected = {
        "cuenta": "8519", "crm_account_id": "crm-account-8519",
        "operador_codigo": "4071", "operador_nombre": "AYELEN PILUSO",
        "crm_cuit": None, "crm_razon_social": CLIENTE,
    }
    return {
        "cuit": "30595201027", "cuenta_original": "32059",
        "cuentas_candidatas": [{"cuenta": "32059"}, selected],
        "cantidad_candidatas_total": 2, "cantidad_evaluadas_crm": 2,
        "cuentas_encontradas_en_crm": [selected],
        "cuenta_seleccionada": selected, "criterio_seleccion": "unica_alternativa_valida",
        "estado_confianza": "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO",
        "confianza_label": "test", "confirmacion_fiscal": False,
        "seleccion_origen": "automatica_unica_alternativa",
        "fuente_relacion": "test", "trazabilidad_texto": None,
        "bloqueado": False, "requiere_seleccion": False, "crm_modo": "test",
    }


def make_client(db, monkeypatch, *, role="admin"):
    user = SimpleNamespace(id=7, email=f"{role}@suizo.com", role=role)
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._require_oportunidades_write] = lambda: user
    app.dependency_overrides[router.get_db] = lambda: db
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "_latest_success_import_run", lambda _db: SimpleNamespace(id=1))
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "test")
    monkeypatch.setattr(router, "_contexto_asignacion_seguro", lambda *_args: {
        "match": {"id": "user-1", "usuario": role, "origen": "match"},
        "usuarios": [{"id": "user-1", "usuario": role}],
        "error": None, "puede_elegir": True,
    })
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_a, **_k: selected_resolution())
    return TestClient(app)


def _n_envios(db) -> int:
    return db.scalar(select(func.count()).select_from(CrmEnvio))


def test_race_lost_at_claim_returns_duplicado_and_never_calls_crm(db, monkeypatch):
    """Simula la carrera: cuando esta request está a punto de reclamar el envío
    (justo antes del INSERT del placeholder), otra request YA comiteó su propia
    fila para la misma (oportunidad_id, crm_modo) — el chequeo inicial de
    `existente` no la vio porque en ese momento todavía no existía.

    El fix tiene que: (1) nunca llamar a `_enviar_real_a_crm` para esta request
    (la carrera se cierra ANTES de tocar el CRM), y (2) devolver la misma forma
    "duplicado" de siempre, con 200, no un 500 genérico por IntegrityError sin
    capturar."""
    client = make_client(db, monkeypatch)
    llamadas_crm: list[int] = []
    monkeypatch.setattr(
        router, "_enviar_real_a_crm",
        lambda *_a: llamadas_crm.append(1) or pytest.fail("no debe llamar al CRM: la carrera se pierde antes"),
    )

    real_periodo = router._periodo_actual

    def periodo_con_carrera_ganada_por_otro():
        # Se ejecuta justo antes de que este request arme su propio placeholder
        # (ver `_periodo_actual()` en oportunidades_router.py) — acá "otra
        # request" ya comiteó la suya para la misma clave.
        ganador = CrmEnvio(
            oportunidad_id=OPORTUNIDAD_ID, cliente_visible=CLIENTE, codigo_articulo=ARTICULO,
            enviado_por="otro.analista@suizo.com", enviado_at=dt.datetime(2026, 8, 25, 10, 0),
            crm_status="ENVIADO_TEST", crm_modo="test",
        )
        db.add(ganador)
        db.commit()
        return real_periodo()

    monkeypatch.setattr(router, "_periodo_actual", periodo_con_carrera_ganada_por_otro)

    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["status"] == "duplicado"
    assert body["enviado_por"] == "otro.analista@suizo.com"
    assert "otro.analista@suizo.com" in body["message"]
    assert llamadas_crm == []
    assert _n_envios(db) == 1  # solo la fila del ganador — nunca se duplicó


def test_crm_error_after_claim_deletes_placeholder_leaves_retryable(db, monkeypatch):
    """Si el CRM rechaza el envío DESPUÉS de reclamado el placeholder, el
    placeholder tiene que borrarse — la oportunidad sigue libre de reintentar,
    mismo contrato que ya regía antes del fix ("ante cualquier falla el envío NO
    se registra")."""
    client = make_client(db, monkeypatch)
    monkeypatch.setattr(
        router, "_enviar_real_a_crm",
        lambda *_a: (_ for _ in ()).throw(CrmError("El CRM está caído.", kind="crm")),
    )

    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")

    assert response.status_code == 503  # kind="crm" -> reintentable
    assert _n_envios(db) == 0
    assert db.scalar(select(func.count()).select_from(CrmEnvioEvento)) == 0

    # Libre de reintentar de verdad: un segundo intento (ahora con éxito) tiene
    # que poder reclamar la MISMA clave sin chocar contra un placeholder viejo.
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda payload, assigned, account_id: {
        "ok": True, "crm_status": "ENVIADO_TEST", "crm_id": "opportunity-1",
        "crm_account_id": account_id, "crm_modo": "test", "assigned_user_id": assigned["id"],
        "assigned_user": assigned["usuario"], "usuario_origen": assigned["origen"],
        "bitacora_error": None,
    })
    response2 = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")
    assert response2.status_code == 200
    assert response2.json()["ok"] is True
    assert _n_envios(db) == 1


def test_crm_ack_not_ok_deletes_placeholder(db, monkeypatch):
    client = make_client(db, monkeypatch)
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda *_a: {"ok": False})

    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")

    assert response.status_code == 502
    assert _n_envios(db) == 0


def test_successful_send_completes_placeholder_single_row(db, monkeypatch):
    """El envío exitoso no crea una fila nueva ADEMÁS del placeholder — completa
    la misma fila que reclamó el envío antes de llamar al CRM."""
    client = make_client(db, monkeypatch)
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda payload, assigned, account_id: {
        "ok": True, "crm_status": "ENVIADO_TEST", "crm_id": "opportunity-1",
        "crm_account_id": account_id, "crm_modo": "test", "assigned_user_id": assigned["id"],
        "assigned_user": assigned["usuario"], "usuario_origen": assigned["origen"],
        "bitacora_error": None,
    })

    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")

    assert response.status_code == 200
    assert _n_envios(db) == 1
    envio = db.execute(select(CrmEnvio)).scalar_one()
    assert envio.crm_status == "ENVIADO_TEST"
    assert envio.crm_id == "opportunity-1"
    assert envio.oportunidad_id == OPORTUNIDAD_ID


def test_ya_enviada_previamente_no_llama_al_crm(db, monkeypatch):
    """El chequeo original (SELECT antes del reclamo) sigue funcionando igual:
    si ya hay una fila para esta clave, corta antes de armar el placeholder."""
    db.add(CrmEnvio(
        oportunidad_id=OPORTUNIDAD_ID, cliente_visible=CLIENTE, codigo_articulo=ARTICULO,
        enviado_por="primero@suizo.com", enviado_at=dt.datetime(2026, 8, 20, 9, 0),
        crm_status="ENVIADO_TEST", crm_modo="test",
    ))
    db.commit()

    client = make_client(db, monkeypatch)
    monkeypatch.setattr(
        router, "_enviar_real_a_crm",
        lambda *_a: pytest.fail("no debe llamar al CRM: ya estaba enviada"),
    )

    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["status"] == "duplicado"
    assert body["enviado_por"] == "primero@suizo.com"
    assert _n_envios(db) == 1
