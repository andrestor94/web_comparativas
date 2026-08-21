from __future__ import annotations

import datetime as dt
import json
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
from web_comparativas.dimensionamiento.account_resolution import AccountSelectionError
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
            ultima_demanda=dt.date(2026, 4, 15), meses_desde_ultima_demanda=4,
            precio_unitario_estimado=100,
            monto_oportunidad=10000, efectividad=0.37, ganados=46, comprado_otra=1,
            en_espera=0, clientes_distintos=25, tipo_multiplicador=1,
            multiplicador_actividad=1, score=10000,
        ))
        session.commit()
        yield session


def selected_resolution(account="8519", *, mode="test", origin="automatica_unica_alternativa"):
    selected = {
        "cuenta": account, "crm_account_id": f"crm-account-{account}",
        "operador_codigo": "4071", "operador_nombre": "AYELEN PILUSO",
        "crm_cuit": None, "crm_razon_social": "SANATORIO JUAN XXIII S.R.L.",
    }
    return {
        "cuit": "30595201027", "cuenta_original": "32059",
        "cuentas_candidatas": [{"cuenta": "32059"}, selected],
        "cantidad_candidatas_total": 2, "cantidad_evaluadas_crm": 2,
        "cuentas_encontradas_en_crm": [selected],
        "cuenta_seleccionada": selected, "criterio_seleccion": "unica_alternativa_valida",
        "estado_confianza": "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO",
        "confianza_label": "Cuenta alternativa relacionada por razón social normalizada; verificar antes de enviar",
        "confirmacion_fiscal": False, "seleccion_origen": origin,
        "fuente_relacion": "dataset.cliente_nombre_homologado -> canon exacto -> clientes.nombre -> clientes.codigo -> Operadores.Codigo",
        "trazabilidad_texto": "Alternativa relacionada únicamente por razón social normalizada.",
        "bloqueado": False, "requiere_seleccion": False, "crm_modo": mode,
    }


def blocked_resolution(state, *, reasons=None, count=2):
    return {
        "cuit": "30595201027", "cuenta_original": "32059",
        "cuentas_candidatas": [{"cuenta": "32059"}],
        "cantidad_candidatas_total": count, "cuentas_encontradas_en_crm": [],
        "cuenta_seleccionada": None, "criterio_seleccion": state.lower(),
        "estado_confianza": state, "confianza_label": state,
        "confirmacion_fiscal": False, "seleccion_origen": "ninguna",
        "fuente_relacion": "test", "trazabilidad_texto": None,
        "motivos_ambiguedad": reasons or [], "bloqueado": True,
        "requiere_seleccion": False,
    }


def make_client(db, monkeypatch):
    user = SimpleNamespace(id=7, email="admin@suizo.com", role="admin")
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._require_oportunidades_write] = lambda: user
    app.dependency_overrides[router.get_db] = lambda: db
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "_latest_success_import_run", lambda _db: SimpleNamespace(id=1))
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "test")
    monkeypatch.setattr(router, "_contexto_asignacion_seguro", lambda *_args: {
        "match": {"id": "user-1", "usuario": "admin", "origen": "match"},
        "usuarios": [{"id": "user-1", "usuario": "admin"}],
        "error": None, "puede_elegir": True,
    })
    return TestClient(app)


def assert_no_send_rows(db):
    assert db.scalar(select(func.count()).select_from(CrmEnvio)) == 0
    assert db.scalar(select(func.count()).select_from(CrmEnvioEvento)) == 0


@pytest.mark.parametrize("requested", ["HACK", "6280"])
def test_real_post_rejects_manipulated_or_other_cuit_account_without_writes(db, monkeypatch, requested):
    client = make_client(db, monkeypatch)
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AccountSelectionError(f"La cuenta {requested} no pertenece a las cuentas relacionadas con la oportunidad.")
    ))
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda *_args: pytest.fail("No debe invocar envío"))
    response = client.post(f"/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada={requested}")
    assert response.status_code == 422
    assert "no pertenece" in response.json()["detail"]
    assert_no_send_rows(db)


def test_real_post_revalidates_stale_modal_and_existing_unlinked_account(db, monkeypatch):
    client = make_client(db, monkeypatch)
    calls = []

    def changed_relation(_db, _run, _opportunity, *, requested_account=None):
        calls.append(requested_account)
        raise AccountSelectionError("La cuenta dejó de pertenecer a la relación vigente.")

    monkeypatch.setattr(router, "_resolve_account_for_opportunity", changed_relation)
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda *_args: pytest.fail("No debe invocar envío"))
    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")
    assert response.status_code == 422
    assert calls == ["8519"]
    assert_no_send_rows(db)


@pytest.mark.parametrize("state,status", [
    ("RELACION_AMBIGUA", 422), ("SIN_RELACION", 422), ("ERROR_CONSULTA_CRM", 503),
])
def test_real_post_blocks_ambiguous_none_and_partial_crm_without_500(db, monkeypatch, state, status):
    client = make_client(db, monkeypatch)
    reasons = ["MAS_DE_25_CUENTAS_CANDIDATAS"] if state == "RELACION_AMBIGUA" else []
    count = 30 if reasons else 2
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_args, **_kwargs: blocked_resolution(state, reasons=reasons, count=count))
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda *_args: pytest.fail("No debe invocar envío"))
    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")
    assert response.status_code == status
    assert response.status_code != 500
    assert_no_send_rows(db)


def test_account_valid_in_test_but_not_prod_is_rejected_in_current_environment(db, monkeypatch):
    client = make_client(db, monkeypatch)
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "prod")
    monkeypatch.setattr(router, "_resolve_account_for_opportunity", lambda *_args, **_kwargs: blocked_resolution("SIN_RELACION"))
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda *_args: pytest.fail("No debe invocar envío"))
    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")
    assert response.status_code == 422
    assert_no_send_rows(db)


def test_valid_post_uses_server_resolution_and_repository_exposes_trace(db, monkeypatch):
    client = make_client(db, monkeypatch)
    resolver_calls = []

    def resolve(_db, _run, _opportunity, *, requested_account=None):
        resolver_calls.append(requested_account)
        return selected_resolution()

    monkeypatch.setattr(router, "_resolve_account_for_opportunity", resolve)
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda payload, assigned, account_id: {
        "ok": True, "crm_status": "ENVIADO_TEST", "crm_id": "opportunity-test-1",
        "crm_account_id": account_id, "crm_modo": "test", "assigned_user_id": assigned["id"],
        "assigned_user": assigned["usuario"], "usuario_origen": assigned["origen"],
        "bitacora_error": None,
    })
    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")
    assert response.status_code == 200
    assert resolver_calls == ["8519"]
    envio = db.execute(select(CrmEnvio)).scalar_one()
    evento = db.execute(select(CrmEnvioEvento)).scalar_one()
    payload = json.loads(envio.payload_snapshot)
    assert payload["cuenta_original"] == "32059"
    assert payload["cuenta_utilizada"] == "8519"
    assert payload["cuenta_estado_confianza"] == "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO"
    assert payload["cuenta_seleccion_origen"] == "automatica_unica_alternativa"
    assert payload["criterio_resolucion"] == "unica_alternativa_valida"
    assert payload["nivel_confianza"] == "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO"
    assert payload["seleccion_cuenta"] == "automatica_unica_alternativa"
    assert payload["cantidad_candidatas"] == 2
    assert payload["cantidad_evaluadas"] == 2
    assert payload["crm_account_id"] == "crm-account-8519"
    assert payload["operador_nombre"] == "AYELEN PILUSO"
    assert json.loads(evento.payload_snapshot)["cuenta_utilizada"] == "8519"
    repository_row = router._envio_to_dict(envio, "CEFTAZIDIMA")
    assert repository_row["cuenta_original"] == "32059"
    assert repository_row["cuenta_utilizada"] == "8519"
    assert repository_row["cuenta_seleccion_origen"] == "automatica_unica_alternativa"
    assert repository_row["operador_nombre"] == "AYELEN PILUSO"
    assert repository_row["cuit"] == "30595201027"
    assert "OPORTUNIDAD DETECTADA POR SIEM" in repository_row["descripcion"]
    assert "DATOS COMPLEMENTARIOS SIEM" in payload["update_text"]
    assert "- Enviado por: admin@suizo.com" in payload["update_text"]
    assert "- Asignación: Automática → admin" in payload["update_text"]
    assert "Alternativa relacionada" in payload["update_text"]

def test_crm_payload_reorganizes_description_and_bitacora_without_losing_data(db):
    oportunidad = db.get(OportunidadSummary, 371)
    asignado = {"id": "crm-user-1", "usuario": "vendedor.crm", "origen": "manual"}
    momento = dt.datetime(2026, 8, 21, 15, 30)

    crm = router._build_crm_payload(
        oportunidad,
        asignado=asignado,
        email_siem="supervisor@suizo.com",
        momento=momento,
    )

    expected_description = (
        "OPORTUNIDAD DETECTADA POR SIEM\n\n"
        "- Origen: Espacio no participado en BIONEXO\n"
        "- Producto: CEFTAZIDIMA\n"
        "- Demanda típica: 100 unidades/mes\n"
        "- Frecuencia observada: 3 de 12 meses analizados\n"
        "- Última demanda: 15/04/2026\n"
        "- Estado de actividad: ACTIVA"
    )
    complementary = (
        "DATOS COMPLEMENTARIOS SIEM\n\n"
        "- Código de artículo: 8111612\n"
        "- Familia: FAMILIA\n"
        "- Rango mensual observado: 100 a 100 unidades\n"
        "- Efectividad histórica: 37%\n"
        "- Adjudicaciones ganadas: 46\n"
        "- Clientes distintos observados: 25"
    )
    trace = (
        "TRAZABILIDAD\n\n"
        "- Enviado por: supervisor@suizo.com\n"
        "- Asignación: Manual → vendedor.crm\n"
        "- Fecha de envío: 21/08/2026 12:30"
    )

    assert crm["payload"]["description"] == expected_description
    assert crm["payload"]["update_text"] == f"{complementary}\n\n{trace}"
    assert crm["bitacora_datos_siem"] == complementary
    assert "SANATORIO JUAN XXIII" not in crm["payload"]["description"]
    assert "10.000" not in crm["payload"]["description"]
    assert crm["payload"]["amount"] == 10000
    assert "SANATORIO JUAN XXIII" in crm["payload"]["name"]
    assert "<br" not in crm["payload"]["description"].lower()
    assert "<br" not in crm["payload"]["update_text"].lower()

    preview = router._row_to_dict(
        oportunidad,
        oportunidad.cuenta_interna,
        {"match": asignado, "email": "supervisor@suizo.com"},
        momento=momento,
    )
    assert preview["crm"]["payload"] == crm["payload"]


def test_final_send_uses_exact_preview_description_and_bitacora(db, monkeypatch):
    oportunidad = db.get(OportunidadSummary, 371)
    asignado = {"id": "crm-user-1", "usuario": "vendedor.crm", "origen": "manual"}
    payload = router._build_crm_payload(
        oportunidad,
        asignado=asignado,
        email_siem="supervisor@suizo.com",
        momento=dt.datetime(2026, 8, 21, 15, 30),
    )["payload"]
    captured = {}

    monkeypatch.setattr(router, "CRM_ENVIO_PLACEHOLDER", lambda: False)
    monkeypatch.setattr(
        crm_client,
        "enviar_oportunidad",
        lambda **kwargs: captured.update(kwargs) or {
            "crm_id": "crm-opportunity-1", "crm_account_id": "crm-account-1",
            "modo": "prod", "bitacora_id": "bitacora-1",
            "assigned_user_id": asignado["id"], "assigned_user": asignado["usuario"],
            "usuario_origen": asignado["origen"], "bitacora_error": None,
        },
    )

    router._enviar_real_a_crm(payload, asignado, "crm-account-1")

    assert captured["description"] == payload["description"]
    assert captured["bitacora_description"] == payload["update_text"]


def test_description_date_fallback_and_automatic_trace_are_clear(db):
    oportunidad = db.get(OportunidadSummary, 371)
    oportunidad.ultima_demanda = None
    crm = router._build_crm_payload(
        oportunidad,
        asignado={"id": "crm-user-2", "usuario": "vendedor.auto", "origen": "match"},
        email_siem="vendedor@suizo.com",
        momento=dt.datetime(2026, 8, 21, 15, 30),
    )

    assert "- Última demanda: Sin dato" in crm["payload"]["description"]
    assert (
        "TRAZABILIDAD\n\n"
        "- Enviado por: vendedor@suizo.com\n"
        "- Asignación: Automática → vendedor.auto\n"
        "- Fecha de envío: 21/08/2026 12:30"
    ) in crm["payload"]["update_text"]


def test_crm_client_forwards_plain_text_newlines_unchanged(monkeypatch):
    calls = []

    def fake_create(_session, _cfg, _token, *, tipo, atributos, paso):
        calls.append((tipo, atributos, paso))
        return "record-id"

    monkeypatch.setattr(crm_client, "_crear_registro", fake_create)
    description = "Línea uno\n\n- Línea dos"
    bitacora = "Datos\n\nTrazabilidad"

    crm_client.crear_oportunidad(
        object(), {}, "token", nombre="Nombre", assigned_user_id="user-id",
        account_id="account-id", amount=10, description=description,
        id_sistema_origen="siem-id",
    )
    crm_client.crear_bitacora(
        object(), {}, "token", parent_id="opportunity-id",
        description=bitacora, status="ACTIVA",
    )

    assert calls[0][1]["description"] == description
    assert calls[1][1]["description"] == bitacora
    assert all("<br" not in call[1]["description"].lower() for call in calls)


def test_manual_assignment_preview_preserves_complementary_bitacora():
    root = os.path.dirname(os.path.dirname(__file__))
    source = os.path.join(
        root,
        "web_comparativas", "static", "js", "mercado_privado_oportunidades.js",
    )
    with open(source, encoding="utf-8") as handle:
        javascript = handle.read()

    assert 'const bitacoraDatosSiem = crmInfo.bitacora_datos_siem || "";' in javascript
    assert "[bitacoraDatosSiem, logs[assigned.id], trace]" in javascript
    assert '.join("\\n\\n")' in javascript
    assert "description.textContent = (payload && payload.description)" in javascript
    assert "bitacora.textContent = (payload && payload.update_text)" in javascript
    assert ".innerHTML = payload" not in javascript

    template_path = os.path.join(
        root, "web_comparativas", "templates", "mercado_privado_oportunidades.html",
    )
    with open(template_path, encoding="utf-8") as handle:
        template = handle.read()
    assert "white-space:pre-line" in template
    assert 'id="crmDescriptionPreview"' in template
    assert 'id="crmBitacoraPreview"' in template


def test_accounts_get_requires_authentication(monkeypatch):
    app = FastAPI()
    app.include_router(router.router)
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    response = TestClient(app).get("/api/mercado-privado/oportunidades/cuentas/371")
    assert response.status_code == 401
    assert response.json()["detail"] == "No autenticado"


def test_accounts_get_rejects_unknown_active_summary(db, monkeypatch):
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._perm_oportunidades] = lambda: SimpleNamespace(
        id=7, email="admin@suizo.com", role="admin",
    )
    app.dependency_overrides[router.get_db] = lambda: db
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "_latest_success_import_run", lambda _db: SimpleNamespace(id=1))
    response = TestClient(app).get("/api/mercado-privado/oportunidades/cuentas/999999")
    assert response.status_code == 404
    assert "corrida activa" in response.json()["detail"]


def test_historical_minimal_snapshot_is_backward_compatible(db, monkeypatch):
    envio = CrmEnvio(
        oportunidad_id="historical-minimal", cliente_visible="CLIENTE HISTORICO",
        codigo_articulo="ART-1", enviado_por="legacy@suizo.com",
        enviado_at=dt.datetime(2026, 1, 1), crm_status="ENVIADO_TEST",
        crm_modo="test", payload_snapshot=json.dumps({"n_cuenta": "32059", "amount": 10}),
    )
    db.add(envio)
    db.flush()
    row = router._envio_to_dict(envio)
    assert row["cuenta_utilizada"] == "32059"
    assert row["cuenta_original"] is None
    assert row["cuenta_estado_confianza"] is None
    assert row["operador_nombre"] is None
    assert row["fuente_relacion_cuenta"] is None
    path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "web_comparativas", "static", "js",
        "mercado_privado_oportunidad_enviada_detalle.js",
    )
    with open(path, encoding="utf-8") as source_file:
        source = source_file.read()
    assert r"No registrado en este env\u00edo" in source

    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._perm_oportunidades] = lambda: SimpleNamespace(
        id=7, email="admin@suizo.com", role="admin",
    )
    app.dependency_overrides[router.get_db] = lambda: db
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "_latest_success_import_run", lambda _db: None)
    response = TestClient(app).get("/api/mercado-privado/oportunidades/enviadas")
    assert response.status_code == 200
    historical = response.json()["data"]["rows"][0]
    assert historical["cuenta_utilizada"] == "32059"
    assert historical["cuenta_original"] is None
    assert historical["cuenta_estado_confianza"] is None

def test_post_rejects_when_automatic_resolution_became_multiple(db, monkeypatch):
    client = make_client(db, monkeypatch)

    def became_multiple(_db, _run, _opportunity, *, requested_account=None):
        result = selected_resolution(origin="manual")
        result["criterio_seleccion"] = "seleccion_manual_entre_alternativas"
        result["cuentas_encontradas_en_crm"] = [
            result["cuenta_seleccionada"],
            {"cuenta": "9000", "crm_account_id": "crm-account-9000"},
        ]
        return result

    monkeypatch.setattr(router, "_resolve_account_for_opportunity", became_multiple)
    monkeypatch.setattr(router, "_enviar_real_a_crm", lambda *_args: pytest.fail("No debe invocar envío"))
    response = client.post("/api/mercado-privado/oportunidades/enviar/371?cuenta_seleccionada=8519")
    assert response.status_code == 422
    assert "cambió desde que abriste el modal" in response.json()["detail"]
    assert_no_send_rows(db)

    js_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "web_comparativas", "static", "js",
        "mercado_privado_oportunidades.js",
    )
    with open(js_path, encoding="utf-8") as source_file:
        source = source_file.read()
    assert 'query.set("cuenta_seleccion_manual", "true")' in source
