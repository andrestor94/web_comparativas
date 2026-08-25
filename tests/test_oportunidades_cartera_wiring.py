from __future__ import annotations

import os
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.models import Base, CarteraOperador, CarteraVendedor, User
from web_comparativas.dimensionamiento.models import (
    CrmEnvio, DimensionamientoImportRun, OportunidadAsignacionManual, OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id
from web_comparativas.routers import oportunidades_router as router


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        User.__table__, CarteraOperador.__table__, CarteraVendedor.__table__,
        DimensionamientoImportRun.__table__, OportunidadSummary.__table__,
        OportunidadAsignacionManual.__table__, CrmEnvio.__table__,
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


def add_opportunity(db, row_id: int, client: str, code: str, cuenta_interna: str) -> OportunidadSummary:
    row = OportunidadSummary(
        id=row_id, import_run_id=1, codigo_articulo=code, cliente_visible=client,
        cuenta_interna=cuenta_interna, producto_nombre=f"Producto {row_id}",
        familia="Familia", unidad_negocio="Unidad", plataforma="Portal",
        tipo_oportunidad="ESTABLE", estado_actividad="ACTIVA",
        meses_demanda_cliente_12m=3, meses_no_participo_12m=3, ventana_meses=12,
        consumo_tipico_mensual=10, consumo_min_mensual=5, consumo_max_mensual=15,
        meses_desde_ultima_demanda=1, precio_unitario_estimado=100,
        monto_oportunidad=1000, efectividad=0.5, ganados=2, comprado_otra=1,
        en_espera=0, clientes_distintos=2, tipo_multiplicador=1,
        multiplicador_actividad=1, score=1000,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def configure_list(monkeypatch, *, cartera_enabled: bool):
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    monkeypatch.setattr(router, "OPORTUNIDADES_CARTERA_ENABLED", lambda: cartera_enabled)
    monkeypatch.setattr(router, "_modo_envio_actual", lambda: "simulado")
    monkeypatch.setattr(router, "_window_meta", lambda _db, _run_id: {"label": "periodo"})
    monkeypatch.setattr(router, "_contexto_asignacion_seguro", lambda *_args: {
        "match": None, "usuarios": [], "sugerido_id": None, "error": None,
        "puede_elegir": True,
    })


@pytest.fixture()
def escenario_cartera(db):
    """Un run con 5 oportunidades: 2 dentro de carteras vinculadas (analista propia +
    supervisor propia, resueltas vía cartera_operadores/cartera_vendedores), 1 del
    analista2 (otro equipo) y 2 huérfanas (sin dueño en ningún lado — buffer de
    Gerente desde 2026-08-25, ya no de Supervisor)."""
    supervisor = make_user(db, email="supervisor@suizo.com", role="supervisor", operador_codigos=["OP-SUP"])
    analista = make_user(
        db, email="analista@suizo.com", role="analista", reporta_a_id=supervisor.id,
        vendedor_codigos=["V-AN"],
    )
    analista2 = make_user(db, email="analista2@suizo.com", role="analista", vendedor_codigos=["V-AN2"])
    admin = make_user(db, email="admin@suizo.com", role="admin")
    auditor = make_user(db, email="auditor@suizo.com", role="auditor")
    gerente = make_user(db, email="gerente@suizo.com", role="gerente", reporta_a_id=None)
    # analista2 no reporta a nadie en este escenario (equipo aparte, sin supervisor
    # propio): sirve para probar "cartera ajena" sin acoplarlo a la jerarquía de
    # supervisor/gerente del escenario principal.

    db.add_all([
        CarteraVendedor(codigo_cliente="AAA", vendedor_codigo="V-AN", unineg="0"),
        CarteraOperador(codigo_cliente="BBB", operador_codigo="OP-SUP"),
        CarteraVendedor(codigo_cliente="CCC", vendedor_codigo="V-AN2", unineg="0"),
        # DDD/EEE ausentes a propósito: huérfanas, buffer de Gerente.
    ])
    db.commit()

    opp_analista = add_opportunity(db, 1, "Cliente A", "ART1", "AAA")
    opp_supervisor = add_opportunity(db, 2, "Cliente B", "ART2", "BBB")
    opp_analista2 = add_opportunity(db, 3, "Cliente C", "ART3", "CCC")
    opp_buffer1 = add_opportunity(db, 4, "Cliente D", "ART4", "DDD")
    opp_buffer2 = add_opportunity(db, 5, "Cliente E", "ART5", "EEE")

    return {
        "supervisor": supervisor, "analista": analista, "analista2": analista2,
        "admin": admin, "auditor": auditor, "gerente": gerente,
        "opp_analista": opp_analista, "opp_supervisor": opp_supervisor,
        "opp_analista2": opp_analista2, "opp_buffer1": opp_buffer1, "opp_buffer2": opp_buffer2,
        "todas_ids": {opp_analista.id, opp_supervisor.id, opp_analista2.id, opp_buffer1.id, opp_buffer2.id},
    }


# ─────────────────────────────────────────────────────────────────────────────
# CONDICIÓN INNEGOCIABLE: switch apagado -> mismas filas para TODOS los roles.
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("role_key", ["admin", "auditor", "gerente", "supervisor", "analista", "analista2"])
def test_switch_apagado_devuelve_todas_las_filas_sin_importar_el_rol(db, monkeypatch, escenario_cartera, role_key):
    configure_list(monkeypatch, cartera_enabled=False)
    user = escenario_cartera[role_key]
    response = router.oportunidades_list(request=object(), _user=user, db=db)
    assert response["data"]["total"] == 5
    assert len(response["data"]["rows"]) == 5
    assert {row["id"] for row in response["data"]["rows"]} == escenario_cartera["todas_ids"]


def test_switch_apagado_no_llama_a_oportunidades_visibles_para(db, monkeypatch, escenario_cartera):
    """No solo "da 5 filas" — confirma que la función de filtrado ni se ejecuta
    cuando el switch está apagado (protege contra un bug que filtre "por accidente"
    a un total que coincida por casualidad)."""
    configure_list(monkeypatch, cartera_enabled=False)
    llamadas = []
    monkeypatch.setattr(
        router, "oportunidades_visibles_para",
        lambda *a, **k: llamadas.append(1) or a[2],
    )
    router.oportunidades_list(request=object(), _user=escenario_cartera["analista"], db=db)
    assert llamadas == []


# ─────────────────────────────────────────────────────────────────────────────
# Switch prendido: la fórmula de visibilidad llega hasta la respuesta del endpoint.
# ─────────────────────────────────────────────────────────────────────────────

def test_switch_prendido_filtra_analista_a_su_cartera(db, monkeypatch, escenario_cartera):
    configure_list(monkeypatch, cartera_enabled=True)
    response = router.oportunidades_list(request=object(), _user=escenario_cartera["analista"], db=db)
    assert response["data"]["total"] == 1
    assert response["data"]["rows"][0]["cliente_visible"] == "Cliente A"


def test_switch_prendido_supervisor_ve_propia_mas_equipo_sin_buffer(db, monkeypatch, escenario_cartera):
    configure_list(monkeypatch, cartera_enabled=True)
    response = router.oportunidades_list(request=object(), _user=escenario_cartera["supervisor"], db=db)
    clientes = {row["cliente_visible"] for row in response["data"]["rows"]}
    assert clientes == {"Cliente A", "Cliente B"}
    assert response["data"]["total"] == 2


def test_switch_prendido_gerente_ve_el_buffer_de_huerfanas(db, monkeypatch, escenario_cartera):
    """Definición de negocio 2026-08-25: nada se pierde — el buffer de cuentas sin
    dueño ahora cae en la bandeja de Gerente (y Admin/Auditor, que ya ven todo)."""
    configure_list(monkeypatch, cartera_enabled=True)
    response = router.oportunidades_list(request=object(), _user=escenario_cartera["gerente"], db=db)
    clientes = {row["cliente_visible"] for row in response["data"]["rows"]}
    # El gerente de este escenario no tiene supervisores a cargo (gerente.reporta_a_id
    # apunta a nadie por debajo): solo debería ver el buffer de huérfanas.
    assert clientes == {"Cliente D", "Cliente E"}


def test_switch_prendido_admin_sigue_viendo_todo(db, monkeypatch, escenario_cartera):
    configure_list(monkeypatch, cartera_enabled=True)
    response = router.oportunidades_list(request=object(), _user=escenario_cartera["admin"], db=db)
    assert response["data"]["total"] == 5


def test_switch_prendido_incluye_asignacion_manual(db, monkeypatch, escenario_cartera):
    """La oportunidad de analista2 (Cliente C) se le asigna a mano al analista del
    escenario: tiene que aparecerle en /list ADEMÁS de la suya."""
    opp_ajena = escenario_cartera["opp_analista2"]
    db.add(OportunidadAsignacionManual(
        oportunidad_id=opportunity_stable_id(opp_ajena.cliente_visible, opp_ajena.codigo_articulo),
        analista_user_id=escenario_cartera["analista"].id,
        asignado_por_user_id=escenario_cartera["supervisor"].id,
    ))
    db.commit()

    configure_list(monkeypatch, cartera_enabled=True)
    response = router.oportunidades_list(request=object(), _user=escenario_cartera["analista"], db=db)
    clientes = {row["cliente_visible"] for row in response["data"]["rows"]}
    assert clientes == {"Cliente A", "Cliente C"}


def test_switch_prendido_via_http_endpoint(db, monkeypatch, escenario_cartera):
    """Confirma el wiring completo también a través de HTTP, no solo llamando la
    función Python directo."""
    configure_list(monkeypatch, cartera_enabled=True)
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._perm_oportunidades] = lambda: escenario_cartera["analista"]
    app.dependency_overrides[router.get_db] = lambda: db
    with TestClient(app) as client:
        response = client.get("/api/mercado-privado/oportunidades/list")
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["total"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Pieza 3: asignación manual — endpoint POST /asignar-analista/{summary_id}
# (No depende de VendedorFusion ni de cartera_operadores/cartera_vendedores: solo
# de rol y `reporta_a_id`, así que corre igual con o sin cartera cargada.)
# ─────────────────────────────────────────────────────────────────────────────

def _asignar_app(db):
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router.get_db] = lambda: db
    return app


def test_supervisor_asigna_a_su_propio_analista(db, monkeypatch, escenario_cartera):
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    app = _asignar_app(db)
    app.dependency_overrides[router._require_oportunidades_asignar] = lambda: escenario_cartera["supervisor"]
    with TestClient(app) as client:
        r = client.post(
            f"/api/mercado-privado/oportunidades/asignar-analista/{escenario_cartera['opp_analista2'].id}",
            params={"analista_id": escenario_cartera["analista"].id},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["data"]["analista_id"] == escenario_cartera["analista"].id

    fila = db.query(OportunidadAsignacionManual).filter(
        OportunidadAsignacionManual.oportunidad_id == body["data"]["oportunidad_id"]
    ).first()
    assert fila is not None
    assert fila.analista_user_id == escenario_cartera["analista"].id
    assert fila.asignado_por_user_id == escenario_cartera["supervisor"].id


def test_supervisor_no_puede_asignar_a_analista_ajeno(db, monkeypatch, escenario_cartera):
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    app = _asignar_app(db)
    app.dependency_overrides[router._require_oportunidades_asignar] = lambda: escenario_cartera["supervisor"]
    with TestClient(app) as client:
        r = client.post(
            f"/api/mercado-privado/oportunidades/asignar-analista/{escenario_cartera['opp_analista'].id}",
            # analista2 NO reporta a este supervisor.
            params={"analista_id": escenario_cartera["analista2"].id},
        )
    assert r.status_code == 403
    assert db.query(OportunidadAsignacionManual).count() == 0


def test_admin_puede_asignar_a_cualquier_analista(db, monkeypatch, escenario_cartera):
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    app = _asignar_app(db)
    app.dependency_overrides[router._require_oportunidades_asignar] = lambda: escenario_cartera["admin"]
    with TestClient(app) as client:
        r = client.post(
            f"/api/mercado-privado/oportunidades/asignar-analista/{escenario_cartera['opp_analista'].id}",
            params={"analista_id": escenario_cartera["analista2"].id},
        )
    assert r.status_code == 200


def test_asignar_a_usuario_que_no_es_analista_falla(db, monkeypatch, escenario_cartera):
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    app = _asignar_app(db)
    app.dependency_overrides[router._require_oportunidades_asignar] = lambda: escenario_cartera["admin"]
    with TestClient(app) as client:
        r = client.post(
            f"/api/mercado-privado/oportunidades/asignar-analista/{escenario_cartera['opp_analista'].id}",
            params={"analista_id": escenario_cartera["supervisor"].id},
        )
    assert r.status_code == 422
    assert db.query(OportunidadAsignacionManual).count() == 0


def test_reasignar_actualiza_en_vez_de_duplicar(db, monkeypatch, escenario_cartera):
    monkeypatch.setattr(router, "OPORTUNIDADES_ENABLED", lambda: True)
    app = _asignar_app(db)
    app.dependency_overrides[router._require_oportunidades_asignar] = lambda: escenario_cartera["admin"]
    opp = escenario_cartera["opp_buffer1"]
    with TestClient(app) as client:
        r1 = client.post(
            f"/api/mercado-privado/oportunidades/asignar-analista/{opp.id}",
            params={"analista_id": escenario_cartera["analista"].id},
        )
        assert r1.status_code == 200
        r2 = client.post(
            f"/api/mercado-privado/oportunidades/asignar-analista/{opp.id}",
            params={"analista_id": escenario_cartera["analista2"].id},
        )
        assert r2.status_code == 200

    filas = db.query(OportunidadAsignacionManual).filter(
        OportunidadAsignacionManual.oportunidad_id == opportunity_stable_id(opp.cliente_visible, opp.codigo_articulo)
    ).all()
    assert len(filas) == 1
    assert filas[0].analista_user_id == escenario_cartera["analista2"].id


def test_roles_permitidos_para_asignar_no_incluyen_analista_ni_gerente_ni_auditor():
    assert router._ROLES_ASIGNADOR == {"admin", "administrator", "administrador", "supervisor"}
    for rol_prohibido in ("analista", "analyst", "gerente", "manager", "auditor", "visor"):
        assert rol_prohibido not in router._ROLES_ASIGNADOR
