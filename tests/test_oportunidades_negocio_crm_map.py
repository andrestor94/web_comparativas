"""Negocio / Subnegocio del artículo -> campos booleanos del CRM.

Cubre:
  1. El mapa estático (crm_negocio_map): normalización, matcheo, negocio derivado del
     subnegocio, etiqueta sin equivalencia, y el caso PROVISORIO del VA 407.
  2. El motor: `oportunidades_summary.subunidad_negocio` queda poblado en el rebuild.
  3. `_build_crm_payload`: mergea SÓLO los campos que aplican con "1" (string), y
     expone `negocio_crm` con la marca `no_mapeado`.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.dimensionamiento.crm_negocio_map import (
    campos_crm_negocio,
    resolver_negocio_subnegocio,
)
from web_comparativas.dimensionamiento.models import (
    Base, DimensionamientoImportRun, DimensionamientoRecord, OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import rebuild_oportunidades_for_run
from web_comparativas.routers import oportunidades_router as router


# ───────────────────────── 1. el mapa ─────────────────────────

def test_match_por_texto_normalizado_negocio_sale_del_subnegocio():
    # "SERVICIOS HOSPITALARIOS" en el dataset -> gerenciamiento_c en el CRM (la
    # etiqueta del negocio no es literal; el negocio sale del parent del subnegocio).
    res = resolver_negocio_subnegocio(
        "SERVICIOS HOSPITALARIOS", "GERENCIAMIENTO CONVENIOS Hospit. fisico"
    )
    assert res["negocio_field"] == "gerenciamiento_c"
    assert res["subnegocio_field"] == "geren_conv_hop_fisico_903_c"
    assert res["campos"] == {"gerenciamiento_c": "1", "geren_conv_hop_fisico_903_c": "1"}
    assert res["no_mapeado"] is False


def test_normalizacion_acentos_y_mayusculas():
    # Acentos, mayúsculas y espacios no deben romper el match.
    a = campos_crm_negocio("MEDICAMENTOS HOSPITALARIOS", "Antibioticos")
    b = campos_crm_negocio("medicamentos hospitalarios", "  antibióticos ")
    assert a == b == {"medicamentos_c": "1", "antibioticos_603_c": "1"}


def test_va_407_provisorio_cuelga_de_accesorios():
    # PROVISORIO (pendiente de Matías): el VA 407 va bajo accesorios_insumos_c, que es
    # lo que dice el dataset. El campo del subnegocio ya es el definitivo.
    res = resolver_negocio_subnegocio(
        "ACCESORIOS E INSUM MED-HOSPITALARIOS", "Insumos medicos de Valor Agregado"
    )
    assert res["subnegocio_field"] == "insumos_medicos_va_407_c"
    assert res["negocio_field"] == "accesorios_insumos_c"


def test_etiqueta_desconocida_no_rompe_y_marca_no_mapeado():
    res = resolver_negocio_subnegocio("CIRUGIA PLASTICA", "Protesis agn")
    assert res["campos"] == {}
    assert res["no_mapeado"] is True
    assert ("negocio", "CIRUGIA PLASTICA") in res["sin_mapear"]
    assert ("subnegocio", "Protesis agn") in res["sin_mapear"]


def test_sin_datos_no_marca_nada():
    res = resolver_negocio_subnegocio(None, None)
    assert res["campos"] == {}
    assert res["no_mapeado"] is False


# ───────────────────────── 2. el motor ─────────────────────────

@pytest.fixture()
def db(monkeypatch):
    monkeypatch.setenv("OPORTUNIDADES_ENABLED", "1")
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=[
        DimensionamientoImportRun.__table__, DimensionamientoRecord.__table__,
        OportunidadSummary.__table__,
    ])
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.add(DimensionamientoImportRun(id=1, source_path="x.csv", status="success"))
        session.commit()
        base = dt.date(2025, 1, 15)
        rid = 0

        def _rec(fecha, resultado, cantidad, valor):
            nonlocal rid
            rid += 1
            return DimensionamientoRecord(
                id_registro_unico=f"REG{rid}", fecha=fecha, plataforma="P",
                cliente_visible="CLIENTE X", codigo_articulo="ART1",
                resultado_participacion=resultado, cantidad_demandada=cantidad,
                valorizacion_estimada=valor, import_run_id=1,
                cuenta_interna="AAA", cuit="20111111112",
                unidad_negocio="MEDICAMENTOS HOSPITALARIOS",
                subunidad_negocio="Antibioticos",
                is_identified=True, is_client=True,
            )

        for m in range(13):
            fecha = dt.date(base.year + (base.month - 1 + m) // 12, (base.month - 1 + m) % 12 + 1, 15)
            for _ in range(5):
                # Precio alto para superar PARAM_MONTO_MIN_ARS (monto = consumo típico * precio).
                session.add(_rec(fecha, "NO_PARTICIPO", 10, 300_000))
            # Adjudicaciones ganadas -> efectividad = 1.0, ganados > 0.
            session.add(_rec(fecha, "GANADO", 5, 150_000))
        session.commit()
        yield session, engine


def test_rebuild_puebla_subunidad_negocio(db):
    session, _ = db
    result = rebuild_oportunidades_for_run(session, 1)
    assert result["status"] == "ok"
    fila = session.query(OportunidadSummary).filter_by(import_run_id=1).one()
    assert fila.unidad_negocio == "MEDICAMENTOS HOSPITALARIOS"
    assert fila.subunidad_negocio == "Antibioticos"
    # Nada sin mapear en este caso.
    assert result["stats"].get("negocio_sin_mapear") == {}


# ─────────────────────── 3. _build_crm_payload ───────────────────────

def _fila_summary(**kw):
    base = dict(
        id=1, import_run_id=1, codigo_articulo="ART1", cliente_visible="CLIENTE X",
        cuenta_interna="AAA", producto_nombre="Prod", monto_oportunidad=1_000_000,
        tipo_oportunidad="ESTABLE", ventana_meses=12, meses_demanda_cliente_12m=12,
    )
    base.update(kw)
    return OportunidadSummary(**base)


def test_payload_mergea_solo_los_que_aplican_con_string_1():
    o = _fila_summary(
        unidad_negocio="MEDICAMENTOS HOSPITALARIOS", subunidad_negocio="Antibioticos",
    )
    crm = router._build_crm_payload(o, cuenta_fusion="AAA")
    payload = crm["payload"]
    assert payload["medicamentos_c"] == "1"
    assert payload["antibioticos_603_c"] == "1"
    # Ningún campo booleano en "0" ni ningún otro negocio prendido.
    assert "accesorios_insumos_c" not in payload
    assert all(v != "0" for v in payload.values())
    assert crm["negocio_crm"]["no_mapeado"] is False


def test_payload_sin_mapa_no_agrega_campos_pero_no_bloquea():
    o = _fila_summary(unidad_negocio="AMBULATORIO", subunidad_negocio="AMBULATORIO")
    crm = router._build_crm_payload(o, cuenta_fusion="AAA")
    assert crm["negocio_crm"]["no_mapeado"] is True
    assert crm["negocio_crm"]["campos"] == {}
    assert not crm["bloqueos"]  # el envío sale igual
