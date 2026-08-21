from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_comparativas.routers import mercado_privado_perfiles_router as router
from web_comparativas.dimensionamiento.models import DimensionamientoFamilyMonthlySummary
from web_comparativas.dimensionamiento.query_service import _apply_common_filters


def test_build_filters_uses_entity_ids_and_keeps_family_and_dates():
    filters = router._build_filters_from_payload({
        "cliente_entidad_ids": [186, "98", "invalid", None],
        "familias": ["Descartables", " Implantes "],
        "fecha_desde": "2026-01-01",
        "fecha_hasta": "2026-06-30",
    })

    assert filters.cliente_entidad_ids == [186, 98]
    assert filters.clientes == []
    assert filters.familias == ["Descartables", "Implantes"]
    assert filters.fecha_desde.isoformat() == "2026-01-01"
    assert filters.fecha_hasta.isoformat() == "2026-06-30"


def test_build_filters_retains_legacy_client_compatibility_for_existing_callers():
    filters = router._build_filters_from_payload({"clientes": ["Cliente legado"]})

    assert filters.clientes == ["Cliente legado"]
    assert filters.cliente_entidad_ids == []


def test_entity_id_and_family_are_applied_to_the_analytics_query():
    filters = router._build_filters_from_payload({
        "cliente_entidad_ids": [186],
        "familias": ["Descartables"],
    })
    stmt = _apply_common_filters(
        select(DimensionamientoFamilyMonthlySummary.id),
        DimensionamientoFamilyMonthlySummary,
        filters,
    )
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "cliente_entidad_id IN (186)" in sql
    assert "familia IN ('Descartables')" in sql


def test_private_client_contract_keeps_256_readable_entities():
    source = [{"value": entity_id, "label": f"Cliente {entity_id:03d}"} for entity_id in range(1, 257)]

    result = router._private_client_options(source)

    assert len(result) == 256
    assert result[0] == {"id": 1, "label": "Cliente 001"}
    assert result[-1] == {"id": 256, "label": "Cliente 256"}
    assert all(set(option) == {"id", "label"} for option in result)
    assert all("[object Object]" not in option["label"] for option in result)


def test_private_filters_endpoint_exposes_id_label_contract(monkeypatch):
    router._CACHE.clear()
    monkeypatch.setattr(router, "get_filter_options", lambda db, filters: {
        "clientes": [
            {"value": 186, "label": "Administrar Salud S.A."},
            {"id": 98, "label": "Agrupación Médica Integral"},
            {"value": "fallback legacy", "label": "No es una entidad"},
        ],
        "familias": ["Descartables", "Implantes"],
        "plataformas": ["Mercado Privado"],
        "date_range": {"min": "2025-01-01", "max": "2026-06-01"},
    })

    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router._get_db] = lambda: object()
    route = next(r for r in app.routes if getattr(r, "path", "") == "/api/mercado-privado/perfiles/filters")
    auth_dependency = route.dependant.dependencies[0].call
    app.dependency_overrides[auth_dependency] = lambda: object()

    response = TestClient(app).post("/api/mercado-privado/perfiles/filters", json={})

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "data": {
            "familias": ["Descartables", "Implantes"],
            "clientes": [
                {"id": 186, "label": "Administrar Salud S.A."},
                {"id": 98, "label": "Agrupación Médica Integral"},
            ],
            "plataformas": ["Mercado Privado"],
            "date_range": {"min": "2025-01-01", "max": "2026-06-01"},
        },
    }


def test_private_frontend_uses_labels_and_sends_entity_ids():
    source = (ROOT / "web_comparativas/static/js/reporte_perfiles_privado.js").read_text(encoding="utf-8")

    assert "options.map(pvNormalizeDropdownOption)" in source
    assert "opt.label.toLowerCase().includes(q)" in source
    assert "pvEsc(opt.label)" in source
    assert "state.visible?.[index]?.value" in source
    assert "pvDropdownSelectedLabel(state)" in source
    assert source.count("cliente_entidad_ids:") == 2
    assert "{ clientes: [PV.artCliente] }" not in source
    assert "clientes: [PV.cliCliente]" not in source
    assert "options.map(cleanMojibakeText)" not in source


def test_private_frontend_keeps_family_and_date_filters():
    source = (ROOT / "web_comparativas/static/js/reporte_perfiles_privado.js").read_text(encoding="utf-8")

    assert "familias: [PV.artFamilia]" in source
    assert "familias: [PV.cliFamilia]" in source
    assert "base.fecha_desde = PV.fechaDesde" in source
    assert "base.fecha_hasta = PV.fechaHasta" in source
    assert "state.query = ''" in source
    assert "state.options.length <= 500" in source
    assert 'if (!PV.artFamilia)' in source
    assert 'if (!PV.cliCliente)' in source
    assert "state.draft = state.selected ?? ''" in source
    assert "state.selected = state.draft ?? ''" in source


def test_private_report_busts_the_previous_frontend_cache():
    template = (ROOT / "web_comparativas/templates/reporte_perfiles_privado.html").read_text(encoding="utf-8")

    assert "reporte_perfiles_privado.js') }}?v=5" in template
