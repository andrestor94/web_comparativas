from __future__ import annotations

import datetime as dt
import os
import sys
from types import SimpleNamespace


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.dimensionamiento import query_service as qs


def test_legacy_family_consumption_payload_needs_refresh():
    legacy_payload = {
        "family_consumption": {
            "months": ["01", "02"],
            "rows": [{"familia": "A", "values": [10, 20]}],
        }
    }

    assert qs._family_consumption_payload_needs_refresh(legacy_payload) is True


def test_current_family_consumption_payload_does_not_need_refresh():
    current_payload = {
        "family_consumption": {
            "months": ["01", "02"],
            "rows": [{"familia": "A", "values": [10, 20]}],
            "total": 1,
        }
    }

    assert qs._family_consumption_payload_needs_refresh(current_payload) is False


def test_truncated_family_consumption_payload_needs_refresh():
    truncated_payload = {
        "family_consumption": {
            "months": ["01"],
            "rows": [{"familia": "A", "values": [10]}],
            "total": 2,
        }
    }

    assert qs._family_consumption_payload_needs_refresh(truncated_payload) is True


def test_bootstrap_refreshes_legacy_snapshot_family_consumption(monkeypatch):
    legacy_snapshot = SimpleNamespace(
        import_run_id=77,
        generated_at=None,
        payload={
            "family_consumption": {
                "months": ["01"],
                "rows": [{"familia": "A", "values": [10]}],
            }
        },
    )
    refreshed_payload = {
        "family_consumption": {
            "months": ["01"],
            "rows": [{"familia": "A", "values": [10]}],
            "total": 1,
        }
    }
    refresh_calls: list[tuple[object, dict, qs.DimensionamientoFilters]] = []

    monkeypatch.setattr(qs, "_normalize_dashboard_filters", lambda session, filters: filters)
    monkeypatch.setattr(qs, "_apply_local_statement_timeout", lambda session, milliseconds: None)
    monkeypatch.setattr(qs, "_has_active_filters", lambda filters: False)
    monkeypatch.setattr(qs, "_get_dashboard_snapshot", lambda session: legacy_snapshot)
    monkeypatch.setattr(qs, "_latest_success_import_run", lambda session: SimpleNamespace(id=77))

    def _refresh(session, payload, filters):
        refresh_calls.append((session, payload, filters))
        return refreshed_payload

    monkeypatch.setattr(qs, "_refresh_bootstrap_family_consumption", _refresh)

    result = qs.get_dashboard_bootstrap(object(), filters=qs.build_filters(), include_status=False)

    assert refresh_calls
    assert result["family_consumption"]["total"] == 1
    assert "page_size" not in result["family_consumption"]


def test_aggregate_bootstrap_family_consumption_keeps_full_universe():
    rows = [
        (
            dt.date(2024, 1, 1),
            "BIONEXO",
            f"Cliente {index}",
            "Buenos Aires",
            f"Familia {index:02d}",
            "4",
            "1",
            "Ganada",
            True,
            True,
            float(500 - index),
            1,
        )
        for index in range(55)
    ]

    payload = qs._aggregate_bootstrap_from_summary_rows(rows)

    assert payload["family_consumption"]["total"] == 55
    assert len(payload["family_consumption"]["rows"]) == 55
    assert payload["family_consumption"]["rows"][0]["familia"] == "Familia 00"
    assert "page_size" not in payload["family_consumption"]
    assert len(payload["top_families"]) == 55
    assert payload["top_families"][0]["familia"] == "Familia 00"
    assert payload["kpis"]["provincias"] == 1


def test_aggregate_bootstrap_uses_visible_client_name_and_counts_provinces():
    rows = [
        (
            dt.date(2024, 1, 1),
            "BIONEXO",
            "Cliente Visible",
            "Buenos Aires",
            "Familia A",
            "4",
            "1",
            "Ganada",
            True,
            False,
            15.0,
            2,
        ),
        (
            dt.date(2024, 2, 1),
            "BIONEXO",
            "Cliente Homologado",
            "Cordoba",
            "Familia B",
            "4",
            "1",
            "Perdida",
            True,
            True,
            5.0,
            1,
        ),
    ]

    payload = qs._aggregate_bootstrap_from_summary_rows(rows)

    assert payload["filters"]["clientes"] == ["Cliente Homologado", "Cliente Visible"]
    assert payload["kpis"]["clientes"] == 2
    assert payload["kpis"]["provincias"] == 2
    assert {item["provincia"] for item in payload["geo"]} == {"Buenos Aires", "Cordoba"}
