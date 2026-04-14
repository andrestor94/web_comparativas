from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas import forecast_service as svc


@pytest.fixture(autouse=True)
def reset_forecast_overrides():
    with svc._overrides_lock:
        svc._client_overrides.clear()
    svc.clear_response_cache()
    yield
    with svc._overrides_lock:
        svc._client_overrides.clear()
    svc.clear_response_cache()


def test_save_client_overrides_removes_entries_that_return_to_default_growth():
    growth_pct = 25.0
    default_monthly_pct = round(svc._monthly_pct_from_annual_growth(growth_pct), 4)

    svc.save_client_overrides(
        "Cliente A",
        [{"articulo": "SKU-1", "date": "2026-01", "pct": 3.5}],
        growth_pct=growth_pct,
    )

    assert svc._has_overrides(growth_pct)
    assert svc._get_client_overrides_snapshot("Cliente A", growth_pct) == {
        ("SKU-1", "2026-01"): 3.5
    }

    svc.save_client_overrides(
        "Cliente A",
        [{"articulo": "SKU-1", "date": "2026-01", "pct": default_monthly_pct}],
        growth_pct=growth_pct,
    )

    assert svc._get_client_overrides_snapshot("Cliente A", growth_pct) == {}
    assert not svc._has_overrides(growth_pct)
    with svc._overrides_lock:
        assert "Cliente A" not in svc._client_overrides


def test_save_client_overrides_replaces_only_visible_cells_and_keeps_other_real_overrides():
    growth_pct = 25.0
    default_monthly_pct = round(svc._monthly_pct_from_annual_growth(growth_pct), 4)

    svc.save_client_overrides(
        "Cliente A",
        [
            {"articulo": "SKU-1", "date": "2026-01", "pct": 3.5},
            {"articulo": "SKU-2", "date": "2026-01", "pct": 4.1},
        ],
        growth_pct=growth_pct,
    )

    svc.save_client_overrides(
        "Cliente A",
        [{"articulo": "SKU-1", "date": "2026-01", "pct": default_monthly_pct}],
        growth_pct=growth_pct,
    )

    assert svc._get_client_overrides_snapshot("Cliente A", growth_pct) == {
        ("SKU-2", "2026-01"): 4.1
    }
    assert svc._has_overrides(growth_pct)


def test_effective_override_snapshot_ignores_legacy_no_op_residue():
    growth_pct = 25.0
    default_monthly_pct = round(svc._monthly_pct_from_annual_growth(growth_pct), 4)

    with svc._overrides_lock:
        svc._client_overrides["Cliente A"] = {
            ("SKU-1", "2026-01"): default_monthly_pct,
        }

    assert svc._get_client_overrides_snapshot("Cliente A", growth_pct) == {}
    assert not svc._has_overrides(growth_pct)
