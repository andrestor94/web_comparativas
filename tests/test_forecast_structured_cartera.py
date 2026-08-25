from __future__ import annotations

import pandas as pd

from web_comparativas import forecast_service as svc


def test_forecast_mask_preserves_account_unit_branches_without_crossing():
    df = pd.DataFrame(
        [
            {"cliente_id": "A", "neg": "UN 1", "monto_yhat": 10},
            {"cliente_id": "A", "neg": "UN 2", "monto_yhat": 1000},
            {"cliente_id": "B", "neg": "UN 1", "monto_yhat": 2000},
            {"cliente_id": "B", "neg": "UN 2", "monto_yhat": 20},
        ]
    )
    branches = ((('A',), ('UN 1',)), (('B',), ('UN 2',)))

    visible = df[svc._cartera_mask(df, branches)]

    assert visible[["cliente_id", "neg"]].values.tolist() == [
        ["A", "UN 1"],
        ["B", "UN 2"],
    ]
    assert visible["monto_yhat"].sum() == 30


def test_forecast_mask_is_fail_closed_for_empty_or_incomplete_scope():
    df = pd.DataFrame([{"cliente_id": "A", "neg": "UN 1"}])

    assert not svc._cartera_mask(df, ()).any()
    assert not svc._cartera_mask(df, ((('A',), ()),)).any()
    assert svc._cartera_sql(()) == "1=0"
    assert svc._cartera_sql(((('A',), ()),)) == "1=0"


def test_forecast_fact_mask_uses_series_unit_mapping_per_branch():
    fact = pd.DataFrame(
        [
            {"cliente_id": "A", "codigo_serie": "P1"},
            {"cliente_id": "A", "codigo_serie": "P2"},
            {"cliente_id": "B", "codigo_serie": "P1"},
            {"cliente_id": "B", "codigo_serie": "P2"},
        ]
    )
    series = pd.DataFrame(
        [
            {"codigo_serie": "P1", "neg": "UN 1"},
            {"codigo_serie": "P2", "neg": "UN 2"},
        ]
    )
    branches = ((('A',), ('UN 1',)), (('B',), ('UN 2',)))

    visible = fact[svc._cartera_mask(fact, branches, series_units=series)]

    assert visible.values.tolist() == [["A", "P1"], ["B", "P2"]]


def test_forecast_filter_options_fail_closed_for_no_branches():
    assert svc.get_filter_options(cartera_branches=()) == {
        "profiles": [],
        "neg": [],
        "subneg": [],
        "labs": [],
        "min_date": None,
        "max_date": None,
    }


def test_forecast_chart_fail_closed_does_not_leak_global_product_count():
    payload = svc.get_chart_data(user_id=-1, cartera_branches=())

    assert payload["history"] == []
    assert payload["forecast"] == []
    assert payload["val_2026"] == []
    assert payload["kpis"]["total_proyeccion_2026"] == 0
    assert payload["kpis"]["fact_2026"] == 0
    assert payload["kpis"]["n_products"] == 0
    assert payload["kpis"]["historical_2025_available"] is False
    assert payload["kpis"]["var_nominal_2025"] is None
    assert payload["kpis"]["var_real_2025"] is None
    assert payload["kpis"]["total_historia"] is None
    assert payload["kpis"]["total_real_2025"] is None
    assert "no está desagregado por cliente" in payload["kpis"]["historical_2025_unavailable_reason"]
