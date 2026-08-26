import pandas as pd

from web_comparativas.forecast_service import _inject_manual_entries_into_chart_totals


def _base_chart():
    return pd.DataFrame(
        [{
            "fecha": pd.Timestamp("2026-08-01"),
            "Total_Forecast": 100.0,
            "Total_User_Adj": 125.0,
            "Total_Adj": 125.0,
            "Total_Li": 90.0,
            "Total_Ls": 110.0,
        }]
    )


def test_adjustment_only_changes_only_user_adjusted_curve():
    manual = pd.DataFrame(
        [{
            "fecha": pd.Timestamp("2026-08-01"),
            "monto_yhat": 20.0,
            "adjustment_only": True,
        }]
    )

    result = _inject_manual_entries_into_chart_totals(_base_chart(), manual, "monto_yhat")
    row = result.iloc[0]

    assert row["Total_Forecast"] == 100.0
    assert row["Total_Li"] == 90.0
    assert row["Total_Ls"] == 110.0
    assert row["Total_Adj"] == 125.0
    assert row["Total_User_Adj"] == 145.0


def test_legacy_manual_entry_keeps_existing_all_curves_behavior():
    manual = pd.DataFrame(
        [{
            "fecha": pd.Timestamp("2026-08-01"),
            "monto_yhat": 10.0,
        }]
    )

    result = _inject_manual_entries_into_chart_totals(_base_chart(), manual, "monto_yhat")
    row = result.iloc[0]

    assert row["Total_Forecast"] == 110.0
    assert row["Total_Li"] == 100.0
    assert row["Total_Ls"] == 120.0
    assert row["Total_Adj"] == 135.0
    assert row["Total_User_Adj"] == 135.0


def test_adjustment_only_new_month_has_zero_base_and_nonzero_adjusted():
    manual = pd.DataFrame(
        [{
            "fecha": pd.Timestamp("2026-09-01"),
            "monto_yhat": -30.0,
            "adjustment_only": True,
        }]
    )

    result = _inject_manual_entries_into_chart_totals(_base_chart(), manual, "monto_yhat")
    row = result[result["fecha"] == pd.Timestamp("2026-09-01")].iloc[0]

    assert row["Total_Forecast"] == 0.0
    assert row["Total_Li"] == 0.0
    assert row["Total_Ls"] == 0.0
    assert row["Total_Adj"] == 0.0
    assert row["Total_User_Adj"] == -30.0
