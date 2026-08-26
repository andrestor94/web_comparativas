import pandas as pd

from web_comparativas.forecast_service import _closed_month_forecast_accuracy


def test_closed_month_accuracy_uses_requested_curve_and_excludes_open_month():
    actual = pd.DataFrame({
        "fecha": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
        "Total_Venta": [100.0, 200.0, 300.0],
    })
    forecast = pd.DataFrame({
        "fecha": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
        "Total_Forecast": [90.0, 180.0, 0.0],
        "Total_Adj": [125.0, 250.0, 0.0],
    })

    model = _closed_month_forecast_accuracy(actual, forecast, "Total_Forecast")
    expectation = _closed_month_forecast_accuracy(actual, forecast, "Total_Adj")

    assert model == 90.0
    assert expectation == 75.0


def test_closed_month_accuracy_returns_zero_without_required_data():
    actual = pd.DataFrame({
        "fecha": pd.to_datetime(["2026-01-01"]),
        "Total_Venta": [100.0],
    })

    assert _closed_month_forecast_accuracy(actual, pd.DataFrame(), "Total_Adj") == 0.0
    assert _closed_month_forecast_accuracy(
        actual, pd.DataFrame({"fecha": pd.to_datetime(["2026-01-01"])}), "Total_Adj"
    ) == 0.0
