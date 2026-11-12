import datetime as dt
import inspect

import pandas as pd

from web_comparativas import forecast_service as svc
from web_comparativas.forecast_service import (
    _accuracy_closed_months,
    _closed_month_forecast_accuracy,
)


def _monthly(months, values, col="Total_Venta"):
    return pd.DataFrame({"fecha": pd.to_datetime(months), col: values})


def test_closed_month_accuracy_uses_requested_curve_and_excludes_open_month():
    actual = _monthly(["2026-01-01", "2026-02-01", "2026-03-01"], [100.0, 200.0, 300.0])
    forecast = pd.DataFrame({
        "fecha": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
        "Total_Forecast": [90.0, 180.0, 0.0],
        "Total_Adj": [125.0, 250.0, 0.0],
    })
    # Hoy 15/03: marzo es el mes en curso (abierto) → no entra.
    today = dt.date(2026, 3, 15)

    model = _closed_month_forecast_accuracy(actual, forecast, "Total_Forecast", today=today)
    expectation = _closed_month_forecast_accuracy(actual, forecast, "Total_Adj", today=today)

    assert model == 90.0
    assert expectation == 75.0


def test_closed_month_accuracy_returns_zero_without_required_data():
    actual = _monthly(["2026-01-01"], [100.0])

    assert _closed_month_forecast_accuracy(actual, pd.DataFrame(), "Total_Adj") == 0.0
    assert _closed_month_forecast_accuracy(
        actual, pd.DataFrame({"fecha": pd.to_datetime(["2026-01-01"])}), "Total_Adj"
    ) == 0.0


def test_last_loaded_month_counts_once_calendar_closed():
    """Regresión carga oct-2026: forecast_fact_2026 trae ene–oct y hoy es 12/11.

    La regla vieja (meses[:-1]) descartaba octubre aunque ya estaba cerrado.
    Con el corte por calendario octubre entra en el promedio.
    """
    months = ["2026-08-01", "2026-09-01", "2026-10-01"]
    actual = _monthly(months, [100.0, 100.0, 100.0])
    forecast = _monthly(months, [100.0, 100.0, 50.0], col="Total_Forecast")

    acc = _closed_month_forecast_accuracy(
        actual, forecast, "Total_Forecast", today=dt.date(2026, 11, 12)
    )

    assert acc == (100.0 + 100.0 + 50.0) / 3


def test_partial_current_month_is_excluded_even_if_loaded():
    """Si la tabla trae el mes en curso (parcial), no entra hasta que cierre."""
    months = ["2026-10-01", "2026-11-01"]
    actual = _monthly(months, [100.0, 100.0])
    forecast = _monthly(months, [100.0, 0.0], col="Total_Forecast")

    acc = _closed_month_forecast_accuracy(
        actual, forecast, "Total_Forecast", today=dt.date(2026, 11, 12)
    )

    assert acc == 100.0


def test_accuracy_closed_months_uses_first_day_of_current_month_as_cap():
    months = list(pd.to_datetime(["2026-09-01", "2026-10-01", "2026-11-01"]))

    # El 1° del mes el mes anterior ya está cerrado.
    assert _accuracy_closed_months(months, dt.date(2026, 11, 1)) == months[:2]
    assert _accuracy_closed_months(months, dt.date(2026, 11, 30)) == months[:2]
    assert _accuracy_closed_months(months, dt.date(2026, 12, 1)) == months
    # Fechas de fin de mes (CSV local) también se cortan por calendario.
    eom = list(pd.to_datetime(["2026-09-30", "2026-10-31", "2026-11-30"]))
    assert _accuracy_closed_months(eom, dt.date(2026, 11, 12)) == eom[:2]
    # Sin meses cerrados → 0 meses (no se toma el mes abierto como antes).
    assert _accuracy_closed_months(months[:1], dt.date(2026, 9, 10)) == []


def test_prod_and_local_chart_paths_use_calendar_cut():
    """Las dos tarjetas en prod (PG) y el camino local usan el mismo corte."""
    for fn in (svc._pg_get_chart_data_inner, svc.get_chart_data, svc._closed_month_forecast_accuracy):
        src = inspect.getsource(fn)
        assert "_accuracy_closed_months(" in src, fn.__name__
        assert "[:-1] if len(" not in src, fn.__name__
