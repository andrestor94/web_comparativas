from types import SimpleNamespace

from web_comparativas import cartera_visibilidad as cartera
from web_comparativas.routers import forecast_router as router


def _restricted_access():
    return router._ForecastAccess(
        branches=((('100',), None),),
        member_user_ids=(77,),
        branch_member_user_ids=((77,),),
        unrestricted=False,
        cache_key="restricted",
    )


def test_render_temporarily_disables_forecast_account_scope(monkeypatch):
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "1")
    monkeypatch.delenv("FORECAST_CARTERA_BYPASS_ALL", raising=False)

    assert cartera.FORECAST_CARTERA_ENABLED() is False


def test_bypass_can_be_reverted_without_code_change(monkeypatch):
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "1")
    monkeypatch.setenv("FORECAST_CARTERA_BYPASS_ALL", "0")

    assert cartera.FORECAST_CARTERA_ENABLED() is True


def test_disabled_scope_makes_forecast_data_global_for_any_module_user(monkeypatch):
    monkeypatch.setenv("FORECAST_CARTERA_BYPASS_ALL", "1")
    user = SimpleNamespace(id=77, role="analista")

    assert router._forecast_data_is_global(user, _restricted_access()) is True


def test_reenabled_scope_preserves_restricted_forecast_access(monkeypatch):
    monkeypatch.setenv("FORECAST_CARTERA_BYPASS_ALL", "0")
    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "1")
    user = SimpleNamespace(id=77, role="analista")

    assert router._forecast_data_is_global(user, _restricted_access()) is False
