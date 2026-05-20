from __future__ import annotations

import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas import forecast_service as svc
from web_comparativas.migrations import ensure_forecast_override_storage
from web_comparativas.models import ForecastUserOverride, SessionLocal, User


def _create_user() -> int:
    ensure_forecast_override_storage()
    email = f"forecast-test-{uuid.uuid4().hex}@example.com"
    with SessionLocal() as session:
        user = User(email=email, password_hash="test", role="admin")
        session.add(user)
        session.commit()
        session.refresh(user)
        return int(user.id)


def _delete_user(user_id: int) -> None:
    with SessionLocal() as session:
        session.query(ForecastUserOverride).filter(
            ForecastUserOverride.user_id == int(user_id)
        ).delete(synchronize_session=False)
        user = session.get(User, int(user_id))
        if user is not None:
            session.delete(user)
        session.commit()


def _active_overrides(user_id: int) -> list[ForecastUserOverride]:
    with SessionLocal() as session:
        return (
            session.query(ForecastUserOverride)
            .filter(ForecastUserOverride.user_id == int(user_id))
            .filter(ForecastUserOverride.is_active.is_(True))
            .order_by(ForecastUserOverride.override_scope, ForecastUserOverride.forecast_month)
            .all()
        )


@pytest.fixture(autouse=True)
def clear_response_cache_between_tests():
    svc.clear_response_cache()
    yield
    svc.clear_response_cache()


def test_cell_override_persists_in_sql_and_is_removed_when_returned_to_default():
    user_id = _create_user()
    growth_pct = 25.0
    default_monthly_pct = round(svc._monthly_pct_from_annual_growth(growth_pct), 4)
    target_month = svc.get_forecast_effective_month()

    try:
        svc.save_client_overrides(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=growth_pct,
            cell_overrides=[
                {"articulo": "SKU-1", "subneg": "Sub A", "date": target_month, "pct": 3.5}
            ],
        )

        assert svc._has_overrides(user_id, growth_pct)
        assert svc._get_client_overrides_snapshot(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=growth_pct,
        ) == {("SKU-1", target_month): 3.5}

        active_rows = _active_overrides(user_id)
        assert len(active_rows) == 1
        assert active_rows[0].override_scope == svc.FORECAST_SCOPE_CELL

        svc.save_client_overrides(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=growth_pct,
            cell_overrides=[
                {
                    "articulo": "SKU-1",
                    "subneg": "Sub A",
                    "date": target_month,
                    "pct": default_monthly_pct,
                }
            ],
        )

        assert svc._get_client_overrides_snapshot(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=growth_pct,
        ) == {}
        assert not svc._has_overrides(user_id, growth_pct)
        assert _active_overrides(user_id) == []
    finally:
        _delete_user(user_id)


def test_subneg_override_persists_and_rehydrates_modal_growths():
    user_id = _create_user()

    try:
        svc.save_client_overrides(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=25.0,
            subneg_overrides=[{"subneg": "Sub A", "growth_pct": 50.0}],
        )

        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {"Sub A": 50.0}

        active_rows = _active_overrides(user_id)
        assert len(active_rows) == 1
        row = active_rows[0]
        assert row.override_scope == svc.FORECAST_SCOPE_SUBNEG
        assert row.subneg == "Sub A"
        assert row.override_growth_pct == pytest.approx(50.0)
        assert row.effective_monthly_pct == pytest.approx(
            svc._monthly_pct_from_annual_growth(50.0)
        )

        svc.save_client_overrides(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=25.0,
            subneg_overrides=[{"subneg": "Sub A", "growth_pct": 25.0}],
        )

        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {}
        assert _active_overrides(user_id) == []
    finally:
        _delete_user(user_id)


def test_client_growth_pct_rehydrates_from_uniform_visible_subneg_overrides():
    negocios = [
        {
            "neg": "Negocio A",
            "subnegs": [
                {"subneg": "Sub A", "products": [{"articulo": "SKU-1"}]},
                {"subneg": "Sub B", "products": [{"articulo": "SKU-2"}]},
            ],
        }
    ]

    assert svc._derive_visible_client_growth_pct(
        negocios, {"Sub A": 38.0, "Sub B": 38.0}, 25.0
    ) == pytest.approx(38.0)
    assert svc._derive_visible_client_growth_pct(
        negocios, {"Sub A": -30.0, "Sub B": -30.0}, 25.0
    ) == pytest.approx(-30.0)
    assert svc._derive_visible_client_growth_pct(
        negocios, {"Sub A": 60.0, "Sub B": 60.0}, 25.0
    ) == pytest.approx(60.0)


def test_client_growth_pct_falls_back_when_subneg_overrides_are_partial_or_mixed():
    negocios = [
        {
            "neg": "Negocio A",
            "subnegs": [
                {"subneg": "Sub A", "products": [{"articulo": "SKU-1"}]},
                {"subneg": "Sub B", "products": [{"articulo": "SKU-2"}]},
            ],
        }
    ]

    assert svc._derive_visible_client_growth_pct(
        negocios, {"Sub A": 38.0}, 25.0
    ) == pytest.approx(25.0)
    assert svc._derive_visible_client_growth_pct(
        negocios, {"Sub A": 38.0, "Sub B": -30.0}, 25.0
    ) == pytest.approx(25.0)
    assert svc._derive_visible_client_growth_pct(
        negocios, {"Sub A": 25.0, "Sub B": 25.0}, 25.0
    ) == pytest.approx(25.0)

    mixed_state = svc._derive_visible_client_growth_state(
        negocios, {"Sub A": 38.0, "Sub B": -30.0}, 25.0
    )
    assert mixed_state["value"] is None
    assert mixed_state["source"] == "mixed"
    assert mixed_state["mixed"] is True


def test_client_growth_pct_ignores_empty_visible_subnegs():
    negocios = [
        {
            "neg": "Negocio A",
            "subnegs": [
                {"subneg": "Sub A", "products": [{"articulo": "SKU-1"}]},
                {"subneg": "Sub sin articulos", "products": []},
            ],
        }
    ]

    state = svc._derive_visible_client_growth_state(
        negocios, {"Sub A": 50.0}, 25.0
    )
    assert state["value"] == pytest.approx(50.0)
    assert state["source"] == "uniform_subneg"
    assert state["mixed"] is False


def test_cell_override_equal_to_subneg_growth_is_not_persisted_twice():
    user_id = _create_user()
    scoped_monthly_pct = round(svc._monthly_pct_from_annual_growth(50.0), 4)

    try:
        svc.save_client_overrides(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=25.0,
            subneg_overrides=[{"subneg": "Sub A", "growth_pct": 50.0}],
            cell_overrides=[
                {
                    "articulo": "SKU-1",
                    "subneg": "Sub A",
                    "date": svc.get_forecast_effective_month(),
                    "pct": scoped_monthly_pct,
                }
            ],
        )

        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {"Sub A": 50.0}
        assert svc._get_client_overrides_snapshot(
            user_id=user_id,
            client_id="Cliente A",
            growth_pct=25.0,
        ) == {}

        active_rows = _active_overrides(user_id)
        assert len(active_rows) == 1
        assert active_rows[0].override_scope == svc.FORECAST_SCOPE_SUBNEG
    finally:
        _delete_user(user_id)


def test_overrides_are_isolated_by_user_and_clear_soft_deactivates_rows():
    user_a = _create_user()
    user_b = _create_user()

    try:
        svc.save_client_overrides(
            user_id=user_a,
            client_id="Cliente A",
            growth_pct=25.0,
            subneg_overrides=[{"subneg": "Sub A", "growth_pct": 40.0}],
            cell_overrides=[
                {"articulo": "SKU-1", "subneg": "Sub A", "date": svc.get_forecast_effective_month(), "pct": 2.9}
            ],
        )

        assert svc._has_overrides(user_a, 25.0)
        assert not svc._has_overrides(user_b, 25.0)
        assert svc._get_client_subneg_growths(user_b, "Cliente A") == {}
        assert svc._get_client_overrides_snapshot(
            user_id=user_b,
            client_id="Cliente A",
            growth_pct=25.0,
        ) == {}

        svc.clear_client_overrides(user_id=user_a, client_id="Cliente A", user_email="tester@example.com")

        assert svc._get_client_subneg_growths(user_a, "Cliente A") == {}
        assert svc._get_client_overrides_snapshot(
            user_id=user_a,
            client_id="Cliente A",
            growth_pct=25.0,
        ) == {}
        assert _active_overrides(user_a) == []
    finally:
        _delete_user(user_a)
        _delete_user(user_b)
