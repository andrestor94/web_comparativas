"""Regresión del contrato pendiente -> aprobado del Forecast."""
from __future__ import annotations

import uuid

import pytest

from web_comparativas import forecast_service as svc
from web_comparativas.migrations import ensure_forecast_override_storage
from web_comparativas.models import (
    ForecastChangeRequest as CR,
    ForecastUserOverride,
    SessionLocal,
    User,
)
from web_comparativas.routers.forecast_router import _apply_review


def _create_analyst() -> tuple[int, str]:
    ensure_forecast_override_storage()
    email = f"forecast-pending-{uuid.uuid4().hex}@example.com"
    with SessionLocal() as session:
        user = User(email=email, password_hash="test", role="analista")
        session.add(user)
        session.commit()
        session.refresh(user)
        return int(user.id), email


def _admin() -> User:
    return User(id=10_000_001, email="admin-reviewer@example.com", role="admin")


def _cleanup(user_id: int, email: str) -> None:
    with SessionLocal() as session:
        session.query(CR).filter(CR.created_by_username == email).delete(
            synchronize_session=False
        )
        session.query(ForecastUserOverride).filter(
            ForecastUserOverride.user_id == user_id
        ).delete(synchronize_session=False)
        user = session.get(User, user_id)
        if user is not None:
            session.delete(user)
        session.commit()


def _save(user_id: int, email: str, value: float) -> None:
    svc.save_client_overrides(
        user_id=user_id,
        client_id="Cliente A",
        growth_pct=25.0,
        user_email=email,
        subneg_overrides=[{"subneg": "Sub A", "growth_pct": value}],
    )


def _requests(email: str) -> list[CR]:
    with SessionLocal() as session:
        return (
            session.query(CR)
            .filter(CR.created_by_username == email)
            .order_by(CR.id)
            .all()
        )


def _official(user_id: int) -> list[ForecastUserOverride]:
    with SessionLocal() as session:
        return (
            session.query(ForecastUserOverride)
            .filter(ForecastUserOverride.user_id == user_id)
            .filter(ForecastUserOverride.is_active.is_(True))
            .all()
        )


@pytest.fixture(autouse=True)
def clear_cache():
    svc.clear_response_cache()
    yield
    svc.clear_response_cache()


def test_pending_does_not_change_official_and_creator_can_preview():
    user_id, email = _create_analyst()
    try:
        _save(user_id, email, 50.0)
        assert _official(user_id) == []
        assert [r.status for r in _requests(email)] == ["pendiente"]
        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {}
        with svc.forecast_override_context(
            owner_user_ids=(user_id,), preview_user_id=user_id
        ):
            assert svc._get_client_subneg_growths(user_id, "Cliente A") == {
                "Sub A": 50.0
            }
    finally:
        _cleanup(user_id, email)


def test_approval_materializes_official_only_then():
    user_id, email = _create_analyst()
    try:
        _save(user_id, email, 50.0)
        request_id = _requests(email)[0].id
        with SessionLocal() as session:
            cr = session.get(CR, request_id)
            owner_id = _apply_review(
                session, cr, status="aprobado", user=_admin(), motivo="ok"
            )
            session.commit()
        assert owner_id == user_id
        assert [r.status for r in _requests(email)] == ["aprobado"]
        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {
            "Sub A": 50.0
        }
    finally:
        _cleanup(user_id, email)


def test_rejection_preserves_official_and_trace():
    user_id, email = _create_analyst()
    try:
        _save(user_id, email, 50.0)
        request_id = _requests(email)[0].id
        with SessionLocal() as session:
            cr = session.get(CR, request_id)
            _apply_review(
                session, cr, status="rechazado", user=_admin(), motivo="fuera de pauta"
            )
            session.commit()
        assert _official(user_id) == []
        decided = _requests(email)[0]
        assert decided.status == "rechazado"
        assert decided.review_comment == "fuera de pauta"
    finally:
        _cleanup(user_id, email)


def test_new_pending_does_not_replace_previously_approved_official():
    user_id, email = _create_analyst()
    try:
        _save(user_id, email, 40.0)
        first_id = _requests(email)[0].id
        with SessionLocal() as session:
            _apply_review(
                session,
                session.get(CR, first_id),
                status="aprobado",
                user=_admin(),
                motivo=None,
            )
            session.commit()
        _save(user_id, email, 70.0)
        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {
            "Sub A": 40.0
        }
        with svc.forecast_override_context(
            owner_user_ids=(user_id,), preview_user_id=user_id
        ):
            assert svc._get_client_subneg_growths(user_id, "Cliente A") == {
                "Sub A": 70.0
            }
    finally:
        _cleanup(user_id, email)


def test_resaving_same_scope_supersedes_previous_pending():
    user_id, email = _create_analyst()
    try:
        _save(user_id, email, 50.0)
        _save(user_id, email, 60.0)
        requests = _requests(email)
        assert [r.status for r in requests] == ["rechazado", "pendiente"]
        assert requests[0].review_comment is not None
        with svc.forecast_override_context(
            owner_user_ids=(user_id,), preview_user_id=user_id
        ):
            assert svc._get_client_subneg_growths(user_id, "Cliente A") == {
                "Sub A": 60.0
            }
    finally:
        _cleanup(user_id, email)


def test_legacy_active_pending_is_hidden_and_can_be_approved_safely():
    user_id, email = _create_analyst()
    try:
        with SessionLocal() as session:
            override = ForecastUserOverride(
                user_id=user_id,
                source_module=svc.FORECAST_OVERRIDE_SOURCE,
                context_key=svc.FORECAST_OVERRIDE_CONTEXT,
                client_selector="Cliente Legacy",
                client_display="Cliente Legacy",
                override_scope=svc.FORECAST_SCOPE_SUBNEG,
                subneg="Sub Legacy",
                codigo_serie="",
                forecast_month="",
                base_growth_pct=25.0,
                override_growth_pct=55.0,
                effective_monthly_pct=svc._monthly_pct_from_annual_growth(55.0),
                is_active=True,
                created_by=email,
            )
            session.add(override)
            session.flush()
            request = CR(
                override_id=override.id,
                source="legacy",
                created_by_user_id=user_id,
                created_by_username=email,
                change_type="suba_pct",
                scope_type=svc.FORECAST_SCOPE_SUBNEG,
                client_selector="Cliente Legacy",
                client_name="Cliente Legacy",
                subneg="Sub Legacy",
                old_value=None,
                new_value=55.0,
                status="pendiente",
            )
            session.add(request)
            session.commit()
            request_id = request.id

        assert svc._get_client_subneg_growths(
            user_id, "Cliente Legacy"
        ) == {}
        with SessionLocal() as session:
            _apply_review(
                session,
                session.get(CR, request_id),
                status="aprobado",
                user=_admin(),
                motivo="migración lógica",
            )
            session.commit()
        assert svc._get_client_subneg_growths(
            user_id, "Cliente Legacy"
        ) == {"Sub Legacy": 55.0}
    finally:
        _cleanup(user_id, email)
