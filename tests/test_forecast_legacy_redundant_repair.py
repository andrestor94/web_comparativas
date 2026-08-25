from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.forecast_legacy_redundant_repair import (
    COMMENT,
    CONFLICT,
    INCOMPLETE,
    NORMAL,
    REDUNDANT,
    REVIEWER_ID,
    ReplayRequest,
    apply_trace_only,
    fingerprint,
    load_snapshot,
    replay_mutable,
    validate,
)
from web_comparativas.models import (
    Base,
    ForecastChangeRequest as CR,
    ForecastUserOverride as Override,
    User,
)

T0 = dt.datetime(2026, 5, 1, 12, 0, 0)


def _row(
    request_id: int,
    old: float,
    new: float,
    *,
    status: str = "aprobado",
    seconds: int = 0,
    override_id: int = 10,
    selector: str = "Cliente A",
) -> ReplayRequest:
    return ReplayRequest(
        id=request_id,
        override_id=override_id,
        created_at=T0 + dt.timedelta(seconds=seconds),
        status=status,
        owner_id=2,
        selector=selector,
        scope="subnegocio",
        old_value=old,
        new_value=new,
    )


def test_replay_classifies_normal_transition():
    candidate = _row(1, 25.0, 40.0, status="pendiente")
    report = replay_mutable([candidate], [candidate.id])
    assert report.counts()[NORMAL] == 1
    assert report.decisions[0].official_before == 25.0
    assert report.final_by_override == ((10, 40.0),)


def test_replay_classifies_redundant_duplicate():
    rows = [
        _row(1, 25.0, -90.0, seconds=1),
        _row(2, -90.0, -99.0, seconds=2),
        _row(3, -90.0, -99.0, status="pendiente", seconds=3),
    ]
    report = replay_mutable(rows, [3])
    assert report.counts()[REDUNDANT] == 1
    assert report.decisions[0].official_before == -99.0
    assert report.decisions[0].official_after == -99.0
    assert report.final_by_override == ((10, -99.0),)


def test_replay_preserves_valid_transition_after_duplicate():
    rows = [
        _row(1, 25.0, 0.0, seconds=1),
        _row(2, 0.0, 5.0, seconds=2),
        _row(3, 0.0, 5.0, status="pendiente", seconds=3),
        _row(4, 5.0, 10.0, seconds=4),
    ]
    report = replay_mutable(rows, [3])
    assert report.counts()[REDUNDANT] == 1
    assert report.decisions[0].official_before == 5.0
    assert report.final_by_override == ((10, 10.0),)


def test_replay_classifies_real_conflict_and_incomplete():
    rows = [
        _row(1, 25.0, 10.0, seconds=1),
        _row(2, 5.0, 20.0, status="pendiente", seconds=2),
        _row(3, 10.0, 30.0, status="pendiente", seconds=3, selector=""),
    ]
    report = replay_mutable(rows, [2, 3])
    assert report.counts()[CONFLICT] == 1
    assert report.counts()[INCOMPLETE] == 1


@pytest.fixture
def temporary_session_factory(tmp_path):
    db_path = tmp_path / "forecast_legacy_repair.sqlite"
    temp_engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)
    Base.metadata.create_all(
        temp_engine,
        tables=[User.__table__, Override.__table__, CR.__table__],
    )
    factory = sessionmaker(
        bind=temp_engine, autoflush=False, expire_on_commit=False, future=True
    )
    try:
        yield factory
    finally:
        temp_engine.dispose()


def _seed_redundant_chain(factory) -> tuple[int, int]:
    with factory() as session:
        session.add_all(
            [
                User(id=REVIEWER_ID, email="reviewer@example.com", password_hash="x", role="admin"),
                User(id=2, email="owner@example.com", password_hash="x", role="analista"),
            ]
        )
        override = Override(
            id=10,
            user_id=2,
            source_module="forecast",
            context_key="default",
            client_selector="Cliente A",
            client_display="Cliente A",
            override_scope="subnegocio",
            subneg="Sub A",
            codigo_serie="",
            forecast_month="",
            base_growth_pct=25.0,
            override_growth_pct=-99.0,
            effective_monthly_pct=-0.318,
            is_active=True,
            created_at=T0,
            updated_at=T0,
        )
        session.add(override)
        session.flush()
        common = {
            "override_id": override.id,
            "source": "legacy",
            "created_by_user_id": 2,
            "created_by_username": "owner@example.com",
            "change_type": "baja_pct",
            "scope_type": "subnegocio",
            "client_selector": "Cliente A",
            "client_name": "Cliente A",
            "subneg": "Sub A",
        }
        first = CR(
            **common, created_at=T0 + dt.timedelta(seconds=1),
            old_value=25.0, new_value=-90.0, status="aprobado",
        )
        second = CR(
            **common, created_at=T0 + dt.timedelta(seconds=2),
            old_value=-90.0, new_value=-99.0, status="aprobado",
        )
        duplicate = CR(
            **common, created_at=T0 + dt.timedelta(seconds=3),
            old_value=-90.0, new_value=-99.0, status="pendiente",
        )
        session.add_all([first, second, duplicate])
        session.commit()
        return int(override.id), int(duplicate.id)


def _override_signature(session, override_id: int) -> tuple:
    row = session.get(Override, override_id)
    return (
        row.override_growth_pct, row.effective_monthly_pct, row.is_active,
        row.updated_at, row.updated_by,
    )


def test_trace_only_apply_is_idempotent_and_never_changes_override(
    temporary_session_factory,
):
    factory = temporary_session_factory
    override_id, duplicate_id = _seed_redundant_chain(factory)
    stamp = dt.datetime(2026, 8, 24, 15, 0, 0)

    with factory() as session:
        snapshot = load_snapshot(session)
        validate(snapshot, expected=1)
        dry_fingerprint = fingerprint(snapshot)
        before = _override_signature(session, override_id)
        result = apply_trace_only(
            session, expected_fingerprint=dry_fingerprint,
            expected=1, reviewed_at=stamp,
        )
        session.commit()
        assert result["closed"] == 1
        assert _override_signature(session, override_id) == before

    with factory() as session:
        row = session.get(CR, duplicate_id)
        assert row.status == "aprobado"
        assert row.reviewed_by_user_id == REVIEWER_ID
        assert row.reviewed_by_username == "reviewer@example.com"
        assert row.reviewed_at == stamp
        assert row.review_comment == COMMENT
        rerun_snapshot = load_snapshot(session)
        validate(rerun_snapshot, expected=1)
        result = apply_trace_only(
            session, expected_fingerprint=fingerprint(rerun_snapshot), expected=1
        )
        session.commit()
        assert result == {
            "closed": 0,
            "already_closed": 1,
            "fingerprint": fingerprint(rerun_snapshot),
            "before_approved": 3,
            "before_pending": 0,
            "after_approved": 3,
            "after_pending": 0,
        }
        assert session.get(CR, duplicate_id).reviewed_at == stamp


def test_trace_only_rolls_back_when_precondition_changed(temporary_session_factory):
    factory = temporary_session_factory
    override_id, duplicate_id = _seed_redundant_chain(factory)
    with factory() as session:
        dry_fingerprint = fingerprint(load_snapshot(session))
    with factory() as session:
        with pytest.raises(RuntimeError, match="Conteos previos cambiaron"):
            apply_trace_only(
                session,
                expected_fingerprint=dry_fingerprint,
                expected=1,
                expected_before=(999, 999),
            )
        session.rollback()
    with factory() as session:
        assert session.get(CR, duplicate_id).status == "pendiente"

    with factory() as session:
        session.get(CR, duplicate_id).new_value = -98.0
        session.commit()

    with factory() as session:
        before = _override_signature(session, override_id)
        with pytest.raises(RuntimeError):
            apply_trace_only(
                session, expected_fingerprint=dry_fingerprint, expected=1
            )
        session.rollback()

    with factory() as session:
        row = session.get(CR, duplicate_id)
        assert row.status == "pendiente"
        assert row.reviewed_at is None
        assert row.review_comment is None
        assert _override_signature(session, override_id) == before
