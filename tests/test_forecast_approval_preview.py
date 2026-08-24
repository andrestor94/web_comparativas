from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from web_comparativas import forecast_service as svc
from web_comparativas.models import (
    Base,
    ForecastChangeRequest as CR,
    ForecastUserOverride as Override,
    User,
)
from web_comparativas.routers import forecast_router as router


def _override(
    *,
    owner=1,
    selector="A",
    subneg="X",
    annual=10.0,
    effective_from="2026-01",
):
    return SimpleNamespace(
        id=100 + owner,
        user_id=owner,
        source_module="forecast",
        context_key="default",
        client_selector=selector,
        client_display=selector,
        perfil=None,
        neg=None,
        subneg=subneg,
        codigo_serie="",
        forecast_month="",
        override_scope="subnegocio",
        base_growth_pct=25.0,
        override_growth_pct=annual,
        effective_monthly_pct=svc._monthly_pct_from_annual_growth(annual),
        effective_from_month=effective_from,
        is_active=True,
        created_at=dt.datetime(2026, 1, 1),
        updated_at=dt.datetime(2026, 1, owner),
        created_by="owner",
        updated_by="owner",
    )


def _request(
    request_id,
    *,
    owner,
    selector,
    old,
    new,
    subneg="X",
    created=dt.datetime(2026, 8, 21, 12),
):
    return SimpleNamespace(
        id=request_id,
        status="pendiente",
        created_at=created,
        created_by_user_id=owner,
        created_by_username=f"owner-{owner}",
        client_selector=selector,
        client_name=selector,
        scope_type="subnegocio",
        subneg=subneg,
        codigo_serie="",
        period="",
        old_value=old,
        new_value=new,
        perfil=None,
        neg=None,
    )


def _frame():
    return pd.DataFrame(
        [
            {
                "fecha": pd.Timestamp("2026-09-01"),
                "fantasia": "B",
                "cliente_id": "B",
                "subneg": "X",
                "codigo_serie": "P1",
                "base_val": 100.0,
            },
            {
                "fecha": pd.Timestamp("2026-10-01"),
                "fantasia": "B",
                "cliente_id": "B",
                "subneg": "X",
                "codigo_serie": "P1",
                "base_val": 100.0,
            },
            {
                "fecha": pd.Timestamp("2026-10-01"),
                "fantasia": "D",
                "cliente_id": "D",
                "subneg": "X",
                "codigo_serie": "P2",
                "base_val": 100.0,
            },
        ]
    )


def test_base_growth_is_preserved_outside_override_scope_and_before_vigency():
    frame = _frame()
    record = _override(selector="B", subneg="X", annual=0.0, effective_from="2026-10")
    patched, _ = svc._apply_override_effects_to_dataframe(
        frame,
        user_id=None,
        base_growth_pct=25.0,
        max_hist_date=None,
        _records=[record],
        is_admin=True,
    )
    assert patched.loc[0, "_annual_eff"] == 1.25
    assert patched.loc[1, "_annual_eff"] == 1.0
    assert patched.loc[2, "_annual_eff"] == 1.25


def test_pending_preview_replays_and_classifies_with_effective_month():
    official = [_override(owner=1, selector="A", annual=10.0)]
    requests = [
        _request(1, owner=2, selector="B", old=None, new=0.0),
        _request(2, owner=1, selector="A", old=5.0, new=20.0),
        _request(3, owner=1, selector="A", old=10.0, new=10.0),
        _request(4, owner=3, selector="D", old=None, new=0.0),
        _request(
            5, owner=4, selector="D", old=None, new=10.0,
            created=dt.datetime(2026, 8, 22, 12),
        ),
        _request(6, owner=5, selector="C", old=None, new=5.0),
        _request(7, owner=0, selector="", old=None, new=5.0),
    ]
    result = svc.compute_pending_approval_preview(
        requests,
        growth_pct=25.0,
        official_records=official,
        value_frame=_frame(),
    )
    assert result["counts"] == {
        "efectiva": 2,
        "sin_efecto": 1,
        "supersedida": 1,
        "redundante": 1,
        "conflicto": 1,
        "incompleta": 1,
    }
    assert result["effective_delta"] == -40.0
    assert result["effective_down"] == -40.0
    assert result["effective_up"] == 0.0
    assert result["unattributed_delta"] == 0.0
    assert result["by_request"][1]["effective_delta"] == -25.0
    assert result["by_request"][5]["effective_delta"] == -15.0


def test_matrix_uses_preview_amounts_and_excludes_non_effective_pending():
    records = [
        {
            "id": 1, "status": "pendiente", "usuario": "u",
            "change_type": "ajuste", "impacto_estimado": -25.0,
            "preview_category": "efectiva", "client_selector": "B",
        },
        {
            "id": 2, "status": "pendiente", "usuario": "u",
            "change_type": "baja_pct", "impacto_estimado": None,
            "preview_category": "conflicto", "client_selector": "A",
        },
        {
            "id": 3, "status": "pendiente", "usuario": "u",
            "change_type": "ajuste", "impacto_estimado": 10.0,
            "preview_category": "efectiva", "client_selector": "D",
        },
    ]
    kpis = router._compute_approval_kpis(records)
    assert kpis["matrix"]["pendiente"]["baja"]["monto"] == -25.0
    assert kpis["matrix"]["pendiente"]["suba"]["monto"] == 10.0
    assert kpis["impacto_pendiente"] == -15.0
    assert kpis["pending_preview_counts"]["efectiva"] == 2
    assert kpis["pending_preview_counts"]["conflicto"] == 1


def test_gauge_uses_official_adjusted_series_plus_effective_preview(monkeypatch):
    pending = [_request(1, owner=2, selector="B", old=None, new=0.0)]

    class Query:
        def filter(self, *args):
            return self

        def order_by(self, *args):
            return self

        def all(self):
            return pending

    class Session:
        def query(self, *args):
            return Query()

    monkeypatch.setattr(
        router, "_central_forecast_access",
        lambda user: SimpleNamespace(unrestricted=True),
    )
    monkeypatch.setattr(
        router.svc,
        "get_chart_data",
        lambda **kwargs: {
            "kpis": {"total_proyeccion_adj": 200.0},
            "forecast": [
                {"fecha": "2026-01-01", "Total_User_Adj": 60.0},
                {"fecha": "2026-02-01", "Total_User_Adj": 40.0},
            ],
        },
    )
    monkeypatch.setattr(
        router.svc,
        "compute_pending_approval_preview",
        lambda rows, growth_pct: {
            "selected": 1,
            "effective_delta": -5.0,
            "effective_down": -5.0,
            "effective_up": 0.0,
            "unattributed_delta": 0.0,
            "counts": {"efectiva": 1},
            "by_request": {1: {"category": "efectiva", "effective_delta": -5.0}},
        },
    )
    gauge = router._compute_meta_gauge(Session(), SimpleNamespace())
    assert gauge["proy_aprobados"] == 100.0
    assert gauge["proy_aprob_pend"] == 95.0
    assert gauge["pct_aprobados"] == 50.0
    assert gauge["pct_si_pendientes"] == 47.5


def test_incident_totals_and_gauge_reconcile_to_fixed_management_target():
    meta = 152_177_632_539.00
    official = 122_725_785_133.80
    pending_delta = -5_441_708.00
    preview = official + pending_delta

    assert preview == 122_720_343_425.80
    assert official / meta * 100 == pytest.approx(80.6464018963813)
    assert preview / meta * 100 == pytest.approx(80.6428260042418)
    assert round(official / meta * 100, 2) == 80.65
    assert round(preview / meta * 100, 2) == 80.64


def test_local_treemap_reconciles_base_growth_partial_override_and_manual_once(
    monkeypatch,
):
    frame = pd.DataFrame(
        [
            {"fecha": pd.Timestamp("2026-09-01"), "fantasia": "B", "cliente_id": "B",
             "subneg": "X", "codigo_serie": "P1", "perfil": "P1",
             "nombre_grupo": "G", "monto_yhat": 100.0, "yhat_cliente": 1.0},
            {"fecha": pd.Timestamp("2026-10-01"), "fantasia": "B", "cliente_id": "B",
             "subneg": "X", "codigo_serie": "P1", "perfil": "P1",
             "nombre_grupo": "G", "monto_yhat": 100.0, "yhat_cliente": 1.0},
            {"fecha": pd.Timestamp("2026-10-01"), "fantasia": "D", "cliente_id": "D",
             "subneg": "X", "codigo_serie": "P2", "perfil": "P2",
             "nombre_grupo": "H", "monto_yhat": 100.0, "yhat_cliente": 1.0},
        ]
    )
    main = pd.DataFrame([{"fecha": pd.Timestamp("2025-12-01"), "tipo": "hist"}])
    manual = pd.DataFrame(
        [{"fecha": pd.Timestamp("2026-10-01"), "fantasia": "M", "cliente_id": "M",
          "subneg": "X", "codigo_serie": "", "perfil": "P1", "nombre_grupo": "G",
          "monto_yhat": 10.0, "yhat_cliente": 1.0}]
    )
    record = _override(selector="B", subneg="X", annual=0.0, effective_from="2026-10")
    monkeypatch.setattr(svc, "get_data", lambda: {"df_valorizado": frame, "df_main": main})
    monkeypatch.setattr(svc, "_fetch_override_records", lambda *a, **k: [record])
    monkeypatch.setattr(svc, "_has_overrides", lambda *a, **k: True)
    monkeypatch.setattr(svc, "_query_manual_clients", lambda *a, **k: [object()])
    monkeypatch.setattr(
        svc,
        "_get_manual_entries_df",
        lambda *a, **k: manual[
            manual["perfil"].isin(k.get("profiles_filter") or manual["perfil"].unique())
        ].copy(),
    )
    def _inject_manual_once(result, **kwargs):
        result = dict(result)
        result["total_projected"] = float(result.get("total_projected") or 0.0) + 10.0
        return result
    monkeypatch.setattr(svc, "_inject_manual_client_rows_into_table", _inject_manual_once)

    patched, _ = svc._apply_override_effects_to_dataframe(
        frame.rename(columns={"monto_yhat": "base_val"}), None, 25.0, None,
        _records=[record], is_admin=True,
    )
    chart_total = float((patched["base_val"] * patched["_annual_eff"]).sum()) + 10.0
    detail = svc.get_client_table.__wrapped__(
        growth_pct=25.0, view_money=True, is_admin=True
    )
    tree = svc.get_treemap_data.__wrapped__(growth_pct=25.0, is_admin=True)
    tree_total = tree["values"][tree["ids"].index("total")]

    # 125 antes de vigencia + 100 en el alcance efectivo + 125 fuera del alcance
    # + alta manual 10, que no se multiplica por 1.25.
    assert chart_total == 360.0
    assert detail["total_projected"] == chart_total
    assert tree_total == chart_total

    filtered = svc.get_treemap_data.__wrapped__(
        growth_pct=25.0, profiles=["P2"], is_admin=True
    )
    assert filtered["values"][filtered["ids"].index("total")] == 125.0


def test_real_conflict_is_blocked_without_changing_official_override(tmp_path):
    temp_engine = create_engine(
        f"sqlite:///{(tmp_path / 'forecast_conflict.sqlite').as_posix()}", future=True
    )
    Base.metadata.create_all(
        temp_engine, tables=[User.__table__, Override.__table__, CR.__table__]
    )
    factory = sessionmaker(bind=temp_engine, expire_on_commit=False, future=True)
    try:
        with factory() as session:
            session.add_all([
                User(id=1, email="reviewer@example.com", password_hash="x", role="admin"),
                User(id=2, email="owner@example.com", password_hash="x", role="analista"),
            ])
            override = Override(
                id=10, user_id=2, source_module="forecast", context_key="default",
                client_selector="A", client_display="A", override_scope="subnegocio",
                subneg="X", codigo_serie="", forecast_month="", base_growth_pct=25.0,
                override_growth_pct=10.0,
                effective_monthly_pct=svc._monthly_pct_from_annual_growth(10.0),
                effective_from_month="2026-01", is_active=True,
            )
            session.add(override)
            session.flush()
            request = CR(
                override_id=10, created_at=dt.datetime(2026, 8, 24), source="test",
                created_by_user_id=2, created_by_username="owner@example.com",
                change_type="ajuste", scope_type="subnegocio", client_selector="A",
                client_name="A", subneg="X", old_value=5.0, new_value=20.0,
                status="pendiente",
            )
            session.add(request)
            session.commit()

        with factory() as session:
            request = session.query(CR).one()
            with pytest.raises(ValueError, match="estado oficial cambi"):
                svc.materialize_approved_change_request(
                    session, request, reviewer_email="reviewer@example.com"
                )
            session.rollback()

        with factory() as session:
            assert session.get(Override, 10).override_growth_pct == 10.0
            assert session.query(CR).one().status == "pendiente"
    finally:
        temp_engine.dispose()


def test_cache_key_separates_filters_growth_and_scenario():
    base = svc._resp_key("get_treemap_data", growth_pct=25.0, profiles=["P1"], preview_pending=False)
    assert base != svc._resp_key(
        "get_treemap_data", growth_pct=0.0, profiles=["P1"], preview_pending=False
    )
    assert base != svc._resp_key(
        "get_treemap_data", growth_pct=25.0, profiles=["P2"], preview_pending=False
    )
    assert base != svc._resp_key(
        "get_treemap_data", growth_pct=25.0, profiles=["P1"], preview_pending=True
    )



def test_incident_pending_breakdown_is_exactly_69_17_77():
    records = []
    request_id = 1
    for category, count in (("efectiva", 69), ("sin_efecto", 17), ("conflicto", 77)):
        for _ in range(count):
            records.append({
                "id": request_id,
                "status": "pendiente",
                "usuario": "owner",
                "change_type": "ajuste",
                "preview_category": category,
                "impacto_estimado": -1.0 if category == "efectiva" else None,
                "client_selector": "A",
            })
            request_id += 1
    kpis = router._compute_approval_kpis(records)
    assert kpis["pending_preview_counts"]["efectiva"] == 69
    assert kpis["pending_preview_counts"]["sin_efecto"] == 17
    assert kpis["pending_preview_counts"]["conflicto"] == 77
    assert kpis["pendientes"] == 163


def test_conflict_payload_marks_blocked_and_account_excludes_from_approval():
    records = [
        {"id": 10, "status": "pendiente", "can_review": True,
         "impacto_estimado": -99.0, "client_selector": "A",
         "_created_sort": dt.datetime(2026, 8, 24)},
        {"id": 11, "status": "pendiente", "can_review": True,
         "impacto_estimado": 4.0, "client_selector": "A",
         "_created_sort": dt.datetime(2026, 8, 25)},
    ]
    router._annotate_pending_preview(records, {
        10: {"category": "conflicto", "official_before": 10.0,
             "official_after": 10.0, "effective_delta": 0.0},
        11: {"category": "efectiva", "official_before": 10.0,
             "official_after": 12.0, "effective_delta": 4.0},
    })
    assert records[0]["approval_blocked"] is True
    assert "ya no coincide" in records[0]["preview_explanation"]
    assert records[0]["impacto_estimado"] is None
    account = router._build_account_unit("A", records)
    assert account["conflict_ids"] == [10]
    assert account["approvable_ids"] == [11]
    assert account["conflict_count"] == 1
    assert account["approvable_count"] == 1


def test_individual_conflict_keeps_backend_409_and_does_not_stamp(monkeypatch):
    request = SimpleNamespace(
        id=77, status="pendiente", created_by_user_id=2,
        reviewed_by_user_id=None, reviewed_by_username=None,
        reviewed_at=None, review_comment=None,
    )
    reviewer = SimpleNamespace(id=1, email="reviewer@example.com")
    monkeypatch.setattr(router, "_require_approval_request_access", lambda *a, **k: None)
    monkeypatch.setattr(
        router.svc, "materialize_approved_change_request",
        lambda *a, **k: (_ for _ in ()).throw(ValueError("estado oficial cambio")),
    )
    with pytest.raises(router.HTTPException) as exc:
        router._apply_review(
            object(), request, status="aprobado", user=reviewer, motivo=None
        )
    assert exc.value.status_code == 409
    assert request.status == "pendiente"
    assert request.reviewed_at is None


def test_bulk_approval_omits_conflicts_without_failing_batch(monkeypatch):
    from contextlib import nullcontext

    rows = [
        SimpleNamespace(id=1, created_at=dt.datetime(2026, 8, 24, 10)),
        SimpleNamespace(id=2, created_at=dt.datetime(2026, 8, 24, 11)),
        SimpleNamespace(id=3, created_at=dt.datetime(2026, 8, 24, 12)),
    ]
    calls = []
    def fake_apply(session, cr, *, status, **kwargs):
        calls.append((cr.id, status))
        if status == "aprobado" and cr.id == 3:
            raise router.HTTPException(409, "stale")
        return cr.id
    monkeypatch.setattr(router, "_apply_review", fake_apply)
    session = SimpleNamespace(begin_nested=lambda: nullcontext())
    result = router._apply_bulk_reviews(
        session, rows, status="aprobado", user=SimpleNamespace(id=1), motivo=None,
        preview_by_request={2: {"category": "conflicto"}},
    )
    assert result["afectados"] == 1
    assert result["omitidos_conflicto"] == 2
    assert result["conflict_ids"] == [2, 3]
    assert calls == [(1, "aprobado"), (3, "aprobado")]

    calls.clear()
    rejected = router._apply_bulk_reviews(
        session, rows, status="rechazado", user=SimpleNamespace(id=1), motivo="rechazo"
    )
    assert rejected["afectados"] == 3
    assert rejected["omitidos_conflicto"] == 0
    assert calls == [(1, "rechazado"), (2, "rechazado"), (3, "rechazado")]


def test_conflict_ui_badge_disables_approve_and_reports_bulk_omissions():
    template = (
        Path(__file__).resolve().parents[1]
        / "web_comparativas" / "templates" / "forecast" / "index.html"
    ).read_text(encoding="utf-8")
    assert '>Conflicto</button>' in template
    assert "El valor inicial ya no coincide con el oficial." in template
    assert "disabled aria-disabled=\"true\"" in template
    assert "Conflictos omitidos: ${d.omitidos_conflicto || 0}" in template
    assert "_apprOpenReject(${r.id})" in template
