"""Autorización jerárquica y por rama de solicitudes Forecast."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from web_comparativas.models import User
from web_comparativas.routers import forecast_router as router
from web_comparativas import policy


class _FakeSession:
    def __init__(self, users):
        self.users = {int(u.id): u for u in users}

    def get(self, model, record_id):
        if model is User:
            return self.users.get(int(record_id))
        return None


def _user(uid: int, role: str, parent: int | None = None) -> User:
    return User(
        id=uid,
        email=f"{role}-{uid}@example.com",
        role=role,
        reporta_a_id=parent,
    )


def _request(owner: int, selector="Cliente A", neg="UN 1"):
    return SimpleNamespace(
        id=900 + owner,
        status="pendiente",
        created_by_user_id=owner,
        client_selector=selector,
        neg=neg,
    )


def _access():
    # Ramas incompatibles si se aplanaran: A/UN1/usuarios 11-21 y
    # B/UN2/usuarios 12-22.
    return router._ForecastAccess(
        branches=((("A",), ("UN 1",)), (("B",), ("UN 2",))),
        member_user_ids=(11, 12, 21, 22),
        branch_member_user_ids=((11, 21), (12, 22)),
        unrestricted=False,
        cache_key="test",
    )


@pytest.fixture
def hierarchy(monkeypatch):
    """Rama con FORECAST_CARTERA_ENABLED=1 explícito (no confiar en el .env
    ambiente): esta fixture prueba la política NUEVA (2026-08-20). Ver
    `test_legacy_matrix_*` para el mismo armado con el flag apagado."""
    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "1")
    manager = _user(3, "gerente")
    supervisor_a = _user(11, "supervisor", 3)
    supervisor_b = _user(12, "supervisor", 3)
    analyst_a = _user(21, "analista", 11)
    analyst_b = _user(22, "analista", 12)
    session = _FakeSession(
        [manager, supervisor_a, supervisor_b, analyst_a, analyst_b]
    )
    monkeypatch.setattr(router, "_central_forecast_access", lambda _u: _access())

    def visible(selector, branches):
        accounts, _units = branches[0]
        return (selector == "Cliente A" and "A" in accounts) or (
            selector == "Cliente B" and "B" in accounts
        )

    monkeypatch.setattr(router.svc, "forecast_client_visible", visible)
    return session, manager, supervisor_a, supervisor_b, analyst_a, analyst_b


def test_analyst_never_sees_or_reviews_via_approvals_tab(hierarchy):
    """Política 2026-08-20: la pestaña Aprobaciones Forecast (y por lo tanto
    _can_view/_can_review_approval_request, que solo ella consume) queda vedada
    para Analista — incluso su propia solicitud. Para ver lo suyo usa "Mis
    cambios pendientes", un mecanismo aparte que no pasa por estas funciones."""
    session, _manager, _sup_a, _sup_b, analyst_a, analyst_b = hierarchy
    own = _request(analyst_a.id)
    other = _request(analyst_b.id, "Cliente B", "UN 2")
    assert not router._can_view_approval_request(session, analyst_a, own)
    assert not router._can_view_approval_request(session, analyst_a, other)
    assert not router._can_review_approval_request(session, analyst_a, own)


def test_supervisor_never_sees_or_reviews_even_inside_own_branch(hierarchy):
    """Política 2026-08-20: Supervisor pierde la pestaña por completo (ni modo
    solo lectura), aunque la solicitud sea de un analista de su propia rama —
    antes SÍ podía revisarla; ese es justo el caso que se corrige acá."""
    session, _manager, supervisor_a, _sup_b, analyst_a, analyst_b = hierarchy
    own_team_request = _request(analyst_a.id)
    assert not router._can_view_approval_request(session, supervisor_a, own_team_request)
    assert not router._can_review_approval_request(session, supervisor_a, own_team_request)
    with pytest.raises(HTTPException) as denied:
        router._require_approval_request_access(session, supervisor_a, own_team_request)
    assert denied.value.status_code == 403

    other_team_request = _request(analyst_b.id, "Cliente B", "UN 2")
    assert not router._can_review_approval_request(session, supervisor_a, other_team_request)
    crossed = _request(analyst_a.id, "Cliente B", "UN 2")
    assert not router._can_review_approval_request(session, supervisor_a, crossed)
    with pytest.raises(HTTPException) as denied_crossed:
        router._require_approval_request_access(session, supervisor_a, crossed)
    assert denied_crossed.value.status_code == 403


def test_manager_reviews_both_supervisor_and_analyst_in_branch(hierarchy):
    """Política 2026-08-20: Gerente decide TODO dentro de su rama, sin importar
    si la propuesta la creó un Analista o un Supervisor — antes solo podía
    revisar al Supervisor directo, nunca a un Analista."""
    session, manager, supervisor_a, _sup_b, analyst_a, _analyst_b = hierarchy
    supervisor_request = _request(supervisor_a.id)
    analyst_request = _request(analyst_a.id)
    assert router._can_review_approval_request(session, manager, supervisor_request)
    assert router._can_view_approval_request(session, manager, analyst_request)
    assert router._can_review_approval_request(session, manager, analyst_request)
    router._require_approval_request_access(session, manager, analyst_request)


def test_never_self_approval(hierarchy):
    session, manager, supervisor_a, _sup_b, _analyst_a, _analyst_b = hierarchy
    assert not router._can_review_approval_request(
        session, supervisor_a, _request(supervisor_a.id)
    )
    assert not router._can_review_approval_request(
        session, manager, _request(manager.id)
    )


def test_admin_global_auditor_read_only_and_unknown_closed(hierarchy):
    session, _manager, supervisor_a, _sup_b, _analyst_a, _analyst_b = hierarchy
    cr = _request(supervisor_a.id)
    admin = _user(100, "admin")
    auditor = _user(101, "auditor")
    unknown = _user(102, "viewer")
    assert router._can_view_approval_request(session, admin, cr)
    assert router._can_review_approval_request(session, admin, cr)
    assert router._can_view_approval_request(session, auditor, cr)
    assert not router._can_review_approval_request(session, auditor, cr)
    assert not router._can_view_approval_request(session, unknown, cr)
    assert not router._can_review_approval_request(session, unknown, cr)


# ─────────────────────────────────────────────────────────────────────────────
# FORECAST_CARTERA_ENABLED apagado: matriz previa a la corrección de política
# (2026-08-20). Objetivo del gate: subir este código no cambia nada visible en
# producción hasta que se prenda el flag. Estos tests NO dependen de
# `_central_forecast_access`/`forecast_client_visible` — con el flag apagado
# esas funciones ni se llaman (ver `_approval_access_for`), así que no hace
# falta el monkeypatch de ramas: alcanza con `hierarchy` para reusar los
# usuarios/sesión, pisando el flag a "0" adentro de cada test.
# ─────────────────────────────────────────────────────────────────────────────

def test_legacy_supervisor_reviews_any_pending_request_no_cartera(hierarchy, monkeypatch):
    """Flag apagado: Supervisor decide sobre CUALQUIER solicitud pendiente, sin
    scoping de cartera ni de UN — a propósito uso un cliente/UN que no
    coincide con ninguna rama de `_access()` (cruzado), porque con el flag
    apagado esa función ni se consulta."""
    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "0")
    session, _manager, supervisor_a, _sup_b, _analyst_a, analyst_b = hierarchy
    crossed = _request(analyst_b.id, "Cliente Que No Existe En Ninguna Rama", "UN Que Tampoco")
    assert router._can_view_approval_request(session, supervisor_a, crossed)
    assert router._can_review_approval_request(session, supervisor_a, crossed)
    router._require_approval_request_access(session, supervisor_a, crossed)


def test_legacy_analyst_sees_only_own_and_never_reviews(hierarchy, monkeypatch):
    """Flag apagado: Analista sigue viendo solo lo propio (no es "sin scoping",
    es ownership simple, no depende de cartera/reporta_a_id) y sigue sin poder
    decidir — eso nunca dependió del flag."""
    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "0")
    session, _manager, _sup_a, _sup_b, analyst_a, analyst_b = hierarchy
    own = _request(analyst_a.id)
    other = _request(analyst_b.id, "Cliente B", "UN 2")
    assert router._can_view_approval_request(session, analyst_a, own)
    assert not router._can_view_approval_request(session, analyst_a, other)
    assert not router._can_review_approval_request(session, analyst_a, own)


def test_legacy_still_bans_self_approval(hierarchy, monkeypatch):
    """La autoaprobación no es parte del "scoping de cartera" — es un piso de
    sanidad independiente del flag."""
    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "0")
    session, manager, supervisor_a, _sup_b, _analyst_a, _analyst_b = hierarchy
    assert not router._can_review_approval_request(session, supervisor_a, _request(supervisor_a.id))
    assert not router._can_review_approval_request(session, manager, _request(manager.id))


def test_flag_toggle_switches_behavior_immediately(hierarchy, monkeypatch):
    """Mismo CR, mismo Supervisor: prender/apagar el flag alcanza para cambiar
    el resultado — `_flag()` lee el env var en cada llamada sin cachear, así
    que bajar el flag revierte sin redeploy."""
    session, _manager, supervisor_a, _sup_b, analyst_a, _analyst_b = hierarchy
    same_branch_request = _request(analyst_a.id)  # cliente/UN de la rama de supervisor_a

    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "0")
    assert router._can_review_approval_request(session, supervisor_a, same_branch_request)

    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "1")
    assert not router._can_review_approval_request(session, supervisor_a, same_branch_request)


def test_policy_aprobaciones_matrix_both_flag_states(monkeypatch):
    """`puede_ver/editar_aprobaciones_forecast` (policy.py) — la puerta del tab
    en sí, un nivel por encima de `_can_view/_can_review_approval_request`."""
    admin = _user(1, "admin")
    manager = _user(3, "gerente")
    supervisor = _user(11, "supervisor")
    analyst = _user(21, "analista")
    auditor = _user(41, "auditor")

    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "0")
    assert policy.puede_ver_aprobaciones_forecast(supervisor)
    assert policy.puede_editar_aprobaciones_forecast(supervisor)
    assert policy.puede_ver_aprobaciones_forecast(analyst)
    assert not policy.puede_editar_aprobaciones_forecast(analyst)

    monkeypatch.setenv("FORECAST_CARTERA_ENABLED", "1")
    assert not policy.puede_ver_aprobaciones_forecast(supervisor)
    assert not policy.puede_editar_aprobaciones_forecast(supervisor)
    assert not policy.puede_ver_aprobaciones_forecast(analyst)
    assert policy.puede_ver_aprobaciones_forecast(manager)
    assert policy.puede_editar_aprobaciones_forecast(manager)
    assert policy.puede_ver_aprobaciones_forecast(auditor)
    assert not policy.puede_editar_aprobaciones_forecast(auditor)
    assert policy.puede_ver_aprobaciones_forecast(admin)
    assert policy.puede_editar_aprobaciones_forecast(admin)
