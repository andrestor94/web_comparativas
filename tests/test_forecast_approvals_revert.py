"""Compuerta de aprobación: RECHAZAR revierte el override vigente.

Cubre la lógica de reversión enganchada en _apply_review (los 3 caminos pasan por
ahí) + la primitiva svc.deactivate_override_by_id:
  - rechazar desactiva el override vigente (vuelve a la base) y aprobar NO,
  - cascada: rechazar 1 CR marca TODAS las pendientes hermanas del mismo override,
  - no-op idempotente sobre override ya inactivo / override_id NULL (sin error),
  - grano subnegocio (revierte el alcance entero),
  - invalidación de caché con el user_id DUEÑO del override (no el del admin),
    tanto por el endpoint individual como por el camino by-ids.

Solo LOCAL. Cada test crea y limpia sus propios usuarios/overrides/change-requests
en app.db (correr con uvicorn DETENIDO).
"""
from __future__ import annotations

import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas import forecast_service as svc
from web_comparativas.migrations import ensure_forecast_override_storage
from web_comparativas.models import (
    ForecastChangeRequest as CR,
    ForecastUserOverride,
    SessionLocal,
    User,
)
from web_comparativas.routers.forecast_router import (
    _ByIdsReviewPayload,
    _ReviewPayload,
    api_approvals_reject,
    _apply_review,
    _review_by_ids,
)


# ── helpers ───────────────────────────────────────────────────────────────────
def _create_cotizador() -> tuple[int, str]:
    """Crea un usuario (dueño de overrides) y devuelve (id, email)."""
    ensure_forecast_override_storage()
    email = f"forecast-revert-{uuid.uuid4().hex}@example.com"
    with SessionLocal() as session:
        user = User(email=email, password_hash="test", role="analista")
        session.add(user)
        session.commit()
        session.refresh(user)
        return int(user.id), email


def _admin_reviewer() -> User:
    """Admin en memoria (no persistido): _apply_review solo lee id/email/role."""
    return User(id=10_000_001, email="admin-reviewer@example.com", role="admin")


def _cleanup(user_id: int, email: str) -> None:
    # Las CR se scopean por el email ÚNICO (uuid), no por user_id: SQLite recicla
    # ids autoincrement y la suite vieja deja CR huérfanas de usuarios borrados.
    with SessionLocal() as session:
        session.query(CR).filter(CR.created_by_username == email).delete(
            synchronize_session=False
        )
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
            .all()
        )


def _crs_for_user(email: str) -> list[CR]:
    # Scope por email único → inmune a la recirculación de ids de SQLite.
    with SessionLocal() as session:
        return (
            session.query(CR)
            .filter(CR.created_by_username == email)
            .order_by(CR.id)
            .all()
        )


def _save_subneg(user_id: int, email: str, growth_pct: float) -> None:
    svc.save_client_overrides(
        user_id=user_id,
        client_id="Cliente A",
        growth_pct=25.0,
        user_email=email,
        subneg_overrides=[{"subneg": "Sub A", "growth_pct": growth_pct}],
    )


@pytest.fixture(autouse=True)
def clear_response_cache_between_tests():
    svc.clear_response_cache()
    yield
    svc.clear_response_cache()


# ── tests ─────────────────────────────────────────────────────────────────────
def test_reject_deactivates_vigente_override():
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    try:
        _save_subneg(user_id, email, 50.0)
        assert len(_active_overrides(user_id)) == 1
        crs = _crs_for_user(email)
        assert len(crs) == 1 and crs[0].status == "pendiente"
        cr_id = crs[0].id

        with SessionLocal() as session:
            cr = session.get(CR, cr_id)
            owner = _apply_review(session, cr, status="rechazado", user=admin, motivo="no aprobado")
            session.commit()

        assert owner == user_id  # dueño devuelto para invalidar su caché
        assert _active_overrides(user_id) == []  # override revertido
        after = _crs_for_user(email)
        assert after[0].status == "rechazado"
        assert after[0].review_comment == "no aprobado"
    finally:
        _cleanup(user_id, email)


def test_approve_does_not_touch_override():
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    try:
        _save_subneg(user_id, email, 50.0)
        cr_id = _crs_for_user(email)[0].id

        with SessionLocal() as session:
            cr = session.get(CR, cr_id)
            owner = _apply_review(session, cr, status="aprobado", user=admin, motivo=None)
            session.commit()

        assert owner is None  # aprobar no revierte → nada que invalidar
        assert len(_active_overrides(user_id)) == 1  # override intacto
        assert _crs_for_user(email)[0].status == "aprobado"
    finally:
        _cleanup(user_id, email)


def test_reject_cascades_to_all_sibling_pending_crs():
    """3 ediciones de un mismo alcance → 3 CR, 1 override vigente. Rechazar la MÁS
    VIEJA deja las 3 en 'rechazado' y el override inactivo (unidad = vigente)."""
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    try:
        _save_subneg(user_id, email, 50.0)
        _save_subneg(user_id, email, 60.0)
        _save_subneg(user_id, email, 70.0)

        crs = _crs_for_user(email)
        assert len(crs) == 3
        assert {c.status for c in crs} == {"pendiente"}
        # upsert → las 3 apuntan al mismo override vigente
        override_ids = {c.override_id for c in crs}
        assert len(override_ids) == 1 and None not in override_ids
        oldest_cr_id = crs[0].id

        with SessionLocal() as session:
            cr = session.get(CR, oldest_cr_id)
            owner = _apply_review(session, cr, status="rechazado", user=admin, motivo="bloque")
            session.commit()

        assert owner == user_id
        assert _active_overrides(user_id) == []
        after = _crs_for_user(email)
        assert len(after) == 3
        assert {c.status for c in after} == {"rechazado"}  # cascada a las 3
        assert all(c.review_comment == "bloque" for c in after)
    finally:
        _cleanup(user_id, email)


def test_reject_noop_on_already_inactive_override():
    """Si el override vinculado ya está inactivo, rechazar es no-op sobre el forecast
    (cambia status, no toca el override) y NO tira error."""
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    try:
        _save_subneg(user_id, email, 50.0)
        cr_id = _crs_for_user(email)[0].id
        # Simular que el override ya fue revertido antes.
        with SessionLocal() as session:
            ov = (
                session.query(ForecastUserOverride)
                .filter(ForecastUserOverride.user_id == int(user_id))
                .first()
            )
            ov.is_active = False
            session.commit()

        with SessionLocal() as session:
            cr = session.get(CR, cr_id)
            owner = _apply_review(session, cr, status="rechazado", user=admin, motivo="tardío")
            session.commit()

        assert owner is None  # nada que desactivar → sin invalidación
        assert _active_overrides(user_id) == []
        assert _crs_for_user(email)[0].status == "rechazado"
    finally:
        _cleanup(user_id, email)


def test_reject_orphan_override_id_null_is_noop():
    """CR sin override_id (backfill/manual): rechazar no rompe, solo cambia status."""
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    try:
        with SessionLocal() as session:
            cr = CR(
                override_id=None,
                source="manual",
                created_by_user_id=user_id,
                created_by_username=email,
                change_type="ajuste",
                scope_type="subnegocio",
                client_selector="Cliente Z",
                client_name="Cliente Z",
                status="pendiente",
            )
            session.add(cr)
            session.commit()
            cr_id = cr.id

        with SessionLocal() as session:
            cr = session.get(CR, cr_id)
            owner = _apply_review(session, cr, status="rechazado", user=admin, motivo="sin override")
            session.commit()

        assert owner is None
        assert _crs_for_user(email)[0].status == "rechazado"
    finally:
        _cleanup(user_id, email)


def test_reject_subneg_scope_reverts_whole_subneg():
    """Grano subnegocio: tras el rechazo, el subneg vuelve a la base (sin growth)."""
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    try:
        _save_subneg(user_id, email, 50.0)
        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {"Sub A": 50.0}
        cr_id = _crs_for_user(email)[0].id

        with SessionLocal() as session:
            cr = session.get(CR, cr_id)
            _apply_review(session, cr, status="rechazado", user=admin, motivo="revertir subneg")
            session.commit()

        assert svc._get_client_subneg_growths(user_id, "Cliente A") == {}
        assert not svc._has_overrides(user_id, 25.0)
    finally:
        _cleanup(user_id, email)


def test_apply_review_returns_owner_uid_for_cache_contract():
    """Contrato que usan los 3 endpoints para invalidar caché: _apply_review
    devuelve el user_id DUEÑO del override (no el del admin revisor)."""
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    try:
        _save_subneg(user_id, email, 50.0)
        cr_id = _crs_for_user(email)[0].id
        with SessionLocal() as session:
            cr = session.get(CR, cr_id)
            owner = _apply_review(session, cr, status="rechazado", user=admin, motivo="x")
            session.commit()
        assert owner == user_id
        assert owner != admin.id
    finally:
        _cleanup(user_id, email)


def test_reject_endpoint_invalidates_owner_cache(monkeypatch):
    """Endpoint individual: tras el reject se invalida la caché del DUEÑO."""
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    cleared: list[int] = []
    monkeypatch.setattr(svc, "_clear_cache_for_override_save", lambda uid: cleared.append(uid))
    try:
        _save_subneg(user_id, email, 50.0)
        cr_id = _crs_for_user(email)[0].id
        cleared.clear()  # ignorar la invalidación propia del save; medir solo el reject

        resp = api_approvals_reject(
            request_id=cr_id,
            payload=_ReviewPayload(motivo="rechazo de prueba"),
            request=None,
            user=admin,
        )

        assert getattr(resp, "status_code", 200) == 200
        assert cleared == [user_id]  # dueño, no el admin
        assert _active_overrides(user_id) == []
        assert _crs_for_user(email)[0].status == "rechazado"
    finally:
        _cleanup(user_id, email)


def test_review_by_ids_path_reverts_and_invalidates_cache(monkeypatch):
    """Camino by-ids (nodo del árbol): revierte el override y invalida la caché del
    dueño igual que el individual (mismo enganche en _apply_review)."""
    user_id, email = _create_cotizador()
    admin = _admin_reviewer()
    cleared: list[int] = []
    monkeypatch.setattr(svc, "_clear_cache_for_override_save", lambda uid: cleared.append(uid))
    try:
        _save_subneg(user_id, email, 50.0)
        cr_id = _crs_for_user(email)[0].id
        cleared.clear()  # ignorar la invalidación propia del save; medir solo el reject

        payload = _ByIdsReviewPayload(
            ids=[cr_id],
            motivo="rechazo by-ids",
            estado="pendiente",
            comercial=email,  # acota el pool re-derivado a este cotizador
        )
        n = _review_by_ids(payload, status="rechazado", user=admin)

        assert n >= 1
        assert cleared == [user_id]
        assert _active_overrides(user_id) == []
        assert _crs_for_user(email)[0].status == "rechazado"
    finally:
        _cleanup(user_id, email)
