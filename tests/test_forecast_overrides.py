from __future__ import annotations

import os
import sys
import uuid
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas import forecast_service as svc
from web_comparativas.migrations import ensure_forecast_override_storage
from web_comparativas.models import ForecastUserOverride, SessionLocal, User
from web_comparativas.routers import forecast_router
from web_comparativas.routers.forecast_router import _can_view_global_forecast_adjustments


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


def _resolved_annual_pct(user_id: int, client: str, subneg: str, global_growth: float) -> float:
    """Tasa anual que el read-path resolveria para client/subneg con los overrides vigentes."""
    maps = svc._build_override_maps(_active_overrides(user_id))
    resolved = svc._resolve_override_for_row(
        selector_candidates=[client],
        subneg=subneg,
        codigo_serie="ART-1",
        forecast_month=svc.get_forecast_effective_month(),
        maps=maps,
        base_growth_pct=global_growth,
    )
    return float(resolved.get("annual_pct"))


def test_subneg_override_persists_when_baseline_is_a_group_wildcard_not_the_global():
    """Guard de efectividad: el baseline es lo que la fila resolveria SIN este override.

    Con un wildcard de grupo en 8%, poner 25% en un subnegocio ES un cambio real aunque
    25 coincida con la tasa global del tablero. Comparar contra la global desactivaba el
    override y la fila caia al wildcard, proyectando 8% en vez del 25% pedido.
    """
    user_id = _create_user()
    global_growth = 25.0
    group_rate = 8.0
    client, subneg = "Cliente Wildcard", "Sub Wildcard"

    try:
        # El grupo deja un override wildcard (subneg="") en 8% para el cliente.
        svc.save_group_expectations(
            user_id=user_id,
            group_name="Grupo Wildcard",
            client_ids=[client],
            growth_pct=group_rate,
            base_growth_pct=global_growth,
            user_email="tester@example.com",
        )
        wildcards = [o for o in _active_overrides(user_id) if not (o.subneg or "")]
        assert len(wildcards) == 1, "setup: se esperaba un wildcard de grupo"
        assert _resolved_annual_pct(user_id, client, subneg, global_growth) == pytest.approx(group_rate)

        # El usuario pone exactamente la tasa global (25) en un subnegocio del cliente.
        svc.save_client_overrides(
            user_id=user_id,
            client_id=client,
            growth_pct=global_growth,
            user_email="tester@example.com",
            subneg_overrides=[{"subneg": subneg, "growth_pct": global_growth}],
        )

        # Debe persistir: el baseline real de la fila era 8 (el wildcard), no 25.
        specific = [o for o in _active_overrides(user_id) if (o.subneg or "") == subneg]
        assert len(specific) == 1, "el override de subnegocio no debe desactivarse"
        assert float(specific[0].override_growth_pct) == pytest.approx(global_growth)
        # Y la fila debe proyectar lo pedido, no la tasa del grupo.
        assert _resolved_annual_pct(user_id, client, subneg, global_growth) == pytest.approx(global_growth)
    finally:
        _delete_user(user_id)


def test_subneg_override_equal_to_real_baseline_is_deactivated():
    """Caso inverso: sin nada por debajo, el baseline real ES la global.

    Poner 25 cuando la fila ya resolveria 25 es redundante -> se desactiva (comportamiento
    correcto que el fix del baseline NO debe romper), y la fila sigue proyectando 25.
    """
    user_id = _create_user()
    global_growth = 25.0
    client, subneg = "Cliente Base", "Sub Base"

    try:
        # Primero un override real distinto de la global.
        svc.save_client_overrides(
            user_id=user_id,
            client_id=client,
            growth_pct=global_growth,
            user_email="tester@example.com",
            subneg_overrides=[{"subneg": subneg, "growth_pct": 8.0}],
        )
        assert len(_active_overrides(user_id)) == 1
        assert _resolved_annual_pct(user_id, client, subneg, global_growth) == pytest.approx(8.0)

        # Ahora el usuario vuelve a la global: sin wildcard debajo, el baseline real es 25.
        svc.save_client_overrides(
            user_id=user_id,
            client_id=client,
            growth_pct=global_growth,
            user_email="tester@example.com",
            subneg_overrides=[{"subneg": subneg, "growth_pct": global_growth}],
        )

        assert _active_overrides(user_id) == [], "un override redundante debe desactivarse"
        # La fila cae a la base y proyecta la global: es exactamente lo que el usuario pidio.
        assert _resolved_annual_pct(user_id, client, subneg, global_growth) == pytest.approx(global_growth)
    finally:
        _delete_user(user_id)


def _annotated_row(user_id: int, client: str, global_growth: float) -> dict:
    """Fila de client-table anotada con la tasa resuelta, como la recibe la grilla."""
    row = {"Cliente": client, "Grupo": ""}
    svc._annotate_rows_with_growth_pct([row], _active_overrides(user_id), global_growth)
    return row


def test_pill_row_inherits_global_rate_when_client_has_no_override():
    """Sin ningun override, la fila igual reporta su tasa resuelta: la global, heredada."""
    user_id = _create_user()
    try:
        row = _annotated_row(user_id, "Cliente Sin Ajuste", 25.0)
        assert row["_growth_pct"] == pytest.approx(25.0)
        assert row["_growth_mixed"] is False
        assert row["_growth_inherited"] is True
    finally:
        _delete_user(user_id)


def test_pill_row_inherits_group_wildcard_rate():
    """Con solo un wildcard de grupo, la fila hereda ESA tasa (no la global)."""
    user_id = _create_user()
    client = "Cliente Hereda Wildcard"
    try:
        svc.save_group_expectations(
            user_id=user_id,
            group_name="Grupo Hereda",
            client_ids=[client],
            growth_pct=8.0,
            base_growth_pct=25.0,
            user_email="tester@example.com",
        )
        assert [o for o in _active_overrides(user_id) if not (o.subneg or "")], "setup: falta el wildcard"

        row = _annotated_row(user_id, client, 25.0)
        assert row["_growth_pct"] == pytest.approx(8.0), "debe heredar el wildcard, no la global"
        assert row["_growth_inherited"] is True, "el wildcard de grupo no es ajuste propio"
        assert row["_growth_mixed"] is False
    finally:
        _delete_user(user_id)


def test_pill_row_marks_own_subneg_override_as_explicit():
    """Un override propio de subnegocio gana sobre el wildcard y se marca explicito."""
    user_id = _create_user()
    client, subneg = "Cliente Propio", "Sub Propio"
    try:
        svc.save_group_expectations(
            user_id=user_id, group_name="Grupo Propio", client_ids=[client],
            growth_pct=8.0, base_growth_pct=25.0, user_email="tester@example.com",
        )
        svc.save_client_overrides(
            user_id=user_id, client_id=client, growth_pct=25.0,
            user_email="tester@example.com",
            subneg_overrides=[{"subneg": subneg, "growth_pct": 40.0}],
        )

        row = _annotated_row(user_id, client, 25.0)
        assert row["_growth_pct"] == pytest.approx(40.0)
        assert row["_growth_inherited"] is False, "un override propio no es heredado"
        assert row["_growth_mixed"] is False
    finally:
        _delete_user(user_id)


def test_pill_shows_inherited_global_after_redundant_override_is_deactivated():
    """Escenario A: setear 25 sobre baseline real 25 desactiva el override (correcto),
    pero la fila NO se queda sin pill: reporta 25 heredada.

    Es el hueco que hacia leer 'no guardo' cuando el guard desactivaba con razon.
    """
    user_id = _create_user()
    client, subneg = "Cliente Redundante", "Sub Redundante"
    global_growth = 25.0
    try:
        svc.save_client_overrides(
            user_id=user_id, client_id=client, growth_pct=global_growth,
            user_email="tester@example.com",
            subneg_overrides=[{"subneg": subneg, "growth_pct": 8.0}],
        )
        row = _annotated_row(user_id, client, global_growth)
        assert row["_growth_pct"] == pytest.approx(8.0) and row["_growth_inherited"] is False

        # El usuario vuelve a la global: el override queda redundante y se desactiva.
        svc.save_client_overrides(
            user_id=user_id, client_id=client, growth_pct=global_growth,
            user_email="tester@example.com",
            subneg_overrides=[{"subneg": subneg, "growth_pct": global_growth}],
        )
        assert _active_overrides(user_id) == [], "el override redundante debe desactivarse"

        row = _annotated_row(user_id, client, global_growth)
        assert row["_growth_pct"] == pytest.approx(global_growth), "la pill debe mostrar 25, no desaparecer"
        assert row["_growth_inherited"] is True
        assert row["_growth_mixed"] is False
    finally:
        _delete_user(user_id)


def test_pill_matches_projection_when_two_users_override_the_same_client():
    """Dos usuarios con overrides sobre el mismo cliente, visto por un admin.

    El fetch con all_users=True trae un registro por usuario para el mismo subnegocio.
    Sin consolidar, la pill leia dos tasas (5 y 50) y caia en "mixto" mientras la
    proyeccion usaba la consolidada (50) — pill y numeros desalineados.

    Se asertan las DOS cosas: que no sale mixta y que su valor es exactamente el que
    proyecta la fila. Va contra _override_records_for_pill (el helper que usa el
    call site real), asi que sacar la consolidacion del codigo rompe este test.
    """
    user_a = _create_user()
    user_b = _create_user()
    client, subnegs = "Cliente Dos Usuarios", ["Sub Uno", "Sub Dos", "Sub Tres"]
    global_growth = 25.0

    try:
        # user_a deja todo en 5%; user_b pisa despues con 50% (gana por updated_at).
        svc.save_client_overrides(
            user_id=user_a, client_id=client, growth_pct=global_growth,
            user_email="a@example.com",
            subneg_overrides=[{"subneg": s, "growth_pct": 5.0} for s in subnegs],
        )
        svc.save_client_overrides(
            user_id=user_b, client_id=client, growth_pct=global_growth,
            user_email="b@example.com",
            subneg_overrides=[{"subneg": s, "growth_pct": 50.0} for s in subnegs],
        )

        # El fetch crudo trae las dos tasas (una por usuario) para los mismos subnegocios.
        crudos = [
            r for r in svc._fetch_override_records(user_b, all_users=True)
            if (r.client_selector or "") == client
        ]
        assert {float(r.override_growth_pct) for r in crudos} == {5.0, 50.0}
        # Ni siquiera sin consolidar debe colarse un blend de 5 y 50: la resolucion pasa
        # por _build_override_maps, que colapsa por clave quedandose con el mas reciente.
        crudo_info = svc._client_growth_pct_map(crudos)[client]
        assert crudo_info["mixed"] is False
        assert crudo_info["value"] == pytest.approx(50.0), "debe ganar el override mas reciente"

        # Camino real: el helper consolida antes de anotar.
        records = svc._override_records_for_pill(user_b, is_admin=True)
        row = {"Cliente": client, "Grupo": ""}
        svc._annotate_rows_with_growth_pct([row], records, global_growth)

        assert row["_growth_mixed"] is False, "la pill no debe salir mixta por duplicados de usuario"

        # ...y su valor debe coincidir con lo que proyecta la fila, subneg por subneg.
        maps = svc._build_override_maps(records)
        for subneg in subnegs:
            resolved = svc._resolve_override_for_row(
                selector_candidates=[client], subneg=subneg, codigo_serie="ART-1",
                forecast_month=svc.get_forecast_effective_month(),
                maps=maps, base_growth_pct=global_growth,
            )
            assert float(resolved["annual_pct"]) == pytest.approx(row["_growth_pct"]), (
                f"pill ({row['_growth_pct']}) != proyeccion ({resolved['annual_pct']}) en {subneg}"
            )
        assert row["_growth_pct"] == pytest.approx(50.0), "debe ganar el override mas reciente"
    finally:
        _delete_user(user_a)
        _delete_user(user_b)


def test_global_growth_is_flat_not_quarter_ramped():
    """La global se aplica PLANA (1 + g/100), igual que en produccion y que los overrides.

    El camino local escalaba por trimestre (_quarters -> x1.25/1.50/1.75/2.00): con la misma
    global, local proyectaba ~27% mas que prod sobre el mismo dato y una fila heredada al 25%
    terminaba creciendo +59% en el anio. Este test fija la convergencia local == prod.
    """
    growth = 25.0
    data = svc.get_data()
    df_val, df_main = data["df_valorizado"], data["df_main"]
    max_hist = df_main[df_main["tipo"] == "hist"]["fecha"].max()
    if pd.isna(max_hist) or df_val.empty:
        pytest.skip("dataset local sin filas historicas/proyectadas")

    # Un cliente sin overrides: su crecimiento sale enteramente de la global.
    user_id = _create_user()
    try:
        with_ovr = {r.client_selector for r in svc._override_records_for_pill(user_id, is_admin=True)}
        totals = (
            df_val[~df_val["fantasia"].isin(with_ovr)]
            .groupby("fantasia")["monto_yhat"].sum().sort_values(ascending=False)
        )
        if totals.empty:
            pytest.skip("no hay clientes sin overrides en el dataset local")
        client = str(totals.index[0])

        table = svc.get_client_table(
            user_id=user_id, view_money=True, growth_pct=growth, is_admin=True
        )
        row = next((r for r in table["rows"] if r["Cliente"].strip() == client.strip()), None)
        assert row is not None, f"{client} no aparece en la tabla"

        base_by_date = (
            df_val[df_val["fantasia"].astype(str).str.strip() == client.strip()]
            .groupby("fecha")["monto_yhat"].sum()
        )
        months = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                  "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
        expected = 1.0 + growth / 100.0

        checked = 0
        for label in table["months"]:
            mon, year = label.split()
            stamp = pd.Timestamp(year=int(year), month=months[mon], day=1)
            if stamp <= max_hist:
                continue
            base = float(base_by_date.get(stamp, 0.0))
            if base <= 0:
                continue
            factor = float(row.get(label, 0.0)) / base
            assert factor == pytest.approx(expected, rel=1e-6), (
                f"{client} {label}: factor {factor:.4f} != {expected:.4f} "
                f"(ramp trimestral reintroducido?)"
            )
            checked += 1

        assert checked >= 4, "muy pocos meses proyectados para validar la planitud"
    finally:
        _delete_user(user_id)


def _pill_blend(user_id: int, client: str, global_growth: float) -> dict:
    """Estado de la pill para un cliente, por el mismo camino que arma la grilla."""
    data = svc.get_data()
    df_val, df_main = data["df_valorizado"], data["df_main"]
    max_hist = df_main[df_main["tipo"] == "hist"]["fecha"].max() if not df_main.empty else None
    records = svc._override_records_for_pill(user_id, is_admin=False)
    weights = svc._subneg_base_weights(
        df_val, client_col="fantasia", value_col="monto_yhat", max_hist_date=max_hist,
        floor_by_client=svc._override_effective_floor_by_client(records),
    )
    return svc._client_growth_pct_map(records, weights, global_growth).get(client)


def _projected_growth(user_id: int, client: str, max_hist) -> float:
    """Crecimiento efectivo real del cliente sobre los meses en que su override rige."""
    patched = svc._get_patched_df_val(user_id=user_id, is_admin=False)
    rows = patched[patched["fantasia"].astype(str).str.strip() == client.strip()]
    rows = rows[(rows["fecha"] > max_hist) & rows["_has_override"].fillna(False)]
    base = (rows["monto_yhat"].astype(float) / rows["_annual_eff"].astype(float)).sum()
    return (rows["monto_yhat"].astype(float).sum() / base - 1.0) * 100.0


def test_mixed_row_reports_dollar_weighted_blend_matching_its_projection():
    """Una fila con tasas dispares reporta el % GENERAL ponderado por base $, no None.

    El blend debe coincidir con lo que proyecta la fila: el factor aplicado es plano
    (1 + r/100), asi que la media de tasas ponderada por base $ ES el crecimiento
    efectivo. _growth_mixed sigue en True, pero solo como flag de color.
    """
    data = svc.get_data()
    df_val, df_main = data["df_valorizado"], data["df_main"]
    if df_val.empty or df_main.empty:
        pytest.skip("dataset local vacio")
    max_hist = df_main[df_main["tipo"] == "hist"]["fecha"].max()
    global_growth = 25.0

    totals = df_val.groupby("fantasia")["monto_yhat"].sum().sort_values(ascending=False)
    client = str(totals.index[0])
    subnegs = sorted(
        df_val[df_val["fantasia"].astype(str).str.strip() == client.strip()]["subneg"]
        .dropna().unique()
    )
    if len(subnegs) < 2:
        pytest.skip(f"{client} no tiene subnegocios suficientes para un blend")

    user_id = _create_user()
    try:
        # --- invariante: tasas uniformes -> esa misma tasa, exacta ---
        svc.save_client_overrides(
            user_id=user_id, client_id=client, growth_pct=global_growth,
            user_email="tester@example.com",
            subneg_overrides=[{"subneg": s, "growth_pct": 50.0} for s in subnegs],
        )
        info = _pill_blend(user_id, client, global_growth)
        assert info is not None, "el cliente deberia estar en el map"
        assert info["value"] == pytest.approx(50.0, abs=1e-9), "uniforme debe dar su tasa exacta"
        assert info["mixed"] is False

        # --- mixto: blend ponderado por $, numerico, y == proyeccion ---
        rates = [10.0, 60.0, 5.0, 80.0, 30.0]
        svc.save_client_overrides(
            user_id=user_id, client_id=client, growth_pct=global_growth,
            user_email="tester@example.com",
            subneg_overrides=[
                {"subneg": s, "growth_pct": rates[i % len(rates)]}
                for i, s in enumerate(subnegs)
            ],
        )
        info = _pill_blend(user_id, client, global_growth)
        assert info is not None
        assert info["mixed"] is True, "tasas dispares deben seguir marcando el blend"
        assert info["value"] is not None, "una fila mixta ya NO puede venir sin tasa"

        projected = _projected_growth(user_id, client, max_hist)
        assert info["value"] == pytest.approx(projected, rel=1e-9), (
            f"blend {info['value']:.6f}% != proyeccion {projected:.6f}%"
        )

        # y no es el promedio simple: la ponderacion por $ tiene que notarse
        simple = sum(rates[i % len(rates)] for i in range(len(subnegs))) / len(subnegs)
        assert abs(info["value"] - simple) > 1.0, (
            "el blend coincide con el promedio simple: la ponderacion por $ no se aplico"
        )
    finally:
        _delete_user(user_id)


def test_forecast_global_viewer_roles_include_admin_and_auditor_only():
    assert _can_view_global_forecast_adjustments(User(role="admin"))
    assert _can_view_global_forecast_adjustments(User(role="auditor"))
    assert _can_view_global_forecast_adjustments(User(role="Auditor"))
    assert _can_view_global_forecast_adjustments(User(role="ROLE_AUDITOR"))
    assert _can_view_global_forecast_adjustments(User(role="Auditor SIEM"))
    assert _can_view_global_forecast_adjustments(User(role="audit"))
    assert _can_view_global_forecast_adjustments(User(role="aud"))
    assert not _can_view_global_forecast_adjustments(User(role="visor"))
    assert not _can_view_global_forecast_adjustments(User(role="analista"))


def test_chart_data_uses_global_override_scope_for_admin_and_auditor(monkeypatch):
    calls = []

    def fake_get_chart_data(**kwargs):
        calls.append(kwargs)
        return {"history": [], "forecast": [], "val_2026": [], "kpis": {}, "has_overrides": kwargs["is_admin"]}

    monkeypatch.setattr(forecast_router.svc, "get_chart_data", fake_get_chart_data)
    monkeypatch.setattr(forecast_router.svc, "get_lab_product_codes", lambda _lab: [])

    admin = User(id=1, email="admin@test.local", role="admin")
    auditor = User(id=2, email="auditor@test.local", role="Auditor SIEM")
    analyst = User(id=3, email="analyst@test.local", role="analista")

    admin_response = forecast_router.api_chart_data(request=None, _user=admin)
    auditor_response = forecast_router.api_chart_data(request=None, _user=auditor)
    analyst_response = forecast_router.api_chart_data(request=None, _user=analyst)

    assert [call["is_admin"] for call in calls] == [True, True, False]
    assert admin_response["has_overrides"] is True
    assert auditor_response["has_overrides"] is True
    assert analyst_response["has_overrides"] is False


def test_chart_data_final_adjusted_series_matches_for_admin_and_auditor(monkeypatch):
    other_user_id = 99
    admin_user_id = 1
    auditor_user_id = 2
    analyst_user_id = 3

    override = SimpleNamespace(
        user_id=other_user_id,
        client_selector="Cliente A",
        override_scope=svc.FORECAST_SCOPE_SUBNEG,
        subneg="Sub A",
        codigo_serie="",
        forecast_month="",
        override_growth_pct=50.0,
        effective_monthly_pct=svc._monthly_pct_from_annual_growth(50.0),
        effective_from_month="2026-01",
        is_active=True,
    )

    def fake_fetch_override_records(user_id, client_selector=None, client_selectors=None, *, all_users=False):
        if all_users:
            return [override]
        return [override] if int(user_id or 0) == other_user_id else []

    data = {
        "df_main": pd.DataFrame(
            [
                {
                    "fecha": pd.Timestamp("2025-12-01"),
                    "tipo": "hist",
                    "perfil": "FAR",
                    "neg": "Neg A",
                    "subneg": "Sub A",
                    "descripcion": "SKU-1",
                    "codigo_serie": "SKU-1",
                    "y": 10.0,
                    "yhat": 10.0,
                    "li": 9.0,
                    "ls": 11.0,
                    "precio": 10.0,
                }
            ]
        ),
        "df_valorizado": pd.DataFrame(
            [
                {
                    "fecha": pd.Timestamp("2026-01-01"),
                    "perfil": "FAR",
                    "neg": "Neg A",
                    "subneg": "Sub A",
                    "descripcion": "SKU-1",
                    "codigo_serie": "SKU-1",
                    "fantasia": "Cliente A",
                    "cliente_id": "C1",
                    "monto_yhat": 100.0,
                    "monto_li": 90.0,
                    "monto_ls": 110.0,
                }
            ]
        ),
        "df_imp_hist": pd.DataFrame(
            [
                {
                    "fecha": pd.Timestamp("2025-12-01"),
                    "perfil": "FAR",
                    "neg": "Neg A",
                    "subneg": "Sub A",
                    "codigo_serie": "SKU-1",
                    "imp_hist": 100.0,
                }
            ]
        ),
        "df_fact_2026": pd.DataFrame(columns=["fecha", "imp_hist"]),
    }

    monkeypatch.setattr(svc, "_fetch_override_records", fake_fetch_override_records)
    monkeypatch.setattr(svc, "get_data", lambda: data)
    monkeypatch.setattr(svc, "_data_cache", {"loaded": True})

    def chart_for(user_id, global_viewer):
        return svc.get_chart_data.__wrapped__(
            user_id=user_id,
            growth_pct=25.0,
            view_money=True,
            is_admin=global_viewer,
        )

    admin = chart_for(admin_user_id, True)
    auditor = chart_for(auditor_user_id, True)
    analyst = chart_for(analyst_user_id, False)

    assert admin["override_debug"]["scope"] == "global"
    assert auditor["override_debug"]["scope"] == "global"
    assert admin["override_debug"]["override_count"] == auditor["override_debug"]["override_count"] == 1
    assert admin["forecast"] == auditor["forecast"]
    assert admin["override_debug"]["has_adjusted_series"]
    assert auditor["override_debug"]["has_adjusted_series"]

    analyst_debug = analyst["override_debug"]
    assert analyst_debug["scope"] == "user"
    assert analyst_debug["override_count"] == 0
    assert not analyst_debug["has_adjusted_series"]
