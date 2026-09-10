import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from web_comparativas.dimensionamiento import query_service as qs
from web_comparativas.dimensionamiento.models import (
    DimensionamientoClienteEntidad,
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoImportRun,
    DimensionamientoRecord,
)
from web_comparativas.models import Base


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        engine,
        tables=[
            DimensionamientoImportRun.__table__,
            DimensionamientoRecord.__table__,
            DimensionamientoFamilyMonthlySummary.__table__,
            DimensionamientoClienteEntidad.__table__,
        ],
    )
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _seed_ranking(db):
    run = DimensionamientoImportRun(source_path="ranking.csv", mode="replace", status="success")
    db.add(run)
    db.flush()

    clients = [
        (1, "Cliente A", True, "Buenos Aires", "C1", 100, 100.0, "GANADO"),
        (2, "Cliente B", False, "Cordoba", "C2", 10, 1000.0, "NO_PARTICIPO"),
        (3, "Cliente C", True, "Buenos Aires", "C3", 8, 500.0, "NO_COTIZADO"),
    ]
    for entity_id, name, is_client, province, account, rows, amount, result in clients:
        db.add(
            DimensionamientoClienteEntidad(
                import_run_id=run.id,
                entidad_key=entity_id,
                es_cliente=is_client,
                nombre_visible=name,
                provincia=province,
                n_formas=1,
                total_registros=rows,
            )
        )
        db.add(
            DimensionamientoRecord(
                id_registro_unico=f"raw-{entity_id}",
                fecha=dt.date(2026, 1, 1),
                plataforma="P",
                cliente_visible=name,
                cliente_entidad_id=entity_id,
                cuenta_interna=account,
                provincia=province,
                is_client=is_client,
                resultado_participacion=result,
                import_run_id=run.id,
            )
        )
        db.add(
            DimensionamientoFamilyMonthlySummary(
                month=dt.date(2026, 1, 1),
                plataforma="P",
                cliente_visible=name,
                cliente_entidad_id=entity_id,
                es_cliente_entidad=is_client,
                provincia=province,
                is_client=is_client,
                is_identified=True,
                resultado_participacion=result,
                total_cantidad=float(rows),
                total_valorizacion=amount,
                total_registros=rows,
                clientes_unicos=1,
                import_run_id=run.id,
            )
        )
    db.commit()
    qs.invalidate_query_cache()
    return run


def test_client_ranking_aggregates_before_metric_limit(db, monkeypatch):
    run = _seed_ranking(db)
    monkeypatch.setattr(qs, "_resolve_aggregate_model", lambda *args, **kwargs: DimensionamientoFamilyMonthlySummary)

    filters = qs.DimensionamientoFilters(import_run_id=run.id)
    complete = qs.get_clients_by_result(db, filters, limit=None)
    by_rows = qs.get_clients_by_result(db, filters, limit=1, metric="renglones")
    by_amount = qs.get_clients_by_result(db, filters, limit=1, metric="valorizacion")

    assert len(complete) == 3
    assert by_rows[0]["cliente"] == "Cliente A"
    assert by_amount[0]["cliente"] == "Cliente B"
    assert by_amount[0]["is_client"] is False
    assert by_amount[0]["cliente_entidad_id"] == 2
    assert set(by_amount[0]["resultados_val"]) == {"NO_PARTICIPO"}


def test_client_ranking_combines_filters_and_preserves_cartera_scope(db, monkeypatch):
    run = _seed_ranking(db)
    monkeypatch.setattr(qs, "_resolve_aggregate_model", lambda *args, **kwargs: DimensionamientoFamilyMonthlySummary)

    scoped = qs.DimensionamientoFilters(
        import_run_id=run.id,
        provincias=["Buenos Aires"],
        cartera_branches=((frozenset({"C1"}), None),),
    )
    payload = qs.get_clients_by_result(db, scoped, limit=None)
    assert [item["cliente_entidad_id"] for item in payload] == [1]

    non_clients = qs.DimensionamientoFilters(import_run_id=run.id, is_client=False)
    payload = qs.get_clients_by_result(db, non_clients, limit=None)
    assert [item["cliente_entidad_id"] for item in payload] == [2]