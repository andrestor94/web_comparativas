from __future__ import annotations

import json
import time

from sqlalchemy import select, func

from web_comparativas.dimensionamiento.models import (
    DimensionamientoDashboardSnapshot,
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoRecord,
)
from web_comparativas.dimensionamiento.query_service import (
    build_filters,
    get_dashboard_bootstrap,
    refresh_default_dashboard_snapshot,
)
from web_comparativas.models import Base, SessionLocal, engine


def main() -> None:
    Base.metadata.create_all(bind=engine)
    session = SessionLocal()
    try:
        records = session.execute(
            select(func.count()).select_from(DimensionamientoRecord)
        ).scalar_one()
        summary_rows = session.execute(
            select(func.count()).select_from(DimensionamientoFamilyMonthlySummary)
        ).scalar_one()
        snapshots = session.execute(
            select(func.count()).select_from(DimensionamientoDashboardSnapshot)
        ).scalar_one()

        started = time.perf_counter()
        refresh_default_dashboard_snapshot(session, commit=True)
        refresh_ms = round((time.perf_counter() - started) * 1000, 2)

        started = time.perf_counter()
        payload = get_dashboard_bootstrap(session, build_filters())
        bootstrap_ms = round((time.perf_counter() - started) * 1000, 2)

        print(
            json.dumps(
                {
                    "backend": engine.url.get_backend_name(),
                    "database": getattr(engine.url, "database", None),
                    "records": records,
                    "summary_rows": summary_rows,
                    "snapshot_rows": snapshots,
                    "snapshot_refresh_ms": refresh_ms,
                    "bootstrap_read_ms": bootstrap_ms,
                    "bootstrap_source": payload.get("meta", {}).get("source"),
                    "bootstrap_stale": payload.get("meta", {}).get("stale"),
                    "clientes": payload.get("kpis", {}).get("clientes"),
                    "renglones": payload.get("kpis", {}).get("renglones"),
                    "familias": payload.get("kpis", {}).get("familias"),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
