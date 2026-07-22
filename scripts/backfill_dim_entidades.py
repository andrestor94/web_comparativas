"""Backfill de la resolución de identidad de clientes de Dimensionamiento.

Puebla, para una o todas las corridas exitosas:
  - dimensionamiento_cliente_entidad (registry de entidades)
  - dimensionamiento_records.cliente_entidad_id
  - dimensionamiento_family_monthly_summary.cliente_entidad_id / es_cliente_entidad

Es idempotente (reemplaza el registry de la corrida y reescribe las columnas). NO borra
ni modifica filas de datos; solo etiqueta. Pensado para correr UNA VEZ sobre datos ya
cargados (los imports nuevos resuelven solos en el finalize de ingestion.py).

Uso:
    python -m scripts.backfill_dim_entidades            # última corrida success
    python -m scripts.backfill_dim_entidades --run 7    # una corrida puntual
    python -m scripts.backfill_dim_entidades --all      # todas las corridas success
    python -m scripts.backfill_dim_entidades --dry-run  # solo reporta stats, no escribe

Requiere que la migración de esquema (ensure_dimensionamiento_entidad_columns) ya haya
corrido — sucede en el startup de main.py.
"""
from __future__ import annotations

import argparse
import sys

from sqlalchemy import select

from web_comparativas.models import SessionLocal
from web_comparativas.dimensionamiento.models import DimensionamientoImportRun
from web_comparativas.dimensionamiento.identity import rebuild_client_entities, resolve_entities


def _target_runs(session, args) -> list[int]:
    if args.run is not None:
        return [args.run]
    q = select(DimensionamientoImportRun.id).where(DimensionamientoImportRun.status == "success")
    if not args.all:
        q = q.order_by(DimensionamientoImportRun.finished_at.desc(), DimensionamientoImportRun.id.desc()).limit(1)
    else:
        q = q.order_by(DimensionamientoImportRun.id.asc())
    return [r for (r,) in session.execute(q).all()]


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill de entidades-cliente de Dimensionamiento")
    ap.add_argument("--run", type=int, default=None, help="import_run_id puntual")
    ap.add_argument("--all", action="store_true", help="todas las corridas success")
    ap.add_argument("--dry-run", action="store_true", help="solo reporta stats, no escribe")
    args = ap.parse_args()

    session = SessionLocal()
    try:
        runs = _target_runs(session, args)
        if not runs:
            print("[BACKFILL] No hay corridas success para procesar.")
            return 1
        for run_id in runs:
            if args.dry_run:
                res = resolve_entities(session, run_id)
                print(f"[BACKFILL][DRY] run={run_id} stats={res.stats} ambiguas={len(res.ambiguous)}")
            else:
                stats = rebuild_client_entities(session, run_id, commit=True)
                print(f"[BACKFILL] run={run_id} OK stats={stats}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
