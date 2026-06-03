"""
Importa / reemplaza el dataset de Dimensionamiento (Mercado Privado) en la base activa.

Este script es un *wrapper delgado y versionable* sobre el módulo de ingesta de
producción `web_comparativas.dimensionamiento.ingestion`. NO reimplementa la carga:
reutiliza exactamente la misma lógica que corre en Render para garantizar paridad
local <-> producción (dedup por id_registro_unico, reconstrucción del summary mensual,
refresh del snapshot del dashboard, registro en dimensionamiento_import_runs, etc.).

Tabla(s) afectada(s):
    - dimensionamiento_records              (datos crudos normalizados)
    - dimensionamiento_family_monthly_summary (resumen mensual recalculado)
    - dimensionamiento_dashboard_snapshots  (snapshot del dashboard recalculado)
    - dimensionamiento_import_runs          (1 fila nueva con métricas de la corrida)

CSV por defecto (ruta EXACTA, con espacios alrededor del guion):
    web_comparativas/data/archivos dimensionamiento/dataset_unificado_valorizado_2025_2026 - 2.csv

Uso local (desde la raíz del proyecto, con el venv activado):
    python scripts/import_dimensionamiento_dataset.py
    python scripts/import_dimensionamiento_dataset.py --csv-path "ruta/al/otro.csv"
    python scripts/import_dimensionamiento_dataset.py --mode upsert   # conserva ids ausentes

Modo por defecto: replace (recarga total lógica de Dimensionamiento).
Siempre corre con force=True para ignorar el hash de la corrida previa.

ADVERTENCIAS:
    - Opera sobre la base configurada por la app (local: web_comparativas/app.db).
    - NO subir app.db ni los backups a Git (ya están en .gitignore).
    - El CSV de datos tampoco se versiona (*.csv está en .gitignore).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Permitir ejecución como script suelto (python scripts/...) agregando la raíz al path.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import func, select  # noqa: E402

from sqlalchemy import delete, or_, text  # noqa: E402

from web_comparativas.dimensionamiento.ingestion import (  # noqa: E402
    _rebuild_summary_table,
    ingest_dimensionamiento_csv,
)
from web_comparativas.dimensionamiento.models import (  # noqa: E402
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoRecord,
)
from web_comparativas.models import IS_SQLITE, SessionLocal  # noqa: E402

DEFAULT_DATASET = (
    PROJECT_ROOT
    / "web_comparativas"
    / "data"
    / "archivos dimensionamiento"
    / "dataset_unificado_valorizado_2025_2026 - 2.csv"
)


def _counts() -> tuple[int, int]:
    session = SessionLocal()
    try:
        records = session.execute(
            select(func.count()).select_from(DimensionamientoRecord)
        ).scalar_one()
        summary = session.execute(
            select(func.count()).select_from(DimensionamientoFamilyMonthlySummary)
        ).scalar_one()
        return records, summary
    finally:
        session.close()


def _ensure_replace_persisted(run_id: int) -> dict[str, int] | None:
    """Salvaguarda local (SQLite): en el flujo en-proceso, el borrado de registros
    de corridas previas y la reconstrucción del summary pueden no persistir al hacer
    commit junto con el refresh del snapshot. Si detectamos registros stale o un
    summary vacío para la corrida nueva, lo reparamos en transacciones aisladas.

    En PostgreSQL (producción) la ingesta persiste correctamente, por lo que esta
    función no realiza cambios (no encuentra inconsistencias) y es un no-op seguro.
    """
    if not IS_SQLITE:
        return None

    session = SessionLocal()
    try:
        stale = session.execute(
            text(
                "SELECT COUNT(*) FROM dimensionamiento_records "
                "WHERE import_run_id IS NULL OR import_run_id != :r"
            ),
            {"r": run_id},
        ).scalar_one()
        summary_for_run = session.execute(
            text(
                "SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary "
                "WHERE import_run_id = :r"
            ),
            {"r": run_id},
        ).scalar_one()

        if not stale and summary_for_run:
            return None  # estado ya consistente, nada que reparar

        print(
            f"[REPARACION] Estado inconsistente detectado (stale_records={stale}, "
            f"summary_run={summary_for_run}). Aplicando reparación aislada...",
            flush=True,
        )

        if stale:
            session.execute(
                delete(DimensionamientoRecord).where(
                    or_(
                        DimensionamientoRecord.import_run_id.is_(None),
                        DimensionamientoRecord.import_run_id != run_id,
                    )
                )
            )
            session.commit()

        _rebuild_summary_table(session, run_id)
        session.commit()

        summary_after = session.execute(
            text(
                "SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary "
                "WHERE import_run_id = :r"
            ),
            {"r": run_id},
        ).scalar_one()
        return {"stale_deleted": int(stale), "summary_rebuilt": int(summary_after)}
    finally:
        session.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv-path",
        dest="csv_path",
        default=str(DEFAULT_DATASET),
        help="Ruta al CSV de Dimensionamiento (por defecto: dataset nuevo en data/archivos dimensionamiento).",
    )
    parser.add_argument(
        "--mode",
        dest="mode",
        choices=["replace", "upsert"],
        default="replace",
        help="replace: recarga total lógica; upsert: conserva registros ausentes.",
    )
    parser.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=int,
        default=10000,
        help="Tamaño de chunk de lectura del CSV.",
    )
    parser.add_argument(
        "--no-force",
        dest="force",
        action="store_false",
        help="No forzar: omite la carga si el hash del CSV no cambió respecto a la última corrida.",
    )
    parser.set_defaults(force=True)
    return parser


def main() -> int:
    # La consola de Windows (cp1252) no puede codificar algunos caracteres que
    # imprime la ingesta. Forzar UTF-8 tolerante evita abortar por encoding.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass

    args = _build_parser().parse_args()
    csv_path = Path(args.csv_path).expanduser()

    if not csv_path.exists():
        print(f"[ERROR] No se encontró el CSV en: {csv_path}", flush=True)
        parent = csv_path.parent
        if parent.exists():
            print(f"[ERROR] Archivos disponibles en {parent}:", flush=True)
            for item in sorted(parent.iterdir()):
                print(f"   - {item.name}", flush=True)
        return 2

    records_before, summary_before = _counts()
    print("============================================================", flush=True)
    print("Importación dataset Dimensionamiento", flush=True)
    print(f"  CSV          : {csv_path}", flush=True)
    print(f"  Modo         : {args.mode}  (force={args.force})", flush=True)
    print(f"  Registros antes : {records_before}", flush=True)
    print(f"  Summary antes   : {summary_before}", flush=True)
    print("------------------------------------------------------------", flush=True)

    result = ingest_dimensionamiento_csv(
        csv_path=str(csv_path),
        chunk_size=args.chunk_size,
        mode=args.mode,
        force=args.force,
    )

    run_id = result.get("run_id")
    if result.get("status") == "success" and run_id is not None:
        repair = _ensure_replace_persisted(int(run_id))
        if repair is not None:
            print(
                f"[REPARACION] stale borrados={repair['stale_deleted']} "
                f"summary reconstruido={repair['summary_rebuilt']} (run_id={run_id})",
                flush=True,
            )

    records_after, summary_after = _counts()
    print("------------------------------------------------------------", flush=True)
    print("Resultado de la ingesta:", flush=True)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    print("------------------------------------------------------------", flush=True)
    print(f"  Registros despues : {records_after}  (delta {records_after - records_before:+d})", flush=True)
    print(f"  Summary despues   : {summary_after}  (delta {summary_after - summary_before:+d})", flush=True)
    print("============================================================", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
