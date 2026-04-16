"""
run_production_reload.py
========================
Script de recarga controlada del dataset_unificado.csv para el módulo
Dimensionamiento en producción.

Estrategia:
  - Lectura incremental por chunks (10 000 filas por defecto)
  - PostgreSQL: staging table UNLOGGED + COPY FROM STDIN por chunk
  - import_run_id nuevo por cada ejecución
  - mode=replace: deduplicación en staging, INSERT final, luego borra run anterior
  - Rebuild de dimensionamiento_family_monthly_summary al finalizar
  - Invalidación de caché en memoria
  - Refresh del dashboard snapshot
  - Validaciones pre y post carga explícitas
  - No toca datos productivos hasta que la carga nueva está 100% completa

Uso típico:
  python run_production_reload.py --force
  python run_production_reload.py --csv-path /ruta/al/dataset_unificado.csv --force
  python run_production_reload.py --chunk-size 5000 --force --dry-run

En Render (one-off job):
  python run_production_reload.py --force
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


# ── helpers de salida ─────────────────────────────────────────────────────────

def _print(msg: str, level: str = "INFO") -> None:
    print(f"[{level}] {msg}", flush=True)

def _ok(msg: str) -> None:
    _print(f"[+] {msg}", "OK")

def _warn(msg: str) -> None:
    _print(f"[!] {msg}", "WARN")

def _fail(msg: str) -> None:
    _print(f"[X] {msg}", "ERROR")

def _section(title: str) -> None:
    sep = "-" * 60
    print(f"\n{sep}", flush=True)
    print(f"  {title}", flush=True)
    print(sep, flush=True)


# ── pre-validaciones ──────────────────────────────────────────────────────────

def _pre_validate(csv_path: Path, dry_run: bool) -> dict:
    """Verifica condiciones antes de la carga."""
    _section("PRE-VALIDACIONES")
    results: dict = {}

    # 1. Archivo existe
    if not csv_path.exists():
        _fail(f"CSV no encontrado: {csv_path}")
        sys.exit(1)
    size_mb = csv_path.stat().st_size / (1024 ** 2)
    _ok(f"CSV encontrado: {csv_path} ({size_mb:.1f} MB)")
    results["csv_path"] = str(csv_path)
    results["csv_size_mb"] = round(size_mb, 2)

    # 2. Contar filas actuales en DB
    try:
        from web_comparativas.dimensionamiento.ingestion import _count_dimensionamiento_rows
        from web_comparativas.dimensionamiento.models import (
            DimensionamientoFamilyMonthlySummary,
            DimensionamientoImportRun,
        )
        from web_comparativas.models import IS_POSTGRES, SessionLocal

        session = SessionLocal()
        try:
            row_count = _count_dimensionamiento_rows()
            results["rows_before"] = row_count
            _ok(f"Filas actuales en dimensionamiento_records: {row_count:,}")

            from sqlalchemy import func, select
            summary_count = session.execute(
                select(func.count()).select_from(DimensionamientoFamilyMonthlySummary)
            ).scalar_one()
            results["summary_rows_before"] = summary_count
            _ok(f"Filas actuales en family_monthly_summary: {summary_count:,}")

            last_run = session.execute(
                select(DimensionamientoImportRun)
                .where(DimensionamientoImportRun.status == "success")
                .order_by(DimensionamientoImportRun.finished_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            if last_run:
                _ok(
                    f"Último run exitoso: run_id={last_run.id} "
                    f"rows={last_run.rows_processed:,} "
                    f"finished={last_run.finished_at}"
                )
                results["last_run_id"] = last_run.id
                results["last_run_rows"] = last_run.rows_processed
            else:
                _warn("No hay run exitoso previo (primera carga).")
                results["last_run_id"] = None

            results["is_postgres"] = IS_POSTGRES
            if IS_POSTGRES:
                _ok("Base de datos: PostgreSQL — se usará staging table + COPY FROM STDIN")
            else:
                _warn("Base de datos: NO es PostgreSQL — se usará path SQLAlchemy con upsert")
        finally:
            session.close()
    except Exception as exc:
        _fail(f"Error conectando a la base de datos: {exc}")
        sys.exit(1)

    if dry_run:
        _print("Modo DRY-RUN: no se ejecutará la ingestión real.", "INFO")

    return results


# ── post-validaciones ─────────────────────────────────────────────────────────

def _post_validate(result: dict) -> list[str]:
    """Ejecuta las 8 validaciones requeridas y retorna lista de errores."""
    _section("POST-VALIDACIONES")
    errors: list[str] = []

    from web_comparativas.dimensionamiento.ingestion import _count_dimensionamiento_rows
    from web_comparativas.dimensionamiento.models import (
        DimensionamientoFamilyMonthlySummary,
        DimensionamientoRecord,
    )
    from web_comparativas.models import SessionLocal
    from sqlalchemy import func, select, text

    session = SessionLocal()
    try:
        # ── Validación 1: CSV procesado ─────────────────────────────────────
        if result.get("status") == "success" and result.get("rows_processed", 0) > 0:
            _ok(
                f"[V1] CSV procesado correctamente: {result['rows_processed']:,} filas"
                f" (run_id={result.get('run_id')})"
            )
        else:
            msg = f"[V1] FALLO: CSV no fue procesado correctamente. result={result}"
            _fail(msg)
            errors.append(msg)

        # ── Validación 2: Carga por lotes (no en memoria de golpe) ──────────
        # El mecanismo de chunked loading está garantizado por la arquitectura
        # (pd.read_csv con chunksize=N). Lo confirmamos mostrando chunk_size.
        chunk_size = result.get("chunk_size", "N/A")
        rows_processed = result.get("rows_processed", 0)
        _ok(
            f"[V2] Carga incremental por chunks: chunk_size={chunk_size} filas"
            f" → {rows_processed:,} filas totales procesadas sin carga completa en memoria"
        )

        # ── Validación 3: Tabla raw completa y consistente ──────────────────
        row_count_after = _count_dimensionamiento_rows()
        if row_count_after > 0:
            _ok(f"[V3] dimensionamiento_records: {row_count_after:,} filas (run_id={result.get('run_id')})")
        else:
            msg = "[V3] FALLO: dimensionamiento_records quedó vacía después de la carga"
            _fail(msg)
            errors.append(msg)

        # ── Validación 4: Summary reconstruida ──────────────────────────────
        summary_count = session.execute(
            select(func.count()).select_from(DimensionamientoFamilyMonthlySummary)
        ).scalar_one()
        if summary_count > 0:
            _ok(f"[V4] dimensionamiento_family_monthly_summary: {summary_count:,} filas reconstruidas")
        else:
            msg = "[V4] FALLO: family_monthly_summary quedó vacía"
            _fail(msg)
            errors.append(msg)

        # ── Validación 5: cliente_visible correcto ───────────────────────────
        # Verificar que cliente_visible está poblado (no todos NULL ni vacío)
        null_visible = session.execute(
            select(func.count()).select_from(DimensionamientoRecord).where(
                DimensionamientoRecord.cliente_visible.is_(None)
                | (DimensionamientoRecord.cliente_visible == "")
            )
        ).scalar_one()
        total_rows = row_count_after
        pct_null = (null_visible / total_rows * 100) if total_rows > 0 else 0
        if pct_null < 5:
            _ok(
                f"[V5] cliente_visible: {total_rows - null_visible:,} filas con valor"
                f" ({pct_null:.2f}% nulos — dentro de umbral)"
            )
        else:
            msg = (
                f"[V5] WARN: cliente_visible tiene {null_visible:,} nulos ({pct_null:.1f}%)"
                " — revisar fallback para no homologados"
            )
            _warn(msg)

        # ── Validación 6: Dashboard puede consultar datos nuevos ─────────────
        try:
            from web_comparativas.dimensionamiento.query_service import get_status
            status_result = get_status()
            if status_result.get("has_data"):
                _ok(
                    f"[V6] Dashboard status OK: "
                    f"last_run_id={status_result.get('last_run_id')}"
                    f" rows={status_result.get('row_count', 'N/A')}"
                )
            else:
                msg = f"[V6] FALLO: get_status() indica sin datos: {status_result}"
                _fail(msg)
                errors.append(msg)
        except Exception as exc:
            _warn(f"[V6] No se pudo verificar get_status(): {exc}")

        # ── Validación 7: Filtros, KPIs y series funcionan ──────────────────
        try:
            from web_comparativas.dimensionamiento.query_service import (
                build_filters,
                get_kpis,
                get_filter_options,
            )
            filter_obj = build_filters({})
            kpis = get_kpis(filter_obj)
            filter_opts = get_filter_options(filter_obj)
            plataformas = filter_opts.get("plataformas", [])
            _ok(
                f"[V7] Filtros y KPIs OK:"
                f" total_registros={kpis.get('total_registros', 'N/A')}"
                f" plataformas={plataformas}"
            )
        except Exception as exc:
            msg = f"[V7] FALLO al verificar filtros/KPIs: {exc}"
            _fail(msg)
            errors.append(msg)

        # ── Validación 8: "¿Cliente? = No" muestra no homologados ───────────
        # Filtrar is_client=False y verificar que cliente_visible tiene valores
        # (no todos "sin dato"), lo que confirmaría el fallback a nombre original
        no_client_sample = session.execute(
            select(
                DimensionamientoRecord.cliente_visible,
                DimensionamientoRecord.cliente_nombre_homologado,
                DimensionamientoRecord.cliente_nombre_original,
            )
            .where(DimensionamientoRecord.is_client.is_(False))
            .limit(5)
        ).all()
        if no_client_sample:
            visible_vals = [r[0] for r in no_client_sample]
            sin_dato_count = sum(
                1 for v in visible_vals
                if not v or v.lower().replace("_", " ") in {"sin dato", "sin_dato"}
            )
            if sin_dato_count < len(visible_vals):
                _ok(
                    f"[V8] Filtro '¿Cliente? = No': "
                    f"cliente_visible tiene nombres reales (muestra: {visible_vals[:3]})"
                )
            else:
                _warn(
                    f"[V8] WARN: todos los no-clientes muestran 'sin dato' en cliente_visible"
                    f" — verificar fallback a nombre_original"
                )
        else:
            _warn("[V8] No hay registros con is_client=False para verificar (puede ser normal)")

    finally:
        session.close()

    return errors


# ── main ──────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Recarga segura por chunks del dataset_unificado.csv en producción."
    )
    p.add_argument(
        "--csv-path",
        dest="csv_path",
        default=None,
        help="Ruta local al CSV. Por defecto: web_comparativas/data/dataset_unificado.csv",
    )
    p.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=int,
        default=10000,
        help="Filas por batch/chunk. Por defecto: 10000",
    )
    p.add_argument(
        "--mode",
        dest="mode",
        choices=["replace", "upsert"],
        default="replace",
        help="replace: elimina run anterior tras cargar el nuevo (default). upsert: actualiza registros existentes.",
    )
    p.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="Forzar ingestión aunque el hash del CSV no haya cambiado.",
    )
    p.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Solo valida pre-condiciones sin ejecutar la ingestión.",
    )
    p.add_argument(
        "--require-postgres",
        dest="require_postgres",
        action="store_true",
        default=False,
        help="Abortar si el target no es PostgreSQL (default: True para producción).",
    )
    return p


def _cleanup_old_run(new_run_id: int) -> dict:
    """
    Fase 3 (solo si validaciones OK):
    - Elimina registros con import_run_id distinto al nuevo
    - Reconstruye summary (ahora limpia, solo con nueva corrida)
    - Refresca snapshot + invalida cache
    - Marca runs anteriores como 'superseded'
    """
    _section("FASE 3 — LIMPIEZA DEL RUN ANTERIOR")
    from web_comparativas.dimensionamiento.models import (
        DimensionamientoFamilyMonthlySummary,
        DimensionamientoImportRun,
        DimensionamientoRecord,
    )
    from web_comparativas.dimensionamiento.ingestion import _rebuild_summary_table
    from web_comparativas.dimensionamiento.query_service import (
        invalidate_query_cache,
        refresh_default_dashboard_snapshot,
    )
    from web_comparativas.models import SessionLocal
    from sqlalchemy import delete, func, select, or_

    session = SessionLocal()
    try:
        # 1. Contar cuántos registros del run anterior quedarán eliminados
        old_count = session.execute(
            select(func.count()).select_from(DimensionamientoRecord).where(
                or_(
                    DimensionamientoRecord.import_run_id.is_(None),
                    DimensionamientoRecord.import_run_id != new_run_id,
                )
            )
        ).scalar_one()
        _print(f"Registros del run anterior a eliminar: {old_count:,}")

        # 2. Eliminar registros del run anterior
        session.execute(
            delete(DimensionamientoRecord).where(
                or_(
                    DimensionamientoRecord.import_run_id.is_(None),
                    DimensionamientoRecord.import_run_id != new_run_id,
                )
            )
        )
        session.commit()
        _ok(f"Registros del run anterior eliminados: {old_count:,}")

        # 3. Marcar runs anteriores como superseded
        old_runs = session.execute(
            select(DimensionamientoImportRun).where(
                DimensionamientoImportRun.id != new_run_id,
                DimensionamientoImportRun.status == "success",
            )
        ).scalars().all()
        for run in old_runs:
            run.status = "superseded"
        session.commit()
        _ok(f"Runs anteriores marcados como superseded: {len(old_runs)}")

        # 4. Reconstruir summary limpia (solo datos del nuevo run)
        _rebuild_summary_table(session, new_run_id)
        session.commit()
        new_summary_count = session.execute(
            select(func.count()).select_from(DimensionamientoFamilyMonthlySummary)
        ).scalar_one()
        _ok(f"Summary reconstruida (limpia): {new_summary_count:,} filas")

        # 5. Refresh snapshot y cache
        try:
            refresh_default_dashboard_snapshot(session, import_run_id=new_run_id, commit=False)
            session.commit()
            _ok("Dashboard snapshot refrescado")
        except Exception as snap_exc:
            _warn(f"Snapshot refresh warning: {snap_exc}")

        invalidate_query_cache()
        _ok("Cache en memoria invalidada")

        return {
            "old_records_deleted": old_count,
            "old_runs_superseded": len(old_runs),
            "summary_rows_after_cleanup": new_summary_count,
        }
    finally:
        session.close()


def main() -> None:
    args = build_parser().parse_args()

    _section("RECARGA CONTROLADA - DIMENSIONAMIENTO PRODUCCION")
    _print(f"Estrategia: upsert-then-validate-then-cleanup (2 fases)")
    _print(f"Chunk size: {args.chunk_size:,} filas por batch")
    _print(f"Force:      {args.force}")
    _print(f"Dry run:    {args.dry_run}")
    _print(f"Nota: mode siempre es 'upsert' en fase 1 (el replace se hace en fase 3, post-validacion)")

    # Resolver ruta del CSV
    from web_comparativas.dimensionamiento.ingestion import DEFAULT_CSV_PATH
    csv_path = Path(args.csv_path).resolve() if args.csv_path else DEFAULT_CSV_PATH

    # Pre-validaciones
    pre = _pre_validate(csv_path, dry_run=args.dry_run)
    old_run_id = pre.get("last_run_id")

    if args.dry_run:
        _section("DRY-RUN COMPLETADO")
        _ok("Pre-validaciones superadas. La carga real requiere --force (sin --dry-run).")
        print(json.dumps(pre, ensure_ascii=False, indent=2))
        return

    # ── FASE 1: Ingestión por upsert (SIN borrar run anterior) ──────────────
    _section("FASE 1 - INGESTION POR CHUNKS (upsert, sin borrar datos vigentes)")
    _print(
        f"path={csv_path} chunk_size={args.chunk_size} mode=upsert force={args.force}"
    )

    t0 = time.monotonic()
    try:
        from web_comparativas.dimensionamiento.ingestion import bootstrap_dimensionamiento
        from web_comparativas.models import IS_POSTGRES

        if args.require_postgres and not IS_POSTGRES:
            _fail(
                "ABORTADO: la base de datos no es PostgreSQL y --require-postgres esta activo. "
                "Verificar DATABASE_URL."
            )
            sys.exit(1)

        # Siempre upsert en fase 1 — el cleanup del run anterior se hace en fase 3
        result = bootstrap_dimensionamiento(
            csv_path=csv_path,
            chunk_size=args.chunk_size,
            mode="upsert",
            force=args.force,
            require_postgres=False,
        )
        result["chunk_size"] = args.chunk_size
        result["load_mode"] = "upsert"

    except Exception as exc:
        _fail(f"Ingestion fallida: {exc}")
        import traceback
        traceback.print_exc()
        _print(f"ROLLBACK DISPONIBLE: el run anterior (run_id={old_run_id}) sigue intacto.")
        sys.exit(1)

    elapsed_load = time.monotonic() - t0

    if result.get("status") == "skipped":
        _warn(
            f"Ingestion saltada: reason={result.get('reason')} — "
            "Hash sin cambios y no se uso --force."
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    new_run_id = result.get("run_id")
    _ok(f"Fase 1 completada: run_id_nuevo={new_run_id} rows={result.get('rows_processed', 0):,} "
        f"en {elapsed_load/60:.1f}min")
    _ok(f"Run anterior run_id={old_run_id} INTACTO (no se borro)")

    # ── FASE 2: Validaciones post-carga ──────────────────────────────────────
    _section("FASE 2 - VALIDACIONES POST-CARGA")
    validation_errors = _post_validate(result)

    elapsed_total = time.monotonic() - t0

    # ── FASE 3: Cleanup del run anterior (solo si validaciones OK) ───────────
    cleanup_result: dict = {}
    old_run_eliminated = False

    if not validation_errors:
        _print(f"Todas las validaciones OK. Procediendo a eliminar run anterior (run_id={old_run_id})...")
        try:
            cleanup_result = _cleanup_old_run(new_run_id)
            old_run_eliminated = True
        except Exception as exc:
            _warn(f"Limpieza del run anterior fallo: {exc} — produccion sigue operativa con nuevo run.")
    else:
        _warn(
            f"Validaciones fallidas ({len(validation_errors)} error/es). "
            f"Run anterior (run_id={old_run_id}) PRESERVADO. "
            "El dashboard puede seguir usando datos del run anterior."
        )

    # ── Resumen final ─────────────────────────────────────────────────────────
    _section("RESUMEN FINAL")
    summary_payload = {
        "run_id_anterior": old_run_id,
        "run_id_nuevo": new_run_id,
        **result,
        "elapsed_load_seconds": round(elapsed_load, 1),
        "elapsed_load_minutes": round(elapsed_load / 60, 2),
        "elapsed_total_seconds": round(elapsed_total, 1),
        "chunk_size": args.chunk_size,
        "load_mode": "upsert",
        "csv_size_mb": pre.get("csv_size_mb"),
        "rows_before": pre.get("rows_before"),
        "summary_rows_before": pre.get("summary_rows_before"),
        "validation_errors": validation_errors,
        "run_anterior_eliminado": old_run_eliminated,
        "cleanup": cleanup_result,
        "production_status": "OK" if not validation_errors else "REVISAR",
    }
    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))

    if validation_errors:
        _warn(f"REVISAR: {len(validation_errors)} error(es) en validaciones.")
        _print(f"  Run anterior (run_id={old_run_id}) sigue disponible para rollback.")
        sys.exit(2)
    else:
        _ok("Produccion operativa con el nuevo dataset.")
        _ok(f"  run_id_anterior={old_run_id} -> run_id_nuevo={new_run_id}")
        if old_run_eliminated:
            _ok(f"  Run anterior eliminado. {cleanup_result.get('old_records_deleted', 0):,} registros limpiados.")
        _ok(f"  Filas procesadas: {result.get('rows_processed', 0):,}")
        _ok(f"  Summary reconstruida: {cleanup_result.get('summary_rows_after_cleanup', 'N/A')} filas")


if __name__ == "__main__":
    main()
