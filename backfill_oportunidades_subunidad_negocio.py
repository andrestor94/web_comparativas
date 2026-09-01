"""
backfill_oportunidades_subunidad_negocio.py
===========================================
Puebla `oportunidades_summary.subunidad_negocio` en las filas de UN run puntual
(default: el run activo — 68 en prod) SIN tocar nada más: ni el dataset, ni las
otras columnas del summary, ni recalcular oportunidades.

El valor sale de los `dimensionamiento_records` del PROPIO run (la etiqueta cruda
`subunidad_negocio` del artículo). `codigo_articulo -> subunidad_negocio` es
determinístico en el dataset; el script lo re-verifica contra la base y SE NIEGA a
aplicar si algún `codigo_articulo` resuelve a más de un subnegocio (mejor pedir
que arreglen el dato que escribir uno equivocado).

Por qué un script y no el push normal: `scripts/push_oportunidades_data.py` hace
DELETE + INSERT del run completo leyendo de local — reemplazaría las 143 filas
reales de prod con datos locales recalculados. Este script solo hace UPDATE de una
columna, matcheando por `codigo_articulo`, sobre las filas que ya están.

Después de correrlo NO hace falta rebuild: el modal y el payload del CRM leen
`subunidad_negocio` en vivo de `oportunidades_summary`.

Dos modos, mismo script:
  - SOLO LECTURA (default): muestra cuántas filas del run tienen la columna en
    NULL, el valor que se resolvería por código, y cuántas no se pueden resolver.
    No escribe nada.
  - ESCRITURA (--apply): hace lo mismo Y ADEMÁS ejecuta los UPDATE, en una
    transacción propia. Por default solo toca filas con `subunidad_negocio IS NULL`;
    con --force también pisa las que ya tengan un valor.

Uso (PowerShell):
    $env:DATABASE_URL = "postgresql://usuario:pass@host-de-render/db"
    $env:APP_ENV = "production"
    python backfill_oportunidades_subunidad_negocio.py                  # solo lectura
    python backfill_oportunidades_subunidad_negocio.py --run-id 68      # explícito
    python backfill_oportunidades_subunidad_negocio.py --run-id 68 --apply
    python backfill_oportunidades_subunidad_negocio.py --run-id 68 --apply --force
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

if "render.com" in os.environ.get("DATABASE_URL", "").lower() and not (
    os.environ.get("APP_ENV", "").strip().lower() == "production" or os.environ.get("RENDER") == "true"
):
    os.environ["APP_ENV"] = "production"  # ver models.py: bloquea local->Render sin esto

from sqlalchemy import func, select, text  # noqa: E402

from web_comparativas.models import SessionLocal, engine  # noqa: E402
from web_comparativas.dimensionamiento.models import (  # noqa: E402
    DimensionamientoImportRun,
    DimensionamientoRecord,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.query_service import _latest_success_import_run  # noqa: E402


def _resolver_run_o_avisar_columna_faltante(db, run_id: int | None):
    """Resuelve el run; mensaje claro (en vez del traceback de SQLAlchemy) si la
    columna `subunidad_negocio` todavía no existe en esta base — pasa si se corre el
    script ANTES de que el deploy con la migración termine de bootear. No hace falta
    ningún paso manual: `_ensure_crm_envio_real_columns` la agrega sola en el arranque
    de la app (ver main.py); solo hay que esperar a que el deploy esté health-checkeado.
    """
    try:
        run = db.get(DimensionamientoImportRun, run_id) if run_id else _latest_success_import_run(db)
        # fuerza a tocar la columna nueva para detectar el "does not exist" acá y no más adelante
        db.execute(select(OportunidadSummary.subunidad_negocio).limit(1)).all()
        return run
    except Exception as exc:
        msg = str(exc).lower()
        if "subunidad_negocio" in msg and ("does not exist" in msg or "no such column" in msg):
            db.rollback()
            print("\n[ERROR] La columna 'oportunidades_summary.subunidad_negocio' todavía no existe en "
                  "esta base. Esto pasa si corriste el script ANTES de que el deploy con la migración "
                  "terminara de bootear — no es un error del script, y no hace falta ningún paso manual: "
                  "la migración corre sola al arrancar la app. Esperá a que el deploy esté "
                  "health-checkeado y reintentá.")
            sys.exit(1)
        raise


def main(run_id: int | None, apply: bool, force: bool) -> None:
    if not engine:
        print("[ERROR] DB engine no disponible (revisá DATABASE_URL / APP_ENV).")
        sys.exit(1)
    print(f"[INFO] DB destino: {engine.url.render_as_string(hide_password=True)}")
    print(f"[INFO] modo: {'ESCRITURA (--apply)' if apply else 'SOLO LECTURA (default — pasá --apply para guardar)'}"
          f"{'  + --force (pisa valores ya seteados)' if force else ''}")

    db = SessionLocal()
    try:
        run = _resolver_run_o_avisar_columna_faltante(db, run_id)
        if run is None:
            print("[ERROR] No se encontró el run (¿id correcto? ¿hay alguna corrida 'success'?).")
            sys.exit(1)
        print(f"[INFO] run_id = {run.id}  status = {run.status!r}  finished_at = {run.finished_at}")

        # ── 1) Mapa codigo_articulo -> subnegocio(s) desde dimensionamiento_records del run ──
        R = DimensionamientoRecord
        pares = db.execute(
            select(R.codigo_articulo, R.subunidad_negocio)
            .where(R.import_run_id == run.id)
            .where(R.codigo_articulo.is_not(None))
            .where(R.codigo_articulo != "")
            .where(R.subunidad_negocio.is_not(None))
            .where(R.subunidad_negocio != "")
            .distinct()
        ).all()
        by_codigo: dict[str, set[str]] = defaultdict(set)
        for codigo, sub in pares:
            by_codigo[codigo].add(sub)

        ambiguos = {c: sorted(s) for c, s in by_codigo.items() if len(s) > 1}
        resoluble = {c: next(iter(s)) for c, s in by_codigo.items() if len(s) == 1}
        print(f"[INFO] dimensionamiento_records run {run.id}: {len(by_codigo)} códigos con subnegocio; "
              f"{len(resoluble)} resuelven a uno solo; {len(ambiguos)} ambiguos.")

        # ── 2) Estado actual de oportunidades_summary para el run ──
        S = OportunidadSummary
        total = db.execute(
            select(func.count()).select_from(S).where(S.import_run_id == run.id)
        ).scalar_one()
        nulos_rows = db.execute(
            select(S.codigo_articulo).where(S.import_run_id == run.id).where(S.subunidad_negocio.is_(None))
        ).scalars().all()
        con_valor = total - len(nulos_rows)
        nulos_resolubles = sum(1 for c in nulos_rows if c in resoluble)
        nulos_sin_datos = sum(1 for c in nulos_rows if c not in resoluble and c not in ambiguos)
        nulos_ambiguos = sum(1 for c in nulos_rows if c in ambiguos)
        objetivo = nulos_rows if not force else db.execute(
            select(S.codigo_articulo).where(S.import_run_id == run.id)
        ).scalars().all()

        print()
        print("=" * 72)
        print(f"oportunidades_summary — run {run.id}")
        print("=" * 72)
        print(f"  filas totales:                         {total}")
        print(f"  con subunidad_negocio ya cargada:      {con_valor}")
        print(f"  con subunidad_negocio en NULL:         {len(nulos_rows)}")
        print(f"    - resolubles (código con 1 subneg.): {nulos_resolubles}")
        print(f"    - sin subnegocio en el dataset:      {nulos_sin_datos}")
        print(f"    - código ambiguo (>1 subneg.):       {nulos_ambiguos}")
        if force:
            print(f"  --force: se evaluarán las {len(objetivo)} filas del run, no solo las NULL")

        if ambiguos:
            print("\n[ATENCIÓN] Códigos con más de un subnegocio en el dataset (NO se tocan sus filas):")
            for c, subs in sorted(ambiguos.items()):
                print(f"    {c}: {subs}")

        # ── 3) Aplicar ──
        if not apply:
            afectaria = sum(1 for c in objetivo if c in resoluble)
            print(f"\n[INFO] SOLO LECTURA: no se escribió nada. Con --apply se actualizarían {afectaria} fila(s).")
            return

        if ambiguos:
            print("\n[ERROR] Hay códigos ambiguos en el run: el script NO aplica en ese caso. "
                  "Corregí el dato en dimensionamiento_records (o el dataset fuente) y reintentá.")
            sys.exit(2)

        cond_null = "" if force else " AND subunidad_negocio IS NULL"
        stmt = text(
            f"UPDATE oportunidades_summary "
            f"SET subunidad_negocio = :sub "
            f"WHERE import_run_id = :run AND codigo_articulo = :cod{cond_null}"
        )
        total_upd = 0
        for codigo, sub in sorted(resoluble.items()):
            res = db.execute(stmt, {"sub": sub, "run": run.id, "cod": codigo})
            total_upd += res.rowcount or 0
        db.commit()
        print(f"\n[OK] {total_upd} fila(s) actualizadas en oportunidades_summary (run {run.id}). "
              f"No se tocó ninguna otra columna ni dimensionamiento_records.")

        restante = db.execute(
            select(func.count()).select_from(S)
            .where(S.import_run_id == run.id).where(S.subunidad_negocio.is_(None))
        ).scalar_one()
        print(f"[INFO] Quedan {restante} fila(s) del run con subunidad_negocio en NULL "
              f"(código sin subnegocio en el dataset — no hay de dónde sacarlo).")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", type=int, default=None, help="import_run_id puntual (default: última corrida success)")
    parser.add_argument("--apply", action="store_true", help="Ejecuta los UPDATE (default: solo los muestra).")
    parser.add_argument("--force", action="store_true", help="También pisa filas que ya tengan un subnegocio cargado.")
    args = parser.parse_args()
    main(run_id=args.run_id, apply=args.apply, force=args.force)
