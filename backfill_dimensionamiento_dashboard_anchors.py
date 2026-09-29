"""
backfill_dimensionamiento_dashboard_anchors.py
===================================================
Calcula las 3 anclas del dashboard de Dimensionamiento — `platform_values`,
`cuenta_entidad_map` y `global_totals` — de UN run puntual (default: el run
activo) y las persiste en `dimensionamiento_import_runs`, para que
`query_service.py` deje de recalcularlas en cada carga del dashboard /
cache-miss:

    - platform_values    : antes `_default_platform_values` — medido ~8.2s
                            escaneando ~152k filas para 3 valores distintos.
    - cuenta_entidad_map  : antes `_cuenta_to_entidad_map` — medido ~17-19s
                            escaneando ~365k filas para 371 pares distintos.
    - global_totals       : antes 2 llamadas separadas —
                              get_status (39s: 2 Parallel Seq Scan sobre 1.4M
                              filas para total_rows + desglose por plataforma)
                              y get_kpis (19s: COUNT(DISTINCT familia) +
                              COUNT(DISTINCT provincia) con Sort external merge
                              Disk 20MB) — medidas contra prod para un usuario
                              SIN restricción de cartera (auditor/admin). Solo
                              tiene sentido/validez para ESE caso: un usuario
                              restringido sigue viendo esas dos queries en
                              vivo, acotadas por el índice de cliente_entidad_id.

Mismo patrón que `backfill_oportunidades_ref_month.py`: llama a las MISMAS
funciones que usa `refresh_default_dashboard_snapshot` en producción — no
reimplementa el cálculo. Por qué hace falta un script aparte, si el cálculo
ya quedó conectado a `refresh_default_dashboard_snapshot`: esa función corre
sola en cada import NUEVO — el run 68 (u otro ya existente) no va a tener estos
campos poblados hasta el PRÓXIMO import, y nadie va a ver la mejora sin correr
esto a mano una vez.

Detalle de implementación para `global_totals`: `get_status`/`get_kpis` AHORA
chequean el propio `run.global_totals` primero (para no recalcular en cada
carga) — así que, para poder RECALCULARLO acá (en particular con --force),
este script pone `run.global_totals = None` en memoria (sin commitear) antes
de llamarlas, para que tomen el camino en vivo. Si algo falla a mitad de
camino, `db.rollback()` deshace también ese `None` transitorio, así que no hay
riesgo de dejar el campo en blanco por error.

Correcciones sobre la 1ra corrida real (falló con "canceling statement due to
statement timeout" en cuenta_entidad_map, y no guardó NINGUNA de las anclas
aunque platform_values sí se había calculado bien):

  1. `SET statement_timeout = 0` en la sesión (plain SET, no LOCAL). OJO: un
     plain SET emitido DENTRO de una transacción que después hace ROLLBACK
     se deshace junto con la transacción (documentado así en Postgres) — por
     eso este script lo vuelve a pedir ANTES de cada ancla, no solo una vez al
     principio, para que un rollback de una ancla fallida no deje a las
     siguientes sin la protección.
  2. Cada ancla se calcula, se guarda (si --apply) y se comitea POR SEPARADO.
     Si una falla, se hace `db.rollback()` para limpiar la transacción
     envenenada (en Postgres, un statement cancelado aborta TODA la
     transacción) y se sigue con la otra. Así un fallo en una nunca le impide
     guardarse a las demás.

Dos modos, mismo script:
  - SOLO LECTURA (default): calcula e imprime los valores que se guardarían. No
    escribe nada en la base.
  - ESCRITURA (--apply): hace lo mismo Y ADEMÁS persiste cada ancla que se
    haya podido calcular (aunque otra haya fallado).

Si el run YA tiene un campo poblado (no NULL), ESE campo no se recalcula salvo
--force — evita recalcular y sobrescribir por error algo que ya quedó bien
(el chequeo es POR CAMPO, no "las tres juntas": si a alguna le falta, recalcula
solo esa).
NO toca `dimensionamiento_records`, `dimensionamiento_family_monthly_summary`
ni el snapshot del dashboard — solo estas 3 columnas de
`dimensionamiento_import_runs`.

Uso (PowerShell):
    $env:DATABASE_URL = "postgresql://usuario:pass@host-de-render/db"
    $env:APP_ENV = "production"
    python backfill_dimensionamiento_dashboard_anchors.py                # solo lectura, run activo
    python backfill_dimensionamiento_dashboard_anchors.py --run-id 68    # explícito
    python backfill_dimensionamiento_dashboard_anchors.py --run-id 68 --apply         # escribe de verdad
    python backfill_dimensionamiento_dashboard_anchors.py --run-id 68 --apply --force # pisa valores ya seteados
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

if "render.com" in os.environ.get("DATABASE_URL", "").lower() and not (
    os.environ.get("APP_ENV", "").strip().lower() == "production" or os.environ.get("RENDER") == "true"
):
    os.environ["APP_ENV"] = "production"  # ver models.py: bloquea local->Render sin esto

from sqlalchemy import text  # noqa: E402

from web_comparativas.models import SessionLocal, engine, IS_POSTGRES  # noqa: E402
from web_comparativas.dimensionamiento.models import DimensionamientoImportRun  # noqa: E402
from web_comparativas.dimensionamiento.query_service import (  # noqa: E402
    _cuenta_to_entidad_map_from_records,
    _default_platform_values,
    _latest_success_import_run,
    build_filters,
    get_kpis,
    get_status,
)


def _resolver_run_o_avisar_columna_faltante(db, run_id: int | None):
    """Igual que en backfill_oportunidades_ref_month.py: mensaje claro (en vez del
    traceback crudo) si las columnas nuevas todavía no existen en esta base — pasa
    si esto se corre contra prod ANTES de que el deploy con la migración termine de
    bootear. No hace falta ningún paso manual:
    `ensure_dimensionamiento_import_runs_dashboard_precalc_columns` corre sola en el
    arranque de la app (ver main.py) — solo hay que esperar a que el deploy esté
    health-checkeado."""
    try:
        return db.get(DimensionamientoImportRun, run_id) if run_id else _latest_success_import_run(db)
    except Exception as exc:
        msg = str(exc).lower()
        if ("platform_values" in msg or "cuenta_entidad_map" in msg or "global_totals" in msg) and (
            "does not exist" in msg or "no such column" in msg
        ):
            db.rollback()
            print("\n[ERROR] Las columnas nuevas de 'dimensionamiento_import_runs' todavía no existen en "
                  "esta base. Esto pasa si corriste el script ANTES de que el deploy con la migración "
                  "terminara de bootear — no es un error de este script, y no hace falta ningún paso "
                  "manual: la migración corre sola al arrancar la app. Esperá a que el deploy esté "
                  "health-checkeado y reintentá.")
            sys.exit(1)
        raise


def _resetear_statement_timeout(db) -> None:
    """Vuelve a pedir statement_timeout=0. Necesario ANTES DE CADA ancla: un
    plain SET emitido dentro de una transacción que después hace ROLLBACK se
    deshace junto con la transacción (comportamiento documentado de Postgres)
    — si no se reafirma acá, el rollback de una ancla fallida dejaría a la
    siguiente sin la protección."""
    if IS_POSTGRES:
        db.execute(text("SET statement_timeout = 0"))


def _calcular_ancla(db, nombre: str, ya_tiene_valor: bool, force: bool, fn):
    """Corre `fn()`, con manejo de errores propio por ancla: si explota, hace
    rollback (limpia la transacción envenenada — un statement cancelado por
    timeout aborta TODA la transacción en Postgres) y devuelve None, para que
    las OTRAS anclas se puedan seguir calculando/guardando igual."""
    if ya_tiene_valor and not force:
        print(f"  {nombre}: ya tiene un valor guardado y no pasaste --force — no se recalcula.")
        return None, 0.0, "ya_poblado"
    _resetear_statement_timeout(db)
    t0 = time.perf_counter()
    try:
        valor = fn()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"  {nombre}: calculado OK ({elapsed_ms:.1f}ms)")
        return valor, elapsed_ms, "ok"
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        db.rollback()
        print(f"  [ERROR] {nombre}: falló a los {elapsed_ms:.1f}ms — {exc}")
        return None, elapsed_ms, "error"


def _calcular_global_totals(db, run: DimensionamientoImportRun) -> dict:
    """get_status/get_kpis chequean run.global_totals PRIMERO (para no
    recalcular en cada carga real) — acá lo ponemos en None EN MEMORIA (sin
    commitear) antes de llamarlas, para forzar el camino en vivo incluso si ya
    había un valor viejo (--force). Si algo falla, el rollback de
    _calcular_ancla deshace también este None transitorio."""
    run.global_totals = None
    status = get_status(db, run.id)
    filtros = build_filters()
    filtros.import_run_id = run.id
    kpis = get_kpis(db, filtros)
    return {
        "total_rows": status.get("total_rows"),
        "platforms": status.get("platforms"),
        "familias": kpis.get("familias"),
        "provincias": kpis.get("provincias"),
        "valorizacion": kpis.get("valorizacion"),
    }


def main(run_id: int | None, apply: bool, force: bool) -> None:
    if not engine:
        print("[ERROR] DB engine no disponible (revisá DATABASE_URL / APP_ENV).")
        sys.exit(1)
    print(f"[INFO] DB destino: {engine.url.render_as_string(hide_password=True)}")
    print(f"[INFO] modo: {'ESCRITURA (--apply)' if apply else 'SOLO LECTURA (default — pasá --apply para guardar)'}")

    db = SessionLocal()
    try:
        _resetear_statement_timeout(db)
        if IS_POSTGRES:
            print("[INFO] statement_timeout = 0 en esta sesión (sin límite, solo para este script).")

        run = _resolver_run_o_avisar_columna_faltante(db, run_id)
        if run is None:
            print("[ERROR] No se encontró el run (¿id correcto? ¿hay alguna corrida 'success'?).")
            sys.exit(1)
        print(f"[INFO] run_id = {run.id}  status = {run.status!r}  finished_at = {run.finished_at}")
        print(f"[INFO] platform_values ACTUAL en la base: {run.platform_values!r}")
        tiene_mapa = run.cuenta_entidad_map is not None
        print(f"[INFO] cuenta_entidad_map ACTUAL en la base: "
              f"{('poblado (' + str(len(run.cuenta_entidad_map)) + ' cuentas)') if tiene_mapa else None!r}")
        print(f"[INFO] global_totals ACTUAL en la base: {run.global_totals!r}")

        print("\n[INFO] Calculando (cada ancla, por separado):")
        platform_values, _, estado_platform = _calcular_ancla(
            db, "platform_values", run.platform_values is not None, force,
            lambda: _default_platform_values(db, run.id),
        )
        # _cuenta_to_entidad_map_from_records() SIEMPRE pega contra
        # dimensionamiento_records — a propósito NO se usa _cuenta_to_entidad_map()
        # (la función "de lectura" que consume query_service.py en cada carga del
        # dashboard) porque ESA lee primero el propio run.cuenta_entidad_map: si
        # ya hay algo guardado ahí (aunque sea {}, que no es NULL), lo devuelve
        # tal cual y este backfill terminaría "recalculando" el ancla leyendo la
        # ancla vieja — se muerde la cola. Bug real detectado set-2026: con esa
        # función, este paso "calculaba" en 0.1ms (el mismo tiempo que
        # platform_values) en vez de escanear las ~400k filas del run.
        cuenta_map, _, estado_cuenta = _calcular_ancla(
            db, "cuenta_entidad_map", run.cuenta_entidad_map is not None, force,
            lambda: _cuenta_to_entidad_map_from_records(db, run.id),
        )
        cuenta_map_json = (
            {cuenta: sorted(entidad_ids) for cuenta, entidad_ids in cuenta_map.items()}
            if cuenta_map is not None else None
        )
        global_totals, _, estado_totales = _calcular_ancla(
            db, "global_totals", run.global_totals is not None, force,
            lambda: _calcular_global_totals(db, run),
        )

        print()
        print("=" * 70)
        print("RESULTADO")
        print("=" * 70)
        print(f"  platform_values     [{estado_platform}] = {platform_values}")
        if cuenta_map_json is not None:
            muestra = dict(list(cuenta_map_json.items())[:3])
            print(f"  cuenta_entidad_map  [{estado_cuenta}] = {len(cuenta_map_json)} cuentas  "
                  f"(muestra: {muestra})")
        else:
            print(f"  cuenta_entidad_map  [{estado_cuenta}]")
        print(f"  global_totals       [{estado_totales}] = {global_totals}")

        if not apply:
            print("\n[INFO] SOLO LECTURA: no se guardó nada. Corré de nuevo con --apply para persistirlo.")
            return

        guardado_algo = False
        if platform_values is not None:
            run.platform_values = platform_values
            db.commit()
            db.refresh(run)
            print(f"\n[OK] platform_values guardado. run.platform_values = {run.platform_values!r}")
            guardado_algo = True
        if cuenta_map_json is not None:
            run.cuenta_entidad_map = cuenta_map_json
            db.commit()
            db.refresh(run)
            print(f"[OK] cuenta_entidad_map guardado. {len(run.cuenta_entidad_map)} cuentas.")
            guardado_algo = True
        if global_totals is not None:
            run.global_totals = global_totals
            db.commit()
            db.refresh(run)
            print(f"[OK] global_totals guardado. run.global_totals = {run.global_totals!r}")
            guardado_algo = True

        if not guardado_algo:
            print("\n[INFO] No se guardó nada (ninguna ancla nueva calculada — ver estados arriba).")
        else:
            print("\n[INFO] No se tocó dimensionamiento_records, dimensionamiento_family_monthly_summary "
                  "ni el snapshot del dashboard — solo las columnas que se ven arriba como guardadas.")
        if "error" in (estado_platform, estado_cuenta, estado_totales):
            print("[WARN] Al menos una ancla falló y NO quedó guardada — revisá el error de arriba y "
                  "volvé a correr el script (con --force si las demás ya se guardaron bien).")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", type=int, default=None, help="import_run_id puntual (default: última corrida success)")
    parser.add_argument("--apply", action="store_true", help="Persiste los valores calculados (default: solo los imprime).")
    parser.add_argument("--force", action="store_true", help="Recalcula y pisa aunque ya haya valores guardados.")
    args = parser.parse_args()
    main(run_id=args.run_id, apply=args.apply, force=args.force)
