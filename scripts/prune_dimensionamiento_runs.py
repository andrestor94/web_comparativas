"""
Poda de corridas viejas de Dimensionamiento en PRODUCCIÓN (Postgres).

`dimensionamiento_records` y `dimensionamiento_family_monthly_summary` acumulan las
filas de TODAS las corridas (cada upload crea un run nuevo; las queries de la app son
run-scoped, así que las viejas solo ocupan disco). El 29/09/2026 el Postgres de Render
se quedó sin espacio (DiskFull) en el push_identity del run 70.

Este script borra de ESAS DOS tablas las filas de corridas viejas y deja solo la
activa y la anterior (red de rollback). NO borra filas de `dimensionamiento_import_runs`
(quedan como auditoría y así no se disparan los CASCADE hacia registry, snapshots,
oportunidades, etc.). Tampoco toca ninguna otra tabla.

Qué se borra (por tabla):
  - filas de runs con id MENOR que el menor de los conservados (cualquier status);
  - filas huérfanas: import_run_id NULL o apuntando a un run que ya no existe.
Qué NO se toca nunca: los runs conservados y cualquier run con id mayor al menor
conservado (p. ej. una corrida en staging de un upload en curso).

Subcomandos (todo es SOLO LECTURA salvo que pases --apply):
  estado       (default) corridas, tamaños, filas por run y estimación de espacio.
  borrar       simula; con --apply --keep A,B borra por lotes (commit por lote).
               Reanudable: si se corta, se repite el mismo comando y sigue.
  vacuum       VACUUM (ANALYZE) de las dos tablas. Deja el espacio reutilizable
               sin bloquear la app. Requiere --apply.
  vacuum-full  VACUUM FULL de UNA tabla. Devuelve disco al sistema pero BLOQUEA la
               tabla mientras corre. Requiere --apply --acepto-bloqueo.

Conexión: variable de entorno DATABASE_URL (la URL EXTERNA de Render), o --db-url.
Solo acepta Postgres.

Uso (PowerShell, una línea, desde la raíz):
    $env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\\.venv\\Scripts\\python.exe scripts\\prune_dimensionamiento_runs.py estado; Remove-Item Env:DATABASE_URL
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402
from sqlalchemy.exc import DBAPIError, OperationalError  # noqa: E402

TABLES = ("dimensionamiento_family_monthly_summary", "dimensionamiento_records")
RUNS = "dimensionamiento_import_runs"
MAX_RETRIES = 6


def _gb(n: float | int | None) -> str:
    return "—" if n is None else f"{n / 1024**3:,.2f} GB"


def _engine(db_url: str, read_only: bool):
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    url = make_url(db_url)
    if not url.drivername.startswith("postgresql"):
        print(f"[ERROR] Solo Postgres (recibí '{url.drivername}').")
        sys.exit(2)
    options = "-c statement_timeout=0"
    if read_only:
        options += " -c default_transaction_read_only=on"
    eng = create_engine(url, pool_pre_ping=True, connect_args={"options": options})
    print(f"[INFO] DB: {url.render_as_string(hide_password=True)}")
    print(f"[INFO] modo: {'SOLO LECTURA' if read_only else 'ESCRITURA'}")
    return eng


def _runs(conn) -> list[dict]:
    rows = conn.execute(text(
        f"SELECT id, status, finished_at, rows_processed FROM {RUNS} ORDER BY id"
    )).mappings().all()
    return [dict(r) for r in rows]


def _keep_default(runs: list[dict]) -> list[int]:
    """Activa + anterior, con el MISMO criterio que la app (latest_success_run_id:
    ORDER BY finished_at DESC, id DESC — en Postgres los NULL van primero en DESC)."""
    ok = [r for r in runs if r["status"] == "success"]
    ok.sort(key=lambda r: (r["finished_at"] is None, r["finished_at"] or dt.datetime.min, r["id"]), reverse=True)
    return sorted(r["id"] for r in ok[:2])


def _groups(conn, table: str, runs: list[dict], min_keep: int) -> list[tuple[str, str]]:
    """Grupos a borrar: un grupo por run viejo con filas + uno de huérfanas."""
    run_ids = {r["id"] for r in runs}
    present = conn.execute(text(
        f"SELECT DISTINCT import_run_id FROM {table}"
    )).scalars().all()
    groups = []
    for rid in sorted(x for x in present if x is not None and x < min_keep and x in run_ids):
        groups.append((f"run {rid}", f"import_run_id = {int(rid)}"))
    orphan_ids = sorted(x for x in present if x is not None and x not in run_ids)
    if None in present:
        groups.append(("import_run_id NULL", "import_run_id IS NULL"))
    for rid in orphan_ids:
        groups.append((f"huérfano (run {rid} inexistente)", f"import_run_id = {int(rid)}"))
    return groups


def _table_sizes(conn, table: str) -> dict:
    r = conn.execute(text("""
        SELECT pg_total_relation_size(c.oid) AS total,
               pg_relation_size(c.oid)       AS heap,
               pg_indexes_size(c.oid)        AS idx,
               s.n_live_tup, s.n_dead_tup, s.last_vacuum, s.last_autovacuum
        FROM pg_class c
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE c.oid = to_regclass(:t)
    """), {"t": table}).mappings().first()
    return dict(r) if r else {}


# ── estado ──────────────────────────────────────────────────────────────────────
def cmd_estado(eng, keep_arg: list[int] | None) -> None:
    with eng.connect() as conn:
        runs = _runs(conn)
        keep = keep_arg or _keep_default(runs)
        if len(keep) < 1:
            print("[ERROR] No hay corridas success.")
            return
        min_keep = min(keep)
        db_size = conn.execute(text("SELECT pg_database_size(current_database())")).scalar()
        print(f"\n[INFO] Tamaño de la base: {_gb(db_size)}")
        try:
            wal = conn.execute(text("SELECT COALESCE(sum(size),0) FROM pg_ls_waldir()")).scalar()
            print(f"[INFO] WAL en disco: {_gb(wal)}")
        except DBAPIError:
            conn.rollback()
            print("[INFO] WAL en disco: sin permiso para medirlo (normal en Render).")

        print("\n[INFO] Tablas más grandes:")
        for t, total in conn.execute(text("""
            SELECT c.relname, pg_total_relation_size(c.oid)
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema')
            ORDER BY 2 DESC LIMIT 10
        """)).all():
            print(f"   {t:<48} {_gb(total)}")

        active = _keep_default(runs)[-1] if _keep_default(runs) else None
        print(f"\n[INFO] Corrida activa: {active} · Conservar: {keep}")
        running_after = [r["id"] for r in runs if r["status"] == "running" and active and r["id"] > active]
        if running_after:
            print(f"[WARN] Hay corridas 'running' posteriores a la activa: {running_after} "
                  "(¿upload en curso?). `borrar --apply` se niega a correr así.")

        total_free = 0.0
        total_full = 0.0
        for table in TABLES:
            sz = _table_sizes(conn, table)
            counts = dict(conn.execute(text(
                f"SELECT import_run_id, count(*) FROM {table} GROUP BY import_run_id"
            )).all())
            run_ids = {r["id"] for r in runs}
            n_total = sum(counts.values()) or 1
            n_keep = sum(n for rid, n in counts.items() if rid in keep)
            n_del = sum(n for rid, n in counts.items()
                        if rid is None or rid not in run_ids or rid < min_keep)
            n_other = n_total - n_keep - n_del if counts else 0
            frac_del = n_del / n_total
            free_est = (sz.get("total") or 0) * frac_del
            full_est = (sz.get("total") or 0) * (n_keep + n_other) / n_total
            total_free += free_est
            total_full = max(total_full, full_est)
            print(f"\n── {table}")
            print(f"   tamaño total {_gb(sz.get('total'))} (datos {_gb(sz.get('heap'))} + índices {_gb(sz.get('idx'))})"
                  f" · vivas≈{sz.get('n_live_tup') or 0:,} muertas≈{sz.get('n_dead_tup') or 0:,}"
                  f" · último vacuum {sz.get('last_vacuum') or sz.get('last_autovacuum') or 'nunca'}")
            for rid in sorted(counts, key=lambda x: (x is None, x or 0)):
                tag = ("CONSERVAR" if rid in keep else
                       "borrar (NULL)" if rid is None else
                       "borrar (huérfano)" if rid not in run_ids else
                       "borrar" if rid < min_keep else "no se toca")
                print(f"   run {str(rid):>5}: {counts[rid]:>12,} filas   {tag}")
            print(f"   → a borrar {n_del:,} de {n_total:,} filas ({frac_del:.0%})"
                  f" · libera ≈ {_gb(free_est)} · se quedan {n_keep + n_other:,}")

        print("\n[ESTIMACIÓN]")
        print(f"   Espacio que queda reutilizable tras borrar + VACUUM: ≈ {_gb(total_free)}")
        print(f"   VACUUM FULL de la tabla más grande necesita libre ≈ {_gb(total_full)} "
              f"(+ lo mismo de margen para WAL ≈ {_gb(total_full * 2)} en total)")
        print("   `borrar` solo necesita margen para WAL: dejá ≥ 2 GB libres.")


# ── borrar ──────────────────────────────────────────────────────────────────────
def _retrying(eng, fn, what: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except OperationalError as exc:
            wait = min(60, 5 * attempt)
            print(f"   [WARN] {what}: conexión cortada ({str(exc).splitlines()[0][:120]}). "
                  f"Reintento {attempt}/{MAX_RETRIES} en {wait}s...", flush=True)
            eng.dispose()
            time.sleep(wait)
    print(f"[ERROR] {what}: sin conexión tras {MAX_RETRIES} intentos. "
          "Repetí el MISMO comando: retoma desde lo que falta.")
    sys.exit(3)


def cmd_borrar(eng, keep_arg: list[int] | None, apply: bool, lote: int, pausa: float,
               max_lotes: int | None) -> None:
    with eng.connect() as conn:
        runs = _runs(conn)
    expected = _keep_default(runs)
    keep = keep_arg or expected
    if apply:
        if not keep_arg:
            print(f"[ERROR] Con --apply tenés que pasar --keep explícito (esperado: {','.join(map(str, expected))}).")
            sys.exit(2)
        if sorted(keep_arg) != expected:
            print(f"[ERROR] --keep {keep_arg} no coincide con activa+anterior según la base: {expected}. No se borra nada.")
            sys.exit(2)
        active = expected[-1]
        running_after = [r["id"] for r in runs if r["status"] == "running" and r["id"] > active]
        if running_after:
            print(f"[ERROR] Hay corridas 'running' posteriores a la activa {active}: {running_after}. "
                  "Terminá o hacé rollback de ese upload antes de podar.")
            sys.exit(2)
    min_keep = min(keep)
    print(f"[INFO] Conservar: {keep} · se borran filas con import_run_id < {min_keep}, NULL o huérfanas")
    print(f"[INFO] Lote: {lote:,} ids por DELETE · pausa {pausa}s"
          + (f" · máximo {max_lotes} lotes" if max_lotes else ""))

    lotes_hechos = 0
    grand = 0
    t0 = time.time()
    for table in TABLES:
        with eng.connect() as conn:
            groups = _groups(conn, table, runs, min_keep)
        if not groups:
            print(f"\n── {table}: nada para borrar.")
            continue
        print(f"\n── {table}: {len(groups)} grupo(s)")
        for label, cond in groups:
            with eng.connect() as conn:
                lo, hi, n = conn.execute(text(
                    f"SELECT min(id), max(id), count(*) FROM {table} WHERE {cond}"
                )).one()
            if not n:
                continue
            print(f"   {label}: {n:,} filas (ids {lo:,}–{hi:,})", flush=True)
            if not apply:
                grand += n
                continue
            done = 0
            cur = lo
            while cur <= hi:
                if max_lotes and lotes_hechos >= max_lotes:
                    print(f"\n[INFO] Tope de {max_lotes} lotes alcanzado. Borradas {grand:,} filas. "
                          "Repetí el comando (sin --max-lotes) para seguir.")
                    return
                a, b = cur, cur + lote

                def _one(a=a, b=b):
                    with eng.begin() as c:
                        c.execute(text("SET LOCAL statement_timeout = '120s'"))
                        c.execute(text("SET LOCAL lock_timeout = '10s'"))
                        return c.execute(text(
                            f"DELETE FROM {table} WHERE id >= :a AND id < :b AND {cond}"
                        ), {"a": a, "b": b}).rowcount

                deleted = _retrying(eng, _one, f"{table} {label} ids {a:,}+")
                done += deleted
                grand += deleted
                lotes_hechos += 1
                cur = b
                if deleted and (lotes_hechos % 20 == 0 or cur > hi):
                    rate = grand / max(time.time() - t0, 1e-6)
                    print(f"      {done:,}/{n:,} ({done / n:.0%}) · total {grand:,} · {rate:,.0f} filas/s", flush=True)
                if pausa:
                    time.sleep(pausa)
            print(f"   ✅ {label}: {done:,} filas borradas", flush=True)

    if apply:
        print(f"\n✅ LISTO: {grand:,} filas borradas en {time.time() - t0:,.0f}s. "
              "Siguiente paso: subcomando `vacuum --apply`.")
    else:
        print(f"\n[SIMULACIÓN] Se borrarían {grand:,} filas. No se tocó nada. "
              f"Para borrar: borrar --apply --keep {','.join(map(str, keep))}")


# ── vacuum ──────────────────────────────────────────────────────────────────────
def _vacuum(eng, table: str, full: bool) -> None:
    with eng.connect() as conn:
        before = _table_sizes(conn, table)
    print(f"\n── {table}: antes {_gb(before.get('total'))} · muertas≈{before.get('n_dead_tup') or 0:,}", flush=True)
    t0 = time.time()
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("SET statement_timeout = 0"))
        conn.execute(text(f"VACUUM ({'FULL, ' if full else ''}ANALYZE) {table}"))
    with eng.connect() as conn:
        after = _table_sizes(conn, table)
    print(f"   después {_gb(after.get('total'))} · muertas≈{after.get('n_dead_tup') or 0:,} "
          f"· {time.time() - t0:,.0f}s", flush=True)


def cmd_vacuum(eng, apply: bool) -> None:
    if not apply:
        print("[INFO] vacuum hace VACUUM (ANALYZE) de las 2 tablas. No bloquea lecturas "
              "ni escrituras. Pasá --apply para correrlo.")
        return
    for table in TABLES:
        _vacuum(eng, table, full=False)
    print("\n✅ VACUUM listo. El tamaño en disco casi no baja: el espacio liberado queda "
          "REUTILIZABLE dentro de las tablas y el próximo upload lo ocupa sin crecer.")


def cmd_vacuum_full(eng, table: str, apply: bool, acepto: bool) -> None:
    if table not in TABLES:
        print(f"[ERROR] --tabla debe ser una de: {', '.join(TABLES)}")
        sys.exit(2)
    with eng.connect() as conn:
        sz = _table_sizes(conn, table)
        sample = conn.execute(text(
            f"SELECT avg(pg_column_size(t.*)) FROM (SELECT * FROM {table} TABLESAMPLE SYSTEM (1)) t"
        )).scalar()
    live = sz.get("n_live_tup") or 0
    heap_est = live * float(sample or 0) * 1.2
    idx_est = (sz.get("idx") or 0) * min(1.0, heap_est / max(sz.get("heap") or 1, 1))
    need = heap_est + idx_est
    print(f"[INFO] {table}: hoy ocupa {_gb(sz.get('total'))}; la copia compacta ocuparía ≈ {_gb(need)}.")
    print(f"[INFO] Libre necesario: ≈ {_gb(need)} para la copia + ≈ {_gb(need)} de margen de WAL = ≈ {_gb(need * 2)}.")
    print("[WARN] VACUUM FULL toma un lock EXCLUSIVO: Dimensionamiento y Oportunidades no cargan mientras dura.")
    if not (apply and acepto):
        print("[INFO] No se corrió. Para correrlo: vacuum-full --tabla ... --apply --acepto-bloqueo")
        return
    _vacuum(eng, table, full=True)
    print("\n✅ VACUUM FULL listo: el espacio volvió al disco.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Poda de corridas viejas de Dimensionamiento (Postgres).")
    ap.add_argument("accion", nargs="?", default="estado",
                    choices=["estado", "borrar", "vacuum", "vacuum-full"])
    ap.add_argument("--db-url", default=os.environ.get("DATABASE_URL"),
                    help="URL de Postgres (default: variable de entorno DATABASE_URL)")
    ap.add_argument("--keep", default=None, help="ids a conservar, ej. 69,70 (obligatorio con borrar --apply)")
    ap.add_argument("--apply", action="store_true", help="ejecutar de verdad (sin esto, solo lectura)")
    ap.add_argument("--lote", type=int, default=5000, help="ids por DELETE (default 5000)")
    ap.add_argument("--pausa", type=float, default=0.2, help="segundos entre lotes (default 0.2)")
    ap.add_argument("--max-lotes", type=int, default=None, help="cortar tras N lotes (para probar)")
    ap.add_argument("--tabla", default=None, help="tabla para vacuum-full")
    ap.add_argument("--acepto-bloqueo", action="store_true", help="confirma el lock exclusivo de vacuum-full")
    args = ap.parse_args()

    if not args.db_url:
        print("[ERROR] Falta DATABASE_URL (o --db-url).")
        return 2
    keep = sorted({int(x) for x in args.keep.split(",") if x.strip()}) if args.keep else None
    writes = args.apply and args.accion != "estado"
    eng = _engine(args.db_url, read_only=not writes)

    if args.accion == "estado":
        cmd_estado(eng, keep)
    elif args.accion == "borrar":
        cmd_borrar(eng, keep, args.apply, args.lote, args.pausa, args.max_lotes)
    elif args.accion == "vacuum":
        cmd_vacuum(eng, args.apply)
    else:
        cmd_vacuum_full(eng, args.tabla or "", args.apply, args.acepto_bloqueo)
    return 0


if __name__ == "__main__":
    sys.exit(main())
