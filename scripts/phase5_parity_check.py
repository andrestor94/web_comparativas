#!/usr/bin/env python
"""
phase5_parity_check.py — Verificación de PARIDAD + baseline para la Fase 5.

Valida que las optimizaciones de la Fase 5 (mover medianas/percentiles de Python a
SQL con `percentile_cont`, y el "último precio" de un loop Python a `DISTINCT ON`)
devuelven EXACTAMENTE el mismo resultado que el código anterior, calculando cada
métrica de las DOS formas contra la MISMA base de datos y comparándolas, con timing.

Pensado para correrse contra una COPIA representativa de producción (PostgreSQL) o
una base de staging ANTES de aprobar el deploy. También corre en SQLite local, pero
ahí `percentile_cont` no existe y el código de la app usa de todos modos la ruta
Python, por lo que la paridad es trivial (se reporta como SKIP).

================================  SEGURIDAD  ================================
ESTRICTAMENTE de solo lectura:
  - Solo SELECT. NUNCA INSERT/UPDATE/DELETE ni CREATE/ALTER/DROP.
  - NUNCA imprime DATABASE_URL ni credenciales (se enmascara host/usuario/pass).
  - Contra una base remota (no SQLite) EXIGE --confirm-remote.
  - No importa la app (evita migraciones de arranque): engine propio.

================================  USO  =====================================
Local (SQLite de la app por defecto):
    python scripts/phase5_parity_check.py

Copia de producción (PostgreSQL) — con autorización:
    DATABASE_URL=postgresql://... python scripts/phase5_parity_check.py \
        --confirm-remote

Opciones:
    --tolerance 0.01   Máxima diferencia absoluta admitida tras redondear a 2 dec.
    --sample-groups 50 Máx. de grupos (meses/proveedores) a comparar en detalle.
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

try:
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import Engine
except Exception as exc:  # pragma: no cover
    print(f"[ERROR] SQLAlchemy no disponible: {exc}", file=sys.stderr)
    sys.exit(2)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_database_url() -> str:
    raw = (os.getenv("DATABASE_URL") or "").strip()
    if raw:
        return raw.replace("postgres://", "postgresql://")
    local_db = _repo_root() / "web_comparativas" / "app.db"
    return f"sqlite:///{local_db.as_posix()}"


def _safe_db_label(engine: Engine) -> str:
    url = engine.url
    backend = url.get_backend_name()
    db_name = url.database or "(memoria)"
    if backend == "sqlite":
        db_name = Path(db_name).name if db_name else "(memoria)"
    return f"{backend} / db={db_name}"


def _is_sqlite(engine: Engine) -> bool:
    return engine.url.get_backend_name() == "sqlite"


def _has_table(engine: Engine, table: str) -> bool:
    try:
        return inspect(engine).has_table(table)
    except Exception:
        return False


# --------------------------------------------------------------------------
# Helpers de comparación
# --------------------------------------------------------------------------
class Result:
    def __init__(self, name: str):
        self.name = name
        self.status = "SKIP"     # PASS / FAIL / SKIP
        self.detail = ""
        self.t_old = None
        self.t_new = None

    def line(self) -> str:
        icon = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️"}.get(self.status, "?")
        timing = ""
        if self.t_old is not None and self.t_new is not None:
            timing = f"  (python={self.t_old*1000:.0f}ms · sql={self.t_new*1000:.0f}ms)"
        return f"  {icon} {self.name}: {self.status}{timing}\n      {self.detail}"


def _close(a: float, b: float, tol: float) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(round(a, 2) - round(b, 2)) <= tol


# --------------------------------------------------------------------------
# Métricas: dimensionamiento_records (Mercado Privado)
# --------------------------------------------------------------------------
def check_privado_kpis_median(engine: Engine, tol: float) -> Result:
    r = Result("privado/articulo/kpis · mediana precio unitario (global)")
    if not _has_table(engine, "dimensionamiento_records"):
        r.detail = "tabla dimensionamiento_records ausente"
        return r
    if _is_sqlite(engine):
        r.detail = "SQLite: la app usa ruta Python (paridad trivial); percentile_cont no aplica"
        return r
    base = (
        "FROM dimensionamiento_records "
        "WHERE cantidad_demandada IS NOT NULL AND cantidad_demandada > 0 "
        "AND valorizacion_estimada IS NOT NULL AND valorizacion_estimada > 0"
    )
    with engine.connect() as conn:
        t0 = time.perf_counter()
        ratios = [
            row[0] / row[1]
            for row in conn.execute(text(
                f"SELECT valorizacion_estimada, cantidad_demandada {base}"
            ))
        ]
        old = round(statistics.median(ratios), 2) if ratios else None
        t_old = time.perf_counter() - t0

        t1 = time.perf_counter()
        new_val = conn.execute(text(
            "SELECT percentile_cont(0.5) WITHIN GROUP "
            "(ORDER BY valorizacion_estimada / cantidad_demandada) " + base
        )).scalar()
        new = round(float(new_val), 2) if new_val is not None else None
        t_new = time.perf_counter() - t1

    r.t_old, r.t_new = t_old, t_new
    r.status = "PASS" if _close(old, new, tol) else "FAIL"
    r.detail = f"n={len(ratios)} · python={old} · sql={new}"
    return r


def check_privado_precio_evolucion(engine: Engine, tol: float, max_groups: int) -> Result:
    r = Result("privado/articulo/precio-evolucion · mediana por mes")
    if not _has_table(engine, "dimensionamiento_records"):
        r.detail = "tabla dimensionamiento_records ausente"
        return r
    if _is_sqlite(engine):
        r.detail = "SQLite: la app usa ruta Python (paridad trivial)"
        return r
    base = (
        "FROM dimensionamiento_records "
        "WHERE cantidad_demandada IS NOT NULL AND cantidad_demandada > 0 "
        "AND valorizacion_estimada IS NOT NULL AND valorizacion_estimada > 0"
    )
    with engine.connect() as conn:
        t0 = time.perf_counter()
        month_ratios: dict = {}
        for row in conn.execute(text(
            "SELECT CAST(date_trunc('month', fecha) AS DATE) AS m, "
            "valorizacion_estimada, cantidad_demandada " + base
        )):
            month_ratios.setdefault(str(row[0]), []).append(row[1] / row[2])
        old = {m: round(statistics.median(v), 2) for m, v in month_ratios.items()}
        t_old = time.perf_counter() - t0

        t1 = time.perf_counter()
        new = {}
        for row in conn.execute(text(
            "SELECT CAST(date_trunc('month', fecha) AS DATE) AS m, "
            "percentile_cont(0.5) WITHIN GROUP "
            "(ORDER BY valorizacion_estimada / cantidad_demandada) AS med "
            + base + " GROUP BY m"
        )):
            new[str(row[0])] = round(float(row[1]), 2) if row[1] is not None else None
        t_new = time.perf_counter() - t1

    r.t_old, r.t_new = t_old, t_new
    diffs = [m for m in set(old) | set(new) if not _close(old.get(m), new.get(m), tol)]
    r.status = "PASS" if not diffs else "FAIL"
    r.detail = (f"meses={len(old)} · coinciden={len(old) - len(diffs)}"
                + (f" · DIFF={diffs[:max_groups]}" if diffs else ""))
    return r


# --------------------------------------------------------------------------
# Métricas: comparativa_rows (Mercado Público)
# --------------------------------------------------------------------------
def check_publico_kpis_median(engine: Engine, tol: float) -> Result:
    r = Result("publico/articulos/kpis · mediana precio unitario")
    if not _has_table(engine, "comparativa_rows"):
        r.detail = "tabla comparativa_rows ausente"
        return r
    if _is_sqlite(engine):
        r.detail = "SQLite: la app usa ruta Python (paridad trivial)"
        return r
    base = ("FROM comparativa_rows "
            "WHERE precio_unitario IS NOT NULL AND fecha_apertura IS NOT NULL")
    with engine.connect() as conn:
        t0 = time.perf_counter()
        prices = [row[0] for row in conn.execute(text(
            f"SELECT precio_unitario {base}")) if row[0] is not None]
        old = round(statistics.median(prices), 2) if prices else None
        t_old = time.perf_counter() - t0

        t1 = time.perf_counter()
        new_val = conn.execute(text(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY precio_unitario) " + base
        )).scalar()
        new = round(float(new_val), 2) if new_val is not None else None
        t_new = time.perf_counter() - t1

    r.t_old, r.t_new = t_old, t_new
    r.status = "PASS" if _close(old, new, tol) else "FAIL"
    r.detail = f"n={len(prices)} · python={old} · sql={new}"
    return r


def check_publico_por_proveedor(engine: Engine, tol: float, max_groups: int) -> Result:
    r = Result("publico/articulos/por-proveedor · mediana + último precio por proveedor")
    if not _has_table(engine, "comparativa_rows"):
        r.detail = "tabla comparativa_rows ausente"
        return r
    if _is_sqlite(engine):
        r.detail = "SQLite: la app usa ruta Python (paridad trivial)"
        return r
    base = ("FROM comparativa_rows "
            "WHERE precio_unitario IS NOT NULL AND precio_unitario > 0 "
            "AND proveedor IS NOT NULL")

    def _median(vals):
        s = sorted(vals)
        n = len(s)
        mid = n // 2
        return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0

    with engine.connect() as conn:
        # OLD: pull rows ordered by date desc, group in Python (median + último).
        t0 = time.perf_counter()
        by_prov: dict = {}
        ultimo_old: dict = {}
        for row in conn.execute(text(
            "SELECT proveedor, precio_unitario " + base
            + " ORDER BY fecha_apertura DESC, id ASC"
        )):
            by_prov.setdefault(row[0], []).append(row[1])
            if row[0] not in ultimo_old:
                ultimo_old[row[0]] = row[1]
        median_old = {p: round(_median(v), 2) for p, v in by_prov.items()}
        t_old = time.perf_counter() - t0

        # NEW: percentile_cont group by + DISTINCT ON.
        t1 = time.perf_counter()
        median_new = {}
        for row in conn.execute(text(
            "SELECT proveedor, percentile_cont(0.5) WITHIN GROUP (ORDER BY precio_unitario) "
            + base + " GROUP BY proveedor"
        )):
            median_new[row[0]] = round(float(row[1]), 2) if row[1] is not None else 0.0
        ultimo_new = {}
        for row in conn.execute(text(
            "SELECT DISTINCT ON (proveedor) proveedor, precio_unitario " + base
            + " ORDER BY proveedor, fecha_apertura DESC, id ASC"
        )):
            ultimo_new[row[0]] = row[1]
        t_new = time.perf_counter() - t1

    r.t_old, r.t_new = t_old, t_new
    med_diffs = [p for p in set(median_old) | set(median_new)
                 if not _close(median_old.get(p), median_new.get(p), tol)]
    ult_diffs = [p for p in set(ultimo_old) | set(ultimo_new)
                 if not _close(ultimo_old.get(p), ultimo_new.get(p), tol)]
    r.status = "PASS" if not med_diffs and not ult_diffs else "FAIL"
    r.detail = (f"proveedores={len(median_old)} · mediana_diffs={len(med_diffs)} "
                f"· ultimo_diffs={len(ult_diffs)}")
    if med_diffs:
        r.detail += f" · MED_DIFF={med_diffs[:max_groups]}"
    if ult_diffs:
        r.detail += f" · ULT_DIFF={ult_diffs[:max_groups]}"
    return r


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Paridad + baseline Fase 5 (read-only).")
    ap.add_argument("--confirm-remote", action="store_true",
                    help="Requerido para apuntar a una base no-SQLite (producción/copia).")
    ap.add_argument("--tolerance", type=float, default=0.01,
                    help="Diferencia absoluta máxima admitida tras redondear a 2 dec.")
    ap.add_argument("--sample-groups", type=int, default=50,
                    help="Máx. de grupos a listar en detalle ante diferencias.")
    args = ap.parse_args()

    # Consola Windows (cp1252) no puede encodear emojis: forzamos UTF-8 si se puede.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

    url = _resolve_database_url()
    engine = create_engine(url, future=True)
    label = _safe_db_label(engine)

    if not _is_sqlite(engine) and not args.confirm_remote:
        print(f"[ABORT] Base remota detectada ({label}).\n"
              "        Reejecutá con --confirm-remote SOLO si tenés autorización y es\n"
              "        una COPIA/staging o producción con permiso explícito (read-only).",
              file=sys.stderr)
        return 3

    print(f"== Fase 5 · Parity check ==  base: {label}  ·  tol={args.tolerance}\n")

    checks = [
        check_privado_kpis_median(engine, args.tolerance),
        check_privado_precio_evolucion(engine, args.tolerance, args.sample_groups),
        check_publico_kpis_median(engine, args.tolerance),
        check_publico_por_proveedor(engine, args.tolerance, args.sample_groups),
    ]
    for c in checks:
        print(c.line())

    fails = [c for c in checks if c.status == "FAIL"]
    passes = [c for c in checks if c.status == "PASS"]
    skips = [c for c in checks if c.status == "SKIP"]
    print(f"\nResumen: {len(passes)} PASS · {len(fails)} FAIL · {len(skips)} SKIP")
    if fails:
        print("RESULTADO: ❌ HAY DIFERENCIAS — NO deployar hasta resolver.")
        return 1
    print("RESULTADO: ✅ Paridad OK (o N/A en SQLite). Apto para validar deploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
