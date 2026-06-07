#!/usr/bin/env python
"""
phase5_evolucion_marca_parity_check.py — Parity test DEDICADO de `evolucion-marca`.

Valida si llevar el endpoint `articulos/evolucion-marca` (Mercado Público) a SQL
produce EXACTAMENTE el mismo resultado que el código Python actual, calculando las
DOS rutas sobre la misma base y comparándolas. El punto crítico es la NORMALIZACIÓN
de la marca: si la versión SQL agrupa una marca distinto que Python, este test lo
reporta de forma EXPLÍCITA (no lo oculta) y da FAIL.

Es PROPUESTA / EXPERIMENTAL: este test NO cambia el router. Solo decide si la
versión SQL sería equivalente sobre los datos reales. Ver
`docs/fase5_evolucion_marca_sql_proposal.md`.

================================  SEGURIDAD  ================================
ESTRICTAMENTE read-only: solo SELECT. NUNCA escribe. No imprime credenciales.
Contra base no-SQLite EXIGE --confirm-remote. No importa la app.

================================  USO  =====================================
Copia/staging o usuario read-only (PostgreSQL):
    DATABASE_URL=postgresql://... python -X utf8 \
        scripts/phase5_evolucion_marca_parity_check.py --confirm-remote

En SQLite local da SKIP (la normalización SQL es PG-only; el router mantiene Python).
"""
from __future__ import annotations

import argparse
import os
import re
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


# ---- Normalización Python: COPIA EXACTA de _normalize_marca del router ----
def _normalize_marca(m: str) -> str:
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.strip()).upper()


def main() -> int:
    ap = argparse.ArgumentParser(description="Parity de evolucion-marca (read-only).")
    ap.add_argument("--confirm-remote", action="store_true")
    ap.add_argument("--tolerance", type=float, default=0.01)
    ap.add_argument("--sample", type=int, default=30,
                    help="Máx. de claves a listar en detalle ante diferencias.")
    args = ap.parse_args()

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

    engine = create_engine(_resolve_database_url(), future=True)
    label = _safe_db_label(engine)

    if not _is_sqlite(engine) and not args.confirm_remote:
        print(f"[ABORT] Base remota ({label}). Reejecutá con --confirm-remote solo si es"
              " copia/staging o conexión read-only autorizada.", file=sys.stderr)
        return 3

    print(f"== Fase 5 · evolucion-marca parity ==  base: {label}  ·  tol={args.tolerance}\n")

    if _is_sqlite(engine):
        print("  ⏭️  SKIP: en SQLite la normalización SQL (regexp_replace/upper PG) no aplica;"
              " el router mantiene la ruta Python. Correr contra una copia PostgreSQL.")
        print("\nRESULTADO: ⏭️ SKIP (SQLite). Validar contra PostgreSQL.")
        return 0

    if not inspect(engine).has_table("comparativa_rows"):
        print("  ❌ tabla comparativa_rows ausente.")
        return 1

    # ── Ruta VIEJA (Python): trae filas, normaliza y agrupa igual que el router ──
    old_q = text(
        "SELECT EXTRACT(year FROM fecha_apertura)::int  AS year, "
        "       EXTRACT(month FROM fecha_apertura)::int AS month, "
        "       marca, precio_unitario "
        "FROM comparativa_rows "
        "WHERE fecha_apertura IS NOT NULL AND precio_unitario IS NOT NULL "
        "  AND marca IS NOT NULL "
        "ORDER BY year, month, marca"
    )
    t0 = time.perf_counter()
    groups: dict = {}
    old_display: dict = {}
    with engine.connect() as conn:
        for row in conn.execute(old_q):
            marca_norm = _normalize_marca(row.marca or "")
            if not marca_norm:
                continue
            if marca_norm not in old_display:
                old_display[marca_norm] = (row.marca or "").strip()
            groups.setdefault((row.year, row.month, marca_norm), []).append(row.precio_unitario)
    old_med = {k: round(statistics.median(v), 2) for k, v in groups.items()}
    t_old = time.perf_counter() - t0

    # ── Ruta NUEVA (SQL): normaliza, agrupa y mediana en PostgreSQL ──
    new_q = text(r"""
        WITH norm AS (
          SELECT
            EXTRACT(year  FROM fecha_apertura)::int                       AS year,
            EXTRACT(month FROM fecha_apertura)::int                       AS month,
            upper(btrim(regexp_replace(marca, '\s+', ' ', 'g')))         AS marca_norm,
            regexp_replace(marca, '^\s+|\s+$', '', 'g')                   AS marca_stripped,
            marca                                                         AS marca_raw,
            precio_unitario
          FROM comparativa_rows
          WHERE fecha_apertura IS NOT NULL AND precio_unitario IS NOT NULL
            AND marca IS NOT NULL
        ),
        filt AS (SELECT * FROM norm WHERE marca_norm <> ''),
        display AS (
          SELECT DISTINCT ON (marca_norm) marca_norm, marca_stripped AS display
          FROM filt
          ORDER BY marca_norm, year, month, marca_raw
        )
        SELECT f.year, f.month, f.marca_norm, d.display,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY f.precio_unitario) AS mediana
        FROM filt f JOIN display d USING (marca_norm)
        GROUP BY f.year, f.month, f.marca_norm, d.display
        ORDER BY f.year, f.month, f.marca_norm
    """)
    t1 = time.perf_counter()
    new_med: dict = {}
    new_display: dict = {}
    with engine.connect() as conn:
        for row in conn.execute(new_q):
            k = (row.year, row.month, row.marca_norm)
            new_med[k] = round(float(row.mediana), 2) if row.mediana is not None else 0.0
            new_display.setdefault(row.marca_norm, row.display)
    t_new = time.perf_counter() - t1

    # ── Comparación ──
    old_norms = set(old_display)
    new_norms = set(new_display)
    norm_only_old = sorted(old_norms - new_norms)
    norm_only_new = sorted(new_norms - old_norms)

    old_keys = set(old_med)
    new_keys = set(new_med)
    key_only_old = sorted(old_keys - new_keys)
    key_only_new = sorted(new_keys - old_keys)

    value_diffs = [
        (k, old_med[k], new_med[k])
        for k in (old_keys & new_keys)
        if abs(old_med[k] - new_med[k]) > args.tolerance
    ]
    display_diffs = [
        (n, old_display[n], new_display[n])
        for n in (old_norms & new_norms)
        if old_display[n] != new_display[n]
    ]

    s = args.sample
    print(f"  Puntos (year,month,marca_norm): python={len(old_keys)} · sql={len(new_keys)}")
    print(f"  Marcas normalizadas:            python={len(old_norms)} · sql={len(new_norms)}")
    print(f"  Tiempos: python={t_old*1000:.0f}ms · sql={t_new*1000:.0f}ms\n")

    fail = False
    if norm_only_old or norm_only_new:
        fail = True
        print(f"  ❌ GROUPING DIFF en marcas normalizadas:")
        if norm_only_old:
            print(f"      solo en PYTHON ({len(norm_only_old)}): {norm_only_old[:s]}")
        if norm_only_new:
            print(f"      solo en SQL    ({len(norm_only_new)}): {norm_only_new[:s]}")
    if key_only_old or key_only_new:
        fail = True
        print(f"  ❌ Claves (year,month,marca) distintas: solo_python={len(key_only_old)} "
              f"· solo_sql={len(key_only_new)}")
        if key_only_old:
            print(f"      ej. solo python: {key_only_old[:s]}")
        if key_only_new:
            print(f"      ej. solo sql:    {key_only_new[:s]}")
    if value_diffs:
        fail = True
        print(f"  ❌ Medianas con |dif| > {args.tolerance} ({len(value_diffs)}): {value_diffs[:s]}")
    if display_diffs:
        # WARN: no bloquea por sí solo (es cosmético si no hubo grouping diff).
        print(f"  ⚠️  Display distinto sin grouping diff ({len(display_diffs)}): {display_diffs[:s]}")

    if not fail:
        print("  ✅ Sin diferencias de agrupación, claves ni medianas (display: ver WARN si hubo).")

    print()
    if fail:
        print("RESULTADO: ❌ FAIL — evolucion-marca NO es equivalente sobre estos datos. "
              "Queda EXPERIMENTAL, no se aplica.")
        return 1
    print("RESULTADO: ✅ PASS — la versión SQL reproduce la agrupación y medianas sobre datos reales."
          + ("  (revisar el WARN de display)" if display_diffs else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
