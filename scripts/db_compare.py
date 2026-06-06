#!/usr/bin/env python
"""
db_compare.py — Compara DOS snapshots JSON generados por db_diagnostics.py
(--json) y produce un reporte Markdown de diferencias entre dos bases
(típicamente SQLite local vs PostgreSQL producción).

================================  SEGURIDAD  ================================
- NO se conecta a ninguna base de datos. Solo lee dos archivos JSON locales.
- No imprime credenciales (los snapshots no contienen URL ni datos de filas).

================================  USO  =====================================
    python scripts/db_compare.py \
        --local docs/db_snapshot_local.json \
        --prod  docs/db_snapshot_prod.json \
        --output docs/db_local_vs_prod.md

El snapshot de cada lado se genera con:
    python scripts/db_diagnostics.py --json docs/db_snapshot_local.json ...
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

FORECAST_PREFIX = "forecast_"
FORECAST_TABLES_IN_ORM = {
    "forecast_user_overrides",
    "forecast_manual_clients",
    "forecast_manual_entries",
    "forecast_change_requests",
}


def _load(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _cols_map(table_info: dict) -> dict[str, str]:
    return {c["name"]: c["type"] for c in table_info.get("columns", [])}


def build_comparison(local: dict, prod: dict) -> str:
    lt = local.get("tables", {})
    pt = prod.get("tables", {})
    lnames = set(lt)
    pnames = set(pt)

    only_local = sorted(lnames - pnames)
    only_prod = sorted(pnames - lnames)
    common = sorted(lnames & pnames)

    lines: list[str] = []
    w = lines.append

    w("# Comparación de esquema — SQLite local vs PostgreSQL producción")
    w("")
    w(f"> Local: **{local.get('meta', {}).get('db_label', '?')}** · "
      f"Prod: **{prod.get('meta', {}).get('db_label', '?')}**")
    w("> Generado por `scripts/db_compare.py` (offline, sin conexión a base).")
    w("")

    # --- Resumen ---
    w("## Resumen")
    w("")
    w(f"- Tablas solo en LOCAL: **{len(only_local)}**")
    w(f"- Tablas solo en PRODUCCIÓN: **{len(only_prod)}**")
    w(f"- Tablas en ambas: **{len(common)}**")
    w("")

    # --- Solo en producción (incluye forecast_* de datos base) ---
    w("## Tablas solo en PRODUCCIÓN (no en local)")
    w("")
    if only_prod:
        for t in only_prod:
            is_fc_base = t.startswith(FORECAST_PREFIX) and t not in FORECAST_TABLES_IN_ORM
            tag = "  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)" if is_fc_base else "  ← REVISAR"
            rows = pt[t].get("rows")
            w(f"- `{t}` ({rows if rows is not None else '—'} filas){tag}")
    else:
        w("_Ninguna._")
    w("")

    # --- Solo en local ---
    w("## Tablas solo en LOCAL (no en producción)")
    w("")
    if only_local:
        for t in only_local:
            rows = lt[t].get("rows")
            w(f"- `{t}` ({rows if rows is not None else '—'} filas)  ← REVISAR (¿fantasma/legado que prod no tiene?)")
    else:
        w("_Ninguna._")
    w("")

    # --- Diferencias de columnas en tablas comunes ---
    w("## Diferencias de columnas (tablas en ambas)")
    w("")
    risky: list[str] = []
    any_diff = False
    for t in common:
        lc = _cols_map(lt[t])
        pc = _cols_map(pt[t])
        added_in_prod = sorted(set(pc) - set(lc))
        missing_in_prod = sorted(set(lc) - set(pc))
        type_changes = sorted(
            c for c in (set(lc) & set(pc)) if _norm_type(lc[c]) != _norm_type(pc[c])
        )
        if not (added_in_prod or missing_in_prod or type_changes):
            continue
        any_diff = True
        w(f"### `{t}`")
        if added_in_prod:
            w(f"- Columnas solo en prod: {', '.join(f'`{c}`' for c in added_in_prod)}")
        if missing_in_prod:
            w(f"- Columnas solo en local: {', '.join(f'`{c}`' for c in missing_in_prod)}")
        for c in type_changes:
            w(f"- ⚠️ Tipo distinto en `{c}`: local=`{lc[c]}` vs prod=`{pc[c]}`")
            risky.append(f"{t}.{c}: {lc[c]} (local) vs {pc[c]} (prod)")
        w("")
    if not any_diff:
        w("_Sin diferencias de columnas en tablas comunes._")
    w("")

    # --- Clasificación de riesgo ---
    w("## Clasificación de diferencias")
    w("")
    w("**Esperables (no riesgosas):**")
    w("")
    w("- Tablas `forecast_*` de datos base presentes solo en producción (en local son CSV/parquet).")
    w("- Tablas vacías en local que en prod tienen datos (entornos distintos).")
    w("")
    w("**Potencialmente riesgosas (validar):**")
    w("")
    if risky:
        for r in risky:
            w(f"- ⚠️ {r}")
    else:
        w("- _Ninguna diferencia de tipo detectada entre columnas comunes._")
    if only_local:
        w(f"- Tablas solo en local ({', '.join(f'`{t}`' for t in only_local)}): confirmar si son fantasma/legado.")
    w("")
    w("---")
    w("_Comparación estructural. No incluye filas de datos ni credenciales._")
    return "\n".join(lines) + "\n"


def _norm_type(t: str) -> str:
    """Normaliza tipos para comparar (evita falsos positivos por variantes de dialecto)."""
    s = (t or "").upper().strip()
    # Equivalencias frecuentes SQLite <-> PostgreSQL.
    aliases = {
        "INTEGER": "INT", "BIGINT": "INT", "SMALLINT": "INT",
        "VARCHAR": "TEXT", "STRING": "TEXT", "CHAR": "TEXT",
        "DOUBLE PRECISION": "FLOAT", "REAL": "FLOAT", "NUMERIC": "FLOAT", "DECIMAL": "FLOAT",
        "TIMESTAMP": "DATETIME", "TIMESTAMP WITHOUT TIME ZONE": "DATETIME",
        "TIMESTAMP WITH TIME ZONE": "DATETIME", "BYTEA": "BLOB",
        "BOOLEAN": "BOOL",
    }
    # Recorta longitudes tipo VARCHAR(120)
    base = s.split("(")[0].strip()
    return aliases.get(base, base)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compara dos snapshots JSON de db_diagnostics.py (offline).")
    parser.add_argument("--local", required=True, help="Snapshot JSON de la base local.")
    parser.add_argument("--prod", required=True, help="Snapshot JSON de la base de producción.")
    parser.add_argument("--output", "-o", required=True, help="Ruta del reporte Markdown de comparación.")
    args = parser.parse_args()

    local = _load(Path(args.local))
    prod = _load(Path(args.prod))
    report = build_comparison(local, prod)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    print(f"[db_compare] Comparación escrita en: {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
