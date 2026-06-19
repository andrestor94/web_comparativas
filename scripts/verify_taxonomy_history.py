#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/verify_taxonomy_history.py
──────────────────────────────────
DIAGNÓSTICO READ-ONLY (Fase 1): muestra cómo quedaría RE-ETIQUETADO el historial
real de `usage_events` bajo la taxonomía única (web_comparativas/tracking_taxonomy.py),
SIN modificar la base de datos.

GARANTÍAS
  • READ-ONLY absoluto: abre la SQLite local en modo `?mode=ro` (URI). Solo SELECT.
    Imposible escribir (INSERT/UPDATE/DELETE/ALTER fallan a nivel driver).
  • SOLO DB LOCAL: se conecta directo al archivo web_comparativas/app.db. NO lee
    DATABASE_URL ni ninguna URL externa; NO importa el módulo `models` (que sí
    podría conectarse a Render). Solo importa `tracking_taxonomy` (liviano).
  • Un solo uso, no cableado a la app.

USO
    python scripts/verify_taxonomy_history.py [ruta_opcional_a_app.db]
"""
from __future__ import annotations

import os
import sys
import sqlite3
from collections import defaultdict
from pathlib import Path

# stdout en UTF-8 (acentos) sin romper en consolas Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── Importar SOLO la taxonomía (no `models`) ──────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from web_comparativas import tracking_taxonomy as tax  # noqa: E402

OTROS_KEY = "otros"


def _resolve_db_path(argv: list[str]) -> Path:
    if len(argv) > 1 and argv[1].strip():
        return Path(argv[1]).expanduser().resolve()
    return REPO_ROOT / "web_comparativas" / "app.db"


def _classify(raw):
    """Mirrors _map_section_name + indica el ORIGEN del match.

    Devuelve dict: {label, module, bucket, match}
      bucket ∈ {"REAL", "OTROS", "SIN_CLASIF"}
    """
    norm = tax._norm(raw)
    key = tax.ALIAS_TO_KEY.get(norm)

    if norm == "":  # section NULL/"" → la UI lo muestra como "Inicio"
        return {"label": "Inicio", "module": None, "bucket": "REAL", "match": "vacía → Inicio"}

    if key is not None and key != OTROS_KEY:
        sec = tax.SECTION_BY_KEY[key]
        return {"label": sec["label"], "module": sec["module"], "bucket": "REAL", "match": "alias directo"}

    if key == OTROS_KEY:
        # marcador nulo explícito (otro/unknown/undefined…); la UI lo omite ("")
        return {"label": "Otros / Sin clasificar", "module": None, "bucket": "OTROS",
                "match": "marcador nulo"}

    # No matcheó ningún alias → humanizado best-effort, sin sección real
    label = tax.label_for(raw) or "(etiqueta vacía)"
    return {"label": label, "module": None, "bucket": "SIN_CLASIF", "match": "SIN ALIAS (humanizado)"}


def main() -> int:
    db_path = _resolve_db_path(sys.argv)

    # Salvaguarda extra: nunca usar URLs externas, aunque estén en el entorno.
    if os.getenv("DATABASE_URL_EXTERNAL"):
        print("[aviso] DATABASE_URL_EXTERNAL está seteada en el entorno; este script la IGNORA "
              "y usa exclusivamente la SQLite local.", flush=True)

    print("=" * 100)
    print("VERIFICACIÓN DE RE-ETIQUETADO DEL HISTORIAL (taxonomía única) — READ-ONLY")
    print(f"DB local: {db_path}")
    print("=" * 100)

    if not db_path.exists():
        print(f"\n[ERROR] No existe el archivo de base local: {db_path}")
        print("        Pasá la ruta como argumento si tu app.db está en otro lugar.")
        return 1

    # Conexión READ-ONLY (URI mode=ro)
    uri = f"file:{db_path.as_posix()}?mode=ro"
    try:
        con = sqlite3.connect(uri, uri=True)
    except Exception as exc:
        print(f"\n[ERROR] No se pudo abrir la DB en modo read-only: {exc}")
        return 1

    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='usage_events'")
        if not cur.fetchone():
            print("\n[INFO] La tabla 'usage_events' no existe en esta DB local. Nada para verificar.")
            return 0

        cur.execute("SELECT COUNT(*) FROM usage_events")
        total_rows = int(cur.fetchone()[0] or 0)
        if total_rows == 0:
            print("\n[INFO] 'usage_events' existe pero está VACÍA. Nada para verificar.")
            return 0

        cur.execute(
            "SELECT section, COUNT(*) AS n FROM usage_events GROUP BY section ORDER BY n DESC"
        )
        rows = cur.fetchall()
    finally:
        con.close()

    # ── Resolver cada clave cruda ────────────────────────────────────────────
    resolved = []  # (raw_display, n, label, module, bucket, match)
    by_label = defaultdict(list)   # label canónica → [(raw, n)]
    totals = {"REAL": 0, "OTROS": 0, "SIN_CLASIF": 0}

    for raw, n in rows:
        n = int(n)
        info = _classify(raw)
        raw_disp = "∅ (NULL/'')" if (raw is None or str(raw).strip() == "") else str(raw)
        resolved.append((raw_disp, n, info["label"], info["module"], info["bucket"], info["match"]))
        totals[info["bucket"]] += n
        if info["bucket"] == "REAL":
            by_label[info["label"]].append((raw_disp, n))

    # ── 1) Tabla principal (orden por frecuencia desc) ───────────────────────
    print("\n" + "─" * 100)
    print("1) CLAVES CRUDAS DEL HISTORIAL → ETIQUETA CANÓNICA")
    print("─" * 100)
    hdr = f"{'section cruda':<34}{'#ev':>7}  {'→ etiqueta canónica':<34}{'módulo':<16}{'origen':<22}"
    print(hdr)
    print("-" * 100)
    for raw_disp, n, label, module, bucket, match in resolved:
        flag = "" if bucket == "REAL" else "  ◀ A REVISAR"
        print(f"{raw_disp[:33]:<34}{n:>7}  {label[:33]:<34}{str(module or '—')[:15]:<16}{match:<22}{flag}")

    # ── 2) Sección "A REVISAR" ───────────────────────────────────────────────
    print("\n" + "─" * 100)
    print("2) A REVISAR")
    print("─" * 100)

    # (a) sin clasificar / otros
    flagged = [r for r in resolved if r[4] in ("OTROS", "SIN_CLASIF")]
    print("\n(a) Claves crudas que NO matchearon una sección real "
          "(caen en 'Otros / Sin clasificar' o se humanizan):")
    if not flagged:
        print("    ✓ Ninguna. Todo el historial matcheó una sección canónica real.")
    else:
        print(f"    {'section cruda':<34}{'#ev':>7}  {'bucket':<12}{'etiqueta mostrada':<30}")
        for raw_disp, n, label, module, bucket, match in sorted(flagged, key=lambda x: -x[1]):
            print(f"    {raw_disp[:33]:<34}{n:>7}  {bucket:<12}{label[:29]:<30}")

    # (b) fusiones: ≥2 claves crudas distintas → misma etiqueta canónica
    print("\n(b) Fusiones (varias claves crudas distintas → MISMA etiqueta canónica). "
          "Esperado por la unificación; confirmar que cada fusión es correcta:")
    fusiones = {lbl: items for lbl, items in by_label.items() if len(items) >= 2}
    if not fusiones:
        print("    (ninguna fusión: cada etiqueta proviene de una sola clave cruda)")
    else:
        for lbl in sorted(fusiones, key=lambda L: -sum(n for _, n in fusiones[L])):
            items = sorted(fusiones[lbl], key=lambda x: -x[1])
            tot = sum(n for _, n in items)
            crudas = ", ".join(f"{r} ({n})" for r, n in items)
            print(f"    • {lbl}  [{tot} ev]")
            print(f"        ← {crudas}")

    # ── 3) Resumen de salud ──────────────────────────────────────────────────
    real = totals["REAL"]
    otros = totals["OTROS"]
    sinclas = totals["SIN_CLASIF"]
    unclassified = otros + sinclas

    def pct(x):
        return (100.0 * x / total_rows) if total_rows else 0.0

    print("\n" + "═" * 100)
    print("3) RESUMEN DE SALUD DEL RE-MAPEO")
    print("═" * 100)
    print(f"  Total de eventos                 : {total_rows}")
    print(f"  Claves crudas distintas          : {len(rows)}")
    print(f"  Con etiqueta canónica REAL       : {real:>7}   ({pct(real):5.1f} %)")
    print(f"  En 'Otros' (marcador nulo)       : {otros:>7}   ({pct(otros):5.1f} %)")
    print(f"  Sin clasificar (sin alias)       : {sinclas:>7}   ({pct(sinclas):5.1f} %)")
    print(f"  ── Total Otros/Sin clasificar    : {unclassified:>7}   ({pct(unclassified):5.1f} %)  ◀ número clave de salud")
    print("═" * 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
