"""DRY-RUN de la resolución de identidad contra un CSV nuevo, SIN escribir en la base.

Corre EXACTAMENTE el mismo mapeo de columnas (ingestion._normalize_row) y el mismo
algoritmo de resolución (identity.resolve_entities) que la ingesta real, pero sobre una
base SQLite en MEMORIA. No toca app.db ni el dataset actual. Pensado para correr ANTES de
ingestar un dataset nuevo y detectar sorpresas leyendo un reporte, no viendo explotar la
ingesta (el assert 1:1 ahora CORTA la ejecución).

Uso:
    python -m scripts.dryrun_dim_dataset --csv-path "ruta\\al\\dataset_nuevo.csv"
    python -m scripts.dryrun_dim_dataset --csv-path "..." --exclude-test
    python -m scripts.dryrun_dim_dataset --csv-path "..." --current-run 7

Reporta:
  - total de entidades, Sí y No
  - entidades NUEVAS respecto del dataset actual (registry de --current-run)
  - entidades que pasaron de No→Sí (se homologaron) y cuáles
  - entidades que pasaron de Sí→No (ALARMA) y cuáles
  - si el invariante 1:1 se rompe y en qué casos exactos (el resolvedor lanza ValueError)
  - casos ambiguos del paso 2 (hoy 0; si aparecen, cuáles)
  - homologados truncados (30/40 chars) y colisiones de prefijo con CUIT distinto
  - filas de prueba nuevas (patrón prueba/test/demo)

El emparejamiento nuevo↔actual es por CUIT compartido; si no hay CUIT, por nombre canónico.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from web_comparativas.dimensionamiento.ingestion import (
    _clean_header,
    _iter_csv_chunks,
    _normalize_row,
    _resolve_csv_read_config,
)
from web_comparativas.dimensionamiento.identity import canon, resolve_entities
from web_comparativas.models import SessionLocal

DRY_RUN_ID = 999
_TEST_PATTERNS = ("prueba", "test", "demo")


def _load_csv_into_memory(csv_path: Path, chunk_size: int) -> Session:
    """Lee el CSV con el MISMO pipeline que la ingesta y lo carga en una DB en memoria."""
    delimiter, observed, sel_orig, dtype_map = _resolve_csv_read_config(csv_path)
    engine = create_engine("sqlite://")  # :memory:
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE dimensionamiento_records ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, import_run_id INTEGER, "
            "cuit TEXT, cliente_nombre_homologado TEXT, cliente_nombre_original TEXT, "
            "cliente_visible TEXT, provincia TEXT)"
        ))
    mem = Session(bind=engine)
    total = 0
    for chunk in _iter_csv_chunks(csv_path, chunk_size, delimiter=delimiter, usecols=sel_orig, dtype_map=dtype_map):
        chunk.columns = [_clean_header(c) for c in chunk.columns]
        rows = []
        for raw in chunk.to_dict("records"):
            rec = _normalize_row(raw)
            rows.append({
                "run": DRY_RUN_ID,
                "cuit": rec.get("cuit"),
                "hom": rec.get("cliente_nombre_homologado"),
                "ori": rec.get("cliente_nombre_original"),
                "vis": rec.get("cliente_visible"),
                "prov": rec.get("provincia"),
            })
        if rows:
            mem.execute(text(
                "INSERT INTO dimensionamiento_records "
                "(import_run_id, cuit, cliente_nombre_homologado, cliente_nombre_original, cliente_visible, provincia) "
                "VALUES (:run, :cuit, :hom, :ori, :vis, :prov)"
            ), rows)
            total += len(rows)
    mem.commit()
    print(f"[DRY-RUN] CSV leído: {total} filas cargadas en memoria (run {DRY_RUN_ID}).")
    return mem


def _current_registry(current_run: int) -> dict[str, dict]:
    """Registry del dataset ACTUAL (real DB, read-only) indexado por entidad."""
    s = SessionLocal()
    try:
        rows = s.execute(text(
            "SELECT entidad_key, es_cliente, nombre_visible, cuits FROM dimensionamiento_cliente_entidad "
            "WHERE import_run_id = :run"
        ), {"run": current_run}).all()
    finally:
        s.close()
    reg = {}
    for key, es_cli, vis, cuits in rows:
        try:
            cuit_list = json.loads(cuits) if cuits else []
        except (ValueError, TypeError):
            cuit_list = []
        reg[key] = {"es_cliente": bool(es_cli), "nombre": vis or "", "cuits": cuit_list, "canon": canon(vis or "")}
    return reg


def _match_current(new_ent, cur_by_cuit, cur_by_canon):
    for cu in new_ent.cuits:
        if cu in cur_by_cuit:
            return cur_by_cuit[cu]
    return cur_by_canon.get(canon(new_ent.nombre_visible))


def main() -> int:
    ap = argparse.ArgumentParser(description="Dry-run de identidad contra un CSV nuevo (no escribe en la base)")
    ap.add_argument("--csv-path", required=True, help="Ruta al CSV nuevo (SIEMPRE explícita)")
    ap.add_argument("--current-run", type=int, default=None, help="import_run_id del dataset actual para comparar (default: último success)")
    ap.add_argument("--exclude-test", action="store_true", help="excluir filas de prueba en la resolución")
    ap.add_argument("--chunk-size", type=int, default=50000)
    args = ap.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"[DRY-RUN] ERROR: no existe el archivo {csv_path}")
        return 2

    # dataset actual para comparar
    if args.current_run is None:
        s = SessionLocal()
        try:
            args.current_run = s.execute(text(
                "SELECT id FROM dimensionamiento_import_runs WHERE status='success' ORDER BY finished_at DESC, id DESC LIMIT 1"
            )).scalar_one_or_none()
        finally:
            s.close()
    current = _current_registry(args.current_run) if args.current_run else {}
    cur_by_cuit = {cu: e for e in current.values() for cu in e["cuits"]}
    cur_by_canon = {e["canon"]: e for e in current.values()}
    print(f"[DRY-RUN] Comparando contra dataset actual run={args.current_run} ({len(current)} entidades).")

    mem = _load_csv_into_memory(csv_path, args.chunk_size)

    # ── Resolución (mismo algoritmo que la ingesta). El assert 1:1 corta acá si se rompe.
    try:
        result = resolve_entities(mem, DRY_RUN_ID, exclude_test=args.exclude_test)
    except ValueError as e:
        print("\n██ INVARIANTE 1:1 ROTO — la ingesta REAL fallaría con este CSV ██")
        print(str(e))
        print("\nCorregir en el origen antes de ingestar. Dry-run abortado.")
        return 3

    ents = result.entities
    print("\n================ REPORTE DRY-RUN ================")
    print(f"Total entidades : {result.stats['total']}   Sí: {result.stats['si']}   No: {result.stats['no']}")
    print(f"  anclas(CUIT)={result.stats['anclas']}  adjuntadas={result.stats['adjuntadas']}  "
          f"propias={result.stats['propias']}  ambiguas={result.stats['ambiguas']}  filas={result.stats['filas']}")

    # nuevas / cambios de estado
    nuevas, no_a_si, si_a_no = [], [], []
    for e in ents:
        m = _match_current(e, cur_by_cuit, cur_by_canon)
        if m is None:
            nuevas.append(e)
        elif (not m["es_cliente"]) and e.es_cliente:
            no_a_si.append((e, m))
        elif m["es_cliente"] and (not e.es_cliente):
            si_a_no.append((e, m))

    print(f"\nEntidades NUEVAS (no estaban en run {args.current_run}): {len(nuevas)}")
    for e in nuevas[:60]:
        print(f"   + [{'Sí' if e.es_cliente else 'No'}] {e.nombre_visible!r} cuits={e.cuits} filas={e.total_registros}")
    if len(nuevas) > 60:
        print(f"   ... (+{len(nuevas)-60} más)")

    print(f"\nEntidades que pasaron de NO → SÍ (se homologaron): {len(no_a_si)}")
    for e, m in no_a_si:
        print(f"   ↑ {m['nombre']!r} → {e.nombre_visible!r} cuits={e.cuits}")

    print(f"\n⚠️  Entidades que pasaron de SÍ → NO (ALARMA, revisar): {len(si_a_no)}")
    for e, m in si_a_no:
        print(f"   ↓ {m['nombre']!r} → {e.nombre_visible!r} cuits={e.cuits}")

    # ambiguos
    print(f"\nCasos AMBIGUOS en el paso 2 (hoy 0): {len(result.ambiguous)}")
    for a in result.ambiguous[:30]:
        print(f"   ? {a}")

    # truncados + colisiones de prefijo
    homs = sorted({e.nombre_visible for e in ents if e.es_cliente})
    trunc40 = [h for h in homs if len(h) == 40]
    trunc30 = [h for h in homs if len(h) == 30]
    print(f"\nHomologados truncados: {len(trunc40)} de 40 chars, {len(trunc30)} de 30 chars")
    prefix_hits = []
    for e in ents:
        na = e.nombre_visible
        if not e.es_cliente or len(na) not in (30, 40):
            continue
        for e2 in ents:
            if e2.entidad_key != e.entidad_key and e2.es_cliente and e2.nombre_visible.startswith(na) and len(e2.nombre_visible) > len(na):
                same_cuit = bool(set(e.cuits) & set(e2.cuits))
                prefix_hits.append((na, e2.nombre_visible, same_cuit))
    print(f"Colisiones de prefijo (truncado es prefijo de otro): {len(prefix_hits)}")
    for na, nb, same in prefix_hits[:30]:
        print(f"   {'(mismo CUIT→fusiona)' if same else '(CUIT distinto→separados)'} {na!r}  ⊂  {nb!r}")

    # encoding '#'
    hashed = [e.nombre_visible for e in ents if "#" in e.nombre_visible]
    print(f"\nNombres con '#' (posible Ñ corrupta): {len(hashed)}")
    for h in hashed[:20]:
        print(f"   {h!r}")

    # filas de prueba nuevas
    test_hits = defaultdict(int)
    rows = mem.execute(text(
        "SELECT cliente_nombre_original, COUNT(*) FROM dimensionamiento_records "
        "WHERE import_run_id=:r GROUP BY cliente_nombre_original"
    ), {"r": DRY_RUN_ID}).all()
    for ori, n in rows:
        low = (ori or "").lower()
        if any(p in low for p in _TEST_PATTERNS):
            test_hits[ori] = n
    print(f"\nOriginales con patrón prueba/test/demo: {len(test_hits)}")
    for name, n in list(test_hits.items())[:30]:
        print(f"   {name!r}  ({n} filas)")

    print("\n================ FIN DRY-RUN (no se escribió nada) ================")
    mem.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
