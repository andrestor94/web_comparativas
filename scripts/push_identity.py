"""Push de la identidad de clientes YA RESUELTA (local) a producción — SÍNCRONO.

Enfoque definitivo: la identidad es un DATO, no un cálculo del server. Este comando la
resuelve en tu máquina (sobre la base local, mismo dataset que prod) y la empuja a prod,
que SOLO la aplica. Es síncrono: ves el progreso y el error si lo hay, en tu terminal.
Cero tareas en background.

Uso (una sola línea, mismo token que el push de datos):
    python -m scripts.push_identity --url https://TU-APP.onrender.com --token EL_TOKEN

Opcionales:
    --local-run 7     corrida local a resolver (default: última success local)
    --remote-run 67   corrida de prod a la que aplicar (default: última success de prod)
    --exclude-test    excluir filas de prueba en la resolución
    --dry-run         resolver y mostrar el resumen SIN enviar nada

Payload chico (~40KB): registry + cuit_map + ori_map en un solo request.
"""
from __future__ import annotations

import argparse
import os
import sys

# Consola Windows (cp1252) no puede imprimir emojis: forzamos UTF-8 para no crashear.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import requests

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except Exception:
    pass

from web_comparativas.models import SessionLocal
from web_comparativas.dimensionamiento.identity import (
    resolve_entities,
    serialize_identity,
    latest_success_run_id,
)

CHUNK_PATH = "/api/mercado-privado/dimensiones/admin/apply-identity-chunk"
ESTADO_PATH = "/api/mercado-privado/dimensiones/admin/estado-identidad"


def _batches(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _post_chunk(base, headers, payload, timeout):
    r = requests.post(base + CHUNK_PATH, json=payload, headers=headers, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
    return r.json()


def main() -> int:
    ap = argparse.ArgumentParser(description="Push de identidad de clientes resuelta a prod (troceado, síncrono)")
    ap.add_argument("--url", required=True, help="URL base de prod (ej: https://tu-app.onrender.com)")
    ap.add_argument("--token", default=None, help="X-Import-Token. Si se omite, se lee DIMENSIONAMIENTO_IMPORT_TOKEN del entorno/.env")
    ap.add_argument("--local-run", type=int, default=None, help="corrida local a resolver (default: última success)")
    ap.add_argument("--remote-run", type=int, default=None, help="corrida de prod a la que aplicar (default: última success de prod)")
    ap.add_argument("--exclude-test", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="resolver y mostrar, sin enviar")
    ap.add_argument("--records", action="store_true", help="FASE 2: además poblar records por lotes (no hace falta para ver el número)")
    ap.add_argument("--summary-batch", type=int, default=10, help="cliente_visible por lote de summary (FASE 1)")
    ap.add_argument("--records-batch", type=int, default=8, help="claves por lote de records (FASE 2)")
    ap.add_argument("--timeout", type=int, default=120, help="timeout por lote, en segundos")
    args = ap.parse_args()

    token = args.token or os.getenv("DIMENSIONAMIENTO_IMPORT_TOKEN")
    args.token = token
    base = args.url.rstrip("/")

    # 1) Resolver LOCAL
    session = SessionLocal()
    try:
        local_run = args.local_run or latest_success_run_id(session)
        if local_run is None:
            print("❌ No hay corrida success en la base local.")
            return 2
        print(f"🔎 Resolviendo identidad local (run {local_run}, exclude_test={args.exclude_test})...", flush=True)
        result = resolve_entities(session, local_run, exclude_test=args.exclude_test)
        identity = serialize_identity(result)
    finally:
        session.close()

    st = result.stats
    print(f"   → {st['total']} entidades ({st['si']} clientes · {st['no']} no clientes), "
          f"visible_map={len(identity['visible_map'])}, cuit_map={len(identity['cuit_map'])}, "
          f"ori_map={len(identity['ori_map'])}, ambiguas={st['ambiguas']}, filas={st['filas']}", flush=True)
    if result.ambiguous:
        print(f"⚠️  {len(result.ambiguous)} casos ambiguos: {result.ambiguous[:5]}")
    if args.dry_run:
        print("🟡 --dry-run: no se envió nada.")
        return 0
    if not args.token:
        print("❌ Falta el token. Pasá --token o definí DIMENSIONAMIENTO_IMPORT_TOKEN en el .env (KEY=valor).")
        return 2

    headers = {"X-Import-Token": args.token, "Content-Type": "application/json"}
    rr = args.remote_run
    base_payload = {"run_id": rr}

    def chunk(**extra):
        return _post_chunk(base, headers, {**base_payload, **extra}, args.timeout)

    try:
        # ── FASE 1: registry + summary (lo que hace visible el número) ──
        print(f"\n[FASE 1] registry ({len(identity['registry'])} entidades)...", flush=True)
        d = chunk(kind="registry", registry=identity["registry"])
        print(f"   ✅ registry aplicado: {d.get('registry')} filas (run {d.get('run_id')})", flush=True)

        vis = identity["visible_map"]
        vb = list(_batches(vis, args.summary_batch))
        print(f"[FASE 1] summary: {len(vis)} cliente_visible en {len(vb)} lotes de {args.summary_batch}...", flush=True)
        for i, b in enumerate(vb, 1):
            d = chunk(kind="summary", rows=b)
            print(f"   lote {i}/{len(vb)}: +{d.get('updated')} filas de summary  (quedan NULL: {d.get('summary_null')})", flush=True)

        # ── FASE 2 (opcional): records por lotes ──
        if args.records:
            cb = list(_batches(identity["cuit_map"], args.records_batch))
            print(f"\n[FASE 2] records por CUIT: {len(identity['cuit_map'])} en {len(cb)} lotes...", flush=True)
            for i, b in enumerate(cb, 1):
                d = chunk(kind="records-cuit", pairs=b)
                print(f"   cuit lote {i}/{len(cb)}: records NULL restantes: {d.get('records_null')}", flush=True)
            ob = list(_batches(identity["ori_map"], args.records_batch))
            print(f"[FASE 2] records por nombre: {len(identity['ori_map'])} en {len(ob)} lotes...", flush=True)
            for i, b in enumerate(ob, 1):
                d = chunk(kind="records-ori", pairs=b)
                print(f"   ori lote {i}/{len(ob)}: records NULL restantes: {d.get('records_null')}", flush=True)

        # ── finalize: refrescar snapshot + estado ──
        print("\n[finalize] refrescando snapshot...", flush=True)
        d = chunk(kind="finalize")
        sn, rn = d.get("summary_null"), d.get("records_null")
        print("✅ ========================================================")
        print(f"✅ FASE 1 COMPLETA (run {d.get('run_id')}). summary_null={sn}  records_null={rn}")
        if sn:
            print(f"⚠️   Quedan {sn} filas de summary sin identidad: prod tiene cliente_visible que el")
            print("⚠️   mapeo local no cubre (dataset distinto). La card cuenta solo lo cubierto.")
        else:
            print("✅   summary cubierto al 100%. La card debería mostrar el número resuelto.")
        if not args.records and rn:
            print(f"ℹ️   records sigue sin poblar ({rn} NULL) — es FASE 2, no hace falta para el número.")
        print("✅ ========================================================")
    except (requests.exceptions.RequestException, RuntimeError) as e:
        print(f"\n❌ Cortó un lote: {e}")
        print("   Nada se perdió: cada lote commitea solo. Volvé a correr el MISMO comando y")
        print("   continúa donde quedó (los lotes ya aplicados se saltean, tocan 0 filas).")
        return 3

    print(f"\n   Estado: curl -s \"{base}{ESTADO_PATH}\" -H \"X-Import-Token: <TOKEN>\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
