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
import sys

import requests

from web_comparativas.models import SessionLocal
from web_comparativas.dimensionamiento.identity import (
    resolve_entities,
    serialize_identity,
    latest_success_run_id,
)

APPLY_PATH = "/api/mercado-privado/dimensiones/admin/apply-identity"
ESTADO_PATH = "/api/mercado-privado/dimensiones/admin/estado-identidad"


def main() -> int:
    ap = argparse.ArgumentParser(description="Push de identidad de clientes resuelta a prod (síncrono)")
    ap.add_argument("--url", required=True, help="URL base de prod (ej: https://tu-app.onrender.com)")
    ap.add_argument("--token", required=True, help="X-Import-Token (el mismo del push de datos)")
    ap.add_argument("--local-run", type=int, default=None, help="corrida local a resolver (default: última success)")
    ap.add_argument("--remote-run", type=int, default=None, help="corrida de prod a la que aplicar (default: última success de prod)")
    ap.add_argument("--exclude-test", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="resolver y mostrar, sin enviar")
    ap.add_argument("--timeout", type=int, default=600, help="timeout del request en segundos")
    args = ap.parse_args()

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
          f"cuit_map={len(identity['cuit_map'])}, ori_map={len(identity['ori_map'])}, "
          f"ambiguas={st['ambiguas']}, filas cubiertas={st['filas']}", flush=True)
    if result.ambiguous:
        print(f"⚠️  {len(result.ambiguous)} casos ambiguos en la resolución: {result.ambiguous[:5]}")

    if args.dry_run:
        print("🟡 --dry-run: no se envió nada.")
        return 0

    # 2) Aplicar en PROD (síncrono)
    payload = {
        "run_id": args.remote_run,  # None → el server usa su última corrida success
        "registry": identity["registry"],
        "cuit_map": identity["cuit_map"],
        "ori_map": identity["ori_map"],
    }
    headers = {"X-Import-Token": args.token, "Content-Type": "application/json"}
    print(f"⬆️  Enviando a {base}{APPLY_PATH} (run remoto: {args.remote_run or 'auto'})... "
          f"esto aplica las 2 UPDATE...FROM en el server, puede tardar ~1 min.", flush=True)
    try:
        r = requests.post(base + APPLY_PATH, json=payload, headers=headers, timeout=args.timeout)
    except requests.exceptions.RequestException as e:
        print(f"❌ Falló el request: {e}")
        return 3

    if r.status_code != 200:
        print(f"❌ El server respondió HTTP {r.status_code}: {r.text[:500]}")
        return 4

    data = r.json()
    print("\n✅ ========================================================")
    print(f"✅ IDENTIDAD APLICADA en prod (run {data.get('run_id')}).")
    print(f"✅   registry={data.get('registry')}  records_null={data.get('records_null')}  summary_null={data.get('summary_null')}")
    if data.get("records_null") or data.get("summary_null"):
        print("⚠️   Hay filas SIN cubrir: prod tiene cuit/nombres que el mapeo local no incluye")
        print("⚠️   (dataset distinto al local). La card va a contar solo lo cubierto.")
    else:
        print("✅   Cobertura total: 0 filas sin identidad.")
    print("✅ ========================================================")
    print(f"   Verificá el estado: curl -s \"{base}{ESTADO_PATH}\" -H \"X-Import-Token: <TOKEN>\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
