"""Push de `oportunidades_summary` desde local a producción — SÍNCRONO, por chunks.

Mismo enfoque que `push_match_data.py`: en Render NUNCA se calcula nada pesado. Este
comando lee `oportunidades_summary` YA CALCULADA en la base LOCAL (correr antes
`scripts/rebuild_oportunidades.py --run-id <run local espejo del activo de prod>`) y la
empuja a prod por lotes chicos; prod SOLO aplica y commitea por lote.

A diferencia de Match, acá NO se crea una corrida nueva en prod: `oportunidades_summary`
cuelga del run de DIMENSIONAMIENTO que ya está vigente en prod (pusheado antes con el
mecanismo de Dimensionamiento). Este script solo reemplaza las filas de ESE run_id.

Reanudable de la forma más simple posible (son ~150 filas, un lote alcanza): el primer
lote resetea (borra) las filas previas del run_id destino en prod; si un lote corta a
mitad de camino, se reejecuta el MISMO comando (vuelve a resetear y reinserta todo —
no hay riesgo de duplicar ni de dejar una mezcla vieja/nueva).

Uso (una sola línea, mismo token que el push de Dimensionamiento/Match):
    python -m scripts.push_oportunidades_data --url https://TU-APP.onrender.com --token EL_TOKEN

Opcionales:
    --local-run-id 12     run LOCAL de Dimensionamiento a leer (default: run activo local)
    --target-run-id 12    run REMOTO (prod) al que quedan asociadas las filas
                           (default: el mismo que --local-run-id — deben ser el MISMO
                           run_id en ambos lados, o sea que el dataset ya viajó y está
                           vigente en prod con ese id exacto)
    --dry-run              mostrar conteos locales, sin enviar nada
    --batch 2000            filas por lote
    --timeout 180            timeout por lote, en segundos
"""
from __future__ import annotations

import argparse
import os
import sys

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

from sqlalchemy import select

from web_comparativas.models import SessionLocal
from web_comparativas.dimensionamiento.models import OportunidadSummary
from web_comparativas.dimensionamiento.query_service import _latest_success_import_run

CHUNK_PATH = "/api/mercado-privado/oportunidades/admin/apply-data-chunk"
ESTADO_PATH = "/api/mercado-privado/oportunidades/admin/estado"

# Columnas que viajan tal cual (todo lo de OportunidadSummary menos id/import_run_id/
# created_at, que los pone el servidor).
_CAMPOS = [
    "codigo_articulo", "cliente_visible", "cuit", "cuenta_interna", "provincia",
    "producto_nombre", "familia", "unidad_negocio", "subunidad_negocio", "plataforma",
    "tipo_oportunidad", "estado_actividad",
    "meses_demanda_cliente_12m", "meses_no_participo_12m", "ventana_meses",
    "consumo_tipico_mensual", "consumo_min_mensual", "consumo_max_mensual",
    "ultima_demanda", "meses_desde_ultima_demanda",
    "precio_unitario_estimado", "monto_oportunidad",
    "efectividad", "ganados", "comprado_otra", "en_espera", "clientes_distintos",
    "tipo_multiplicador", "multiplicador_actividad", "score",
]


def _batches(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _row_dict(o: OportunidadSummary) -> dict:
    d = {campo: getattr(o, campo) for campo in _CAMPOS}
    if d.get("ultima_demanda") is not None:
        d["ultima_demanda"] = d["ultima_demanda"].isoformat()
    return d


def _post_chunk(base, headers, payload, timeout):
    r = requests.post(base + CHUNK_PATH, json=payload, headers=headers, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
    return r.json()


def main() -> int:
    ap = argparse.ArgumentParser(description="Push de oportunidades_summary a prod (troceado, síncrono, reanudable)")
    ap.add_argument("--url", required=True, help="URL base de prod (ej: https://tu-app.onrender.com)")
    ap.add_argument("--token", default=None, help="X-Import-Token. Default: DIMENSIONAMIENTO_IMPORT_TOKEN del entorno/.env")
    ap.add_argument("--local-run-id", type=int, default=None, help="run LOCAL de Dimensionamiento a leer (default: run activo local)")
    ap.add_argument("--target-run-id", type=int, default=None, help="run REMOTO (prod) destino (default: igual a --local-run-id)")
    ap.add_argument("--dry-run", action="store_true", help="mostrar conteos locales, sin enviar")
    ap.add_argument("--batch", type=int, default=2000, help="filas por lote")
    ap.add_argument("--timeout", type=int, default=180, help="timeout por lote, en segundos")
    args = ap.parse_args()

    token = args.token or os.getenv("DIMENSIONAMIENTO_IMPORT_TOKEN")
    base = args.url.rstrip("/")

    # 1) Leer LOCAL (oportunidades_summary ya calculada — nada se recalcula acá).
    session = SessionLocal()
    try:
        run_id = args.local_run_id
        if run_id is None:
            latest = _latest_success_import_run(session)
            run_id = latest.id if latest else None
        if run_id is None:
            print("❌ No hay run activo de Dimensionamiento en la base local.")
            return 2

        filas = session.execute(
            select(OportunidadSummary).where(OportunidadSummary.import_run_id == run_id)
        ).scalars().all()
        if not filas:
            print(f"❌ oportunidades_summary está VACÍA para el run local {run_id}. "
                  f"Corré antes: python scripts/rebuild_oportunidades.py --run-id {run_id}")
            return 2

        rows = [_row_dict(o) for o in filas]
    finally:
        session.close()

    target_run_id = args.target_run_id if args.target_run_id is not None else run_id
    nb = args.batch
    print(f"🔎 Run local {run_id} -> run destino en prod {target_run_id}: "
          f"{len(rows)} filas ({(len(rows)+nb-1)//nb} lote(s) de {nb})", flush=True)
    if args.dry_run:
        print("🟡 --dry-run: no se envió nada.")
        return 0
    if not token:
        print("❌ Falta el token. Pasá --token o definí DIMENSIONAMIENTO_IMPORT_TOKEN en el .env (KEY=valor).")
        return 2

    headers = {"X-Import-Token": token, "Content-Type": "application/json"}

    def chunk(**payload):
        return _post_chunk(base, headers, payload, args.timeout)

    try:
        lotes = list(_batches(rows, nb))
        for i, b in enumerate(lotes, 1):
            d = chunk(kind="oportunidades-summary", run_id=target_run_id, rows=b, reset=(i == 1))
            print(f"   lote {i}/{len(lotes)}: +{d.get('insertadas')} (total remoto run {target_run_id}: {d.get('total')})", flush=True)

        print("✅ ========================================================")
        print(f"✅ PUSH COMPLETO: run {target_run_id} en prod tiene {lotes and d.get('total')} filas "
              f"(local esperaba {len(rows)}).")
        if d.get("total") != len(rows):
            print(f"⚠️  El conteo NO coincide ({d.get('total')} vs {len(rows)} esperadas). "
                  f"Reintentá el mismo comando (resetea y reinserta todo).")
            return 3
        print("✅ ========================================================")
    except (requests.exceptions.RequestException, RuntimeError) as e:
        print(f"\n❌ Cortó un lote: {e}")
        print("   Reintentá el MISMO comando: el primer lote resetea y reinserta todo, no queda a medias.")
        return 3

    print(f"\n   Estado: curl.exe -s \"{base}{ESTADO_PATH}\" -H \"X-Import-Token: <TOKEN>\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
