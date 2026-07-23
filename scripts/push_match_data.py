"""Push de los DATOS del módulo Match desde local a producción — SÍNCRONO.

Mismo enfoque que push_identity: en Render NUNCA se calcula nada pesado. Este comando
lee la base LOCAL (propuestas ya importadas del Excel + tablas precalculadas
match_negocio_map / match_demanda_desc, que se computan acá si faltan) y empuja todo a
prod por lotes chicos; prod SOLO aplica y commitea por lote. Reanudable: si un lote
corta, se reejecuta con --resume-run y los inserts idempotentes saltean lo ya aplicado.

Uso (una sola línea, mismo token que el push de Dimensionamiento):
    python -m scripts.push_match_data --url https://TU-APP.onrender.com --token EL_TOKEN

Opcionales:
    --local-run 1      corrida local de Match a subir (default: última approved local)
    --resume-run 3     corrida REMOTA ya creada, para reanudar un push cortado
    --dry-run          mostrar conteos locales sin enviar nada
    --batch 2000       filas por lote
    --timeout 180      timeout por lote, en segundos

La corrida remota nace pending_approval y SOLO se aprueba en el finalize, cuando el
conteo de prod coincide con el local (verificación server-side): un push a medias
nunca queda como corrida vigente.
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

from sqlalchemy import func, select

from web_comparativas.models import SessionLocal
from web_comparativas.match.models import (
    MATCH_RUN_APPROVED,
    MatchDemandaDesc,
    MatchImportRun,
    MatchNegocioMap,
    MatchPropuesta,
)
from web_comparativas.match.service import ensure_match_demanda_desc, ensure_negocio_map

CHUNK_PATH = "/api/mercado-privado/match/admin/apply-data-chunk"
ESTADO_PATH = "/api/mercado-privado/match/admin/estado"


def _batches(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _post_chunk(base, headers, payload, timeout):
    r = requests.post(base + CHUNK_PATH, json=payload, headers=headers, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
    return r.json()


def main() -> int:
    ap = argparse.ArgumentParser(description="Push de datos de Match a prod (troceado, síncrono, reanudable)")
    ap.add_argument("--url", required=True, help="URL base de prod (ej: https://tu-app.onrender.com)")
    ap.add_argument("--token", default=None, help="X-Import-Token. Default: DIMENSIONAMIENTO_IMPORT_TOKEN del entorno/.env")
    ap.add_argument("--local-run", type=int, default=None, help="corrida local de Match (default: última approved)")
    ap.add_argument("--resume-run", type=int, default=None, help="corrida REMOTA existente para reanudar un push cortado")
    ap.add_argument("--dry-run", action="store_true", help="mostrar conteos locales, sin enviar")
    ap.add_argument("--batch", type=int, default=2000, help="filas por lote")
    ap.add_argument("--timeout", type=int, default=180, help="timeout por lote, en segundos")
    args = ap.parse_args()

    token = args.token or os.getenv("DIMENSIONAMIENTO_IMPORT_TOKEN")
    base = args.url.rstrip("/")

    # 1) Leer LOCAL (y precalcular las tablas chicas si faltan — SOLO local).
    session = SessionLocal()
    try:
        if args.local_run is not None:
            run = session.get(MatchImportRun, args.local_run)
        else:
            run = session.execute(
                select(MatchImportRun)
                .where(MatchImportRun.status == MATCH_RUN_APPROVED)
                .order_by(MatchImportRun.finished_at.desc(), MatchImportRun.id.desc())
                .limit(1)
            ).scalar_one_or_none()
        if run is None:
            print("❌ No hay corrida de Match approved en la base local (correr scripts/import_match_propuestas.py).")
            return 2

        print(f"🔎 Corrida local {run.id} ({run.status}), precalculando tablas chicas si faltan...", flush=True)
        nm = ensure_negocio_map(session)
        dd = ensure_match_demanda_desc(session)

        P = MatchPropuesta
        propuestas = [
            {
                "producto_plataforma": r.producto_plataforma,
                "nivel_confianza": r.nivel_confianza,
                "score_mejor": r.score_mejor,
                "candidato_codigo": r.candidato_codigo,
                "candidato_descripcion": r.candidato_descripcion,
                "score_tfidf": r.score_tfidf,
                "score_fuzzy": r.score_fuzzy,
                "score_pharma": r.score_pharma,
            }
            for r in session.execute(
                select(P).where(P.import_run_id == run.id).order_by(P.id.asc())
            ).scalars()
        ]
        negocio_rows = [
            [m.codigo, m.negocio, m.subnegocio]
            for m in session.execute(select(MatchNegocioMap).order_by(MatchNegocioMap.codigo)).scalars()
        ]
        demanda_rows = [
            [d.desc_norm, d.renglones, d.clientes]
            for d in session.execute(select(MatchDemandaDesc).order_by(MatchDemandaDesc.desc_norm)).scalars()
        ]
        # Conteo por nivel para el metadata del run remoto (referencia en la UI).
        counts_by_nivel = {
            (n or "?"): int(c)
            for n, c in session.execute(
                select(P.nivel_confianza, func.count(P.id))
                .where(P.import_run_id == run.id)
                .group_by(P.nivel_confianza)
            ).all()
        }
    finally:
        session.close()

    nb = args.batch
    print(f"   → propuestas={len(propuestas)} ({(len(propuestas)+nb-1)//nb} lotes de {nb}), "
          f"negocio_map={len(negocio_rows)}, demanda_desc={len(demanda_rows)} "
          f"(precalc local: negocio_map total={nm['total']}, demanda total={dd['total']})", flush=True)
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
        # 2) start / resume
        d = chunk(kind="start", resume_run=args.resume_run,
                  source_path=f"push:local-run-{run.id}:{run.source_path}")
        remote_run = d["run_id"]
        print(f"\n[PUSH] corrida remota {remote_run} ({'reanudada' if d.get('resumed') else 'nueva, pending_approval'})", flush=True)

        # 3) propuestas por lotes (idempotente por UNIQUE del run remoto)
        lotes = list(_batches(propuestas, nb))
        for i, b in enumerate(lotes, 1):
            d = chunk(kind="propuestas", run_id=remote_run, rows=b)
            print(f"   propuestas lote {i}/{len(lotes)}: +{d.get('insertadas')} (total remoto: {d.get('total_run')})", flush=True)

        # 4) tablas precalculadas (reset SOLO en el primer lote de un push nuevo)
        reset = args.resume_run is None
        nlotes = list(_batches(negocio_rows, nb))
        for i, b in enumerate(nlotes, 1):
            d = chunk(kind="negocio-map", rows=b, reset=(reset and i == 1))
            print(f"   negocio-map lote {i}/{len(nlotes)}: total remoto {d.get('total')}", flush=True)
        dlotes = list(_batches(demanda_rows, nb))
        for i, b in enumerate(dlotes, 1):
            d = chunk(kind="demanda-desc", rows=b, reset=(reset and i == 1))
            print(f"   demanda-desc lote {i}/{len(dlotes)}: total remoto {d.get('total')}", flush=True)

        # 5) finalize: aprueba SOLO si el conteo remoto coincide con el local.
        d = chunk(kind="finalize", run_id=remote_run,
                  rows_esperadas=len(propuestas), counts_by_nivel=counts_by_nivel)
        print("✅ ========================================================")
        print(f"✅ PUSH COMPLETO: corrida remota {d.get('run_id')} APROBADA "
              f"({d.get('filas')} filas, {d.get('articulos')} artículos).")
        print("✅ ========================================================")
    except (requests.exceptions.RequestException, RuntimeError) as e:
        print(f"\n❌ Cortó un lote: {e}")
        print("   Nada se perdió: cada lote commitea solo y la corrida remota sigue pending_approval")
        print("   (no vigente). Reanudá con el MISMO comando agregando: --resume-run <run_remoto>")
        return 3

    print(f"\n   Estado: curl.exe -s \"{base}{ESTADO_PATH}\" -H \"X-Import-Token: <TOKEN>\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
