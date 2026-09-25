"""Regenera `match_monodroga_map` (código → monodroga) para los artículos de la corrida
vigente de Match, desde Fusion (`dbo.vsl_art_alfabeta_full.monodroga`, catálogo
Alfabeta; la misma vista que usa Indicadores). Correr LOCAL (Fusion no es accesible desde
Render) y regenerar después de cada import de Match.

Uso (desde la raíz):
    .\\.venv\\Scripts\\python.exe scripts/rebuild_match_monodroga_map.py [--dry-run]

Fusion: solo SELECT, en lotes de 1000 códigos. Guarda SOLO los códigos con monodroga
cargada (no completa con otra fuente). Vacía y repuebla `match_monodroga_map`; no toca
propuestas, homologaciones ni los otros mapas.

La columna es varchar (Modern_Spanish_CI_AS = cp1252) y el puente PowerShell de Fusion
rompe los acentos al pasar por stdout ('codeína' llega como 'code�na'): por eso se pide
en hexadecimal y se decodifica acá.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import delete, func, select  # noqa: E402

from web_comparativas.indicadores_db import get_fusion_db  # noqa: E402
from web_comparativas.match.models import MatchMonodrogaMap, MatchPropuesta  # noqa: E402
from web_comparativas.match.service import latest_approved_run  # noqa: E402
from web_comparativas.models import SessionLocal, engine  # noqa: E402

CHUNK = 1000

QUERY = """
SELECT f.codigo AS codigo,
       CONVERT(varchar(max), CAST(f.monodroga AS varbinary(max)), 2) AS monodroga_hex
FROM dbo.vsl_art_alfabeta_full f
WHERE f.codigo IN ({placeholders})
"""


def _decode(hexstr) -> str:
    if not hexstr:
        return ""
    return bytes.fromhex(str(hexstr)).decode("cp1252", errors="replace").strip()


def fetch_monodrogas(codigos: list[int]) -> dict[str, str]:
    out: dict[str, str] = {}
    with get_fusion_db() as conn:
        cur = conn.cursor()
        for i in range(0, len(codigos), CHUNK):
            chunk = codigos[i:i + CHUNK]
            cur.execute(QUERY.format(placeholders=",".join("?" for _ in chunk)), chunk)
            for codigo, hx in cur.fetchall():
                mono = _decode(hx)
                if mono:
                    out[str(codigo).strip()] = mono
            print(f"[MONODROGA] lote {i // CHUNK + 1}: {len(chunk)} códigos -> {len(out)} con monodroga", flush=True)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="Consulta Fusion y reporta, sin escribir.")
    args = ap.parse_args()

    MatchMonodrogaMap.__table__.create(bind=engine, checkfirst=True)
    db = SessionLocal()
    try:
        run = latest_approved_run(db)
        if run is None:
            print("[MONODROGA] No hay corrida aprobada de Match. Nada que hacer.")
            return
        codigos = sorted({
            str(c).strip()
            for c in db.execute(
                select(MatchPropuesta.candidato_codigo)
                .where(MatchPropuesta.import_run_id == run.id, MatchPropuesta.candidato_codigo.isnot(None))
                .distinct()
            ).scalars()
            if str(c).strip()
        })
        numericos = [int(c) for c in codigos if c.isdigit()]
        print(f"[MONODROGA] corrida {run.id}: {len(codigos)} artículos ({len(numericos)} con código numérico)")

        mapa = fetch_monodrogas(numericos)
        pct = (len(mapa) * 100.0 / len(codigos)) if codigos else 0.0
        print(f"[MONODROGA] con monodroga: {len(mapa)} de {len(codigos)} ({pct:.1f}%), "
              f"{len(set(mapa.values()))} distintas")
        print("[MONODROGA] más frecuentes:", Counter(mapa.values()).most_common(8))
        if args.dry_run:
            print("[MONODROGA] --dry-run: no se escribió nada.")
            return

        antes = int(db.execute(select(func.count(MatchMonodrogaMap.codigo))).scalar_one() or 0)
        db.execute(delete(MatchMonodrogaMap))
        db.bulk_insert_mappings(
            MatchMonodrogaMap, [{"codigo": c, "monodroga": m} for c, m in sorted(mapa.items())]
        )
        db.commit()
        ahora = int(db.execute(select(func.count(MatchMonodrogaMap.codigo))).scalar_one() or 0)
        print(f"[MONODROGA] match_monodroga_map: antes={antes} -> ahora={ahora}. OK")
    finally:
        db.close()


if __name__ == "__main__":
    main()
