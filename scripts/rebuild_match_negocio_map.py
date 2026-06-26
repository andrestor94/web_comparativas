"""Regenera el mapa compacto `match_negocio_map` (código → negocio/subnegocio) desde
`dimensionamiento_records`. Correr con el SERVER BAJADO (escribe en app.db).

Uso (desde la raíz, con el venv activado):
    python scripts/rebuild_match_negocio_map.py

Lee SOLO `dimensionamiento_records` (no toca sus índices ni datos). Vacía y repuebla
`match_negocio_map`. No toca match_propuestas ni match_import_runs.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import delete, func, select  # noqa: E402

from web_comparativas.match.models import MatchNegocioMap  # noqa: E402
from web_comparativas.match.service import ensure_negocio_map  # noqa: E402
from web_comparativas.models import SessionLocal  # noqa: E402


def main() -> None:
    db = SessionLocal()
    try:
        antes = int(db.execute(select(func.count(MatchNegocioMap.codigo))).scalar_one() or 0)
        # Vaciar para forzar el repoblado completo (ensure rellena si está vacía).
        db.execute(delete(MatchNegocioMap))
        db.commit()
        res = ensure_negocio_map(db)
        total = int(db.execute(select(func.count(MatchNegocioMap.codigo))).scalar_one() or 0)
        negs = db.execute(select(func.count(func.distinct(MatchNegocioMap.negocio)))).scalar_one()
        print(f"[REBUILD] match_negocio_map: antes={antes} -> ahora={total} (negocios distintos={negs})")
        print(f"[REBUILD] filled={res['filled']}")
        print("[REBUILD] OK")
    finally:
        db.close()


if __name__ == "__main__":
    main()
