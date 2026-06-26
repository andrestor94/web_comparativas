"""Regenera el resumen compacto `match_demanda_desc` (descripción de portal normalizada
→ renglones/clientes) desde `dimensionamiento_records`. Correr con el SERVER BAJADO
(escribe en app.db).

Uso (desde la raíz, con el venv activado):
    python scripts/rebuild_match_demanda_desc.py

Lee SOLO `dimensionamiento_records` (no toca sus índices ni datos). Vacía y repuebla
`match_demanda_desc`. No toca match_propuestas ni match_import_runs.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import delete, func, select  # noqa: E402

from web_comparativas.match.models import MatchDemandaDesc  # noqa: E402
from web_comparativas.match.service import ensure_match_demanda_desc  # noqa: E402
from web_comparativas.models import SessionLocal  # noqa: E402


def main() -> None:
    db = SessionLocal()
    try:
        antes = int(db.execute(select(func.count(MatchDemandaDesc.desc_norm))).scalar_one() or 0)
        db.execute(delete(MatchDemandaDesc))  # vaciar → ensure repuebla
        db.commit()
        res = ensure_match_demanda_desc(db)
        total = int(db.execute(select(func.count(MatchDemandaDesc.desc_norm))).scalar_one() or 0)
        print(f"[REBUILD] match_demanda_desc: antes={antes} -> ahora={total} (filled={res['filled']})")
        print("[REBUILD] OK")
    finally:
        db.close()


if __name__ == "__main__":
    main()
