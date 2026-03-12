from __future__ import annotations

import json

from sqlalchemy import select, func

from web_comparativas.dimensionamiento.models import DimensionamientoRecord
from web_comparativas.models import IS_POSTGRES, IS_SQLITE, SessionLocal, engine


def main() -> None:
    session = SessionLocal()
    try:
        row_count = session.execute(
            select(func.count()).select_from(DimensionamientoRecord)
        ).scalar_one()
        payload = {
            "backend": engine.url.get_backend_name(),
            "is_postgres": IS_POSTGRES,
            "is_sqlite": IS_SQLITE,
            "host": getattr(engine.url, "host", None),
            "database": getattr(engine.url, "database", None),
            "table": DimensionamientoRecord.__tablename__,
            "row_count": row_count,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    finally:
        session.close()


if __name__ == "__main__":
    main()
