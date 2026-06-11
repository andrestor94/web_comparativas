"""ETL de la dimensión ind_articulos desde Fusion (camino dimensión, reemplazo total).

Conductora: dbo.articulos ⋈ dbo.vsl_art_alfabeta_full por codigo (solape 100%, codigo
único en ambas). `articulos` (BASE TABLE) aporta descrip/unineg; `vsl_art_alfabeta_full`
(VIEW) aporta marca/lab_nombre/familia/monodroga.

Carga SOLO el universo real: los artículos presentes en los 3 staging (no el catálogo
completo). El universo se arma desde SQLite local; los atributos se traen desde Fusion en
lotes (la lista es ~35k, se pasa en chunks con placeholders ?, nunca una IN gigante).

Escribe SOLO en ind_articulos_staging (reemplazo total: es dimensión, no incremental).
NO toca la tabla publicada.

Uso:
    python web_comparativas/indicadores_etl_articulos.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime

from sqlalchemy import text

from web_comparativas.indicadores_db import get_fusion_db

import web_comparativas.indicadores_summary_models  # noqa: F401
from web_comparativas.indicadores_summary_models import (
    IndArticulosStaging,
    IndEtlControl,
)
from web_comparativas.models import Base, engine, SessionLocal


CHUNK = 1000

UNIVERSO_SQL = """
SELECT DISTINCT articulo FROM (
    SELECT articulo FROM ind_rentabilidad_lineas_staging
    UNION SELECT articulo FROM ind_inflacion_facturacion_mensual_staging
    UNION SELECT articulo FROM ind_inflacion_pvp_mensual_staging
) u
"""

QUERY_ATRIBUTOS = """
SELECT
    a.codigo                       AS articulo,
    a.descrip                      AS descripcion,
    a.unineg                       AS unineg,
    f.marca                        AS marca,
    f.lab_nombre                   AS laboratorio,
    f.familia                      AS familia,
    f.monodroga                    AS principio_activo
FROM dbo.articulos a
LEFT JOIN dbo.vsl_art_alfabeta_full f ON f.codigo = a.codigo
WHERE a.codigo IN ({placeholders});
"""


def _norm(value):
    """LTRIM/RTRIM; '' o solo espacios -> None."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _to_int(value):
    return None if value is None else int(value)


def _rows_to_dicts(cursor) -> list:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_universo() -> list:
    """Lista de articulos distintos (int) desde los 3 staging en SQLite."""
    with engine.connect() as conn:
        rows = conn.execute(text(UNIVERSO_SQL)).fetchall()
    return [int(r[0]) for r in rows if r[0] is not None]


def fetch_atributos(codigos: list) -> list:
    """Trae atributos desde Fusion para una lista de codigos, en lotes de CHUNK."""
    out = []
    lotes = 0
    with get_fusion_db() as conn:
        cursor = conn.cursor()
        for i in range(0, len(codigos), CHUNK):
            chunk = codigos[i:i + CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            cursor.execute(QUERY_ATRIBUTOS.format(placeholders=placeholders), chunk)
            out.extend(_rows_to_dicts(cursor))
            lotes += 1
            print(f"[ETL articulos]   lote {lotes}: {len(chunk)} codigos -> acumulado {len(out)} filas", flush=True)
    return out, lotes


def clear_staging() -> int:
    session = SessionLocal()
    try:
        n = session.query(IndArticulosStaging).delete()
        session.commit()
        return n
    finally:
        session.close()


def persist_control(filas_staging: int) -> None:
    session = SessionLocal()
    try:
        row = session.query(IndEtlControl).filter_by(fuente="articulos").first()
        if row is None:
            row = IndEtlControl(fuente="articulos")
            session.add(row)
        row.watermark_idhisto = None
        row.ventana_desde = None
        row.ventana_hasta = None
        row.ultima_corrida = datetime.now()
        row.estado = "staging"
        row.filas_staging = filas_staging
        session.commit()
    finally:
        session.close()


def run() -> None:
    Base.metadata.create_all(bind=engine)  # idempotente; asegura staging + control locales
    print(f"[ETL articulos] DB destino (staging): {engine.url}", flush=True)

    t0 = time.monotonic()
    universo = get_universo()
    print(f"[ETL articulos] universo (articulos distintos en los 3 staging): {len(universo)}", flush=True)

    rows, lotes = fetch_atributos(universo)
    dt_q = time.monotonic() - t0

    objetos = [
        IndArticulosStaging(
            articulo=int(r["articulo"]),
            marca=_norm(r.get("marca")),
            descripcion=_norm(r.get("descripcion")),
            laboratorio=_norm(r.get("laboratorio")),
            familia=_norm(r.get("familia")),
            principio_activo=_norm(r.get("principio_activo")),
            unineg=_to_int(r.get("unineg")),
        )
        for r in rows
    ]

    borradas = clear_staging()
    print(f"[ETL articulos] staging limpiado: {borradas} filas borradas", flush=True)

    session = SessionLocal()
    try:
        session.bulk_save_objects(objetos)
        session.commit()
        total_staging = session.query(IndArticulosStaging).count()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    persist_control(len(objetos))

    print("[ETL articulos] ---------------- RESUMEN CARGA ----------------", flush=True)
    print(f"[ETL articulos] universo: {len(universo)}  lotes: {lotes}  filas Fusion: {len(rows)}", flush=True)
    print(f"[ETL articulos] filas escritas en staging: {len(objetos)}", flush=True)
    print(f"[ETL articulos] total filas en ind_articulos_staging: {total_staging}", flush=True)
    print(f"[ETL articulos] tiempo total: {dt_q:.1f}s", flush=True)
    print(f"[ETL articulos] control persistido en ind_etl_control (fuente='articulos')", flush=True)


def main() -> None:
    run()


if __name__ == "__main__":
    main()
