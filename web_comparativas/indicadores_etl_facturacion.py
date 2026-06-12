"""ETL de facturación mensual agregada desde ETL_Data.rentabililad_x_cliente.

Grano: articulo × cadneg × mes. La agregación se hace EN SQL SERVER (devuelve poco),
los 12 meses en UNA sola consulta. Patrón de corridas: escribe en
ind_inflacion_facturacion_mensual etiquetando cada fila con el import_run_id recibido
(corrida creada en ind_import_run por indicadores_etl_runner). No toca otras corridas.

NOTA DE DISEÑO: el SQL original de Inflación (indicadores_inflacion_service.QUERY_FACTURACION)
agrupa SOLO por articulo y hace MAX(cadneg). Acá agrego `cadneg` al GROUP BY para preservar
ese eje de análisis (grano articulo×cadneg×mes). unidades como FLOAT (igual que el
original); facturacion como Decimal(19,4).

Fechas: DECLARE @ini/@fin DATE (nunca comparar string contra datetime directo, por el
bug de DATEFORMAT Español). El mes sale de CONVERT(CHAR(7), fecha, 120) (estilo 120 es
locale-independiente).

Uso (normalmente vía web_comparativas/indicadores_etl_runner.py):
    python web_comparativas/indicadores_etl_facturacion.py base 12 <import_run_id>
"""

from __future__ import annotations

import sys
import time
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP

from web_comparativas.indicadores_db import get_etl_db

import web_comparativas.indicadores_summary_models  # noqa: F401
from web_comparativas.indicadores_summary_models import (
    IndInflacionFacturacionMensual,
    IndEtlControl,
)
from web_comparativas.models import Base, engine, SessionLocal


# Carga los 12 meses de una. Filtro: solo ventana de fecha (DATE). Grano articulo×cadneg×mes.
QUERY_FACTURACION = """
DECLARE @ini DATE = ?;
DECLARE @fin DATE = ?;
SELECT
    LTRIM(RTRIM(CAST(articulo AS VARCHAR(50)))) AS articulo,
    LTRIM(RTRIM(cadneg))                        AS cadneg,
    CONVERT(CHAR(7), fecha, 120)                AS mes,
    SUM(CAST(cant AS FLOAT))                    AS unidades,
    SUM(CAST(importe AS DECIMAL(19,4)))         AS facturacion
FROM dbo.rentabililad_x_cliente
WHERE fecha IS NOT NULL
  AND fecha >= @ini AND fecha < @fin
GROUP BY
    LTRIM(RTRIM(CAST(articulo AS VARCHAR(50)))),
    LTRIM(RTRIM(cadneg)),
    CONVERT(CHAR(7), fecha, 120);
"""

_Q4 = Decimal("0.0001")


def _to_decimal(value) -> "Decimal | None":
    if value is None:
        return None
    return Decimal(str(value)).quantize(_Q4, rounding=ROUND_HALF_UP)


def _rows_to_dicts(cursor) -> list:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_facturacion(ini: date, fin: date) -> list:
    """Ejecuta la consulta agregada para [ini, fin) y devuelve las filas (lista de dicts)."""
    with get_etl_db() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_FACTURACION, [ini, fin])
        return _rows_to_dicts(cursor)


def clear_run(import_run_id: int) -> int:
    """Borra las filas de ESTA corrida (no toca otras corridas)."""
    session = SessionLocal()
    try:
        n = (session.query(IndInflacionFacturacionMensual)
                    .filter_by(import_run_id=import_run_id)
                    .delete(synchronize_session=False))
        session.commit()
        return n
    finally:
        session.close()


def persist_control(ventana_desde: date, ventana_hasta: date, filas_staging: int) -> None:
    session = SessionLocal()
    try:
        row = session.query(IndEtlControl).filter_by(fuente="rentabililad_x_cliente").first()
        if row is None:
            row = IndEtlControl(fuente="rentabililad_x_cliente")
            session.add(row)
        row.watermark_idhisto = None
        row.ventana_desde = ventana_desde
        row.ventana_hasta = ventana_hasta
        row.ultima_corrida = datetime.now()
        row.estado = "staging"
        row.filas_staging = filas_staging
        session.commit()
    finally:
        session.close()


def run_base_load(ventana_desde: date, ventana_hasta: date, *, import_run_id: int) -> int:
    Base.metadata.create_all(bind=engine)  # idempotente; asegura tablas + control locales
    print(f"[ETL factur] DB destino: {engine.url}  corrida={import_run_id}", flush=True)
    print(f"[ETL factur] Ventana: [{ventana_desde} .. {ventana_hasta})", flush=True)

    t0 = time.monotonic()
    rows = fetch_facturacion(ventana_desde, ventana_hasta)
    dt_q = time.monotonic() - t0
    print(f"[ETL factur] filas devueltas por SQL: {len(rows)}  tiempo_query={dt_q:.1f}s", flush=True)

    # Guardas previas a escribir: articulo numérico y cadneg no-nulo (clave natural del UNIQUE).
    # Con el patrón de corridas levantan RuntimeError para que el runner marque 'failed'.
    bad_art = [r.get("articulo") for r in rows
               if not (str(r.get("articulo") or "").strip().lstrip("-").isdigit())]
    null_cadneg = sum(1 for r in rows if (r.get("cadneg") is None or str(r.get("cadneg")).strip() == ""))
    if bad_art:
        raise RuntimeError(f"[ETL factur] {len(bad_art)} articulos no numéricos. Muestra: {bad_art[:10]}")
    if null_cadneg:
        raise RuntimeError(f"[ETL factur] {null_cadneg} filas con cadneg NULL/vacío (clave natural articulo+cadneg+mes).")

    borradas = clear_run(import_run_id)
    print(f"[ETL factur] corrida limpiada: {borradas} filas borradas", flush=True)

    objetos = [
        IndInflacionFacturacionMensual(
            articulo=int(str(r["articulo"]).strip()),
            cadneg=str(r["cadneg"]).strip(),
            mes=r["mes"],
            unidades=(float(r["unidades"]) if r.get("unidades") is not None else None),
            facturacion=_to_decimal(r.get("facturacion")),
            import_run_id=import_run_id,
        )
        for r in rows
    ]
    session = SessionLocal()
    try:
        session.bulk_save_objects(objetos)
        session.commit()
        total_corrida = (session.query(IndInflacionFacturacionMensual)
                                .filter_by(import_run_id=import_run_id).count())
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    persist_control(ventana_desde, ventana_hasta, len(objetos))

    print("[ETL factur] ---------------- RESUMEN CARGA BASE ----------------", flush=True)
    print(f"[ETL factur] filas escritas en esta ejecución: {len(objetos)}", flush=True)
    print(f"[ETL factur] total filas de la corrida en ind_inflacion_facturacion_mensual: {total_corrida}", flush=True)
    print(f"[ETL factur] control persistido en ind_etl_control (fuente='rentabililad_x_cliente')", flush=True)
    return len(objetos)


def _labels_meses(desde: date, hasta: date) -> list:
    """Etiquetas 'YYYY-MM' de los meses calendario en [desde, hasta)."""
    labels = []
    cur = date(desde.year, desde.month, 1)
    while cur < hasta:
        labels.append(cur.strftime("%Y-%m"))
        cur = date(cur.year + 1, 1, 1) if cur.month == 12 else date(cur.year, cur.month + 1, 1)
    return labels


def run_incremental_refresh(ventana_desde: date, ventana_hasta: date, *, import_run_id: int) -> int:
    """Refresh INCREMENTAL: borra SOLO los meses de [ventana_desde, ventana_hasta) DE ESTA
    corrida y reinserta el re-agregado fresco, en una misma transacción.

    A diferencia de run_base_load NO hace clear_run total ni toca ind_etl_control (la
    ventana publicada de la corrida es más amplia que la refrescada; el control lo sigue
    escribiendo solo la carga base). Mismas guardas de calidad que la carga base, más una
    guarda de ventana: si el SQL devolviera un mes fuera del rango, ese mes no habría sido
    borrado antes y el insert lo duplicaría/colisionaría — se corta con error.
    """
    Base.metadata.create_all(bind=engine)
    labels = _labels_meses(ventana_desde, ventana_hasta)
    print(f"[ETL factur] refresh incremental corrida={import_run_id}  meses={labels}", flush=True)

    t0 = time.monotonic()
    rows = fetch_facturacion(ventana_desde, ventana_hasta)
    dt_q = time.monotonic() - t0
    print(f"[ETL factur] filas devueltas por SQL: {len(rows)}  tiempo_query={dt_q:.1f}s", flush=True)

    bad_art = [r.get("articulo") for r in rows
               if not (str(r.get("articulo") or "").strip().lstrip("-").isdigit())]
    null_cadneg = sum(1 for r in rows if (r.get("cadneg") is None or str(r.get("cadneg")).strip() == ""))
    if bad_art:
        raise RuntimeError(f"[ETL factur] {len(bad_art)} articulos no numéricos. Muestra: {bad_art[:10]}")
    if null_cadneg:
        raise RuntimeError(f"[ETL factur] {null_cadneg} filas con cadneg NULL/vacío (clave natural articulo+cadneg+mes).")
    fuera = [r.get("mes") for r in rows if r.get("mes") not in set(labels)]
    if fuera:
        raise RuntimeError(f"[ETL factur] {len(fuera)} filas con mes fuera de la ventana de refresh. Muestra: {fuera[:5]}")

    objetos = [
        IndInflacionFacturacionMensual(
            articulo=int(str(r["articulo"]).strip()),
            cadneg=str(r["cadneg"]).strip(),
            mes=r["mes"],
            unidades=(float(r["unidades"]) if r.get("unidades") is not None else None),
            facturacion=_to_decimal(r.get("facturacion")),
            import_run_id=import_run_id,
        )
        for r in rows
    ]
    session = SessionLocal()
    try:
        borradas = (session.query(IndInflacionFacturacionMensual)
                           .filter(IndInflacionFacturacionMensual.mes.in_(labels),
                                   IndInflacionFacturacionMensual.import_run_id == import_run_id)
                           .delete(synchronize_session=False))
        session.bulk_save_objects(objetos)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    print(f"[ETL factur] refresh: {borradas} filas borradas, {len(objetos)} reinsertadas", flush=True)
    return len(objetos)


def _ventana_meses(n: int, hoy: date) -> "tuple[date, date]":
    """[primer día de (mes_actual - n) .. primer día del mes actual)."""
    hasta = date(hoy.year, hoy.month, 1)
    y, m = hoy.year, hoy.month - n
    while m <= 0:
        m += 12
        y -= 1
    desde = date(y, m, 1)
    return desde, hasta


def main(argv: list) -> None:
    if argv and argv[0] == "base":
        if len(argv) < 3:
            print("Falta import_run_id: python ...etl_facturacion.py base <n_meses> <import_run_id>")
            print("(normalmente se ejecuta vía web_comparativas/indicadores_etl_runner.py)")
            raise SystemExit(2)
        desde, hasta = _ventana_meses(int(argv[1]), date.today())
        run_base_load(desde, hasta, import_run_id=int(argv[2]))
        return
    print("Ejecutar vía web_comparativas/indicadores_etl_runner.py (requiere import_run_id).")
    raise SystemExit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
