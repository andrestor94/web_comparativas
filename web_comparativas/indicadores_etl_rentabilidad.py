"""ETL de rentabilidad nivel línea desde ETL_Data.rentabilidad_cliente (camino B).

Fuente: dbo.rentabilidad_cliente + LEFT JOIN dbo.clientes (solo SELECT). Patrón de
corridas: escribe en ind_rentabilidad_lineas etiquetando cada fila con el import_run_id
recibido (corrida creada en ind_import_run por indicadores_etl_runner). No toca filas
de otras corridas.

Capacidades de análisis preservadas: el extracto NO aplica renta1<0 ni comprob<>'NC'
(esos filtros difieren entre las 3 consultas que leen la tabla y se aplican en la
LECTURA). El ETL solo aplica el filtro COMÚN: ventana de fecha + universo de
cadneg/artículos. importe, renta1 y comprob se traen crudos.

Fechas: se reciben como DATE (DECLARE @ini/@fin) para evitar el bug de DATEFORMAT
Español (comparar string contra datetime directo). La columna rc.fecha se trae cruda
(datetime) y el bridge la serializa como /Date(epoch_ms)/, que decodificamos a datetime
de Python en hora local ART (UTC-3) para preservar el wall-clock almacenado.

Uso (normalmente vía web_comparativas/indicadores_etl_runner.py):
    python web_comparativas/indicadores_etl_rentabilidad.py base 12 <import_run_id>
"""

from __future__ import annotations

import re
import sys
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from web_comparativas.indicadores_db import get_fusion_db, get_etl_db  # noqa: F401  (etl en uso)

# Registramos los modelos summary y tomamos el engine/Session locales (SQLite).
import web_comparativas.indicadores_summary_models  # noqa: F401
from web_comparativas.indicadores_summary_models import (
    IndRentabilidadLineas,
    IndEtlControl,
)
from web_comparativas.models import Base, engine, SessionLocal


# SQL por mes. Filtro COMÚN únicamente: ventana de fecha + universo cadneg/artículos.
# NO incluye renta1<0 ni comprob<>'NC' (se aplican en la lectura).
QUERY_RENTABILIDAD_MES = """
DECLARE @ini DATE = ?;
DECLARE @fin DATE = ?;
WITH ClientesBase AS (
    SELECT codigo, fantasia, cliente_grupo,
        CASE WHEN nombre_grupo = 'SIN GRUPO' OR nombre_grupo IS NULL
             THEN fantasia ELSE nombre_grupo END AS NombreCliente
    FROM dbo.clientes
)
SELECT
    rc.ctacte                       AS ctacte,
    cb.cliente_grupo                AS cliente_grupo,
    cb.NombreCliente                AS nombre_cliente,
    rc.articulo                     AS articulo,
    LTRIM(RTRIM(rc.cadneg))         AS cadneg,
    rc.fecha                        AS fecha,
    rc.cant                         AS cant,
    rc.importe                      AS importe,
    rc.renta1                       AS renta1,
    rc.comprob                      AS comprob
FROM dbo.rentabilidad_cliente rc
LEFT JOIN ClientesBase cb ON rc.ctacte = cb.codigo
WHERE rc.fecha IS NOT NULL
  AND rc.fecha >= @ini AND rc.fecha < @fin
  AND (
        LTRIM(RTRIM(rc.cadneg)) IN ('2 - 1','2 - 2','2 - 3','2 - 4','2 - 5')
        OR CAST(rc.articulo AS VARCHAR(20)) IN ('8111612','8142146','8134261')
  );
"""

_Q4 = Decimal("0.0001")
_ART = timezone(timedelta(hours=-3))   # Argentina UTC-3 (sin DST)
_DATE_RE = re.compile(r"/Date\((-?\d+)")


def _to_decimal(value) -> "Decimal | None":
    """money/DECIMAL -> Decimal(19,4). Llega como número JSON (float) vía el bridge."""
    if value is None:
        return None
    return Decimal(str(value)).quantize(_Q4, rounding=ROUND_HALF_UP)


def _parse_bridge_datetime(value) -> "datetime | None":
    """Decodifica /Date(epoch_ms)/ del bridge a datetime naive en wall-clock ART."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value)
    m = _DATE_RE.search(text)
    if m:
        ms = int(m.group(1))
        dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt_utc.astimezone(_ART).replace(tzinfo=None)
    # Fallback: ISO (por si alguna vez viene ya formateada)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _to_int(value):
    return None if value is None else int(value)


def _add_month(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _rows_to_dicts(cursor) -> list:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def process_month(inicio_mes: date, fin_mes: date) -> list:
    """Ejecuta el SQL para UN mes y devuelve las filas crudas (lista de dicts)."""
    with get_etl_db() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_RENTABILIDAD_MES, [inicio_mes, fin_mes])
        return _rows_to_dicts(cursor)


def write_month(inicio_mes: date, fin_mes: date, rows: list, import_run_id: int) -> int:
    """Borra el mes (por rango de fecha) DE ESTA CORRIDA y reinserta etiquetado. Idempotente."""
    ini_dt = datetime.combine(inicio_mes, dtime.min)
    fin_dt = datetime.combine(fin_mes, dtime.min)
    session = SessionLocal()
    try:
        (session.query(IndRentabilidadLineas)
                .filter(IndRentabilidadLineas.fecha >= ini_dt,
                        IndRentabilidadLineas.fecha < fin_dt,
                        IndRentabilidadLineas.import_run_id == import_run_id)
                .delete(synchronize_session=False))
        objetos = [
            IndRentabilidadLineas(
                ctacte=_to_int(r.get("ctacte")),
                cliente_grupo=_to_int(r.get("cliente_grupo")),
                nombre_cliente=r.get("nombre_cliente"),
                articulo=int(r["articulo"]),
                cadneg=(r.get("cadneg") or None),
                fecha=_parse_bridge_datetime(r.get("fecha")),
                cant=_to_int(r.get("cant")),
                importe=_to_decimal(r.get("importe")),
                renta1=_to_decimal(r.get("renta1")),
                comprob=r.get("comprob"),
                import_run_id=import_run_id,
            )
            for r in rows
        ]
        session.bulk_save_objects(objetos)
        session.commit()
        return len(objetos)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def run(meses: list, import_run_id: int) -> int:
    """meses: lista de (inicio_mes: date, fin_mes: date, mes_label: str)."""
    Base.metadata.create_all(bind=engine)  # idempotente; asegura tablas locales
    print(f"[ETL rentab] DB destino: {engine.url}  corrida={import_run_id}", flush=True)
    print(f"[ETL rentab] Meses a procesar: {[m[2] for m in meses]}", flush=True)

    total_escritas = 0
    t_total0 = time.monotonic()
    for inicio, fin, label in meses:
        t0 = time.monotonic()
        rows = process_month(inicio, fin)
        dt_q = time.monotonic() - t0
        escritas = write_month(inicio, fin, rows, import_run_id)
        total_escritas += escritas
        print(
            f"[ETL rentab] mes={label}  filas={len(rows)}  escritas={escritas}  "
            f"tiempo_query={dt_q:.1f}s",
            flush=True,
        )
    t_total = time.monotonic() - t_total0

    session = SessionLocal()
    try:
        total_corrida = (session.query(IndRentabilidadLineas)
                                .filter_by(import_run_id=import_run_id).count())
    finally:
        session.close()

    print("[ETL rentab] ---------------- RESUMEN SMOKE TEST ----------------", flush=True)
    print(f"[ETL rentab] filas escritas en esta ejecución: {total_escritas}", flush=True)
    print(f"[ETL rentab] total filas de la corrida en ind_rentabilidad_lineas: {total_corrida}", flush=True)
    print(f"[ETL rentab] tiempo total loop: {t_total:.1f}s", flush=True)
    return total_escritas


def clear_run(import_run_id: int) -> int:
    """Borra las filas de ESTA corrida en ind_rentabilidad_lineas (no toca otras corridas)."""
    session = SessionLocal()
    try:
        n = (session.query(IndRentabilidadLineas)
                    .filter_by(import_run_id=import_run_id)
                    .delete(synchronize_session=False))
        session.commit()
        return n
    finally:
        session.close()


def persist_control(ventana_desde: date, ventana_hasta: date, filas_staging: int) -> None:
    """Upsert de ind_etl_control para fuente='rentabilidad_cliente' (camino B: sin watermark id)."""
    session = SessionLocal()
    try:
        row = session.query(IndEtlControl).filter_by(fuente="rentabilidad_cliente").first()
        if row is None:
            row = IndEtlControl(fuente="rentabilidad_cliente")
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


def run_base_load(n_meses: int = 12, hoy: "date | None" = None, *, import_run_id: int) -> int:
    """Carga base: limpia la corrida, loop de n_meses, persiste control (sin watermark id)."""
    hoy = hoy or date.today()
    Base.metadata.create_all(bind=engine)  # idempotente; asegura tablas + control locales
    meses = _ultimos_meses_completos(n_meses, hoy)
    ventana_desde = meses[0][0]
    ventana_hasta = date(hoy.year, hoy.month, 1)  # primer día del mes actual

    print(f"[ETL rentab] DB destino: {engine.url}  corrida={import_run_id}", flush=True)
    print(f"[ETL rentab] Ventana retención: {n_meses} meses  [{ventana_desde} .. {ventana_hasta})", flush=True)

    borradas = clear_run(import_run_id)
    print(f"[ETL rentab] corrida limpiada: {borradas} filas borradas", flush=True)

    t_total0 = time.monotonic()
    total_escritas = 0
    for inicio, fin, label in meses:
        t0 = time.monotonic()
        rows = process_month(inicio, fin)
        dt_q = time.monotonic() - t0
        escritas = write_month(inicio, fin, rows, import_run_id)
        total_escritas += escritas
        print(
            f"[ETL rentab] mes={label}  filas={len(rows)}  escritas={escritas}  "
            f"tiempo_query={dt_q:.1f}s",
            flush=True,
        )
    t_total = time.monotonic() - t_total0

    session = SessionLocal()
    try:
        total_corrida = (session.query(IndRentabilidadLineas)
                                .filter_by(import_run_id=import_run_id).count())
    finally:
        session.close()

    persist_control(ventana_desde, ventana_hasta, total_escritas)

    print("[ETL rentab] ---------------- RESUMEN CARGA BASE ----------------", flush=True)
    print(f"[ETL rentab] meses procesados: {len(meses)}", flush=True)
    print(f"[ETL rentab] filas escritas en esta ejecución: {total_escritas}", flush=True)
    print(f"[ETL rentab] total filas de la corrida en ind_rentabilidad_lineas: {total_corrida}", flush=True)
    print(f"[ETL rentab] tiempo total loop: {t_total:.1f}s", flush=True)
    print(f"[ETL rentab] control persistido en ind_etl_control (fuente='rentabilidad_cliente')", flush=True)
    return total_escritas


def _ultimos_meses_completos(n: int, hoy: date) -> list:
    """Los n meses calendario completos más recientes respecto de hoy (excluye el mes en curso)."""
    fin = date(hoy.year, hoy.month, 1)
    meses = []
    for _ in range(n):
        inicio = date(fin.year - 1, 12, 1) if fin.month == 1 else date(fin.year, fin.month - 1, 1)
        meses.append((inicio, fin, inicio.strftime("%Y-%m")))
        fin = inicio
    meses.reverse()
    return meses


def main(argv: list) -> None:
    if argv and argv[0] == "base":
        if len(argv) < 3:
            print("Falta import_run_id: python ...etl_rentabilidad.py base <n_meses> <import_run_id>")
            print("(normalmente se ejecuta vía web_comparativas/indicadores_etl_runner.py)")
            raise SystemExit(2)
        run_base_load(n_meses=int(argv[1]), import_run_id=int(argv[2]))
        return
    print("Ejecutar vía web_comparativas/indicadores_etl_runner.py (requiere import_run_id).")
    raise SystemExit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
