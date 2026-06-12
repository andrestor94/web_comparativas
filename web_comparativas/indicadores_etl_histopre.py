"""ETL de PVP mensual desde Fusion.histopre (camino A) — extractor + smoke test.

Fuente: Fusion.dbo.histopre (solo SELECT). Watermark conceptual: idhisto (IDENTITY,
append-only). La agregación se hace EN SQL SERVER (un mes por consulta, para respetar
el timeout del bridge); las fechas vuelven ya formateadas como texto desde el SQL para
evitar el /Date(epoch)/ que produce ConvertTo-Json en el puente PowerShell.

Patrón de corridas: escribe en ind_inflacion_pvp_mensual etiquetando cada fila con el
import_run_id recibido (corrida creada en ind_import_run por indicadores_etl_runner).
No toca filas de otras corridas. El watermark global (MAX(idhisto)) se persiste en
ind_etl_control en la carga base, igual que antes.

Uso (normalmente vía web_comparativas/indicadores_etl_runner.py):
    python web_comparativas/indicadores_etl_histopre.py base 36 <import_run_id>
"""

from __future__ import annotations

import sys
import time
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP

from web_comparativas.indicadores_db import get_fusion_db

# Registramos los modelos summary y tomamos el engine/Session locales (SQLite).
import web_comparativas.indicadores_summary_models  # noqa: F401
from web_comparativas.indicadores_summary_models import (
    IndInflacionPvpMensual,
    IndEtlControl,
)
from web_comparativas.models import Base, engine, SessionLocal


# Consulta de extracción (placeholders ? = inicio_mes, inicio_mes_siguiente).
# Las fechas se reciben como DATE (DECLARE @ini/@fin) para que SQL Server parsee
# 'YYYY-MM-DD' como ISO culture-invariant; comparar el string del parámetro directo
# contra la columna DATETIME se mal-interpretaba bajo LANGUAGE Español (DATEFORMAT dmy).
# Mismo grano y mismo ROW_NUMBER; el orden de los ? no cambia (@ini primero, @fin después).
QUERY_PVP_MES = """
DECLARE @ini DATE = ?;
DECLARE @fin DATE = ?;

WITH base AS (
    SELECT
        h.articulo,
        CONVERT(CHAR(7), h.fecha, 120) AS mes,
        h.prepubact, h.fecha, h.idhisto,
        ROW_NUMBER() OVER (
            PARTITION BY h.articulo, CONVERT(CHAR(7), h.fecha, 120)
            ORDER BY h.fecha DESC, h.idhisto DESC
        ) AS rn
    FROM dbo.histopre h
    WHERE h.fecha IS NOT NULL
      AND h.prepubact IS NOT NULL
      AND h.prepubact >= 1
      AND h.fecha >= @ini AND h.fecha < @fin
)
SELECT articulo, mes,
       CONVERT(VARCHAR(10), fecha, 23) AS fecha_snapshot,
       CAST(prepubact AS DECIMAL(19,4)) AS pvp
FROM base WHERE rn = 1 ORDER BY articulo;
"""

# Watermark global de la tabla: MAX(idhisto) sin filtro de fecha (idhisto es IDENTITY
# append-only). No se acota por ventana — se toma al inicio del ETL para no perder
# altas que entren durante la corrida. Sin parámetros => sin el problema de DATEFORMAT.
QUERY_MAX_IDHISTO = "SELECT MAX(idhisto) AS max_idhisto FROM dbo.histopre;"

_Q4 = Decimal("0.0001")


def _to_decimal(value) -> "Decimal | None":
    """Reconstruye Decimal(19,4) desde lo que devuelva el bridge (llega como float vía JSON)."""
    if value is None:
        return None
    return Decimal(str(value)).quantize(_Q4, rounding=ROUND_HALF_UP)


def _add_month(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _rows_to_dicts(cursor) -> list:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def process_month(inicio_mes: date, fin_mes: date) -> list:
    """Ejecuta la consulta exacta para UN mes y devuelve las filas (lista de dicts)."""
    with get_fusion_db() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_PVP_MES, [inicio_mes, fin_mes])
        return _rows_to_dicts(cursor)


def max_idhisto_global():
    """MAX(idhisto) global de dbo.histopre (sin filtro de fecha). Watermark del ETL."""
    with get_fusion_db() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_MAX_IDHISTO)
        row = cursor.fetchone()
    return row[0] if row else None


def clear_run(import_run_id: int) -> int:
    """Borra las filas de ESTA corrida en ind_inflacion_pvp_mensual (no toca otras corridas)."""
    session = SessionLocal()
    try:
        n = (session.query(IndInflacionPvpMensual)
                    .filter_by(import_run_id=import_run_id)
                    .delete(synchronize_session=False))
        session.commit()
        return n
    finally:
        session.close()


def persist_watermark(watermark, ventana_desde: date, ventana_hasta: date, filas_staging: int) -> None:
    """Upsert de ind_etl_control para fuente='histopre' con el watermark global."""
    session = SessionLocal()
    try:
        row = session.query(IndEtlControl).filter_by(fuente="histopre").first()
        if row is None:
            row = IndEtlControl(fuente="histopre")
            session.add(row)
        row.watermark_idhisto = watermark
        row.ventana_desde = ventana_desde
        row.ventana_hasta = ventana_hasta
        row.ultima_corrida = datetime.now()
        row.estado = "staging"
        row.filas_staging = filas_staging
        session.commit()
    finally:
        session.close()


def write_month(mes: str, rows: list, import_run_id: int) -> int:
    """Borra el mes DE ESTA CORRIDA y reinserta etiquetado (idempotente dentro de la corrida)."""
    session = SessionLocal()
    try:
        (session.query(IndInflacionPvpMensual)
                .filter_by(mes=mes, import_run_id=import_run_id)
                .delete(synchronize_session=False))
        objetos = [
            IndInflacionPvpMensual(
                articulo=int(r["articulo"]),
                mes=r["mes"],
                fecha_snapshot=date.fromisoformat(r["fecha_snapshot"]) if r.get("fecha_snapshot") else None,
                pvp=_to_decimal(r.get("pvp")),
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
    print(f"[ETL histopre] DB destino: {engine.url}  corrida={import_run_id}", flush=True)
    print(f"[ETL histopre] Meses a procesar: {[m[2] for m in meses]}", flush=True)

    total_escritas = 0
    for inicio, fin, label in meses:
        t0 = time.monotonic()
        rows = process_month(inicio, fin)
        dt_q = time.monotonic() - t0
        escritas = write_month(label, rows, import_run_id)
        total_escritas += escritas
        print(
            f"[ETL histopre] mes={label}  filas={len(rows)}  escritas={escritas}  "
            f"tiempo_query={dt_q:.1f}s",
            flush=True,
        )

    session = SessionLocal()
    try:
        total_corrida = (session.query(IndInflacionPvpMensual)
                                .filter_by(import_run_id=import_run_id).count())
    finally:
        session.close()

    print("[ETL histopre] ---------------- RESUMEN SMOKE TEST ----------------", flush=True)
    print(f"[ETL histopre] filas escritas en esta ejecución: {total_escritas}", flush=True)
    print(f"[ETL histopre] total filas de la corrida en ind_inflacion_pvp_mensual: {total_corrida}", flush=True)
    print(f"[ETL histopre] MAX(idhisto) global (informativo, NO persistido): {max_idhisto_global()}", flush=True)
    return total_escritas


def run_base_load(n_meses: int = 36, hoy: "date | None" = None, *, import_run_id: int) -> int:
    """Carga base: watermark global al inicio, limpia la corrida, loop de n_meses, persiste control."""
    hoy = hoy or date.today()
    Base.metadata.create_all(bind=engine)  # idempotente; asegura tablas + control locales
    meses = _ultimos_meses_completos(n_meses, hoy)
    ventana_desde = meses[0][0]
    ventana_hasta = date(hoy.year, hoy.month, 1)  # primer día del mes actual

    print(f"[ETL histopre] DB destino: {engine.url}  corrida={import_run_id}", flush=True)
    print(f"[ETL histopre] Ventana retención: {n_meses} meses  [{ventana_desde} .. {ventana_hasta})", flush=True)

    # PASO 5: watermark global tomado AL INICIO (no perder altas durante el ETL).
    watermark = max_idhisto_global()
    print(f"[ETL histopre] watermark_idhisto (MAX global al inicio): {watermark}", flush=True)

    borradas = clear_run(import_run_id)
    print(f"[ETL histopre] corrida limpiada: {borradas} filas borradas", flush=True)

    t_total0 = time.monotonic()
    total_escritas = 0
    for inicio, fin, label in meses:
        t0 = time.monotonic()
        rows = process_month(inicio, fin)
        dt_q = time.monotonic() - t0
        escritas = write_month(label, rows, import_run_id)
        total_escritas += escritas
        print(
            f"[ETL histopre] mes={label}  filas={len(rows)}  escritas={escritas}  "
            f"tiempo_query={dt_q:.1f}s",
            flush=True,
        )
    t_total = time.monotonic() - t_total0

    session = SessionLocal()
    try:
        total_corrida = (session.query(IndInflacionPvpMensual)
                                .filter_by(import_run_id=import_run_id).count())
    finally:
        session.close()

    persist_watermark(watermark, ventana_desde, ventana_hasta, total_escritas)

    print("[ETL histopre] ---------------- RESUMEN CARGA BASE ----------------", flush=True)
    print(f"[ETL histopre] meses procesados: {len(meses)}", flush=True)
    print(f"[ETL histopre] filas escritas en esta ejecución: {total_escritas}", flush=True)
    print(f"[ETL histopre] total filas de la corrida en ind_inflacion_pvp_mensual: {total_corrida}", flush=True)
    print(f"[ETL histopre] tiempo total loop: {t_total:.1f}s", flush=True)
    print(f"[ETL histopre] watermark persistido en ind_etl_control (fuente='histopre'): {watermark}", flush=True)
    return total_escritas


def _ultimos_meses_completos(n: int, hoy: date) -> list:
    """Los n meses calendario completos más recientes respecto de hoy (excluye el mes en curso)."""
    primero_mes_actual = date(hoy.year, hoy.month, 1)
    fin = primero_mes_actual            # primer día del mes en curso = fin exclusivo del último mes completo
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
            print("Falta import_run_id: python ...etl_histopre.py base <n_meses> <import_run_id>")
            print("(normalmente se ejecuta vía web_comparativas/indicadores_etl_runner.py)")
            raise SystemExit(2)
        run_base_load(n_meses=int(argv[1]), import_run_id=int(argv[2]))
        return
    print("Ejecutar vía web_comparativas/indicadores_etl_runner.py (requiere import_run_id).")
    raise SystemExit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
