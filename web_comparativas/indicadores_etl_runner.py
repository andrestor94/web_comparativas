"""Orquestador LOCAL de la carga base de las tablas summary de Indicadores (patrón corridas).

Flujo:
  1. Crea una corrida en ind_import_run (status='running') y obtiene su id.
  2. Corre los 4 ETL etiquetando con ese import_run_id:
       histopre (PVP, 36 meses) -> rentabilidad (12) -> facturación (12) -> artículos.
     Artículos va al FINAL: su universo sale de las otras 3 tablas DE ESTA corrida.
  3. Si todo OK: pone la corrida en 'approved' y guarda rows_por_tabla con los conteos.
     (En LOCAL se aprueba directo, sin flujo humano. En producción la corrida quedará en
     'pending_approval' y la aprobará un humano — esa pieza es del endpoint de import.)
  4. Si algo falla: pone la corrida en 'failed' (con la nota del error) y corta.

Uso:
    python -m web_comparativas.indicadores_etl_runner             # 36/12/12 (carga base)
    python -m web_comparativas.indicadores_etl_runner 36 12 12    # meses pvp/rentab/factur
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime

from sqlalchemy import text

import web_comparativas.indicadores_summary_models  # noqa: F401  (registra los modelos)
from web_comparativas.indicadores_summary_models import (
    IndArticulos,
    IndImportRun,
    IndInflacionFacturacionMensual,
    IndInflacionPvpMensual,
    IndRentabilidadLineas,
)
from web_comparativas.models import Base, engine, SessionLocal

from web_comparativas import indicadores_etl_articulos as etl_articulos
from web_comparativas import indicadores_etl_facturacion as etl_facturacion
from web_comparativas import indicadores_etl_histopre as etl_histopre
from web_comparativas import indicadores_etl_rentabilidad as etl_rentabilidad

_TABLAS = {
    "ind_inflacion_pvp_mensual": IndInflacionPvpMensual,
    "ind_rentabilidad_lineas": IndRentabilidadLineas,
    "ind_inflacion_facturacion_mensual": IndInflacionFacturacionMensual,
    "ind_articulos": IndArticulos,
}


def _set_run(run_id: int, **valores) -> None:
    session = SessionLocal()
    try:
        run = session.query(IndImportRun).filter_by(id=run_id).first()
        for clave, valor in valores.items():
            setattr(run, clave, valor)
        session.commit()
    finally:
        session.close()


def run_carga_base(n_pvp: int = 36, n_rentab: int = 12, n_factur: int = 12) -> int:
    """Carga base completa bajo UNA corrida nueva. Devuelve el import_run_id."""
    Base.metadata.create_all(bind=engine)

    session = SessionLocal()
    try:
        run = IndImportRun(status="running")
        session.add(run)
        session.commit()
        session.refresh(run)
        rid = run.id
    finally:
        session.close()
    print(f"[runner] corrida creada: import_run_id={rid} (status=running)", flush=True)

    t0 = time.monotonic()
    try:
        etl_histopre.run_base_load(n_meses=n_pvp, import_run_id=rid)
        etl_rentabilidad.run_base_load(n_meses=n_rentab, import_run_id=rid)
        desde, hasta = etl_facturacion._ventana_meses(n_factur, date.today())
        etl_facturacion.run_base_load(desde, hasta, import_run_id=rid)
        etl_articulos.run(import_run_id=rid)

        session = SessionLocal()
        try:
            conteos = {
                nombre: session.query(modelo).filter_by(import_run_id=rid).count()
                for nombre, modelo in _TABLAS.items()
            }
        finally:
            session.close()

        ahora = datetime.utcnow()  # misma convención que created_at (default utcnow del modelo)
        _set_run(
            rid,
            status="approved",  # LOCAL: aprobación directa. En prod: 'pending_approval'.
            finalized_at=ahora,
            approved_at=ahora,
            approved_by="local (aprobación directa, sin flujo humano)",
            rows_por_tabla=json.dumps(conteos),
        )
        print(f"[runner] ---------------- RESUMEN CORRIDA {rid} ----------------", flush=True)
        for nombre, n in conteos.items():
            print(f"[runner] {nombre}: {n} filas", flush=True)
        print(f"[runner] estado final: approved  tiempo total: {time.monotonic() - t0:.1f}s", flush=True)
        return rid
    except Exception as exc:
        _set_run(rid, status="failed", finalized_at=datetime.utcnow(), nota=str(exc)[:2000])
        print(f"[runner] CORRIDA {rid} FAILED: {exc}", flush=True)
        raise


# Columnas a copiar en el copy-forward (todas menos el id surrogate; import_run_id se
# reasigna). ind_articulos NO se copia: etl_articulos.run() la reconstruye entera para la
# corrida (clear_run + reemplazo total del universo) — copiarla sería trabajo descartado.
# line_seq se copia TAL CUAL: en local es NULL (lo regenera el push de producción) y el
# UNIQUE (import_run_id, line_seq) es NULL-safe, así que la copia no colisiona.
_COPY_FORWARD = {
    "ind_inflacion_pvp_mensual": "articulo, mes, fecha_snapshot, pvp",
    "ind_rentabilidad_lineas": ("ctacte, cliente_grupo, nombre_cliente, articulo, cadneg, "
                                "fecha, cant, importe, renta1, comprob, line_seq"),
    "ind_inflacion_facturacion_mensual": "articulo, cadneg, mes, unidades, facturacion",
}


def _corrida_activa_id() -> "int | None":
    """Id de la corrida 'approved' activa (la más reciente por approved_at, desempate id)."""
    session = SessionLocal()
    try:
        run = (session.query(IndImportRun)
                      .filter_by(status="approved")
                      .order_by(IndImportRun.approved_at.desc(), IndImportRun.id.desc())
                      .first())
        return run.id if run else None
    finally:
        session.close()


def run_carga_incremental(meses_refresh: int = 3) -> int:
    """Corrida INCREMENTAL bajo una corrida nueva. Devuelve el import_run_id.

    Flujo: copy-forward de la corrida activa (INSERT...SELECT local, sin Fusion) +
    refresh desde Fusion/ETL_Data de los últimos `meses_refresh` meses completos:
      - PVP: meses tocados por idhisto > watermark (clampeados a la ventana cargada y a
        meses completos) UNIDOS a los últimos meses_refresh; actualiza el watermark.
      - Rentabilidad / Facturación: últimos meses_refresh (delete por mes + reinsert,
        scoped a la corrida nueva).
      - Artículos: reemplazo total del universo de la corrida nueva (es dimensión).
    La corrida queda como FOTO COMPLETA (historia copiada + meses frescos), lista para
    aprobación. Requiere una corrida base aprobada previa. NO toca run_carga_base.
    """
    Base.metadata.create_all(bind=engine)

    activa_id = _corrida_activa_id()
    if activa_id is None:
        raise RuntimeError("no hay corrida base para incrementar, corré carga base primero")

    session = SessionLocal()
    try:
        run = IndImportRun(status="running", nota=f"incremental sobre corrida {activa_id}")
        session.add(run)
        session.commit()
        session.refresh(run)
        rid = run.id
    finally:
        session.close()
    print(f"[runner incr] corrida creada: import_run_id={rid} (incremental sobre corrida activa {activa_id})", flush=True)

    t0 = time.monotonic()
    try:
        # 1) COPY-FORWARD local (sin Fusion): historia de la corrida activa -> corrida nueva.
        for tabla, cols in _COPY_FORWARD.items():
            with engine.begin() as conn:
                conn.execute(
                    text(f"INSERT INTO {tabla} ({cols}, import_run_id) "
                         f"SELECT {cols}, :nuevo FROM {tabla} WHERE import_run_id = :activa"),
                    {"nuevo": rid, "activa": activa_id},
                )
        session = SessionLocal()
        try:
            copiadas = {
                nombre: session.query(modelo).filter_by(import_run_id=rid).count()
                for nombre, modelo in _TABLAS.items()
            }
        finally:
            session.close()
        print(f"[runner incr] copy-forward desde corrida {activa_id}: {copiadas}", flush=True)

        hoy = date.today()
        meses_ventana = etl_histopre._ultimos_meses_completos(meses_refresh, hoy)
        labels_ventana = [m[2] for m in meses_ventana]
        primer_dia_mes_actual = date(hoy.year, hoy.month, 1)
        print(f"[runner incr] meses de refresh (últimos {meses_refresh} completos): {labels_ventana}", flush=True)

        # 2) PVP: meses con idhisto nuevo (desde el watermark) ∪ últimos meses_refresh.
        control = etl_histopre.leer_control()
        wm_prev = control["watermark_idhisto"] if control else None
        wm_nuevo = etl_histopre.max_idhisto_global()  # al inicio, como en base: no perder altas durante la corrida
        if wm_prev is None:
            print("[runner incr] PVP: sin watermark previo en ind_etl_control -> refresco solo la ventana", flush=True)
            labels_idhisto = []
        else:
            labels_idhisto = etl_histopre.meses_con_idhisto_nuevo(wm_prev)
        # Clamp: solo meses completos dentro de la ventana ya cargada (un idhisto con fecha
        # retro-datada fuera de ventana, o del mes en curso, no se refresca — se reporta).
        ventana_desde_ctrl = (control["ventana_desde"] if control else None) or meses_ventana[0][0]
        desde_label = ventana_desde_ctrl.strftime("%Y-%m")
        ultimo_completo = labels_ventana[-1]
        labels_idhisto_validos = sorted({m for m in labels_idhisto if desde_label <= m <= ultimo_completo})
        descartados = sorted(set(labels_idhisto) - set(labels_idhisto_validos))
        if descartados:
            print(f"[runner incr] PVP: meses con idhisto nuevo fuera de ventana/completos (NO refrescados): {descartados}", flush=True)
        labels_pvp = sorted(set(labels_ventana) | set(labels_idhisto_validos))
        print(f"[runner incr] PVP: watermark previo={wm_prev}  nuevo={wm_nuevo}  "
              f"meses idhisto-nuevo={labels_idhisto_validos}  meses a refrescar={labels_pvp}", flush=True)
        filas_pvp = 0
        for label in labels_pvp:
            inicio = date.fromisoformat(label + "-01")
            fin = etl_histopre._add_month(inicio)
            t_m = time.monotonic()
            rows = etl_histopre.process_month(inicio, fin)
            escritas = etl_histopre.write_month(label, rows, rid)
            filas_pvp += escritas
            print(f"[runner incr] PVP mes={label}  filas={len(rows)}  escritas={escritas}  "
                  f"tiempo_query={time.monotonic() - t_m:.1f}s", flush=True)
        # Watermark actualizado; la ventana del control conserva el desde original de la base.
        etl_histopre.persist_watermark(wm_nuevo, ventana_desde_ctrl, primer_dia_mes_actual, filas_pvp)
        print(f"[runner incr] PVP: watermark actualizado {wm_prev} -> {wm_nuevo}", flush=True)

        # 3) Rentabilidad: últimos meses_refresh (write_month borra por rango de fecha
        #    scoped a la corrida nueva — única defensa contra duplicados, tabla sin clave natural).
        for inicio, fin, label in meses_ventana:
            t_m = time.monotonic()
            rows = etl_rentabilidad.process_month(inicio, fin)
            escritas = etl_rentabilidad.write_month(inicio, fin, rows, rid)
            print(f"[runner incr] rentab mes={label}  filas={len(rows)}  escritas={escritas}  "
                  f"tiempo_query={time.monotonic() - t_m:.1f}s", flush=True)

        # 4) Facturación: delete por mes + re-agregado (una sola query a ETL_Data).
        etl_facturacion.run_incremental_refresh(meses_ventana[0][0], primer_dia_mes_actual, import_run_id=rid)

        # 5) Artículos: reemplazo total del universo de la corrida nueva (corre al final).
        etl_articulos.run(import_run_id=rid)

        session = SessionLocal()
        try:
            conteos = {
                nombre: session.query(modelo).filter_by(import_run_id=rid).count()
                for nombre, modelo in _TABLAS.items()
            }
        finally:
            session.close()

        ahora = datetime.utcnow()
        _set_run(
            rid,
            status="approved",  # LOCAL: aprobación directa, igual que run_carga_base.
            finalized_at=ahora,
            approved_at=ahora,
            approved_by=f"local (incremental sobre corrida {activa_id}, aprobación directa)",
            rows_por_tabla=json.dumps(conteos),
        )
        print(f"[runner incr] ---------------- RESUMEN CORRIDA {rid} (incremental) ----------------", flush=True)
        for nombre, n in conteos.items():
            print(f"[runner incr] {nombre}: {n} filas", flush=True)
        print(f"[runner incr] estado final: approved  tiempo total: {time.monotonic() - t0:.1f}s", flush=True)
        return rid
    except Exception as exc:
        _set_run(rid, status="failed", finalized_at=datetime.utcnow(), nota=str(exc)[:2000])
        print(f"[runner incr] CORRIDA {rid} FAILED: {exc}", flush=True)
        raise


def main(argv: list) -> None:
    if argv and argv[0] == "incremental":
        run_carga_incremental(meses_refresh=int(argv[1]) if len(argv) >= 2 else 3)
        return
    n_pvp = int(argv[0]) if len(argv) >= 1 else 36
    n_rentab = int(argv[1]) if len(argv) >= 2 else 12
    n_factur = int(argv[2]) if len(argv) >= 3 else 12
    run_carga_base(n_pvp=n_pvp, n_rentab=n_rentab, n_factur=n_factur)


if __name__ == "__main__":
    main(sys.argv[1:])
