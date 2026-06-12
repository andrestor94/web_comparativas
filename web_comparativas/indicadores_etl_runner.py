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


def main(argv: list) -> None:
    n_pvp = int(argv[0]) if len(argv) >= 1 else 36
    n_rentab = int(argv[1]) if len(argv) >= 2 else 12
    n_factur = int(argv[2]) if len(argv) >= 3 else 12
    run_carga_base(n_pvp=n_pvp, n_rentab=n_rentab, n_factur=n_factur)


if __name__ == "__main__":
    main(sys.argv[1:])
