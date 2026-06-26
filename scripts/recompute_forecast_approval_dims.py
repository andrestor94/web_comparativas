"""
recompute_forecast_approval_dims.py
===================================
Diagnostica y completa las DIMENSIONES de "Aprobaciones Forecast" en DOS pasos
INDEPENDIENTES (cada uno se puede correr por separado):

  PASO PERFIL  (--perfil)
      Muchas solicitudes tienen `perfil` NULL (los overrides por subnegocio no
      traen perfil). Como cada cliente tiene un unico perfil, se deriva del
      cliente (fantasia -> perfil) desde la base valorizada real. Asi el filtro
      "Perfil" vuelve a funcionar.
      Target: forecast_change_requests.perfil

  PASO NEG     (--neg)
      La columna `neg` (negocio) esta 100% NULL: el flujo de guardado por
      subnegocio/celda nunca la persistia. `neg` ES derivable de `subneg` de
      forma 1:1 desde el maestro (forecast_valorizado). Este paso completa neg
      cruzando subneg -> neg con el mapa cacheado de forecast_service.
      Targets: forecast_change_requests.neg  Y  forecast_user_overrides.neg
               (origen y change-request quedan consistentes).
      subneg sin match en el maestro (p. ej. 'General') se SALTA -> queda NULL.
      No se inventa mapeo.

  GRUPO        : NO se persiste (se calcula en vivo con el mapa cliente->grupo).
                 Solo se REPORTA cobertura para evidencia.

Fuente de los mapas (igual que Forecast / "Proyeccion mas expectativa"):
  - Produccion (PostgreSQL): tabla forecast_valorizado
  - Local (SQLite): parquet df_valorizado

MODOS
  (sin --apply)  DIAGNOSE - SOLO LECTURA. No modifica nada. Reporta cobertura.
  --apply        Aplica los UPDATE de los pasos seleccionados.

SELECCION DE PASOS
  (sin flags de paso)  corre AMBOS pasos (perfil + neg).
  --perfil             corre SOLO el paso perfil.
  --neg                corre SOLO el paso neg.
  (--perfil --neg)     corre ambos (equivalente al default).

SEGURIDAD
  - NO hace DROP / DELETE / TRUNCATE / INSERT.
  - Solo UPDATE de las columnas `perfil` / `neg`, y solo donde estaban NULL/vacias.
  - IDEMPOTENTE: re-ejecutar no cambia nada nuevo.
  - NO toca app.db de forma destructiva.

USO
  Diagnostico (seguro, primero):
      python scripts/recompute_forecast_approval_dims.py
  Aplicar todo:
      python scripts/recompute_forecast_approval_dims.py --apply
  Aplicar solo neg:
      python scripts/recompute_forecast_approval_dims.py --neg --apply
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def _norm(s) -> str:
    return str(s or "").strip().lower()


def _is_empty(v) -> bool:
    return v is None or str(v).strip() == ""


def _step_perfil(session, CR, perfil_of: dict[str, str], apply: bool) -> None:
    """Completa forecast_change_requests.perfil derivandolo del cliente."""
    print("-" * 70)
    print("PASO PERFIL  (target: forecast_change_requests.perfil)")
    rows = session.query(CR).all()
    perfil_ok = perfil_null = derivable = no_derivable = actualizados = 0
    for cr in rows:
        if not _is_empty(cr.perfil):
            perfil_ok += 1
            continue
        perfil_null += 1
        p = perfil_of.get(_norm(cr.client_name or cr.client_selector))
        if p:
            derivable += 1
            if apply:
                cr.perfil = p
                actualizados += 1
        else:
            no_derivable += 1
    print("  ya tenian perfil            : %d" % perfil_ok)
    print("  perfil NULL/vacio           : %d" % perfil_null)
    print("    -> derivables             : %d" % derivable)
    print("    -> no derivables          : %d" % no_derivable)
    if apply:
        print("  ACTUALIZADOS (perfil)       : %d" % actualizados)


def _step_neg_target(session, Model, label: str, subneg_neg: dict[str, str], apply: bool) -> None:
    """Completa <Model>.neg cruzando subneg -> neg (1:1) desde el maestro."""
    rows = session.query(Model).all()
    neg_ok = neg_null = match = nomatch = actualizados = 0
    sample_nomatch: list[str] = []
    for r in rows:
        if not _is_empty(r.neg):
            neg_ok += 1
            continue
        neg_null += 1
        sub = _norm(getattr(r, "subneg", None))
        n = subneg_neg.get(sub) if sub else None
        if n:
            match += 1
            if apply:
                r.neg = n
                actualizados += 1
        else:
            nomatch += 1
            if len(sample_nomatch) < 8 and sub:
                sample_nomatch.append(repr(getattr(r, "subneg", None)))
    print("  [%s]" % label)
    print("    ya tenian neg             : %d" % neg_ok)
    print("    neg NULL/vacio            : %d" % neg_null)
    print("      -> con match (subneg)   : %d" % match)
    print("      -> sin match (queda NULL): %d" % nomatch)
    if sample_nomatch:
        print("      ejemplos sin match      : %s" % ", ".join(sorted(set(sample_nomatch))))
    if apply:
        print("    ACTUALIZADOS (neg)        : %d" % actualizados)


def _step_neg(session, CR, OV, subneg_neg: dict[str, str], apply: bool) -> None:
    print("-" * 70)
    print("PASO NEG     (targets: forecast_change_requests.neg + forecast_user_overrides.neg)")
    print("  subnegocios mapeables en maestro: %d" % len(subneg_neg))
    if not subneg_neg:
        print("  [!] Mapa subneg->neg VACIO: revisar forecast_valorizado / parquet. Paso NEG sin efecto.")
        return
    _step_neg_target(session, CR, "forecast_change_requests", subneg_neg, apply)
    if OV is not None:
        _step_neg_target(session, OV, "forecast_user_overrides", subneg_neg, apply)


def main(apply: bool, do_perfil: bool, do_neg: bool) -> int:
    from web_comparativas.models import (
        init_db, SessionLocal,
        ForecastChangeRequest as CR,
        ForecastUserOverride as OV,
    )
    from web_comparativas import forecast_service as svc

    if SessionLocal is None or CR is None:
        print("ERROR: almacenamiento ORM no disponible.")
        return 1

    init_db()

    pasos = []
    if do_perfil:
        pasos.append("PERFIL")
    if do_neg:
        pasos.append("NEG")

    print("=" * 70)
    print("Recompute dimensiones - Aprobaciones Forecast")
    print("  modo  : %s" % ("APPLY" if apply else "DIAGNOSE (solo lectura)"))
    print("  pasos : %s" % " + ".join(pasos))
    print("=" * 70)

    # Mapas (force=True para no usar cache potencialmente vieja en un saneo puntual).
    dim = svc.get_client_dim_map(force=True)
    perfil_of = {k: v.get("perfil") for k, v in dim.items() if v.get("perfil")}
    subneg_neg = svc.get_subneg_neg_map(force=True) if do_neg else {}

    is_pg = svc.engine is not None and "postgresql" in str(svc.engine.url)
    print("Fuente mapas: %s" % ("postgresql:forecast_valorizado" if is_pg else "parquet:df_valorizado"))
    print("  clientes con perfil       : %d" % len(perfil_of))
    if do_neg:
        print("  subneg con negocio (1:1)  : %d" % len(subneg_neg))

    # Cobertura de GRUPO (solo reporte).
    cli_grupo = {k for k, v in dim.items() if v.get("grupo")}
    with SessionLocal() as session:
        rows = session.query(CR).all()
        con_grupo = sum(1 for cr in rows if _norm(cr.client_name or cr.client_selector) in cli_grupo)
        print("  GRUPO (cobertura, en vivo): %d/%d resolverian grupo" % (con_grupo, len(rows)))

        if do_perfil:
            _step_perfil(session, CR, perfil_of, apply)
        if do_neg:
            _step_neg(session, CR, OV, subneg_neg, apply)

        if apply:
            session.commit()

    print("=" * 70)
    if not apply:
        print("DIAGNOSTICO (no se modifico nada). Para aplicar:")
        print("    python scripts/recompute_forecast_approval_dims.py --apply")
    else:
        print("OK - solo se completaron las columnas seleccionadas (NULL/vacio -> valor).")
        print("Status, impactos, revisor, comentario y fechas quedaron intactos.")
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    _apply = "--apply" in args
    _perfil_flag = "--perfil" in args
    _neg_flag = "--neg" in args
    # Sin flags de paso => ambos pasos.
    if not _perfil_flag and not _neg_flag:
        _perfil_flag = _neg_flag = True
    raise SystemExit(main(apply=_apply, do_perfil=_perfil_flag, do_neg=_neg_flag))
