"""
recompute_forecast_approval_dims.py
===================================
Diagnostica y completa las DIMENSIONES de las solicitudes de
"Aprobaciones Forecast" (tabla forecast_change_requests):

  - PERFIL  : muchas solicitudes lo tienen NULL (los overrides por subnegocio
              no traen perfil). Como cada cliente tiene un unico perfil, se
              deriva del cliente (fantasia -> perfil) desde la base valorizada
              real. Asi el filtro "Perfil" vuelve a funcionar.
  - GRUPO   : NO se persiste (se calcula en vivo en cada request con el mapa
              cliente->grupo). Este script solo REPORTA la cobertura de grupos
              para dar evidencia de que la agrupacion va a funcionar.

Fuente de los mapas (igual que Forecast / "Proyeccion mas expectativa"):
  - Produccion (PostgreSQL): tabla forecast_valorizado
  - Local (SQLite): parquet df_valorizado

MODOS
  (sin flags)  DIAGNOSE - SOLO LECTURA. No modifica nada. Reporta:
               - fuente y tamano del mapa cliente->{grupo,perfil},
               - muestra de 20 pendientes,
               - cuantas solicitudes resolverian GRUPO (cobertura),
               - cuantas tienen PERFIL NULL y son derivables.
  --apply      Completa SOLO la columna `perfil` (donde estaba NULL y se puede
               derivar). NO toca grupo, status, impactos, revisor, comentario,
               fechas ni autor.

SEGURIDAD
  - NO hace DROP / DELETE / TRUNCATE / INSERT.
  - Solo UPDATE de la columna `perfil`, y solo donde estaba NULL.
  - IDEMPOTENTE: re-ejecutar no cambia nada nuevo.
  - NO toca app.db de forma destructiva.

USO
  Diagnostico (seguro, primero):
      python scripts/recompute_forecast_approval_dims.py
  Aplicar:
      python scripts/recompute_forecast_approval_dims.py --apply
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def _norm(s) -> str:
    return str(s or "").strip().lower()


def main(apply: bool) -> int:
    from web_comparativas.models import init_db, SessionLocal, ForecastChangeRequest as CR
    from web_comparativas import forecast_service as svc

    if SessionLocal is None or CR is None:
        print("ERROR: almacenamiento ORM no disponible.")
        return 1

    init_db()

    print("=" * 70)
    print("Recompute dimensiones (grupo/perfil) - Aprobaciones Forecast  [modo: %s]"
          % ("APPLY" if apply else "DIAGNOSE (solo lectura)"))
    print("=" * 70)

    dim = svc.get_client_dim_map(force=True)
    n_grupo = sum(1 for v in dim.values() if v.get("grupo"))
    n_perfil = sum(1 for v in dim.values() if v.get("perfil"))
    # detectar fuente
    is_pg = svc.engine is not None and "postgresql" in str(svc.engine.url)
    print("Mapa cliente -> {grupo, perfil}:")
    print("  fuente            : %s" % ("postgresql:forecast_valorizado" if is_pg else "parquet:df_valorizado"))
    print("  clientes en mapa  : %d" % len(dim))
    print("  con grupo         : %d" % n_grupo)
    print("  con perfil        : %d" % n_perfil)
    if not dim:
        print("  [!] Mapa VACIO: no se puede agrupar ni derivar perfil en este entorno.")
        print("      Revisar que forecast_valorizado (PG) o el parquet esten disponibles.")

    cli_grupo = {k for k, v in dim.items() if v.get("grupo")}
    perfil_of = {k: v.get("perfil") for k, v in dim.items() if v.get("perfil")}

    with SessionLocal() as session:
        # Muestra de 20 pendientes
        muestra = session.query(CR).filter(CR.status == "pendiente").limit(20).all()
        print("-" * 70)
        print("Muestra de hasta 20 PENDIENTES (cliente -> grupo? / perfil actual):")
        for cr in muestra:
            cli = _norm(cr.client_name or cr.client_selector)
            g = dim.get(cli, {}).get("grupo")
            p = dim.get(cli, {}).get("perfil")
            print("  id=%s cli=%r grupo=%r perfil_actual=%r perfil_derivable=%r"
                  % (cr.id, (cr.client_name or cr.client_selector or "")[:26], g, cr.perfil, p))

        rows = session.query(CR).all()
        total = len(rows)
        con_grupo = sin_grupo = 0
        perfil_ok = perfil_null = perfil_derivable = perfil_no_derivable = 0
        actualizados = 0
        sample_nogrupo: list[str] = []

        for cr in rows:
            cli = _norm(cr.client_name or cr.client_selector)
            # Cobertura de grupo (en vivo)
            if cli in cli_grupo:
                con_grupo += 1
            else:
                sin_grupo += 1
                if len(sample_nogrupo) < 8:
                    sample_nogrupo.append("id=%s cli=%r (en_mapa=%s)" % (cr.id, cli[:30], cli in dim))
            # Perfil
            if cr.perfil:
                perfil_ok += 1
            else:
                perfil_null += 1
                p = perfil_of.get(cli)
                if p:
                    perfil_derivable += 1
                    if apply:
                        cr.perfil = p
                        actualizados += 1
                else:
                    perfil_no_derivable += 1

        if apply:
            session.commit()

    print("-" * 70)
    print("Solicitudes analizadas        : %d" % total)
    print("GRUPO:")
    print("  resolverian grupo (cobert.) : %d" % con_grupo)
    print("  quedarian sueltas           : %d" % sin_grupo)
    print("PERFIL:")
    print("  ya tenian perfil            : %d" % perfil_ok)
    print("  perfil NULL                 : %d" % perfil_null)
    print("    -> derivables             : %d" % perfil_derivable)
    print("    -> no derivables          : %d" % perfil_no_derivable)
    if apply:
        print("  ACTUALIZADOS (perfil)       : %d" % actualizados)
    print("-" * 70)
    if sample_nogrupo:
        print("Ejemplos que quedarian SUELTOS (sin grupo):")
        for s in sample_nogrupo:
            print("  -", s)
    print("=" * 70)
    if not apply:
        print("DIAGNOSTICO (no se modifico nada). Para aplicar (solo completa perfil):")
        print("    python scripts/recompute_forecast_approval_dims.py --apply")
    else:
        print("OK - solo se completo la columna `perfil`. Grupo se calcula en vivo.")
        print("Status, impactos, revisor, comentario y fechas quedaron intactos.")
    return 0


if __name__ == "__main__":
    _apply = "--apply" in sys.argv[1:]
    raise SystemExit(main(apply=_apply))
