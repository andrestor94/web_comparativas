"""
recompute_forecast_approval_impacts.py
======================================
Diagnostica y recalcula el IMPACTO ESTIMADO faltante de las solicitudes de
"Aprobaciones Forecast" (tabla forecast_change_requests).

Por que: tras el backfill, muchas filas quedaron con
``estimated_amount_delta = NULL`` (se ven como N/D en la tabla y la matriz).
El impacto se estima cruzando el alcance del override (cliente / cliente+subneg
/ subnegocio / articulo / perfil) contra la base valorizada REAL:
  - Produccion (PostgreSQL): tabla forecast_valorizado
  - Local (SQLite): parquet df_valorizado
NO depende del periodo: si el override es anual/global, se estima sobre el
alcance completo.

MODOS
  (sin flags)  DIAGNOSE — SOLO LECTURA. No modifica nada. Imprime:
               - fuente y salud de la base valorizada (resolver),
               - muestra de 10 solicitudes pendientes con todos sus campos,
               - cuantos se pueden recalcular y cuantos no, con el por que.
  --apply      Actualiza SOLO los campos de impacto faltantes:
                 estimated_amount_base, estimated_amount_delta
               de las filas con estimated_amount_delta NULL que se puedan
               calcular. NO toca status, revisor, comentario, fechas ni autor.

SEGURIDAD
  - NO hace DROP / DELETE / TRUNCATE / INSERT.
  - Solo UPDATE de 2 columnas de impacto, y solo donde estaban NULL.
  - IDEMPOTENTE: re-ejecutar no cambia nada nuevo.
  - Preserva: status, reviewed_by_*, reviewed_at, review_comment,
    created_by_*, change_type, fechas.
  - NO toca app.db de forma destructiva (usa la conexion configurada por la app).

USO
  Diagnostico (seguro, primero):
      python scripts/recompute_forecast_approval_impacts.py
  Aplicar:
      python scripts/recompute_forecast_approval_impacts.py --apply
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

    init_db()  # asegura la tabla (aditivo, no destructivo)

    print("=" * 70)
    print("Recompute impactos - Aprobaciones Forecast  [modo: %s]"
          % ("APPLY" if apply else "DIAGNOSE (solo lectura)"))
    print("=" * 70)

    # 1) Resolver de base valorizada (fuente real)
    resolver = svc.get_scope_value_resolver(force=True)
    print("Base valorizada (resolver):")
    print("  fuente              : %s" % resolver.get("source"))
    print("  disponible (ok)     : %s" % resolver.get("ok"))
    print("  clientes (fantasia) : %d" % len(resolver.get("client", {})))
    print("  grupos              : %d" % len(resolver.get("group", {})))
    print("  subnegocios         : %d" % len(resolver.get("subneg", {})))
    print("  articulos (codigo)  : %d" % len(resolver.get("codigo", {})))
    print("  perfiles            : %d" % len(resolver.get("perfil", {})))
    print("  pares cliente+subneg: %d" % len(resolver.get("client_subneg", {})))
    if not resolver.get("ok"):
        print("  [!] La base valorizada NO esta disponible en este entorno.")
        print("      Sin ella NO se puede estimar impacto. Revisar forecast_valorizado / parquet.")

    cli_set = set(resolver.get("client", {})) | set(resolver.get("group", {}))
    sub_set = set(resolver.get("subneg", {}))
    cod_set = set(resolver.get("codigo", {}))
    perf_set = set(resolver.get("perfil", {}))

    # 2) Muestra de 10 pendientes con todos los campos
    with SessionLocal() as session:
        muestra = session.query(CR).filter(CR.status == "pendiente").limit(10).all()
        print("-" * 70)
        print("Muestra de hasta 10 solicitudes PENDIENTES:")
        for cr in muestra:
            print("  id=%s scope=%r cli=%r grupo? perfil=%r neg=%r subneg=%r cod=%r per=%r "
                  "old=%s new=%s absdelta=%s base=%s delta=%s src=%r"
                  % (cr.id, cr.scope_type, (cr.client_name or cr.client_selector or "")[:24],
                     cr.perfil, cr.neg, cr.subneg, cr.codigo_serie, cr.period,
                     cr.old_value, cr.new_value, cr.absolute_delta,
                     cr.estimated_amount_base, cr.estimated_amount_delta, cr.source))

        # 3) Recorrer todas y clasificar
        rows = session.query(CR).all()
        total = len(rows)
        ya_calculados = recalculables = sin_base = errores = actualizados = null_delta = 0
        sin_period = 0
        rzn = {"sin_delta": 0, "sin_identificadores": 0, "cliente_no_en_base": 0,
               "subneg_no_en_base": 0, "sin_match": 0}
        sample_nd: list[str] = []
        sample_ok: list[str] = []

        for cr in rows:
            if cr.estimated_amount_delta is not None:
                ya_calculados += 1
                continue
            null_delta += 1
            if not (cr.period and str(cr.period).strip() and str(cr.period) != "-"):
                sin_period += 1

            abs_delta = cr.absolute_delta
            if abs_delta is None:
                try:
                    nv = cr.new_value
                    ov = cr.old_value if cr.old_value is not None else 0.0
                    abs_delta = (float(nv) - float(ov)) if nv is not None else None
                except Exception:
                    abs_delta = None

            try:
                base = svc.resolve_scope_base(
                    resolver, perfil=cr.perfil, subneg=cr.subneg,
                    codigo_serie=cr.codigo_serie, client_selector=cr.client_selector,
                )
            except Exception:
                base = None
                errores += 1

            if base is None or abs_delta is None:
                sin_base += 1
                # Categorizar el por que
                cli = _norm(cr.client_name or cr.client_selector)
                sub = _norm(cr.subneg)
                cod = _norm(cr.codigo_serie)
                perf = _norm(cr.perfil)
                if abs_delta is None:
                    rzn["sin_delta"] += 1
                elif not (cli or sub or cod or perf):
                    rzn["sin_identificadores"] += 1
                elif cli and cli not in cli_set and not (sub and sub in sub_set):
                    rzn["cliente_no_en_base"] += 1
                elif sub and sub not in sub_set and not (cli and cli in cli_set):
                    rzn["subneg_no_en_base"] += 1
                else:
                    rzn["sin_match"] += 1
                if len(sample_nd) < 8:
                    sample_nd.append("id=%s cli=%r(in_base=%s) subneg=%r(in_base=%s) cod=%r absdelta=%s"
                                     % (cr.id, cli[:22], cli in cli_set, sub[:18], sub in sub_set, cod[:14], abs_delta))
                continue

            delta = round(float(base) * (float(abs_delta) / 100.0), 2)
            recalculables += 1
            if len(sample_ok) < 8:
                sample_ok.append("id=%s base=%.0f delta_pct=%.2f -> impacto=%.0f"
                                 % (cr.id, base, abs_delta, delta))
            if apply:
                cr.estimated_amount_base = float(base)
                cr.estimated_amount_delta = delta
                actualizados += 1

        if apply:
            session.commit()

    # 4) Resumen
    print("-" * 70)
    print("Solicitudes analizadas      : %d" % total)
    print("Ya tenian impacto           : %d" % ya_calculados)
    print("Con impacto NULL            : %d" % null_delta)
    print("   (de esas, sin periodo)   : %d" % sin_period)
    print("   -> recalculables         : %d" % recalculables)
    print("   -> sin base suficiente   : %d" % sin_base)
    print("Errores omitidos            : %d" % errores)
    if apply:
        print("ACTUALIZADOS (impacto)      : %d" % actualizados)
    print("-" * 70)
    print("Desglose de los SIN base:")
    print("   sin diferencia %% (delta)         : %d" % rzn["sin_delta"])
    print("   sin cliente/subneg/art/perfil    : %d" % rzn["sin_identificadores"])
    print("   cliente NO esta en base valoriz. : %d" % rzn["cliente_no_en_base"])
    print("   subneg NO esta en base valoriz.  : %d" % rzn["subneg_no_en_base"])
    print("   otros sin match                  : %d" % rzn["sin_match"])
    print("-" * 70)
    if sample_ok:
        print("Ejemplos recalculables:")
        for s in sample_ok:
            print("  +", s)
    if sample_nd:
        print("Ejemplos SIN base (quedan N/D):")
        for s in sample_nd:
            print("  -", s)
    print("=" * 70)
    if not apply:
        print("DIAGNOSTICO (no se modifico nada). Para aplicar:")
        print("    python scripts/recompute_forecast_approval_impacts.py --apply")
    else:
        print("OK - solo se actualizaron estimated_amount_base / estimated_amount_delta.")
        print("Estado, revisor, comentario y fechas quedaron intactos. Idempotente.")
    return 0


if __name__ == "__main__":
    _apply = "--apply" in sys.argv[1:]
    raise SystemExit(main(apply=_apply))
