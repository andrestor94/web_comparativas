"""
recompute_forecast_approval_impacts.py
======================================
Recalcula el IMPACTO ESTIMADO faltante de las solicitudes de
"Aprobaciones Forecast" (tabla forecast_change_requests).

Por qué: tras el backfill, muchas filas quedaron con
``estimated_amount_delta = NULL`` (se ven como N/D en la tabla y $0 en la
matriz). Este script recalcula esos importes usando la misma base valorizada
que usa Forecast (fact_forecast_valorizado.parquet -> monto_yhat).

MODOS
  --diagnose   (por defecto) SOLO LECTURA. No modifica nada. Imprime:
               - salud de la base valorizada (filas, monto_yhat, rango fechas),
               - estado de los impactos en forecast_change_requests,
               - simulación: cuántos se pueden recalcular y cuántos no (y por qué).
  --apply      Actualiza SOLO los campos de impacto faltantes:
                 estimated_amount_base, estimated_amount_delta
               de las filas con estimated_amount_delta NULL que se puedan
               calcular. NO toca status, revisor, comentario, fechas ni autor.

GARANTÍAS DE SEGURIDAD
  - NO hace DROP / DELETE / TRUNCATE / INSERT.
  - Solo UPDATE de 2 columnas de impacto, y solo donde estaban NULL.
  - IDEMPOTENTE: re-ejecutar no cambia nada nuevo (ya no quedan NULL calculables).
  - Preserva intactos: status, reviewed_by_*, reviewed_at, review_comment,
    created_by_*, change_type, fechas.
  - NO toca app.db de forma destructiva (usa la conexión configurada por la app:
    SQLite local o PostgreSQL en Render según DATABASE_URL).

USO
  Diagnóstico (seguro, recomendado primero):
      python scripts/recompute_forecast_approval_impacts.py
  Aplicar:
      python scripts/recompute_forecast_approval_impacts.py --apply
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def _valorizado_diag(svc) -> dict:
    import pandas as pd
    info = {"ok": False, "rows": 0, "has_monto_yhat": False,
            "fecha_min": None, "fecha_max": None, "n_fantasias": 0, "n_subneg": 0}
    try:
        df = svc.get_data().get("df_valorizado", pd.DataFrame())
    except Exception as exc:
        info["error"] = f"get_data() falló: {exc}"
        return info
    if df is None or df.empty:
        info["error"] = "df_valorizado vacío o no disponible"
        return info
    info["ok"] = True
    info["rows"] = int(len(df))
    info["has_monto_yhat"] = "monto_yhat" in df.columns
    if "fecha" in df.columns:
        try:
            info["fecha_min"] = str(df["fecha"].min())
            info["fecha_max"] = str(df["fecha"].max())
        except Exception:
            pass
    if "fantasia" in df.columns:
        info["n_fantasias"] = int(df["fantasia"].nunique())
    if "subneg" in df.columns:
        info["n_subneg"] = int(df["subneg"].nunique())
    return info


def main(apply: bool) -> int:
    from web_comparativas.models import init_db, SessionLocal, ForecastChangeRequest as CR
    from web_comparativas import forecast_service as svc

    if SessionLocal is None or CR is None:
        print("ERROR: almacenamiento ORM no disponible.")
        return 1

    init_db()  # asegura que la tabla exista (aditivo, no destructivo)

    print("=" * 64)
    print("Recompute impactos — Aprobaciones Forecast  [modo: %s]" % ("APPLY" if apply else "DIAGNOSE (solo lectura)"))
    print("=" * 64)

    # 1) Salud de la base valorizada
    vd = _valorizado_diag(svc)
    print("Base valorizada (fact_forecast_valorizado):")
    if not vd["ok"]:
        print("  [!] NO disponible:", vd.get("error", "desconocido"))
        print("  -> Sin esta base NO se puede estimar impacto. Revisar que el parquet")
        print("    exista y cargue en este entorno antes de aplicar.")
    else:
        print("  filas=%s | monto_yhat=%s | fantasías=%s | subneg=%s"
              % (vd["rows"], vd["has_monto_yhat"], vd["n_fantasias"], vd["n_subneg"]))
        print("  rango fecha: %s -> %s" % (vd["fecha_min"], vd["fecha_max"]))

    # 2) Estado actual de impactos
    total = 0
    ya_calculados = 0
    null_delta = 0
    recalculables = 0
    sin_base = 0
    errores = 0
    actualizados = 0
    sample_ok: list[str] = []
    sample_nd: list[str] = []

    with SessionLocal() as session:
        rows = session.query(CR).all()
        total = len(rows)
        for cr in rows:
            if cr.estimated_amount_delta is not None:
                ya_calculados += 1
                continue
            null_delta += 1

            # Diferencia % (puntos): usa absolute_delta; si falta, new - old
            abs_delta = cr.absolute_delta
            if abs_delta is None:
                try:
                    nv = cr.new_value if cr.new_value is not None else None
                    ov = cr.old_value if cr.old_value is not None else 0.0
                    abs_delta = (float(nv) - float(ov)) if nv is not None else None
                except Exception:
                    abs_delta = None

            try:
                base = svc.estimate_scope_amount(
                    perfil=cr.perfil, subneg=cr.subneg, codigo_serie=cr.codigo_serie,
                    client_selector=cr.client_selector, forecast_month=cr.period,
                )
            except Exception:
                base = None
                errores += 1

            if base is None or abs_delta is None:
                sin_base += 1
                if len(sample_nd) < 5:
                    sample_nd.append("%s | %s | subneg=%r cod=%r per=%r" % (
                        cr.client_name, cr.change_type, cr.subneg, cr.codigo_serie, cr.period))
                continue

            delta = round(float(base) * (float(abs_delta) / 100.0), 2)
            recalculables += 1
            if len(sample_ok) < 5:
                sample_ok.append("%s | base=%.0f delta_pct=%.2f -> impacto=%.0f" % (
                    cr.client_name, base, abs_delta, delta))

            if apply:
                cr.estimated_amount_base = float(base)
                cr.estimated_amount_delta = delta
                actualizados += 1

        if apply:
            session.commit()

    # 3) Resumen
    print("-" * 64)
    print("Solicitudes analizadas      : %d" % total)
    print("Impactos ya calculados      : %d" % ya_calculados)
    print("Con impacto NULL            : %d" % null_delta)
    print("  -> recalculables           : %d" % recalculables)
    print("  -> sin base suficiente     : %d" % sin_base)
    print("Errores omitidos            : %d" % errores)
    if apply:
        print("ACTUALIZADOS (impacto)      : %d" % actualizados)
    print("-" * 64)
    if sample_ok:
        print("Ejemplos recalculables:")
        for s in sample_ok:
            print("  +", s)
    if sample_nd:
        print("Ejemplos SIN base (quedan N/D):")
        for s in sample_nd:
            print("  -", s)
    print("=" * 64)
    if not apply:
        print("DIAGNÓSTICO (no se modificó nada). Para aplicar:")
        print("    python scripts/recompute_forecast_approval_impacts.py --apply")
    else:
        print("OK — solo se actualizaron estimated_amount_base / estimated_amount_delta.")
        print("Estado, revisor, comentario y fechas quedaron intactos. Idempotente.")
    return 0


if __name__ == "__main__":
    _apply = "--apply" in sys.argv[1:]
    raise SystemExit(main(apply=_apply))
