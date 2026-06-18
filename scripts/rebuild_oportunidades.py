"""
Reconstruye la tabla precalculada `oportunidades_summary` (Oportunidades de Venta)
contra el run activo de Dimensionamiento, SIN reimportar el CSV.

Reutiliza exactamente el mismo motor que corre en el flujo de import
(`web_comparativas.dimensionamiento.oportunidades.rebuild_oportunidades_for_run`),
para garantizar paridad local <-> producción.

Además imprime la SALIDA DE INSPECCIÓN para validar los números a mano:
  1. Total de oportunidades que califican.
  2. Distribución por tipo_oportunidad y por estado_actividad.
  3. TOP 20 por score (tabla legible).
  4. Detalle de cálculo de 3 ejemplos (sumas mensuales + counts de efectividad),
     RECALCULADO de forma independiente desde dimensionamiento_records.
  5. Funnel de descartes por filtro.

Uso (desde la raíz del proyecto, venv activado):
    python scripts/rebuild_oportunidades.py
    python scripts/rebuild_oportunidades.py --run-id 5
    python scripts/rebuild_oportunidades.py --examples 5

Opera sobre la base configurada por la app (local: web_comparativas/app.db).
"""
from __future__ import annotations

import argparse
import datetime as dt
import statistics
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import func, select  # noqa: E402

from web_comparativas.dimensionamiento.models import (  # noqa: E402
    DimensionamientoRecord,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento import oportunidades as opp  # noqa: E402
from web_comparativas.dimensionamiento.query_service import (  # noqa: E402
    _latest_success_import_run,
)
from web_comparativas.models import SessionLocal  # noqa: E402


def _fmt_money(value: float) -> str:
    return f"${value:,.0f}"


def _fmt_num(value: float) -> str:
    return f"{value:,.1f}"


def _print_header(title: str) -> None:
    print("\n" + "=" * 100, flush=True)
    print(title, flush=True)
    print("=" * 100, flush=True)


def _truncate(text: str | None, width: int) -> str:
    s = str(text or "")
    return s if len(s) <= width else s[: width - 1] + "…"


def _distribuciones(session, run_id: int) -> None:
    _print_header("2) DISTRIBUCIONES")
    print("\nPor tipo_oportunidad:", flush=True)
    rows = session.execute(
        select(OportunidadSummary.tipo_oportunidad, func.count(OportunidadSummary.id))
        .where(OportunidadSummary.import_run_id == run_id)
        .group_by(OportunidadSummary.tipo_oportunidad)
        .order_by(func.count(OportunidadSummary.id).desc())
    ).all()
    for tipo, cnt in rows:
        print(f"   {tipo:<14} {cnt:>8,}", flush=True)

    print("\nPor estado_actividad:", flush=True)
    rows = session.execute(
        select(OportunidadSummary.estado_actividad, func.count(OportunidadSummary.id))
        .where(OportunidadSummary.import_run_id == run_id)
        .group_by(OportunidadSummary.estado_actividad)
        .order_by(func.count(OportunidadSummary.id).desc())
    ).all()
    for estado, cnt in rows:
        print(f"   {estado:<14} {cnt:>8,}", flush=True)


def _top20(session, run_id: int) -> list[OportunidadSummary]:
    _print_header("3) TOP 20 POR SCORE")
    top = session.execute(
        select(OportunidadSummary)
        .where(OportunidadSummary.import_run_id == run_id)
        .order_by(OportunidadSummary.score.desc())
        .limit(20)
    ).scalars().all()

    # mDem = meses_demanda_cliente_12m (todos los estados, clasifica tipo)
    # mNP  = meses_no_participo_12m (solo NO_PARTICIPO, define el monto)
    headers = (
        f"{'#':>2} {'tipo':<12} {'estado':<8} {'cliente':<22} {'producto':<26} "
        f"{'familia':<16} {'mDem':>4} {'mNP':>3} {'cons.tip':>9} {'min-max':>15} "
        f"{'efect':>6} {'precio':>10} {'monto':>14} {'score':>14}"
    )
    print(headers, flush=True)
    print("-" * len(headers), flush=True)
    for i, o in enumerate(top, start=1):
        minmax = f"{o.consumo_min_mensual:,.0f}-{o.consumo_max_mensual:,.0f}"
        print(
            f"{i:>2} {_truncate(o.tipo_oportunidad,12):<12} {_truncate(o.estado_actividad,8):<8} "
            f"{_truncate(o.cliente_visible,22):<22} {_truncate(o.producto_nombre,26):<26} "
            f"{_truncate(o.familia,16):<16} {o.meses_demanda_cliente_12m:>4} {o.meses_no_participo_12m:>3} "
            f"{o.consumo_tipico_mensual:>9,.1f} {minmax:>15} "
            f"{o.efectividad:>6.2f} {_fmt_money(o.precio_unitario_estimado):>10} "
            f"{_fmt_money(o.monto_oportunidad):>14} {_fmt_money(o.score):>14}",
            flush=True,
        )
    return top


def _detalle_ejemplo(session, run_id: int, o: OportunidadSummary, ref_month: dt.date, window_start: dt.date, window_end: dt.date) -> None:
    """Recalcula de forma INDEPENDIENTE desde dimensionamiento_records para validar."""
    R = DimensionamientoRecord
    print("\n" + "-" * 100, flush=True)
    print(f"EJEMPLO  cliente={o.cliente_visible!r}  codigo={o.codigo_articulo!r}  producto={o.producto_nombre!r}", flush=True)
    print(f"   tipo={o.tipo_oportunidad}  estado={o.estado_actividad}  ventana={window_start.isoformat()}..{ref_month.isoformat()} (incl)", flush=True)

    # Demanda NO_PARTICIPO del par en ventana -> sumas mensuales.
    demanda = session.execute(
        select(R.fecha, R.cantidad_demandada, R.valorizacion_estimada)
        .where(R.import_run_id == run_id)
        .where(R.fecha < window_end)
        .where(R.resultado_participacion == opp.ESTADO_NO_PARTICIPO)
        .where(R.fecha >= window_start)
        .where(R.codigo_articulo == o.codigo_articulo)
        .where(R.cliente_visible == o.cliente_visible)
    ).all()

    monthly = defaultdict(float)
    total_cant = 0.0
    total_val = 0.0
    for fecha, cant, val in demanda:
        if isinstance(fecha, dt.datetime):
            fecha = fecha.date()
        cant = float(cant or 0)
        val = float(val or 0)
        monthly[f"{fecha.year:04d}-{fecha.month:02d}"] += cant
        total_cant += cant
        total_val += val

    sums = {k: monthly[k] for k in sorted(monthly)}
    nonzero = [v for v in sums.values() if v > 0]
    print("   [A] Sumas mensuales NO_PARTICIPO (mes -> cantidad) — define el monto:", flush=True)
    for k, v in sums.items():
        marca = "" if v > 0 else "   (mes sin demanda, NO entra a la mediana)"
        print(f"      {k}: {v:,.1f}{marca}", flush=True)
    mediana = statistics.median(nonzero) if nonzero else 0
    precio = (total_val / total_cant) if total_cant > 0 else 0
    print(f"   meses_no_participo_12m = {len(nonzero)}  (esperado tabla={o.meses_no_participo_12m})", flush=True)
    print(f"   mediana(meses NO_PARTICIPO con demanda) = {mediana:,.4f}  (esperado consumo_tipico={o.consumo_tipico_mensual:,.4f})", flush=True)
    print(f"   min={min(nonzero) if nonzero else 0:,.1f} max={max(nonzero) if nonzero else 0:,.1f}", flush=True)
    print(f"   SUM(val)={_fmt_money(total_val)} / SUM(cant)={total_cant:,.1f} = precio_unit {precio:,.4f}  (esperado={o.precio_unitario_estimado:,.4f})", flush=True)
    print(f"   monto = mediana*precio = {_fmt_money(mediana*precio)}  (esperado={_fmt_money(o.monto_oportunidad)})", flush=True)

    # [B] Demanda del cliente TODOS los estados -> meses_demanda_cliente_12m (clasifica el tipo).
    demanda_cli = session.execute(
        select(R.fecha, R.cantidad_demandada, R.resultado_participacion)
        .where(R.import_run_id == run_id)
        .where(R.fecha >= window_start)
        .where(R.fecha < window_end)
        .where(R.codigo_articulo == o.codigo_articulo)
        .where(R.cliente_visible == o.cliente_visible)
    ).all()
    cli_monthly = defaultdict(float)
    cli_estados = defaultdict(lambda: defaultdict(float))
    ultima = None
    for fecha, cant, estado in demanda_cli:
        if isinstance(fecha, dt.datetime):
            fecha = fecha.date()
        mk = f"{fecha.year:04d}-{fecha.month:02d}"
        cli_monthly[mk] += float(cant or 0)
        cli_estados[mk][estado] += float(cant or 0)
        if ultima is None or fecha > ultima:
            ultima = fecha
    cli_nonzero = [v for v in cli_monthly.values() if v > 0]
    print("   [B] Sumas mensuales TODOS los estados (mes -> cantidad por estado) — clasifica el tipo:", flush=True)
    for mk in sorted(cli_estados):
        detalle = " ".join(f"{e}={v:,.0f}" for e, v in sorted(cli_estados[mk].items()))
        print(f"      {mk}: total={cli_monthly[mk]:,.1f}  [{detalle}]", flush=True)
    print(f"   meses_demanda_cliente_12m = {len(cli_nonzero)}  (esperado tabla={o.meses_demanda_cliente_12m})  -> tipo={o.tipo_oportunidad}", flush=True)
    print(f"   ultima_demanda (all states) = {ultima}  (esperado tabla={o.ultima_demanda})  -> estado={o.estado_actividad}", flush=True)
    print(f"   >>> DIFERENCIA: cliente demanda {len(cli_nonzero)}/12 meses, pero NO_PARTICIPO solo {len(nonzero)}/12", flush=True)

    # Efectividad del codigo (histórico completo).
    counts = session.execute(
        select(R.resultado_participacion, func.count(R.id))
        .where(R.import_run_id == run_id)
        .where(R.codigo_articulo == o.codigo_articulo)
        .group_by(R.resultado_participacion)
    ).all()
    counts_map = {estado: int(c) for estado, c in counts}
    ganados = counts_map.get(opp.ESTADO_GANADO, 0)
    comprado = counts_map.get(opp.ESTADO_COMPRADO_OTRA, 0)
    espera = counts_map.get(opp.ESTADO_EN_ESPERA, 0)
    denom = ganados + comprado + espera
    efectividad = (ganados / denom) if denom > 0 else 0
    print(f"   Efectividad codigo (histórico completo): counts={counts_map}", flush=True)
    print(
        f"   efectividad = ganados/(ganados+comprado_otra+en_espera) = {ganados}/({ganados}+{comprado}+{espera}) "
        f"= {efectividad:.4f}  (esperado={o.efectividad:.4f})",
        flush=True,
    )
    print(
        f"   score = monto*efect*tipo_mult*act_mult = {_fmt_money(o.monto_oportunidad)} * {o.efectividad:.4f} "
        f"* {o.tipo_multiplicador} * {o.multiplicador_actividad} = {_fmt_money(o.score)}",
        flush=True,
    )


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", dest="run_id", type=int, default=None, help="Run a procesar (default: run activo).")
    parser.add_argument("--examples", dest="examples", type=int, default=3, help="Cantidad de ejemplos de detalle (default 3).")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        run_id = args.run_id
        if run_id is None:
            latest = _latest_success_import_run(session)
            run_id = latest.id if latest else None
        if run_id is None:
            print("[ERROR] No hay run activo (success) en dimensionamiento_import_runs.", flush=True)
            return 2

        print(f"OPORTUNIDADES_ENABLED = {opp.OPORTUNIDADES_ENABLED()}", flush=True)
        print(f"Reconstruyendo oportunidades para run_id={run_id} ...", flush=True)
        result = opp.rebuild_oportunidades_for_run(session, run_id, commit=True)
        print(f"Rebuild status={result.get('status')} filas={result.get('rows')}", flush=True)

        stats = result.get("stats") or {}
        ref_month = dt.date.fromisoformat(stats["ref_month"]) if stats.get("ref_month") else None
        window_start = dt.date.fromisoformat(stats["window_start"]) if stats.get("window_start") else None
        window_end = dt.date.fromisoformat(stats["window_end"]) if stats.get("window_end") else None

        total = session.execute(
            select(func.count(OportunidadSummary.id)).where(OportunidadSummary.import_run_id == run_id)
        ).scalar_one()

        _print_header("0) ANCLAJE DE VENTANA (último mes completo, umbral auto-referido)")
        print(f"   anchor_mode           : {stats.get('anchor_mode')}", flush=True)
        print(f"   volumen_referencia    : {stats.get('volumen_referencia'):,.0f} renglones/mes (mediana de meses densos)", flush=True)
        print(f"   PARAM_MES_COMPLETO_PCT/PISO : {opp.PARAM_MES_COMPLETO_PCT:.0%} / {opp.PARAM_MES_COMPLETO_PISO_MIN:,}", flush=True)
        print(f"   PARAM_UMBRAL_MES_COMPLETO  : {stats.get('umbral_mes_completo'):,.0f} renglones", flush=True)
        print("   Clasificación de meses:", flush=True)
        for row in (stats.get("meses_clasificacion") or []):
            marca = "  <-- ÚLTIMO COMPLETO (ref_month)" if row["mes"] == stats.get("ref_month", "")[:7] else ""
            print(f"      {row['mes']}  {row['renglones']:>8,}  {row['estado']}{marca}", flush=True)

        _print_header("1) TOTAL DE OPORTUNIDADES QUE CALIFICAN")
        print(f"   Run activo            : {run_id}", flush=True)
        print(f"   Mes de referencia     : {stats.get('ref_month')}  (max_fecha={stats.get('max_fecha')})", flush=True)
        print(f"   Ventana demanda       : {stats.get('window_start')} .. {stats.get('ref_month')} (incl)  ({opp.VENTANA_MESES} meses)", flush=True)
        print(f"   Pares NO_PARTICIPO     : {stats.get('pares_no_participo'):,}", flush=True)
        print(f"   Candidatos (demanda+precio>0): {stats.get('candidatos'):,}", flush=True)
        print(f"   >>> OPORTUNIDADES QUE CALIFICAN: {total:,}", flush=True)

        _distribuciones(session, run_id)
        top = _top20(session, run_id)

        _print_header("4) DETALLE DE CÁLCULO (recalculado independientemente para validar a mano)")
        if ref_month and window_start and window_end:
            for o in top[: max(0, args.examples)]:
                _detalle_ejemplo(session, run_id, o, ref_month, window_start, window_end)
        else:
            print("   (sin stats de ventana; ¿run sin datos?)", flush=True)

        _print_header("5) FUNNEL DE DESCARTES (independiente por filtro, sobre candidatos)")
        discard = stats.get("discard") or {}
        cand = stats.get("candidatos") or 0
        labels = {
            "sin_demanda": "Sin demanda (0 meses con suma>0) [pre-candidato]",
            "precio_cero": "Precio no calculable (SUM cantidad=0)  [pre-candidato]",
            "no_identificado": "is_identified = False",
            "efectividad_baja": f"Efectividad < {opp.PARAM_EFECTIVIDAD_MIN:.0%}",
            "monto_bajo": f"Monto < {_fmt_money(opp.PARAM_MONTO_MIN_ARS)}",
            "sin_ganados": "ganados = 0",
        }
        for key, label in labels.items():
            n = discard.get(key, 0)
            pct = f"({n / cand:.1%} de candidatos)" if cand and key not in {"sin_demanda", "precio_cero"} else ""
            print(f"   {label:<52} {n:>8,}  {pct}", flush=True)
        print(f"\n   {'CALIFICAN (pasan los 4 filtros)':<52} {total:>8,}", flush=True)

        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
