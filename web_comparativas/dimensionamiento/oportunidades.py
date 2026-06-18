"""Motor de cálculo de Oportunidades de Venta (ventas perdidas recuperables).

Fase 1A: solo el motor + la tabla precalculada `oportunidades_summary`. Sin UI.

Grano: una oportunidad = un par (cliente_visible + codigo_articulo).

Dos agregaciones con ventanas distintas que se combinan por codigo_articulo:
  A) DEMANDA  -> solo filas resultado_participacion = NO_PARTICIPO del par, en los
                 ÚLTIMOS 12 MESES relativos al mes más reciente CON DATOS del run
                 activo (max(fecha) truncado a mes), NO a la fecha de hoy.
  B) EFECTIVIDAD -> TODAS las filas del codigo_articulo (todos los clientes, todos
                 los estados), sobre TODO EL HISTÓRICO del run.

Fuente: dimensionamiento_records del run activo (_latest_success_import_run).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import statistics
from collections import defaultdict
from typing import Any

from sqlalchemy import case, delete, func, select
from sqlalchemy.orm import Session

from web_comparativas.models import IS_POSTGRES

from .models import (
    DimensionamientoRecord,
    OportunidadSummary,
)
from .query_service import _latest_success_import_run  # reutiliza la misma definición de run activo

logger = logging.getLogger("wc.dimensionamiento.oportunidades")

# ──────────────────────────────────────────────────────────────────────────────
# PARÁMETROS — ajustables al tope del módulo (constantes nombradas)
# ──────────────────────────────────────────────────────────────────────────────

# Kill-switch: si está off, rebuild_oportunidades_for_run no hace nada.
def OPORTUNIDADES_ENABLED() -> bool:
    raw = (os.getenv("OPORTUNIDADES_ENABLED") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off", "n"}


def CRM_ENVIO_PLACEHOLDER() -> bool:
    """Modo PRUEBA del envío a CRM (default ON mientras la API real esté diferida).

    Default: ON (no seteado == true). Valores que lo apagan: 0/false/no/off/n.

    Con el flag ON:
      - `_enviar_real_a_crm` NO llama a ningún sistema externo.
      - Los registros en `crm_envios` se marcan con crm_status='SIMULADO' (fácilmente
        borrables con scripts/clear_crm_envios.py).
      - Permite ejercitar el flujo de duplicados en la UI sin enviar nada al CRM y sin
        bloquear oportunidades de forma permanente.

    AL CONECTAR LA API REAL: poner CRM_ENVIO_PLACEHOLDER=0 (false/off) para que entre el
    bridge real y los envíos queden con crm_status definitivo.

    ⚠️ PRODUCCIÓN: si este flag queda en ON en prod, los envíos a CRM NUNCA llegan al
    sistema real — se registran como 'SIMULADO' y se "envían" al vacío, en silencio. Al
    hacer go-live de la integración, asegurarse de setearlo en 0 en el entorno de prod.
    """
    raw = (os.getenv("CRM_ENVIO_PLACEHOLDER") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off", "n"}


# Estados de resultado_participacion (valores reales en los datos)
ESTADO_NO_PARTICIPO = "NO_PARTICIPO"
ESTADO_GANADO = "GANADO"
ESTADO_COMPRADO_OTRA = "COMPRADO_OTRA_EMPRESA"
ESTADO_EN_ESPERA = "EN_ESPERA"

# Ventana de demanda
VENTANA_MESES = 12

# Anclaje de la ventana en el ÚLTIMO MES COMPLETO (no en max(fecha)).
# El umbral de "mes completo" es AUTO-REFERIDO sobre el propio dataset, para que se
# mantenga solo a medida que se cargan más meses (no atado a un año fijo):
#   volumen_referencia = mediana de renglones/mes SOLO sobre los meses cuyo conteo
#                        supera PARAM_MES_COMPLETO_PISO_MIN (excluye meses casi vacíos).
#   PARAM_UMBRAL_MES_COMPLETO = PARAM_MES_COMPLETO_PCT * volumen_referencia.
# Un mes es COMPLETO si su conteo de renglones >= ese umbral; ref_month = el más reciente.
PARAM_MES_COMPLETO_PCT = 0.50      # % de la mediana de referencia que define "completo"
PARAM_MES_COMPLETO_PISO_MIN = 2000  # piso absoluto de renglones/mes para entrar a la mediana

# Clasificación de tipo según meses_con_demanda_12m
PARAM_ESTABLE_MIN = 11       # ESTABLE      si >= 11
PARAM_RECURRENTE_MIN = 6     # RECURRENTE   si 6..10
PARAM_INTERMITENTE_MIN = 3   # INTERMITENTE si 3..5
# PUNTUAL si 1..2

TIPO_ESTABLE = "ESTABLE"
TIPO_RECURRENTE = "RECURRENTE"
TIPO_INTERMITENTE = "INTERMITENTE"
TIPO_PUNTUAL = "PUNTUAL"

TIPO_MULTIPLICADOR = {
    TIPO_ESTABLE: 1.2,
    TIPO_RECURRENTE: 1.0,
    TIPO_INTERMITENTE: 0.7,
    TIPO_PUNTUAL: 0.4,
}

# Actividad
PARAM_RECENCIA_MESES = 6
ESTADO_ACTIVA = "ACTIVA"
ESTADO_DORMIDA = "DORMIDA"
ACTIVIDAD_MULTIPLICADOR = {
    ESTADO_ACTIVA: 1.0,
    ESTADO_DORMIDA: 0.6,
}

# Filtros de calificación
PARAM_EFECTIVIDAD_MIN = 0.30
PARAM_MONTO_MIN_ARS = 1_000_000


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def opportunity_stable_id(cliente_visible: str | None, codigo_articulo: str | None) -> str:
    """ID estable y determinístico de una oportunidad (control de duplicados en CRM).

    Es sha1(cliente_visible | codigo_articulo) normalizado (16 hex). Coincide con el
    GRANO del motor (la clave del dict `pares`), por lo que es el mismo entre corridas.
    DELIBERADAMENTE NO incluye:
      - cuit / unidad_negocio: son atributos del renglón más reciente del par → pueden
        driftear entre recálculos; cuit además es nullable.
      - monto / efectividad / score: cambian en cada corrida.
    Normalización: strip + lower, separador "|" que no aparece en los códigos.

    ⚠️ ADVERTENCIA DE ESTABILIDAD — NO TOCAR A LA LIGERA ⚠️
    El oportunidad_id DEPENDE de cómo se normaliza `cliente_visible`:
      (a) la propia normalización de ESTE hash (strip + lower), y
      (b) la forma en que el ingest/engine produce `cliente_visible` aguas arriba
          (hoy: nombre en MAYÚSCULAS, truncado a ~40 chars).
    CUALQUIER cambio en (a) o en (b) — recortar a otra longitud, sacar/poner acentos,
    cambiar mayúsculas, re-normalizar nombres — genera oportunidad_id DISTINTOS para las
    MISMAS oportunidades. Como `crm_envios` ya tiene ids calculados con la fórmula vieja,
    el control de duplicados dejaría de reconocer los envíos previos EN SILENCIO (no hay
    error; simplemente vuelve a permitir reenviar lo ya enviado). Si alguna vez hay que
    cambiar la normalización, planificar una migración/recálculo de los oportunidad_id en
    crm_envios; no cambiarla sin más.
    """
    cli = (cliente_visible or "").strip().lower()
    cod = (codigo_articulo or "").strip().lower()
    return hashlib.sha1(f"{cli}|{cod}".encode("utf-8")).hexdigest()[:16]


def _month_floor(d: dt.date) -> dt.date:
    return dt.date(d.year, d.month, 1)


def _subtract_months(d: dt.date, months: int) -> dt.date:
    """Primer día del mes resultante de restar `months` meses a `d`."""
    total = (d.year * 12 + (d.month - 1)) - months
    year, month = divmod(total, 12)
    return dt.date(year, month + 1, 1)


def _month_diff(later: dt.date, earlier: dt.date) -> int:
    """Diferencia en meses entre dos fechas (later - earlier), a nivel mes."""
    return (later.year - earlier.year) * 12 + (later.month - earlier.month)


def _clasificar_tipo(meses_con_demanda: int) -> str:
    if meses_con_demanda >= PARAM_ESTABLE_MIN:
        return TIPO_ESTABLE
    if meses_con_demanda >= PARAM_RECURRENTE_MIN:
        return TIPO_RECURRENTE
    if meses_con_demanda >= PARAM_INTERMITENTE_MIN:
        return TIPO_INTERMITENTE
    return TIPO_PUNTUAL


def _resolver_run_id(session: Session, run_id: int | None) -> int | None:
    if run_id is not None:
        return run_id
    latest = _latest_success_import_run(session)
    return latest.id if latest else None


def _month_label_expr(column):
    """Etiqueta 'YYYY-MM' del mes, dialect-aware (SQLite/PostgreSQL)."""
    if IS_POSTGRES:
        return func.to_char(column, "YYYY-MM")
    return func.strftime("%Y-%m", column)


def _detectar_ultimo_mes_completo(session: Session, run_id: int) -> dict[str, Any] | None:
    """Detecta el último mes COMPLETO del run con umbral AUTO-REFERIDO.

    volumen_referencia = mediana de renglones/mes sobre los meses con conteo >=
    PARAM_MES_COMPLETO_PISO_MIN (excluye meses casi vacíos del cálculo).
    umbral = PARAM_MES_COMPLETO_PCT * volumen_referencia.
    ref_month = primer día del mes COMPLETO más reciente. Si ninguno califica como
    completo (dataset chico), cae al último mes con datos.
    """
    R = DimensionamientoRecord
    label = _month_label_expr(R.fecha)
    rows = session.execute(
        select(label, func.count(R.id))
        .where(R.import_run_id == run_id)
        .where(R.fecha.is_not(None))
        .group_by(label)
        .order_by(label)
    ).all()
    meses = [(str(m), int(c)) for m, c in rows if m]
    if not meses:
        return None

    counts_ref = [c for _, c in meses if c >= PARAM_MES_COMPLETO_PISO_MIN]
    if not counts_ref:
        counts_ref = [c for _, c in meses]
    volumen_referencia = float(statistics.median(counts_ref))
    umbral = volumen_referencia * PARAM_MES_COMPLETO_PCT

    clasificacion = [
        {"mes": m, "renglones": c, "estado": "COMPLETO" if c >= umbral else "PARCIAL"}
        for m, c in meses
    ]
    completos = [row["mes"] for row in clasificacion if row["estado"] == "COMPLETO"]
    ultimo = completos[-1] if completos else meses[-1][0]
    ref_month = dt.date(int(ultimo[:4]), int(ultimo[5:7]), 1)
    return {
        "ref_month": ref_month,
        "umbral": umbral,
        "volumen_referencia": volumen_referencia,
        "clasificacion": clasificacion,
        "ultimo_mes_completo": ultimo,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Cálculo de efectividad por codigo_articulo (histórico completo del run)
# ──────────────────────────────────────────────────────────────────────────────

def _efectividad_por_codigo(session: Session, run_id: int) -> dict[str, dict[str, Any]]:
    R = DimensionamientoRecord
    stmt = (
        select(
            R.codigo_articulo,
            func.coalesce(func.sum(case((R.resultado_participacion == ESTADO_GANADO, 1), else_=0)), 0),
            func.coalesce(func.sum(case((R.resultado_participacion == ESTADO_COMPRADO_OTRA, 1), else_=0)), 0),
            func.coalesce(func.sum(case((R.resultado_participacion == ESTADO_EN_ESPERA, 1), else_=0)), 0),
            func.count(func.distinct(R.cliente_visible)),
        )
        .where(R.import_run_id == run_id)
        .where(R.codigo_articulo.is_not(None))
        .where(R.codigo_articulo != "")
        .group_by(R.codigo_articulo)
    )
    out: dict[str, dict[str, Any]] = {}
    for codigo, ganados, comprado_otra, en_espera, clientes in session.execute(stmt):
        ganados = int(ganados or 0)
        comprado_otra = int(comprado_otra or 0)
        en_espera = int(en_espera or 0)
        denom = ganados + comprado_otra + en_espera
        efectividad = (ganados / denom) if denom > 0 else 0.0
        out[codigo] = {
            "ganados": ganados,
            "comprado_otra": comprado_otra,
            "en_espera": en_espera,
            "clientes_distintos": int(clientes or 0),
            "efectividad": efectividad,
        }
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Cálculo principal
# ──────────────────────────────────────────────────────────────────────────────

def computar_oportunidades(session: Session, run_id: int) -> dict[str, Any]:
    """Calcula las oportunidades del run (sin escribir en la tabla).

    Devuelve {
        "rows": [dict por oportunidad calificada],
        "stats": {funnel de descartes + universo de candidatos + meta},
    }
    """
    R = DimensionamientoRecord

    # Mes de referencia = ÚLTIMO MES COMPLETO del run (NO max(fecha), NO hoy). La
    # ventana de 12 meses se cuenta hacia atrás desde ahí y se cierra en ref_month,
    # de modo que los meses parciales más recientes (a medio cargar) NO la contaminan.
    max_fecha = session.execute(
        select(func.max(R.fecha)).where(R.import_run_id == run_id)
    ).scalar()
    if max_fecha is None:
        return {"rows": [], "stats": {"reason": "run_sin_datos", "run_id": run_id}}
    if isinstance(max_fecha, dt.datetime):
        max_fecha = max_fecha.date()

    anchor = _detectar_ultimo_mes_completo(session, run_id)
    ref_month = anchor["ref_month"] if anchor else _month_floor(max_fecha)
    window_start = _subtract_months(ref_month, VENTANA_MESES - 1)
    # Fin de ventana EXCLUSIVO = primer día del mes siguiente a ref_month.
    window_end = _subtract_months(ref_month, -1)

    # Efectividad por codigo (histórico completo).
    efectividad_map = _efectividad_por_codigo(session, run_id)

    # ── Pase 1: DEMANDA NO_PARTICIPO (define la oportunidad y el monto recuperable) ──
    # El universo de pares = pares con demanda NO_PARTICIPO. consumo/precio/monto se
    # calculan SOLO con estas filas.
    np_stmt = (
        select(
            R.cliente_visible,
            R.codigo_articulo,
            R.fecha,
            R.cantidad_demandada,
            R.valorizacion_estimada,
        )
        .where(R.import_run_id == run_id)
        .where(R.resultado_participacion == ESTADO_NO_PARTICIPO)
        .where(R.fecha >= window_start)
        .where(R.fecha < window_end)
        .where(R.codigo_articulo.is_not(None))
        .where(R.codigo_articulo != "")
        .where(R.cliente_visible.is_not(None))
        .where(R.cliente_visible != "")
    )

    pares: dict[tuple[str, str], dict[str, Any]] = {}
    for cliente, codigo, fecha, cantidad, valorizacion in session.execute(np_stmt):
        if isinstance(fecha, dt.datetime):
            fecha = fecha.date()
        cantidad = float(cantidad or 0)
        valorizacion = float(valorizacion or 0)
        key = (cliente, codigo)
        par = pares.get(key)
        if par is None:
            par = {
                "cliente_visible": cliente,
                "codigo_articulo": codigo,
                "np_monthly": defaultdict(float),   # "YYYY-MM" -> sum(cantidad) NO_PARTICIPO
                "cli_monthly": defaultdict(float),   # "YYYY-MM" -> sum(cantidad) TODOS los estados
                "total_cant": 0.0,                   # NO_PARTICIPO
                "total_val": 0.0,                    # NO_PARTICIPO
                "ultima_demanda": None,              # all-states (se setea en pase 2)
                "_attr_fecha": None,                 # all-states
                "cuit": None,
                "provincia": None,
                "producto_nombre": None,
                "familia": None,
                "unidad_negocio": None,
                "plataforma": None,
                "is_identified": False,
            }
            pares[key] = par
        month_key = f"{fecha.year:04d}-{fecha.month:02d}"
        par["np_monthly"][month_key] += cantidad
        par["total_cant"] += cantidad
        par["total_val"] += valorizacion

    # ── Pase 2: DEMANDA DEL CLIENTE en TODOS los estados (recurrencia + actividad) ──
    # Solo enriquece pares ya presentes (los que tienen NO_PARTICIPO). Define
    # meses_demanda_cliente_12m, ultima_demanda y los atributos representativos.
    all_stmt = (
        select(
            R.cliente_visible,
            R.codigo_articulo,
            R.fecha,
            R.cantidad_demandada,
            R.cuit,
            R.provincia,
            R.producto_nombre_original,
            R.descripcion_articulo,
            R.familia,
            R.unidad_negocio,
            R.plataforma,
            R.is_identified,
        )
        .where(R.import_run_id == run_id)
        .where(R.fecha >= window_start)
        .where(R.fecha < window_end)
        .where(R.codigo_articulo.is_not(None))
        .where(R.codigo_articulo != "")
        .where(R.cliente_visible.is_not(None))
        .where(R.cliente_visible != "")
    )
    for row in session.execute(all_stmt):
        (
            cliente, codigo, fecha, cantidad,
            cuit, provincia, prod_orig, desc_art, familia, unidad, plataforma, is_identified,
        ) = row
        key = (cliente, codigo)
        par = pares.get(key)
        if par is None:
            continue  # par sin demanda NO_PARTICIPO -> no es oportunidad
        if isinstance(fecha, dt.datetime):
            fecha = fecha.date()
        cantidad = float(cantidad or 0)
        month_key = f"{fecha.year:04d}-{fecha.month:02d}"
        par["cli_monthly"][month_key] += cantidad
        if par["ultima_demanda"] is None or fecha > par["ultima_demanda"]:
            par["ultima_demanda"] = fecha
        # Atributos representativos = renglón más reciente del par (cualquier estado).
        if par["_attr_fecha"] is None or fecha >= par["_attr_fecha"]:
            par["_attr_fecha"] = fecha
            par["cuit"] = cuit
            par["provincia"] = provincia
            par["producto_nombre"] = prod_orig or desc_art
            par["familia"] = familia
            par["unidad_negocio"] = unidad
            par["plataforma"] = plataforma
            par["is_identified"] = bool(is_identified)

    # Funnel de descartes (contados de forma INDEPENDIENTE sobre el universo de
    # candidatos = pares con demanda>0 y precio>0).
    discard = {
        "sin_demanda": 0,        # ningún mes con suma > 0
        "precio_cero": 0,        # SUM(cantidad)=0 -> precio no calculable
        "no_identificado": 0,    # is_identified = False
        "efectividad_baja": 0,   # efectividad < PARAM_EFECTIVIDAD_MIN
        "monto_bajo": 0,         # monto_oportunidad < PARAM_MONTO_MIN_ARS
        "sin_ganados": 0,        # ganados == 0
    }
    candidatos = 0
    rows: list[dict[str, Any]] = []

    for (cliente, codigo), par in pares.items():
        # Demanda NO_PARTICIPO -> monto recuperable (NO cambia).
        np_sums = [v for v in par["np_monthly"].values() if v > 0]
        meses_no_participo = len(np_sums)
        if meses_no_participo == 0:
            discard["sin_demanda"] += 1
            continue

        total_cant = par["total_cant"]
        total_val = par["total_val"]
        # Precio unitario estimado, manejando división por cero explícitamente.
        if total_cant <= 0:
            discard["precio_cero"] += 1
            continue
        precio_unitario = total_val / total_cant
        if precio_unitario <= 0:
            discard["precio_cero"] += 1
            continue

        # A partir de acá es un CANDIDATO (tiene demanda y precio válido).
        candidatos += 1

        consumo_tipico = float(statistics.median(np_sums))
        consumo_min = float(min(np_sums))
        consumo_max = float(max(np_sums))
        monto = consumo_tipico * precio_unitario

        # Demanda del cliente (TODOS los estados) -> clasifica el tipo (recurrencia real).
        cli_sums = [v for v in par["cli_monthly"].values() if v > 0]
        meses_demanda_cliente = len(cli_sums)

        eff = efectividad_map.get(codigo, {
            "ganados": 0, "comprado_otra": 0, "en_espera": 0,
            "clientes_distintos": 0, "efectividad": 0.0,
        })
        ganados = eff["ganados"]
        efectividad = eff["efectividad"]

        tipo = _clasificar_tipo(meses_demanda_cliente)
        tipo_mult = TIPO_MULTIPLICADOR[tipo]

        # Actividad y última demanda sobre TODOS los estados (cuán reciente es la necesidad).
        ultima_demanda = par["ultima_demanda"]
        meses_desde = _month_diff(ref_month, _month_floor(ultima_demanda))
        estado_act = ESTADO_ACTIVA if meses_desde <= PARAM_RECENCIA_MESES else ESTADO_DORMIDA
        act_mult = ACTIVIDAD_MULTIPLICADOR[estado_act]

        score = monto * efectividad * tipo_mult * act_mult

        # Filtros (contados independientemente).
        fail_identified = not par["is_identified"]
        fail_eff = efectividad < PARAM_EFECTIVIDAD_MIN
        fail_monto = monto < PARAM_MONTO_MIN_ARS
        fail_ganados = ganados <= 0
        if fail_identified:
            discard["no_identificado"] += 1
        if fail_eff:
            discard["efectividad_baja"] += 1
        if fail_monto:
            discard["monto_bajo"] += 1
        if fail_ganados:
            discard["sin_ganados"] += 1

        if fail_identified or fail_eff or fail_monto or fail_ganados:
            continue

        rows.append({
            "import_run_id": run_id,
            "codigo_articulo": codigo,
            "cliente_visible": cliente,
            "cuit": par["cuit"],
            "provincia": par["provincia"],
            "producto_nombre": par["producto_nombre"],
            "familia": par["familia"],
            "unidad_negocio": par["unidad_negocio"],
            "plataforma": par["plataforma"],
            "tipo_oportunidad": tipo,
            "estado_actividad": estado_act,
            "meses_demanda_cliente_12m": meses_demanda_cliente,
            "meses_no_participo_12m": meses_no_participo,
            "ventana_meses": VENTANA_MESES,
            "consumo_tipico_mensual": consumo_tipico,
            "consumo_min_mensual": consumo_min,
            "consumo_max_mensual": consumo_max,
            "ultima_demanda": ultima_demanda,
            "meses_desde_ultima_demanda": meses_desde,
            "precio_unitario_estimado": precio_unitario,
            "monto_oportunidad": monto,
            "efectividad": efectividad,
            "ganados": ganados,
            "comprado_otra": eff["comprado_otra"],
            "en_espera": eff["en_espera"],
            "clientes_distintos": eff["clientes_distintos"],
            "tipo_multiplicador": tipo_mult,
            "multiplicador_actividad": act_mult,
            "score": score,
        })

    stats = {
        "run_id": run_id,
        "ref_month": ref_month.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),  # exclusivo
        "max_fecha": max_fecha.isoformat(),
        "anchor_mode": "ultimo_mes_completo" if anchor else "max_fecha_fallback",
        "umbral_mes_completo": (anchor["umbral"] if anchor else None),
        "volumen_referencia": (anchor["volumen_referencia"] if anchor else None),
        "meses_clasificacion": (anchor["clasificacion"] if anchor else None),
        "pares_no_participo": len(pares),
        "candidatos": candidatos,
        "calificadas": len(rows),
        "discard": discard,
    }
    return {"rows": rows, "stats": stats}


# ──────────────────────────────────────────────────────────────────────────────
# Rebuild run-scoped (mismo patrón que _rebuild_summary_for_run)
# ──────────────────────────────────────────────────────────────────────────────

def rebuild_oportunidades_for_run(
    session: Session,
    run_id: int | None = None,
    *,
    commit: bool = True,
) -> dict[str, Any]:
    """Borra las oportunidades del run e inserta las calculadas. Idempotente.

    Si OPORTUNIDADES_ENABLED está off, no toca nada. Respeta el run activo si
    run_id es None.
    """
    if not OPORTUNIDADES_ENABLED():
        logger.info("[OPORTUNIDADES] kill-switch OFF (OPORTUNIDADES_ENABLED) — rebuild omitido.")
        return {"status": "disabled", "rows": 0}

    target_run_id = _resolver_run_id(session, run_id)
    if target_run_id is None:
        logger.warning("[OPORTUNIDADES] No hay run activo (success). Rebuild omitido.")
        return {"status": "no_run", "rows": 0}

    logger.info("[OPORTUNIDADES] Rebuild start run_id=%s", target_run_id)
    result = computar_oportunidades(session, target_run_id)
    rows = result["rows"]
    stats = result["stats"]
    logger.info(
        "[OPORTUNIDADES] run_id=%s anchor=%s ref_month=%s ventana=%s..%s (excl) "
        "umbral_mes_completo=%s max_fecha=%s",
        target_run_id, stats.get("anchor_mode"), stats.get("ref_month"),
        stats.get("window_start"), stats.get("window_end"),
        stats.get("umbral_mes_completo"), stats.get("max_fecha"),
    )

    # Borrado run-scoped + inserción.
    session.execute(
        delete(OportunidadSummary).where(OportunidadSummary.import_run_id == target_run_id)
    )
    if rows:
        session.execute(OportunidadSummary.__table__.insert(), rows)
    if commit:
        session.commit()

    inserted = int(
        session.query(func.count(OportunidadSummary.id))
        .filter_by(import_run_id=target_run_id)
        .scalar()
        or 0
    )
    logger.info(
        "[OPORTUNIDADES] Rebuild done run_id=%s calificadas=%s candidatos=%s",
        target_run_id, inserted, stats.get("candidatos"),
    )
    return {"status": "ok", "rows": inserted, "stats": stats}
