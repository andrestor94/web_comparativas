"""API de Oportunidades de Venta (Mercado Privado) — Fase 1B.

Sirve las oportunidades del run activo leyendo la tabla PRECALCULADA
`oportunidades_summary` (NO recalcula al vuelo). Arma el payload CRM por
oportunidad (Capa B), pero NO lo envía a ningún sistema externo: la conexión
real al CRM es una fase futura. Los campos que requieren confirmación del CRM
quedan marcados como PENDIENTE_*.

Paridad SQLite/PG y patrón de run activo (_latest_success_import_run), igual que
Dimensionamiento.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from web_comparativas.policy import require_perm, is_admin, is_manager
from web_comparativas.dimensionamiento.models import (
    CrmEnvio,
    CrmEnvioEvento,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import (
    CRM_ENVIO_PLACEHOLDER,
    OPORTUNIDADES_ENABLED,
    VENTANA_MESES,
    _detectar_ultimo_mes_completo,
    _subtract_months,
    opportunity_stable_id,
)
from web_comparativas.dimensionamiento.query_service import _latest_success_import_run

router = APIRouter(prefix="/api/mercado-privado/oportunidades", tags=["oportunidades"])
logger = logging.getLogger("wc.oportunidades.api")

# Misma key de permiso que la pestaña: gobierna acceso vía can_access.
AllowedUser = Depends(require_perm("mercado_privado.oportunidades"))

_MESES_ES = [
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
]

# Placeholders visibles hasta que se conecte el CRM.
CRM_CURRENCY_PENDIENTE = "PENDIENTE_CRM"
CRM_ASSIGNED_PENDIENTE = "PENDIENTE_MAPEO_CRM"
CRM_LEAD_SOURCE = "SIEM"
CRM_SALES_STAGE = "Prospecting"


def get_db(request: Request) -> Session:
    db = getattr(request.state, "db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="No hay sesión de base de datos disponible.")
    return db


def _require_enabled() -> None:
    if not OPORTUNIDADES_ENABLED():
        raise HTTPException(status_code=404, detail="Módulo Oportunidades deshabilitado.")


def _mes_label(iso_month: str | None) -> str | None:
    """'2025-05' -> 'may-2025'."""
    if not iso_month or len(iso_month) < 7:
        return None
    try:
        y, m = int(iso_month[:4]), int(iso_month[5:7])
        return f"{_MESES_ES[m - 1]}-{y}"
    except (ValueError, IndexError):
        return None


def _fmt_money(value: float | None) -> str:
    return f"${(value or 0):,.0f}".replace(",", ".")


def _fmt_pct(value: float | None) -> str:
    return f"{(value or 0) * 100:.0f}%"


def _fmt_num(value: float | None) -> str:
    v = value or 0
    return f"{v:,.0f}".replace(",", ".") if v == int(v) else f"{v:,.1f}".replace(",", ".")


def _build_crm_payload(o: OportunidadSummary) -> dict[str, Any]:
    """Arma el payload CRM (NO se envía). Campos PENDIENTE_* marcados aparte."""
    producto = (o.producto_nombre or o.codigo_articulo or "").strip()
    cliente = (o.cliente_visible or "Sin cliente").strip()
    plataforma = o.plataforma or "el portal"
    familia = o.familia or "Sin familia"
    unidad = o.unidad_negocio or "Sin unidad"
    consumo = _fmt_num(o.consumo_tipico_mensual)
    rango = f"{_fmt_num(o.consumo_min_mensual)}–{_fmt_num(o.consumo_max_mensual)}"
    ultima = o.ultima_demanda.isoformat() if o.ultima_demanda else "s/d"

    description = (
        f"Oportunidad de venta detectada por SIEM sobre el espacio NO PARTICIPADO en {plataforma}. "
        f"El cliente {cliente} ({o.provincia or 's/provincia'}) demanda «{producto}» "
        f"(cód. {o.codigo_articulo}, familia {familia}) con un consumo típico de {consumo} u/mes "
        f"(rango mensual {rango}), apareciendo en {o.meses_demanda_cliente_12m} de {o.ventana_meses} meses analizados. "
        f"Última demanda: {ultima} ({o.meses_desde_ultima_demanda} meses atrás; estado {o.estado_actividad}). "
        f"Efectividad histórica del producto: {_fmt_pct(o.efectividad)} "
        f"({o.ganados} adjudicaciones ganadas, {o.clientes_distintos} clientes distintos). "
        f"Monto mensual estimado recuperable: {_fmt_money(o.monto_oportunidad)}."
    )

    update_text = (
        f"[SIEM] Negocio: {unidad}. Familia: {familia}. "
        f"Monto mensual estimado: {_fmt_money(o.monto_oportunidad)} "
        f"({consumo} u/mes × {_fmt_money(o.precio_unitario_estimado)}/u). "
        f"Base de cálculo: mediana de la demanda NO participada en la ventana de {o.ventana_meses} meses "
        f"(anclada en el último mes completo). Tipo {o.tipo_oportunidad}, actividad {o.estado_actividad}, "
        f"score {_fmt_money(o.score)}."
    )

    payload = {
        "name": f"SIEM [{o.tipo_oportunidad}] | {producto} | {cliente}",
        "currency_id": CRM_CURRENCY_PENDIENTE,
        "amount": round(float(o.monto_oportunidad or 0), 2),
        "n_cuenta": o.cuit,
        "description": description,
        "lead_source": CRM_LEAD_SOURCE,
        "sales_stage": CRM_SALES_STAGE,
        "assigned_user": CRM_ASSIGNED_PENDIENTE,
        "update_text": update_text,
    }

    # Campos PENDIENTE de confirmación con el equipo del CRM (marcados para la UI).
    pendientes = ["currency_id", "assigned_user"]
    # Campos que dependen del dataset y quedaron vacíos (faltantes reales).
    faltantes: list[str] = []
    if not o.cuit:
        faltantes.append("n_cuenta")
    if not (o.producto_nombre or "").strip():
        faltantes.append("producto_nombre")
    if not (o.cliente_visible or "").strip():
        faltantes.append("cliente_visible")

    return {"payload": payload, "pendientes_crm": pendientes, "faltantes_dataset": faltantes}


def _row_to_dict(o: OportunidadSummary) -> dict[str, Any]:
    crm = _build_crm_payload(o)
    return {
        "id": o.id,
        "oportunidad_id": opportunity_stable_id(o.cliente_visible, o.codigo_articulo),
        "tipo_oportunidad": o.tipo_oportunidad,
        "estado_actividad": o.estado_actividad,
        "cliente_visible": o.cliente_visible,
        "provincia": o.provincia,
        "cuit": o.cuit,
        "producto_nombre": o.producto_nombre,
        "codigo_articulo": o.codigo_articulo,
        "familia": o.familia,
        "unidad_negocio": o.unidad_negocio,
        "plataforma": o.plataforma,
        "consumo_tipico_mensual": o.consumo_tipico_mensual,
        "consumo_min_mensual": o.consumo_min_mensual,
        "consumo_max_mensual": o.consumo_max_mensual,
        "meses_demanda_cliente_12m": o.meses_demanda_cliente_12m,
        "meses_no_participo_12m": o.meses_no_participo_12m,
        "ventana_meses": o.ventana_meses,
        "ultima_demanda": o.ultima_demanda.isoformat() if o.ultima_demanda else None,
        "meses_desde_ultima_demanda": o.meses_desde_ultima_demanda,
        "efectividad": o.efectividad,
        "ganados": o.ganados,
        "comprado_otra": o.comprado_otra,
        "en_espera": o.en_espera,
        "clientes_distintos": o.clientes_distintos,
        "precio_unitario_estimado": o.precio_unitario_estimado,
        "monto_oportunidad": o.monto_oportunidad,
        "score": o.score,
        "crm": crm,
    }


def _window_meta(db: Session, run_id: int) -> dict[str, Any]:
    """Etiqueta de la ventana de demanda vigente (desde el ancla del motor)."""
    anchor = _detectar_ultimo_mes_completo(db, run_id)
    if not anchor:
        return {"label": None, "ref_month": None, "window_start": None, "window_end": None}
    ref_month = anchor["ref_month"]
    window_start = _subtract_months(ref_month, VENTANA_MESES - 1)
    start_label = _mes_label(window_start.strftime("%Y-%m"))
    end_label = _mes_label(ref_month.strftime("%Y-%m"))
    return {
        "label": f"{start_label} a {end_label}" if start_label and end_label else None,
        "ref_month": ref_month.isoformat(),
        "window_start": window_start.isoformat(),
        "window_meses": VENTANA_MESES,
    }


@router.get("/list")
def oportunidades_list(
    request: Request,
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Lista las oportunidades del run activo desde la tabla precalculada."""
    _require_enabled()
    latest = _latest_success_import_run(db)
    if latest is None:
        return {"ok": True, "data": {"run_id": None, "total": 0, "window": {}, "rows": []}}

    rows = db.execute(
        select(OportunidadSummary)
        .where(OportunidadSummary.import_run_id == latest.id)
        .order_by(OportunidadSummary.score.desc())
    ).scalars().all()

    data_rows = [_row_to_dict(o) for o in rows]

    # Estado de envío al CRM: una sola query por todos los oportunidad_id del run.
    # Cada fila lleva `envio` para que la UI refleje quién/cuándo ya la envió.
    ids = [r["oportunidad_id"] for r in data_rows]
    enviados: dict[str, CrmEnvio] = {}
    if ids:
        for e in db.execute(
            select(CrmEnvio).where(CrmEnvio.oportunidad_id.in_(ids))
        ).scalars().all():
            enviados[e.oportunidad_id] = e
    for r in data_rows:
        e = enviados.get(r["oportunidad_id"])
        r["envio"] = (
            {
                "enviado": True,
                "enviado_por": e.enviado_por,
                "enviado_at": e.enviado_at.isoformat() if e.enviado_at else None,
                "crm_status": e.crm_status,
            }
            if e
            else {"enviado": False}
        )

    # Resumen de completitud CRM (para detectar faltantes antes del go-live).
    faltan_cuit = sum(1 for r in data_rows if not r["cuit"])
    completeness = {
        "total": len(data_rows),
        "faltan_n_cuenta": faltan_cuit,
        "faltan_producto": sum(1 for r in data_rows if not (r["producto_nombre"] or "").strip()),
        "faltan_cliente": sum(1 for r in data_rows if not (r["cliente_visible"] or "").strip()),
    }
    logger.info(
        "[OPORTUNIDADES][API] list run_id=%s total=%s faltan_cuit=%s",
        latest.id, len(data_rows), faltan_cuit,
    )
    return {
        "ok": True,
        "data": {
            "run_id": latest.id,
            "total": len(data_rows),
            "window": _window_meta(db, latest.id),
            "completeness": completeness,
            "rows": data_rows,
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Envío al CRM (Feature 1: sello del usuario · Feature 2: control de duplicados)
# ──────────────────────────────────────────────────────────────────────────────

def _enviar_real_a_crm(payload: dict[str, Any]) -> dict[str, Any]:
    """Punto único de envío al CRM. El registro en `crm_envios` se hace JUSTO DESPUÉS
    de que esta función confirme ok=True (ver `oportunidades_enviar`), de modo que el
    control de duplicados solo se active ante envíos efectivos.

    Modo PRUEBA (CRM_ENVIO_PLACEHOLDER on, default mientras la API esté diferida):
      NO llama a ningún sistema externo; devuelve ACK simulado con crm_status='SIMULADO'.
      Las filas quedan marcadas SIMULADO → purgables con scripts/clear_crm_envios.py,
      así se puede ejercitar el flujo de duplicados en la UI sin bloquear de forma
      permanente.

    TODO(CRM): cuando se conecte la API real, poner CRM_ENVIO_PLACEHOLDER=0 y reemplazar
    el cuerpo del bloque `else` por la llamada real, propagando ok=False ante un rechazo
    del CRM (en ese caso NO se registra el envío). El resto del flujo no cambia.
    """
    if CRM_ENVIO_PLACEHOLDER():
        return {"ok": True, "crm_status": "SIMULADO", "crm_id": None}
    # Rama del envío real (diferida): hoy todavía no hay API conectada.
    return {"ok": True, "crm_status": "PENDIENTE_ENVIO_REAL", "crm_id": None}


def _periodo_actual() -> str:
    """YYYYMM de hoy (clave del modo 'por período', hoy solo informativo)."""
    return dt.datetime.utcnow().strftime("%Y%m")


def _msg_ya_enviada(e: CrmEnvio) -> str:
    fecha = e.enviado_at.strftime("%d/%m/%Y") if e.enviado_at else "fecha desconocida"
    return f"Esta oportunidad ya fue enviada al CRM por {e.enviado_por} el {fecha}."


@router.post("/enviar/{summary_id}")
def oportunidades_enviar(
    summary_id: int,
    user=AllowedUser,
    db: Session = Depends(get_db),
    override: bool = False,
):
    """Envía una oportunidad al CRM con sello del usuario y control de duplicados.

    Flujo:
      1. Resuelve la oportunidad del run activo (identidad estable = oportunidad_id).
      2. Si YA fue enviada y NO hay override → NO reenvía; devuelve mensaje claro.
         - override solo lo pueden pedir Admin/Gerente (require_perm ya garantizó el
           acceso a la sección; acá validamos el ROL para el reenvío).
      3. Si no existe (o override autorizado) → sella el payload (enviado_por/at/id),
         intenta el envío real (hoy diferido) y SOLO ante ACK OK registra en
         `crm_envios` (+ bitácora `crm_envio_eventos`).
    """
    _require_enabled()
    latest = _latest_success_import_run(db)
    if latest is None:
        raise HTTPException(status_code=404, detail="No hay corrida activa de oportunidades.")

    o = db.execute(
        select(OportunidadSummary)
        .where(OportunidadSummary.id == summary_id)
        .where(OportunidadSummary.import_run_id == latest.id)
    ).scalars().first()
    if o is None:
        raise HTTPException(status_code=404, detail="Oportunidad no encontrada en la corrida activa.")

    oportunidad_id = opportunity_stable_id(o.cliente_visible, o.codigo_articulo)
    existente = db.execute(
        select(CrmEnvio).where(CrmEnvio.oportunidad_id == oportunidad_id)
    ).scalars().first()

    # ── Control de duplicados ──
    es_override = bool(override) and (is_admin(user) or is_manager(user))
    if existente is not None and not es_override:
        # Bloqueo permanente: NO reenvía. Mensaje claro con quién y cuándo.
        return {
            "ok": False,
            "status": "duplicado",
            "message": _msg_ya_enviada(existente),
            "enviado_por": existente.enviado_por,
            "enviado_at": existente.enviado_at.isoformat() if existente.enviado_at else None,
        }
    if existente is not None and bool(override) and not es_override:
        # Pidió override pero su rol no lo habilita (defensa explícita).
        raise HTTPException(
            status_code=403,
            detail="Solo Admin/Gerente pueden reenviar una oportunidad ya enviada.",
        )

    # ── Feature 1: sello del usuario (server-side, NUNCA del cliente) ──
    enviado_por = getattr(user, "email", None)
    enviado_por_id = getattr(user, "id", None)
    if not enviado_por:
        raise HTTPException(status_code=401, detail="Usuario sin email en la sesión.")
    enviado_at = dt.datetime.utcnow()

    crm = _build_crm_payload(o)
    payload = dict(crm["payload"])
    payload["enviado_por"] = enviado_por
    payload["enviado_por_id"] = enviado_por_id
    payload["enviado_at"] = enviado_at.isoformat()

    # ── Envío real (hoy DIFERIDO). El registro se hace SOLO si el ACK es OK. ──
    ack = _enviar_real_a_crm(payload)
    if not ack.get("ok"):
        raise HTTPException(status_code=502, detail="El CRM rechazó el envío. Reintentá más tarde.")

    crm_status = ack.get("crm_status") or "PENDIENTE_ENVIO_REAL"
    periodo = _periodo_actual()
    payload_snapshot = json.dumps(payload, ensure_ascii=False)

    if existente is None:
        # Primer envío: fila canónica + evento ENVIO.
        db.add(CrmEnvio(
            oportunidad_id=oportunidad_id,
            periodo_yyyymm=periodo,
            cliente_visible=o.cliente_visible,
            cuit=o.cuit,
            codigo_articulo=o.codigo_articulo,
            unidad_negocio=o.unidad_negocio,
            enviado_por=enviado_por,
            enviado_por_id=enviado_por_id,
            enviado_at=enviado_at,
            crm_status=crm_status,
            payload_snapshot=payload_snapshot,
        ))
        evento = "ENVIO"
        status = "enviado"
    else:
        # Override Admin/Gerente: NO se toca la fila canónica (preserva primer emisor);
        # se anota un evento de reenvío en la bitácora (no rompe el UNIQUE).
        evento = "REENVIO_OVERRIDE"
        status = "reenviado_override"

    db.add(CrmEnvioEvento(
        oportunidad_id=oportunidad_id,
        evento=evento,
        periodo_yyyymm=periodo,
        enviado_por=enviado_por,
        enviado_por_id=enviado_por_id,
        enviado_at=enviado_at,
        crm_status=crm_status,
        payload_snapshot=payload_snapshot,
        nota=("override de reenvío" if evento == "REENVIO_OVERRIDE" else None),
    ))
    db.commit()

    logger.info(
        "[OPORTUNIDADES][API] enviar oportunidad_id=%s evento=%s por=%s run_id=%s",
        oportunidad_id, evento, enviado_por, latest.id,
    )
    return {
        "ok": True,
        "status": status,
        "oportunidad_id": oportunidad_id,
        "enviado_por": enviado_por,
        "enviado_at": enviado_at.isoformat(),
        "crm_status": crm_status,
        "payload": payload,
        "pendientes_crm": crm["pendientes_crm"],
        "faltantes_dataset": crm["faltantes_dataset"],
    }
