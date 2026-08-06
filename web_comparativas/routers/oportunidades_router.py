"""API de Oportunidades de Venta (Mercado Privado) — Fase 1B + envío real al CRM.

Sirve las oportunidades del run activo leyendo la tabla PRECALCULADA
`oportunidades_summary` (NO recalcula al vuelo) y arma el payload CRM por
oportunidad (Capa B).

ENVÍO AL CRM (SuiteCRM V8): el circuito vive en `dimensionamiento/crm_client.py`;
acá sólo se decide QUÉ se manda, se sella con el usuario y se persiste el
resultado. Dos modos, gobernados por env:
  - CRM_ENVIO_PLACEHOLDER=1 (default): simula, no toca el CRM (crm_status SIMULADO).
  - CRM_ENVIO_PLACEHOLDER=0: envío real contra el CRM que indique CRM_MODO (test|prod).

El cliente se identifica en el CRM por su Nº de cuenta de FUSION (dataset:
`cuenta_interna`), NO por el cuit. Ante cualquier rechazo del CRM el envío NO se
registra, de modo que la oportunidad queda libre para reintentar.

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

from web_comparativas.policy import require_perm, is_admin
from web_comparativas.dimensionamiento import crm_client
from web_comparativas.dimensionamiento.crm_client import CrmError
from web_comparativas.dimensionamiento.models import (
    CrmEnvio,
    CrmEnvioEvento,
    DimensionamientoRecord,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import (
    CRM_ENVIO_PLACEHOLDER,
    OPORTUNIDADES_ENABLED,
    VENTANA_MESES,
    _detectar_ultimo_mes_completo,
    _subtract_months,
    normalizar_cuenta_fusion,
    opportunity_stable_id,
)
from web_comparativas.dimensionamiento.query_service import _latest_success_import_run

router = APIRouter(prefix="/api/mercado-privado/oportunidades", tags=["oportunidades"])
logger = logging.getLogger("wc.oportunidades.api")

# Misma key de permiso que la pestaña: gobierna acceso vía can_access.
_perm_oportunidades = require_perm("mercado_privado.oportunidades")
AllowedUser = Depends(_perm_oportunidades)

# Gate de ESCRITURA (regla vigente del proyecto, jul-2026): en visualización Gerente
# se iguala a Auditor, pero en escritura queda AFUERA. Enviar a CRM es escritura →
# admin/analista/supervisor. (El diseño de junio permitía override Admin/Gerente;
# se adapta: override solo Admin.)
_WRITE_ROLES = {"admin", "administrator", "administrador", "analista", "analyst", "supervisor"}

# Quién puede ELEGIR a quién se asigna la oportunidad en el CRM (ago-2026):
#   Analista            -> solo a sí mismo. No ve el selector.
#   Supervisor o superior -> elige entre los usuarios del CRM.
# El analista trabaja su propia cartera: dejarlo asignar a un tercero le permitiría
# mover trabajo (y comisión) sin control. El supervisor sí distribuye, es su función.
_ROLES_ANALISTA = {"analista", "analyst"}


def _rol(user) -> str:
    return (getattr(user, "role", "") or "").strip().lower()


def _puede_elegir_asignado(user) -> bool:
    """True para supervisor o superior; False para analista."""
    return _rol(user) not in _ROLES_ANALISTA


def _require_oportunidades_write(request: Request):
    user = _perm_oportunidades(request)
    role = (getattr(user, "role", "") or "").strip().lower()
    if role not in _WRITE_ROLES:
        raise HTTPException(
            status_code=403,
            detail="Solo lectura: tu rol no puede enviar oportunidades al CRM.",
        )
    return user


AllowedWriter = Depends(_require_oportunidades_write)

_MESES_ES = [
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
]

def _modo_envio_actual() -> str:
    """Entorno al que iría un envío HECHO AHORA: 'simulado' | 'test' | 'prod'.

    Es la segunda mitad de la clave de duplicados (oportunidad_id, crm_modo), así que
    se calcula ANTES de consultar si ya se envió: el bloqueo se evalúa contra el mismo
    entorno al que se está por mandar, no contra todos.
    """
    return "simulado" if CRM_ENVIO_PLACEHOLDER() else crm_client.CRM_MODO()


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


def _texto_asignado(asignado: dict[str, str] | None) -> str | None:
    """Cómo se muestra el usuario asignado en el modal.

    Match automático: "asignado a vos (<usuario>)" — quien envía es quien recibe.
    Selección manual: se dice EXPLÍCITAMENTE que la eligió quien envía, porque significa
    que la oportunidad NO queda en sus manos y eso hay que verlo antes de confirmar, no
    descubrirlo después en el CRM.

    Sin asignar devuelve None, NUNCA un texto de error: `assigned_user` es un campo del
    payload, y meterle una frase tipo "no se pudo resolver" haría que un mensaje de error
    viaje como si fuera un dato.
    """
    if not asignado or not asignado.get("usuario"):
        return None
    if asignado.get("origen") == "manual":
        return f"asignado a {asignado['usuario']} (selección manual)"
    return f"asignado a vos ({asignado['usuario']})"


def _build_crm_payload(
    o: OportunidadSummary,
    cuenta_fusion: str | None = None,
    asignado: dict[str, str] | None = None,
    *,
    usuarios_disponibles: bool = False,
    error_crm: str | None = None,
    puede_elegir: bool = True,
    email_siem: str | None = None,
    momento: dt.datetime | None = None,
) -> dict[str, Any]:
    """Arma el payload CRM. `cuenta_fusion` es el Nº de cuenta de FUSION del cliente
    (dataset: `cuenta_interna`), que es con lo que el CRM resuelve la cuenta. Si no se
    pasa, se toma de la propia fila del summary. `asignado` es el usuario decidido
    ({id, usuario, origen}): el match automático con quien envía, o su selección manual.

    `usuarios_disponibles` distingue los dos motivos por los que puede no haber asignado:
    si hay lista de usuarios, falta ELEGIR (se resuelve en el modal); si no la hay, el
    CRM no responde y el envío queda bloqueado.
    """
    cuenta_fusion = normalizar_cuenta_fusion(
        cuenta_fusion if cuenta_fusion is not None else getattr(o, "cuenta_interna", None)
    )
    producto = (o.producto_nombre or o.codigo_articulo or "").strip()
    cliente = (o.cliente_visible or "Sin cliente").strip()
    plataforma = o.plataforma or "el portal"
    familia = o.familia or "Sin familia"
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

    # Bitácora: corta a propósito. Todo el detalle de negocio ya está en `description`;
    # acá solo va lo que no está en ningún otro lado (quién envió, a quién, cuándo).
    # Es EXACTAMENTE el texto que se manda: el mismo helper lo arma para el modal y para
    # el envío, así que la vista previa no puede quedar diciendo otra cosa.
    update_text = (
        crm_client.texto_bitacora(email_siem, asignado, momento or dt.datetime.utcnow())
        if asignado else None
    )

    payload = {
        "name": f"SIEM [{o.tipo_oportunidad}] | {producto} | {cliente}",
        "currency_id": crm_client.CRM_CURRENCY_ID,
        "amount": round(float(o.monto_oportunidad or 0), 2),
        # Nº de cuenta de FUSION: con esto el CRM resuelve el account_id (paso 3).
        # OJO: NO es el cuit — el endpoint Cuentas_por_numero_fusion espera n_cuenta_c.
        "n_cuenta": cuenta_fusion,
        "description": description,
        "tipooportunidad_c": crm_client.CRM_TIPO_OPORTUNIDAD,
        "lead_source": crm_client.CRM_LEAD_SOURCE,
        "sales_stage": crm_client.CRM_SALES_STAGE,
        # Vínculo BIDIRECCIONAL con el CRM: `sistema_origen_c` marca de dónde vino la
        # oportunidad y `id_sistema_origen_c` es la identidad estable de SIEM con la que
        # el CRM arma su botón "Ver en SIEM". Se usa el id estable
        # (sha1(cliente|codigo)) y NO el `id` de la fila del summary, que es run-scoped
        # y cambia en cada recálculo mensual: un link armado con ese id se rompería solo.
        "sistema_origen_c": crm_client.CRM_SISTEMA_ORIGEN,
        "id_sistema_origen_c": opportunity_stable_id(o.cliente_visible, o.codigo_articulo),
        "assigned_user": _texto_asignado(asignado),
        "update_text": update_text,
    }

    # Sin pendientes: currency_id va en duro ("-99") y assigned_user llega resuelto.
    pendientes: list[str] = []
    # Campos que dependen del dataset y quedaron vacíos (faltantes reales).
    faltantes: list[str] = []
    if not cuenta_fusion:
        faltantes.append("n_cuenta")
    if not (o.producto_nombre or "").strip():
        faltantes.append("producto_nombre")
    if not (o.cliente_visible or "").strip():
        faltantes.append("cliente_visible")

    # BLOQUEOS: datos sin los cuales el envío NO puede salir, y que el usuario NO puede
    # resolver desde el modal. La UI deshabilita el botón y muestra el motivo; el backend
    # vuelve a chequearlo (nunca se confía en el cliente).
    bloqueos: list[str] = []
    if not cuenta_fusion:
        bloqueos.append(
            "Esta oportunidad no tiene número de cuenta de fusión en el dataset, "
            "que es el dato con el que el CRM identifica al cliente."
        )
    # Distinto de un bloqueo: falta ELEGIR a quién asignar, y eso sí se resuelve en el
    # modal con el selector. Solo aplica a quien puede elegir (supervisor o superior).
    requiere_asignacion = False
    if payload["assigned_user"] is None:
        if not puede_elegir:
            # Analista sin match: no tiene a quién asignar y tampoco puede elegir. Se
            # corta con el motivo REAL (su usuario no existe en el CRM) en vez de
            # dejarlo con un botón muerto sin explicación.
            bloqueos.append(
                "Tu usuario no está dado de alta en el CRM, así que no podés enviar "
                "esta oportunidad. Pedí el alta al equipo del CRM, o que la envíe un "
                "supervisor."
            )
        elif usuarios_disponibles:
            requiere_asignacion = True
        else:
            bloqueos.append(
                "No se pudo obtener la lista de usuarios del CRM para asignar la "
                "oportunidad" + (f": {error_crm}" if error_crm else ".")
            )

    return {
        "payload": payload,
        "pendientes_crm": pendientes,
        "faltantes_dataset": faltantes,
        "bloqueos": bloqueos,
        "requiere_asignacion": requiere_asignacion,
        # La UI solo dibuja el selector si esto es True; el backend lo revalida igual.
        "puede_elegir": puede_elegir,
    }


def _cuentas_fusion_desde_records(
    db: Session,
    run_id: int,
    clientes: list[str],
) -> dict[str, str]:
    """Mapa cliente_visible -> Nº de cuenta de FUSION leído de dimensionamiento_records.

    FALLBACK para summaries construidos ANTES de que `cuenta_interna` existiera en
    `oportunidades_summary`: sin esto, habría que reconstruir el summary sí o sí para
    poder enviar. Se resuelve en UNA query por request (no N+1) y solo para los clientes
    que hagan falta. Si un cliente tuviera más de una cuenta en el run, se descarta por
    ambiguo (mejor pedir el dato que mandar la cuenta equivocada al CRM).
    """
    if not clientes:
        return {}
    R = DimensionamientoRecord
    filas = db.execute(
        select(R.cliente_visible, R.cuenta_interna)
        .where(R.import_run_id == run_id)
        .where(R.cliente_visible.in_(clientes))
        .where(R.cuenta_interna.is_not(None))
        .distinct()
    ).all()

    candidatas: dict[str, set[str]] = {}
    for cliente, cuenta in filas:
        normalizada = normalizar_cuenta_fusion(cuenta)
        if normalizada:
            candidatas.setdefault(cliente, set()).add(normalizada)
    salida: dict[str, str] = {}
    for cliente, cuentas in candidatas.items():
        if len(cuentas) == 1:
            salida[cliente] = next(iter(cuentas))
        else:
            logger.warning(
                "[OPORTUNIDADES] cliente '%s' con %s cuentas de fusión distintas: %s",
                cliente, len(cuentas), sorted(cuentas),
            )
    return salida


def _resolver_cuenta_fusion(db: Session, o: OportunidadSummary, run_id: int) -> str | None:
    """Cuenta de fusión de UNA oportunidad: la del summary o, si falta, la de records."""
    directa = normalizar_cuenta_fusion(getattr(o, "cuenta_interna", None))
    if directa:
        return directa
    return _cuentas_fusion_desde_records(db, run_id, [o.cliente_visible]).get(o.cliente_visible)


def _contexto_asignacion_seguro(email: str | None, puede_elegir: bool = True) -> dict[str, Any]:
    """Contexto de asignación para la VISTA PREVIA, sin poder romper la página.

    Devuelve {"match", "usuarios", "sugerido_id", "error"}. Se corre una sola vez por
    request (depende del usuario logueado, no de la fila). Dos cuidados deliberados:
      - Si falta configuración del CRM NO se sale a la red: sin esto, cada carga de la
        página pagaría un timeout de conexión antes de fallar.
      - Cualquier CrmError se traga: que el CRM esté caído tiene que bloquear el envío
        con un motivo claro, no dejar sin listado a todo el módulo.
    """
    vacio = {"match": None, "usuarios": [], "sugerido_id": None, "error": None,
             "puede_elegir": puede_elegir, "email": email}
    try:
        crm_client.crm_config()
    except CrmError as exc:
        return {**vacio, "error": exc.mensaje}
    try:
        ctx = {**crm_client.contexto_asignacion(email), "error": None,
               "puede_elegir": puede_elegir, "email": email}
    except CrmError as exc:
        logger.warning("[OPORTUNIDADES][API] no se pudo leer usuarios del CRM: %s", exc.mensaje)
        return {**vacio, "error": exc.mensaje}
    if not puede_elegir:
        # El analista no elige: no tiene sentido mandarle 82 usuarios al navegador,
        # y no exponerlos evita que un cliente modificado los ofrezca igual.
        ctx["usuarios"] = []
        ctx["sugerido_id"] = None
    return ctx


def _bitacoras_por_usuario(ctx: dict[str, Any], momento: dt.datetime) -> dict[str, str]:
    """Texto de bitácora ya armado para CADA usuario elegible del selector.

    Se precalcula server-side (son strings cortos, y la lista completa son 82) para que
    el modal muestre el texto EXACTO al cambiar la selección, sin que el front tenga que
    reimplementar el formato: si el front lo compusiera por su cuenta, cualquier retoque
    del texto habría que hacerlo en dos lados y tarde o temprano divergirían.
    """
    match = ctx.get("match") or {}
    email = ctx.get("email")
    salida: dict[str, str] = {}
    for u in ctx.get("usuarios") or []:
        origen = "match" if u["id"] == match.get("id") else "manual"
        salida[u["id"]] = crm_client.texto_bitacora(
            email, {**u, "origen": origen}, momento,
        )
    return salida


def _row_to_dict(
    o: OportunidadSummary,
    cuenta_fusion: str | None = None,
    ctx: dict[str, Any] | None = None,
    momento: dt.datetime | None = None,
) -> dict[str, Any]:
    ctx = ctx or {}
    crm = _build_crm_payload(
        o, cuenta_fusion, ctx.get("match"),
        usuarios_disponibles=bool(ctx.get("usuarios")),
        error_crm=ctx.get("error"),
        puede_elegir=bool(ctx.get("puede_elegir", True)),
        email_siem=ctx.get("email"),
        momento=momento,
    )
    return {
        "id": o.id,
        "oportunidad_id": opportunity_stable_id(o.cliente_visible, o.codigo_articulo),
        "tipo_oportunidad": o.tipo_oportunidad,
        "estado_actividad": o.estado_actividad,
        "cliente_visible": o.cliente_visible,
        "provincia": o.provincia,
        "cuit": o.cuit,
        "cuenta_fusion": crm["payload"].get("n_cuenta"),
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

    # Cuenta de fusión: la del summary; para las filas viejas (summary anterior a la
    # columna) se resuelve en UNA query contra records, no por fila.
    sin_cuenta = [
        o.cliente_visible
        for o in rows
        if not normalizar_cuenta_fusion(getattr(o, "cuenta_interna", None)) and o.cliente_visible
    ]
    respaldo = _cuentas_fusion_desde_records(db, latest.id, sorted(set(sin_cuenta)))
    # Contexto de asignación: UNA consulta al CRM por request (no por fila) — depende
    # del usuario logueado. Trae el match automático Y la lista completa de usuarios,
    # que es la que alimenta el selector cuando no hay match.
    ctx = _contexto_asignacion_seguro(
        getattr(_user, "email", None), _puede_elegir_asignado(_user)
    )
    # Un único instante para toda la respuesta: si cada fila tomara su propia hora, dos
    # filas de la misma pantalla podrían mostrar minutos distintos en la bitácora.
    ahora = dt.datetime.utcnow()
    data_rows = [
        _row_to_dict(
            o,
            normalizar_cuenta_fusion(getattr(o, "cuenta_interna", None))
            or respaldo.get(o.cliente_visible),
            ctx,
            ahora,
        )
        for o in rows
    ]

    # Estado de envío al CRM: una sola query por todos los oportunidad_id del run.
    # Cada fila lleva `envio` para que la UI refleje quién/cuándo ya la envió.
    # El estado "enviada" es POR ENTORNO: se mira solo el modo al que se enviaría ahora.
    # Si no se filtrara, una oportunidad probada en TEST aparecería bloqueada al operar
    # en PROD, que es justo lo que el bloqueo por entorno viene a evitar.
    modo_actual = _modo_envio_actual()
    ids = [r["oportunidad_id"] for r in data_rows]
    enviados: dict[str, CrmEnvio] = {}
    if ids:
        for e in db.execute(
            select(CrmEnvio)
            .where(CrmEnvio.oportunidad_id.in_(ids))
            .where(CrmEnvio.crm_modo == modo_actual)
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
                # Con crm_id la UI cambia "Enviar a CRM" por "Ver en CRM".
                "crm_id": e.crm_id,
                "crm_url": crm_client.crm_detail_url(e.crm_id) if e.crm_id else None,
                "crm_modo": e.crm_modo,
            }
            if e
            else {"enviado": False}
        )

    # Resumen de completitud CRM (para detectar faltantes antes del go-live).
    faltan_cuenta = sum(1 for r in data_rows if not r["cuenta_fusion"])
    completeness = {
        "total": len(data_rows),
        "faltan_n_cuenta": faltan_cuenta,
        "faltan_producto": sum(1 for r in data_rows if not (r["producto_nombre"] or "").strip()),
        "faltan_cliente": sum(1 for r in data_rows if not (r["cliente_visible"] or "").strip()),
    }
    logger.info(
        "[OPORTUNIDADES][API] list run_id=%s total=%s faltan_cuenta_fusion=%s",
        latest.id, len(data_rows), faltan_cuenta,
    )
    return {
        "ok": True,
        "data": {
            "run_id": latest.id,
            "total": len(data_rows),
            "window": _window_meta(db, latest.id),
            "completeness": completeness,
            "crm_modo": modo_actual,
            # Insumos del selector de asignación (iguales para todas las filas).
            "crm_asignacion": {
                "match": ctx.get("match"),
                "usuarios": ctx.get("usuarios") or [],
                "sugerido_id": ctx.get("sugerido_id"),
                "error": ctx.get("error"),
                "puede_elegir": bool(ctx.get("puede_elegir", True)),
                # Texto de bitácora ya armado por usuario: el modal lo muestra tal cual
                # al cambiar la selección, sin recomponerlo del lado del cliente.
                "bitacora_por_usuario": _bitacoras_por_usuario(ctx, ahora),
            },
            "rows": data_rows,
        },
    }


@router.get("/enviadas")
def oportunidades_enviadas(
    request: Request,
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Repositorio de oportunidades YA enviadas al CRM (todos los entornos).

    A diferencia de /list, NO se filtra por `crm_modo`: el sentido de esta vista es
    justamente ver todo lo enviado y a dónde fue, así que el entorno es una columna
    más. Tampoco se limita al run activo: se lee de `crm_envios`, que sobrevive a los
    recálculos mensuales (ahí está la gracia del `oportunidad_id` estable).

    Monto y producto NO son columnas de `crm_envios`: el monto sale del
    `payload_snapshot` (el que se mandó realmente, no uno recalculado hoy) y el
    producto del summary del run activo, con el código de artículo como respaldo
    cuando la oportunidad ya no califica.
    """
    _require_enabled()
    envios = db.execute(
        select(CrmEnvio).order_by(CrmEnvio.enviado_at.desc())
    ).scalars().all()

    # Producto legible: una sola query al run activo, no una por fila.
    productos: dict[str, str] = {}
    latest = _latest_success_import_run(db)
    if latest is not None:
        for o in db.execute(
            select(OportunidadSummary).where(OportunidadSummary.import_run_id == latest.id)
        ).scalars().all():
            productos[opportunity_stable_id(o.cliente_visible, o.codigo_articulo)] = (
                (o.producto_nombre or "").strip() or o.codigo_articulo or ""
            )

    filas = []
    for e in envios:
        monto = None
        if e.payload_snapshot:
            try:
                monto = json.loads(e.payload_snapshot).get("amount")
            except (ValueError, TypeError):
                monto = None
        filas.append({
            "oportunidad_id": e.oportunidad_id,
            "cliente_visible": e.cliente_visible,
            "producto": productos.get(e.oportunidad_id) or e.codigo_articulo or "—",
            "codigo_articulo": e.codigo_articulo,
            "unidad_negocio": e.unidad_negocio,
            "monto_oportunidad": monto,
            "enviado_por": e.enviado_por,
            "enviado_at": e.enviado_at.isoformat() if e.enviado_at else None,
            "asignado_a": e.crm_assigned_usuario,
            "asignado_origen": e.crm_assigned_origen,
            "crm_modo": e.crm_modo,
            "crm_status": e.crm_status,
            "crm_id": e.crm_id,
            "crm_url": crm_client.crm_detail_url(e.crm_id) if e.crm_id else None,
            "en_run_activo": e.oportunidad_id in productos,
        })

    logger.info("[OPORTUNIDADES][API] enviadas total=%s", len(filas))
    return {"ok": True, "data": {"total": len(filas), "rows": filas}}


# ──────────────────────────────────────────────────────────────────────────────
# Envío al CRM (Feature 1: sello del usuario · Feature 2: control de duplicados)
# ──────────────────────────────────────────────────────────────────────────────

# Traducción de la falla del CRM a un HTTP status propio. Ninguna cae en 500: el
# usuario tiene que poder distinguir "el cliente no está en el CRM" (acción suya) de
# "el CRM se cayó" (reintentar) y de "falta configurar el CRM" (acción de sistemas).
_CRM_STATUS_HTTP = {
    "cuenta_no_encontrada": 422,   # el cliente no existe en el CRM
    "dato": 422,                   # el CRM rechazó un dato del payload
    "auth": 502,                   # credenciales rechazadas
    "config": 503,                 # falta configuración del lado de SIEM
    "crm": 503,                    # 5xx del CRM -> reintentable
    "red": 503,                    # timeout / conexión -> reintentable
    "respuesta": 502,              # contrato roto
}


def _enviar_real_a_crm(payload: dict[str, Any], asignado: dict[str, str]) -> dict[str, Any]:
    """Punto único de envío al CRM. El registro en `crm_envios` se hace JUSTO DESPUÉS
    de que esta función confirme ok=True (ver `oportunidades_enviar`), de modo que el
    control de duplicados solo se active ante envíos efectivos.

    Modo PRUEBA (CRM_ENVIO_PLACEHOLDER on, default): NO llama a ningún sistema externo;
    devuelve ACK simulado con crm_status='SIMULADO'. Las filas quedan marcadas SIMULADO
    → purgables con scripts/clear_crm_envios.py, así se puede ejercitar el flujo de
    duplicados en la UI sin enviar nada al CRM. Se mantiene a propósito para poder
    seguir simulando después del go-live.

    Modo REAL (CRM_ENVIO_PLACEHOLDER=0): corre el circuito completo de `crm_client`
    (token → usuario → cuenta → oportunidad → bitácora) contra el CRM que indique
    CRM_MODO (test | prod). Cualquier falla sale como CrmError y NO se registra el
    envío, para que la oportunidad quede libre de reintentar.
    """
    if CRM_ENVIO_PLACEHOLDER():
        # Aun simulando se registra a quién se habría asignado y cómo se decidió: si no,
        # el ensayo no ejercita la parte que más importa auditar.
        return {
            "ok": True, "crm_status": "SIMULADO", "crm_id": None, "crm_modo": "simulado",
            "assigned_user_id": asignado.get("id"),
            "assigned_user": asignado.get("usuario"),
            "usuario_origen": asignado.get("origen"),
        }

    resultado = crm_client.enviar_oportunidad(
        nombre=payload["name"],
        email_usuario=payload.get("enviado_por"),
        asignado=asignado,
        n_cuenta_fusion=payload.get("n_cuenta"),
        amount=payload.get("amount") or 0,
        description=payload.get("description") or "",
        bitacora_description=payload.get("update_text") or "",
        id_sistema_origen=payload.get("id_sistema_origen_c") or "",
        estado_siem=payload.get("estado_siem"),
    )
    modo = resultado["modo"]
    return {
        "ok": True,
        "crm_status": f"ENVIADO_{modo.upper()}",
        "crm_id": resultado["crm_id"],
        "crm_account_id": resultado["crm_account_id"],
        "crm_modo": modo,
        "assigned_user_id": resultado["assigned_user_id"],
        # Nombre del usuario que el CRM aceptó: es lo que se muestra y se guarda en el
        # snapshot; sin reenviarlo acá, el payload quedaría con el texto de la preview.
        "assigned_user": resultado.get("assigned_user"),
        "usuario_origen": resultado["usuario_origen"],
        "bitacora_id": resultado["bitacora_id"],
        "bitacora_error": resultado["bitacora_error"],
    }


def _periodo_actual() -> str:
    """YYYYMM de hoy (clave del modo 'por período', hoy solo informativo)."""
    return dt.datetime.utcnow().strftime("%Y%m")


def _nota_evento(evento: str, ack: dict[str, Any]) -> str | None:
    """Nota de la bitácora interna: override y/o fallo del paso 5 en el CRM."""
    notas: list[str] = []
    if evento == "REENVIO_OVERRIDE":
        notas.append("override de reenvío")
    if ack.get("bitacora_error"):
        notas.append(f"bitácora del CRM no creada: {ack['bitacora_error']}")
    if ack.get("usuario_origen") == "manual":
        notas.append(f"asignado manualmente a {ack.get('assigned_user') or ack.get('assigned_user_id')}")
    return " | ".join(notas) if notas else None


def _decidir_asignado(
    ctx: dict[str, Any],
    assigned_user_id: str | None,
) -> dict[str, str] | None:
    """Decide el usuario del CRM asignado, respetando el rol de quien envía.

    ANALISTA (`puede_elegir=False`): SOLO puede asignarse a sí mismo.
      - Con match → esa persona.
      - Pidiendo otro id → 422. Este chequeo es el que importa: la UI no le muestra el
        selector, pero eso es cosmético; quien llame al endpoint a mano tiene que
        rebotar igual.
      - Sin match → None, y el endpoint corta con "no estás dado de alta en el CRM".

    SUPERVISOR O SUPERIOR (`puede_elegir=True`): elige entre los usuarios del CRM.
      - Con selección explícita → esa, validada contra la lista REAL (nunca se confía
        en el id que manda el cliente).
      - Sin selección → su propio match si lo tiene; si no, None → 422 pidiendo elegir.
    """
    match = ctx.get("match")
    elegido = (assigned_user_id or "").strip()
    puede_elegir = bool(ctx.get("puede_elegir", True))

    if not puede_elegir:
        if elegido and not (match and elegido == match["id"]):
            raise HTTPException(
                status_code=422,
                detail=("Como Analista solo podés asignarte las oportunidades a vos "
                        "mismo. Para asignarla a otra persona, pedíselo a un supervisor."),
            )
        return match

    if not elegido:
        return match
    for u in ctx.get("usuarios") or []:
        if u["id"] == elegido:
            # Si eligió su propio usuario, no es una reasignación: se registra como match.
            if match and u["id"] == match["id"]:
                return match
            return {"id": u["id"], "usuario": u["usuario"], "origen": "manual"}
    raise HTTPException(
        status_code=422,
        detail="El usuario del CRM elegido no existe en la lista de usuarios habilitados.",
    )


def _msg_ya_enviada(e: CrmEnvio) -> str:
    fecha = e.enviado_at.strftime("%d/%m/%Y") if e.enviado_at else "fecha desconocida"
    # Se nombra el entorno: con bloqueo por (oportunidad_id, crm_modo), "ya fue enviada"
    # sin decir a dónde se lee como un bloqueo global y confunde.
    entorno = {
        "simulado": "en modo simulado",
        "test": "al CRM de TEST",
        "prod": "al CRM de PRODUCCIÓN",
    }.get(e.crm_modo or "", "al CRM")
    return f"Esta oportunidad ya fue enviada {entorno} por {e.enviado_por} el {fecha}."


@router.post("/enviar/{summary_id}")
def oportunidades_enviar(
    summary_id: int,
    user=AllowedWriter,
    db: Session = Depends(get_db),
    override: bool = False,
    assigned_user_id: str | None = None,
):
    """Envía una oportunidad al CRM con sello del usuario y control de duplicados.

    ESCRITURA (regla jul-2026): solo admin/analista/supervisor (AllowedWriter);
    Gerente y Auditor ven el módulo pero no envían. Flujo:
      1. Resuelve la oportunidad del run activo (identidad estable = oportunidad_id).
      2. Si YA fue enviada y NO hay override → NO reenvía; devuelve mensaje claro.
         - override (reenvío) solo Admin.
      3. Si no existe (o override autorizado) → sella el payload (enviado_por/at/id),
         corre el envío al CRM y SOLO ante ACK OK registra en `crm_envios`
         (+ bitácora `crm_envio_eventos`), guardando el id que devolvió el CRM.
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
    # Duplicados POR ENTORNO: el bloqueo es (oportunidad_id, crm_modo). Lo enviado a
    # TEST no bloquea PROD (ni al revés), y lo simulado no bloquea nada real.
    modo_actual = _modo_envio_actual()
    existente = db.execute(
        select(CrmEnvio)
        .where(CrmEnvio.oportunidad_id == oportunidad_id)
        .where(CrmEnvio.crm_modo == modo_actual)
    ).scalars().first()

    # ── Control de duplicados ──
    # Override (reenvío) SOLO Admin: la regla vigente saca a Gerente de toda
    # escritura, y el reenvío es la escritura más sensible (pisa el bloqueo).
    es_override = bool(override) and is_admin(user)
    if existente is not None and bool(override) and not es_override:
        # Pidió override pero su rol no lo habilita (defensa explícita).
        raise HTTPException(
            status_code=403,
            detail="Solo Admin puede reenviar una oportunidad ya enviada.",
        )
    if existente is not None and not es_override:
        # Bloqueo permanente: NO reenvía. Mensaje claro con quién y cuándo.
        return {
            "ok": False,
            "status": "duplicado",
            "message": _msg_ya_enviada(existente),
            "enviado_por": existente.enviado_por,
            "enviado_at": existente.enviado_at.isoformat() if existente.enviado_at else None,
            "crm_id": existente.crm_id,
            "crm_url": crm_client.crm_detail_url(existente.crm_id) if existente.crm_id else None,
            "crm_modo": existente.crm_modo,
        }

    # ── Feature 1: sello del usuario (server-side, NUNCA del cliente) ──
    enviado_por = getattr(user, "email", None)
    enviado_por_id = getattr(user, "id", None)
    if not enviado_por:
        raise HTTPException(status_code=401, detail="Usuario sin email en la sesión.")
    enviado_at = dt.datetime.utcnow()

    cuenta_fusion = _resolver_cuenta_fusion(db, o, latest.id)
    # El usuario asignado se decide SIEMPRE antes de mandar nada, en los dos modos: es
    # un requisito del envío, no un adorno del modal. Si no se puede decidir, se corta
    # acá y no sale ningún request al CRM.
    ctx = _contexto_asignacion_seguro(enviado_por, _puede_elegir_asignado(user))
    asignado = _decidir_asignado(ctx, assigned_user_id)
    crm = _build_crm_payload(
        o, cuenta_fusion, asignado,
        usuarios_disponibles=bool(ctx.get("usuarios")),
        error_crm=ctx.get("error"),
        puede_elegir=bool(ctx.get("puede_elegir", True)),
        email_siem=enviado_por,
        # La hora del sello es la que va a la bitácora: el texto que se manda y el que
        # queda en el snapshot son el mismo, sin recomponerse en ningún lado.
        momento=enviado_at,
    )

    # Chequeo server-side: la UI ya deshabilita el botón, pero el endpoint es la
    # autoridad — un POST a mano no puede saltearse esto.
    if crm["bloqueos"]:
        logger.warning(
            "[OPORTUNIDADES][API] envío bloqueado oportunidad_id=%s: %s",
            oportunidad_id, " | ".join(crm["bloqueos"]),
        )
        raise HTTPException(status_code=422, detail=" ".join(crm["bloqueos"]))
    if crm["requiere_asignacion"]:
        # Sin match automático y sin selección: NO se elige por el usuario.
        raise HTTPException(
            status_code=422,
            detail=(
                "Tu usuario de SIEM no coincide con ningún usuario del CRM. "
                "Elegí a qué usuario del CRM asignar esta oportunidad antes de enviarla."
            ),
        )

    payload = dict(crm["payload"])
    payload["enviado_por"] = enviado_por
    payload["enviado_por_id"] = enviado_por_id
    payload["enviado_at"] = enviado_at.isoformat()
    # Estado de la oportunidad en SIEM -> `status` de la bitácora en el CRM (paso 5).
    payload["estado_siem"] = o.estado_actividad

    # ── Envío real. El registro se hace SOLO si el ACK es OK: ante cualquier falla la
    # oportunidad queda SIN registrar, o sea libre de reintentar sin pedir override. ──
    try:
        ack = _enviar_real_a_crm(payload, asignado)
    except CrmError as exc:
        logger.warning(
            "[OPORTUNIDADES][API] envío rechazado oportunidad_id=%s kind=%s paso=%s: %s",
            oportunidad_id, exc.kind, exc.paso, exc.mensaje,
        )
        raise HTTPException(
            status_code=_CRM_STATUS_HTTP.get(exc.kind, 502),
            detail=exc.mensaje,
        ) from exc
    if not ack.get("ok"):
        raise HTTPException(status_code=502, detail="El CRM rechazó el envío. Reintentá más tarde.")

    crm_status = ack.get("crm_status") or "PENDIENTE_ENVIO_REAL"
    crm_id = ack.get("crm_id")
    crm_url = crm_client.crm_detail_url(crm_id) if crm_id else None
    # El usuario que el CRM aceptó de verdad manda sobre el de la vista previa: el
    # snapshot tiene que reflejar a quién quedó asignada la oportunidad realmente.
    if ack.get("assigned_user"):
        payload["assigned_user"] = _texto_asignado({
            "usuario": ack["assigned_user"], "origen": ack.get("usuario_origen", ""),
        })
        payload["assigned_user_id"] = ack.get("assigned_user_id")
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
            crm_id=crm_id,
            crm_account_id=ack.get("crm_account_id"),
            # NUNCA None: es parte de la clave única (oportunidad_id, crm_modo) y un
            # NULL desactivaría el bloqueo de duplicados sin dar ninguna señal.
            crm_modo=ack.get("crm_modo") or modo_actual,
            # Asignación: a quién quedó y cómo se decidió (match | manual). `enviado_por`
            # de arriba guarda a quien disparó el envío, que puede ser otra persona.
            crm_assigned_user_id=ack.get("assigned_user_id"),
            crm_assigned_usuario=ack.get("assigned_user"),
            crm_assigned_origen=ack.get("usuario_origen"),
            payload_snapshot=payload_snapshot,
        ))
        evento = "ENVIO"
        status = "enviado"
    else:
        # Override Admin DENTRO DEL MISMO ENTORNO (con el bloqueo por (oportunidad_id,
        # crm_modo), un envío a otro entorno ya no cae acá: es una fila nueva).
        # NO se toca el sello del primer emisor (enviado_por/at), que es lo que preserva
        # la fila canónica. Excepción: si la fila no tenía id de CRM y este reenvío sí lo
        # creó, se completa — si no, "Ver en CRM" nunca aparecería pese a existir la
        # oportunidad del otro lado.
        if crm_id and not existente.crm_id:
            existente.crm_id = crm_id
            existente.crm_account_id = ack.get("crm_account_id")
            existente.crm_status = crm_status
        else:
            crm_url = crm_client.crm_detail_url(existente.crm_id) if existente.crm_id else crm_url
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
        crm_id=crm_id,
        payload_snapshot=payload_snapshot,
        nota=_nota_evento(evento, ack),
    ))
    db.commit()

    logger.info(
        "[OPORTUNIDADES][API] enviar oportunidad_id=%s evento=%s por=%s run_id=%s "
        "crm_status=%s crm_id=%s",
        oportunidad_id, evento, enviado_por, latest.id, crm_status, crm_id,
    )
    return {
        "ok": True,
        "status": status,
        "oportunidad_id": oportunidad_id,
        "enviado_por": enviado_por,
        "enviado_at": enviado_at.isoformat(),
        "crm_status": crm_status,
        "crm_id": crm_id,
        "crm_url": crm_url,
        "crm_modo": ack.get("crm_modo") or modo_actual,
        "assigned_user": ack.get("assigned_user"),
        "usuario_origen": ack.get("usuario_origen"),
        # La oportunidad SÍ quedó creada; solo falló el paso 5 (bitácora). Se avisa en
        # la UI sin marcar el envío como fallido.
        "bitacora_error": ack.get("bitacora_error"),
        "payload": payload,
        "pendientes_crm": crm["pendientes_crm"],
        "faltantes_dataset": crm["faltantes_dataset"],
    }
