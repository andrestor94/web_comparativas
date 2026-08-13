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
import re
import unicodedata
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from web_comparativas.policy import require_perm, is_admin
from web_comparativas.models import User, VendedorFusion
from web_comparativas.dimensionamiento.account_resolution import (
    AccountSelectionError,
    relationship_candidates,
    select_account_resolution,
)
from web_comparativas.dimensionamiento import crm_client
from web_comparativas.dimensionamiento.crm_client import CrmError
from web_comparativas.dimensionamiento.models import (
    CrmEnvio,
    CrmEnvioEvento,
    DimensionamientoImportRun,
    DimensionamientoRecord,
    OportunidadAsignacionManual,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import (
    CRM_ENVIO_PLACEHOLDER,
    CRM_USUARIO_SIEM_ID,
    OPORTUNIDADES_CARTERA_ENABLED,
    OPORTUNIDADES_ENABLED,
    VENTANA_MESES,
    _detectar_ultimo_mes_completo,
    _subtract_months,
    normalizar_cuenta_fusion,
    opportunity_stable_id,
)
from web_comparativas.dimensionamiento.oportunidades_visibilidad import (
    analistas_a_cargo,
    oportunidades_visibles_para,
    supervisores_a_cargo,
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
_ROLES_SUPERVISOR = {"supervisor"}
_ROLES_GERENTE = {"gerente", "manager"}
# Quién ve la lista COMPLETA de 82 usuarios del CRM en el selector de envío (ago-2026):
# solo Admin/Auditor. Supervisor y Gerente ven acotado a su propia estructura (ver
# _contexto_asignacion_seguro); Analista no ve selector en absoluto (_puede_elegir_asignado).
_ROLES_CRM_LISTA_COMPLETA = {"admin", "administrator", "administrador", "auditor", "visor", "viewer"}


def _ids_y_emails_visibles_en_envios(db: Session, user) -> tuple[set[int], set[str]] | None:
    """Identidades cuyos envios puede auditar ``user``.

    ``None`` significa acceso total (Admin/Auditor). El resto usa la jerarquia
    comercial explicita de ``reporta_a_id``. Los emails complementan al FK porque
    los envios historicos pueden no tener ``enviado_por_id``.
    """
    rol = _rol(user)
    if rol in _ROLES_CRM_LISTA_COMPLETA:
        return None

    ids: set[int] = set()
    user_id = getattr(user, "id", None)
    if user_id is not None:
        ids.add(int(user_id))
        if rol in _ROLES_SUPERVISOR:
            ids.update(analistas_a_cargo(db, int(user_id)))
        elif rol in _ROLES_GERENTE:
            supervisores_ids = set(supervisores_a_cargo(db, int(user_id)))
            ids.update(supervisores_ids)
            for supervisor_id in supervisores_ids:
                ids.update(analistas_a_cargo(db, supervisor_id))

    emails = {
        str(email).strip().lower()
        for (email,) in (db.query(User.email).filter(User.id.in_(ids)).all() if ids else [])
        if email
    }
    email_actor = str(getattr(user, "email", "") or "").strip().lower()
    if email_actor:
        emails.add(email_actor)
    return ids, emails


def _envios_visibles_stmt(db: Session, user):
    """SELECT server-side de envios autorizado para el actor actual."""
    stmt = select(CrmEnvio)
    scope = _ids_y_emails_visibles_en_envios(db, user)
    if scope is None:
        return stmt
    ids, emails = scope
    condiciones = []
    if ids:
        condiciones.append(CrmEnvio.enviado_por_id.in_(ids))
    if emails:
        condiciones.append(func.lower(func.trim(CrmEnvio.enviado_por)).in_(emails))
    # Un usuario sin identidad utilizable no debe heredar acceso por accidente.
    return stmt.where(or_(*condiciones)) if condiciones else stmt.where(False)


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

# Quién puede ASIGNAR una oportunidad a mano a un analista (ago-2026): Supervisor
# (solo a analistas de SU equipo) o Admin (a cualquier analista). Deliberadamente
# más chico que _WRITE_ROLES: un Analista no asigna (solo se autoasigna al enviar al
# CRM, que es otra acción) y Gerente/Auditor son de lectura.
_ROLES_ASIGNADOR = {"admin", "administrator", "administrador", "supervisor"}


def _require_oportunidades_asignar(request: Request):
    user = _perm_oportunidades(request)
    if _rol(user) not in _ROLES_ASIGNADOR:
        raise HTTPException(
            status_code=403,
            detail="Solo Supervisor o Admin pueden asignar oportunidades a un analista.",
        )
    return user


AllowedAssigner = Depends(_require_oportunidades_asignar)

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
    if asignado.get("origen") == "sistema":
        return f"asignado a {asignado['usuario']} (usuario de sistema)"
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


def _operador_crm_matches(db: Session, operador_codigos: set[str]) -> dict[str, dict[str, str] | None]:
    """{operador_codigo: {"id","usuario"}|None} — el usuario del CRM que corresponde a
    la PERSONA de Fusión dueña de la cuenta (vía su identidad en SIEM: VendedorFusion),
    no al de quien está enviando. Es la persona real de la cuenta en Fusión/CRM, así
    que aplica sin importar la estructura de quien pide el envío.

    UNA sola consulta a usuarios_rendidores para todos los códigos pedidos (evita N
    llamadas al CRM si hay varias cuentas candidatas). Si el CRM no responde, no
    revienta nada: se devuelve vacío y el selector cae al comportamiento normal
    (acotado por estructura), que es exactamente lo que ya había antes de esto.
    """
    codigos = {c for c in operador_codigos if c}
    if not codigos:
        return {}
    vendedores = (
        db.query(VendedorFusion)
        .filter(VendedorFusion.codigo_vendedor.in_(codigos), VendedorFusion.user_id.isnot(None))
        .all()
    )
    if not vendedores:
        return {}
    try:
        usuarios_crm = crm_client.contexto_asignacion(None).get("usuarios") or []
    except CrmError:
        return {}
    salida: dict[str, dict[str, str] | None] = {}
    for v in vendedores:
        email = v.user.email if v.user else None
        salida[v.codigo_vendedor] = crm_client.buscar_match_usuario(usuarios_crm, email)
    return salida


def _emails_bajo_mi_estructura(db: Session, user) -> set[str]:
    """Emails de SIEM de las personas que quien pide esto puede elegir en el selector
    del CRM: el propio + su estructura, según el rol. Reutiliza la MISMA jerarquía
    (reporta_a_id) que ya usa oportunidades_visibilidad.py para la cartera comercial —
    "poder ver la cartera de alguien" y "poder asignarle una oportunidad en el CRM" son
    la misma noción de "está bajo mi estructura".
    """
    rol = _rol(user)
    ids = {user.id}
    if rol in _ROLES_SUPERVISOR:
        ids |= set(analistas_a_cargo(db, user.id))
    elif rol in _ROLES_GERENTE:
        supervisores_ids = set(supervisores_a_cargo(db, user.id))
        ids |= supervisores_ids
        for sid in supervisores_ids:
            ids |= set(analistas_a_cargo(db, sid))
    filas = db.query(User.email).filter(User.id.in_(ids)).all()
    return {e[0] for e in filas if e[0]}


def _contexto_asignacion_seguro(user, db: Session) -> dict[str, Any]:
    """Contexto de asignación para la VISTA PREVIA, sin poder romper la página.

    Devuelve {"match", "usuarios", "sugerido_id", "error"}. Se corre una sola vez por
    request (depende del usuario logueado, no de la fila). Dos cuidados deliberados:
      - Si falta configuración del CRM NO se sale a la red: sin esto, cada carga de la
        página pagaría un timeout de conexión antes de fallar.
      - Cualquier CrmError se traga: que el CRM esté caído tiene que bloquear el envío
        con un motivo claro, no dejar sin listado a todo el módulo.

    La lista de usuarios que se devuelve YA viene acotada por rol (ago-2026):
      - Analista: vacía (no ve selector, `_puede_elegir_asignado` en False).
      - Supervisor: el CRM de sí mismo + el de sus analistas a cargo.
      - Gerente: el CRM de sus supervisores a cargo + transitivamente sus analistas.
      - Admin/Auditor: los 82, sin acotar.
    Como `_decidir_asignado` valida el `assigned_user_id` elegido CONTRA esta misma
    lista (`ctx["usuarios"]`), acotarla acá alcanza para blindar server-side también
    el envío — no hace falta un segundo chequeo en `/enviar`.
    """
    email = getattr(user, "email", None)
    puede_elegir = _puede_elegir_asignado(user)
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

    rol = _rol(user)
    if rol not in _ROLES_CRM_LISTA_COMPLETA:
        # Supervisor o Gerente: acotar a la estructura propia (match por email de SIEM
        # sin dominio contra el campo `usuario` del CRM, mismo criterio que ya usa
        # crm_client.buscar_match_usuario).
        emails_permitidos = _emails_bajo_mi_estructura(db, user)
        usuarios_permitidos = {
            crm_client._usuario_desde_email(e) for e in emails_permitidos
        }
        ctx["usuarios"] = [
            u for u in ctx["usuarios"] if u["usuario"].strip().lower() in usuarios_permitidos
        ]
        if ctx["sugerido_id"] and not any(u["id"] == ctx["sugerido_id"] for u in ctx["usuarios"]):
            ctx["sugerido_id"] = None

    # Opción fija "usuario de sistema" (CRM_USUARIO_SIEM_ID, ago-2026): para cuando
    # quien envía no tiene usuario propio en el CRM (p. ej. el primer envío real,
    # hecho por Admin). Se agrega DESPUÉS del acotado por estructura a propósito: es
    # una opción de sistema, no de la cartera de nadie, y tiene que sobrevivir al
    # filtro de Supervisor/Gerente. `_decidir_asignado` valida contra esta misma
    # lista, así que agregarla acá alcanza para habilitarla también server-side.
    siem_id = CRM_USUARIO_SIEM_ID()
    if siem_id and not any(u["id"] == siem_id for u in ctx["usuarios"]):
        ctx["usuarios"] = ctx["usuarios"] + [
            {"id": siem_id, "usuario": "Usuario SIEM (sistema)", "es_sistema": True}
        ]
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


def _oportunidades_pendientes(db: Session, run_id: int, crm_modo: str):
    """Filas pendientes comparando la clave canonica exacta del envio.

    ``oportunidades_summary`` no persiste ``oportunidad_id``. Por eso no se arma un
    anti-join aproximado con columnas descriptivas: se calcula la identidad con
    ``opportunity_stable_id``, la misma funcion usada por el payload y por
    ``crm_envios.oportunidad_id``, y se excluye en backend antes de serializar/KPIs.
    Son dos consultas acotadas y nunca una consulta por fila.
    """
    enviados = set(
        db.execute(
            select(CrmEnvio.oportunidad_id).where(CrmEnvio.crm_modo == crm_modo)
        ).scalars()
    )
    rows = db.execute(
        select(OportunidadSummary)
        .where(OportunidadSummary.import_run_id == run_id)
        .order_by(OportunidadSummary.score.desc())
    ).scalars().all()
    return [
        row for row in rows
        if opportunity_stable_id(row.cliente_visible, row.codigo_articulo) not in enviados
    ]


def _resolve_account_for_opportunity(
    db: Session,
    run_id: int,
    opportunity: OportunidadSummary,
    *,
    requested_account: str | None = None,
) -> dict[str, Any]:
    """Reconstruye la relación y consulta el CRM en cada GET/POST; JavaScript no decide."""
    relationship = relationship_candidates(db, run_id, opportunity)
    crm_mode = _modo_envio_actual()
    results: dict[str, dict[str, Any]] = {}
    # Las relaciones ambiguas y los conjuntos >25 se bloquean ANTES de consultar. Para
    # una oportunidad sin puente nominal todavía puede confirmarse su cuenta original.
    can_query = not relationship.get("relacion_ambigua") and not relationship.get("exceso_candidatos")
    codes = [row["cuenta"] for row in relationship.get("cuentas_candidatas") or []] if can_query else []
    if crm_mode != "simulado" and codes:
        try:
            lookup = crm_client.consultar_cuentas(codes, detener_si_primera_existe=True)
            results = lookup["results"]
        except CrmError as exc:
            results = {
                code: {
                    "exists": None, "error": exc.mensaje, "kind": exc.kind,
                    "reintentable": exc.reintentable,
                }
                for code in codes
            }
    return select_account_resolution(
        relationship, results, crm_mode=crm_mode, requested_account=requested_account,
    )


def _account_resolution_message(resolution: dict[str, Any]) -> str:
    original = resolution.get("cuenta_original") or "sin dato"
    cuit = resolution.get("cuit") or "sin CUIT válido"
    evaluated = [row["cuenta"] for row in resolution.get("cuentas_candidatas") or []]
    confidence = resolution.get("estado_confianza")
    criterion = resolution.get("criterio_seleccion")
    if confidence == "RELACION_AMBIGUA":
        reasons = ", ".join(resolution.get("motivos_ambiguedad") or []) or "identidad no concluyente"
        total = resolution.get("cantidad_candidatas_total", len(evaluated))
        return (
            f"La relación de cuentas es ambigua y no se enviará. Motivos: {reasons}. "
            f"Candidatas detectadas: {total}; no se truncaron ni se seleccionó ninguna."
        )
    if confidence == "ERROR_CONSULTA_CRM":
        return "No se pudieron verificar todas las cuentas con el CRM. No se envió; reintentá más tarde."
    if criterion == "multiples_alternativas":
        return (
            "La cuenta original no existe y hay varias alternativas válidas. "
            "Elegí explícitamente una cuenta; la decisión quedará registrada como manual."
        )
    return (
        "Ninguna cuenta vinculada fue encontrada en el CRM del entorno actual. "
        f"Cuenta original: {original}. CUIT: {cuit}. "
        f"Cuentas evaluadas ({len(evaluated)}): {', '.join(evaluated) or 'ninguna'}."
    )

def _active_opportunity(db: Session, summary_id: int) -> tuple[Any, OportunidadSummary]:
    latest = _latest_success_import_run(db)
    if latest is None:
        raise HTTPException(status_code=404, detail="No hay corrida activa de oportunidades.")
    opportunity = db.execute(
        select(OportunidadSummary)
        .where(OportunidadSummary.id == summary_id)
        .where(OportunidadSummary.import_run_id == latest.id)
    ).scalars().first()
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Oportunidad no encontrada en la corrida activa.")
    return latest, opportunity


def _payload_snapshot(e: CrmEnvio) -> dict[str, Any]:
    if not e.payload_snapshot:
        return {}
    try:
        value = json.loads(e.payload_snapshot)
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# Nombre del cliente para pantalla (solo presentación)
#
# SIEM guarda la razón social; el CRM suele guardar el nombre de fantasía y le agrega
# el sufijo de sucursal entre paréntesis ("SANATORIO ARGENTINO S.R.L. (L.H.)"). Cuando
# son el mismo nombre escrito distinto, mostrar los dos es ruido; cuando de verdad
# difieren ("DELTA S.A." vs "CLINICA SANTA MARIA (L.H)"), ocultar uno esconde algo que
# el usuario necesita ver antes de enviar.
#
# Nada de esto participa de la resolución de cuentas ni del payload que viaja al CRM.
# ──────────────────────────────────────────────────────────────────────────────

_SUFIJO_SUCURSAL = re.compile(r"\([^)]*\)")


def _display_key(name: str | None) -> str:
    """Clave laxa, SOLO para decidir si dos nombres son el mismo cliente escrito distinto.

    Descarta acentos, el sufijo de sucursal entre paréntesis y toda la puntuación, así
    "SANATORIO ARGENTINO SRL" y "SANATORIO ARGENTINO S.R.L. (L.H.)" colapsan en la misma
    clave. Deliberadamente NO se usa para relacionar cuentas: el puente del resolver
    sigue siendo `canon` con igualdad exacta y no lo toca este cambio.
    """
    text = unicodedata.normalize("NFKD", str(name or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _SUFIJO_SUCURSAL.sub(" ", text)
    return re.sub(r"[^A-Za-z0-9]+", "", text).upper()


def _cliente_display(
    dataset_name: str | None, crm_name: str | None, cuenta: Any,
) -> dict[str, Any] | None:
    dataset = (dataset_name or "").strip() or None
    crm = (crm_name or "").strip() or None
    if not dataset and not crm:
        return None
    sufijo = f" (cuenta {cuenta})" if cuenta else ""
    if dataset and crm and _display_key(dataset) != _display_key(crm):
        return {"texto": f"{dataset} — {crm}{sufijo}", "dos_nombres": True}
    return {"texto": f"{dataset or crm}{sufijo}", "dos_nombres": False}


def _active_summaries_by_opportunity_id(db: Session) -> dict[str, OportunidadSummary]:
    """Snapshot comercial disponible en la corrida activa, indexado por identidad estable."""
    latest = _latest_success_import_run(db)
    if latest is None:
        return {}
    rows = db.execute(
        select(OportunidadSummary).where(OportunidadSummary.import_run_id == latest.id)
    ).scalars().all()
    return {
        opportunity_stable_id(row.cliente_visible, row.codigo_articulo): row
        for row in rows
    }


def _envio_to_dict(
    e: CrmEnvio,
    producto: str | None = None,
    oportunidad: OportunidadSummary | None = None,
) -> dict[str, Any]:
    payload = _payload_snapshot(e)
    return {
        'oportunidad_id': e.oportunidad_id,
        'cliente_visible': e.cliente_visible,
        'cuit': e.cuit,
        # Los registros viejos no tienen razón social del CRM: cae solo el nombre SIEM.
        'cliente_display': _cliente_display(
            e.cliente_visible,
            payload.get('crm_razon_social_informada'),
            payload.get('cuenta_utilizada') or payload.get('n_cuenta'),
        ),
        'producto': (
            producto
            or (oportunidad.producto_nombre if oportunidad is not None else None)
            or payload.get('producto_nombre')
            or e.codigo_articulo
            or '-'
        ),
        'codigo_articulo': e.codigo_articulo,
        'unidad_negocio': e.unidad_negocio,
        'datos_oportunidad_disponibles': oportunidad is not None,
        'tipo_oportunidad': oportunidad.tipo_oportunidad if oportunidad is not None else None,
        'estado_actividad': oportunidad.estado_actividad if oportunidad is not None else None,
        'provincia': oportunidad.provincia if oportunidad is not None else None,
        'familia': oportunidad.familia if oportunidad is not None else None,
        'plataforma': oportunidad.plataforma if oportunidad is not None else None,
        'consumo_tipico_mensual': oportunidad.consumo_tipico_mensual if oportunidad is not None else None,
        'consumo_min_mensual': oportunidad.consumo_min_mensual if oportunidad is not None else None,
        'consumo_max_mensual': oportunidad.consumo_max_mensual if oportunidad is not None else None,
        'meses_demanda_cliente_12m': oportunidad.meses_demanda_cliente_12m if oportunidad is not None else None,
        'meses_no_participo_12m': oportunidad.meses_no_participo_12m if oportunidad is not None else None,
        'ventana_meses': oportunidad.ventana_meses if oportunidad is not None else None,
        'ultima_demanda': oportunidad.ultima_demanda.isoformat() if oportunidad is not None and oportunidad.ultima_demanda else None,
        'meses_desde_ultima_demanda': oportunidad.meses_desde_ultima_demanda if oportunidad is not None else None,
        'efectividad': oportunidad.efectividad if oportunidad is not None else None,
        'ganados': oportunidad.ganados if oportunidad is not None else None,
        'clientes_distintos': oportunidad.clientes_distintos if oportunidad is not None else None,
        'precio_unitario_estimado': oportunidad.precio_unitario_estimado if oportunidad is not None else None,
        'monto_oportunidad': payload.get('amount'),
        'descripcion': payload.get('description'),
        'enviado_por': e.enviado_por,
        'enviado_at': e.enviado_at.isoformat() if e.enviado_at else None,
        'asignado_a': e.crm_assigned_usuario,
        'asignado_origen': e.crm_assigned_origen,
        'crm_modo': e.crm_modo,
        'crm_status': e.crm_status,
        'crm_id': e.crm_id,
        # Trazabilidad retrocompatible: las filas anteriores exponen None sin migración.
        'cuenta_original': payload.get('cuenta_original'),
        'cuenta_utilizada': payload.get('cuenta_utilizada') or payload.get('n_cuenta'),
        'cuenta_criterio': payload.get('cuenta_criterio') or payload.get('criterio_resolucion'),
        'cuenta_estado_confianza': payload.get('cuenta_estado_confianza') or payload.get('nivel_confianza'),
        'cuenta_confianza_label': payload.get('cuenta_confianza_label'),
        'cuenta_confirmacion_fiscal': payload.get('cuenta_confirmacion_fiscal'),
        'cuenta_seleccion_origen': payload.get('cuenta_seleccion_origen') or payload.get('seleccion_cuenta'),
        'operador_codigo': payload.get('operador_codigo'),
        'operador_nombre': payload.get('operador_nombre'),
        'fuente_relacion_cuenta': payload.get('fuente_relacion_cuenta') or payload.get('fuente_relacion'),
        'cantidad_candidatas': payload.get('cantidad_candidatas') or payload.get('cuentas_evaluadas'),
        'cuentas_evaluadas': payload.get('cantidad_evaluadas', payload.get('cuentas_evaluadas')),
        'crm_account_id': payload.get('crm_account_id') or e.crm_account_id,
        'crm_cuit_informado': payload.get('crm_cuit_informado'),
        'crm_razon_social_informada': payload.get('crm_razon_social_informada'),
        'crm_url': crm_client.crm_detail_url(e.crm_id) if e.crm_id else None,
    }


@router.get('/list')
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

    modo_actual = _modo_envio_actual()
    rows = _oportunidades_pendientes(db, latest.id, modo_actual)

    # Visibilidad por cartera comercial — kill-switch OPORTUNIDADES_CARTERA_ENABLED,
    # default OFF: mientras esté apagado esta línea ni se ejecuta, `rows` sigue
    # siendo exactamente lo que ya era (mismas filas para todo el mundo, como hoy).
    if OPORTUNIDADES_CARTERA_ENABLED():
        rows = oportunidades_visibles_para(db, _user, rows)

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
    ctx = _contexto_asignacion_seguro(_user, db)
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

    # La consulta base ya retiro los envios del entorno activo. Se conserva el contrato
    # para que un cliente con JS anterior siga interpretando cada fila como pendiente.
    for r in data_rows:
        r['envio'] = {'enviado': False}

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


@router.get("/cuentas/{summary_id}")
def oportunidad_cuentas(
    summary_id: int,
    request: Request,
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Preview read-only de cuentas candidatas y existentes en el CRM activo."""
    _require_enabled()
    latest, opportunity = _active_opportunity(db, summary_id)
    try:
        resolution = _resolve_account_for_opportunity(db, latest.id, opportunity)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo cargar el maestro de cuentas: {exc}") from exc
    resolution["message"] = None if not resolution.get("bloqueado") else _account_resolution_message(resolution)
    dataset_nombre = (
        next(iter(resolution.get("nombres_homologados_originales") or []), None)
        or opportunity.cliente_visible
    )
    # Cada cuenta hallada lleva su propio texto: si el usuario elige otra en el
    # selector, el front la muestra sin recalcular nada ni volver a preguntarle al CRM.
    for row in resolution.get("cuentas_encontradas_en_crm") or []:
        row["cliente_display"] = _cliente_display(
            dataset_nombre, row.get("crm_razon_social") or row.get("crm_nombre"), row.get("cuenta"),
        )
    seleccionada = resolution.get("cuenta_seleccionada") or {}
    resolution["cliente_display"] = _cliente_display(
        dataset_nombre,
        seleccionada.get("crm_razon_social") or seleccionada.get("crm_nombre"),
        seleccionada.get("cuenta"),
    ) if seleccionada else None

    # Operador de Fusión -> su usuario del CRM (si tiene identidad vinculada en SIEM).
    # Solo tiene sentido calcularlo para quien ve selector (Analista nunca lo usa: se
    # asigna siempre a sí mismo). Se anota en CADA candidata (no solo la seleccionada)
    # para que cambiar de cuenta en el selector del modal no pierda el dato sin volver
    # a pedirle nada al CRM.
    if _puede_elegir_asignado(_user):
        candidatas = resolution.get("cuentas_encontradas_en_crm") or []
        codigos = {row.get("operador_codigo") for row in candidatas if row.get("operador_codigo")}
        if seleccionada.get("operador_codigo"):
            codigos.add(seleccionada["operador_codigo"])
        operador_matches = _operador_crm_matches(db, codigos)
        for row in candidatas:
            row["operador_asignado_crm"] = operador_matches.get(row.get("operador_codigo"))
        if seleccionada:
            seleccionada["operador_asignado_crm"] = operador_matches.get(seleccionada.get("operador_codigo"))

    return {"ok": True, "data": resolution}


@router.get("/enviadas")
def oportunidades_enviadas(
    request: Request,
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    """Repositorio de oportunidades YA enviadas al CRM (todos los entornos visibles).

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
        _envios_visibles_stmt(db, _user).order_by(CrmEnvio.enviado_at.desc())
    ).scalars().all()

    # Una sola query al run activo para completar el detalle comercial de las filas
    # que todavia califican. Los historicos conservan el resumen textual sellado.
    oportunidades = _active_summaries_by_opportunity_id(db)

    filas = []
    for e in envios:
        oportunidad = oportunidades.get(e.oportunidad_id)
        row = _envio_to_dict(e, oportunidad=oportunidad)
        row["en_run_activo"] = oportunidad is not None
        filas.append(row)

    logger.info("[OPORTUNIDADES][API] enviadas total=%s", len(filas))
    return {"ok": True, "data": {"total": len(filas), "rows": filas}}


# ──────────────────────────────────────────────────────────────────────────────
# Consulta puntual para el deep-link del repositorio de enviadas
# ──────────────────────────────────────────────────────────────────────────────

@router.get('/enviadas/detalle')
def oportunidad_enviada_detalle(
    request: Request,
    oportunidad_id: str = Query(..., max_length=255),
    _user=AllowedUser,
    db: Session = Depends(get_db),
):
    '''Resuelve el id exacto enviado al CRM exclusivamente en el entorno activo.'''
    _require_enabled()
    modo_actual = _modo_envio_actual()
    envio = db.execute(
        _envios_visibles_stmt(db, _user)
        .where(CrmEnvio.oportunidad_id == oportunidad_id)
        .where(CrmEnvio.crm_modo == modo_actual)
    ).scalars().first()
    if envio is None:
        return {
            'ok': True,
            'data': {
                'found': False,
                'oportunidad_id': oportunidad_id,
                'crm_modo': modo_actual,
                'row': None,
            },
        }
    return {
        'ok': True,
        'data': {
            'found': True,
            'oportunidad_id': oportunidad_id,
            'crm_modo': modo_actual,
            'row': _envio_to_dict(
                envio,
                oportunidad=_active_summaries_by_opportunity_id(db).get(envio.oportunidad_id),
            ),
        },
    }


# Envío al CRM (Feature 1: sello del usuario · Feature 2: control de duplicados)
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


def _enviar_real_a_crm(
    payload: dict[str, Any], asignado: dict[str, str], crm_account_id: str | None = None,
) -> dict[str, Any]:
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
        crm_account_id_validado=crm_account_id,
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


def _nota_evento(
    evento: str, ack: dict[str, Any], account_trace: str | None = None,
) -> str | None:
    """Nota de la bitácora interna: override y/o fallo del paso 5 en el CRM."""
    notas: list[str] = []
    if account_trace:
        notas.append(account_trace)
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
    usuarios = ctx.get("usuarios") or []
    if not usuarios:
        # La lista se vuelve a pedir al CRM en el momento del envío. Si esa consulta
        # falla, la selección del usuario sigue siendo válida: lo que no se pudo es
        # verificarla. Decir "no existe" mandaría a buscar un problema inexistente en
        # una elección correcta, y encima como algo definitivo en vez de reintentable.
        raise HTTPException(
            status_code=503,
            detail=("No se pudo verificar la lista de usuarios con el CRM"
                    + (f": {ctx['error']}" if ctx.get("error") else ".")
                    + " La oportunidad no se envió. Reintentá en unos minutos."),
        )
    for u in usuarios:
        if u["id"] == elegido:
            # Si eligió su propio usuario, no es una reasignación: se registra como match.
            if match and u["id"] == match["id"]:
                return match
            # La opción fija "usuario de sistema" (CRM_USUARIO_SIEM_ID) se distingue de
            # una reasignación real: queda con su propio origen en crm_assigned_origen,
            # para poder identificar después los envíos hechos sin usuario propio.
            origen = "sistema" if u.get("es_sistema") else "manual"
            return {"id": u["id"], "usuario": u["usuario"], "origen": origen}
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
    cuenta_seleccionada: str | None = None,
    cuenta_seleccion_manual: bool = False,
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

    try:
        account_resolution = _resolve_account_for_opportunity(
            db, latest.id, o, requested_account=cuenta_seleccionada,
        )
    except AccountSelectionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo cargar el maestro de cuentas: {exc}") from exc
    selected_account = account_resolution.get("cuenta_seleccionada")
    if not selected_account:
        status_code = 503 if account_resolution.get("estado_confianza") == "ERROR_CONSULTA_CRM" else 422
        raise HTTPException(status_code=status_code, detail=_account_resolution_message(account_resolution))
    if (
        account_resolution.get("criterio_seleccion") == "seleccion_manual_entre_alternativas"
        and not cuenta_seleccion_manual
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                "La relación de cuentas cambió desde que abriste el modal y ahora tiene "
                "varias alternativas válidas. Actualizá el modal y elegí la cuenta explícitamente."
            ),
        )
    cuenta_fusion = selected_account["cuenta"]
    # El usuario asignado se decide SIEMPRE antes de mandar nada, en los dos modos: es
    # un requisito del envío, no un adorno del modal. Si no se puede decidir, se corta
    # acá y no sale ningún request al CRM.
    ctx = _contexto_asignacion_seguro(user, db)
    # Si la cuenta tiene un operador de Fusión con usuario propio en el CRM, esa
    # persona es SIEMPRE una opción válida (es la dueña real de la cuenta), aunque
    # quede fuera de la estructura de quien envía — por eso se suma acá, a la lista
    # que `_decidir_asignado` valida, en vez de tocar esa función.
    operador_codigo = (selected_account or {}).get("operador_codigo")
    if operador_codigo and _puede_elegir_asignado(user):
        operador_match = _operador_crm_matches(db, {operador_codigo}).get(operador_codigo)
        if operador_match and not any(u["id"] == operador_match["id"] for u in ctx["usuarios"]):
            ctx["usuarios"] = ctx["usuarios"] + [operador_match]
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
    payload["cuenta_original"] = account_resolution.get("cuenta_original")
    payload["cuenta_utilizada"] = selected_account["cuenta"]
    payload["cuenta_criterio"] = account_resolution.get("criterio_seleccion")
    payload["criterio_resolucion"] = account_resolution.get("criterio_seleccion")
    payload["cuenta_estado_confianza"] = account_resolution.get("estado_confianza")
    payload["nivel_confianza"] = account_resolution.get("estado_confianza")
    payload["cuenta_confianza_label"] = account_resolution.get("confianza_label")
    payload["cuenta_confirmacion_fiscal"] = account_resolution.get("confirmacion_fiscal", False)
    payload["cuenta_seleccion_origen"] = account_resolution.get("seleccion_origen")
    payload["seleccion_cuenta"] = account_resolution.get("seleccion_origen")
    payload["crm_cuit_informado"] = selected_account.get("crm_cuit")
    payload["crm_razon_social_informada"] = selected_account.get("crm_razon_social")
    payload["operador_codigo"] = selected_account.get("operador_codigo")
    payload["operador_nombre"] = selected_account.get("operador_nombre")
    payload["fuente_relacion_cuenta"] = account_resolution.get("fuente_relacion")
    payload["fuente_relacion"] = account_resolution.get("fuente_relacion")
    payload["cantidad_candidatas"] = account_resolution.get("cantidad_candidatas_total", len(account_resolution.get("cuentas_candidatas") or []))
    payload["cantidad_evaluadas"] = account_resolution.get("cantidad_evaluadas_crm", 0)
    payload["cuentas_evaluadas"] = payload["cantidad_evaluadas"]
    if account_resolution.get("trazabilidad_texto"):
        payload["update_text"] = (
            f"{payload.get('update_text') or ''} {account_resolution['trazabilidad_texto']}"
        ).strip()

    payload["enviado_por"] = enviado_por
    payload["enviado_por_id"] = enviado_por_id
    payload["enviado_at"] = enviado_at.isoformat()
    # Estado de la oportunidad en SIEM -> `status` de la bitácora en el CRM (paso 5).
    payload["estado_siem"] = o.estado_actividad

    # ── Envío real. El registro se hace SOLO si el ACK es OK: ante cualquier falla la
    # oportunidad queda SIN registrar, o sea libre de reintentar sin pedir override. ──
    try:
        ack = _enviar_real_a_crm(payload, asignado, selected_account.get("crm_account_id"))
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

    payload["crm_account_id"] = ack.get("crm_account_id")
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
        nota=_nota_evento(evento, ack, account_resolution.get("trazabilidad_texto")),
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
        "cuenta_resolucion": account_resolution,
        # La oportunidad SÍ quedó creada; solo falló el paso 5 (bitácora). Se avisa en
        # la UI sin marcar el envío como fallido.
        "bitacora_error": ack.get("bitacora_error"),
        "payload": payload,
        "pendientes_crm": crm["pendientes_crm"],
        "faltantes_dataset": crm["faltantes_dataset"],
    }


@router.post("/asignar-analista/{summary_id}")
def oportunidades_asignar_analista(
    summary_id: int,
    analista_id: int,
    user=AllowedAssigner,
    db: Session = Depends(get_db),
):
    """Asigna a mano una oportunidad puntual a un Analista (cartera comercial,
    pieza 3, ago-2026). Pisa la cartera por vendedor: con OPORTUNIDADES_CARTERA_ENABLED
    prendido, el analista la va a ver aunque la cuenta sea de otro vendedor de Fusión
    (ver `oportunidades_visibilidad.py`). Esta acción es independiente del kill-switch
    de filtrado — un Supervisor puede ir preparando asignaciones antes de que se
    prenda, no tienen efecto visible hasta ese momento.

    Server-side (nunca se confía en el cliente):
      - Solo Supervisor (a analistas de SU equipo) o Admin (a cualquier analista).
      - El destino tiene que existir, ser Analista, y —si quien pide es Supervisor—
        reportarle a él (`reporta_a_id`).
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

    analista = db.get(User, analista_id)
    if not analista or not analista.has_role("analista", "analyst"):
        raise HTTPException(status_code=422, detail="El usuario elegido no es un Analista.")
    if not is_admin(user) and analista.reporta_a_id != getattr(user, "id", None):
        raise HTTPException(
            status_code=403,
            detail="Solo podés asignar oportunidades a analistas de tu propio equipo.",
        )

    oportunidad_id = opportunity_stable_id(o.cliente_visible, o.codigo_articulo)
    existente = db.execute(
        select(OportunidadAsignacionManual)
        .where(OportunidadAsignacionManual.oportunidad_id == oportunidad_id)
    ).scalars().first()
    ahora = dt.datetime.utcnow()
    if existente:
        existente.analista_user_id = analista.id
        existente.asignado_por_user_id = getattr(user, "id", None)
        existente.asignado_en = ahora
    else:
        db.add(OportunidadAsignacionManual(
            oportunidad_id=oportunidad_id,
            analista_user_id=analista.id,
            asignado_por_user_id=getattr(user, "id", None),
            asignado_en=ahora,
        ))
    db.commit()

    logger.info(
        "[OPORTUNIDADES][API] asignar-analista oportunidad_id=%s analista_id=%s por=%s",
        oportunidad_id, analista.id, getattr(user, "email", None),
    )
    return {
        "ok": True,
        "data": {
            "oportunidad_id": oportunidad_id,
            "analista_id": analista.id,
            "analista_nombre": analista.display_name,
            "asignado_por": getattr(user, "email", None),
            "asignado_en": ahora.isoformat(),
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Push de datos desde local (patrón push_match_data / Dimensionamiento):
# `oportunidades_summary` se calcula LOCAL (scripts/rebuild_oportunidades.py) y
# viaja como DATO por lotes chicos; acá SOLO se aplica y commitea por lote.
# Protegido por el MISMO token de import de Dimensionamiento (X-Import-Token).
# NO se gatea por OPORTUNIDADES_ENABLED a propósito: el push tiene que poder
# correr ANTES de encender el módulo (kill-switch OFF = riesgo cero mientras se
# cargan los datos).
# ──────────────────────────────────────────────────────────────────────────────

from web_comparativas.routers.dimensiones_router import verify_import_token  # noqa: E402


@router.get("/admin/estado")
def oportunidades_admin_estado(
    _token: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Estado verificable por curl (runbook de prod): run activo de Dimensionamiento,
    filas de `oportunidades_summary` de ESE run, y los kill-switches."""
    latest = _latest_success_import_run(db)
    filas = 0
    if latest is not None:
        filas = int(db.execute(
            select(func.count(OportunidadSummary.id))
            .where(OportunidadSummary.import_run_id == latest.id)
        ).scalar_one() or 0)
    return {
        "ok": True,
        "oportunidades_enabled": OPORTUNIDADES_ENABLED(),
        "oportunidades_cartera_enabled": OPORTUNIDADES_CARTERA_ENABLED(),
        "crm_envio_placeholder": CRM_ENVIO_PLACEHOLDER(),
        "crm_modo": crm_client.CRM_MODO(),
        "run_activo": latest.id if latest else None,
        "oportunidades_summary_filas": filas,
    }


@router.post("/admin/apply-data-chunk")
def oportunidades_admin_apply_data_chunk(
    payload: dict[str, Any] = Body(...),
    _token: str = Depends(verify_import_token),
    db: Session = Depends(get_db),
):
    """Aplica UN LOTE de `oportunidades_summary` calculado en local. Síncrono, commit
    por lote. kind:
      - 'oportunidades-summary': payload.run_id (el run de Dimensionamiento YA
        vigente en prod — este endpoint NO crea uno nuevo, solo cuelga filas de la
        tabla precalculada de ese run) + payload.rows [dicts de OportunidadSummary,
        sin id/import_run_id/created_at] + payload.reset (True SOLO en el primer
        lote de un push: borra las filas previas de ESE run_id antes de insertar,
        para que reintentar el push no duplique).
    """
    kind = payload.get("kind")
    try:
        if kind == "oportunidades-summary":
            run_id = int(payload.get("run_id"))
            run = db.get(DimensionamientoImportRun, run_id)
            if run is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"run {run_id} no existe en prod (pusheá primero el dataset de Dimensionamiento).",
                )

            if payload.get("reset"):
                borradas = (
                    db.query(OportunidadSummary)
                    .filter(OportunidadSummary.import_run_id == run_id)
                    .delete(synchronize_session=False)
                )
                db.commit()
                logger.info("[OPORTUNIDADES][PUSH] reset run_id=%s (borradas=%s)", run_id, borradas)

            rows = payload.get("rows") or []
            now = dt.datetime.utcnow()
            columnas = {c.name for c in OportunidadSummary.__table__.columns}
            mappings = []
            for r in rows:
                if not r.get("codigo_articulo"):
                    continue
                m = {k: v for k, v in r.items() if k in columnas}
                m["import_run_id"] = run_id
                m["created_at"] = now
                mappings.append(m)
            if mappings:
                db.execute(OportunidadSummary.__table__.insert(), mappings)
            db.commit()

            total = int(db.execute(
                select(func.count(OportunidadSummary.id))
                .where(OportunidadSummary.import_run_id == run_id)
            ).scalar_one() or 0)
            logger.info(
                "[OPORTUNIDADES][PUSH] run_id=%s +%s filas (total run=%s)",
                run_id, len(mappings), total,
            )
            return {"ok": True, "kind": kind, "run_id": run_id, "insertadas": len(mappings), "total": total}

        raise HTTPException(status_code=400, detail=f"kind inválido: {kind!r}")
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("[OPORTUNIDADES][PUSH] apply-data-chunk kind=%s FALLO", kind)
        raise HTTPException(status_code=500, detail=f"Error en lote {kind}: {exc}")
