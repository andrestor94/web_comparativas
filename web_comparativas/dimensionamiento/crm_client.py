"""Cliente del CRM (SuiteCRM V8) para el envío real de Oportunidades.

Implementa el circuito probado en Postman, en este orden y por CADA envío:

  1. POST /Api/access_token                                  -> access_token (1h)
  2. GET  /Api/V8/custom/reports/usuarios_rendidores          -> usuario asignado
  3. GET  /Api/V8/custom/reports/Cuentas_por_numero_fusion    -> cuenta (por Nº FUSION)
  4. POST /Api/V8/module  (type=Opportunities)                -> crea la oportunidad
  5. POST /Api/V8/module  (type=KNN_BitacoraMsj)              -> deja la bitácora

El token NO se cachea: se pide uno nuevo en cada envío (decisión explícita — evita
lidiar con expiración/invalidación a cambio de un request extra por envío, que es
despreciable frente al volumen real de envíos).

CREDENCIALES: SIEMPRE de entorno, nunca hardcodeadas. La app carga
`web_comparativas/.env` (no el de la raíz):
    CRM_BASE_URL, CRM_CLIENT_ID, CRM_CLIENT_SECRET, CRM_USUARIO_FALLBACK_ID, CRM_MODO

Este módulo NO toca la base de datos ni conoce el modelo de SIEM: recibe datos ya
resueltos y devuelve ids del CRM. Toda falla sale como `CrmError` con un `kind` que
el router traduce a un mensaje y un HTTP status entendibles (nunca un 500 pelado).
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any

import requests

logger = logging.getLogger("wc.oportunidades.crm")

# (connect, read). El CRM de TEST puede tardar en responder los reports custom.
TIMEOUT = (10, 45)

MODO_TEST = "test"
MODO_PROD = "prod"
MODOS_VALIDOS = {MODO_TEST, MODO_PROD}

# Valores fijos del contrato acordado con el equipo del CRM.
CRM_CURRENCY_ID = "-99"          # moneda por defecto del CRM
CRM_TIPO_OPORTUNIDAD = "Venta"
CRM_SALES_STAGE = "Prospecting"
CRM_LEAD_SOURCE = "Self Generated"
CRM_SISTEMA_ORIGEN = "SIEM"

MODULE_OPPORTUNITIES = "Opportunities"
MODULE_BITACORA = "KNN_BitacoraMsj"


class CrmError(Exception):
    """Falla del circuito de envío, clasificada para que la UI diga algo útil.

    kind:
      config              -> falta configuración (env vars). No reintentable.
      auth                -> 401/403: credenciales rechazadas por el CRM.
      dato                -> 400/422: el CRM rechazó un dato del payload.
      cuenta_no_encontrada-> paso 3 sin resultados: el cliente no existe en el CRM.
      crm                 -> 5xx del CRM. Reintentable.
      red                 -> timeout / DNS / conexión. Reintentable.
      respuesta           -> respondió OK pero sin el dato esperado (contrato roto).
    """

    def __init__(
        self,
        mensaje: str,
        *,
        kind: str,
        http_status: int | None = None,
        reintentable: bool = False,
        paso: str | None = None,
    ) -> None:
        super().__init__(mensaje)
        self.mensaje = mensaje
        self.kind = kind
        self.http_status = http_status
        self.reintentable = reintentable
        self.paso = paso


# ──────────────────────────────────────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────────────────────────────────────

def CRM_MODO() -> str:
    """Entorno del CRM al que se envía: 'test' (default) | 'prod'.

    Existe para NO tocar producción del CRM por error: el default es test, y prod
    requiere setearlo explícitamente. Se registra en cada envío (`crm_envios.crm_modo`)
    y se loguea en WARNING cuando es prod, para que quede rastro de qué salió a dónde.
    """
    raw = (os.getenv("CRM_MODO") or MODO_TEST).strip().lower()
    return raw if raw in MODOS_VALIDOS else MODO_TEST


def crm_config() -> dict[str, str]:
    """Lee y valida la configuración del CRM. Falla temprano y con nombres concretos."""
    modo_raw = (os.getenv("CRM_MODO") or MODO_TEST).strip().lower()
    if modo_raw not in MODOS_VALIDOS:
        raise CrmError(
            f"CRM_MODO='{modo_raw}' no es válido. Valores admitidos: test | prod.",
            kind="config",
        )

    base_url = (os.getenv("CRM_BASE_URL") or "").strip().rstrip("/")
    client_id = (os.getenv("CRM_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("CRM_CLIENT_SECRET") or "").strip()

    faltantes = [
        nombre
        for nombre, valor in (
            ("CRM_BASE_URL", base_url),
            ("CRM_CLIENT_ID", client_id),
            ("CRM_CLIENT_SECRET", client_secret),
        )
        if not valor
    ]
    if faltantes:
        raise CrmError(
            "Falta configuración del CRM: " + ", ".join(faltantes)
            + ". Cargalas en web_comparativas/.env (no en el .env de la raíz).",
            kind="config",
        )

    return {
        "base_url": base_url,
        "client_id": client_id,
        "client_secret": client_secret,
        "usuario_fallback_id": (os.getenv("CRM_USUARIO_FALLBACK_ID") or "").strip(),
        "modo": modo_raw,
        "verify": _verificacion_tls(),
    }


def _verificacion_tls() -> Any:
    """Qué usar como `verify` de requests: True | ruta a un CA bundle | False.

    POR QUÉ EXISTE: el CRM presenta un certificado válido de Sectigo, pero **no envía
    la cadena intermedia**. Los navegadores la resuelven solos (AIA fetching); Python
    no, y falla con "unable to get local issuer certificate". La solución correcta es
    apuntar CRM_CA_BUNDLE a un bundle que incluya ese intermedio (o que el equipo de
    infra arregle la cadena en el servidor).

    CRM_SSL_VERIFY=0 apaga la verificación por completo. Es una puerta de emergencia
    EXPLÍCITA: hay que setearla a mano, avisa por WARNING en cada envío y deja el
    tráfico expuesto a interceptación. Nunca es el default.
    """
    raw = (os.getenv("CRM_SSL_VERIFY") or "").strip().lower()
    if raw in {"0", "false", "no", "off", "n"}:
        logger.warning(
            "[CRM] CRM_SSL_VERIFY=%s -> verificación TLS DESACTIVADA. El tráfico con el "
            "CRM (incluidas las credenciales) queda expuesto a interceptación. "
            "Usar solo como último recurso; lo correcto es CRM_CA_BUNDLE.", raw,
        )
        return False
    bundle = (os.getenv("CRM_CA_BUNDLE") or "").strip()
    if bundle:
        if not os.path.isfile(bundle):
            raise CrmError(
                f"CRM_CA_BUNDLE apunta a un archivo que no existe: {bundle}",
                kind="config",
            )
        return bundle
    return True


def _nueva_sesion(cfg: dict[str, Any]) -> requests.Session:
    """Sesión HTTP con la política TLS de la config aplicada a TODOS los requests."""
    sesion = requests.Session()
    sesion.verify = cfg["verify"]
    return sesion


def crm_detail_url(crm_id: str) -> str | None:
    """URL del DetailView de la oportunidad en el CRM (botón 'Ver en CRM')."""
    base_url = (os.getenv("CRM_BASE_URL") or "").strip().rstrip("/")
    if not base_url or not crm_id:
        return None
    return f"{base_url}/index.php?action=DetailView&module=Opportunities&record={crm_id}"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers HTTP / parseo tolerante
# ──────────────────────────────────────────────────────────────────────────────

def _extraer_detalle(resp: requests.Response) -> str:
    """Saca el mensaje de error que devuelve el CRM, sea cual sea su envoltorio.

    SuiteCRM alterna entre JSON:API ({"errors": {...}}), {"error": {...}} y texto
    plano según el endpoint y el tipo de falla. Mostrar ESTE texto al usuario es lo
    que pidió el circuito: sin esto, un 400 por un dato mal formado se ve igual que
    una caída del CRM.
    """
    try:
        data = resp.json()
    except ValueError:
        texto = (resp.text or "").strip()
        return texto[:300] if texto else f"HTTP {resp.status_code} sin cuerpo."

    if isinstance(data, dict):
        for clave in ("errors", "error"):
            bloque = data.get(clave)
            if isinstance(bloque, dict):
                detalle = bloque.get("detail") or bloque.get("message") or bloque.get("title")
                if detalle:
                    return str(detalle)[:300]
            elif isinstance(bloque, list) and bloque:
                primero = bloque[0]
                if isinstance(primero, dict):
                    detalle = primero.get("detail") or primero.get("message") or primero.get("title")
                    if detalle:
                        return str(detalle)[:300]
            elif isinstance(bloque, str) and bloque:
                return bloque[:300]
        for clave in ("detail", "message", "error_description"):
            if data.get(clave):
                return str(data[clave])[:300]

    return f"HTTP {resp.status_code}."


def _revisar(resp: requests.Response, *, paso: str) -> None:
    """Traduce el status HTTP del CRM a un CrmError clasificado. 2xx pasa de largo."""
    if resp.ok:
        return
    detalle = _extraer_detalle(resp)
    codigo = resp.status_code
    if codigo in (401, 403):
        raise CrmError(
            f"El CRM rechazó las credenciales ({paso}): {detalle}",
            kind="auth", http_status=codigo, paso=paso,
        )
    if codigo >= 500:
        raise CrmError(
            f"El CRM devolvió un error interno ({paso}): {detalle}",
            kind="crm", http_status=codigo, reintentable=True, paso=paso,
        )
    # 400/422 y demás 4xx: problema con un dato que mandamos.
    raise CrmError(
        f"El CRM rechazó el dato enviado ({paso}): {detalle}",
        kind="dato", http_status=codigo, paso=paso,
    )


def _request(
    sesion: requests.Session,
    metodo: str,
    url: str,
    *,
    paso: str,
    **kwargs: Any,
) -> requests.Response:
    """Ejecuta el request traduciendo las fallas de red a CrmError reintentable."""
    try:
        return sesion.request(metodo, url, timeout=TIMEOUT, **kwargs)
    except requests.Timeout as exc:
        raise CrmError(
            f"El CRM no respondió a tiempo ({paso}). Reintentá en unos minutos.",
            kind="red", reintentable=True, paso=paso,
        ) from exc
    except requests.RequestException as exc:
        raise CrmError(
            f"No se pudo conectar con el CRM ({paso}): {exc.__class__.__name__}.",
            kind="red", reintentable=True, paso=paso,
        ) from exc


def _json(resp: requests.Response, *, paso: str) -> Any:
    try:
        return resp.json()
    except ValueError as exc:
        raise CrmError(
            f"El CRM devolvió una respuesta ilegible ({paso}).",
            kind="respuesta", paso=paso,
        ) from exc


def _verificar_status(data: Any, *, paso: str) -> None:
    """Los reports custom traen {"status": true|false, ...} ADEMÁS del status HTTP.

    Un `status: false` con HTTP 200 es una falla que el código de HTTP no delata; sin
    este chequeo se seguiría de largo y el error recién aparecería más adelante, con un
    mensaje que no dice nada.
    """
    if isinstance(data, dict) and data.get("status") is False:
        detalle = str(data.get("message") or "").strip() or "el CRM rechazó la consulta"
        raise CrmError(f"El CRM devolvió un error ({paso}): {detalle}", kind="crm",
                       reintentable=True, paso=paso)


def _iter_registros(data: Any) -> list[dict[str, Any]]:
    """Normaliza la respuesta de los reports custom a una lista de dicts planos.

    Formas REALES (verificadas en Postman, ago-2026):
      usuarios_rendidores        -> {"status": true, "data": [ {id, usuario, legajo_c} ]}
      Cuentas_por_numero_fusion  -> {"status": true, "data": {id, name, n_cuenta_c},
                                     "message": "", "status_code": 200}   <- data es OBJETO
      creación en /Api/V8/module -> {"data": {"type": "Opportunity", "id": "...",
                                              "attributes": {...}}}
    O sea: `data` puede ser lista U objeto según el endpoint. Se cubren esas dos y se
    deja el resto de envoltorios como red de contención por si algún report difiere.
    """
    registros: list[Any]
    if isinstance(data, list):
        registros = data
    elif isinstance(data, dict):
        for clave in ("data", "records", "result", "results", "rows", "items"):
            valor = data.get(clave)
            if isinstance(valor, list):
                registros = valor
                break
            if isinstance(valor, dict):
                registros = [valor]
                break
        else:
            registros = [data]
    else:
        return []

    planos: list[dict[str, Any]] = []
    for reg in registros:
        if not isinstance(reg, dict):
            continue
        plano = {k: v for k, v in reg.items() if k != "attributes"}
        attrs = reg.get("attributes")
        if isinstance(attrs, dict):
            plano.update(attrs)
        planos.append(plano)
    return planos


def _valor(registro: dict[str, Any], claves: tuple[str, ...]) -> str | None:
    """Primer valor no vacío entre `claves`, comparando sin distinguir mayúsculas."""
    normalizado = {str(k).strip().lower(): v for k, v in registro.items()}
    for clave in claves:
        valor = normalizado.get(clave.lower())
        if valor not in (None, "", []):
            return str(valor).strip()
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Paso 1 — token
# ──────────────────────────────────────────────────────────────────────────────

def obtener_token(sesion: requests.Session, cfg: dict[str, str]) -> str:
    """POST /Api/access_token con client_credentials. Token nuevo por envío."""
    url = f"{cfg['base_url']}/Api/access_token"
    resp = _request(
        sesion, "POST", url, paso="autenticación",
        json={
            "grant_type": "client_credentials",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
        },
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    _revisar(resp, paso="autenticación")
    data = _json(resp, paso="autenticación")
    _verificar_status(data, paso="autenticación")

    token = None
    if isinstance(data, dict):
        token = data.get("access_token")
        if not token and isinstance(data.get("data"), dict):
            token = data["data"].get("access_token")
    if not token:
        raise CrmError(
            "El CRM no devolvió un access_token.", kind="respuesta", paso="autenticación",
        )
    return str(token)


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Paso 2 — usuario asignado
# ──────────────────────────────────────────────────────────────────────────────

def _usuario_desde_email(email: str | None) -> str:
    """Toma la parte local del mail de SIEM ('nombre@suizo.com' -> 'nombre')."""
    return (email or "").strip().split("@")[0].strip().lower()


# Claves del nombre de usuario en usuarios_rendidores. La REAL es "usuario" (viene como
# el mail de Suizo sin dominio, p.ej. "jacqueline.gallo"); el resto es red de contención.
_CLAVES_USUARIO = ("usuario", "user_name", "username", "user")
_CLAVES_ID_USUARIO = ("id", "user_id", "assigned_user_id")


def listar_usuarios(
    sesion: requests.Session,
    cfg: dict[str, str],
    token: str,
) -> list[dict[str, Any]]:
    """GET /Api/V8/custom/reports/usuarios_rendidores -> [{id, usuario, legajo_c}, ...]."""
    paso = "usuarios del CRM"
    url = f"{cfg['base_url']}/Api/V8/custom/reports/usuarios_rendidores"
    resp = _request(sesion, "GET", url, paso=paso, headers=_headers(token))
    _revisar(resp, paso=paso)
    data = _json(resp, paso=paso)
    _verificar_status(data, paso=paso)
    return _iter_registros(data)


def normalizar_usuarios(registros: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Deja la lista del CRM como [{id, usuario}] ordenada, lista para un selector."""
    salida = []
    for reg in registros:
        crm_id = _valor(reg, _CLAVES_ID_USUARIO)
        usuario = _valor(reg, _CLAVES_USUARIO)
        if crm_id and usuario:
            salida.append({"id": crm_id, "usuario": usuario})
    return sorted(salida, key=lambda u: u["usuario"].lower())


def buscar_match_usuario(
    usuarios: list[dict[str, str]],
    email_siem: str | None,
) -> dict[str, str] | None:
    """Match del mail de SIEM sin dominio contra el campo `usuario` del CRM.

    Devuelve {"id", "usuario", "origen": "match"} o None. NO cae a ningún fallback:
    la asignación automática solo procede cuando la persona que envía ES la persona
    del CRM. Si no hay match, la decisión es de quien envía (selección manual), no del
    sistema — asignar en silencio a un tercero hace que la oportunidad aparezca en el
    CRM a nombre de alguien que no la generó.
    """
    buscado = _usuario_desde_email(email_siem)
    if not buscado:
        return None
    for u in usuarios:
        if u["usuario"].strip().lower() == buscado:
            return {"id": u["id"], "usuario": u["usuario"], "origen": "match"}
    return None


def contexto_asignacion(email_siem: str | None) -> dict[str, Any]:
    """Todo lo necesario para decidir el usuario asignado, sin enviar nada.

    Devuelve {"match": {...}|None, "usuarios": [{id, usuario}], "sugerido_id": str|None}.
    `sugerido_id` sale de CRM_USUARIO_FALLBACK_ID y sirve SOLO para pre-seleccionar una
    opción en el selector: nunca se aplica solo. Un envío asignado a alguien que quien
    envía no eligió es exactamente lo que esto evita.
    """
    cfg = crm_config()
    with _nueva_sesion(cfg) as sesion:
        token = obtener_token(sesion, cfg)
        usuarios = normalizar_usuarios(listar_usuarios(sesion, cfg, token))
    sugerido = (cfg.get("usuario_fallback_id") or "").strip() or None
    if sugerido and not any(u["id"] == sugerido for u in usuarios):
        logger.warning("[CRM] CRM_USUARIO_FALLBACK_ID=%s no está en usuarios_rendidores.", sugerido)
        sugerido = None
    return {
        "match": buscar_match_usuario(usuarios, email_siem),
        "usuarios": usuarios,
        "sugerido_id": sugerido,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Paso 3 — cuenta por número de FUSION
# ──────────────────────────────────────────────────────────────────────────────

def buscar_cuenta_id(
    sesion: requests.Session,
    cfg: dict[str, str],
    token: str,
    n_cuenta: str,
) -> str:
    """GET Cuentas_por_numero_fusion?n_cuenta_c=<nro> -> id de la cuenta en el CRM.

    404 (o 200 con lista vacía) = la cuenta no existe en el CRM: se corta el envío con
    `kind='cuenta_no_encontrada'` para que el usuario vea un mensaje concreto y no un
    error genérico. Es el caso esperable cuando el cliente todavía no fue dado de alta.
    """
    paso = "cuenta en el CRM"
    url = f"{cfg['base_url']}/Api/V8/custom/reports/Cuentas_por_numero_fusion"
    resp = _request(
        sesion, "GET", url, paso=paso,
        headers=_headers(token), params={"n_cuenta_c": n_cuenta},
    )
    def no_encontrada() -> CrmError:
        return CrmError(
            f"El cliente no existe en el CRM (cuenta de fusión {n_cuenta}). "
            "Hay que darlo de alta en el CRM antes de enviar la oportunidad.",
            kind="cuenta_no_encontrada", http_status=404, paso=paso,
        )

    if resp.status_code == 404:
        raise no_encontrada()
    _revisar(resp, paso=paso)

    data = _json(resp, paso=paso)
    # Acá `status: false` significa "no la encontré", no "me caí": el circuito tiene que
    # cortar con el mensaje de cuenta inexistente, no con un error reintentable.
    if isinstance(data, dict) and data.get("status") is False:
        raise no_encontrada()

    for reg in _iter_registros(data):
        cuenta_id = _valor(reg, ("id", "account_id", "cuenta_id"))
        if cuenta_id:
            return cuenta_id
    raise no_encontrada()


# ──────────────────────────────────────────────────────────────────────────────
# Pasos 4 y 5 — crear la oportunidad y su bitácora
# ──────────────────────────────────────────────────────────────────────────────

def _crear_registro(
    sesion: requests.Session,
    cfg: dict[str, str],
    token: str,
    *,
    tipo: str,
    atributos: dict[str, Any],
    paso: str,
) -> str:
    """POST /Api/V8/module con el envoltorio JSON:API. Devuelve el id creado."""
    url = f"{cfg['base_url']}/Api/V8/module"
    resp = _request(
        sesion, "POST", url, paso=paso,
        headers=_headers(token),
        json={"data": {"type": tipo, "attributes": atributos}},
    )
    _revisar(resp, paso=paso)

    data = _json(resp, paso=paso)
    _verificar_status(data, paso=paso)
    nuevo_id = None
    if isinstance(data, dict):
        bloque = data.get("data")
        if isinstance(bloque, dict):
            nuevo_id = bloque.get("id")
        elif isinstance(bloque, list) and bloque and isinstance(bloque[0], dict):
            nuevo_id = bloque[0].get("id")
        if not nuevo_id:
            nuevo_id = data.get("id")
    if not nuevo_id:
        raise CrmError(
            f"El CRM no devolvió el id del registro creado ({paso}).",
            kind="respuesta", paso=paso,
        )
    return str(nuevo_id)


def crear_oportunidad(
    sesion: requests.Session,
    cfg: dict[str, str],
    token: str,
    *,
    nombre: str,
    assigned_user_id: str,
    account_id: str,
    amount: float,
    description: str,
    id_sistema_origen: str,
    date_closed: str | None = None,
) -> str:
    atributos: dict[str, Any] = {
        "name": nombre,
        "assigned_user_id": assigned_user_id,
        "currency_id": CRM_CURRENCY_ID,
        "amount": round(float(amount or 0), 2),
        "tipooportunidad_c": CRM_TIPO_OPORTUNIDAD,
        "sales_stage": CRM_SALES_STAGE,
        "lead_source": CRM_LEAD_SOURCE,
        "description": description,
        "sistema_origen_c": CRM_SISTEMA_ORIGEN,
        "id_sistema_origen_c": id_sistema_origen,
        "account_id": account_id,
    }
    # date_closed es opcional en el contrato: se omite si no hay fecha tentativa.
    if date_closed:
        atributos["date_closed"] = date_closed
    return _crear_registro(
        sesion, cfg, token,
        tipo=MODULE_OPPORTUNITIES, atributos=atributos, paso="alta de la oportunidad",
    )


def crear_bitacora(
    sesion: requests.Session,
    cfg: dict[str, str],
    token: str,
    *,
    parent_id: str,
    description: str,
    status: str | None,
) -> str:
    return _crear_registro(
        sesion, cfg, token,
        tipo=MODULE_BITACORA,
        atributos={
            "parent_type": MODULE_OPPORTUNITIES,
            "parent_id": parent_id,
            "description": description,
            "status": status or "",
        },
        paso="bitácora de la oportunidad",
    )


def fecha_cierre_tentativa() -> str | None:
    """Fecha tentativa de cierre (YYYY-MM-DD) según CRM_DIAS_CIERRE_TENTATIVO.

    Sin la variable seteada devuelve None y el campo `date_closed` se OMITE: SIEM no
    tiene una fecha de cierre real, así que inventar una sería meterle ruido al pipeline
    del CRM. Si el equipo comercial quiere una, se setea la cantidad de días.
    """
    raw = (os.getenv("CRM_DIAS_CIERRE_TENTATIVO") or "").strip()
    if not raw:
        return None
    try:
        dias = int(raw)
    except ValueError:
        logger.warning("[CRM] CRM_DIAS_CIERRE_TENTATIVO='%s' no es un entero; se omite.", raw)
        return None
    return (dt.date.today() + dt.timedelta(days=max(dias, 0))).isoformat()


# ──────────────────────────────────────────────────────────────────────────────
# Circuito completo
# ──────────────────────────────────────────────────────────────────────────────

def _texto_bitacora(
    base: str,
    email_siem: str | None,
    asignado: dict[str, str],
) -> str:
    """Antepone la trazabilidad de QUIÉN disparó el envío al texto de la bitácora.

    El `assigned_user_id` de la oportunidad dice quién la TRABAJA en el CRM; no dice
    quién la generó. Cuando se asigna a otra persona (selección manual), sin esta línea
    el rastro del origen se pierde: en el CRM la oportunidad aparecería como de esa
    persona, sin nada que indique que la mandó alguien más desde SIEM.
    """
    quien = (email_siem or "usuario desconocido").strip()
    origen = asignado.get("origen")
    if origen == "manual":
        cabecera = (
            f"Enviada desde SIEM por {quien}, que la asignó manualmente a "
            f"{asignado.get('usuario') or asignado.get('id')}."
        )
    else:
        cabecera = f"Enviada desde SIEM por {quien}."
    return f"{cabecera} {base}".strip()

def enviar_oportunidad(
    *,
    nombre: str,
    email_usuario: str | None,
    asignado: dict[str, str],
    n_cuenta_fusion: str | None,
    amount: float,
    description: str,
    bitacora_description: str,
    id_sistema_origen: str,
    estado_siem: str | None,
) -> dict[str, Any]:
    """Corre el circuito completo (pasos 1..5) y devuelve el resultado del envío.

    `asignado` ({id, usuario, origen}) viene YA DECIDIDO por el router: o es el match
    automático con quien envía, o es la selección manual que esa persona hizo en el
    modal. Este módulo no elige por su cuenta a quién asignar.

    `email_usuario` es el usuario de SIEM que dispara el envío. Va SIEMPRE a la bitácora
    del CRM, aunque la oportunidad quede asignada a otra persona: quién la generó y
    quién la trabaja son dos cosas distintas y ambas tienen que quedar registradas.

    Devuelve {crm_id, crm_account_id, assigned_user_id, assigned_user, usuario_origen,
              modo, bitacora_id, bitacora_error}.

    Si falla el paso 5 (bitácora) NO se propaga el error: la oportunidad YA quedó
    creada en el CRM y perder su id acá obligaría al usuario a reenviar, duplicándola
    del lado del CRM. Se devuelve `bitacora_error` para avisar sin romper el envío.
    """
    cfg = crm_config()

    if not (asignado or {}).get("id"):
        raise CrmError(
            "No hay usuario del CRM asignado para esta oportunidad.",
            kind="dato", paso="usuarios del CRM",
        )

    n_cuenta = (n_cuenta_fusion or "").strip()
    if not n_cuenta:
        raise CrmError(
            "La oportunidad no tiene número de cuenta de fusión en el dataset, "
            "que es el dato con el que el CRM identifica al cliente. No se puede enviar.",
            kind="dato", paso="cuenta en el CRM",
        )

    if cfg["modo"] == MODO_PROD:
        logger.warning("[CRM] ENVÍO EN MODO PROD hacia %s", cfg["base_url"])

    assigned_user_id = asignado["id"]
    with _nueva_sesion(cfg) as sesion:
        token = obtener_token(sesion, cfg)
        account_id = buscar_cuenta_id(sesion, cfg, token, n_cuenta)

        crm_id = crear_oportunidad(
            sesion, cfg, token,
            nombre=nombre,
            assigned_user_id=assigned_user_id,
            account_id=account_id,
            amount=amount,
            description=description,
            id_sistema_origen=id_sistema_origen,
            date_closed=fecha_cierre_tentativa(),
        )

        bitacora_id: str | None = None
        bitacora_error: str | None = None
        try:
            bitacora_id = crear_bitacora(
                sesion, cfg, token,
                parent_id=crm_id,
                description=_texto_bitacora(bitacora_description, email_usuario, asignado),
                status=estado_siem,
            )
        except CrmError as exc:
            # La oportunidad ya existe en el CRM: se avisa, pero el envío es exitoso.
            bitacora_error = exc.mensaje
            logger.error(
                "[CRM] oportunidad %s creada pero falló la bitácora: %s", crm_id, exc.mensaje,
            )

    logger.info(
        "[CRM] envío OK modo=%s crm_id=%s account_id=%s usuario=%s/%s (%s) bitacora=%s",
        cfg["modo"], crm_id, account_id, assigned_user_id, asignado["usuario"],
        asignado["origen"], bitacora_id,
    )
    return {
        "crm_id": crm_id,
        "crm_account_id": account_id,
        "assigned_user_id": assigned_user_id,
        "assigned_user": asignado["usuario"],
        "usuario_origen": asignado["origen"],
        "modo": cfg["modo"],
        "bitacora_id": bitacora_id,
        "bitacora_error": bitacora_error,
    }
