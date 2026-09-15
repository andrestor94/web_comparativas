"""Resolución de visibilidad por cartera de cuentas — motor único compartido por
Forecast, Dimensionamiento y (más adelante, detrás de OPORTUNIDADES_CARTERA_ENABLED)
Oportunidades. Ver informe de auditoría "Cartera de Cuentas" (2026-08-19) y decisiones
tomadas el mismo día.

Contrato — FAIL-CLOSED (decisión de negocio, no accidente):
    Un usuario sin códigos de cartera asignados ve CERO clientes. Nunca "todos".
    Esto es la semántica INVERSA de `User.module_access` (donde NULL = acceso total)
    — no reutilizar esa convención acá. Ver comentario en `models.py` sobre
    `cartera_operador_codigos` / `cartera_vendedor_codigos` / `cartera_unineg_scope`.

Contrato — `cartera_unineg_scope` acota SOLO el lado vendedor:
    `operadores_comerciales.csv` no trae unidad de negocio, así que el lado
    operador de la cartera propia de un Supervisor entra siempre completo — con
    UN asignada, sin asignar, o con el scope vacío (fail-closed solo aplica al
    lado vendedor, nunca al operador). Ver `_cartera_propia()`.

Reusa la jerarquía (`analistas_a_cargo` / `supervisores_a_cargo`, vía `reporta_a_id`)
de `org_hierarchy.py` — un solo lugar donde vive la lógica de "quién reporta a
quién" para cartera comercial, compartido con Oportunidades
(`oportunidades_visibilidad.py`, enganchada a este mismo motor desde 2026-08-25).
Este motor resuelve contra los padrones `cartera_operadores` / `cartera_vendedores`
(22 operadores / 248 vendedores, Mercado Público + Privado) — no contra
`VendedorFusion`, que modela otra relación (identidad 1 a 1 de los 16 vendedores de
Fusión) y quedó fuera de la visibilidad por cartera.

Identidad del cliente: el código numérico compartido por operadores_comerciales.csv
(columna `codigo`), Cliente_vendedor.csv (columna `cliente`) y `clientes.csv` de
Forecast (columna `codigo`, de ahí sale `forecast_*.cliente_id`). Ver Sección 2 del
informe de auditoría para la cobertura medida de este cruce.
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from web_comparativas.models import User, UserReporte, CarteraOperador, CarteraVendedor
from web_comparativas.fusion_name_matching import fusion_codes_for_user
from web_comparativas.org_hierarchy import (
    analistas_a_cargo,
    supervisores_a_cargo,
)

# Este motor nuevo reconoce EXCLUSIVAMENTE los cinco roles canónicos seleccionables
# en S.I.C. Cualquier alias histórico o rol desconocido cae en NONE_ (fail-closed).
# Gerente y Supervisor tienen alcance agregado por jerarquía, no acceso total.
_ROLES_FULL_READ = {"admin", "auditor"}
_ROLES_ANALISTA = {"analista"}
_ROLES_SUPERVISOR = {"supervisor"}
_ROLES_GERENTE = {"gerente"}


@dataclass(frozen=True)
class CarteraBranchScope:
    '''Una rama autorizada, manteniendo juntas cuentas, UN y miembros.'''

    root_user_id: int
    member_user_ids: frozenset[int] = field(default_factory=frozenset)
    codigos_cliente: frozenset[str] = field(default_factory=frozenset)
    # None: Analista sin restriccion por UN. Vacio: configuracion incompleta.
    unidades_negocio: frozenset[str] | None = None


@dataclass(frozen=True)
class CarteraScope:
    """Resultado de la resolución para un usuario.

    `unrestricted=True`  -> sin filtro, ve todo (admin/auditor).
    `unrestricted=False` -> ve exactamente `codigos_cliente` (puede ser el set
                             vacío: fail-closed, "no ve nada").
    """

    unrestricted: bool
    codigos_cliente: frozenset[str] = field(default_factory=frozenset)
    branches: tuple[CarteraBranchScope, ...] = field(default_factory=tuple)

    def permite(self, codigo_cliente: str | int | None) -> bool:
        if self.unrestricted:
            return True
        if codigo_cliente is None:
            return False
        return str(codigo_cliente).strip() in self.codigos_cliente


ALL = CarteraScope(unrestricted=True)
NONE_ = CarteraScope(unrestricted=False, codigos_cliente=frozenset())


def _flag(name: str) -> bool:
    raw = (os.getenv(name) or "false").strip().lower()
    return raw not in {"0", "false", "no", "off", "n"}


def FORECAST_CARTERA_ENABLED() -> bool:
    """Kill-switch por módulo (mismo patrón que OPORTUNIDADES_CARTERA_ENABLED en
    dimensionamiento/oportunidades.py), default OFF. Necesario: apenas se prende,
    todo usuario sin cartera cargada en `users.cartera_*` pasa a ver CERO clientes en
    Forecast (fail-closed) — recién tiene sentido prenderlo después de correr
    push_cartera_data.py Y asignar cartera a cada usuario desde S.I.C."""
    return _flag("FORECAST_CARTERA_ENABLED")


def FORECAST_CARTERA_BYPASS_ALL() -> bool:
    """Bypass temporal del filtro de cuentas solo para las vistas de datos Forecast.

    No altera la matriz de roles ni las reglas de Aprobaciones Forecast. En Render
    queda activo por defecto y se revierte definiendo el valor explícito en 0.
    """
    raw = os.getenv("FORECAST_CARTERA_BYPASS_ALL")
    return _flag("RENDER") if raw is None else _flag("FORECAST_CARTERA_BYPASS_ALL")


def DIMENSIONAMIENTO_CARTERA_ENABLED() -> bool:
    """Kill-switch para el filtro de cartera en Dimensionamiento — todavía sin
    consumidor (pendiente medir cobertura real contra prod antes de aplicarlo,
    ver informe de auditoría 2026-08-19). Default OFF."""
    return _flag("DIMENSIONAMIENTO_CARTERA_ENABLED")


def _codes(value: Iterable | None) -> set[str]:
    """Normaliza una columna JSONEncodedList (lista|None) a un set[str] sin vacíos.
    None Y [] se tratan IGUAL acá — ambos son "sin códigos" (ver contrato fail-closed
    en el docstring del módulo)."""
    if not value:
        return set()
    return {str(v).strip() for v in value if str(v).strip()}


def _clientes_por_operador(db: Session, codigos_operador: set[str]) -> set[str]:
    if not codigos_operador:
        return set()
    rows = (
        db.query(CarteraOperador.codigo_cliente)
        .filter(CarteraOperador.operador_codigo.in_(codigos_operador))
        .distinct()
        .all()
    )
    return {r[0] for r in rows}


def _clientes_por_vendedor(
    db: Session, codigos_vendedor: set[str], unineg_scope: set[str] | None
) -> set[str]:
    """`unineg_scope=None` -> sin filtro de unidad de negocio (usuario común/analista,
    la BU no forma parte de su llave de autorización). `unineg_scope=set()` (vacío)
    -> filtra a NINGUNA unineg -> fail-closed (Supervisor sin BU asignada explícita
    no hereda acceso "sin restricción" por omisión)."""
    if not codigos_vendedor:
        return set()
    q = db.query(CarteraVendedor.codigo_cliente).filter(
        CarteraVendedor.vendedor_codigo.in_(codigos_vendedor)
    )
    if unineg_scope is not None:
        q = q.filter(CarteraVendedor.unineg.in_(unineg_scope))
    return {r[0] for r in q.distinct().all()}


def _cartera_propia(db: Session, u: User, unineg_scope: set[str] | None = None) -> set[str]:
    """Cuentas propias de un usuario: unión de lo que le toca por su código de
    operador y por su(s) código(s) de vendedor. `unineg_scope` se aplica
    EXCLUSIVAMENTE al lado vendedor: `operadores_comerciales.csv` no trae unidad
    de negocio, así que el lado operador entra siempre completo, sin importar el
    valor de `unineg_scope` (incluido vacío) — no hay columna `unineg` contra la
    que filtrarlo. Bug real que esto corrige (2026-08-24, caso Yanina Sassone):
    una intersección posterior contra `CarteraVendedor` se aplicaba sobre la unión
    completa (operador + vendedor), así que un cliente que solo existe en
    `cartera_operadores` (Mercado Público, sin fila en `cartera_vendedores`)
    quedaba excluido siempre, sin importar qué UN se le asignara al Supervisor.

    `unineg_scope` es EXCLUSIVAMENTE el scope del propio `u` — nunca el de un
    superior ni el de un subordinado (regla corregida 2026-08-20: la jerarquía
    manda, la UN de un jefe no filtra lo que hereda de su equipo). Los
    llamadores que arman la cartera de un Supervisor/Gerente deben pasar
    `unineg_scope=None` cuando invocan esto para un ANALISTA/SUPERVISOR a
    cargo — ver `clientes_visibles_para`."""
    fusion_operadores, fusion_vendedores, _match = fusion_codes_for_user(db, u)
    operadores = _codes(u.cartera_operador_codigos) | fusion_operadores
    vendedores = _codes(u.cartera_vendedor_codigos) | fusion_vendedores

    codigos_operador = _clientes_por_operador(db, operadores)
    if unineg_scope is not None and not unineg_scope:
        # Supervisor sin BU asignada explícita: fail-closed, pero solo del lado
        # vendedor — el operador no tiene UN de la que "cerrarse".
        codigos_vendedor: set[str] = set()
    else:
        codigos_vendedor = _clientes_por_vendedor(db, vendedores, unineg_scope)
    return codigos_operador | codigos_vendedor


def _cartera_de_equipo(db: Session, miembros: list[User]) -> set[str]:
    """Cartera PROPIA de VARIOS usuarios de una sola vez — 2 queries en TOTAL
    (operador + vendedor), no 2 por miembro. Arregla el N+1 de
    `clientes_visibles_para` para Supervisor/Gerente: antes, un Supervisor con
    N analistas a cargo pagaba ~2N+ queries (una `_cartera_propia` completa por
    analista) — medido en prod: 76 queries / 15s para un equipo de 14, con el
    mismo statement repetido 14 veces. `unineg_scope` es SIEMPRE None acá
    (miembros heredados de un Supervisor/Gerente nunca llevan scope de BU
    propio — ver `_cartera_propia`)."""
    if not miembros:
        return set()
    operadores: set[str] = set()
    vendedores: set[str] = set()
    for m in miembros:
        # fusion_codes_for_user NO pega la DB salvo que cartera_fusion_enabled
        # sea True (chequeo en memoria sobre la columna ya cargada) — así que
        # esto no reintroduce el N+1 para el caso común (fusión apagada).
        fusion_operadores, fusion_vendedores, _match = fusion_codes_for_user(db, m)
        operadores |= _codes(m.cartera_operador_codigos) | fusion_operadores
        vendedores |= _codes(m.cartera_vendedor_codigos) | fusion_vendedores
    return _clientes_por_operador(db, operadores) | _clientes_por_vendedor(db, vendedores, None)


def _usuarios_por_id(db: Session, ids: Iterable[int]) -> dict[int, User]:
    """Trae varios User por id en UNA query (`IN (...)`) en vez de un `db.get()`
    por id en un loop — mismo motivo que `_cartera_de_equipo`."""
    ids = [i for i in ids if i is not None]
    if not ids:
        return {}
    filas = db.execute(select(User).where(User.id.in_(ids))).scalars().all()
    return {u.id: u for u in filas}


# ─────────────────────────────────────────────────────────────────────────────
# Caché en memoria de clientes_visibles_para, por user.id (ago-2026). Motivo:
# la resolución de cartera de un Supervisor/Gerente (aunque ya batcheada arriba)
# sigue costando 2+ queries por carga de pantalla, y hoy se recalcula en CADA
# carga de Dimensionamiento/Oportunidades/Forecast para el mismo usuario.
#
# Invalidación: SIEMPRE completa (invalidate_cartera_scope_cache() sin
# argumentos), nunca "solo este usuario" — un cambio en la cartera/jerarquía
# de UN usuario puede repercutir en la cartera AGREGADA de su Supervisor y,
# transitivamente, de su Gerente. Calcular exactamente a quién afecta cada
# cambio es exactamente el tipo de lógica que es fácil dejar incompleta (ver
# el propio bug de Myriam/Daniela documentado arriba en este archivo) — limpiar
# todo es más caro por request pero CERO riesgo de servir una cartera vieja a
# alguien que no era el usuario editado. El caché es chico (un objeto liviano
# por usuario activo), así que recalcular todo de nuevo es barato.
#
# El TTL de abajo es un backstop, NO el mecanismo principal: cubre el caso de
# un cambio que ocurre por un camino que no llama a
# invalidate_cartera_scope_cache() (hoy conocido: push_cartera_data.py, script
# externo que reescribe cartera_operadores/cartera_vendedores fuera del
# proceso web — no hay forma de que un cache en memoria de ESTE proceso se
# entere de una escritura de OTRO proceso sin un mecanismo aparte). Los 3
# puntos de escritura del lado de la app (crear/editar/borrar usuario en
# sic_router.py) SÍ llaman a invalidate_cartera_scope_cache() — para esos, el
# cambio se ve al instante, sin esperar el TTL.
_CARTERA_SCOPE_CACHE: dict[int, dict] = {}
_CARTERA_SCOPE_CACHE_LOCK = threading.Lock()
_TTL_CARTERA_SCOPE = 900.0  # 15 min — backstop, ver comentario arriba.


def invalidate_cartera_scope_cache() -> None:
    """Limpia TODO el caché de clientes_visibles_para. Llamar después de
    cualquier cambio que pueda afectar cartera o jerarquía de cualquier
    usuario (alta/baja/edición en S.I.C.) — ver sic_router.py."""
    with _CARTERA_SCOPE_CACHE_LOCK:
        _CARTERA_SCOPE_CACHE.clear()


def clientes_visibles_para(db: Session, user: User) -> CarteraScope:
    """Resuelve la cartera visible para `user`, cacheada por user.id (ver
    _CARTERA_SCOPE_CACHE arriba). El cálculo real vive en
    `_resolver_cartera_visible` — esta función solo agrega el caché, para que
    ningún caller tenga que cambiar (todos siguen llamando a
    `clientes_visibles_para` igual que siempre)."""
    now = time.perf_counter()
    cached = _CARTERA_SCOPE_CACHE.get(user.id)
    if cached is not None and now - cached["ts"] < _TTL_CARTERA_SCOPE:
        return cached["val"]
    scope = _resolver_cartera_visible(db, user)
    with _CARTERA_SCOPE_CACHE_LOCK:
        _CARTERA_SCOPE_CACHE[user.id] = {"ts": now, "val": scope}
    return scope


def _is_admin_para_ver_como(user: User) -> bool:
    """Import perezoso (evita ciclo de import a nivel de módulo): `policy.py`
    importa `visibility_service`/`nav`, ninguno de los cuales importa este
    archivo, pero se mantiene local para no acoplar el orden de import de todo
    el paquete a este único uso."""
    from web_comparativas.policy import is_admin
    return is_admin(user)


def _coerce_ver_como_ids(value) -> "set[int]":
    """Normaliza `ver_como_user_ids` a un set[int] vacío ante CUALQUIER valor que no
    sea una lista/tupla/set de ids reales — en particular, el propio objeto
    `fastapi.Depends(...)`/`Query(...)` que Python asigna como default cuando una
    ruta con ese parámetro se llama directo (fuera del ciclo de request de FastAPI,
    p.ej. en tests que invocan la función del router a mano) en vez de `None`. Sin
    esto, ese objeto se cuela hasta acá y `resolve_effective_scope` explota al
    intentar iterarlo — con esto, simplemente se trata como "sin selección"."""
    if not value or not isinstance(value, (list, tuple, set, frozenset)):
        return set()
    out: set[int] = set()
    for item in value:
        try:
            out.add(int(item))
        except (TypeError, ValueError):
            continue
    return out


def clientes_visibles_para_usuarios(db: Session, users: Iterable[User]) -> CarteraScope:
    """Unión de `clientes_visibles_para` para VARIOS usuarios — motor de "Ver como
    usuario" (Mercado Privado, admin-only, sep-2026). Cada usuario resuelve su
    propia cartera exactamente como hoy (su propia jerarquía, su propio
    `cartera_unineg_scope`) — esta función SOLO agrega los resultados ya resueltos,
    no toca `clientes_visibles_para`/`_resolver_cartera_visible` para un usuario
    solo. `unrestricted=True` si CUALQUIERA de los usuarios seleccionados ya es
    unrestricted por sí mismo (admin/auditor) — mismo criterio que si ese usuario
    mirara el módulo por su cuenta."""
    users = list(users)
    if not users:
        return NONE_
    scopes = [clientes_visibles_para(db, u) for u in users]
    if any(s.unrestricted for s in scopes):
        return ALL
    codigos = frozenset(codigo for s in scopes for codigo in s.codigos_cliente)
    branches = tuple(branch for s in scopes for branch in s.branches)
    return CarteraScope(unrestricted=False, codigos_cliente=codigos, branches=branches)


def resolve_effective_scope(
    db: Session, user: User, ver_como_user_ids: Iterable[int] | None = None
) -> CarteraScope:
    """Punto de entrada único de "Ver como usuario" (Mercado Privado, admin-only,
    sep-2026) — los módulos deben resolver su scope a través de ESTA función, no
    llamando a `clientes_visibles_para(db, user)` directo, para que la selección
    admin se aplique de forma consistente en todos lados.

    Seguridad — SIEMPRE server-side: si `user` no es admin, `ver_como_user_ids` se
    IGNORA por completo (sin error visible) y el resultado es IDÉNTICO a
    `clientes_visibles_para(db, user)` de siempre. Nunca confiar en el front.

    Sin selección (lista vacía/None), o admin sin ids resueltos: comportamiento
    IDÉNTICO al de hoy (Admin ve todo vía `clientes_visibles_para` → ALL).

    Con selección Y admin: la cartera efectiva es la UNIÓN de la cartera visible de
    cada usuario seleccionado — ver `clientes_visibles_para_usuarios`.

    El chequeo de admin usa `policy.is_admin` (ADMIN_ROLES: admin/administrator/
    administrador) — el mismo criterio que gatea el resto de las pantallas
    admin-only de la app — y no el rol canónico único ("admin") que reconoce el
    resto de este motor (ver docstring del módulo): así el control de "Ver como
    usuario" queda visible/activo para exactamente los mismos usuarios que ya ven
    hoy cualquier otra función admin-only."""
    ids = _coerce_ver_como_ids(ver_como_user_ids)
    if not ids or not _is_admin_para_ver_como(user):
        return clientes_visibles_para(db, user)
    usuarios = _usuarios_por_id(db, ids)
    if not usuarios:
        return clientes_visibles_para(db, user)
    return clientes_visibles_para_usuarios(db, usuarios.values())


def _resolver_cartera_visible(db: Session, user: User) -> CarteraScope:
    """Cálculo real de la cartera visible para `user` — sin caché (la cachea
    `clientes_visibles_para`, que es la función pública). Función pura: no
    consulta ningún feature flag — quien la llame decide si corresponde
    aplicarla en ese módulo."""
    rol = (user.role or "").strip().lower()

    if rol in _ROLES_FULL_READ:
        return ALL

    if rol in _ROLES_ANALISTA:
        # Usuario común: sin scope de BU, ve exactamente su propia cartera.
        codigos = _cartera_propia(db, user, unineg_scope=None)
        branch = CarteraBranchScope(
            root_user_id=user.id,
            member_user_ids=frozenset({user.id}),
            codigos_cliente=frozenset(codigos),
            unidades_negocio=None,
        )
        return CarteraScope(
            unrestricted=False,
            codigos_cliente=branch.codigos_cliente,
            branches=(branch,),
        )

    if rol in _ROLES_SUPERVISOR:
        # Regla corregida 2026-08-20: la jerarquía manda. cartera_unineg_scope
        # SOLO acota la cartera PROPIA de este Supervisor (sus propios códigos
        # de vendedor/operador) — set() vacío sigue siendo fail-closed ahí. Lo
        # que hereda de cada Analista a cargo entra COMPLETO, sin aplicar este
        # scope: la UN del jefe no es un filtro sobre las cuentas del equipo.
        # Bug real que esto corrige: Myriam (vendedor 1, unineg 4) reporta a
        # Daniela (scope 5+15) — Daniela no veía nada de Myriam porque su
        # cartera se intersecaba contra un scope que no es el suyo.
        unineg_scope = _codes(user.cartera_unineg_scope)
        codigos = _cartera_propia(db, user, unineg_scope=unineg_scope)
        analistas_map = _usuarios_por_id(db, analistas_a_cargo(db, user.id))
        member_ids = {user.id} | set(analistas_map.keys())
        codigos |= _cartera_de_equipo(db, list(analistas_map.values()))
        branch = CarteraBranchScope(
            root_user_id=user.id,
            member_user_ids=frozenset(member_ids),
            codigos_cliente=frozenset(codigos),
            unidades_negocio=frozenset(unineg_scope),
        )
        return CarteraScope(
            unrestricted=False,
            codigos_cliente=branch.codigos_cliente,
            branches=(branch,),
        )

    if rol in _ROLES_GERENTE:
        # Sin cartera propia (igual que en oportunidades_visibilidad.py): agrega la
        # cartera de sus supervisores a cargo y, transitivamente, la de los analistas
        # de esos supervisores.
        # Regla corregida 2026-08-20 (mismo criterio que Supervisor arriba): la UN
        # del supervisor acota SOLO la cartera PROPIA de ESE supervisor — nunca lo
        # que sus analistas aportan a la rama. La jerarquía manda de punta a punta.
        branches: list[CarteraBranchScope] = []
        supervisores_map = _usuarios_por_id(db, supervisores_a_cargo(db, user.id))
        for supervisor in supervisores_map.values():
            sup_unineg_scope = _codes(supervisor.cartera_unineg_scope)
            branch_codes = _cartera_propia(db, supervisor, unineg_scope=sup_unineg_scope)
            analistas_map = _usuarios_por_id(db, analistas_a_cargo(db, supervisor.id))
            member_ids = {supervisor.id} | set(analistas_map.keys())
            branch_codes |= _cartera_de_equipo(db, list(analistas_map.values()))
            branches.append(CarteraBranchScope(
                root_user_id=supervisor.id,
                member_user_ids=frozenset(member_ids),
                codigos_cliente=frozenset(branch_codes),
                unidades_negocio=frozenset(sup_unineg_scope),
            ))
        codigos = frozenset(
            codigo for branch in branches for codigo in branch.codigos_cliente
        )
        return CarteraScope(
            unrestricted=False,
            codigos_cliente=codigos,
            branches=tuple(branches),
        )

    # Cualquier otro rol: fail-closed.
    return NONE_


def usuarios_con_cartera(db: Session) -> "frozenset[int]":
    """IDs de usuarios para los que `clientes_visibles_para` devolvería AL MENOS UNA
    cuenta — usado para acotar el padrón de "Ver como usuario" (sep-2026, Mercado
    Privado) a usuarios con cartera real, en vez de listar a todo el mundo.

    Resuelve el conjunto ENTERO en un número FIJO de queries (no una por usuario):
    2 para traer usuarios y jerarquía completos, 2 para los padrones de cartera
    (operadores/vendedores), y como mucho una tanda acotada al puñado de usuarios
    con `cartera_fusion_enabled=True` (bandera rara, opt-in por usuario — ver
    `fusion_name_matching.fusion_codes_for_user`, que ya sale gratis cuando está
    apagada). Todo lo demás se resuelve en memoria con sets de Python. Deliberado:
    llamar a `clientes_visibles_para` en un loop por usuario reproduciría el mismo
    problema de performance ya visto en Aprobaciones Forecast (recorre la jerarquía
    y pega la DB por cada Supervisor/Gerente, en vez de una sola vez para todos).

    admin/auditor quedan AFUERA (unrestricted=True -> codigos_cliente vacío por
    construcción, ver `ALL` arriba): "ver como" alguien que ya ve todo no aporta
    nada distinto de no seleccionar a nadie."""
    usuarios = db.query(User).all()
    usuarios_por_id = {u.id: u for u in usuarios}

    operador_codes_con_cuenta = {
        row[0] for row in db.query(CarteraOperador.operador_codigo).distinct().all()
    }
    vendedor_unineg_por_codigo: dict[str, set[str]] = {}
    for codigo, unineg in db.query(CarteraVendedor.vendedor_codigo, CarteraVendedor.unineg).distinct().all():
        vendedor_unineg_por_codigo.setdefault(codigo, set()).add(unineg)

    analistas_de: dict[int, list[User]] = {}
    supervisores_de: dict[int, list[User]] = {}
    for superior_id, subordinado_id in db.query(UserReporte.superior_id, UserReporte.subordinado_id).all():
        hijo = usuarios_por_id.get(subordinado_id)
        if hijo is None:
            continue
        rol_hijo = (hijo.role or "").strip().lower()
        if rol_hijo in _ROLES_ANALISTA:
            analistas_de.setdefault(superior_id, []).append(hijo)
        elif rol_hijo in _ROLES_SUPERVISOR:
            supervisores_de.setdefault(superior_id, []).append(hijo)

    # Fusión: opt-in por usuario (columna, no flag global) y ya sale gratis (early
    # return sin pegar la DB) para quien la tiene apagada — ver fusion_codes_for_user.
    # Se resuelve UNA vez por usuario fusion-enabled (nunca dentro de un loop por
    # cada Supervisor/Gerente que lo tenga en su equipo).
    fusion_por_usuario: dict[int, tuple[set[str], set[str]]] = {}
    for u in usuarios:
        if getattr(u, "cartera_fusion_enabled", False):
            fop, fven, _match = fusion_codes_for_user(db, u)
            if fop or fven:
                fusion_por_usuario[u.id] = (fop, fven)

    def _tiene_cartera_propia(u: User, unineg_scope: "set[str] | None") -> bool:
        """Mismo criterio que `_cartera_propia`, pero solo pregunta "¿hay algo?"
        en vez de traer el set completo de cuentas — evita el `IN (...)` contra
        cartera_operadores/cartera_vendedores por usuario."""
        fop, fven = fusion_por_usuario.get(u.id, (set(), set()))
        operadores = _codes(u.cartera_operador_codigos) | fop
        if operadores & operador_codes_con_cuenta:
            return True
        vendedores = _codes(u.cartera_vendedor_codigos) | fven
        if not vendedores:
            return False
        if unineg_scope is not None and not unineg_scope:
            return False  # fail-closed: sin BU asignada, lado vendedor (ver _cartera_propia)
        for codigo in vendedores:
            disponibles = vendedor_unineg_por_codigo.get(codigo)
            if not disponibles:
                continue
            if unineg_scope is None or (disponibles & unineg_scope):
                return True
        return False

    def _equipo_tiene_cartera(miembros: list[User]) -> bool:
        # Miembros heredados de un Supervisor/Gerente: unineg_scope=None, igual
        # criterio que _cartera_de_equipo (nunca el UN del jefe, ver su docstring).
        return any(_tiene_cartera_propia(m, None) for m in miembros)

    resultado: set[int] = set()
    for u in usuarios:
        rol = (u.role or "").strip().lower()
        if rol in _ROLES_FULL_READ:
            continue
        if rol in _ROLES_ANALISTA:
            if _tiene_cartera_propia(u, None):
                resultado.add(u.id)
        elif rol in _ROLES_SUPERVISOR:
            unineg_scope = _codes(u.cartera_unineg_scope)
            if _tiene_cartera_propia(u, unineg_scope) or _equipo_tiene_cartera(analistas_de.get(u.id, [])):
                resultado.add(u.id)
        elif rol in _ROLES_GERENTE:
            for supervisor in supervisores_de.get(u.id, []):
                sup_unineg_scope = _codes(supervisor.cartera_unineg_scope)
                if (
                    _tiene_cartera_propia(supervisor, sup_unineg_scope)
                    or _equipo_tiene_cartera(analistas_de.get(supervisor.id, []))
                ):
                    resultado.add(u.id)
                    break
        # cualquier otro rol: fail-closed, nunca entra (igual que _resolver_cartera_visible)

    return frozenset(resultado)
