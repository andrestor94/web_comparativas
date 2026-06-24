# web_comparativas/policy.py
"""
Motor centralizado de permisos para el sistema S.I.C.
Fuente única de verdad para todas las decisiones de autorización.

══════════════════════════════════════════════════════════════════════
DEFINICIÓN DE ROLES (canónica, sin excepciones)
══════════════════════════════════════════════════════════════════════

ADMIN (admin, administrator, administrador)
  - Ve: TODO el sistema sin restricción de BU ni apartado.
  - Puede: cargar, editar, eliminar, gestionar usuarios, gestionar grupos,
           acceder a todos los módulos.
  - BU requerida: NO.

AUDITOR (auditor, visor, viewer)
  - Ve: TODO el sistema sin restricción de BU ni apartado (igual que Admin).
  - Puede: SOLO LECTURA. Sin excepciones. No carga, no edita, no elimina,
           no gestiona usuarios, no gestiona grupos.
  - BU requerida: NO.
  - DIFERENCIA con Admin: puede ver exactamente lo mismo, pero no puede
    ejecutar ninguna acción de escritura.

MANAGER / GERENTE (gerente, manager)
  - Ve: TODO el sistema sin restricción de BU (misma visibilidad que Admin/Auditor).
  - Puede: cargar archivos y operar funcionalmente.
  - No puede: gestionar usuarios ni gestionar grupos (eso es exclusivo de Admin).
  - BU requerida: NO.
  - DIFERENCIA con Admin: no gestiona usuarios/grupos.
  - DIFERENCIA con Auditor: SÍ puede realizar acciones de escritura operativa.
  - DIFERENCIA con Supervisor: visibilidad cross-BU (no restringido a una sola BU).

SUPERVISOR (supervisor)
  - Ve: sus propias cargas + todas las cargas de analistas de su misma BU.
  - Puede: cargar, gestionar grupos dentro de su BU, coordinar analistas.
  - No puede: gestionar usuarios globalmente, crear usuarios.
  - BU requerida: SÍ (obligatoria — sin BU el supervisor solo ve lo propio).

ANALISTA (analista, analyst)
  - Ve: sus propias cargas + cargas de miembros de grupos válidos (mismo BU,
        grupo creado por admin/supervisor, excluyendo supervisores/gerentes).
  - Puede: cargar archivos propios.
  - No puede: gestionar usuarios, gestionar grupos.
  - BU requerida: SÍ (obligatoria — sin BU solo ve lo propio y no puede tener grupos).

══════════════════════════════════════════════════════════════════════
ACCESS_SCOPE — DEFINICIÓN FORMAL (NO ES PERMISO DE DATOS)
══════════════════════════════════════════════════════════════════════

access_scope es un campo de ROUTING, no de seguridad de datos.

  Valores válidos: "todos", "mercado_publico", "mercado_privado", "privado"
  Efecto real: determina a qué URL se redirige el usuario post-login.
  Efecto en datos: NINGUNO. El modelo Upload no tiene columna market_type.
                   Un analista con scope="mercado_privado" que navega manualmente
                   a /mercado-publico ve los datos de su BU en ese contexto.

  Si en el futuro se requiere filtro real de datos por mercado:
    1. Agregar columna `market_type` a la tabla `uploads`.
    2. Filtrar en uploads_visible_query() por market_type según access_scope.
    3. Cambiar el nombre del campo a algo como `routing_market` para evitar
       confusión semántica con permisos reales.

══════════════════════════════════════════════════════════════════════
JERARQUÍA DE EVALUACIÓN (prioridad descendente)
══════════════════════════════════════════════════════════════════════

  1. Rol global         → admin/auditor/manager: visibilidad total de datos.
  2. Tipo de acción     → auditor: DENY en toda escritura. Sin excepciones.
  3. Propiedad del dato → owner siempre puede ver y modificar lo propio.
  4. Grupo asignado     → expande visibilidad del analista. No eleva permisos.
  5. Unidad de negocio  → frontera estructural para supervisor y analista.
  6. Apartado/scope     → solo routing post-login, no filtro de DB.
  7. Deny-by-default    → cualquier caso no cubierto explícitamente = DENY.

══════════════════════════════════════════════════════════════════════
FUENTE ÚNICA DE VERDAD — MAPA DE FUNCIONES
══════════════════════════════════════════════════════════════════════

  can_view_upload       → policy.py  (wrapper sobre visibility_service)
  can_create_upload     → policy.py
  can_edit_upload       → policy.py
  can_delete_upload     → policy.py
  can_manage_user       → policy.py
  can_manage_group      → policy.py
  can_access_module     → policy.py
  visible_upload_scope  → policy.py  (delega a visibility_service)
  visible_user_scope    → policy.py  (delega a visibility_service)
  get_visible_user_ids  → visibility_service.py  (implementación SQL)
  uploads_visible_query → visibility_service.py  (implementación SQL)
  resolve_login_redirect→ policy.py
"""
from __future__ import annotations
from typing import TYPE_CHECKING

from fastapi import Request, HTTPException

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from web_comparativas.models import Upload, User, Group

# Re-exportamos los helpers de rol desde visibility_service (fuente única de verdad)
from web_comparativas.visibility_service import (
    _role_of,
    _norm,
    ADMIN_ROLES,
    AUDITOR_ROLES,
    MANAGER_ROLES,
    SUPERVISOR_ROLES,
    ANALYST_ROLES,
    _FULL_READ_ROLES,
    get_visible_user_ids,
    uploads_visible_query,
    can_view_upload as _can_view_upload_data,
)

# Estructura declarativa del menú (única fuente de verdad de keys/roles/jerarquía)
from web_comparativas import nav
from web_comparativas.nav import MENU, MENU_BY_KEY, MODULES, TOP_LEVEL_KEYS


# ──────────────────────────────────────────────────────────────────────────────
# Predicados de rol (nombres semánticamente correctos)
# ──────────────────────────────────────────────────────────────────────────────

def is_admin(user) -> bool:
    """Admin puro: acceso total, puede gestionar usuarios y grupos."""
    return _role_of(user) in ADMIN_ROLES


def is_auditor(user) -> bool:
    """Auditor: acceso de lectura total. No puede escribir nada."""
    return _role_of(user) in AUDITOR_ROLES


def is_manager(user) -> bool:
    """
    Gerente/Manager: visibilidad total (igual que Admin/Auditor en datos).
    Puede cargar y operar. NO puede gestionar usuarios ni grupos.
    Diferencia clave vs Admin: sin gestión de usuarios/grupos.
    Diferencia clave vs Auditor: SÍ puede escribir/cargar.
    """
    return _role_of(user) in MANAGER_ROLES


def is_supervisor(user) -> bool:
    """Supervisor: opera dentro de su BU. Puede coordinar analistas."""
    return _role_of(user) in SUPERVISOR_ROLES


def is_analyst(user) -> bool:
    """Analista: opera dentro de su BU y grupos asignados."""
    return _role_of(user) in ANALYST_ROLES


def has_full_read(user) -> bool:
    """
    True si el usuario tiene visibilidad de lectura irrestricta (admin, auditor, manager).
    Usar esta función en lugar del confuso _is_admin de visibility_service.
    """
    return _role_of(user) in _FULL_READ_ROLES


def has_write_access(user) -> bool:
    """
    True si el usuario puede realizar acciones de escritura.
    Auditor queda excluido explícitamente aunque tenga lectura total.
    """
    if not user:
        return False
    return _role_of(user) not in AUDITOR_ROLES


def puede_editar_ficha_pliego(user) -> bool:
    """
    ¿Puede `user` usar la edición inline de las tarjetas de la Ficha del pliego
    (Mercado Público → Lectura de Pliegos)?

    Regla: TODOS los roles con acceso a la sección menos Auditor (solo lectura).
    El acceso a la sección en sí lo gobierna el menú/can_access; esta función
    decide únicamente el permiso de EDITAR las tarjetas, no el de ver la ficha.

    Fuente única de verdad: delega en has_write_access (role ∉ AUDITOR_ROLES),
    de modo que Admin, Gerente, Supervisor, Analista y demás roles de escritura
    ven el lápiz y Auditor/visor/viewer quedan en solo lectura.
    """
    return has_write_access(user)


# ──────────────────────────────────────────────────────────────────────────────
# Sección: Aprobaciones Forecast (pestaña dentro del módulo Forecast) — VER / EDITAR
# ──────────────────────────────────────────────────────────────────────────────
#
# Matriz por rol (ÚNICA fuente de verdad de esta sección):
#   Admin      → ver + editar
#   Gerente    → ver + editar
#   Auditor    → ver (solo lectura, NO edita)
#   Analista   → sin acceso
#   Supervisor → sin acceso
#
# IMPORTANTE: NO se reutiliza has_write_access para "editar" — ese predicado
# (role ∉ AUDITOR_ROLES) habilitaría también a Supervisor y Analista, que aquí NO
# deben editar. La regla de Aprobaciones es más restrictiva, por eso son predicados
# dedicados, en el mismo patrón sección-específico que puede_editar_ficha_pliego.
# Se construyen sobre los role-sets canónicos (sin inventar vocabulario nuevo).

def puede_ver_aprobaciones_forecast(user) -> bool:
    """
    ¿Puede `user` VER la pestaña/lectura de "Aprobaciones Forecast"?
    Admin, Gerente y Auditor: SÍ. Analista y Supervisor: NO.
    Gobierna el render del tab/panel y los endpoints GET de la sección.
    """
    if not user:
        return False
    return _role_of(user) in (ADMIN_ROLES | MANAGER_ROLES | AUDITOR_ROLES)


def puede_editar_aprobaciones_forecast(user) -> bool:
    """
    ¿Puede `user` EJECUTAR acciones que cambian estado (aprobar/rechazar, individual
    o por grupo) en "Aprobaciones Forecast"?
    SOLO Admin y Gerente. Auditor (solo lectura), Analista y Supervisor: NO.
    Gobierna los endpoints POST de mutación y la visibilidad de los controles de edición.
    """
    if not user:
        return False
    return _role_of(user) in (ADMIN_ROLES | MANAGER_ROLES)


# ──────────────────────────────────────────────────────────────────────────────
# Acción: Uploads — LEER
# ──────────────────────────────────────────────────────────────────────────────

def can_view_upload(session: "Session", user, upload: "Upload") -> bool:
    """
    ¿Puede `user` ver el upload?

    Jerarquía:
      1. Admin/Auditor/Manager → siempre True.
      2. Owner del upload → siempre True.
      3. Grupo válido o analistas de BU → según visibility_service.
      4. Deny-by-default.
    """
    if not user or not upload:
        return False
    if has_full_read(user):
        return True
    if int(upload.user_id) == int(user.id):  # owner siempre puede ver lo propio
        return True
    return _can_view_upload_data(session, user, upload)


# ──────────────────────────────────────────────────────────────────────────────
# Acción: Uploads — CREAR
# ──────────────────────────────────────────────────────────────────────────────

def can_create_upload(user) -> bool:
    """
    ¿Puede `user` cargar un nuevo archivo/proceso?

    Reglas:
      - Auditor: NO (solo lectura, sin excepciones).
      - Admin, Supervisor, Analista: SÍ.
      - Cualquier otro rol: NO (deny-by-default).
    """
    if not user:
        return False
    role = _role_of(user)
    # Auditor explícitamente bloqueado aunque es "full read"
    if role in AUDITOR_ROLES:
        return False
    # Roles que pueden cargar
    allowed = ADMIN_ROLES | MANAGER_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES
    return role in allowed


# ──────────────────────────────────────────────────────────────────────────────
# Acción: Uploads — EDITAR / ELIMINAR
# ──────────────────────────────────────────────────────────────────────────────

def can_edit_upload(user, upload: "Upload") -> bool:
    """
    ¿Puede `user` editar los metadatos de un upload?

    Reglas:
      - Auditor: NO.
      - Admin: SÍ (cualquier upload).
      - Supervisor/Analista: SÍ si es el owner.
      - Otros: NO.
    """
    if not user or not upload:
        return False
    if is_auditor(user):
        return False
    if is_admin(user):
        return True
    return int(upload.user_id) == int(user.id)


def can_delete_upload(user, upload: "Upload") -> bool:
    """
    ¿Puede `user` eliminar un upload?

    Reglas:
      - Auditor: NO.
      - Admin: SÍ (cualquier upload).
      - Supervisor: SÍ solo si es el owner.
      - Analista: SÍ solo si es el owner.
      - Otros: NO.
    """
    if not user or not upload:
        return False
    if is_auditor(user):
        return False
    if is_admin(user):
        return True
    return int(upload.user_id) == int(user.id)


# ──────────────────────────────────────────────────────────────────────────────
# Acción: Usuarios — VER
# ──────────────────────────────────────────────────────────────────────────────

def can_view_user(session: "Session", actor, target_user: "User") -> bool:
    """
    ¿Puede `actor` ver el perfil/datos de `target_user`?

    Reglas:
      - Admin/Auditor/Manager: SÍ (ven a todos).
      - Supervisor: SÍ si el target está en su BU (rol analista o él mismo).
      - Analista: SÍ si el target está en visible_user_ids (mismo grupo/BU).
      - Propio usuario: siempre SÍ.
      - Deny-by-default.
    """
    if not actor or not target_user:
        return False
    if int(actor.id) == int(target_user.id):
        return True
    if has_full_read(actor):
        return True
    visible = get_visible_user_ids(session, actor)
    return int(target_user.id) in visible


# ──────────────────────────────────────────────────────────────────────────────
# Acción: Usuarios — GESTIONAR (crear, editar, eliminar, cambiar rol)
# ──────────────────────────────────────────────────────────────────────────────

def can_manage_user(actor, target_user: "User" = None) -> bool:
    """
    ¿Puede `actor` crear/editar/eliminar/cambiar rol de `target_user`?

    Reglas:
      - Admin: SÍ siempre.
      - Auditor, Supervisor, Analista, otros: NO.

    Nota: la gestión de usuarios es exclusiva del rol Admin.
    El Supervisor puede gestionar GRUPOS (no usuarios directamente).
    """
    if not actor:
        return False
    return is_admin(actor)


# ──────────────────────────────────────────────────────────────────────────────
# Acción: Grupos — GESTIONAR
# ──────────────────────────────────────────────────────────────────────────────

def can_manage_group(actor, group: "Group" = None) -> bool:
    """
    ¿Puede `actor` crear/editar/eliminar un grupo?

    Reglas:
      - Admin: SÍ (cualquier grupo).
      - Supervisor: SÍ solo si el grupo pertenece a su misma BU (o si group es None → crear nuevo).
      - Otros: NO.
    """
    if not actor:
        return False
    if is_admin(actor):
        return True
    if is_supervisor(actor):
        if group is None:
            return True  # puede crear grupos en su BU
        group_bu = _norm(getattr(group, "business_unit", None))
        actor_bu = _norm(getattr(actor, "unit_business", None))
        if not actor_bu:
            return False  # supervisor sin BU: denegado (configuración inválida)
        return group_bu == actor_bu
    return False


def can_add_member_to_group(actor, target_user: "User", group: "Group") -> bool:
    """
    ¿Puede `actor` agregar a `target_user` como miembro del `group`?

    Reglas:
      - Admin: SÍ (cualquier usuario, cualquier grupo).
      - Supervisor: SÍ si target comparte su BU Y el grupo corresponde a esa BU.
      - Otros: NO.
    """
    if not actor or not target_user or not group:
        return False
    if is_admin(actor):
        return True
    if is_supervisor(actor):
        actor_bu = _norm(getattr(actor, "unit_business", None))
        target_bu = _norm(getattr(target_user, "unit_business", None))
        group_bu = _norm(getattr(group, "business_unit", None))
        if not actor_bu:
            return False
        same_bu_target = bool(target_bu) and target_bu == actor_bu
        group_ok = (not group_bu) or group_bu == actor_bu
        return same_bu_target and group_ok
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Acción: Módulos / Sub-secciones — ACCESO (jerárquico, basado en nav.MENU)
# ──────────────────────────────────────────────────────────────────────────────
#
# Modelo:
#   - El ROL define el TECHO de keys visibles (nav.MENU[key].roles + ancestros).
#     admin y gerente/manager son UNIVERSALES (techo = todo el MENU).
#   - El campo por-usuario User.module_access SOLO restringe dentro de ese techo:
#       · NULL  → acceso completo a todas las hojas que su rol permita (legacy).
#       · lista → set de keys de HOJA concedidas; efectivo = lista ∩ hojas-del-rol.
#       · []    → sin acceso a ninguna hoja del catálogo.
#
# MODULES (catálogo top-level key→label) y la jerarquía vienen de nav (única fuente).

# Módulos admin-only / históricos que NO están en nav.MENU: se gobiernan SOLO por
# rol (no por module_access), igual que siempre. No se tocan.
_LEGACY_MODULE_ROLES: dict[str, set[str]] = {
    "admin_usuarios": ADMIN_ROLES,
    "admin_grupos":   ADMIN_ROLES | SUPERVISOR_ROLES,
    "reports":        ADMIN_ROLES | AUDITOR_ROLES | MANAGER_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
    "helpdesk":       ADMIN_ROLES | AUDITOR_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
}

# Roles universales para el control por módulo: ven TODO el catálogo (techo = MENU).
#   admin            → superusuario.
#   gerente/manager  → lectura total (techo = todos los módulos, incluido S.I.C).
# Lo que cada uno tiene DE VERDAD lo define module_access; el techo no recorta nada.
_UNIVERSAL_MENU_ROLES = ADMIN_ROLES | MANAGER_ROLES


def role_allows_key(role: str, key: str) -> bool:
    """
    ¿El rol `role` puede ver/acceder la `key`?
    Chequea nav.MENU[key].roles Y los roles de todos sus ancestros.
    admin y gerente/manager → universales (siempre True).
    """
    role_n = _norm(role)
    if role_n in _UNIVERSAL_MENU_ROLES:
        return True
    item = MENU_BY_KEY.get(key)
    if item is None:
        return False
    node = item
    while node is not None:
        roles = node.get("roles")
        if roles is not None and role_n not in roles:
            return False
        parent = node.get("parent")
        node = MENU_BY_KEY.get(parent) if parent else None
    return True


def role_allows(user, key: str) -> bool:
    """Variante de role_allows_key sobre el objeto user."""
    if not user:
        return False
    return role_allows_key(_role_of(user), key)


def role_allowed_leaves(role: str) -> set[str]:
    """Hojas del MENU que el rol `role` puede ver (techo de hojas del rol)."""
    return {lk for lk in nav.leaf_keys() if role_allows_key(role, lk)}


def parse_module_access(raw):
    """
    Normaliza el valor crudo de User.module_access a list[str] | None,
    independientemente de cómo lo devuelva el motor de base de datos.

    Motivación: en PostgreSQL la columna es TEXT (ver migrations.py) y, según el
    driver/dialecto, el valor JSON puede llegar como STRING sin deserializar
    ('["mercado_publico.home", ...]') en vez de como lista. Si no se parsea, el
    código que itera o hace `in` sobre module_access recorre el string carácter a
    carácter → granted_set vacío → bucle de login. En SQLite llega como lista.

    Semántica (NO confundir None con vacío):
      - None            → None  (NULL = legacy / acceso completo al techo del rol).
      - list / tuple    → [str(x).strip() for x in raw].
      - str ""          → []    (configurado sin acceso explícito).
      - str JSON lista  → la lista parseada (strip por elemento).
      - str no parseable → []   (no rompe; tratado como sin acceso).
    """
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw]
    if isinstance(raw, str):
        s = raw.strip()
        if s == "":
            return []
        try:
            import json
            val = json.loads(s)
        except Exception:
            return []
        if isinstance(val, (list, tuple)):
            return [str(x).strip() for x in val]
        return []
    return []


def granted_set(user) -> set[str]:
    """
    Hojas EFECTIVAMENTE concedidas al usuario:
      - module_access NULL  → todas las hojas que su rol permita (legacy = completo).
      - module_access lista → lista ∩ hojas-permitidas-por-rol.
      - module_access []    → vacío.
    El valor crudo se normaliza con parse_module_access (tolera str JSON de Postgres).
    """
    if not user:
        return set()
    allowed = role_allowed_leaves(_role_of(user))
    grant = parse_module_access(getattr(user, "module_access", None))
    if grant is None:
        return allowed  # NULL = sin configurar → acceso completo del rol
    return set(grant) & allowed


def can_access(user, key: str) -> bool:
    """
    ¿Puede `user` acceder a la `key` (hoja o rama)?
      - Hoja → key ∈ granted_set.
      - Rama → alguna hoja descendiente ∈ granted_set.
    Siempre AND con role_allows (el techo del rol).
    """
    if not user or key not in MENU_BY_KEY:
        return False
    if not role_allows(user, key):
        return False
    gset = granted_set(user)
    if nav.is_leaf(key):
        return key in gset
    return any(leaf in gset for leaf in nav.descendant_leaves(key))


def can_access_module(user, module: str) -> bool:
    """
    Compatibilidad: acceso a un módulo top-level del catálogo (vía can_access),
    o a un módulo admin-only histórico (solo por rol).
    Deny-by-default para módulos desconocidos.
    """
    if not user:
        return False
    if module in MENU_BY_KEY:
        return can_access(user, module)
    allowed_roles = _LEGACY_MODULE_ROLES.get(module)
    if allowed_roles is None:
        return False
    return _role_of(user) in allowed_roles


def derive_access_scope(granted_leaves) -> str:
    """
    Deriva access_scope (routing post-login) a partir de las hojas concedidas:
      - público y privado → "todos"
      - solo público → "publico" ; solo privado → "privado"
      - ninguno → "todos" (default seguro; el landing real lo decide
        resolve_login_redirect según el rol).
    """
    keys = {str(k) for k in (granted_leaves or [])}
    has_pub = any(k.startswith("mercado_publico") for k in keys)
    has_priv = any(k.startswith("mercado_privado") for k in keys)
    if has_pub and has_priv:
        return "todos"
    if has_pub:
        return "publico"
    if has_priv:
        return "privado"
    return "todos"


def normalize_module_access(keys, role: str) -> list[str]:
    """
    Normaliza las keys recibidas del formulario:
      = [k for k in keys if k es HOJA válida de MENU and role_allows_key(rol, k)]
    Orden estable según nav.leaf_keys().
    """
    recibidas = {str(k).strip() for k in (keys or [])}
    return [lk for lk in nav.leaf_keys() if lk in recibidas and role_allows_key(role, lk)]


# ──────────────────────────────────────────────────────────────────────────────
# Dependencia FastAPI: exigir acceso a una key (módulo o sub-sección)
# ──────────────────────────────────────────────────────────────────────────────

def require_perm(key: str):
    """
    Fábrica de dependencia FastAPI que exige acceso a la `key` del MENU.
      - 401 si no hay sesión.
      - 403 si not can_access(user, key).
      - devuelve el User (drop-in de _require_user).
    """
    def _dep(request: Request):
        user = getattr(request.state, "user", None)
        if not user:
            raise HTTPException(status_code=401, detail="No autenticado")
        if not can_access(user, key):
            raise HTTPException(status_code=403, detail="Sección no autorizada para este usuario.")
        return user
    return _dep


# Alias retrocompatible: require_module(key) == require_perm(key).
require_module = require_perm


# Apartados top-level (criterio ÚNICO): S.I.C cuenta como un apartado más, igual que
# los mercados, forecast e indicadores. Una sola lista y un solo contador para todo
# (Suite /markets, botón "Cambiar Mercado", "Volver a SIEM").
_TOP_KEYS = ["sic", "mercado_publico", "mercado_privado", "forecast", "indicadores_comerciales"]


def accessible_top_count(user) -> int:
    """Cuántos apartados top-level (S.I.C incluido) puede abrir el user."""
    if not user:
        return 0
    return sum(1 for k in _TOP_KEYS if can_access(user, k))


def can_switch_market(user) -> bool:
    """True si el user puede entrar a 2+ apartados top-level (hay algo entre qué cambiar)."""
    return accessible_top_count(user) >= 2


# ──────────────────────────────────────────────────────────────────────────────
# Helpers para el FORMULARIO de permisos (templates/sic/users_form.html)
# ──────────────────────────────────────────────────────────────────────────────

# Roles ofrecidos en el <select> de Rol del formulario (clave canónica).
FORM_ROLES: list[str] = ["admin", "gerente", "supervisor", "analista", "auditor"]


def role_ceilings_map() -> dict[str, list[str]]:
    """
    {rol: [keys de hoja que ese rol puede tener]} para los roles del formulario.
    El JS lo usa para habilitar/deshabilitar hojas al cambiar el rol.
    admin y gerente → todas las hojas (universales).
    """
    return {r: sorted(role_allowed_leaves(r)) for r in FORM_ROLES}


def form_nav_tree() -> list[dict]:
    """Árbol anidado del MENU para construir el árbol de checkboxes del formulario."""
    return nav.menu_tree()


# ──────────────────────────────────────────────────────────────────────────────
# Scope de visibilidad de datos
# ──────────────────────────────────────────────────────────────────────────────

def visible_upload_scope(session: "Session", user):
    """
    Query base de uploads visibles para `user` (delegado a visibility_service).
    Usar este en todos los routers para garantizar una única fuente de verdad.
    """
    return uploads_visible_query(session, user)


def visible_user_scope(session: "Session", user) -> set[int]:
    """
    Set de user_ids visibles para `user` (delegado a visibility_service).
    """
    return get_visible_user_ids(session, user)


# ──────────────────────────────────────────────────────────────────────────────
# Validaciones de configuración de usuario
# ──────────────────────────────────────────────────────────────────────────────

def validate_user_config(user) -> list[str]:
    """
    Devuelve una lista de errores de configuración del usuario.
    Lista vacía = configuración válida.

    Reglas obligatorias:
      - Supervisor: DEBE tener unit_business.
      - Analista: DEBE tener unit_business.
      - Auditor: puede existir sin BU (alcance global de lectura).
      - Admin: puede existir sin BU (acceso total).
    """
    errors: list[str] = []
    role = _role_of(user)
    bu = _norm(getattr(user, "unit_business", None))
    scope = _norm(getattr(user, "access_scope", None) or "todos")

    if role in SUPERVISOR_ROLES and not bu:
        errors.append(
            "Supervisor sin unidad de negocio: quedará con visibilidad reducida a sus propias cargas."
        )

    if role in ANALYST_ROLES and not bu:
        errors.append(
            "Analista sin unidad de negocio: solo verá sus propias cargas y no podrá pertenecer a grupos válidos."
        )

    valid_scopes = {"todos", "mercado_publico", "mercado_privado", "publico", "privado", ""}
    if scope not in valid_scopes:
        errors.append(
            f"access_scope inválido: '{scope}'. Valores permitidos: todos, mercado_publico, mercado_privado."
        )

    return errors


def first_accessible_url(user) -> str:
    """
    Primera URL del MENU (hojas EN ORDEN de definición) a la que `user` tiene acceso
    real (can_access = role_allows + granted_set). Es la landing correcta para roles
    restringidos: cae en la primera sección que SÍ puede abrir, sin mandarlo a una que
    le daría 403.

    Fallback "/mi/password" (siempre accesible) si no puede entrar a ninguna sección.
    """
    if not user:
        return "/mi/password"
    for key in nav.leaf_keys():
        if can_access(user, key):
            url = MENU_BY_KEY.get(key, {}).get("url")
            if url:
                return url
    return "/mi/password"


def resolve_login_redirect(user) -> str:
    """
    Determina la URL de destino después del login según rol.

    Regla única para el redirect post-login — NO duplicar esta lógica en rutas.
    """
    if not user:
        return "/login"
    role = _role_of(user)

    if role in (ANALYST_ROLES | SUPERVISOR_ROLES):
        # Landing derivada del MENU: primera sección accesible (respeta module_access).
        return first_accessible_url(user)

    # Roles "universales" (admin/auditor/gerente/manager): el panel general "/" solo sirve
    # de landing para quien puede ver el contexto público. Con module_access ACOTADO
    # (no NULL) la landing debe ser su primera sección accesible; con NULL (acceso total)
    # va a "/" como siempre.
    if getattr(user, "module_access", None) is not None:
        return first_accessible_url(user)

    return "/"  # acceso total (module_access NULL) → panel general
