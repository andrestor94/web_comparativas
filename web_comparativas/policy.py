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
# Acción: Módulos — ACCESO
# ──────────────────────────────────────────────────────────────────────────────

# Módulos disponibles y sus roles permitidos
_MODULE_ACCESS: dict[str, set[str]] = {
    "sic":               ADMIN_ROLES | AUDITOR_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
    "dimensionamiento":  ADMIN_ROLES | AUDITOR_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
    "mercado_publico":   ADMIN_ROLES | AUDITOR_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
    "mercado_privado":   ADMIN_ROLES | AUDITOR_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
    "admin_usuarios":    ADMIN_ROLES,
    "admin_grupos":      ADMIN_ROLES | SUPERVISOR_ROLES,
    "reports":           ADMIN_ROLES | AUDITOR_ROLES | MANAGER_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
    "helpdesk":          ADMIN_ROLES | AUDITOR_ROLES | SUPERVISOR_ROLES | ANALYST_ROLES,
}


def can_access_module(user, module: str) -> bool:
    """
    ¿Puede `user` acceder al módulo indicado?

    Módulos válidos: 'sic', 'dimensionamiento', 'mercado_publico', 'mercado_privado',
                     'admin_usuarios', 'admin_grupos', 'reports', 'helpdesk'.

    Deny-by-default para módulos desconocidos.
    """
    if not user:
        return False
    allowed_roles = _MODULE_ACCESS.get(module)
    if allowed_roles is None:
        return False  # módulo desconocido → denegado
    return _role_of(user) in allowed_roles


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


def resolve_login_redirect(user) -> str:
    """
    Determina la URL de destino después del login según rol y access_scope.

    Regla única para el redirect post-login — NO duplicar esta lógica en rutas.
    """
    if not user:
        return "/login"
    role = _role_of(user)
    scope = _norm(getattr(user, "access_scope", None) or "todos")

    if role in (ANALYST_ROLES | SUPERVISOR_ROLES):
        if scope in ("privado", "mercado_privado"):
            return "/mercado-privado"
        return "/mercado-publico"  # default seguro para analista/supervisor

    return "/"  # Admin, Auditor, Manager → panel general
