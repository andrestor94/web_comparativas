# web_comparativas/visibility_service.py
from __future__ import annotations
from typing import Optional, Dict, Set, Tuple, List
from dataclasses import dataclass

from sqlalchemy.orm import Session
from sqlalchemy import or_, func, select
from sqlalchemy.orm import aliased

# Importamos solo los modelos/base de datos, no helpers de visibilidad
from .models import (
    Upload,
    User,
    Group,
    GroupMember,
)

# Estados que ya usas en tu flujo:
PENDING_STATUSES = ("pending", "classifying", "processing", "reviewing")
DASHBOARD_STATUS = "dashboard"
DONE_STATUS = "done"

# ----------------------------------------------------------------------
# Roles y normalizadores
# ----------------------------------------------------------------------
ADMIN_ROLES = {"admin", "administrator", "administrador"}
AUDITOR_ROLES = {"auditor", "visor", "viewer"}
MANAGER_ROLES = {"gerente", "manager"}
SUPERVISOR_ROLES = {"supervisor"}
ANALYST_ROLES = {"analista", "analyst"}  # nombre(s) de rol para analistas


def _norm(s) -> str:
    return (str(s or "")).strip().lower()


def _role_of(user) -> str:
    # tolera "rol" por compatibilidad
    return _norm(getattr(user, "role", "") or getattr(user, "rol", ""))


def _is_admin(user) -> bool:
    # Lectura total para admin y auditor
    r = _role_of(user)
    return (r in ADMIN_ROLES) or (r in AUDITOR_ROLES)


def _is_supervisor(user) -> bool:
    return _role_of(user) in SUPERVISOR_ROLES


def _is_analyst(user) -> bool:
    return _role_of(user) in ANALYST_ROLES


def _is_manager(user) -> bool:
    return _role_of(user) in MANAGER_ROLES


# ----------------------------------------------------------------------
# Núcleo de visibilidad
# ----------------------------------------------------------------------
def get_visible_user_ids(session: Session, user) -> set[int]:
    """
    Devuelve los IDs de usuarios cuyos uploads puede ver `user`:

      - ADMIN/AUDITOR: todos los usuarios.
      - GERENTE/SUPERVISOR: todos los usuarios con rol ANALISTA de su misma unidad de negocio + él mismo.
      - ANALISTA: él mismo + miembros de sus grupos creados por admin/supervisor/gerente
                  que compartan su unidad de negocio.
      - RESTO: él mismo + miembros de sus grupos, EXCLUYENDO a cualquier supervisor o gerente.
    """
    if not user:
        return set()

    # 1) Admin/Auditor: ve a todos
    if _is_admin(user):
        return {uid for (uid,) in session.query(User.id).all()}

    me = int(user.id)

    # 2) Gerente / Supervisor: todos los ANALISTAS de su misma BU (+ él mismo)
    if _is_manager(user) or _is_supervisor(user):
        ids: Set[int] = {me}
        bu_norm = _norm(getattr(user, "unit_business", None))
        if bu_norm:
            analyst_ids = [
                uid
                for (uid,) in (
                    session.query(User.id)
                    .filter(
                        func.lower(func.trim(User.unit_business)) == bu_norm,
                        func.lower(User.role).in_(tuple(ANALYST_ROLES)),
                    )
                    .all()
                )
            ]
            ids.update(int(x) for x in analyst_ids)
        return ids

    # 3) Analista: él mismo + miembros de grupos válidos dentro de su BU
    if _is_analyst(user):
        ids: Set[int] = {me}
        bu_norm = _norm(getattr(user, "unit_business", None))
        if not bu_norm:
            return ids

        allowed_roles = tuple(ADMIN_ROLES | SUPERVISOR_ROLES | MANAGER_ROLES)
        creator_alias = aliased(User)
        group_ids = [
            int(gid)
            for (gid,) in (
                session.query(GroupMember.group_id)
                .join(Group, GroupMember.group_id == Group.id)
                .join(creator_alias, Group.created_by_user_id == creator_alias.id)
                .filter(
                    GroupMember.user_id == me,
                    func.lower(creator_alias.role).in_(allowed_roles),
                    func.lower(func.trim(Group.business_unit)) == bu_norm,
                )
                .all()
            )
        ]

        if not group_ids:
            return ids

        member_rows = (
            session.query(GroupMember.user_id)
            .join(Group, GroupMember.group_id == Group.id)
            .join(User, GroupMember.user_id == User.id)
            .filter(
                GroupMember.group_id.in_(group_ids),
                func.lower(func.trim(User.unit_business)) == bu_norm,
                ~func.lower(User.role).in_(tuple(SUPERVISOR_ROLES | MANAGER_ROLES)),
            )
            .all()
        )
        ids.update(int(uid) for (uid,) in member_rows)
        return ids

    # 4) Otros (p.ej. roles custom no listados): restringido solo a su propio usuario.
    #    Esto mantiene un comportamiento conservador si el rol no coincide
    #    exactamente con los conocidos (evitando saltos de visibilidad inesperados).
    return {me}


def visible_user_ids(session: Session, user) -> set[int]:
    """Alias retrocompatible para código que siga usando el nombre previo."""
    return get_visible_user_ids(session, user)


def uploads_visible_query(session: Session, user):
    """
    Query base de Uploads visibles para el actor.
      - Admin/Auditor: sin filtro (ve TODO).
      - Analista: uploads de los usuarios visibles (sin fallback por 'mismo proceso').
      - Supervisor/Otros: filtra por user_ids visibles + fallback por 'mismo proceso'.
    """
    if _is_admin(user):
        return session.query(Upload)

    me = int(user.id)

    # Regla para Analista: uploads de usuarios visibles (sin fallback por proceso)
    if _is_analyst(user):
        ids = list(visible_user_ids(session, user) or {me})
        return session.query(Upload).filter(Upload.user_id.in_(ids))

    # Resto de roles: visibilidad por usuarios + fallback por mismo proceso
    ids = list(visible_user_ids(session, user)) or [me]

    # Subquery con los proceso_key que cargó el propio usuario
    own_keys_subq = (
        session.query(Upload.proceso_key)
        .filter(
            Upload.user_id == me,
            Upload.proceso_key.isnot(None),
        )
        .subquery()
    )

    return (
        session.query(Upload)
        .filter(
            or_(
                Upload.user_id.in_(ids),  # regla tradicional por visibilidad de usuario
                Upload.proceso_key.in_(select(own_keys_subq.c.proceso_key)),  # mismo proceso
            )
        )
    )


def can_view_upload(session: Session, user, upload: Upload) -> bool:
    """
    Chequeo puntual de permiso de lectura:
      - Admin/Auditor: siempre True
      - Analista: True si el owner está en visible_user_ids (sin fallback por proceso).
      - Otros: True si el owner está en visible_user_ids o si cargó algún upload con el mismo proceso_key
    """
    if _is_admin(user):
        return True

    me = int(user.id)

    # Regla para Analista: puede ver uploads de usuarios visibles (sin fallback)
    if _is_analyst(user):
        return int(upload.user_id) in (visible_user_ids(session, user) or {me})

    # Otros roles: mismo comportamiento anterior
    ids = visible_user_ids(session, user) or {me}
    if int(upload.user_id) in ids:
        return True

    if upload.proceso_key:
        exists_same_key = (
            session.query(Upload.id)
            .filter(
                Upload.user_id == me,
                Upload.proceso_key == upload.proceso_key,
            )
            .limit(1)
            .first()
            is not None
        )
        if exists_same_key:
            return True

    return False


# ----------------------------------------------------------------------
# Paginación utilitaria
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class Page:
    page: int = 1
    page_size: int = 25

    @property
    def offset(self) -> int:
        p = max(1, int(self.page))
        return (p - 1) * self.page_size

    @property
    def limit(self) -> int:
        # límite de seguridad para no cargar de más
        return min(max(1, int(self.page_size)), 200)


# ----------------------------------------------------------------------
# Consultas de listado (buscador + estado) usando la visibilidad anterior
# ----------------------------------------------------------------------
def visible_uploads(
    session: Session,
    user,
    q: Optional[str] = None,
    status: Optional[str] = None,
    page: Page = Page(),
    order: str = "-created_at",
):
    """
    Devuelve (items, total) de uploads visibles al usuario:
      - Admin/Auditor: todos
      - Analista: propios + miembros visibles de sus grupos (sin fallback por proceso)
      - Supervisor: propios + analistas de su BU
      - Otros: propios + compañeros de grupo (sin supervisores)
      + Fallback por 'mismo proceso' solo para roles NO analistas.
    """
    qry = uploads_visible_query(session, user)

    if q:
        pattern = f"%{q.strip()}%"
        qry = qry.filter(
            or_(
                Upload.original_filename.ilike(pattern),
                Upload.proceso_nro.ilike(pattern),
                Upload.buyer_hint.ilike(pattern),
                Upload.province_hint.ilike(pattern),
                Upload.platform_hint.ilike(pattern),
            )
        )

    if status:
        qry = qry.filter(Upload.status == status)

    total = qry.count()

    if order == "-created_at":
        qry = qry.order_by(Upload.created_at.desc())
    elif order == "created_at":
        qry = qry.order_by(Upload.created_at.asc())
    elif order == "-updated_at":
        qry = qry.order_by(Upload.updated_at.desc())
    elif order == "updated_at":
        qry = qry.order_by(Upload.updated_at.asc())

    items = qry.offset(page.offset).limit(page.limit).all()
    return items, total


# ----------------------------------------------------------------------
# KPIs y “recientes” con visibilidad aplicada
# ----------------------------------------------------------------------
def kpis_for_home(session: Session, user) -> Dict[str, int]:
    """
    KPIs para las tarjetas del panel, restringidos a visibilidad:
      - pending: pending|classifying|processing|reviewing
      - done: done
      - total: todos los visibles
    """
    # Rama rápida para admin/auditor: no filtra por usuario
    if _is_admin(user):
        pending = (
            session.query(func.count(Upload.id))
            .filter(Upload.status.in_(PENDING_STATUSES))
            .scalar()
            or 0
        )
        done = (
            session.query(func.count(Upload.id))
            .filter(Upload.status == DONE_STATUS)
            .scalar()
            or 0
        )
        total = session.query(func.count(Upload.id)).scalar() or 0
        return {"pending": pending, "done": done, "total": total}

    ids = list(visible_user_ids(session, user) or {int(user.id)})

    pending = (
        session.query(func.count(Upload.id))
        .filter(Upload.user_id.in_(ids), Upload.status.in_(PENDING_STATUSES))
        .scalar()
        or 0
    )
    done = (
        session.query(func.count(Upload.id))
        .filter(Upload.user_id.in_(ids), Upload.status == DONE_STATUS)
        .scalar()
        or 0
    )
    total = (
        session.query(func.count(Upload.id))
        .filter(Upload.user_id.in_(ids))
        .scalar()
        or 0
    )
    return {"pending": pending, "done": done, "total": total}


def recent_done(session: Session, user, limit: int = 5):
    """Últimos finalizados visibles (para 'Últimos procesados')."""
    return (
        uploads_visible_query(session, user)
        .filter(Upload.status == DONE_STATUS)
        .order_by(Upload.updated_at.desc())
        .limit(limit)
        .all()
    )


def recent_dashboards(session: Session, user, limit: int = 5):
    """Opcional: últimos con estado 'dashboard' visibles."""
    return (
        uploads_visible_query(session, user)
        .filter(Upload.status == DASHBOARD_STATUS)
        .order_by(Upload.updated_at.desc())
        .limit(limit)
        .all()
    )
