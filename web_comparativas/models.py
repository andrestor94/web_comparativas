# web_comparativas/models.py
from __future__ import annotations
from pathlib import Path
import os
import datetime as dt
from typing import Iterable, Set, List
import re  # <-- para normalizar procesos
import logging

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, ForeignKey, Float, event, text,
    UniqueConstraint, select, Boolean, JSON, Text, func, or_
)
from sqlalchemy.orm import (
    declarative_base, relationship, sessionmaker, scoped_session, aliased
)
from sqlalchemy.orm import foreign  # <-- para relación sin FK físico
from sqlalchemy import inspect  # para introspección de columnas

# ----------------------------------------------------------------------
# Unidades de negocio (exportadas para el resto de la app)
# ----------------------------------------------------------------------
BUSINESS_UNITS = [
    "Productos Hospitalarios",
    "Estética Médica y Reconstructiva",
    "Tratamientos Especiales",
    "Otros",
]

def normalize_unit_business(v: str) -> str:
    v = (v or "").strip()
    alias = {
        "Hospitalario Publico": "Productos Hospitalarios",
        "Hospitalario Público": "Productos Hospitalarios",
        "Hospitalario Privado": "Productos Hospitalarios",
    }
    v = alias.get(v, v)
    return v if v in BUSINESS_UNITS else "Otros"

# ----------------------------------------------------------------------
# Configuración de la DB (local o Render)
# ----------------------------------------------------------------------
RENDER_MODE = os.getenv("RENDER") == "true" or "render" in os.getenv("RENDER_EXTERNAL_HOSTNAME", "").lower()

# En Render usamos el path del proyecto, en local el del paquete
BASE_DIR = Path("/opt/render/project/src") if RENDER_MODE else Path(__file__).resolve().parent

# Por defecto, SQLite en web_comparativas/app.db
DB_FILE = BASE_DIR / "app.db"

# Si hay DATABASE_URL, la usamos (y normalizamos postgres:// -> postgresql://)
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if DATABASE_URL:
    SQLALCHEMY_DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://")
else:
    SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_FILE.as_posix()}"

if SQLALCHEMY_DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
else:
    # Postgres en Render:
    # 1. Timeout de conexión (TCP): 10s
    # 2. SSL requerido
    # 3. Timeout de consulta (statement): 5000ms para evitar hangs infinitos por bloqueos
    connect_args = {
        "connect_timeout": 10,
        "sslmode": "require",
        "options": "-c statement_timeout=5000"
    }

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,
    connect_args=connect_args,
    future=True
)

# Banderas útiles
IS_SQLITE = engine.url.get_backend_name() == "sqlite"
IS_POSTGRES = engine.url.get_backend_name().startswith("postgresql")

print(
    f"[DB DEBUG] Config: SSL=require, Timeout=5s. Backend: {engine.url.get_backend_name()} "
    f"(database={engine.url.database})",
    flush=True
)

# Activar foreign keys en SQLite
@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    if IS_SQLITE:
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

# Session factory + scoped_session
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
db_session = scoped_session(SessionLocal)

Base = declarative_base()

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def normalize_proceso_nro(value: str | None) -> str | None:
    """
    Normaliza el número de proceso para poder detectar duplicados.
    - trim
    - mayúsculas
    - colapsa espacios (incluye saltos de línea)
    """
    if not value:
        return None
    s = str(value).strip().upper()
    s = re.sub(r"\s+", " ", s)  # reemplazar múltiples espacios/saltos por uno
    return s or None

# ----------------------------------------------------------------------
# MODELOS
# ----------------------------------------------------------------------

# ---------- Usuarios / roles ----------
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, index=True, nullable=False)
    name = Column(String(120), nullable=True, default=None)
    full_name = Column(String, nullable=True)
    password_hash = Column(String)
    role = Column(String, default="auditor")  # visor/auditor/admin/supervisor…
    # Atributo del ORM = unit_business; columna física = business_unit
    unit_business = Column("business_unit", String(120), nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)
    
    # Campo para Segmentación de Acceso: "Mercado Publico", "Mercado Privado", "Todos"
    access_scope = Column(String, default="todos")

    # Relaciones con métricas de uso (EVITAR eager load masivo)
    # Antes: lazy="selectin" -> traía TODO el historial al cargar el usuario.
    # Ahora: lazy="dynamic" -> devuelve un Query object, o lazy=True para carga bajo demanda si se accede.
    # Usamos lazy="dynamic" para poder filtrar si fuese necesario, o simplemente "select" (True).
    # Como son logs, mejor no cargarlos por defecto.
    usage_events = relationship("UsageEvent", back_populates="user", lazy="select")
    usage_sessions = relationship("UsageSession", back_populates="user", lazy="select")

    # ---- Helpers de rol/unidad ----
    def _role_norm(self) -> str:
        return (self.role or "").strip().lower()

    def has_role(self, *roles: Iterable[str]) -> bool:
        wanted = {str(r).strip().lower() for r in roles}
        return self._role_norm() in wanted

    def is_admin(self) -> bool:
        return self.has_role("admin", "administrator", "administrador")

    def is_supervisor(self) -> bool:
        return self.has_role("supervisor")

    def can_manage_groups(self) -> bool:
        """Puede acceder a UI de Grupos."""
        return self.is_admin() or self.is_supervisor()

    def same_business_unit_as(self, other: "User") -> bool:
        a = (self.unit_business or "").strip().lower()
        b = (getattr(other, "unit_business", None) or "").strip().lower()
        return bool(a) and a == b

    def can_add_member(self, target: "User", group: "Group") -> bool:
        """
        Reglas:
          - Admin: puede agregar a cualquiera en cualquier grupo.
          - Supervisor: puede agregar SOLO si target comparte su BU y el grupo
                        corresponde a esa misma BU.
        """
        if self.is_admin():
            return True
        if not self.is_supervisor():
            return False
        same_bu_target = self.same_business_unit_as(target)
        group_ok = (not group.business_unit) or (
            (self.unit_business or "").strip().lower()
            == (group.business_unit or "").strip().lower()
        )
        return same_bu_target and group_ok

    def needs_admin_approval(self, target: "User", group: "Group") -> bool:
        if self.is_admin():
            return False
        if not self.is_supervisor():
            return True
        return (not self.same_business_unit_as(target)) or (
            group.business_unit
            and (self.unit_business or "").strip().lower()
            != (group.business_unit or "").strip().lower()
        )

    @property
    def display_name(self) -> str:
        return self.full_name or self.name or self.email

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email!r} role={self.role!r} bu={self.unit_business!r}>"

# Hacemos accesible la lista desde la clase (lo usa main/users_form)
User.BUSINESS_UNITS = BUSINESS_UNITS


# ---------- Cargas ----------
class Upload(Base):
    __tablename__ = "uploads"

    id = Column(Integer, primary_key=True)
    # mantenemos sin FK físico para evitar migración; relación read-only debajo
    user_id = Column(Integer, index=True, nullable=True)

    # Snapshot del cargador (permite auditoría aunque se elimine el User)
    uploaded_by_name = Column(String(120), nullable=True)   # ej: "Juan Pérez"
    uploaded_by_email = Column(String(255), nullable=True)  # ej: "juan@acme.com"

    # Metadatos visibles del proceso
    proceso_nro = Column(String, index=True)
    # clave normalizada para evitar duplicados
    proceso_key = Column(String, index=True)  # <-- clave normalizada

    apertura_fecha = Column(String, index=True)   # guardada como 'YYYY-MM-DD'
    cuenta_nro = Column(String, index=True)

    # Nuevos hints desde el formulario
    platform_hint = Column(String, index=True)    # BAC/COMPRAR/PBAC, etc.
    buyer_hint = Column(String, index=True)       # Comprador/entidad
    province_hint = Column(String, index=True)    # Provincia/Municipio

    # Archivo original y paths
    original_filename = Column(String)
    original_path = Column(String)
    base_dir = Column(String)

    # Datos de detección/procesamiento
    detected_source = Column(String)
    script_key = Column(String)

    # Estado
    status = Column(String, default="pending", index=True)

    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)
    updated_at = Column(
        DateTime, default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow, nullable=False, index=True
    )

    # Relación conveniente para comentarios (no carga en cascada)
    comments = relationship(
        "Comment",
        back_populates="upload",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    # ---- Relación de solo lectura al usuario propietario (NO crea FK en DB) ----
    user = relationship(
        "User",
        primaryjoin="foreign(Upload.user_id) == User.id",
        viewonly=True,
        lazy="joined",
    )

    # ---- Helpers de UI / auditoría ----
    @property
    def uploader_name(self) -> str | None:
        """
        Nombre estable del cargador:
        1) snapshot (uploaded_by_name)
        2) si existe el User, usa full_name/name/email
        3) si todo falla, usa uploaded_by_email
        """
        if self.uploaded_by_name:
            return self.uploaded_by_name
        if self.user:
            return self.user.full_name or self.user.name or self.user.email
        return self.uploaded_by_email

    @property
    def uploader_email(self) -> str | None:
        if self.uploaded_by_email:
            return self.uploaded_by_email
        if self.user:
            return self.user.email
        return None

    def __repr__(self) -> str:
        return f"<Upload id={self.id} status={self.status!r}>"

    # helper por si se necesita desde la app
    def ensure_proceso_key(self):
        self.proceso_key = normalize_proceso_nro(self.proceso_nro)

    __table_args__ = (
        # Evita duplicar el MISMO proceso para el MISMO usuario
        # (usuarios diferentes pueden cargar el mismo proceso sin problema)
        UniqueConstraint("user_id", "proceso_key", name="uq_upload_user_proceso"),
    )


# listeners para que siempre se complete proceso_key
@event.listens_for(Upload, "before_insert")
def _upload_before_insert(mapper, connection, target: Upload):
    # proceso_key
    target.proceso_key = normalize_proceso_nro(target.proceso_nro)
    # snapshot del cargador si no vino seteado
    try:
        if (not getattr(target, "uploaded_by_name", None) or not getattr(target, "uploaded_by_email", None)) and getattr(target, "user_id", None):
            row = connection.execute(
                text("SELECT full_name, name, email FROM users WHERE id = :uid"),
                {"uid": int(target.user_id)},
            ).fetchone()
            if row:
                full_name, name, email = row[0], row[1], row[2]
                name_pref = (full_name or "") or (name or "") or (email or "")
                if not target.uploaded_by_name and name_pref:
                    target.uploaded_by_name = name_pref
                if not target.uploaded_by_email and email:
                    target.uploaded_by_email = email
    except Exception:
        # no bloquear inserción si algo falla
        pass


@event.listens_for(Upload, "before_update")
def _upload_before_update(mapper, connection, target: Upload):
    # Recalcular siempre si hay proceso_nro; evita quedar desincronizado
    if target.proceso_nro:
        target.proceso_key = normalize_proceso_nro(target.proceso_nro)
    # completar snapshot si sigue vacío y tenemos user_id
    try:
        if (not getattr(target, "uploaded_by_name", None) or not getattr(target, "uploaded_by_email", None)) and getattr(target, "user_id", None):
            row = connection.execute(
                text("SELECT full_name, name, email FROM users WHERE id = :uid"),
                {"uid": int(target.user_id)},
            ).fetchone()
            if row:
                full_name, name, email = row[0], row[1], row[2]
                name_pref = (full_name or "") or (name or "") or (email or "")
                if not target.uploaded_by_name and name_pref:
                    target.uploaded_by_name = name_pref
                if not target.uploaded_by_email and email:
                    target.uploaded_by_email = email
    except Exception:
        pass


# ---------- Runs del procesamiento ----------
class Run(Base):
    __tablename__ = "runs"

    id = Column(Integer, primary_key=True)
    upload_id = Column(Integer, ForeignKey("uploads.id"), nullable=False, index=True)
    status = Column(String, index=True)
    started_at = Column(DateTime, index=True)
    ended_at = Column(DateTime, index=True)
    logs_path = Column(String)
    # upload = relationship("Upload", backref="runs")


# ---------- Archivos normalizados ----------
class NormalizedFile(Base):
    __tablename__ = "normalized_files"

    id = Column(Integer, primary_key=True)
    upload_id = Column(Integer, ForeignKey("uploads.id"), nullable=False, index=True)
    path = Column(String)
    row_count = Column(Integer)
    checksum = Column(String)
    # upload = relationship("Upload", backref="normalized_files")


# ---------- Dashboards generados ----------
class Dashboard(Base):
    __tablename__ = "dashboards"

    id = Column(Integer, primary_key=True)
    upload_id = Column(Integer, ForeignKey("uploads.id"), nullable=False, index=True)
    json_path = Column(String)
    html_path = Column(String)
    published_at = Column(DateTime, index=True)
    # upload = relationship("Upload", backref="dashboards")


# ---------- Notificaciones System ----------
class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)
    category = Column(String(50), default="system", index=True)  # helpdesk, system, processing
    link = Column(String(500), nullable=True)  # URL action
    is_read = Column(Boolean, default=False, nullable=False, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)

    user = relationship("User", backref="notifications")

    def __repr__(self) -> str:
        return f"<Notification id={self.id} user={self.user_id} title={self.title!r}>"


# ---------- Log de notificaciones por email (idempotencia) ----------
class EmailNotification(Base):
    __tablename__ = "email_notifications"

    id = Column(Integer, primary_key=True)
    upload_id = Column(Integer, ForeignKey("uploads.id"), nullable=False, index=True)
    recipient = Column(String(255), nullable=False, index=True)
    event = Column(String(50), nullable=False, default="done_email", index=True)
    sent_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("upload_id", "recipient", "event", name="uq_email_notif"),
    )

    def __repr__(self) -> str:
        return f"<EmailNotification up={self.upload_id} to={self.recipient!r} event={self.event!r}>"


# ---------- Grupos y miembros (N:N con User) ----------
class Group(Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(120), nullable=False, unique=True, index=True)
    # BU del grupo: guía la política para supervisores
    business_unit = Column(String(120), nullable=True, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)

    created_by = relationship("User", foreign_keys=[created_by_user_id])
    # memberships -> filas de GroupMember
    memberships = relationship(
        "GroupMember",
        cascade="all, delete-orphan",
        back_populates="group",
        lazy="selectin",
    )

    @property
    def users(self):
        # Conveniencia para acceder a la lista de usuarios
        return [m.user for m in self.memberships]

    def __repr__(self) -> str:
        return f"<Group id={self.id} name={self.name!r} bu={self.business_unit!r}>"


class GroupMember(Base):
    __tablename__ = "group_members"
    __table_args__ = (
        UniqueConstraint("group_id", "user_id", name="uq_group_user"),
    )

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey("groups.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    # owner | member | analyst | supervisor (string informativo)
    role_in_group = Column(String(32), default="member", nullable=False, index=True)
    added_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    added_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)

    group = relationship("Group", back_populates="memberships")
    user = relationship("User", foreign_keys=[user_id])
    added_by = relationship("User", foreign_keys=[added_by_user_id])

    def __repr__(self) -> str:
        return f"<GroupMember g={self.group_id} u={self.user_id} role={self.role_in_group!r}>"


# ---------- Vistas guardadas por usuario ----------
class SavedView(Base):
    """
    Preferencias guardadas por usuario para una vista concreta (p.ej. 'dashboard').
    payload almacena un JSON con filtros/ajustes (supplier_filter, fit_mode, density,
    column_order, hidden_columns, date_range, search_query, etc.).
    """
    __tablename__ = "saved_views"
    __table_args__ = (
        UniqueConstraint("user_id", "view_id", "name", name="uq_savedview_user_view_name"),
    )

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    view_id = Column(String(64), nullable=False, index=True, default="dashboard")  # p.ej. 'dashboard'
    name = Column(String(120), nullable=False)  # nombre legible: "Vista Suizo Compacta"
    is_default = Column(Boolean, nullable=False, default=False, index=True)
    payload = Column(JSON, nullable=False, default=dict)  # JSON con los ajustes
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow,
                        nullable=False, index=True)

    user = relationship("User", backref="saved_views")

    def __repr__(self) -> str:
        return f"<SavedView id={self.id} user={self.user_id} view={self.view_id!r} name={self.name!r} default={self.is_default}>"


# ---------- Configuración general (AppConfig) ----------
class AppConfig(Base):
    """
    Configuración simple clave/valor para la aplicación.
    Aquí vamos a guardar, entre otras cosas, la contraseña especial de RESET.
    """
    __tablename__ = "app_config"

    id = Column(Integer, primary_key=True)
    key = Column(String(120), nullable=False, unique=True, index=True)
    value = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow,
                        nullable=False, index=True)




# ---------- Comentarios / Feedback ----------
class Comment(Base):
    """
    Comentario/feedback asociado a un Upload (tablero). Permite hilos (parent_id).
    """
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)

    upload_id = Column(Integer, ForeignKey("uploads.id", ondelete="CASCADE"), nullable=False, index=True)
    author_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    # Hilo (respuesta a otro comentario)
    parent_id = Column(Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=True, index=True)

    # Contenido y estado
    body = Column(Text, nullable=False)  # texto del comentario
    is_resolved = Column(Boolean, default=False, nullable=False, index=True)

    # Extra (etiquetas, tipo: 'bug','idea','dato', etc.)
    meta = Column(JSON, nullable=True)

    # Tiempos
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False, index=True)
    deleted_at = Column(DateTime, nullable=True, index=True)  # soft-delete opcional

    # Relaciones
    upload = relationship("Upload", back_populates="comments")
    author = relationship("User", foreign_keys=[author_user_id])
    parent = relationship("Comment", remote_side=[id], backref="replies", lazy="selectin")

    def __repr__(self) -> str:
        body_preview = (self.body or "").strip().replace("\n", " ")
        if len(body_preview) > 24:
            body_preview = body_preview[:24] + "…"
        return f"<Comment id={self.id} up={self.upload_id} by={self.author_user_id} '{body_preview}'>"


# ---------- Métricas de uso: eventos y sesiones ----------
class UsageEvent(Base):
    """
    Evento granular de uso de la interfaz.
    Cada acción relevante genera un registro acá.
    """
    __tablename__ = "usage_events"

    id = Column(Integer, primary_key=True, index=True)

    # Momento exacto del evento
    timestamp = Column(DateTime, nullable=False, index=True, default=dt.datetime.utcnow)

    # Sesión lógica del usuario
    session_id = Column(String(64), nullable=False, index=True)

    # Usuario y rol asociado
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user_role = Column(String(32), nullable=False, index=True)

    # Qué pasó y dónde pasó
    action_type = Column(String(50), nullable=False, index=True)   # login, page_view, file_upload, etc.
    section = Column(String(100), nullable=True, index=True)       # home, buscador, dimensiones, etc.
    resource_id = Column(String(100), nullable=True)               # id de proceso / archivo, si aplica

    # Duración aproximada de la acción (ms, opcional)
    duration_ms = Column(Integer, nullable=True)

    # Datos extra en JSON (filtros aplicados, parámetros, etc.)
    extra_data = Column(JSON, nullable=True)

    # Datos técnicos
    ip = Column(String(50), nullable=True)
    user_agent = Column(Text, nullable=True)

    # Relación con el usuario
    user = relationship("User", back_populates="usage_events")

    def __repr__(self) -> str:
        return f"<UsageEvent id={self.id} user={self.user_id} action={self.action_type!r} section={self.section!r}>"


class UsageSession(Base):
    """
    Resumen agregado de una sesión de trabajo del usuario.
    Una sesión agrupa muchos UsageEvent.
    """
    __tablename__ = "usage_sessions"

    id = Column(Integer, primary_key=True, index=True)

    # Identificador lógico de la sesión
    session_id = Column(String(64), nullable=False, unique=True, index=True)

    # Usuario y rol
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user_role = Column(String(32), nullable=False, index=True)

    # Tiempos
    start_time = Column(DateTime, nullable=False, index=True, default=dt.datetime.utcnow)
    end_time = Column(DateTime, nullable=True, index=True)

    # Minutos totales / activos / inactivos
    duration_minutes = Column(Float, nullable=True)
    active_minutes = Column(Float, nullable=True)
    idle_minutes = Column(Float, nullable=True)

    # Métricas agregadas dentro de la sesión
    files_uploaded = Column(Integer, nullable=False, default=0)
    actions_count = Column(Integer, nullable=False, default=0)
    sections_visited = Column(Integer, nullable=False, default=0)

    # Relación con el usuario
    user = relationship("User", back_populates="usage_sessions")

    def __repr__(self) -> str:
        return f"<UsageSession id={self.id} user={self.user_id} session={self.session_id!r}>"


    def __repr__(self) -> str:
        return f"<UsageSession id={self.id} user={self.user_id} session={self.session_id!r}>"


# ----------------------------------------------------------------------
# Chat System (Teams-like)
# ----------------------------------------------------------------------

class ChatChannel(Base):
    """
    Canal de chat. Puede ser 'direct' (1 a 1) o 'group'.
    """
    __tablename__ = "chat_channels"

    id = Column(Integer, primary_key=True)
    type = Column(String(20), default="direct", nullable=False) # direct, group
    name = Column(String(100), nullable=True) # Para grupos
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False, index=True)

    members = relationship("ChatMember", back_populates="channel", cascade="all, delete-orphan", lazy="selectin")
    messages = relationship("ChatMessage", back_populates="channel", cascade="all, delete-orphan", lazy="selectin")

    def __repr__(self) -> str:
        return f"<ChatChannel id={self.id} type={self.type!r}>"


class ChatMember(Base):
    """
    Miembros de un canal.
    """
    __tablename__ = "chat_members"
    __table_args__ = (
        UniqueConstraint("channel_id", "user_id", name="uq_chat_member"),
    )

    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("chat_channels.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    joined_at = Column(DateTime, default=dt.datetime.utcnow)
    last_read_at = Column(DateTime, default=dt.datetime.utcnow) # Para saber qué mensajes son nuevos

    channel = relationship("ChatChannel", back_populates="members")
    user = relationship("User")

    def __repr__(self) -> str:
        return f"<ChatMember channel={self.channel_id} user={self.user_id}>"


class ChatMessage(Base):
    """
    Mensajes de chat.
    """
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("chat_channels.id"), nullable=False, index=True)
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    content = Column(Text, nullable=True) # Puede ser nulo si solo es attachment
    attachment_path = Column(String, nullable=True) # Path al archivo/imagen
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)

    channel = relationship("ChatChannel", back_populates="messages")
    sender = relationship("User")

    def __repr__(self) -> str:
        return f"<ChatMessage id={self.id} channel={self.channel_id} sender={self.sender_id}>"


# ----------------------------------------------------------------------
# Helpers de visibilidad (utilizables desde vistas/rutas)
# ----------------------------------------------------------------------
_ADMIN_ROLES = {"admin", "administrator", "administrador"}
_AUDITOR_ROLES = {"auditor", "visor", "viewer"}
_SUPERVISOR_ROLES = {"supervisor"}
_ANALYST_ROLES = {"analista", "analyst"}

def _norm(s) -> str:
    return (str(s or "")).strip().lower()

def _role_of(u: User) -> str:
    return _norm(getattr(u, "role", "") or getattr(u, "rol", ""))

def visible_user_ids(session, user: User) -> Set[int]:
    """
    Devuelve el set de user_id cuyos uploads puede ver `user`:
      - ADMIN/AUDITOR: todos
      - SUPERVISOR: él mismo + todos los ANALISTAS de su misma unidad de negocio
      - ANALISTA: él mismo + miembros de grupos creados por admin/supervisor
                  que compartan su unidad de negocio
      - RESTO: iguales a 'solo él mismo'
    """
    if not user:
        return set()

    r = _role_of(user)
    if r in _ADMIN_ROLES or r in _AUDITOR_ROLES:
        rows = session.execute(select(User.id)).scalars().all()
        return {int(x) for x in rows}

    my_id = int(user.id)

    # Supervisores: él + analistas de su BU (se mantiene igual)
    if r in _SUPERVISOR_ROLES:
        ids: Set[int] = {my_id}
        bu = _norm(user.unit_business)
        if bu:
            analyst_ids = session.execute(
                select(User.id).where(
                    func.lower(func.trim(User.unit_business)) == bu,
                    func.lower(User.role).in_(tuple(_ANALYST_ROLES)),
                )
            ).scalars().all()
            ids.update(int(x) for x in analyst_ids)
        return ids

    if r in _ANALYST_ROLES:
        ids: Set[int] = {my_id}
        bu = _norm(user.unit_business)
        if not bu:
            return ids

        allowed_roles = tuple(_ADMIN_ROLES | _SUPERVISOR_ROLES)
        creator_alias = aliased(User)
        group_ids = [
            int(gid)
            for gid in session.execute(
                select(GroupMember.group_id)
                .join(Group, GroupMember.group_id == Group.id)
                .join(creator_alias, Group.created_by_user_id == creator_alias.id)
                .where(
                    GroupMember.user_id == my_id,
                    func.lower(creator_alias.role).in_(allowed_roles),
                    func.lower(func.trim(Group.business_unit)) == bu,
                )
            ).scalars().all()
        ]

        if not group_ids:
            return ids

        member_ids = session.execute(
            select(GroupMember.user_id)
            .join(Group, GroupMember.group_id == Group.id)
            .join(User, GroupMember.user_id == User.id)
            .where(
                GroupMember.group_id.in_(group_ids),
                func.lower(func.trim(User.unit_business)) == bu,
                ~func.lower(User.role).in_(tuple(_SUPERVISOR_ROLES)),
            )
        ).scalars().all()
        ids.update(int(x) for x in member_ids)
        return ids

    # Otros roles no listados: SOLO lo propio
    return {my_id}


def uploads_visible_query(session, user: User):
    """
    Query utilitario para listar historial 'visible' al usuario:
    - Admin/Auditor: TODOS.
    - Supervisor: propios + analistas de su BU.
    - Analista: propios + miembros visibles de sus grupos (sin fallback por proceso).
    - Regla 'mismo proceso_key' SOLO aplica a NO-analistas.
    """
    r = _role_of(user)
    if r in _ADMIN_ROLES or r in _AUDITOR_ROLES:
        return session.query(Upload)

    # Analista: uploads de usuarios visibles (sin fallback por proceso)
    if r in _ANALYST_ROLES:
        ids = list(visible_user_ids(session, user) or {int(user.id)})
        return session.query(Upload).filter(Upload.user_id.in_(ids))

    # Supervisor (y otros no-analistas con visibilidad extendida)
    ids = visible_user_ids(session, user) or {int(user.id)}

    # Para no-analistas (p.ej. supervisores), si el usuario cargó un proceso,
    # puede ver tableros de otros con el mismo proceso_key (se mantiene).
    own_keys_subq = (
        select(Upload.proceso_key)
        .where(Upload.user_id == int(user.id), Upload.proceso_key.isnot(None))
        .subquery()
    )

    return session.query(Upload).filter(
        or_(
            Upload.user_id.in_(ids),
            Upload.proceso_key.in_(select(own_keys_subq.c.proceso_key)),
        )
    )


def comments_visible_query(session, user: User, upload_id: int | None = None):
    """
    Comentarios visibles para `user` (mismo criterio que uploads_visible_query).
    Si `upload_id` se provee, filtra por ese upload.
    """
    ids = visible_user_ids(session, user)
    q = session.query(Comment).join(Upload, Comment.upload_id == Upload.id).filter(
        Upload.user_id.in_(ids)
    )
    if upload_id is not None:
        q = q.filter(Comment.upload_id == int(upload_id))
    return q


def can_view_upload(session, user: User, upload: Upload) -> bool:
    """
    Chequeo puntual de autorización para abrir el tablero:
      - Admin/Auditor: True
      - Supervisor: reglas de visible_user_ids + 'mismo proceso_key'
      - Analista: True si el owner está en sus usuarios visibles (sin fallback por proceso).
    """
    if user is None or upload is None:
        return False

    r = _role_of(user)
    if r in _ADMIN_ROLES or r in _AUDITOR_ROLES:
        return True

    # Analista: solo uploads de usuarios visibles (sin fallback)
    if r in _ANALYST_ROLES:
        return int(upload.user_id) in (visible_user_ids(session, user) or {int(user.id)})

    # Supervisores (u otros no-analistas): reglas ampliadas
    if int(upload.user_id) in (visible_user_ids(session, user) or {int(user.id)}):
        return True

    if upload.proceso_key:
        exists_same = session.query(Upload.id).filter(
            Upload.user_id == int(user.id),
            Upload.proceso_key == upload.proceso_key,
        ).limit(1).first()
        if exists_same is not None:
            return True

    return False


def find_self_duplicate_upload(session, *, user_id: int, proceso_nro: str | None = None, proceso_key: str | None = None) -> Upload | None:
    """
    Busca si el usuario ya cargó ese mismo proceso (normalizado).
    Devuelve el Upload existente o None.
    """
    key = proceso_key or normalize_proceso_nro(proceso_nro)
    if not key:
        return None
    return (
        session.query(Upload)
        .filter(Upload.user_id == int(user_id), Upload.proceso_key == key)
        .order_by(Upload.created_at.desc())
        .first()
    )


# ----------------------------------------------------------------------
# Helpers de configuración / contraseña de RESET
# ----------------------------------------------------------------------
RESET_PASSWORD_KEY = "reset_password"

def get_config_value(session, key: str) -> str | None:
    """
    Devuelve el valor (string) asociado a una clave en AppConfig, o None si no existe.
    """
    row = session.query(AppConfig).filter(AppConfig.key == key).first()
    return row.value if row else None


def set_config_value(session, key: str, value: str | None) -> None:
    """
    Crea o actualiza una clave en AppConfig.
    """
    row = session.query(AppConfig).filter(AppConfig.key == key).first()
    if row is None:
        row = AppConfig(key=key, value=value or "")
        session.add(row)
    else:
        row.value = value or ""
    session.commit()


def get_reset_password(session) -> str | None:
    """
    Obtiene la contraseña de RESET (texto plano por ahora).
    """
    return get_config_value(session, RESET_PASSWORD_KEY)


def verify_reset_password(session, password: str) -> bool:
    """
    Verifica si la contraseña enviada coincide con la almacenada.
    Si no hay contraseña configurada, devuelve False (no permite reset).
    """
    if not password:
        return False
    stored = get_reset_password(session)
    if not stored:
        return False
    return stored == password


def set_reset_password(session, new_password: str) -> None:
    """
    Define/actualiza la contraseña de RESET.
    (Más adelante la vamos a usar desde una vista/endpoint para que el admin la cambie.)
    """
    set_config_value(session, RESET_PASSWORD_KEY, new_password or "")


# ----------------------------------------------------------------------
# Inicialización de la base y migraciones simples
# ----------------------------------------------------------------------
def _ensure_business_unit_column():
    """
    Autocorrección sencilla: si la columna 'business_unit' no existe en 'users',
    la agrega con ALTER TABLE. Funciona en SQLite y otros backends comunes.
    """
    try:
        insp = inspect(engine)
        cols = [c["name"] for c in insp.get_columns("users")]
        if "business_unit" not in cols:
            ddl = (
                "ALTER TABLE users ADD COLUMN business_unit VARCHAR(120)"
                if engine.url.get_backend_name() != "sqlite"
                else "ALTER TABLE users ADD COLUMN business_unit TEXT"
            )
            with engine.begin() as conn:
                conn.execute(text(ddl))
    except Exception:
        # Silencioso: si falla, no bloquea el arranque
        pass


def _migrate_old_units():
    """
    Convierte valores antiguos ('Hospitalario Publico/Privado') a la nueva etiqueta
    'Productos Hospitalarios'. No toca filas con NULL o ya migradas.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE users
                    SET business_unit = 'Productos Hospitalarios'
                    WHERE business_unit IN (
                        'Hospitalario Publico','Hospitalario Público','Hospitalario Privado'
                    )
                """)
            )
    except Exception:
        pass


def _ensure_upload_hint_columns():
    """
    Si la tabla 'uploads' fue creada antes de agregar los hints, añade:
      - platform_hint
      - buyer_hint
      - province_hint
    """
    try:
        insp = inspect(engine)
        cols = [c["name"] for c in insp.get_columns("uploads")]
        ddls = []
        if "platform_hint" not in cols:
            ddls.append("ALTER TABLE uploads ADD COLUMN platform_hint TEXT")
        if "buyer_hint" not in cols:
            ddls.append("ALTER TABLE uploads ADD COLUMN buyer_hint TEXT")
        if "province_hint" not in cols:
            ddls.append("ALTER TABLE uploads ADD COLUMN province_hint TEXT")
        # (por compatibilidad) índices útiles si la tabla ya existía
        with engine.begin() as conn:
            for ddl in ddls:
                conn.execute(text(ddl))
    except Exception:
        pass


def _ensure_upload_proceso_key_column():
    """
    Asegura la columna 'proceso_key' en uploads y la llena para los registros viejos.
    Esto es el soporte para evitar cargas duplicadas por número de proceso.
    """
    try:
        insp = inspect(engine)
        cols = [c["name"] for c in insp.get_columns("uploads")]
        need_alter = "proceso_key" not in cols
        if need_alter:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE uploads ADD COLUMN proceso_key TEXT"))
        # rellenar las filas que no lo tengan
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE uploads
                    SET proceso_key = UPPER(TRIM(proceso_nro))
                    WHERE (proceso_key IS NULL OR proceso_key = '')
                      AND proceso_nro IS NOT NULL
                """)
            )
            # índice para búsquedas rápidas por proceso
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_uploads_proceso_key ON uploads(proceso_key)")
            )
    except Exception:
        # no bloquear arranque si falla
        pass


def _ensure_unique_user_proceso_index():
    """
    Intenta crear un índice ÚNICO (user_id, proceso_key) para evitar duplicados
    del mismo proceso por el mismo usuario. Si falla (p.ej., ya hay duplicados),
    crea un índice normal como fallback para no romper el arranque.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_upload_user_proceso "
                "ON uploads(user_id, proceso_key)"
            ))
    except Exception:
        # Fallback: índice no-único
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_upload_user_proceso "
                    "ON uploads(user_id, proceso_key)"
                ))
        except Exception:
            pass


def _ensure_upload_indexes():
    """
    Crea índices si no existen (para tablas ya creadas).
    """
    try:
        idx_sql = [
            "CREATE INDEX IF NOT EXISTS idx_uploads_proceso ON uploads(proceso_nro)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_apertura ON uploads(apertura_fecha)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_cuenta ON uploads(cuenta_nro)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_platform ON uploads(platform_hint)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_buyer ON uploads(buyer_hint)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_province ON uploads(province_hint)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_created ON uploads(created_at)",
        ]
        with engine.begin() as conn:
            for sql in idx_sql:
                conn.execute(text(sql))
    except Exception:
        # No bloquear si algún backend no soporta IF NOT EXISTS
        pass


def _ensure_saved_views_indexes():
    """
    Índices útiles para búsquedas y default rápido.
    """
    try:
        idx_sql = [
            "CREATE INDEX IF NOT EXISTS idx_savedviews_user ON saved_views(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_savedviews_viewid ON saved_views(view_id)",
            "CREATE INDEX IF NOT EXISTS idx_savedviews_user_view_default ON saved_views(user_id, view_id, is_default)"
        ]
        with engine.begin() as conn:
            for sql in idx_sql:
                conn.execute(text(sql))
    except Exception:
        pass


def _ensure_comments_indexes():
    """
    Índices para tabla de comentarios (si ya existía sin índices).
    """
    try:
        idx_sql = [
            "CREATE INDEX IF NOT EXISTS idx_comments_upload ON comments(upload_id)",
            "CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(author_user_id)",
            "CREATE INDEX IF NOT EXISTS idx_comments_created ON comments(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_comments_resolved ON comments(is_resolved)",
            "CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_id)",
        ]
        with engine.begin() as conn:
            for sql in idx_sql:
                conn.execute(text(sql))
    except Exception:
        pass


def _ensure_email_notifications_indexes():
    """
    Garantiza índices/único para email_notifications en DBs ya existentes.
    """
    try:
        idx_sql = [
            # único lógico para evitar duplicados si la tabla ya existía sin constraint
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_email_notif ON email_notifications(upload_id, recipient, event)",
            "CREATE INDEX IF NOT EXISTS idx_email_notif_recipient ON email_notifications(recipient)",
            "CREATE INDEX IF NOT EXISTS idx_email_notif_upload ON email_notifications(upload_id)",
        ]
        with engine.begin() as conn:
            for sql in idx_sql:
                conn.execute(text(sql))
    except Exception:
        pass


def _ensure_upload_uploader_snapshot_columns():
    """
    Añade uploaded_by_name / uploaded_by_email si faltan (por compatibilidad
    con bases ya creadas). Tu script externo también las crea; esto es idempotente.
    """
    try:
        insp = inspect(engine)
        cols = [c["name"] for c in insp.get_columns("uploads")]
        ddls = []
        if "uploaded_by_name" not in cols:
            ddls.append("ALTER TABLE uploads ADD COLUMN uploaded_by_name TEXT")
        if "uploaded_by_email" not in cols:
            ddls.append("ALTER TABLE uploads ADD COLUMN uploaded_by_email TEXT")
        with engine.begin() as conn:
            for ddl in ddls:
                conn.execute(text(ddl))
    except Exception:
        # no bloquear arranque si falla
        pass


def _ensure_usage_indexes():
    """
    Índices básicos para tablas de métricas de uso.
    No es crítico si falla; solo mejora performance de consultas.
    """
    try:
        idx_sql = [
            "CREATE INDEX IF NOT EXISTS idx_usage_events_user_time ON usage_events(user_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_usage_events_action ON usage_events(action_type)",
            "CREATE INDEX IF NOT EXISTS idx_usage_sessions_user_start ON usage_sessions(user_id, start_time)",
        ]
        with engine.begin() as conn:
            for sql in idx_sql:
                conn.execute(text(sql))
    except Exception:
        pass


# --------- Bootstrap de admin desde variables de entorno ---------
def _bootstrap_admin_from_env():
    """
    Crea un usuario admin si ADMIN_EMAIL y ADMIN_PASSWORD están seteados
    y no existe ya un usuario con ese email.
    CONTRASEÑA: se guarda en password_hash tal cual.
    Si tu login usa hash, pasame el handler y adapto la asignación en 1 línea.
    """
    admin_email = (os.getenv("ADMIN_EMAIL") or "").strip().lower()
    admin_password = (os.getenv("ADMIN_PASSWORD") or "").strip()
    if not admin_email or not admin_password:
        return
    s = db_session()
    try:
        exists = s.query(User).filter(func.lower(User.email) == admin_email).first()
        if exists:
            return
        u = User(
            email=admin_email,
            name="Administrador",
            role="admin",
            unit_business=None,
            created_at=dt.datetime.utcnow(),
        )
        u.password_hash = admin_password  # <-- ajustar si tu login usa hash
        s.add(u)
        s.commit()
        logging.getLogger("bootstrap").info("[bootstrap] Admin creado: %s", admin_email)
    except Exception:
        s.rollback()
        logging.getLogger("bootstrap").exception("[bootstrap] Error creando admin")
    finally:
        s.close()


def _bootstrap_reset_password_from_env():
    """
    Inicializa la contraseña de RESET desde la variable RESET_PASSWORD
    si todavía no hay ninguna definida.
    Esto es opcional, pero útil para el primer arranque.
    """
    reset_pwd = (os.getenv("RESET_PASSWORD") or "").strip()
    if not reset_pwd:
        return
    s = db_session()
    try:
        current = get_reset_password(s)
        if current:
            return  # ya hay una configurada, no la pisamos
        set_reset_password(s, reset_pwd)
        logging.getLogger("bootstrap").info("[bootstrap] Reset password inicial configurada desde entorno")
    except Exception:
        s.rollback()
        logging.getLogger("bootstrap").exception("[bootstrap] Error configurando reset password inicial")
    finally:
        s.close()


def init_db():
    """Crea tablas si no existen y asegura columnas nuevas e índices básicos."""
    Base.metadata.create_all(bind=engine)
    _ensure_business_unit_column()
    _migrate_old_units()
    _ensure_upload_hint_columns()
    _ensure_upload_proceso_key_column()  # <-- clave normalizada
    _ensure_unique_user_proceso_index()  # <-- evita duplicados por usuario+proceso
    _ensure_upload_indexes()
    _ensure_saved_views_indexes()
    _ensure_comments_indexes()
    _ensure_email_notifications_indexes()
    _ensure_upload_uploader_snapshot_columns()  # <-- snapshot uploader (nuevo)
    _ensure_usage_indexes()  # <-- índices para métricas de uso
    _bootstrap_admin_from_env()  # <-- crea admin si tenés ADMIN_EMAIL/PASSWORD
    _bootstrap_reset_password_from_env()  # <-- configura contraseña RESET si hay RESET_PASSWORD


# ----------------------------------------------------------------------
# Helpers útiles para otros módulos (p.ej. email_service)
# ----------------------------------------------------------------------
def get_admin_or_auditor_emails(session) -> List[str]:
    """
    Devuelve lista de correos de usuarios con rol admin/administrator/administrador
    y auditor/visor/viewer (lectura total). Útil para notificar errores globales.
    """
    roles = tuple([r.lower() for r in (_ADMIN_ROLES | _AUDITOR_ROLES)])
    rows = session.execute(
        select(User.email).where(func.lower(User.role).in_(roles))
    ).scalars().all()
    # filtra nulos/duplicados con orden estable
    seen = set()
    out: List[str] = []
    for e in rows:
        if e and e not in seen:
            out.append(e)
            seen.add(e)
    return out


# ----------------------------------------------------------------------
# S.I.C Help Desk Models
# ----------------------------------------------------------------------
class Ticket(Base):
    __tablename__ = "tickets"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    
    title = Column(String, nullable=False)
    category = Column(String, default="consulta") # error, sugerencia, consulta, acceso
    priority = Column(String, default="media")    # baja, media, alta
    status = Column(String, default="abierto")    # abierto, pendiente, resuelto, cerrado
    
    created_at = Column(DateTime, default=dt.datetime.utcnow)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow)

    # Relationships
    user = relationship("User", backref="tickets")
    messages = relationship("TicketMessage", back_populates="ticket", cascade="all, delete-orphan", order_by="TicketMessage.created_at")

class TicketMessage(Base):
    __tablename__ = "ticket_messages"

    id = Column(Integer, primary_key=True, index=True)
    ticket_id = Column(Integer, ForeignKey("tickets.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    message = Column(Text, nullable=False)
    is_internal = Column(Boolean, default=False)  # Para notas internas de admins si se quisiera
    created_at = Column(DateTime, default=dt.datetime.utcnow)

    # Relationships
    ticket = relationship("Ticket", back_populates="messages")
    user = relationship("User")

