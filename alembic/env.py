"""
Alembic environment — PREPARACIÓN CONTROLADA (Fase 3).

Diseño de seguridad:
  - Resuelve la URL desde DATABASE_URL (no desde alembic.ini) para no versionar
    credenciales. Si no hay DATABASE_URL, usa el SQLite local de la app.
  - Si la URL apunta a una base NO-SQLite (posible producción), EXIGE la variable
    de entorno ALEMBIC_ALLOW_REMOTE=1. Así Render / un entorno productivo no
    ejecuta una migración por accidente.
  - `include_object` evita que autogenerate proponga DROPear tablas que existen
    en la base pero no están en el ORM (p.ej. forecast_main/valorizado y otras
    tablas de datos base o legado). Nunca generamos destrucción de lo no modelado.
  - render_as_batch=True para que las migraciones sean compatibles con SQLite
    (que no soporta ALTER TABLE completo).

Este archivo NO se ejecuta en el arranque de la app: solo corre cuando se invoca
el comando `alembic` manualmente.
"""
from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# --- Config de Alembic (alembic.ini) ---
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Asegura que el paquete de la app sea importable.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Metadata objetivo: el Base del ORM de la app.
# Importar models registra TODOS los modelos en Base.metadata (necesario para
# autogenerate). No dispara migraciones ni create_all por sí mismo.
from web_comparativas.models import Base  # noqa: E402

# Importa el submódulo de dimensionamiento para que sus tablas también queden
# registradas en el mismo Base.metadata.
try:  # pragma: no cover - import defensivo
    import web_comparativas.dimensionamiento.models  # noqa: F401,E402
except Exception:
    pass

target_metadata = Base.metadata


def _resolve_url() -> str:
    raw = (os.getenv("DATABASE_URL") or "").strip()
    if raw:
        return raw.replace("postgres://", "postgresql://")
    local_db = _REPO_ROOT / "web_comparativas" / "app.db"
    return f"sqlite:///{local_db.as_posix()}"


def _guard_remote(url: str) -> None:
    is_sqlite = url.startswith("sqlite")
    if not is_sqlite and os.getenv("ALEMBIC_ALLOW_REMOTE") != "1":
        raise SystemExit(
            "\n[ALEMBIC][BLOQUEADO] La URL apunta a una base NO-SQLite (posible producción).\n"
            "  Para correr Alembic contra una base remota, definí ALEMBIC_ALLOW_REMOTE=1\n"
            "  SOLO si tenés backup vigente y autorización explícita.\n"
        )


def _include_object(obj, name, type_, reflected, compare_to):
    """
    Evita que autogenerate proponga DROP de tablas/objetos que existen en la base
    pero NO están en el ORM (forecast_* de datos base, legado, etc.).
    """
    if type_ == "table" and name not in target_metadata.tables:
        return False
    return True


def run_migrations_offline() -> None:
    url = _resolve_url()
    _guard_remote(url)
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=url.startswith("sqlite"),
        compare_type=True,
        include_object=_include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = _resolve_url()
    _guard_remote(url)
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = url
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=url.startswith("sqlite"),
            compare_type=True,
            include_object=_include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
