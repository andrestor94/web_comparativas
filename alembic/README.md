# Alembic — adopción controlada (Fase 3, NO activo en producción)

Este directorio es **preparación**. Alembic todavía **no gestiona** la base de
SIEM: las migraciones reales siguen siendo las funciones artesanales `ensure_*`
de `web_comparativas/migrations.py`, que **no se tocan ni se eliminan**.

## Reglas de oro

- ❌ Alembic **no** corre en el arranque de la app (no hay hook en `startup`).
- ❌ **No** ejecutar `alembic upgrade`/`downgrade` contra PostgreSQL producción.
- ❌ **No** hacer `stamp head` contra producción.
- ✅ `env.py` resuelve la URL desde `DATABASE_URL` (no hay credenciales en el repo).
- ✅ Contra una base remota (no SQLite), `env.py` **exige** `ALEMBIC_ALLOW_REMOTE=1`.
- ✅ `include_object` impide que autogenerate proponga **DROP** de tablas que no
  están en el ORM (forecast_* de datos base, legado, etc.).
- ✅ `render_as_batch=True` en SQLite → migraciones compatibles con ALTER limitado.

## Requisito

Alembic no está en `requirements.txt` de producción (a propósito). Instalalo solo
en tu entorno local:

```bash
pip install -r requirements-dev.txt   # incluye alembic
```

## Procedimiento de baseline (SOLO LOCAL, documentado)

El objetivo del baseline es que Alembic "adopte" el esquema actual **sin recrear
nada**. Pasos sugeridos, todos en local contra SQLite:

1. Generar el baseline por autogenerate y **revisarlo a mano**:
   ```bash
   alembic revision --autogenerate -m "baseline esquema actual"
   ```
   Revisar el archivo en `alembic/versions/`: debe reflejar el esquema actual.
   `include_object` ya evita DROPs de tablas no modeladas (forecast_*, legado).

2. Marcar la base local como "ya en baseline" **sin aplicar DDL**:
   ```bash
   alembic stamp head
   ```
   (`stamp` solo escribe la versión en `alembic_version`; no altera tablas.)

3. Para producción: **no** correr esto todavía. Cuando haya backup + autorización,
   el plan es `alembic stamp <baseline>` (no `upgrade`) para adoptar el esquema
   existente sin recrearlo. Ver `docs/plan_migracion_base_datos_siem.md`.

## Separar DDL de backfills

Las migraciones de Alembic deben contener **solo DDL** (estructura). Los backfills
pesados (rellenar columnas, reconstruir summary, respaldar BLOBs) siguen como
tareas/scripts aparte, nunca dentro de una migración que bloquee el deploy.
