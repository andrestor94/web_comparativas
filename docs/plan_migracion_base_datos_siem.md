# Plan de migración segura de la base de datos — SIEM

> **Fase 3 — Preparación de arquitectura y migraciones seguras.**
> Documento **práctico**: pensado para ejecutarse paso a paso, con backup y rollback.
> Nada de este plan se ejecuta contra producción sin autorización explícita + backup.
>
> Complementa: [`auditoria_base_datos_siem.md`](auditoria_base_datos_siem.md),
> [`forecast_tables_orm_proposal.md`](forecast_tables_orm_proposal.md),
> [`plan_optimizacion_consultas_siem.md`](plan_optimizacion_consultas_siem.md),
> [`backup_postgres_render_checklist.md`](backup_postgres_render_checklist.md).

---

## 1. Objetivo de la reestructuración

Ordenar la capa de datos de SIEM para que sea **mantenible, performante y versionada**,
sin perder datos ni romper producción. En concreto:

- Que cada tabla tenga función clara y esté **modelada en el ORM** (incluidas `forecast_*`).
- Que las migraciones tengan **versionado formal y rollback** (Alembic), conviviendo con
  las `ensure_*` actuales hasta poder retirarlas con seguridad.
- Que las consultas analíticas pesadas se optimicen **sin cambiar resultados**.
- Que SQLite local y PostgreSQL producción mantengan **estructuras compatibles**, con
  PostgreSQL como **fuente de verdad**.

> **No** es objetivo de la Fase 3 ejecutar ninguno de estos cambios en producción.
> La Fase 3 deja todo **preparado y revisable**.

---

## 2. Riesgos principales

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Pérdida/corrupción de datos en producción | 🔴 Crítico | Backup obligatorio + rollback probado antes de cualquier DDL. |
| Script de ingesta Forecast con `if_exists="replace"` | 🔴 Crítico | **DROPea y recrea** `forecast_main/imp_hist/fact_2026/product_labs`. Nunca correrlo "para probar". Ver §-Forecast. |
| Autogenerate de Alembic proponiendo DROP de tablas no modeladas | 🟠 Alto | `include_object` en `env.py` ya filtra tablas fuera del ORM. Revisar SIEMPRE el baseline a mano. |
| Divergencia SQLite local ↔ PostgreSQL prod | 🟠 Alto | `render_as_batch` + tipos compatibles + diagnóstico comparativo. |
| Migración bloqueando el deploy de Render | 🟠 Medio | DDL separado de backfills; nada pesado en startup. |
| `create_all` materializando tablas fantasma | 🟡 Bajo | Tratar en fase de deprecación, no ahora. |

---

## 3. Estrategia de backup (antes de CUALQUIER cambio estructural)

Detalle paso a paso en [`backup_postgres_render_checklist.md`](backup_postgres_render_checklist.md).
Resumen:

1. **Snapshot** del PostgreSQL de Render (panel de Render) **y** `pg_dump` lógico a archivo.
2. Guardar **conteos por tabla** (correr `scripts/db_diagnostics.py` contra prod, con
   `--confirm-remote --estimate`, **solo con tu autorización**) → `docs/db_diagnostics_prod.md`.
3. Verificar que el dump existe, pesa lo razonable y **restaura** en una base de prueba.
4. Recién entonces se habilita ejecutar DDL.

---

## 4. Estrategia de rollback

- **Antes:** snapshot Render + `pg_dump`.
- **Si una migración falla:**
  1. No improvisar sobre la base afectada.
  2. Restaurar el snapshot de Render (o `pg_restore` del dump) en la misma instancia o en una nueva.
  3. Repuntar `DATABASE_URL` si se restauró en instancia nueva.
- **Migraciones reversibles:** cada migración Alembic debe tener `downgrade()` real.
  Las que no se puedan revertir (ej. data backfill destructivo) **no entran como migración**:
  van como script aparte con su propio backup.
- **Regla:** ninguna operación estructural sin un camino de vuelta probado.

---

## 5. Baseline de la base actual (adoptar el esquema sin recrearlo)

El baseline hace que Alembic "reconozca" el esquema existente **sin DDL destructivo**:

1. **Local (SQLite):**
   ```bash
   pip install -r requirements-dev.txt
   alembic revision --autogenerate -m "baseline esquema actual"   # REVISAR a mano
   alembic stamp head                                              # solo marca versión
   ```
   `include_object` (en `alembic/env.py`) evita que el autogenerate proponga DROP de
   `forecast_*` de datos base o tablas legado.

2. **Producción (PostgreSQL) — diferido:** con backup + autorización, adoptar el esquema
   con `alembic stamp <baseline>` (NO `upgrade`), para que Alembic tome el esquema real
   como punto de partida sin tocar tablas.

---

## 6. Introducir Alembic sin romper el esquema existente

- Archivos ya preparados: `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako`,
  `alembic/README.md`, `alembic/versions/` (vacío). Ver [`alembic/README.md`](../alembic/README.md).
- **Conviven** con `ensure_*`: Alembic NO las reemplaza todavía y NO se ejecuta en startup.
- `env.py` resuelve `DATABASE_URL` (sin credenciales en repo) y **bloquea** ejecuciones
  remotas salvo `ALEMBIC_ALLOW_REMOTE=1`.
- Alembic NO se agrega a `requirements.txt` de producción (solo `requirements-dev.txt`),
  para no alterar el build de Render ni darle forma de ejecutar algo inesperado.

### Retiro de `ensure_*` (futuro, no ahora)
Una vez que Alembic gestione el esquema en prod (baseline aplicado), las `ensure_*` se
retiran **de a una**, confirmando que la migración equivalente ya está en Alembic.

---

## 7. Manejo SQLite local ↔ PostgreSQL producción

- **Fuente de verdad: PostgreSQL.** SQLite se alinea, no al revés.
- Migraciones con `render_as_batch=True` → compatibles con el ALTER limitado de SQLite.
- Tipos a vigilar (ver advertencias del diagnóstico): `JSON` (TEXT en SQLite, idealmente
  `JSONB` en PG), `BLOB`/`BYTEA`, `DateTime(timezone=True)` vs naive, `Boolean` defaults.
- Las `forecast_*` de datos base **no existen en SQLite local** (son CSV/parquet): los
  modelos deben tolerar su ausencia en local.

---

## 8. Qué se puede hacer primero SIN alterar datos

(Todo esto es seguro y no requiere backup de prod.)

- ✅ Documentación (esta fase).
- ✅ `scripts/db_diagnostics.py` (read-only).
- ✅ Scaffolding de Alembic inerte.
- ✅ Modelos `forecast_*` propuestos en módulo inerte (`forecast_models_proposed.py`).
- ✅ Optimizaciones de consulta **clase A** (mismas filas, sin cambio de resultado) —
  ver [`plan_optimizacion_consultas_siem.md`](plan_optimizacion_consultas_siem.md).
- ✅ Índices nuevos validados con `EXPLAIN` (no alteran datos), aplicados primero en local.

---

## 9. Qué requiere migración (con backup + autorización)

- Integrar `forecast_*` al ORM real (read-only) + baseline Alembic.
- Separar BLOBs de `uploads` a `upload_blobs` (migración de datos auditada).
- Tablas resumen/materializadas para Mercado Público/Privado.
- Índices en producción sobre tablas grandes (`CREATE INDEX CONCURRENTLY`).
- Retención/archivado de `usage_events`.

---

## 10. Qué queda PROHIBIDO hasta tener backup

- ❌ Cualquier `DROP` / `DELETE` / `TRUNCATE`.
- ❌ Re-ejecutar el script de ingesta Forecast (`if_exists="replace"` = destructivo).
- ❌ `alembic upgrade`/`downgrade`/`stamp` contra producción.
- ❌ Renombrar tablas (fantasma o no).
- ❌ Separación real de `upload_blobs`.
- ❌ Modificar esquema de `forecast_*`.
- ❌ Deploy con migraciones activas.

---

## 11. Orden recomendado de implementación

1. **Fase 3 (esta):** docs + diagnóstico + Alembic inerte + modelos propuestos. *(sin prod)*
2. **Backup + diagnóstico de producción** (con tu OK): snapshot + `pg_dump` + `db_diagnostics_prod.md`.
3. **Baseline Alembic en local**, revisado a mano.
4. **Optimizaciones clase A** (seguras) en una rama, con QA.
5. **Índices** validados con `EXPLAIN` → primero local, luego prod (con backup).
6. **Integrar `forecast_*` al ORM** (read-only) + `stamp` baseline en prod (con backup).
7. **Cálculos a SQL** (clase B) por endpoint, con verificación de resultados idénticos.
8. **Tablas resumen / `upload_blobs` / retención** (estructural, con migración + backup).
9. **Deprecación controlada** de tablas fantasma (rename `zz_deprecated_*`, nunca DROP directo).

> Cada paso = rama separada + commit claro + QA + tu autorización antes de tocar prod.

---

## 12. Fase 4 — Estado del diagnóstico de producción

> Rama: `chore/db-production-diagnostics-phase-4` (apilada sobre la de Fase 3).

### Qué quedó preparado (sin tocar producción)
- `scripts/db_diagnostics.py` mejorado: apéndice de detalle por tabla
  (columnas/tipos/nullable, PK, FK con destino, índices con columnas/unicidad) +
  export `--json` de snapshot estructural.
- `scripts/db_compare.py`: compara dos snapshots JSON (local vs prod) **offline**.
- Snapshot **local** generado: `docs/db_snapshot_local.json` (52 tablas).
- `docs/db_diagnostics_report_production.md`: placeholder + runbook credential-safe.

### Estado de la ejecución contra producción
- ⏳ **PENDIENTE.** El asistente no tiene credenciales de prod y, por política, no las
  pide por chat ni las ejecuta. El backup y el diagnóstico read-only los corre el
  usuario en su máquina (ver runbook en `db_diagnostics_report_production.md` y
  `backup_postgres_render_checklist.md`).
- Hasta tener `docs/db_snapshot_prod.json`, **no hay foto real de producción**: el
  mapa real de `forecast_*` y la comparación local↔prod quedan a la espera de ese dato.

### Hallazgos confirmados (de código, ya conocidos)
- **Ingesta Forecast destructiva**: `migrate_forecast_csv_to_postgres.py` usa
  `if_exists="replace"` sobre `forecast_main` (44), `forecast_valorizado` (139),
  `forecast_imp_hist` (165), `forecast_fact_2026` (198), `forecast_product_labs` (232).
  Re-ejecutarlo **DROPea y recrea** esas tablas en prod. **Prohibido correrlo** en
  esta y las próximas fases. Propuesta (NO implementar aún): guarda por ambiente
  (ej. exigir `ALLOW_PROD_REPLACE=1` o bloquear si el host es de Render) — documentado,
  sin tocar el flujo productivo todavía.

### Riesgos nuevos a validar con el diagnóstico real
- Tipos exactos de `forecast_*` en PG (¿`FLOAT8` vs `NUMERIC` en montos? ¿`TIMESTAMP` tz?).
- Existencia/ausencia de PK/constraints (por `to_sql index=False`, probablemente sin PK).
- Diferencias de columnas entre lo inferido del código y la tabla real.
- Tablas en prod que no estén en local (¿alguna `forecast_*` extra o auxiliar no vista?).

### Qué se puede hacer en Fase 5 (después de tener la foto real)
- Validar/ajustar `forecast_models_proposed.py` contra el esquema real.
- Baseline de Alembic en local (revisado a mano).
- Optimizaciones clase A + índices validados con `EXPLAIN` (local primero).

### Qué NO se debe tocar todavía
- Nada de producción (DDL/DML), Alembic contra prod, ingesta Forecast, `upload_blobs`,
  índices en prod, renombrados, deprecaciones, purga de logs. Todo requiere backup + OK.
