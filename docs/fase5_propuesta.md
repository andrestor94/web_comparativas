# Fase 5 — Propuesta concreta (optimización segura, sin cambios destructivos)

> Basada en los hallazgos reales de producción (`hallazgos_produccion_fase4.md`).
> **Acciones concretas**, priorizadas. Regla transversal: sin DROP/ALTER/DELETE,
> sin migraciones en prod, sin tocar índices todavía, sin ingesta Forecast.

---

## 0. Rama y forma de trabajo

- **Rama:** `chore/db-phase-5-safe-optimizations` (desde la rama actual de Fase 4, apilada).
- Cada bloque (A–E) = sub-rama o commit aislado + QA + tu autorización antes de prod.
- Lo único que se aplica a prod en Fase 5 son **cambios de código** (queries/modelos
  read-only). Índices, deprecaciones y migraciones quedan **propuestos**, esperando backup.

---

## A. Forecast

### A1 — Modelar la familia VIEJA como read-only (CÓDIGO, seguro)
- Integrar `forecast_main, forecast_valorizado, forecast_imp_hist, forecast_fact_2026,
  forecast_product_labs` al ORM con tipos **reales del snapshot** (TEXT/DOUBLE), como
  modelos **read-only** (la app nunca escribe; la carga sigue por los scripts).
- PK: como no tienen PK real, mapear con `__mapper_args__` sobre `ctid` (Postgres) o una
  PK lógica solo-ORM; **no** crear PK en la tabla.
- Excluir del `create_all` local (no existen en SQLite). 
- Actualizar `forecast_models_proposed.py` → modelos definitivos validados contra prod.

### A2 — NO tocar la familia NUEVA (validación humana)
- `forecast_base, forecast_dataset_base, forecast_articulo, forecast_cliente,
  forecast_negocio`: **no modelar, no migrar, no borrar**. Primero confirmar su rol.

### A3 — Consultas pesadas sobre forecast (CÓDIGO, clase A/B)
- Revisar en `forecast_service.py` las queries con **cientos de OR**: reemplazar
  `WHERE col = v1 OR col = v2 OR ...` por `WHERE col IN (...)` o por un `JOIN`/`VALUES`
  temporal (mismo resultado, plan mucho mejor). Es **clase A** (resultado idéntico).
- `forecast_valorizado` (702k): empujar agregaciones (`SUM(monto_yhat) GROUP BY`) a SQL
  en vez de `.iterrows()` en pandas (treemap/chart).

### A4 — Índices duplicados forecast (PROPUESTA, NO ejecutar)
- Documentados como redundantes (`idx_fc_*` == `ix_fc_*`). **No** se eliminan en Fase 5.
- Plan: tras backup, `DROP INDEX CONCURRENTLY` de los duplicados exactos (libera espacio
  y acelera escrituras). Requiere backup + autorización.

---

## B. Dimensionamiento

### B1 — Confirmar uso real de summary (CÓDIGO/diagnóstico)
- El logging de Fase 2 (`_log_summary_fallback`) ya permite ver si algún endpoint cae a
  `dimensionamiento_records`. **Acción:** revisar logs de prod y listar endpoints que
  todavía escanean records cuando deberían usar summary.

### B2 — Bloat de índices (PROPUESTA, NO ejecutar)
- `records` 27 índices, `summary` 26. Muchos se solapan: `ix_dim_records_<x>_norm`
  (funcionales) vs `ix_dimensionamiento_records_<x>` (directos) vs compuestos `_date`.
- **Acción Fase 5:** auditar con `EXPLAIN ANALYZE` qué índices usan realmente los
  endpoints, y **proponer** una lista de índices candidatos a retirar (con backup).
  **No** se elimina nada en Fase 5.

### B3 — Granularidad del summary (ANÁLISIS)
- 259.702 filas de summary para 317.236 de records → casi no comprime. Causa probable:
  la clave de agregación incluye `cliente_nombre_homologado` (alta cardinalidad).
- **Acción:** documentar si conviene un segundo nivel de summary más agregado para los
  KPIs que no necesitan cliente. **Propuesta**, requiere validación + migración futura.

---

## C. Mercado Público

- `comparativa_rows`: 86k filas, 42 MB. Revisar endpoints del Reporte de Perfiles
  (`mercado_publico_perfiles_router.py`).
- **C1 (clase B):** mover medianas/percentiles de Python a SQL
  (`percentile_cont` en Postgres) con fallback SQLite, verificando resultado idéntico.
- **C2 (clase A):** `LIMIT` defensivos ya iniciados en Fase 2; completar los `DISTINCT`
  e históricos sin límite de `articulos_por_proveedor`.
- Ver detalle por endpoint en `plan_optimizacion_consultas_siem.md`.

---

## D. Tablas candidatas a revisión (clasificación, **sin borrar**)

| Tabla | Clasificación | Acción propuesta |
|---|---|---|
| `chat_channels/members/messages` | **Legado probable** (feature nunca terminada) | Validar con humano → futuro rename `zz_deprecated_*` |
| `revision_sessions` | **Feature pausada** (curación IA) | Validar si se retomará |
| `runs`, `normalized_files`, `dashboards` | **Legado probable** (pipeline viejo) | Confirmar 0 escritura actual |
| `password_reset_requests` | **Todavía usada** (flujo reset) | Mantener |
| `app_users`, `clients`, `products`, `orders`, `order_items` | **Legado ajeno** (scaffold inicial, fuera del dominio) | **Validación humana**: ¿se pueden deprecar? |
| `forecast_base/dataset_base/articulo/cliente/negocio` | **Requiere validación** (sin uso visible) | No tocar hasta confirmar rol |

> **Estrategia de deprecación futura (sin DROP):** (1) confirmar 0 uso en prod por logs +
> código, (2) backup, (3) `ALTER TABLE ... RENAME TO zz_deprecated_<tabla>` (reversible),
> (4) observar N semanas, (5) recién entonces evaluar DROP con autorización explícita.
> Nada de esto en Fase 5.

---

## E. Migraciones / Alembic

- **No** ejecutar Alembic contra prod en Fase 5.
- Preparar **solo el baseline local** (ver `alembic/README.md`): `alembic revision
  --autogenerate` revisado a mano + `alembic stamp head` **en local**.
- `include_object` ya evita que el autogenerate proponga DROP de las `forecast_*` no
  modeladas ni de las legado.
- **Espera backup completo** antes de cualquier `stamp`/`upgrade` en producción.

---

## Entregables Fase 5 → clasificación pedida

### 5. Qué se puede implementar YA solo en código (sin tocar datos/estructura)
- A1 (modelos read-only forecast viejo), A3 (OR→IN, agregaciones SQL).
- B1 (revisar logs/uso de summary).
- C1/C2 (medianas a SQL, LIMIT defensivos).
- E baseline **local**.

### 6. Qué requiere BACKUP COMPLETO antes
- A4 (drop de índices duplicados forecast).
- B2 (retiro de índices redundantes en dimensionamiento).
- Cualquier rename de tablas legado (deprecación).
- `alembic stamp` baseline en prod.
- Retención/archivado de `usage_events`.

### 7. Qué requiere VALIDACIÓN HUMANA
- Rol de la familia forecast **nueva** (¿WIP, reemplazo, legado?).
- Si `app_users/clients/products/orders/order_items` se pueden deprecar.
- Si `chat_*` y `revision_sessions` son features canceladas.
- Confirmar que `runs/normalized_files/dashboards` ya no se escriben.

### 8. Orden de trabajo recomendado (Fase 5)
1. A1 modelos read-only forecast viejo (código) + validar tipos contra snapshot.
2. C2 + A3 optimizaciones clase A (LIMIT, OR→IN). QA de resultado idéntico.
3. B1 revisar uso real de summary por logs.
4. C1 medianas a SQL (clase B) con verificación.
5. E baseline Alembic **local**.
6. **Propuestas** A4/B2 (índices) y deprecaciones → quedan listas, esperando backup + OK.

> Fase 5 entrega **mejoras de código aplicables** + un set de cambios estructurales
> **listos pero no ejecutados**, que se aplican en Fase 6 con backup y rollback.
