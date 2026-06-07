# Tablas `forecast_*`: mapa y propuesta de integración al ORM

> **Fase 3 — preparación.** Este documento mapea las tablas `forecast_*` de **datos
> base** que existen en PostgreSQL producción pero **no** están en el ORM, y propone
> cómo integrarlas **sin modificarlas**. No se ejecuta ningún cambio sobre Forecast.
>
> Modelos propuestos (inertes): [`web_comparativas/forecast_models_proposed.py`](../web_comparativas/forecast_models_proposed.py).

---

## 1. Contexto

El módulo Forecast tiene dos clases de tablas:

- **Overrides / manuales / aprobaciones** — `forecast_user_overrides`, `forecast_manual_clients`,
  `forecast_manual_entries`, `forecast_change_requests`. **Ya están en el ORM** (`models.py`). ✅
- **Datos base** — `forecast_main`, `forecast_valorizado`, `forecast_imp_hist`,
  `forecast_fact_2026`, `forecast_product_labs`. **NO están en el ORM.** ⛔ Objetivo de esta sección.

En **local** los datos base son CSV/Parquet en `web_comparativas/data/forecast_data/`.
En **producción (Render)** son **tablas PostgreSQL** creadas y cargadas por scripts de ingesta,
no por el ORM. Por eso `models.py` no las conoce y `scripts/db_diagnostics.py` no las ve en
SQLite local.

### ⚠️ Riesgo crítico de la ingesta
Los scripts cargan con **`if_exists="replace"`** → **DROPea y recrea** la tabla en cada corrida:

| Tabla | Script | Línea | Modo |
|---|---|---|---|
| `forecast_main` | `migrate_forecast_csv_to_postgres.py` | 44 | `replace` (destructivo) |
| `forecast_valorizado` | `migrate_forecast_csv_to_postgres.py` / `reload_valorizado.py` | 139 | `replace` 1er chunk + `append` |
| `forecast_imp_hist` | `migrate_forecast_csv_to_postgres.py` | 165 | `replace` (destructivo) |
| `forecast_fact_2026` | `migrate_forecast_csv_to_postgres.py` / `migrate_local_to_render.py` | 198 | `replace` (destructivo) |
| `forecast_product_labs` | `migrate_forecast_csv_to_postgres.py` | 232 | `replace` (destructivo) |

> **Nunca** correr estos scripts "para probar" contra producción: borran y recrean la tabla.
> `to_sql(..., index=False)` → la tabla productiva **no tiene PK**; los índices se crean aparte
> con `CREATE INDEX IF NOT EXISTS` después del `to_sql`.

---

## 2. Mapa de tablas (esquema inferido del código)

> Tipos **inferidos** desde los DataFrames/CSV y queries. Pendiente validar contra el
> esquema real de PostgreSQL (ver §4 Incógnitas).

### `forecast_main` — series base
Origen: `forecast_base_consolidado.csv`. Índices reales: `perfil, neg, subneg, codigo_serie`.
Columnas: `perfil, neg, subneg, codigo_serie, fecha(ts), tipo('hist'/fcst), yhat, yhat_lower,
yhat_upper, monto_yhat, y, precio, descripcion, familia`.
Queries (forecast_service.py): `MAX(fecha) WHERE tipo='hist'` (296); `AVG(precio) GROUP BY codigo_serie`
(319); `DISTINCT codigo_serie/neg/perfil/subneg` (2726, 3271, 4970, 4982, 4994, 3783).

### `forecast_valorizado` — ~702k filas (la pesada)
Origen: `fact_forecast_valorizado.parquet`. Validación de negocio: `SUM(monto_yhat)` ≈ referencia.
Índices reales: `fecha, perfil, codigo_serie, cliente_id`.
Columnas: `codigo_serie, fecha(ts), monto_yhat, monto_li, monto_ls, perfil, cliente_id,
fantasia, nombre_grupo, neg, subneg, descripcion`.
Queries: `DISTINCT fecha/codigo_serie/fantasia` (402, 429, 1344, 1771); agregaciones
`SUM(monto_yhat) GROUP BY ...` (1049, 1063-1073, 4364, 4399, 4407).

### `forecast_imp_hist` — histórico real
Origen: `importe_historico.csv`. Índice real: `perfil`.
Columnas: `perfil, codigo_serie, fecha(ts), imp_hist, tipo`.
Queries: `GROUP BY fecha` (3510); `SUM(imp_hist) WHERE año=2025` (reload_valorizado.py:266).

### `forecast_fact_2026` — facturación real Ene-Abr 2026
Origen: `facturacion_real_2026_sin_neg2.csv`. Índices reales: `tipocli, cliente_id, fecha`.
Columnas: `fecha(ts), cliente_id, codigo_serie, imp_hist, perfil(interno), tipocli(perfil comercial), tipo`.
**Ojo:** `perfil` es un código interno (ej. "9 - 1"); el perfil comercial real es **`tipocli`**.

### `forecast_product_labs` — mapping series→laboratorios
Origen: derivado de `dataset_base.csv` + `Articulos 1.csv`. Índice real: `codigo_serie`.
Columnas: `codigo_serie, laboratorios (JSON serializado como texto)`.
Queries: `SELECT codigo_serie, laboratorios` (2758); join con valorizado para filtrar por lab (4237).

---

## 3. Propuesta de integración al ORM (sin romper producción)

**Estrategia recomendada: modelos read-only a nivel de aplicación.**

1. Definir los 5 modelos en el ORM real (`Base`), **pero**:
   - La app **nunca** hace `INSERT/UPDATE/DELETE` sobre ellos. La carga sigue por los scripts
     de ingesta existentes (sin cambios). Se documenta la convención "read-only".
   - Como la tabla productiva no tiene PK, mapear con una PK sintética sobre `ctid`/rowid o
     declarar `__mapper_args__` con clave compuesta lógica **solo para el ORM** (no DDL).
2. **No** dejar que `create_all` las recree:
   - En SQLite local quedarían vacías (los datos base son CSV/parquet). Opción A: excluirlas del
     `create_all` local. Opción B: aceptarlas vacías. Recomendado: **excluirlas en local**.
3. **Alembic:** registrar su esquema por `stamp` del baseline (NO recrearlas). `include_object`
   ya evita que autogenerate proponga DROP de estas tablas.
4. Beneficio: el esquema queda **versionado y visible**, las queries pueden tipar columnas, y se
   elimina el punto ciego — sin tocar un solo byte de datos.

> El módulo [`forecast_models_proposed.py`](../web_comparativas/forecast_models_proposed.py) ya
> tiene estos modelos con un `Base` **propio e independiente** (inerte): sirve de borrador para
> revisar nombres/tipos antes de integrarlos al `Base` real.

---

## 4. Incógnitas — requieren inspeccionar PostgreSQL producción

Resolver corriendo `scripts/db_diagnostics.py --confirm-remote` contra prod (con tu autorización):

1. **Tipos exactos**: ¿`TEXT`/`VARCHAR`/`NUMERIC`/`FLOAT8`? ¿`fecha` es `TIMESTAMP` con/ sin tz?
2. **Precisión numérica** de `monto_yhat` (¿`FLOAT8` alcanza para la validación monetaria?).
3. **Constraints/Defaults**: ¿`NOT NULL`, `CHECK`, `DEFAULT`? (probablemente ninguno, por `to_sql`).
4. **Tamaño real** de cada tabla y % de NULLs por columna.
5. **¿Particiones o vistas materializadas** sobre estas tablas? (esperable: no).
6. **Sincronía** parquet/CSV local ↔ tabla Render: ¿hay proceso de reconciliación o es manual?
7. **Columnas extra** no vistas en el código (la tabla productiva podría tener más columnas que el
   DataFrame si se cargó en versiones distintas).

> Hasta resolver esto, los modelos propuestos son **borrador**. No integrar al `Base` real
> sin validar el esquema productivo.

---

## 5. ACTUALIZACIÓN FASE 4 — esquema REAL de producción ✅

El diagnóstico real (`hallazgos_produccion_fase4.md`) confirmó y **amplió** el mapa:
existen **10** tablas `forecast_*` de datos base (no 5), en **dos familias**.

### 5.1 Familia VIEJA — usada por la app (`forecast_service.py`), SIN PK, columnas TEXT
- `forecast_main` (277.452, 27 cols TEXT, sin PK): `periodo, codigo_serie,
  nivel_agregacion, perfil, neg, subneg, familia_x, tipo, y, yhat, li, ls, submodelo, ...,
  fecha:TIMESTAMP, precio:DOUBLE`.
- `forecast_valorizado` (702.436, 18 cols): `monto_yhat/li/ls:DOUBLE`,
  `yhat_cliente/li_cliente/ls_cliente:BIGINT`, `fantasia, nombre_grupo, neg, subneg`.
- `forecast_imp_hist` (44.861): `periodo, codigo_serie, perfil, imp_hist:DOUBLE, tipo, fecha`.
- `forecast_fact_2026` (206.246): `fecha, codigo_serie, perfil, cliente_id, ..., tipocli`.
- `forecast_product_labs` (2.914): `codigo_serie, laboratorios:TEXT`.
- ⚠️ **Índices duplicados** (`idx_fc_*` y `ix_fc_*` sobre las mismas columnas).
- Estos son los que `forecast_models_proposed.py` debe modelar (ajustar tipos a los
  **TEXT/DOUBLE reales** del snapshot; las inferencias de tipos de Fase 3 quedan corregidas).

### 5.2 Familia NUEVA tipada — SIN referencias en el código ⚠️ (validación humana)
- `forecast_base` (966.072, PK id, VARCHAR/INTEGER/DOUBLE, incluye `etiqueta_upper`).
- `forecast_dataset_base` (221.424, PK id, `qty_mes:DOUBLE`).
- `forecast_articulo` (121.701, PK id) · `forecast_cliente` (40.092, PK id) ·
  `forecast_negocio` (145, PK id).
- **0 referencias** en `web_comparativas/*.py`. **No modelar ni tocar** hasta validar su rol.

> Corrección importante: Fase 3 asumía tipos numéricos para `forecast_main/valorizado`;
> el snapshot real muestra que **muchas columnas son TEXT**. Al integrarlos al ORM,
> declarar los tipos reales y castear en las queries donde haga falta.
