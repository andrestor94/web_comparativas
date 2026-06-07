# Reporte de diagnóstico de base de datos — SIEM

> Generado por `scripts/db_diagnostics.py` (solo lectura).
> Fecha (UTC): 2026-06-07T00:46:05
> Motor: **postgresql / db=web_comparativas_db**  ·  conteo: **estimado**

Este reporte es estructural. No contiene filas de datos ni credenciales.

## Resumen

- Total de tablas: **66**
- Tablas `forecast_*` totales: **14** (en ORM: 4, NO en ORM / datos base: 10)
- Tablas vacías (0 filas): **0**
- Tablas grandes (>= 100,000 filas): **8**
- Tablas candidatas a revisión (legado/fantasma): **8**

## Tablas (conteo, tamaño, PK, FK, índices)

| Tabla | Filas | Tamaño | Cols | PK | FK | Índices | Notas |
|---|---:|---:|---:|---|---:|---:|---|
| `app_config` | -1 | 32.0 KB | 5 | id | 0 | 3 |  |
| `app_users` | -1 | 8.0 KB | 3 | username | 0 | 0 |  |
| `chat_channels` | -1 | 16.0 KB | 5 | id | 0 | 1 | revisión |
| `chat_members` | -1 | 32.0 KB | 5 | id | 2 | 3 | revisión |
| `chat_messages` | -1 | 40.0 KB | 6 | id | 2 | 3 | revisión |
| `clients` | -1 | 32.0 KB | 12 | id | 0 | 2 |  |
| `comments` | -1 | 112.0 KB | 10 | id | 3 | 12 |  |
| `comparativa_rows` | 86,081 | 42.1 MB | 23 | id | 1 | 8 |  |
| `dashboards` | -1 | 32.0 KB | 5 | id | 1 | 2 | revisión |
| `dimensionamiento_dashboard_snapshots` | 1 | 656.0 KB | 6 | id | 1 | 4 |  |
| `dimensionamiento_family_monthly_summary` | 259,702 | 542.3 MB | 17 | id | 1 | 26 | grande |
| `dimensionamiento_import_errors` | -1 | 32.0 KB | 6 | id | 1 | 2 |  |
| `dimensionamiento_import_runs` | 12 | 224.0 KB | 17 | id | 0 | 5 |  |
| `dimensionamiento_records` | 317,236 | 607.6 MB | 27 | id | 1 | 27 | grande |
| `email_notifications` | -1 | 56.0 KB | 5 | id | 1 | 6 |  |
| `forecast_articulo` | 121,701 | 25.6 MB | 9 | id | 0 | 3 | forecast_* (NO en ORM — datos base); grande |
| `forecast_base` | 966,072 | 257.9 MB | 19 | id | 0 | 8 | forecast_* (NO en ORM — datos base); grande |
| `forecast_change_requests` | 2,270 | 1.9 MB | 28 | id | 1 | 6 | forecast_* (en ORM) |
| `forecast_cliente` | 40,092 | 8.9 MB | 11 | id | 0 | 5 | forecast_* (NO en ORM — datos base) |
| `forecast_dataset_base` | 221,424 | 52.3 MB | 10 | id | 0 | 7 | forecast_* (NO en ORM — datos base); grande |
| `forecast_fact_2026` | 206,246 | 46.5 MB | 12 | — | 0 | 5 | forecast_* (NO en ORM — datos base); grande |
| `forecast_imp_hist` | 44,861 | 6.9 MB | 6 | — | 0 | 3 | forecast_* (NO en ORM — datos base) |
| `forecast_main` | 277,452 | 114.0 MB | 27 | — | 0 | 9 | forecast_* (NO en ORM — datos base); grande |
| `forecast_manual_clients` | -1 | 24.0 KB | 10 | id | 1 | 1 | forecast_* (en ORM) |
| `forecast_manual_entries` | -1 | 24.0 KB | 15 | id | 1 | 1 | forecast_* (en ORM) |
| `forecast_negocio` | 145 | 104.0 KB | 4 | id | 0 | 3 | forecast_* (NO en ORM — datos base) |
| `forecast_product_labs` | 2,914 | 744.0 KB | 2 | — | 0 | 2 | forecast_* (NO en ORM — datos base) |
| `forecast_user_overrides` | 3,771 | 4.5 MB | 21 | id | 1 | 12 | forecast_* (en ORM) |
| `forecast_valorizado` | 702,436 | 263.6 MB | 18 | — | 0 | 11 | forecast_* (NO en ORM — datos base); grande |
| `group_members` | -1 | 120.0 KB | 6 | id | 3 | 6 |  |
| `groups` | -1 | 88.0 KB | 5 | id | 1 | 4 |  |
| `normalized_files` | -1 | 24.0 KB | 5 | id | 1 | 1 | revisión |
| `notifications` | 115 | 152.0 KB | 8 | id | 1 | 4 |  |
| `order_items` | -1 | 24.0 KB | 11 | id | 1 | 1 |  |
| `orders` | -1 | 16.0 KB | 16 | order_id | 0 | 0 |  |
| `password_reset_requests` | -1 | 80.0 KB | 12 | id | 0 | 3 | revisión |
| `pliego_actos_admin` | -1 | 48.0 KB | 8 | id | 1 | 1 |  |
| `pliego_analitica` | -1 | 48.0 KB | 3 | id | 1 | 1 |  |
| `pliego_archivos` | -1 | 72.8 MB | 9 | id | 1 | 1 |  |
| `pliego_control_carga` | -1 | 48.0 KB | 3 | id | 1 | 1 |  |
| `pliego_cronograma` | -1 | 88.0 KB | 8 | id | 1 | 1 |  |
| `pliego_documentos` | -1 | 80.0 KB | 8 | id | 1 | 1 |  |
| `pliego_edit_history` | -1 | 24.0 KB | 16 | id | 2 | 1 |  |
| `pliego_excel_cargas` | -1 | 768.0 KB | 10 | id | 2 | 1 |  |
| `pliego_faltantes` | 156 | 112.0 KB | 9 | id | 1 | 1 |  |
| `pliego_field_overrides` | -1 | 32.0 KB | 17 | id | 2 | 2 |  |
| `pliego_fusion_cabecera` | -1 | 48.0 KB | 3 | id | 1 | 1 |  |
| `pliego_fusion_renglones` | 1,704 | 1.0 MB | 9 | id | 1 | 1 |  |
| `pliego_garantias` | 70 | 80.0 KB | 10 | id | 1 | 1 |  |
| `pliego_hallazgos` | -1 | 112.0 KB | 8 | id | 1 | 1 |  |
| `pliego_historial` | -1 | 80.0 KB | 7 | id | 2 | 1 |  |
| `pliego_proceso` | -1 | 104.0 KB | 3 | id | 1 | 1 |  |
| `pliego_renglones` | 2,009 | 1.1 MB | 13 | id | 1 | 1 |  |
| `pliego_requisitos` | 244 | 112.0 KB | 10 | id | 1 | 1 |  |
| `pliego_solicitudes` | 17 | 120.0 KB | 21 | id | 4 | 4 |  |
| `pliego_trazabilidad` | 409 | 208.0 KB | 8 | id | 1 | 1 |  |
| `products` | -1 | 24.0 KB | 6 | id | 0 | 2 |  |
| `revision_sessions` | -1 | 40.0 KB | 8 | id | 1 | 3 | revisión |
| `runs` | -1 | 48.0 KB | 6 | id | 1 | 4 | revisión |
| `saved_views` | -1 | 88.0 KB | 8 | id | 1 | 9 |  |
| `ticket_messages` | -1 | 96.0 KB | 6 | id | 2 | 2 |  |
| `tickets` | -1 | 64.0 KB | 11 | id | 2 | 2 |  |
| `uploads` | 29 | 19.4 MB | 22 | id | 0 | 21 |  |
| `usage_events` | 68,752 | 30.9 MB | 12 | id | 1 | 9 |  |
| `usage_sessions` | -1 | 64.0 KB | 12 | id | 1 | 7 |  |
| `users` | 10 | 64.0 KB | 10 | id | 0 | 2 |  |

## Tablas `forecast_*` de datos base (NO modeladas en el ORM)

> Estas son el objetivo de modelado de la Fase 3. En SQLite local normalmente
> NO aparecen (los datos base son CSV/parquet); existen como tablas en PostgreSQL
> producción. Las `forecast_*` que SÍ están en el ORM (overrides, manuales,
> aprobaciones) no se listan aquí porque ya están modeladas.

### `forecast_articulo` — 121701 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `codigo` | VARCHAR(100) | True |
| `descrip` | VARCHAR(300) | True |
| `predrog` | VARCHAR(200) | True |
| `cantenv` | DOUBLE PRECISION | True |
| `laboratorio_descrip` | VARCHAR(200) | True |
| `familia` | VARCHAR(200) | True |
| `unineg` | VARCHAR(100) | True |
| `sunineg` | VARCHAR(100) | True |

### `forecast_base` — 966072 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `fecha` | TIMESTAMP | True |
| `periodo` | VARCHAR(20) | True |
| `codigo_serie` | VARCHAR(100) | True |
| `nivel_agregacion` | VARCHAR(50) | True |
| `perfil` | VARCHAR(50) | True |
| `neg` | INTEGER | True |
| `subneg` | INTEGER | True |
| `familia` | VARCHAR(200) | True |
| `tipo` | VARCHAR(50) | True |
| `y` | DOUBLE PRECISION | True |
| `yhat` | DOUBLE PRECISION | True |
| `li` | DOUBLE PRECISION | True |
| `ls` | DOUBLE PRECISION | True |
| `submodelo` | VARCHAR(50) | True |
| `clasificacion_serie` | VARCHAR(50) | True |
| `version_param` | VARCHAR(50) | True |
| `precio` | DOUBLE PRECISION | True |
| `etiqueta_upper` | VARCHAR(50) | True |

### `forecast_cliente` — 40092 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `codigo` | VARCHAR(50) | True |
| `nombre` | VARCHAR(200) | True |
| `fantasia` | VARCHAR(200) | True |
| `grupo` | VARCHAR(50) | True |
| `perfil` | VARCHAR(100) | True |
| `provincia` | VARCHAR(100) | True |
| `vendedor_abrev` | VARCHAR(100) | True |
| `cliente_grupo` | VARCHAR(50) | True |
| `nombre_grupo` | VARCHAR(200) | True |
| `tipocli` | VARCHAR(50) | True |

### `forecast_dataset_base` — 221424 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `codigo_serie` | VARCHAR(100) | True |
| `perfil` | VARCHAR(50) | True |
| `qty_mes` | DOUBLE PRECISION | True |
| `periodo` | VARCHAR(20) | True |
| `nivel_agregacion` | VARCHAR(50) | True |
| `neg` | INTEGER | True |
| `subneg` | INTEGER | True |
| `familia` | VARCHAR(200) | True |
| `fecha` | TIMESTAMP | True |

### `forecast_fact_2026` — 206246 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `fecha` | TIMESTAMP | True |
| `codigo_serie` | TEXT | True |
| `perfil` | TEXT | True |
| `cliente_id` | TEXT | True |
| `familia` | TEXT | True |
| `descripcion` | TEXT | True |
| `nivel_agregacion` | TEXT | True |
| `articulo_codigo` | TEXT | True |
| `y` | TEXT | True |
| `imp_hist` | DOUBLE PRECISION | True |
| `tipo` | TEXT | True |
| `tipocli` | TEXT | True |

### `forecast_imp_hist` — 44861 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `periodo` | TEXT | True |
| `codigo_serie` | TEXT | True |
| `perfil` | TEXT | True |
| `imp_hist` | DOUBLE PRECISION | True |
| `tipo` | TEXT | True |
| `fecha` | TIMESTAMP | True |

### `forecast_main` — 277452 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `periodo` | TEXT | True |
| `codigo_serie` | TEXT | True |
| `nivel_agregacion` | TEXT | True |
| `perfil` | TEXT | True |
| `neg` | TEXT | True |
| `subneg` | TEXT | True |
| `familia_x` | TEXT | True |
| `tipo` | TEXT | True |
| `y` | TEXT | True |
| `yhat` | TEXT | True |
| `li` | TEXT | True |
| `ls` | TEXT | True |
| `submodelo` | TEXT | True |
| `clasificacion_serie` | TEXT | True |
| `version_param` | TEXT | True |
| `articulo` | TEXT | True |
| `codigo` | TEXT | True |
| `descrip` | TEXT | True |
| `predrog` | TEXT | True |
| `cantenv` | TEXT | True |
| `laboratorio_descrip` | TEXT | True |
| `familia_y` | TEXT | True |
| `unineg` | TEXT | True |
| `sunineg` | TEXT | True |
| `descripcion` | TEXT | True |
| `fecha` | TIMESTAMP | True |
| `precio` | DOUBLE PRECISION | True |

### `forecast_negocio` — 145 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `unidad` | INTEGER | True |
| `subunidad` | INTEGER | True |
| `descrip` | VARCHAR(200) | True |

### `forecast_product_labs` — 2914 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `codigo_serie` | TEXT | True |
| `laboratorios` | TEXT | True |

### `forecast_valorizado` — 702436 filas

| Columna | Tipo | Nullable |
|---|---|---|
| `periodo` | TEXT | True |
| `fecha` | TIMESTAMP | True |
| `codigo_serie` | TEXT | True |
| `perfil` | TEXT | True |
| `cliente_id` | TEXT | True |
| `yhat_cliente` | BIGINT | True |
| `li_cliente` | BIGINT | True |
| `ls_cliente` | BIGINT | True |
| `monto_yhat` | DOUBLE PRECISION | True |
| `monto_li` | DOUBLE PRECISION | True |
| `monto_ls` | DOUBLE PRECISION | True |
| `nivel_agregacion` | TEXT | True |
| `descripcion` | TEXT | True |
| `clasificacion_serie` | TEXT | True |
| `fantasia` | TEXT | True |
| `nombre_grupo` | TEXT | True |
| `neg` | TEXT | True |
| `subneg` | TEXT | True |


## Tablas vacías

_Ninguna._

## Tablas candidatas a revisión (legado / fantasma)

> Presentes en esta base. **No eliminar** sin validación + backup (ver plan de migración).

- `chat_channels` — -1 filas
- `chat_members` — -1 filas
- `chat_messages` — -1 filas
- `dashboards` — -1 filas
- `normalized_files` — -1 filas
- `password_reset_requests` — -1 filas
- `revision_sessions` — -1 filas
- `runs` — -1 filas

## Advertencias de compatibilidad SQLite <-> PostgreSQL

- **`comments`**
  - `meta`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`dimensionamiento_dashboard_snapshots`**
  - `payload`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`dimensionamiento_import_errors`**
  - `raw_payload`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`dimensionamiento_import_runs`**
  - `expected_columns`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
  - `observed_columns`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
  - `summary`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`pliego_analitica`**
  - `datos`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`pliego_archivos`**
  - `contenido_bytes`: binario (BLOB/BYTEA) — candidato a separar de la tabla
- **`pliego_control_carga`**
  - `datos`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`pliego_excel_cargas`**
  - `contenido_bytes`: binario (BLOB/BYTEA) — candidato a separar de la tabla
- **`pliego_fusion_cabecera`**
  - `datos`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`pliego_fusion_renglones`**
  - `datos_extra`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`pliego_hallazgos`**
  - `datos_extra`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`pliego_proceso`**
  - `datos`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`pliego_renglones`**
  - `datos_extra`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`saved_views`**
  - `payload`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
- **`uploads`**
  - `normalized_content`: binario (BLOB/BYTEA) — candidato a separar de la tabla
  - `original_content`: binario (BLOB/BYTEA) — candidato a separar de la tabla
- **`usage_events`**
  - `extra_data`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL

## Apéndice — detalle por tabla

### `app_config` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `key` | VARCHAR(120) | False |
| `value` | VARCHAR(255) | True |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |

**PK:** id

**FK:** —

**Índices:**
- `ix_app_config_created_at` [created_at]
  - `CREATE INDEX ix_app_config_created_at ON public.app_config USING btree (created_at)`
- `ix_app_config_key` [key] (unique)
  - `CREATE UNIQUE INDEX ix_app_config_key ON public.app_config USING btree (key)`
- `ix_app_config_updated_at` [updated_at]
  - `CREATE INDEX ix_app_config_updated_at ON public.app_config USING btree (updated_at)`

### `app_users` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `username` | VARCHAR(64) | False |
| `password_hash` | VARCHAR(255) | False |
| `created_at` | TIMESTAMP | False |

**PK:** username

**FK:** —

**Índices:** —

### `chat_channels` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `type` | VARCHAR(20) | False |
| `name` | VARCHAR(100) | True |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |

**PK:** id

**FK:** —

**Índices:**
- `ix_chat_channels_updated_at` [updated_at]
  - `CREATE INDEX ix_chat_channels_updated_at ON public.chat_channels USING btree (updated_at)`

### `chat_members` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `channel_id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `joined_at` | TIMESTAMP | True |
| `last_read_at` | TIMESTAMP | True |

**PK:** id

**FK:**
- (channel_id) → `chat_channels` (id)
- (user_id) → `users` (id)

**Índices:**
- `ix_chat_members_channel_id` [channel_id]
  - `CREATE INDEX ix_chat_members_channel_id ON public.chat_members USING btree (channel_id)`
- `ix_chat_members_user_id` [user_id]
  - `CREATE INDEX ix_chat_members_user_id ON public.chat_members USING btree (user_id)`
- `uq_chat_member` [channel_id, user_id] (unique)
  - `CREATE UNIQUE INDEX uq_chat_member ON public.chat_members USING btree (channel_id, user_id)`

### `chat_messages` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `channel_id` | INTEGER | False |
| `sender_id` | INTEGER | False |
| `content` | TEXT | True |
| `attachment_path` | VARCHAR | True |
| `created_at` | TIMESTAMP | False |

**PK:** id

**FK:**
- (channel_id) → `chat_channels` (id)
- (sender_id) → `users` (id)

**Índices:**
- `ix_chat_messages_channel_id` [channel_id]
  - `CREATE INDEX ix_chat_messages_channel_id ON public.chat_messages USING btree (channel_id)`
- `ix_chat_messages_created_at` [created_at]
  - `CREATE INDEX ix_chat_messages_created_at ON public.chat_messages USING btree (created_at)`
- `ix_chat_messages_sender_id` [sender_id]
  - `CREATE INDEX ix_chat_messages_sender_id ON public.chat_messages USING btree (sender_id)`

### `clients` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `numero_cliente` | VARCHAR(80) | False |
| `razon_social` | VARCHAR(255) | False |
| `cuit` | VARCHAR(80) | False |
| `mail` | VARCHAR(160) | False |
| `celular` | VARCHAR(80) | False |
| `direccion` | VARCHAR(255) | False |
| `ciudad` | VARCHAR(120) | False |
| `provincia` | VARCHAR(120) | False |
| `codigo_postal` | VARCHAR(40) | False |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |

**PK:** id

**FK:** —

**Índices:**
- `clients_numero_cliente_index` [numero_cliente]
  - `CREATE INDEX clients_numero_cliente_index ON public.clients USING btree (numero_cliente)`
- `clients_razon_social_index` [razon_social]
  - `CREATE INDEX clients_razon_social_index ON public.clients USING btree (razon_social)`

### `comments` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | False |
| `author_user_id` | INTEGER | True |
| `parent_id` | INTEGER | True |
| `body` | TEXT | False |
| `is_resolved` | BOOLEAN | False |
| `meta` | JSON | True |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |
| `deleted_at` | TIMESTAMP | True |

**PK:** id

**FK:**
- (author_user_id) → `users` (id)
- (parent_id) → `comments` (id)
- (upload_id) → `uploads` (id)

**Índices:**
- `idx_comments_author` [author_user_id]
  - `CREATE INDEX idx_comments_author ON public.comments USING btree (author_user_id)`
- `idx_comments_created` [created_at]
  - `CREATE INDEX idx_comments_created ON public.comments USING btree (created_at)`
- `idx_comments_parent` [parent_id]
  - `CREATE INDEX idx_comments_parent ON public.comments USING btree (parent_id)`
- `idx_comments_resolved` [is_resolved]
  - `CREATE INDEX idx_comments_resolved ON public.comments USING btree (is_resolved)`
- `idx_comments_upload` [upload_id]
  - `CREATE INDEX idx_comments_upload ON public.comments USING btree (upload_id)`
- `ix_comments_author_user_id` [author_user_id]
  - `CREATE INDEX ix_comments_author_user_id ON public.comments USING btree (author_user_id)`
- `ix_comments_created_at` [created_at]
  - `CREATE INDEX ix_comments_created_at ON public.comments USING btree (created_at)`
- `ix_comments_deleted_at` [deleted_at]
  - `CREATE INDEX ix_comments_deleted_at ON public.comments USING btree (deleted_at)`
- `ix_comments_is_resolved` [is_resolved]
  - `CREATE INDEX ix_comments_is_resolved ON public.comments USING btree (is_resolved)`
- `ix_comments_parent_id` [parent_id]
  - `CREATE INDEX ix_comments_parent_id ON public.comments USING btree (parent_id)`
- `ix_comments_updated_at` [updated_at]
  - `CREATE INDEX ix_comments_updated_at ON public.comments USING btree (updated_at)`
- `ix_comments_upload_id` [upload_id]
  - `CREATE INDEX ix_comments_upload_id ON public.comments USING btree (upload_id)`

### `comparativa_rows` — 86,081 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | False |
| `fecha_apertura` | DATE | True |
| `nro_proceso` | VARCHAR(255) | True |
| `comprador` | TEXT | True |
| `plataforma` | VARCHAR(60) | True |
| `cuenta` | VARCHAR(60) | True |
| `provincia` | VARCHAR(120) | True |
| `proveedor` | TEXT | True |
| `renglon` | VARCHAR(60) | True |
| `alternativa` | VARCHAR(60) | True |
| `codigo` | VARCHAR(120) | True |
| `descripcion` | TEXT | True |
| `cantidad_solicitada` | DOUBLE PRECISION | True |
| `unidad_medida` | VARCHAR(60) | True |
| `precio_unitario` | DOUBLE PRECISION | True |
| `cantidad_ofertada` | DOUBLE PRECISION | True |
| `total_por_renglon` | DOUBLE PRECISION | True |
| `especificacion_tecnica` | TEXT | True |
| `marca` | VARCHAR(255) | True |
| `posicion` | INTEGER | True |
| `rubro` | VARCHAR(255) | True |
| `created_at` | TIMESTAMP | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:**
- `ix_comp_rows_comprador_fecha` [comprador, fecha_apertura]
  - `CREATE INDEX ix_comp_rows_comprador_fecha ON public.comparativa_rows USING btree (comprador, fecha_apertura)`
- `ix_comp_rows_descripcion_fecha` [descripcion, fecha_apertura]
  - `CREATE INDEX ix_comp_rows_descripcion_fecha ON public.comparativa_rows USING btree (descripcion, fecha_apertura)`
- `ix_comp_rows_fecha_apertura` [fecha_apertura]
  - `CREATE INDEX ix_comp_rows_fecha_apertura ON public.comparativa_rows USING btree (fecha_apertura)`
- `ix_comp_rows_marca_fecha` [marca, fecha_apertura]
  - `CREATE INDEX ix_comp_rows_marca_fecha ON public.comparativa_rows USING btree (marca, fecha_apertura)`
- `ix_comp_rows_proveedor_fecha` [proveedor, fecha_apertura]
  - `CREATE INDEX ix_comp_rows_proveedor_fecha ON public.comparativa_rows USING btree (proveedor, fecha_apertura)`
- `ix_comp_rows_upload_proveedor` [upload_id, proveedor]
  - `CREATE INDEX ix_comp_rows_upload_proveedor ON public.comparativa_rows USING btree (upload_id, proveedor)`
- `ix_comparativa_rows_nro_proceso` [nro_proceso]
  - `CREATE INDEX ix_comparativa_rows_nro_proceso ON public.comparativa_rows USING btree (nro_proceso)`
- `ix_comparativa_rows_upload_id` [upload_id]
  - `CREATE INDEX ix_comparativa_rows_upload_id ON public.comparativa_rows USING btree (upload_id)`

### `dashboards` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | False |
| `json_path` | VARCHAR | True |
| `html_path` | VARCHAR | True |
| `published_at` | TIMESTAMP | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:**
- `ix_dashboards_published_at` [published_at]
  - `CREATE INDEX ix_dashboards_published_at ON public.dashboards USING btree (published_at)`
- `ix_dashboards_upload_id` [upload_id]
  - `CREATE INDEX ix_dashboards_upload_id ON public.dashboards USING btree (upload_id)`

### `dimensionamiento_dashboard_snapshots` — 1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `snapshot_key` | VARCHAR(100) | False |
| `version` | VARCHAR(20) | False |
| `payload` | JSON | False |
| `generated_at` | TIMESTAMP | False |
| `import_run_id` | INTEGER | True |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dimensionamiento_dashboard_snapshots_generated_at` [generated_at]
  - `CREATE INDEX ix_dimensionamiento_dashboard_snapshots_generated_at ON public.dimensionamiento_dashboard_snapshots USING btree (generated_at)`
- `ix_dimensionamiento_dashboard_snapshots_import_run_id` [import_run_id]
  - `CREATE INDEX ix_dimensionamiento_dashboard_snapshots_import_run_id ON public.dimensionamiento_dashboard_snapshots USING btree (import_run_id)`
- `ix_dimensionamiento_dashboard_snapshots_version` [version]
  - `CREATE INDEX ix_dimensionamiento_dashboard_snapshots_version ON public.dimensionamiento_dashboard_snapshots USING btree (version)`
- `uq_dim_dashboard_snapshots_key_run` [snapshot_key, import_run_id] (unique)
  - `CREATE UNIQUE INDEX uq_dim_dashboard_snapshots_key_run ON public.dimensionamiento_dashboard_snapshots USING btree (snapshot_key, import_run_id)`

### `dimensionamiento_family_monthly_summary` — 259,702 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `month` | DATE | False |
| `plataforma` | VARCHAR(40) | False |
| `cliente_nombre_homologado` | TEXT | True |
| `cliente_visible` | TEXT | True |
| `provincia` | VARCHAR(120) | True |
| `familia` | TEXT | True |
| `unidad_negocio` | TEXT | True |
| `subunidad_negocio` | TEXT | True |
| `resultado_participacion` | VARCHAR(120) | True |
| `is_identified` | BOOLEAN | False |
| `is_client` | BOOLEAN | False |
| `total_cantidad` | DOUBLE PRECISION | False |
| `total_valorizacion` | DOUBLE PRECISION | False |
| `total_registros` | INTEGER | False |
| `clientes_unicos` | INTEGER | False |
| `import_run_id` | INTEGER | False |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dim_sum_familia_qty` [familia, total_cantidad]
  - `CREATE INDEX ix_dim_sum_familia_qty ON public.dimensionamiento_family_monthly_summary USING btree (familia, total_cantidad)`
- `ix_dim_sum_is_client_cliente` [is_client, cliente_nombre_homologado]
  - `CREATE INDEX ix_dim_sum_is_client_cliente ON public.dimensionamiento_family_monthly_summary USING btree (is_client, cliente_nombre_homologado)`
- `ix_dim_sum_isclient_family_month` [is_client, familia, month]
  - `CREATE INDEX ix_dim_sum_isclient_family_month ON public.dimensionamiento_family_monthly_summary USING btree (is_client, familia, month)`
- `ix_dim_sum_isclient_province_month` [is_client, provincia, month]
  - `CREATE INDEX ix_dim_sum_isclient_province_month ON public.dimensionamiento_family_monthly_summary USING btree (is_client, provincia, month)`
- `ix_dim_sum_isclient_result_month` [is_client, resultado_participacion, month]
  - `CREATE INDEX ix_dim_sum_isclient_result_month ON public.dimensionamiento_family_monthly_summary USING btree (is_client, resultado_participacion, month)`
- `ix_dim_sum_isclient_unit_month` [is_client, unidad_negocio, month]
  - `CREATE INDEX ix_dim_sum_isclient_unit_month ON public.dimensionamiento_family_monthly_summary USING btree (is_client, unidad_negocio, month)`
- `ix_dim_sum_provincia_month` [provincia, month]
  - `CREATE INDEX ix_dim_sum_provincia_month ON public.dimensionamiento_family_monthly_summary USING btree (provincia, month)`
- `ix_dim_sum_resultado_plat` [resultado_participacion, plataforma]
  - `CREATE INDEX ix_dim_sum_resultado_plat ON public.dimensionamiento_family_monthly_summary USING btree (resultado_participacion, plataforma)`
- `ix_dim_sum_unidad_month_total` [unidad_negocio, month, total_registros]
  - `CREATE INDEX ix_dim_sum_unidad_month_total ON public.dimensionamiento_family_monthly_summary USING btree (unidad_negocio, month, total_registros)`
- `ix_dim_summary_client_month` [cliente_nombre_homologado, month]
  - `CREATE INDEX ix_dim_summary_client_month ON public.dimensionamiento_family_monthly_summary USING btree (cliente_nombre_homologado, month)`
- `ix_dim_summary_family_month` [familia, month]
  - `CREATE INDEX ix_dim_summary_family_month ON public.dimensionamiento_family_monthly_summary USING btree (familia, month)`
- `ix_dim_summary_platform_month` [plataforma, month]
  - `CREATE INDEX ix_dim_summary_platform_month ON public.dimensionamiento_family_monthly_summary USING btree (plataforma, month)`
- `ix_dim_summary_visible_month` [cliente_visible, month]
  - `CREATE INDEX ix_dim_summary_visible_month ON public.dimensionamiento_family_monthly_summary USING btree (cliente_visible, month)`
- `ix_dimensionamiento_family_monthly_summary_cliente_nomb_b9ba` [cliente_nombre_homologado]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_cliente_nomb_b9ba ON public.dimensionamiento_family_monthly_summary USING btree (cliente_nombre_homologado)`
- `ix_dimensionamiento_family_monthly_summary_cliente_visible` [cliente_visible]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_cliente_visible ON public.dimensionamiento_family_monthly_summary USING btree (cliente_visible)`
- `ix_dimensionamiento_family_monthly_summary_familia` [familia]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_familia ON public.dimensionamiento_family_monthly_summary USING btree (familia)`
- `ix_dimensionamiento_family_monthly_summary_import_run_id` [import_run_id]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_import_run_id ON public.dimensionamiento_family_monthly_summary USING btree (import_run_id)`
- `ix_dimensionamiento_family_monthly_summary_is_client` [is_client]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_is_client ON public.dimensionamiento_family_monthly_summary USING btree (is_client)`
- `ix_dimensionamiento_family_monthly_summary_is_identified` [is_identified]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_is_identified ON public.dimensionamiento_family_monthly_summary USING btree (is_identified)`
- `ix_dimensionamiento_family_monthly_summary_month` [month]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_month ON public.dimensionamiento_family_monthly_summary USING btree (month)`
- `ix_dimensionamiento_family_monthly_summary_plataforma` [plataforma]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_plataforma ON public.dimensionamiento_family_monthly_summary USING btree (plataforma)`
- `ix_dimensionamiento_family_monthly_summary_provincia` [provincia]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_provincia ON public.dimensionamiento_family_monthly_summary USING btree (provincia)`
- `ix_dimensionamiento_family_monthly_summary_resultado_pa_7dcb` [resultado_participacion]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_resultado_pa_7dcb ON public.dimensionamiento_family_monthly_summary USING btree (resultado_participacion)`
- `ix_dimensionamiento_family_monthly_summary_subunidad_negocio` [subunidad_negocio]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_subunidad_negocio ON public.dimensionamiento_family_monthly_summary USING btree (subunidad_negocio)`
- `ix_dimensionamiento_family_monthly_summary_unidad_negocio` [unidad_negocio]
  - `CREATE INDEX ix_dimensionamiento_family_monthly_summary_unidad_negocio ON public.dimensionamiento_family_monthly_summary USING btree (unidad_negocio)`
- `uq_dim_family_monthly_summary` [month, plataforma, cliente_nombre_homologado, cliente_visible, provincia, familia, unidad_negocio, subunidad_negocio, resultado_participacion, is_identified, is_client, import_run_id] (unique)
  - `CREATE UNIQUE INDEX uq_dim_family_monthly_summary ON public.dimensionamiento_family_monthly_summary USING btree (month, plataforma, cliente_nombre_homologado, cliente_visible, provincia, familia, unidad_negocio, subunidad_negocio, resultado_participacion, is_identified, is_client, import_run_id)`

### `dimensionamiento_import_errors` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `import_run_id` | INTEGER | False |
| `row_number` | INTEGER | False |
| `error_message` | TEXT | False |
| `raw_payload` | JSON | True |
| `created_at` | TIMESTAMP | False |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dimensionamiento_import_errors_created_at` [created_at]
  - `CREATE INDEX ix_dimensionamiento_import_errors_created_at ON public.dimensionamiento_import_errors USING btree (created_at)`
- `ix_dimensionamiento_import_errors_import_run_id` [import_run_id]
  - `CREATE INDEX ix_dimensionamiento_import_errors_import_run_id ON public.dimensionamiento_import_errors USING btree (import_run_id)`

### `dimensionamiento_import_runs` — 12 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `source_path` | VARCHAR(500) | False |
| `source_hash` | VARCHAR(64) | True |
| `source_mtime` | TIMESTAMP | True |
| `mode` | VARCHAR(20) | False |
| `status` | VARCHAR(20) | False |
| `chunk_size` | INTEGER | False |
| `started_at` | TIMESTAMP | False |
| `finished_at` | TIMESTAMP | True |
| `rows_processed` | INTEGER | False |
| `rows_inserted` | INTEGER | False |
| `rows_updated` | INTEGER | False |
| `rows_rejected` | INTEGER | False |
| `expected_columns` | JSON | True |
| `observed_columns` | JSON | True |
| `summary` | JSON | True |
| `error_message` | TEXT | True |

**PK:** id

**FK:** —

**Índices:**
- `ix_dimensionamiento_import_runs_finished_at` [finished_at]
  - `CREATE INDEX ix_dimensionamiento_import_runs_finished_at ON public.dimensionamiento_import_runs USING btree (finished_at)`
- `ix_dimensionamiento_import_runs_mode` [mode]
  - `CREATE INDEX ix_dimensionamiento_import_runs_mode ON public.dimensionamiento_import_runs USING btree (mode)`
- `ix_dimensionamiento_import_runs_source_hash` [source_hash]
  - `CREATE INDEX ix_dimensionamiento_import_runs_source_hash ON public.dimensionamiento_import_runs USING btree (source_hash)`
- `ix_dimensionamiento_import_runs_started_at` [started_at]
  - `CREATE INDEX ix_dimensionamiento_import_runs_started_at ON public.dimensionamiento_import_runs USING btree (started_at)`
- `ix_dimensionamiento_import_runs_status` [status]
  - `CREATE INDEX ix_dimensionamiento_import_runs_status ON public.dimensionamiento_import_runs USING btree (status)`

### `dimensionamiento_records` — 317,236 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `id_registro_unico` | VARCHAR(255) | False |
| `fecha` | DATE | False |
| `plataforma` | VARCHAR(40) | False |
| `cliente_nombre_homologado` | TEXT | True |
| `cliente_nombre_original` | TEXT | True |
| `cuit` | VARCHAR(32) | True |
| `provincia` | VARCHAR(120) | True |
| `cuenta_interna` | VARCHAR(120) | True |
| `codigo_articulo` | VARCHAR(120) | True |
| `descripcion` | TEXT | True |
| `clasificacion_suizo` | TEXT | True |
| `descripcion_articulo` | TEXT | True |
| `familia` | TEXT | True |
| `unidad_negocio` | TEXT | True |
| `subunidad_negocio` | TEXT | True |
| `cantidad_demandada` | DOUBLE PRECISION | False |
| `resultado_participacion` | VARCHAR(120) | True |
| `producto_nombre_original` | TEXT | True |
| `fecha_procesamiento` | TIMESTAMP | True |
| `is_identified` | BOOLEAN | False |
| `is_client` | BOOLEAN | False |
| `import_run_id` | INTEGER | True |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |
| `cliente_visible` | TEXT | True |
| `valorizacion_estimada` | DOUBLE PRECISION | True |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dim_records_client_date` [cliente_nombre_homologado, fecha]
  - `CREATE INDEX ix_dim_records_client_date ON public.dimensionamiento_records USING btree (cliente_nombre_homologado, fecha)`
- `ix_dim_records_cliente_hom_norm` [<expression>] · funcional/expresión
  - expresiones: upper(TRIM(BOTH FROM COALESCE(cliente_nombre_homologado, ''::character varying::text)))
  - `CREATE INDEX ix_dim_records_cliente_hom_norm ON public.dimensionamiento_records USING btree (upper(TRIM(BOTH FROM COALESCE(cliente_nombre_homologado, (''::character varying)::text))))`
- `ix_dim_records_familia_norm` [<expression>] · funcional/expresión
  - expresiones: upper(TRIM(BOTH FROM COALESCE(familia, ''::character varying::text)))
  - `CREATE INDEX ix_dim_records_familia_norm ON public.dimensionamiento_records USING btree (upper(TRIM(BOTH FROM COALESCE(familia, (''::character varying)::text))))`
- `ix_dim_records_family_date` [familia, fecha]
  - `CREATE INDEX ix_dim_records_family_date ON public.dimensionamiento_records USING btree (familia, fecha)`
- `ix_dim_records_plataforma_norm` [<expression>] · funcional/expresión
  - expresiones: upper(TRIM(BOTH FROM COALESCE(plataforma, ''::character varying)::text))
  - `CREATE INDEX ix_dim_records_plataforma_norm ON public.dimensionamiento_records USING btree (upper(TRIM(BOTH FROM (COALESCE(plataforma, ''::character varying))::text)))`
- `ix_dim_records_platform_date` [plataforma, fecha]
  - `CREATE INDEX ix_dim_records_platform_date ON public.dimensionamiento_records USING btree (plataforma, fecha)`
- `ix_dim_records_province_date` [provincia, fecha]
  - `CREATE INDEX ix_dim_records_province_date ON public.dimensionamiento_records USING btree (provincia, fecha)`
- `ix_dim_records_provincia_norm` [<expression>] · funcional/expresión
  - expresiones: upper(TRIM(BOTH FROM COALESCE(provincia, ''::character varying)::text))
  - `CREATE INDEX ix_dim_records_provincia_norm ON public.dimensionamiento_records USING btree (upper(TRIM(BOTH FROM (COALESCE(provincia, ''::character varying))::text)))`
- `ix_dim_records_result_date` [resultado_participacion, fecha]
  - `CREATE INDEX ix_dim_records_result_date ON public.dimensionamiento_records USING btree (resultado_participacion, fecha)`
- `ix_dim_records_resultado_norm` [<expression>] · funcional/expresión
  - expresiones: upper(TRIM(BOTH FROM COALESCE(resultado_participacion, ''::character varying)::text))
  - `CREATE INDEX ix_dim_records_resultado_norm ON public.dimensionamiento_records USING btree (upper(TRIM(BOTH FROM (COALESCE(resultado_participacion, ''::character varying))::text)))`
- `ix_dim_records_subunidad_norm` [<expression>] · funcional/expresión
  - expresiones: upper(TRIM(BOTH FROM COALESCE(subunidad_negocio, ''::character varying::text)))
  - `CREATE INDEX ix_dim_records_subunidad_norm ON public.dimensionamiento_records USING btree (upper(TRIM(BOTH FROM COALESCE(subunidad_negocio, (''::character varying)::text))))`
- `ix_dim_records_unidad_norm` [<expression>] · funcional/expresión
  - expresiones: upper(TRIM(BOTH FROM COALESCE(unidad_negocio, ''::character varying::text)))
  - `CREATE INDEX ix_dim_records_unidad_norm ON public.dimensionamiento_records USING btree (upper(TRIM(BOTH FROM COALESCE(unidad_negocio, (''::character varying)::text))))`
- `ix_dim_records_unit_subunit_date` [unidad_negocio, subunidad_negocio, fecha]
  - `CREATE INDEX ix_dim_records_unit_subunit_date ON public.dimensionamiento_records USING btree (unidad_negocio, subunidad_negocio, fecha)`
- `ix_dimensionamiento_records_cliente_nombre_homologado` [cliente_nombre_homologado]
  - `CREATE INDEX ix_dimensionamiento_records_cliente_nombre_homologado ON public.dimensionamiento_records USING btree (cliente_nombre_homologado)`
- `ix_dimensionamiento_records_codigo_articulo` [codigo_articulo]
  - `CREATE INDEX ix_dimensionamiento_records_codigo_articulo ON public.dimensionamiento_records USING btree (codigo_articulo)`
- `ix_dimensionamiento_records_familia` [familia]
  - `CREATE INDEX ix_dimensionamiento_records_familia ON public.dimensionamiento_records USING btree (familia)`
- `ix_dimensionamiento_records_fecha` [fecha]
  - `CREATE INDEX ix_dimensionamiento_records_fecha ON public.dimensionamiento_records USING btree (fecha)`
- `ix_dimensionamiento_records_fecha_procesamiento` [fecha_procesamiento]
  - `CREATE INDEX ix_dimensionamiento_records_fecha_procesamiento ON public.dimensionamiento_records USING btree (fecha_procesamiento)`
- `ix_dimensionamiento_records_import_run_id` [import_run_id]
  - `CREATE INDEX ix_dimensionamiento_records_import_run_id ON public.dimensionamiento_records USING btree (import_run_id)`
- `ix_dimensionamiento_records_is_client` [is_client]
  - `CREATE INDEX ix_dimensionamiento_records_is_client ON public.dimensionamiento_records USING btree (is_client)`
- `ix_dimensionamiento_records_is_identified` [is_identified]
  - `CREATE INDEX ix_dimensionamiento_records_is_identified ON public.dimensionamiento_records USING btree (is_identified)`
- `ix_dimensionamiento_records_plataforma` [plataforma]
  - `CREATE INDEX ix_dimensionamiento_records_plataforma ON public.dimensionamiento_records USING btree (plataforma)`
- `ix_dimensionamiento_records_provincia` [provincia]
  - `CREATE INDEX ix_dimensionamiento_records_provincia ON public.dimensionamiento_records USING btree (provincia)`
- `ix_dimensionamiento_records_resultado_participacion` [resultado_participacion]
  - `CREATE INDEX ix_dimensionamiento_records_resultado_participacion ON public.dimensionamiento_records USING btree (resultado_participacion)`
- `ix_dimensionamiento_records_subunidad_negocio` [subunidad_negocio]
  - `CREATE INDEX ix_dimensionamiento_records_subunidad_negocio ON public.dimensionamiento_records USING btree (subunidad_negocio)`
- `ix_dimensionamiento_records_unidad_negocio` [unidad_negocio]
  - `CREATE INDEX ix_dimensionamiento_records_unidad_negocio ON public.dimensionamiento_records USING btree (unidad_negocio)`
- `uq_dim_records_id_run` [id_registro_unico, import_run_id] (unique)
  - `CREATE UNIQUE INDEX uq_dim_records_id_run ON public.dimensionamiento_records USING btree (id_registro_unico, import_run_id)`

### `email_notifications` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | False |
| `recipient` | VARCHAR(255) | False |
| `event` | VARCHAR(50) | False |
| `sent_at` | TIMESTAMP | False |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:**
- `idx_email_notif_recipient` [recipient]
  - `CREATE INDEX idx_email_notif_recipient ON public.email_notifications USING btree (recipient)`
- `idx_email_notif_upload` [upload_id]
  - `CREATE INDEX idx_email_notif_upload ON public.email_notifications USING btree (upload_id)`
- `ix_email_notifications_event` [event]
  - `CREATE INDEX ix_email_notifications_event ON public.email_notifications USING btree (event)`
- `ix_email_notifications_recipient` [recipient]
  - `CREATE INDEX ix_email_notifications_recipient ON public.email_notifications USING btree (recipient)`
- `ix_email_notifications_upload_id` [upload_id]
  - `CREATE INDEX ix_email_notifications_upload_id ON public.email_notifications USING btree (upload_id)`
- `uq_email_notif` [upload_id, recipient, event] (unique)
  - `CREATE UNIQUE INDEX uq_email_notif ON public.email_notifications USING btree (upload_id, recipient, event)`

### `forecast_articulo` — 121,701 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `codigo` | VARCHAR(100) | True |
| `descrip` | VARCHAR(300) | True |
| `predrog` | VARCHAR(200) | True |
| `cantenv` | DOUBLE PRECISION | True |
| `laboratorio_descrip` | VARCHAR(200) | True |
| `familia` | VARCHAR(200) | True |
| `unineg` | VARCHAR(100) | True |
| `sunineg` | VARCHAR(100) | True |

**PK:** id

**FK:** —

**Índices:**
- `ix_forecast_articulo_codigo` [codigo]
  - `CREATE INDEX ix_forecast_articulo_codigo ON public.forecast_articulo USING btree (codigo)`
- `ix_forecast_articulo_id` [id]
  - `CREATE INDEX ix_forecast_articulo_id ON public.forecast_articulo USING btree (id)`
- `ix_forecast_articulo_laboratorio_descrip` [laboratorio_descrip]
  - `CREATE INDEX ix_forecast_articulo_laboratorio_descrip ON public.forecast_articulo USING btree (laboratorio_descrip)`

### `forecast_base` — 966,072 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `fecha` | TIMESTAMP | True |
| `periodo` | VARCHAR(20) | True |
| `codigo_serie` | VARCHAR(100) | True |
| `nivel_agregacion` | VARCHAR(50) | True |
| `perfil` | VARCHAR(50) | True |
| `neg` | INTEGER | True |
| `subneg` | INTEGER | True |
| `familia` | VARCHAR(200) | True |
| `tipo` | VARCHAR(50) | True |
| `y` | DOUBLE PRECISION | True |
| `yhat` | DOUBLE PRECISION | True |
| `li` | DOUBLE PRECISION | True |
| `ls` | DOUBLE PRECISION | True |
| `submodelo` | VARCHAR(50) | True |
| `clasificacion_serie` | VARCHAR(50) | True |
| `version_param` | VARCHAR(50) | True |
| `precio` | DOUBLE PRECISION | True |
| `etiqueta_upper` | VARCHAR(50) | True |

**PK:** id

**FK:** —

**Índices:**
- `ix_forecast_base_codigo_serie` [codigo_serie]
  - `CREATE INDEX ix_forecast_base_codigo_serie ON public.forecast_base USING btree (codigo_serie)`
- `ix_forecast_base_fecha` [fecha]
  - `CREATE INDEX ix_forecast_base_fecha ON public.forecast_base USING btree (fecha)`
- `ix_forecast_base_id` [id]
  - `CREATE INDEX ix_forecast_base_id ON public.forecast_base USING btree (id)`
- `ix_forecast_base_neg` [neg]
  - `CREATE INDEX ix_forecast_base_neg ON public.forecast_base USING btree (neg)`
- `ix_forecast_base_perfil` [perfil]
  - `CREATE INDEX ix_forecast_base_perfil ON public.forecast_base USING btree (perfil)`
- `ix_forecast_base_periodo` [periodo]
  - `CREATE INDEX ix_forecast_base_periodo ON public.forecast_base USING btree (periodo)`
- `ix_forecast_base_subneg` [subneg]
  - `CREATE INDEX ix_forecast_base_subneg ON public.forecast_base USING btree (subneg)`
- `ix_forecast_base_tipo` [tipo]
  - `CREATE INDEX ix_forecast_base_tipo ON public.forecast_base USING btree (tipo)`

### `forecast_change_requests` — 2,270 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `created_at` | TIMESTAMP | False |
| `override_id` | INTEGER | True |
| `source` | VARCHAR(30) | False |
| `created_by_user_id` | INTEGER | True |
| `created_by_username` | VARCHAR(255) | True |
| `change_type` | VARCHAR(30) | False |
| `scope_type` | VARCHAR(20) | True |
| `client_selector` | VARCHAR(255) | True |
| `client_name` | VARCHAR(255) | True |
| `perfil` | VARCHAR(120) | True |
| `neg` | VARCHAR(120) | True |
| `subneg` | VARCHAR(255) | True |
| `codigo_serie` | VARCHAR(120) | True |
| `descripcion_articulo` | VARCHAR(255) | True |
| `period` | VARCHAR(7) | True |
| `field_changed` | VARCHAR(60) | True |
| `old_value` | DOUBLE PRECISION | True |
| `new_value` | DOUBLE PRECISION | True |
| `absolute_delta` | DOUBLE PRECISION | True |
| `percentage_delta` | DOUBLE PRECISION | True |
| `estimated_amount_base` | DOUBLE PRECISION | True |
| `estimated_amount_delta` | DOUBLE PRECISION | True |
| `status` | VARCHAR(12) | False |
| `reviewed_by_user_id` | INTEGER | True |
| `reviewed_by_username` | VARCHAR(255) | True |
| `reviewed_at` | TIMESTAMP | True |
| `review_comment` | TEXT | True |

**PK:** id

**FK:**
- (override_id) → `forecast_user_overrides` (id)

**Índices:**
- `ix_forecast_change_requests_change_type` [change_type]
  - `CREATE INDEX ix_forecast_change_requests_change_type ON public.forecast_change_requests USING btree (change_type)`
- `ix_forecast_change_requests_client_selector` [client_selector]
  - `CREATE INDEX ix_forecast_change_requests_client_selector ON public.forecast_change_requests USING btree (client_selector)`
- `ix_forecast_change_requests_created_at` [created_at]
  - `CREATE INDEX ix_forecast_change_requests_created_at ON public.forecast_change_requests USING btree (created_at)`
- `ix_forecast_change_requests_created_by_user_id` [created_by_user_id]
  - `CREATE INDEX ix_forecast_change_requests_created_by_user_id ON public.forecast_change_requests USING btree (created_by_user_id)`
- `ix_forecast_change_requests_override_id` [override_id]
  - `CREATE INDEX ix_forecast_change_requests_override_id ON public.forecast_change_requests USING btree (override_id)`
- `ix_forecast_change_requests_status` [status]
  - `CREATE INDEX ix_forecast_change_requests_status ON public.forecast_change_requests USING btree (status)`

### `forecast_cliente` — 40,092 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `codigo` | VARCHAR(50) | True |
| `nombre` | VARCHAR(200) | True |
| `fantasia` | VARCHAR(200) | True |
| `grupo` | VARCHAR(50) | True |
| `perfil` | VARCHAR(100) | True |
| `provincia` | VARCHAR(100) | True |
| `vendedor_abrev` | VARCHAR(100) | True |
| `cliente_grupo` | VARCHAR(50) | True |
| `nombre_grupo` | VARCHAR(200) | True |
| `tipocli` | VARCHAR(50) | True |

**PK:** id

**FK:** —

**Índices:**
- `ix_forecast_cliente_cliente_grupo` [cliente_grupo]
  - `CREATE INDEX ix_forecast_cliente_cliente_grupo ON public.forecast_cliente USING btree (cliente_grupo)`
- `ix_forecast_cliente_codigo` [codigo]
  - `CREATE INDEX ix_forecast_cliente_codigo ON public.forecast_cliente USING btree (codigo)`
- `ix_forecast_cliente_id` [id]
  - `CREATE INDEX ix_forecast_cliente_id ON public.forecast_cliente USING btree (id)`
- `ix_forecast_cliente_nombre_grupo` [nombre_grupo]
  - `CREATE INDEX ix_forecast_cliente_nombre_grupo ON public.forecast_cliente USING btree (nombre_grupo)`
- `ix_forecast_cliente_perfil` [perfil]
  - `CREATE INDEX ix_forecast_cliente_perfil ON public.forecast_cliente USING btree (perfil)`

### `forecast_dataset_base` — 221,424 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `codigo_serie` | VARCHAR(100) | True |
| `perfil` | VARCHAR(50) | True |
| `qty_mes` | DOUBLE PRECISION | True |
| `periodo` | VARCHAR(20) | True |
| `nivel_agregacion` | VARCHAR(50) | True |
| `neg` | INTEGER | True |
| `subneg` | INTEGER | True |
| `familia` | VARCHAR(200) | True |
| `fecha` | TIMESTAMP | True |

**PK:** id

**FK:** —

**Índices:**
- `ix_forecast_dataset_base_codigo_serie` [codigo_serie]
  - `CREATE INDEX ix_forecast_dataset_base_codigo_serie ON public.forecast_dataset_base USING btree (codigo_serie)`
- `ix_forecast_dataset_base_fecha` [fecha]
  - `CREATE INDEX ix_forecast_dataset_base_fecha ON public.forecast_dataset_base USING btree (fecha)`
- `ix_forecast_dataset_base_id` [id]
  - `CREATE INDEX ix_forecast_dataset_base_id ON public.forecast_dataset_base USING btree (id)`
- `ix_forecast_dataset_base_neg` [neg]
  - `CREATE INDEX ix_forecast_dataset_base_neg ON public.forecast_dataset_base USING btree (neg)`
- `ix_forecast_dataset_base_perfil` [perfil]
  - `CREATE INDEX ix_forecast_dataset_base_perfil ON public.forecast_dataset_base USING btree (perfil)`
- `ix_forecast_dataset_base_periodo` [periodo]
  - `CREATE INDEX ix_forecast_dataset_base_periodo ON public.forecast_dataset_base USING btree (periodo)`
- `ix_forecast_dataset_base_subneg` [subneg]
  - `CREATE INDEX ix_forecast_dataset_base_subneg ON public.forecast_dataset_base USING btree (subneg)`

### `forecast_fact_2026` — 206,246 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `fecha` | TIMESTAMP | True |
| `codigo_serie` | TEXT | True |
| `perfil` | TEXT | True |
| `cliente_id` | TEXT | True |
| `familia` | TEXT | True |
| `descripcion` | TEXT | True |
| `nivel_agregacion` | TEXT | True |
| `articulo_codigo` | TEXT | True |
| `y` | TEXT | True |
| `imp_hist` | DOUBLE PRECISION | True |
| `tipo` | TEXT | True |
| `tipocli` | TEXT | True |

**PK:** —

**FK:** —

**Índices:**
- `idx_ff26_cliente` [cliente_id]
  - `CREATE INDEX idx_ff26_cliente ON public.forecast_fact_2026 USING btree (cliente_id)`
- `idx_ff26_fecha` [fecha]
  - `CREATE INDEX idx_ff26_fecha ON public.forecast_fact_2026 USING btree (fecha)`
- `idx_ff26_tipocli` [tipocli]
  - `CREATE INDEX idx_ff26_tipocli ON public.forecast_fact_2026 USING btree (tipocli)`
- `ix_fc_fact2026_cliente_fecha` [cliente_id, fecha]
  - `CREATE INDEX ix_fc_fact2026_cliente_fecha ON public.forecast_fact_2026 USING btree (cliente_id, fecha)`
- `ix_fc_fact2026_codigo_fecha` [codigo_serie, fecha]
  - `CREATE INDEX ix_fc_fact2026_codigo_fecha ON public.forecast_fact_2026 USING btree (codigo_serie, fecha)`

### `forecast_imp_hist` — 44,861 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `periodo` | TEXT | True |
| `codigo_serie` | TEXT | True |
| `perfil` | TEXT | True |
| `imp_hist` | DOUBLE PRECISION | True |
| `tipo` | TEXT | True |
| `fecha` | TIMESTAMP | True |

**PK:** —

**FK:** —

**Índices:**
- `idx_fc_hist_perfil` [perfil]
  - `CREATE INDEX idx_fc_hist_perfil ON public.forecast_imp_hist USING btree (perfil)`
- `ix_fc_imp_hist_codigo_fecha` [codigo_serie, fecha]
  - `CREATE INDEX ix_fc_imp_hist_codigo_fecha ON public.forecast_imp_hist USING btree (codigo_serie, fecha)`
- `ix_fc_imp_hist_perfil_fecha` [perfil, fecha]
  - `CREATE INDEX ix_fc_imp_hist_perfil_fecha ON public.forecast_imp_hist USING btree (perfil, fecha)`

### `forecast_main` — 277,452 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `periodo` | TEXT | True |
| `codigo_serie` | TEXT | True |
| `nivel_agregacion` | TEXT | True |
| `perfil` | TEXT | True |
| `neg` | TEXT | True |
| `subneg` | TEXT | True |
| `familia_x` | TEXT | True |
| `tipo` | TEXT | True |
| `y` | TEXT | True |
| `yhat` | TEXT | True |
| `li` | TEXT | True |
| `ls` | TEXT | True |
| `submodelo` | TEXT | True |
| `clasificacion_serie` | TEXT | True |
| `version_param` | TEXT | True |
| `articulo` | TEXT | True |
| `codigo` | TEXT | True |
| `descrip` | TEXT | True |
| `predrog` | TEXT | True |
| `cantenv` | TEXT | True |
| `laboratorio_descrip` | TEXT | True |
| `familia_y` | TEXT | True |
| `unineg` | TEXT | True |
| `sunineg` | TEXT | True |
| `descripcion` | TEXT | True |
| `fecha` | TIMESTAMP | True |
| `precio` | DOUBLE PRECISION | True |

**PK:** —

**FK:** —

**Índices:**
- `idx_fc_main_codigo_serie` [codigo_serie]
  - `CREATE INDEX idx_fc_main_codigo_serie ON public.forecast_main USING btree (codigo_serie)`
- `idx_fc_main_neg` [neg]
  - `CREATE INDEX idx_fc_main_neg ON public.forecast_main USING btree (neg)`
- `idx_fc_main_perfil` [perfil]
  - `CREATE INDEX idx_fc_main_perfil ON public.forecast_main USING btree (perfil)`
- `idx_fc_main_subneg` [subneg]
  - `CREATE INDEX idx_fc_main_subneg ON public.forecast_main USING btree (subneg)`
- `ix_fc_main_codigo_serie` [codigo_serie]
  - `CREATE INDEX ix_fc_main_codigo_serie ON public.forecast_main USING btree (codigo_serie)`
- `ix_fc_main_fecha` [fecha]
  - `CREATE INDEX ix_fc_main_fecha ON public.forecast_main USING btree (fecha)`
- `ix_fc_main_perfil` [perfil]
  - `CREATE INDEX ix_fc_main_perfil ON public.forecast_main USING btree (perfil)`
- `ix_fc_main_tipo` [tipo]
  - `CREATE INDEX ix_fc_main_tipo ON public.forecast_main USING btree (tipo)`
- `ix_fc_main_tipo_fecha` [tipo, fecha]
  - `CREATE INDEX ix_fc_main_tipo_fecha ON public.forecast_main USING btree (tipo, fecha)`

### `forecast_manual_clients` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `nombre_cliente` | VARCHAR(255) | False |
| `grupo` | VARCHAR(255) | True |
| `is_active` | BOOLEAN | False |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |
| `created_by` | VARCHAR(255) | True |
| `deleted_at` | TIMESTAMP | True |
| `deleted_by` | VARCHAR(255) | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_forecast_manual_clients_user_id` [user_id]
  - `CREATE INDEX ix_forecast_manual_clients_user_id ON public.forecast_manual_clients USING btree (user_id)`

### `forecast_manual_entries` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `client_id` | INTEGER | False |
| `neg` | VARCHAR(120) | False |
| `subneg` | VARCHAR(255) | False |
| `codigo_serie` | VARCHAR(120) | False |
| `descripcion` | VARCHAR(255) | True |
| `unidad_medida` | VARCHAR(50) | True |
| `forecast_month` | VARCHAR(7) | False |
| `cantidad` | DOUBLE PRECISION | False |
| `costo_unitario` | DOUBLE PRECISION | False |
| `monto_total` | DOUBLE PRECISION | False |
| `is_active` | BOOLEAN | False |
| `deleted_at` | TIMESTAMP | True |
| `deleted_by` | VARCHAR(255) | True |
| `perfil` | VARCHAR(120) | True |

**PK:** id

**FK:**
- (client_id) → `forecast_manual_clients` (id)

**Índices:**
- `ix_forecast_manual_entries_client_id` [client_id]
  - `CREATE INDEX ix_forecast_manual_entries_client_id ON public.forecast_manual_entries USING btree (client_id)`

### `forecast_negocio` — 145 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `unidad` | INTEGER | True |
| `subunidad` | INTEGER | True |
| `descrip` | VARCHAR(200) | True |

**PK:** id

**FK:** —

**Índices:**
- `ix_forecast_negocio_id` [id]
  - `CREATE INDEX ix_forecast_negocio_id ON public.forecast_negocio USING btree (id)`
- `ix_forecast_negocio_subunidad` [subunidad]
  - `CREATE INDEX ix_forecast_negocio_subunidad ON public.forecast_negocio USING btree (subunidad)`
- `ix_forecast_negocio_unidad` [unidad]
  - `CREATE INDEX ix_forecast_negocio_unidad ON public.forecast_negocio USING btree (unidad)`

### `forecast_product_labs` — 2,914 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `codigo_serie` | TEXT | True |
| `laboratorios` | TEXT | True |

**PK:** —

**FK:** —

**Índices:**
- `idx_fc_labs_cdg` [codigo_serie]
  - `CREATE INDEX idx_fc_labs_cdg ON public.forecast_product_labs USING btree (codigo_serie)`
- `ix_fc_labs_codigo` [codigo_serie]
  - `CREATE INDEX ix_fc_labs_codigo ON public.forecast_product_labs USING btree (codigo_serie)`

### `forecast_user_overrides` — 3,771 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `source_module` | VARCHAR(50) | False |
| `context_key` | VARCHAR(120) | False |
| `client_selector` | VARCHAR(255) | False |
| `client_display` | VARCHAR(255) | True |
| `perfil` | VARCHAR(120) | True |
| `neg` | VARCHAR(120) | True |
| `subneg` | VARCHAR(255) | False |
| `codigo_serie` | VARCHAR(120) | False |
| `forecast_month` | VARCHAR(7) | False |
| `override_scope` | VARCHAR(20) | False |
| `base_growth_pct` | DOUBLE PRECISION | True |
| `override_growth_pct` | DOUBLE PRECISION | True |
| `effective_monthly_pct` | DOUBLE PRECISION | True |
| `is_active` | BOOLEAN | False |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |
| `created_by` | VARCHAR(255) | True |
| `updated_by` | VARCHAR(255) | True |
| `effective_from_month` | VARCHAR(7) | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_fc_override_context_lookup` [source_module, context_key, user_id, updated_at]
  - `CREATE INDEX ix_fc_override_context_lookup ON public.forecast_user_overrides USING btree (source_module, context_key, user_id, updated_at)`
- `ix_fc_override_scope_lookup` [user_id, source_module, override_scope, client_selector, subneg, codigo_serie, forecast_month, is_active]
  - `CREATE INDEX ix_fc_override_scope_lookup ON public.forecast_user_overrides USING btree (user_id, source_module, override_scope, client_selector, subneg, codigo_serie, forecast_month, is_active)`
- `ix_fc_override_user_client_active` [user_id, source_module, client_selector, is_active]
  - `CREATE INDEX ix_fc_override_user_client_active ON public.forecast_user_overrides USING btree (user_id, source_module, client_selector, is_active)`
- `ix_forecast_user_overrides_client_selector` [client_selector]
  - `CREATE INDEX ix_forecast_user_overrides_client_selector ON public.forecast_user_overrides USING btree (client_selector)`
- `ix_forecast_user_overrides_context_key` [context_key]
  - `CREATE INDEX ix_forecast_user_overrides_context_key ON public.forecast_user_overrides USING btree (context_key)`
- `ix_forecast_user_overrides_created_at` [created_at]
  - `CREATE INDEX ix_forecast_user_overrides_created_at ON public.forecast_user_overrides USING btree (created_at)`
- `ix_forecast_user_overrides_is_active` [is_active]
  - `CREATE INDEX ix_forecast_user_overrides_is_active ON public.forecast_user_overrides USING btree (is_active)`
- `ix_forecast_user_overrides_override_scope` [override_scope]
  - `CREATE INDEX ix_forecast_user_overrides_override_scope ON public.forecast_user_overrides USING btree (override_scope)`
- `ix_forecast_user_overrides_source_module` [source_module]
  - `CREATE INDEX ix_forecast_user_overrides_source_module ON public.forecast_user_overrides USING btree (source_module)`
- `ix_forecast_user_overrides_updated_at` [updated_at]
  - `CREATE INDEX ix_forecast_user_overrides_updated_at ON public.forecast_user_overrides USING btree (updated_at)`
- `ix_forecast_user_overrides_user_id` [user_id]
  - `CREATE INDEX ix_forecast_user_overrides_user_id ON public.forecast_user_overrides USING btree (user_id)`
- `uq_forecast_user_override_scope` [user_id, source_module, context_key, client_selector, override_scope, subneg, codigo_serie, forecast_month] (unique)
  - `CREATE UNIQUE INDEX uq_forecast_user_override_scope ON public.forecast_user_overrides USING btree (user_id, source_module, context_key, client_selector, override_scope, subneg, codigo_serie, forecast_month)`

### `forecast_valorizado` — 702,436 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `periodo` | TEXT | True |
| `fecha` | TIMESTAMP | True |
| `codigo_serie` | TEXT | True |
| `perfil` | TEXT | True |
| `cliente_id` | TEXT | True |
| `yhat_cliente` | BIGINT | True |
| `li_cliente` | BIGINT | True |
| `ls_cliente` | BIGINT | True |
| `monto_yhat` | DOUBLE PRECISION | True |
| `monto_li` | DOUBLE PRECISION | True |
| `monto_ls` | DOUBLE PRECISION | True |
| `nivel_agregacion` | TEXT | True |
| `descripcion` | TEXT | True |
| `clasificacion_serie` | TEXT | True |
| `fantasia` | TEXT | True |
| `nombre_grupo` | TEXT | True |
| `neg` | TEXT | True |
| `subneg` | TEXT | True |

**PK:** —

**FK:** —

**Índices:**
- `idx_fc_val_cliente_id` [cliente_id]
  - `CREATE INDEX idx_fc_val_cliente_id ON public.forecast_valorizado USING btree (cliente_id)`
- `idx_fc_val_codigo_serie` [codigo_serie]
  - `CREATE INDEX idx_fc_val_codigo_serie ON public.forecast_valorizado USING btree (codigo_serie)`
- `idx_fc_val_fecha` [fecha]
  - `CREATE INDEX idx_fc_val_fecha ON public.forecast_valorizado USING btree (fecha)`
- `idx_fc_val_perfil` [perfil]
  - `CREATE INDEX idx_fc_val_perfil ON public.forecast_valorizado USING btree (perfil)`
- `ix_fc_val_cliente_id` [cliente_id]
  - `CREATE INDEX ix_fc_val_cliente_id ON public.forecast_valorizado USING btree (cliente_id)`
- `ix_fc_val_codigo_fecha` [codigo_serie, fecha]
  - `CREATE INDEX ix_fc_val_codigo_fecha ON public.forecast_valorizado USING btree (codigo_serie, fecha)`
- `ix_fc_val_fantasia` [fantasia]
  - `CREATE INDEX ix_fc_val_fantasia ON public.forecast_valorizado USING btree (fantasia)`
- `ix_fc_val_fantasia_filters` [fantasia, perfil, neg, subneg, fecha]
  - `CREATE INDEX ix_fc_val_fantasia_filters ON public.forecast_valorizado USING btree (fantasia, perfil, neg, subneg, fecha)`
- `ix_fc_val_fecha` [fecha]
  - `CREATE INDEX ix_fc_val_fecha ON public.forecast_valorizado USING btree (fecha)`
- `ix_fc_val_filters_fecha` [perfil, neg, subneg, fecha]
  - `CREATE INDEX ix_fc_val_filters_fecha ON public.forecast_valorizado USING btree (perfil, neg, subneg, fecha)`
- `ix_fc_val_perfil_neg_subneg` [perfil, neg, subneg]
  - `CREATE INDEX ix_fc_val_perfil_neg_subneg ON public.forecast_valorizado USING btree (perfil, neg, subneg)`

### `group_members` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `group_id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `role_in_group` | VARCHAR(32) | False |
| `added_by_user_id` | INTEGER | True |
| `added_at` | TIMESTAMP | False |

**PK:** id

**FK:**
- (added_by_user_id) → `users` (id)
- (group_id) → `groups` (id)
- (user_id) → `users` (id)

**Índices:**
- `ix_group_members_added_at` [added_at]
  - `CREATE INDEX ix_group_members_added_at ON public.group_members USING btree (added_at)`
- `ix_group_members_added_by_user_id` [added_by_user_id]
  - `CREATE INDEX ix_group_members_added_by_user_id ON public.group_members USING btree (added_by_user_id)`
- `ix_group_members_group_id` [group_id]
  - `CREATE INDEX ix_group_members_group_id ON public.group_members USING btree (group_id)`
- `ix_group_members_role_in_group` [role_in_group]
  - `CREATE INDEX ix_group_members_role_in_group ON public.group_members USING btree (role_in_group)`
- `ix_group_members_user_id` [user_id]
  - `CREATE INDEX ix_group_members_user_id ON public.group_members USING btree (user_id)`
- `uq_group_user` [group_id, user_id] (unique)
  - `CREATE UNIQUE INDEX uq_group_user ON public.group_members USING btree (group_id, user_id)`

### `groups` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `name` | VARCHAR(120) | False |
| `business_unit` | VARCHAR(120) | True |
| `created_by_user_id` | INTEGER | False |
| `created_at` | TIMESTAMP | False |

**PK:** id

**FK:**
- (created_by_user_id) → `users` (id)

**Índices:**
- `ix_groups_business_unit` [business_unit]
  - `CREATE INDEX ix_groups_business_unit ON public.groups USING btree (business_unit)`
- `ix_groups_created_at` [created_at]
  - `CREATE INDEX ix_groups_created_at ON public.groups USING btree (created_at)`
- `ix_groups_created_by_user_id` [created_by_user_id]
  - `CREATE INDEX ix_groups_created_by_user_id ON public.groups USING btree (created_by_user_id)`
- `ix_groups_name` [name] (unique)
  - `CREATE UNIQUE INDEX ix_groups_name ON public.groups USING btree (name)`

### `normalized_files` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | False |
| `path` | VARCHAR | True |
| `row_count` | INTEGER | True |
| `checksum` | VARCHAR | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:**
- `ix_normalized_files_upload_id` [upload_id]
  - `CREATE INDEX ix_normalized_files_upload_id ON public.normalized_files USING btree (upload_id)`

### `notifications` — 115 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `title` | VARCHAR(255) | False |
| `message` | TEXT | False |
| `category` | VARCHAR(50) | True |
| `link` | VARCHAR(500) | True |
| `is_read` | BOOLEAN | False |
| `created_at` | TIMESTAMP | False |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_notifications_category` [category]
  - `CREATE INDEX ix_notifications_category ON public.notifications USING btree (category)`
- `ix_notifications_created_at` [created_at]
  - `CREATE INDEX ix_notifications_created_at ON public.notifications USING btree (created_at)`
- `ix_notifications_is_read` [is_read]
  - `CREATE INDEX ix_notifications_is_read ON public.notifications USING btree (is_read)`
- `ix_notifications_user_id` [user_id]
  - `CREATE INDEX ix_notifications_user_id ON public.notifications USING btree (user_id)`

### `order_items` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `order_id` | VARCHAR(32) | False |
| `fecha` | VARCHAR(40) | False |
| `razon_social` | VARCHAR(255) | False |
| `codigo_articulo` | VARCHAR(80) | False |
| `articulo` | VARCHAR(255) | False |
| `precio_lista` | NUMERIC(12, 2) | False |
| `precio_unitario` | NUMERIC(12, 2) | False |
| `cantidad` | INTEGER | False |
| `total` | NUMERIC(12, 2) | False |
| `estado` | VARCHAR(80) | False |

**PK:** id

**FK:**
- (order_id) → `orders` (order_id)

**Índices:**
- `order_items_order_id_index` [order_id]
  - `CREATE INDEX order_items_order_id_index ON public.order_items USING btree (order_id)`

### `orders` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `order_id` | VARCHAR(32) | False |
| `fecha` | VARCHAR(40) | False |
| `numero_cliente` | VARCHAR(80) | False |
| `razon_social` | VARCHAR(255) | False |
| `cuit` | VARCHAR(80) | False |
| `direccion` | VARCHAR(255) | False |
| `ciudad` | VARCHAR(120) | False |
| `provincia` | VARCHAR(120) | False |
| `codigo_postal` | VARCHAR(40) | False |
| `franja_horaria` | VARCHAR(120) | False |
| `mail` | VARCHAR(160) | False |
| `celular` | VARCHAR(80) | False |
| `observaciones` | TEXT | True |
| `estado` | VARCHAR(80) | False |
| `usuario` | VARCHAR(64) | False |
| `productos_json` | TEXT | False |

**PK:** order_id

**FK:** —

**Índices:** —

### `password_reset_requests` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_email` | VARCHAR(255) | False |
| `full_name` | VARCHAR(255) | False |
| `department` | VARCHAR(120) | True |
| `comment` | TEXT | True |
| `request_date` | TIMESTAMP | False |
| `status` | VARCHAR(30) | False |
| `handled_by` | VARCHAR(255) | True |
| `handled_date` | TIMESTAMP | True |
| `admin_observation` | TEXT | True |
| `temporary_password_generated` | BOOLEAN | False |
| `must_change_password_on_next_login` | BOOLEAN | False |

**PK:** id

**FK:** —

**Índices:**
- `ix_password_reset_requests_request_date` [request_date]
  - `CREATE INDEX ix_password_reset_requests_request_date ON public.password_reset_requests USING btree (request_date)`
- `ix_password_reset_requests_status` [status]
  - `CREATE INDEX ix_password_reset_requests_status ON public.password_reset_requests USING btree (status)`
- `ix_password_reset_requests_user_email` [user_email]
  - `CREATE INDEX ix_password_reset_requests_user_email ON public.password_reset_requests USING btree (user_email)`

### `pliego_actos_admin` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `tipo_acto` | VARCHAR | True |
| `numero` | VARCHAR | True |
| `numero_especial` | VARCHAR | True |
| `fecha` | VARCHAR | True |
| `organismo_emisor` | VARCHAR | True |
| `descripcion` | TEXT | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_actos_admin_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_actos_admin_solicitud_id ON public.pliego_actos_admin USING btree (solicitud_id)`

### `pliego_analitica` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `datos` | JSON | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_analitica_solicitud_id` [solicitud_id] (unique)
  - `CREATE UNIQUE INDEX ix_pliego_analitica_solicitud_id ON public.pliego_analitica USING btree (solicitud_id)`

### `pliego_archivos` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `nombre_original` | VARCHAR | False |
| `nombre_guardado` | VARCHAR | False |
| `tipo_mime` | VARCHAR | True |
| `tamano_bytes` | INTEGER | True |
| `url_path` | VARCHAR | True |
| `creado_en` | TIMESTAMP | True |
| `contenido_bytes` | BYTEA | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_archivos_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_archivos_solicitud_id ON public.pliego_archivos USING btree (solicitud_id)`

### `pliego_control_carga` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `datos` | JSON | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_control_carga_solicitud_id` [solicitud_id] (unique)
  - `CREATE UNIQUE INDEX ix_pliego_control_carga_solicitud_id ON public.pliego_control_carga USING btree (solicitud_id)`

### `pliego_cronograma` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `hito` | VARCHAR | True |
| `fecha` | VARCHAR | True |
| `hora` | VARCHAR | True |
| `lugar_medio` | VARCHAR | True |
| `estado_dato` | VARCHAR | True |
| `fuente` | VARCHAR | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_cronograma_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_cronograma_solicitud_id ON public.pliego_cronograma USING btree (solicitud_id)`

### `pliego_documentos` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `nombre` | VARCHAR | True |
| `tipo` | VARCHAR | True |
| `rol` | VARCHAR | True |
| `obligatorio` | VARCHAR | True |
| `estado_lectura` | VARCHAR | True |
| `fecha` | VARCHAR | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_documentos_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_documentos_solicitud_id ON public.pliego_documentos USING btree (solicitud_id)`

### `pliego_edit_history` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `entity_type` | VARCHAR(64) | False |
| `entity_id` | INTEGER | True |
| `section_key` | VARCHAR(128) | True |
| `field_key` | VARCHAR(256) | False |
| `field_label` | VARCHAR(256) | True |
| `old_value` | TEXT | True |
| `new_value` | TEXT | True |
| `old_status` | VARCHAR(64) | True |
| `new_status` | VARCHAR(64) | True |
| `edited_by_user_id` | INTEGER | True |
| `edited_by_name` | VARCHAR(256) | True |
| `edited_by_role` | VARCHAR(64) | True |
| `edited_at` | TIMESTAMP | True |
| `reason` | TEXT | True |

**PK:** id

**FK:**
- (edited_by_user_id) → `users` (id)
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_edit_history_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_edit_history_solicitud_id ON public.pliego_edit_history USING btree (solicitud_id)`

### `pliego_excel_cargas` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `nombre_archivo` | VARCHAR | False |
| `version` | INTEGER | True |
| `url_path` | VARCHAR | True |
| `cargado_por_id` | INTEGER | True |
| `creado_en` | TIMESTAMP | True |
| `es_activa` | BOOLEAN | True |
| `observaciones` | TEXT | True |
| `contenido_bytes` | BYTEA | True |

**PK:** id

**FK:**
- (cargado_por_id) → `users` (id)
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_excel_cargas_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_excel_cargas_solicitud_id ON public.pliego_excel_cargas USING btree (solicitud_id)`

### `pliego_faltantes` — 156 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `campo_objetivo` | VARCHAR | True |
| `motivo` | VARCHAR | True |
| `detalle` | TEXT | True |
| `criticidad` | VARCHAR | True |
| `accion_recomendada` | TEXT | True |
| `estado` | VARCHAR | True |
| `fuente` | VARCHAR | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_faltantes_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_faltantes_solicitud_id ON public.pliego_faltantes USING btree (solicitud_id)`

### `pliego_field_overrides` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `entity_type` | VARCHAR(64) | False |
| `entity_id` | INTEGER | True |
| `section_key` | VARCHAR(128) | True |
| `field_key` | VARCHAR(256) | False |
| `field_label` | VARCHAR(256) | True |
| `original_value` | TEXT | True |
| `edited_value` | TEXT | True |
| `original_status` | VARCHAR(64) | True |
| `edited_status` | VARCHAR(64) | True |
| `edited_by_user_id` | INTEGER | True |
| `edited_by_name` | VARCHAR(256) | True |
| `edited_by_role` | VARCHAR(64) | True |
| `edited_at` | TIMESTAMP | True |
| `reason` | TEXT | True |
| `is_active` | BOOLEAN | False |

**PK:** id

**FK:**
- (edited_by_user_id) → `users` (id)
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pfo_pliego_entity` [solicitud_id, entity_type, entity_id, field_key]
  - `CREATE INDEX ix_pfo_pliego_entity ON public.pliego_field_overrides USING btree (solicitud_id, entity_type, entity_id, field_key)`
- `ix_pliego_field_overrides_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_field_overrides_solicitud_id ON public.pliego_field_overrides USING btree (solicitud_id)`

### `pliego_fusion_cabecera` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `datos` | JSON | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_fusion_cabecera_solicitud_id` [solicitud_id] (unique)
  - `CREATE UNIQUE INDEX ix_pliego_fusion_cabecera_solicitud_id ON public.pliego_fusion_cabecera USING btree (solicitud_id)`

### `pliego_fusion_renglones` — 1,704 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `numero_renglon` | VARCHAR | True |
| `codigo_item` | VARCHAR | True |
| `descripcion` | TEXT | True |
| `cantidad` | VARCHAR | True |
| `unidad` | VARCHAR | True |
| `precio_unitario_estimado` | VARCHAR | True |
| `datos_extra` | JSON | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_fusion_renglones_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_fusion_renglones_solicitud_id ON public.pliego_fusion_renglones USING btree (solicitud_id)`

### `pliego_garantias` — 70 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `tipo` | VARCHAR | True |
| `requerida` | VARCHAR | True |
| `porcentaje` | VARCHAR | True |
| `base_calculo` | VARCHAR | True |
| `plazo` | VARCHAR | True |
| `formas_admitidas` | TEXT | True |
| `estado_dato` | VARCHAR | True |
| `fuente` | VARCHAR | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_garantias_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_garantias_solicitud_id ON public.pliego_garantias USING btree (solicitud_id)`

### `pliego_hallazgos` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `categoria` | VARCHAR | True |
| `hallazgo` | TEXT | True |
| `impacto` | VARCHAR | True |
| `accion_sugerida` | TEXT | True |
| `fuente` | VARCHAR | True |
| `datos_extra` | JSONB | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_hallazgos_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_hallazgos_solicitud_id ON public.pliego_hallazgos USING btree (solicitud_id)`

### `pliego_historial` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `estado_anterior` | VARCHAR | True |
| `estado_nuevo` | VARCHAR | False |
| `comentario` | TEXT | True |
| `usuario_id` | INTEGER | True |
| `creado_en` | TIMESTAMP | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)
- (usuario_id) → `users` (id)

**Índices:**
- `ix_pliego_historial_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_historial_solicitud_id ON public.pliego_historial USING btree (solicitud_id)`

### `pliego_proceso` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `datos` | JSON | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_proceso_solicitud_id` [solicitud_id] (unique)
  - `CREATE UNIQUE INDEX ix_pliego_proceso_solicitud_id ON public.pliego_proceso USING btree (solicitud_id)`

### `pliego_renglones` — 2,009 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `orden` | INTEGER | True |
| `numero_renglon` | VARCHAR | True |
| `codigo_item` | VARCHAR | True |
| `descripcion` | TEXT | True |
| `cantidad` | VARCHAR | True |
| `unidad` | VARCHAR | True |
| `destino_efector` | VARCHAR | True |
| `entrega_parcial` | VARCHAR | True |
| `obs_tecnicas` | TEXT | True |
| `estado` | VARCHAR | True |
| `datos_extra` | JSON | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_renglones_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_renglones_solicitud_id ON public.pliego_renglones USING btree (solicitud_id)`

### `pliego_requisitos` — 244 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `categoria` | VARCHAR | True |
| `descripcion` | TEXT | True |
| `obligatorio` | VARCHAR | True |
| `momento_presentacion` | VARCHAR | True |
| `medio_presentacion` | VARCHAR | True |
| `vigencia` | VARCHAR | True |
| `estado_dato` | VARCHAR | True |
| `fuente` | VARCHAR | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_requisitos_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_requisitos_solicitud_id ON public.pliego_requisitos USING btree (solicitud_id)`

### `pliego_solicitudes` — 17 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `titulo` | VARCHAR | False |
| `organismo` | VARCHAR | True |
| `nombre_licitacion` | VARCHAR | True |
| `numero_proceso` | VARCHAR | True |
| `expediente` | VARCHAR | True |
| `observaciones_usuario` | TEXT | True |
| `estado` | VARCHAR | True |
| `creado_por_id` | INTEGER | False |
| `admin_responsable_id` | INTEGER | True |
| `creado_en` | TIMESTAMP | True |
| `actualizado_en` | TIMESTAMP | True |
| `publicado_en` | TIMESTAMP | True |
| `enviado_a_gpt_en` | TIMESTAMP | True |
| `enviado_a_gpt_por_id` | INTEGER | True |
| `procesado_externamente_en` | TIMESTAMP | True |
| `observaciones_procesamiento` | TEXT | True |
| `observaciones_admin` | TEXT | True |
| `client_request_id` | VARCHAR(64) | True |
| `deleted_at` | TIMESTAMP | True |
| `deleted_by_id` | INTEGER | True |

**PK:** id

**FK:**
- (admin_responsable_id) → `users` (id)
- (creado_por_id) → `users` (id)
- (deleted_by_id) → `users` (id)
- (enviado_a_gpt_por_id) → `users` (id)

**Índices:**
- `ix_pliego_solicitudes_deleted_at` [deleted_at]
  - `CREATE INDEX ix_pliego_solicitudes_deleted_at ON public.pliego_solicitudes USING btree (deleted_at)`
- `ix_pliego_solicitudes_estado` [estado]
  - `CREATE INDEX ix_pliego_solicitudes_estado ON public.pliego_solicitudes USING btree (estado)`
- `ix_pliego_solicitudes_id` [id]
  - `CREATE INDEX ix_pliego_solicitudes_id ON public.pliego_solicitudes USING btree (id)`
- `uq_pliego_solicitud_client_request_id` [client_request_id] (unique)
  - `CREATE UNIQUE INDEX uq_pliego_solicitud_client_request_id ON public.pliego_solicitudes USING btree (client_request_id)`

### `pliego_trazabilidad` — 409 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `campo` | VARCHAR | True |
| `valor_extraido` | TEXT | True |
| `documento_fuente` | VARCHAR | True |
| `pagina_seccion` | VARCHAR | True |
| `tipo_evidencia` | VARCHAR | True |
| `observacion` | TEXT | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_trazabilidad_solicitud_id` [solicitud_id]
  - `CREATE INDEX ix_pliego_trazabilidad_solicitud_id ON public.pliego_trazabilidad USING btree (solicitud_id)`

### `products` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `codigo` | VARCHAR(64) | True |
| `nombre` | VARCHAR(255) | False |
| `importe` | NUMERIC(12, 2) | False |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |

**PK:** id

**FK:** —

**Índices:**
- `products_codigo_key` [codigo] (unique)
  - `CREATE UNIQUE INDEX products_codigo_key ON public.products USING btree (codigo)`
- `products_nombre_index` [nombre]
  - `CREATE INDEX products_nombre_index ON public.products USING btree (nombre)`

### `revision_sessions` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `tender_id` | VARCHAR | False |
| `user_id` | INTEGER | False |
| `field_path` | VARCHAR | False |
| `original_value` | VARCHAR | True |
| `corrected_value` | VARCHAR | True |
| `confidence_at_revision` | DOUBLE PRECISION | True |
| `created_at` | TIMESTAMP | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_revision_sessions_id` [id]
  - `CREATE INDEX ix_revision_sessions_id ON public.revision_sessions USING btree (id)`
- `ix_revision_sessions_tender_id` [tender_id]
  - `CREATE INDEX ix_revision_sessions_tender_id ON public.revision_sessions USING btree (tender_id)`
- `ix_revision_sessions_user_id` [user_id]
  - `CREATE INDEX ix_revision_sessions_user_id ON public.revision_sessions USING btree (user_id)`

### `runs` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | False |
| `status` | VARCHAR | True |
| `started_at` | TIMESTAMP | True |
| `ended_at` | TIMESTAMP | True |
| `logs_path` | VARCHAR | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:**
- `ix_runs_ended_at` [ended_at]
  - `CREATE INDEX ix_runs_ended_at ON public.runs USING btree (ended_at)`
- `ix_runs_started_at` [started_at]
  - `CREATE INDEX ix_runs_started_at ON public.runs USING btree (started_at)`
- `ix_runs_status` [status]
  - `CREATE INDEX ix_runs_status ON public.runs USING btree (status)`
- `ix_runs_upload_id` [upload_id]
  - `CREATE INDEX ix_runs_upload_id ON public.runs USING btree (upload_id)`

### `saved_views` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `view_id` | VARCHAR(64) | False |
| `name` | VARCHAR(120) | False |
| `is_default` | BOOLEAN | False |
| `payload` | JSON | False |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `idx_savedviews_user` [user_id]
  - `CREATE INDEX idx_savedviews_user ON public.saved_views USING btree (user_id)`
- `idx_savedviews_user_view_default` [user_id, view_id, is_default]
  - `CREATE INDEX idx_savedviews_user_view_default ON public.saved_views USING btree (user_id, view_id, is_default)`
- `idx_savedviews_viewid` [view_id]
  - `CREATE INDEX idx_savedviews_viewid ON public.saved_views USING btree (view_id)`
- `ix_saved_views_created_at` [created_at]
  - `CREATE INDEX ix_saved_views_created_at ON public.saved_views USING btree (created_at)`
- `ix_saved_views_is_default` [is_default]
  - `CREATE INDEX ix_saved_views_is_default ON public.saved_views USING btree (is_default)`
- `ix_saved_views_updated_at` [updated_at]
  - `CREATE INDEX ix_saved_views_updated_at ON public.saved_views USING btree (updated_at)`
- `ix_saved_views_user_id` [user_id]
  - `CREATE INDEX ix_saved_views_user_id ON public.saved_views USING btree (user_id)`
- `ix_saved_views_view_id` [view_id]
  - `CREATE INDEX ix_saved_views_view_id ON public.saved_views USING btree (view_id)`
- `uq_savedview_user_view_name` [user_id, view_id, name] (unique)
  - `CREATE UNIQUE INDEX uq_savedview_user_view_name ON public.saved_views USING btree (user_id, view_id, name)`

### `ticket_messages` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `ticket_id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `message` | TEXT | False |
| `is_internal` | BOOLEAN | True |
| `created_at` | TIMESTAMP | True |

**PK:** id

**FK:**
- (ticket_id) → `tickets` (id)
- (user_id) → `users` (id)

**Índices:**
- `ix_ticket_messages_id` [id]
  - `CREATE INDEX ix_ticket_messages_id ON public.ticket_messages USING btree (id)`
- `ix_ticket_messages_ticket_id` [ticket_id]
  - `CREATE INDEX ix_ticket_messages_ticket_id ON public.ticket_messages USING btree (ticket_id)`

### `tickets` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `title` | VARCHAR | False |
| `category` | VARCHAR | True |
| `priority` | VARCHAR | True |
| `status` | VARCHAR | True |
| `created_at` | TIMESTAMP | True |
| `updated_at` | TIMESTAMP | True |
| `modulo_origen` | VARCHAR(50) | True |
| `pliego_solicitud_id` | INTEGER | True |
| `contexto_extra` | TEXT | True |

**PK:** id

**FK:**
- (pliego_solicitud_id) → `pliego_solicitudes` (id)
- (user_id) → `users` (id)

**Índices:**
- `ix_tickets_id` [id]
  - `CREATE INDEX ix_tickets_id ON public.tickets USING btree (id)`
- `ix_tickets_user_id` [user_id]
  - `CREATE INDEX ix_tickets_user_id ON public.tickets USING btree (user_id)`

### `uploads` — 29 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | True |
| `uploaded_by_name` | VARCHAR(120) | True |
| `uploaded_by_email` | VARCHAR(255) | True |
| `proceso_nro` | VARCHAR | True |
| `proceso_key` | VARCHAR | True |
| `apertura_fecha` | VARCHAR | True |
| `cuenta_nro` | VARCHAR | True |
| `platform_hint` | VARCHAR | True |
| `buyer_hint` | VARCHAR | True |
| `province_hint` | VARCHAR | True |
| `original_filename` | VARCHAR | True |
| `original_path` | VARCHAR | True |
| `base_dir` | VARCHAR | True |
| `detected_source` | VARCHAR | True |
| `script_key` | VARCHAR | True |
| `status` | VARCHAR | True |
| `created_at` | TIMESTAMP | False |
| `updated_at` | TIMESTAMP | False |
| `normalized_content` | BYTEA | True |
| `dashboard_json` | TEXT | True |
| `original_content` | BYTEA | True |

**PK:** id

**FK:** —

**Índices:**
- `idx_uploads_apertura` [apertura_fecha]
  - `CREATE INDEX idx_uploads_apertura ON public.uploads USING btree (apertura_fecha)`
- `idx_uploads_buyer` [buyer_hint]
  - `CREATE INDEX idx_uploads_buyer ON public.uploads USING btree (buyer_hint)`
- `idx_uploads_created` [created_at]
  - `CREATE INDEX idx_uploads_created ON public.uploads USING btree (created_at)`
- `idx_uploads_cuenta` [cuenta_nro]
  - `CREATE INDEX idx_uploads_cuenta ON public.uploads USING btree (cuenta_nro)`
- `idx_uploads_platform` [platform_hint]
  - `CREATE INDEX idx_uploads_platform ON public.uploads USING btree (platform_hint)`
- `idx_uploads_proceso` [proceso_nro]
  - `CREATE INDEX idx_uploads_proceso ON public.uploads USING btree (proceso_nro)`
- `idx_uploads_proceso_key` [proceso_key]
  - `CREATE INDEX idx_uploads_proceso_key ON public.uploads USING btree (proceso_key)`
- `idx_uploads_province` [province_hint]
  - `CREATE INDEX idx_uploads_province ON public.uploads USING btree (province_hint)`
- `idx_uploads_status` [status]
  - `CREATE INDEX idx_uploads_status ON public.uploads USING btree (status)`
- `ix_uploads_apertura_fecha` [apertura_fecha]
  - `CREATE INDEX ix_uploads_apertura_fecha ON public.uploads USING btree (apertura_fecha)`
- `ix_uploads_buyer_hint` [buyer_hint]
  - `CREATE INDEX ix_uploads_buyer_hint ON public.uploads USING btree (buyer_hint)`
- `ix_uploads_created_at` [created_at]
  - `CREATE INDEX ix_uploads_created_at ON public.uploads USING btree (created_at)`
- `ix_uploads_cuenta_nro` [cuenta_nro]
  - `CREATE INDEX ix_uploads_cuenta_nro ON public.uploads USING btree (cuenta_nro)`
- `ix_uploads_platform_hint` [platform_hint]
  - `CREATE INDEX ix_uploads_platform_hint ON public.uploads USING btree (platform_hint)`
- `ix_uploads_proceso_key` [proceso_key]
  - `CREATE INDEX ix_uploads_proceso_key ON public.uploads USING btree (proceso_key)`
- `ix_uploads_proceso_nro` [proceso_nro]
  - `CREATE INDEX ix_uploads_proceso_nro ON public.uploads USING btree (proceso_nro)`
- `ix_uploads_province_hint` [province_hint]
  - `CREATE INDEX ix_uploads_province_hint ON public.uploads USING btree (province_hint)`
- `ix_uploads_status` [status]
  - `CREATE INDEX ix_uploads_status ON public.uploads USING btree (status)`
- `ix_uploads_updated_at` [updated_at]
  - `CREATE INDEX ix_uploads_updated_at ON public.uploads USING btree (updated_at)`
- `ix_uploads_user_id` [user_id]
  - `CREATE INDEX ix_uploads_user_id ON public.uploads USING btree (user_id)`
- `uq_upload_user_proceso` [user_id, proceso_key] (unique)
  - `CREATE UNIQUE INDEX uq_upload_user_proceso ON public.uploads USING btree (user_id, proceso_key)`

### `usage_events` — 68,752 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `timestamp` | TIMESTAMP | False |
| `session_id` | VARCHAR(64) | False |
| `user_id` | INTEGER | False |
| `user_role` | VARCHAR(32) | False |
| `action_type` | VARCHAR(50) | False |
| `section` | VARCHAR(100) | True |
| `resource_id` | VARCHAR(100) | True |
| `duration_ms` | INTEGER | True |
| `extra_data` | JSON | True |
| `ip` | VARCHAR(50) | True |
| `user_agent` | TEXT | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `idx_usage_events_action` [action_type]
  - `CREATE INDEX idx_usage_events_action ON public.usage_events USING btree (action_type)`
- `idx_usage_events_user_time` [user_id, timestamp]
  - `CREATE INDEX idx_usage_events_user_time ON public.usage_events USING btree (user_id, "timestamp")`
- `ix_usage_events_action_type` [action_type]
  - `CREATE INDEX ix_usage_events_action_type ON public.usage_events USING btree (action_type)`
- `ix_usage_events_id` [id]
  - `CREATE INDEX ix_usage_events_id ON public.usage_events USING btree (id)`
- `ix_usage_events_section` [section]
  - `CREATE INDEX ix_usage_events_section ON public.usage_events USING btree (section)`
- `ix_usage_events_session_id` [session_id]
  - `CREATE INDEX ix_usage_events_session_id ON public.usage_events USING btree (session_id)`
- `ix_usage_events_timestamp` [timestamp]
  - `CREATE INDEX ix_usage_events_timestamp ON public.usage_events USING btree ("timestamp")`
- `ix_usage_events_user_id` [user_id]
  - `CREATE INDEX ix_usage_events_user_id ON public.usage_events USING btree (user_id)`
- `ix_usage_events_user_role` [user_role]
  - `CREATE INDEX ix_usage_events_user_role ON public.usage_events USING btree (user_role)`

### `usage_sessions` — -1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `session_id` | VARCHAR(64) | False |
| `user_id` | INTEGER | False |
| `user_role` | VARCHAR(32) | False |
| `start_time` | TIMESTAMP | False |
| `end_time` | TIMESTAMP | True |
| `duration_minutes` | DOUBLE PRECISION | True |
| `active_minutes` | DOUBLE PRECISION | True |
| `idle_minutes` | DOUBLE PRECISION | True |
| `files_uploaded` | INTEGER | False |
| `actions_count` | INTEGER | False |
| `sections_visited` | INTEGER | False |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `idx_usage_sessions_user_start` [user_id, start_time]
  - `CREATE INDEX idx_usage_sessions_user_start ON public.usage_sessions USING btree (user_id, start_time)`
- `ix_usage_sessions_end_time` [end_time]
  - `CREATE INDEX ix_usage_sessions_end_time ON public.usage_sessions USING btree (end_time)`
- `ix_usage_sessions_id` [id]
  - `CREATE INDEX ix_usage_sessions_id ON public.usage_sessions USING btree (id)`
- `ix_usage_sessions_session_id` [session_id] (unique)
  - `CREATE UNIQUE INDEX ix_usage_sessions_session_id ON public.usage_sessions USING btree (session_id)`
- `ix_usage_sessions_start_time` [start_time]
  - `CREATE INDEX ix_usage_sessions_start_time ON public.usage_sessions USING btree (start_time)`
- `ix_usage_sessions_user_id` [user_id]
  - `CREATE INDEX ix_usage_sessions_user_id ON public.usage_sessions USING btree (user_id)`
- `ix_usage_sessions_user_role` [user_role]
  - `CREATE INDEX ix_usage_sessions_user_role ON public.usage_sessions USING btree (user_role)`

### `users` — 10 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `email` | VARCHAR | False |
| `name` | VARCHAR(120) | True |
| `full_name` | VARCHAR | True |
| `password_hash` | VARCHAR | True |
| `role` | VARCHAR | True |
| `business_unit` | VARCHAR(120) | True |
| `created_at` | TIMESTAMP | False |
| `access_scope` | VARCHAR(50) | True |
| `must_change_password` | BOOLEAN | False |

**PK:** id

**FK:** —

**Índices:**
- `ix_users_created_at` [created_at]
  - `CREATE INDEX ix_users_created_at ON public.users USING btree (created_at)`
- `ix_users_email` [email] (unique)
  - `CREATE UNIQUE INDEX ix_users_email ON public.users USING btree (email)`

---
_Backend analizado: postgresql / db=web_comparativas_db._
