# Reporte de diagnóstico de base de datos — SIEM

> Generado por `scripts/db_diagnostics.py` (solo lectura).
> Fecha (UTC): 2026-06-06T21:13:21
> Motor: **sqlite / db=app.db**  ·  conteo: **exacto**

Este reporte es estructural. No contiene filas de datos ni credenciales.

## Resumen

- Total de tablas: **52**
- Tablas `forecast_*` totales: **4** (en ORM: 4, NO en ORM / datos base: 0)
- Tablas vacías (0 filas): **22**
- Tablas grandes (>= 100,000 filas): **2**
- Tablas candidatas a revisión (legado/fantasma): **8**

## Tablas (conteo, tamaño, PK, FK, índices)

| Tabla | Filas | Tamaño | Cols | PK | FK | Índices | Notas |
|---|---:|---:|---:|---|---:|---:|---|
| `app_config` | 0 | — | 5 | id | 0 | 3 | vacía |
| `chat_channels` | 0 | — | 5 | id | 0 | 1 | revisión; vacía |
| `chat_members` | 0 | — | 5 | id | 2 | 2 | revisión; vacía |
| `chat_messages` | 0 | — | 6 | id | 2 | 3 | revisión; vacía |
| `comments` | 0 | — | 10 | id | 3 | 12 | vacía |
| `comparativa_rows` | 1,092 | — | 23 | id | 1 | 8 |  |
| `dashboards` | 0 | — | 5 | id | 1 | 0 | revisión; vacía |
| `dimensionamiento_dashboard_snapshots` | 1 | — | 6 | id | 1 | 4 |  |
| `dimensionamiento_family_monthly_summary` | 259,702 | — | 17 | id | 1 | 16 | grande |
| `dimensionamiento_import_errors` | 0 | — | 6 | id | 1 | 2 | vacía |
| `dimensionamiento_import_runs` | 5 | — | 17 | id | 0 | 5 |  |
| `dimensionamiento_records` | 319,128 | — | 27 | id | 1 | 21 | grande |
| `email_notifications` | 0 | — | 5 | id | 1 | 6 | vacía |
| `forecast_change_requests` | 52 | — | 28 | id | 1 | 6 | forecast_* (en ORM) |
| `forecast_manual_clients` | 10 | — | 10 | id | 1 | 1 | forecast_* (en ORM) |
| `forecast_manual_entries` | 15 | — | 15 | id | 1 | 1 | forecast_* (en ORM) |
| `forecast_user_overrides` | 57 | — | 21 | id | 1 | 11 | forecast_* (en ORM) |
| `group_members` | 0 | — | 6 | id | 3 | 0 | vacía |
| `groups` | 0 | — | 5 | id | 1 | 1 | vacía |
| `normalized_files` | 0 | — | 5 | id | 1 | 0 | revisión; vacía |
| `notifications` | 6 | — | 8 | id | 1 | 4 |  |
| `oportunidades` | 794 | — | 13 | id | 0 | 9 |  |
| `password_reset_requests` | 0 | — | 12 | id | 0 | 0 | revisión; vacía |
| `pliego_actos_admin` | 0 | — | 8 | id | 1 | 1 | vacía |
| `pliego_analitica` | 0 | — | 3 | id | 1 | 1 | vacía |
| `pliego_archivos` | 3 | — | 9 | id | 1 | 1 |  |
| `pliego_control_carga` | 0 | — | 3 | id | 1 | 1 | vacía |
| `pliego_cronograma` | 4 | — | 8 | id | 1 | 1 |  |
| `pliego_documentos` | 3 | — | 8 | id | 1 | 1 |  |
| `pliego_edit_history` | 1 | — | 16 | id | 2 | 1 |  |
| `pliego_excel_cargas` | 1 | — | 10 | id | 2 | 1 |  |
| `pliego_faltantes` | 15 | — | 9 | id | 1 | 1 |  |
| `pliego_field_overrides` | 1 | — | 17 | id | 2 | 2 |  |
| `pliego_fusion_cabecera` | 0 | — | 3 | id | 1 | 1 | vacía |
| `pliego_fusion_renglones` | 2 | — | 9 | id | 1 | 1 |  |
| `pliego_garantias` | 7 | — | 10 | id | 1 | 1 |  |
| `pliego_hallazgos` | 5 | — | 8 | id | 1 | 1 |  |
| `pliego_historial` | 3 | — | 7 | id | 2 | 1 |  |
| `pliego_proceso` | 1 | — | 3 | id | 1 | 1 |  |
| `pliego_renglones` | 2 | — | 13 | id | 1 | 1 |  |
| `pliego_requisitos` | 19 | — | 10 | id | 1 | 1 |  |
| `pliego_solicitudes` | 1 | — | 21 | id | 4 | 4 |  |
| `pliego_trazabilidad` | 34 | — | 8 | id | 1 | 1 |  |
| `revision_sessions` | 0 | — | 8 | id | 1 | 3 | revisión; vacía |
| `runs` | 0 | — | 6 | id | 1 | 0 | revisión; vacía |
| `saved_views` | 0 | — | 8 | id | 1 | 8 | vacía |
| `ticket_messages` | 0 | — | 6 | id | 2 | 2 | vacía |
| `tickets` | 0 | — | 11 | id | 2 | 2 | vacía |
| `uploads` | 1 | — | 22 | id | 0 | 11 |  |
| `usage_events` | 4,610 | — | 12 | id | 1 | 9 |  |
| `usage_sessions` | 0 | — | 12 | id | 1 | 7 | vacía |
| `users` | 3 | — | 10 | id | 0 | 1 |  |

## Tablas `forecast_*` de datos base (NO modeladas en el ORM)

> Estas son el objetivo de modelado de la Fase 3. En SQLite local normalmente
> NO aparecen (los datos base son CSV/parquet); existen como tablas en PostgreSQL
> producción. Las `forecast_*` que SÍ están en el ORM (overrides, manuales,
> aprobaciones) no se listan aquí porque ya están modeladas.

_No se detectaron tablas `forecast_*` de datos base en esta base._
> Esperable en SQLite local. Correr el diagnóstico contra PostgreSQL producción
> (con autorización) para inspeccionar su esquema real.

## Tablas vacías

`app_config`, `chat_channels`, `chat_members`, `chat_messages`, `comments`, `dashboards`, `dimensionamiento_import_errors`, `email_notifications`, `group_members`, `groups`, `normalized_files`, `password_reset_requests`, `pliego_actos_admin`, `pliego_analitica`, `pliego_control_carga`, `pliego_fusion_cabecera`, `revision_sessions`, `runs`, `saved_views`, `ticket_messages`, `tickets`, `usage_sessions`

## Tablas candidatas a revisión (legado / fantasma)

> Presentes en esta base. **No eliminar** sin validación + backup (ver plan de migración).

- `chat_channels` — 0 filas
- `chat_members` — 0 filas
- `chat_messages` — 0 filas
- `dashboards` — 0 filas
- `normalized_files` — 0 filas
- `password_reset_requests` — 0 filas
- `revision_sessions` — 0 filas
- `runs` — 0 filas

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
- **`oportunidades`**
  - `raw_data`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL
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
  - `original_content`: binario (BLOB/BYTEA) — candidato a separar de la tabla
  - `normalized_content`: binario (BLOB/BYTEA) — candidato a separar de la tabla
- **`usage_events`**
  - `extra_data`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL

## Apéndice — detalle por tabla

### `app_config` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `key` | VARCHAR(120) | False |
| `value` | VARCHAR(255) | True |
| `created_at` | DATETIME | False |
| `updated_at` | DATETIME | False |

**PK:** id

**FK:** —

**Índices:**
- `ix_app_config_created_at` [created_at]
- `ix_app_config_key` [key] (unique)
- `ix_app_config_updated_at` [updated_at]

### `chat_channels` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `type` | VARCHAR(20) | False |
| `name` | VARCHAR(100) | True |
| `created_at` | DATETIME | False |
| `updated_at` | DATETIME | False |

**PK:** id

**FK:** —

**Índices:**
- `ix_chat_channels_updated_at` [updated_at]

### `chat_members` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `channel_id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `joined_at` | DATETIME | True |
| `last_read_at` | DATETIME | True |

**PK:** id

**FK:**
- (channel_id) → `chat_channels` (id)
- (user_id) → `users` (id)

**Índices:**
- `ix_chat_members_channel_id` [channel_id]
- `ix_chat_members_user_id` [user_id]

### `chat_messages` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `channel_id` | INTEGER | False |
| `sender_id` | INTEGER | False |
| `content` | TEXT | True |
| `attachment_path` | VARCHAR | True |
| `created_at` | DATETIME | False |

**PK:** id

**FK:**
- (channel_id) → `chat_channels` (id)
- (sender_id) → `users` (id)

**Índices:**
- `ix_chat_messages_channel_id` [channel_id]
- `ix_chat_messages_created_at` [created_at]
- `ix_chat_messages_sender_id` [sender_id]

### `comments` — 0 filas

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
| `created_at` | DATETIME | False |
| `updated_at` | DATETIME | False |
| `deleted_at` | DATETIME | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)
- (author_user_id) → `users` (id)
- (parent_id) → `comments` (id)

**Índices:**
- `idx_comments_author` [author_user_id]
- `idx_comments_created` [created_at]
- `idx_comments_parent` [parent_id]
- `idx_comments_resolved` [is_resolved]
- `idx_comments_upload` [upload_id]
- `ix_comments_author_user_id` [author_user_id]
- `ix_comments_created_at` [created_at]
- `ix_comments_deleted_at` [deleted_at]
- `ix_comments_is_resolved` [is_resolved]
- `ix_comments_parent_id` [parent_id]
- `ix_comments_updated_at` [updated_at]
- `ix_comments_upload_id` [upload_id]

### `comparativa_rows` — 1,092 filas

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
| `cantidad_solicitada` | FLOAT | True |
| `unidad_medida` | VARCHAR(60) | True |
| `precio_unitario` | FLOAT | True |
| `cantidad_ofertada` | FLOAT | True |
| `total_por_renglon` | FLOAT | True |
| `especificacion_tecnica` | TEXT | True |
| `marca` | VARCHAR(255) | True |
| `posicion` | INTEGER | True |
| `rubro` | VARCHAR(255) | True |
| `created_at` | DATETIME | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:**
- `ix_comp_rows_comprador_fecha` [comprador, fecha_apertura]
- `ix_comp_rows_descripcion_fecha` [descripcion, fecha_apertura]
- `ix_comp_rows_fecha_apertura` [fecha_apertura]
- `ix_comp_rows_marca_fecha` [marca, fecha_apertura]
- `ix_comp_rows_proveedor_fecha` [proveedor, fecha_apertura]
- `ix_comp_rows_upload_proveedor` [upload_id, proveedor]
- `ix_comparativa_rows_nro_proceso` [nro_proceso]
- `ix_comparativa_rows_upload_id` [upload_id]

### `dashboards` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | True |
| `json_path` | VARCHAR | True |
| `html_path` | VARCHAR | True |
| `published_at` | DATETIME | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:** —

### `dimensionamiento_dashboard_snapshots` — 1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `snapshot_key` | VARCHAR(100) | False |
| `version` | VARCHAR(20) | False |
| `payload` | JSON | False |
| `generated_at` | DATETIME | False |
| `import_run_id` | INTEGER | True |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dimensionamiento_dashboard_snapshots_generated_at` [generated_at]
- `ix_dimensionamiento_dashboard_snapshots_import_run_id` [import_run_id]
- `ix_dimensionamiento_dashboard_snapshots_snapshot_key` [snapshot_key] (unique)
- `ix_dimensionamiento_dashboard_snapshots_version` [version]

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
| `total_cantidad` | FLOAT | False |
| `total_valorizacion` | FLOAT | False |
| `total_registros` | INTEGER | False |
| `clientes_unicos` | INTEGER | False |
| `import_run_id` | INTEGER | False |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dim_summary_client_month` [cliente_nombre_homologado, month]
- `ix_dim_summary_family_month` [familia, month]
- `ix_dim_summary_platform_month` [plataforma, month]
- `ix_dim_summary_visible_month` [cliente_visible, month]
- `ix_dimensionamiento_family_monthly_summary_cliente_nombre_homologado` [cliente_nombre_homologado]
- `ix_dimensionamiento_family_monthly_summary_cliente_visible` [cliente_visible]
- `ix_dimensionamiento_family_monthly_summary_familia` [familia]
- `ix_dimensionamiento_family_monthly_summary_import_run_id` [import_run_id]
- `ix_dimensionamiento_family_monthly_summary_is_client` [is_client]
- `ix_dimensionamiento_family_monthly_summary_is_identified` [is_identified]
- `ix_dimensionamiento_family_monthly_summary_month` [month]
- `ix_dimensionamiento_family_monthly_summary_plataforma` [plataforma]
- `ix_dimensionamiento_family_monthly_summary_provincia` [provincia]
- `ix_dimensionamiento_family_monthly_summary_resultado_participacion` [resultado_participacion]
- `ix_dimensionamiento_family_monthly_summary_subunidad_negocio` [subunidad_negocio]
- `ix_dimensionamiento_family_monthly_summary_unidad_negocio` [unidad_negocio]

### `dimensionamiento_import_errors` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `import_run_id` | INTEGER | False |
| `row_number` | INTEGER | False |
| `error_message` | TEXT | False |
| `raw_payload` | JSON | True |
| `created_at` | DATETIME | False |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dimensionamiento_import_errors_created_at` [created_at]
- `ix_dimensionamiento_import_errors_import_run_id` [import_run_id]

### `dimensionamiento_import_runs` — 5 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `source_path` | VARCHAR(500) | False |
| `source_hash` | VARCHAR(64) | True |
| `source_mtime` | DATETIME | True |
| `mode` | VARCHAR(20) | False |
| `status` | VARCHAR(20) | False |
| `chunk_size` | INTEGER | False |
| `started_at` | DATETIME | False |
| `finished_at` | DATETIME | True |
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
- `ix_dimensionamiento_import_runs_mode` [mode]
- `ix_dimensionamiento_import_runs_source_hash` [source_hash]
- `ix_dimensionamiento_import_runs_started_at` [started_at]
- `ix_dimensionamiento_import_runs_status` [status]

### `dimensionamiento_records` — 319,128 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `id_registro_unico` | VARCHAR(255) | False |
| `fecha` | DATE | False |
| `plataforma` | VARCHAR(40) | False |
| `cliente_nombre_homologado` | TEXT | True |
| `cliente_nombre_original` | TEXT | True |
| `cliente_visible` | TEXT | True |
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
| `cantidad_demandada` | FLOAT | False |
| `valorizacion_estimada` | FLOAT | True |
| `resultado_participacion` | VARCHAR(120) | True |
| `producto_nombre_original` | TEXT | True |
| `fecha_procesamiento` | DATETIME | True |
| `is_identified` | BOOLEAN | False |
| `is_client` | BOOLEAN | False |
| `import_run_id` | INTEGER | True |
| `created_at` | DATETIME | False |
| `updated_at` | DATETIME | False |

**PK:** id

**FK:**
- (import_run_id) → `dimensionamiento_import_runs` (id)

**Índices:**
- `ix_dim_records_client_date` [cliente_nombre_homologado, fecha]
- `ix_dim_records_family_date` [familia, fecha]
- `ix_dim_records_platform_date` [plataforma, fecha]
- `ix_dim_records_province_date` [provincia, fecha]
- `ix_dim_records_result_date` [resultado_participacion, fecha]
- `ix_dim_records_unit_subunit_date` [unidad_negocio, subunidad_negocio, fecha]
- `ix_dim_records_visible_date` [cliente_visible, fecha]
- `ix_dimensionamiento_records_cliente_nombre_homologado` [cliente_nombre_homologado]
- `ix_dimensionamiento_records_cliente_visible` [cliente_visible]
- `ix_dimensionamiento_records_codigo_articulo` [codigo_articulo]
- `ix_dimensionamiento_records_familia` [familia]
- `ix_dimensionamiento_records_fecha` [fecha]
- `ix_dimensionamiento_records_fecha_procesamiento` [fecha_procesamiento]
- `ix_dimensionamiento_records_import_run_id` [import_run_id]
- `ix_dimensionamiento_records_is_client` [is_client]
- `ix_dimensionamiento_records_is_identified` [is_identified]
- `ix_dimensionamiento_records_plataforma` [plataforma]
- `ix_dimensionamiento_records_provincia` [provincia]
- `ix_dimensionamiento_records_resultado_participacion` [resultado_participacion]
- `ix_dimensionamiento_records_subunidad_negocio` [subunidad_negocio]
- `ix_dimensionamiento_records_unidad_negocio` [unidad_negocio]

### `email_notifications` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | False |
| `recipient` | VARCHAR(255) | False |
| `event` | VARCHAR(50) | False |
| `sent_at` | DATETIME | False |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:**
- `idx_email_notif_recipient` [recipient]
- `idx_email_notif_upload` [upload_id]
- `ix_email_notifications_event` [event]
- `ix_email_notifications_recipient` [recipient]
- `ix_email_notifications_upload_id` [upload_id]
- `uq_email_notif` [upload_id, recipient, event] (unique)

### `forecast_change_requests` — 52 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `created_at` | DATETIME | False |
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
| `old_value` | FLOAT | True |
| `new_value` | FLOAT | True |
| `absolute_delta` | FLOAT | True |
| `percentage_delta` | FLOAT | True |
| `estimated_amount_base` | FLOAT | True |
| `estimated_amount_delta` | FLOAT | True |
| `status` | VARCHAR(12) | False |
| `reviewed_by_user_id` | INTEGER | True |
| `reviewed_by_username` | VARCHAR(255) | True |
| `reviewed_at` | DATETIME | True |
| `review_comment` | TEXT | True |

**PK:** id

**FK:**
- (override_id) → `forecast_user_overrides` (id)

**Índices:**
- `ix_forecast_change_requests_change_type` [change_type]
- `ix_forecast_change_requests_client_selector` [client_selector]
- `ix_forecast_change_requests_created_at` [created_at]
- `ix_forecast_change_requests_created_by_user_id` [created_by_user_id]
- `ix_forecast_change_requests_override_id` [override_id]
- `ix_forecast_change_requests_status` [status]

### `forecast_manual_clients` — 10 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `nombre_cliente` | VARCHAR(255) | False |
| `grupo` | VARCHAR(255) | True |
| `is_active` | BOOLEAN | False |
| `created_at` | DATETIME | False |
| `updated_at` | DATETIME | False |
| `created_by` | VARCHAR(255) | True |
| `deleted_at` | DATETIME | True |
| `deleted_by` | VARCHAR(255) | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_forecast_manual_clients_user_id` [user_id]

### `forecast_manual_entries` — 15 filas

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
| `cantidad` | FLOAT | False |
| `costo_unitario` | FLOAT | False |
| `monto_total` | FLOAT | False |
| `is_active` | BOOLEAN | False |
| `deleted_at` | DATETIME | True |
| `deleted_by` | VARCHAR(255) | True |
| `perfil` | VARCHAR(120) | True |

**PK:** id

**FK:**
- (client_id) → `forecast_manual_clients` (id)

**Índices:**
- `ix_forecast_manual_entries_client_id` [client_id]

### `forecast_user_overrides` — 57 filas

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
| `base_growth_pct` | FLOAT | True |
| `override_growth_pct` | FLOAT | True |
| `effective_monthly_pct` | FLOAT | True |
| `effective_from_month` | VARCHAR(7) | True |
| `is_active` | BOOLEAN | False |
| `created_at` | DATETIME | False |
| `updated_at` | DATETIME | False |
| `created_by` | VARCHAR(255) | True |
| `updated_by` | VARCHAR(255) | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_fc_override_context_lookup` [source_module, context_key, user_id, updated_at]
- `ix_fc_override_scope_lookup` [user_id, source_module, override_scope, client_selector, subneg, codigo_serie, forecast_month, is_active]
- `ix_fc_override_user_client_active` [user_id, source_module, client_selector, is_active]
- `ix_forecast_user_overrides_client_selector` [client_selector]
- `ix_forecast_user_overrides_context_key` [context_key]
- `ix_forecast_user_overrides_created_at` [created_at]
- `ix_forecast_user_overrides_is_active` [is_active]
- `ix_forecast_user_overrides_override_scope` [override_scope]
- `ix_forecast_user_overrides_source_module` [source_module]
- `ix_forecast_user_overrides_updated_at` [updated_at]
- `ix_forecast_user_overrides_user_id` [user_id]

### `group_members` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `group_id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `role_in_group` | VARCHAR(32) | False |
| `added_by_user_id` | INTEGER | True |
| `added_at` | DATETIME | False |

**PK:** id

**FK:**
- (group_id) → `groups` (id)
- (user_id) → `users` (id)
- (added_by_user_id) → `users` (id)

**Índices:** —

### `groups` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `name` | VARCHAR(120) | False |
| `business_unit` | VARCHAR(120) | True |
| `created_by_user_id` | INTEGER | False |
| `created_at` | DATETIME | False |

**PK:** id

**FK:**
- (created_by_user_id) → `users` (id)

**Índices:**
- `ix_groups_name` [name] (unique)

### `normalized_files` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | True |
| `path` | VARCHAR | True |
| `row_count` | INTEGER | True |
| `checksum` | VARCHAR | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:** —

### `notifications` — 6 filas

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
| `created_at` | DATETIME | False |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_notifications_category` [category]
- `ix_notifications_created_at` [created_at]
- `ix_notifications_is_read` [is_read]
- `ix_notifications_user_id` [user_id]

### `oportunidades` — 794 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `proceso_nro` | VARCHAR | True |
| `fecha_apertura` | DATETIME | True |
| `comprador` | VARCHAR | True |
| `provincia` | VARCHAR | True |
| `plataforma` | VARCHAR | True |
| `presupuesto` | FLOAT | True |
| `descripcion` | TEXT | True |
| `tipo_proceso` | VARCHAR | True |
| `estado_evaluacion` | VARCHAR | True |
| `estado_normalizado` | VARCHAR | True |
| `created_at` | DATETIME | False |
| `raw_data` | JSON | True |

**PK:** id

**FK:** —

**Índices:**
- `ix_oportunidades_comprador` [comprador]
- `ix_oportunidades_created_at` [created_at]
- `ix_oportunidades_estado_evaluacion` [estado_evaluacion]
- `ix_oportunidades_estado_normalizado` [estado_normalizado]
- `ix_oportunidades_fecha_apertura` [fecha_apertura]
- `ix_oportunidades_plataforma` [plataforma]
- `ix_oportunidades_proceso_nro` [proceso_nro]
- `ix_oportunidades_provincia` [provincia]
- `ix_oportunidades_tipo_proceso` [tipo_proceso]

### `password_reset_requests` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | True |
| `user_email` | VARCHAR(255) | False |
| `full_name` | VARCHAR(255) | False |
| `department` | VARCHAR(120) | True |
| `comment` | TEXT | True |
| `request_date` | DATETIME | False |
| `status` | VARCHAR(30) | False |
| `handled_by` | VARCHAR(255) | True |
| `handled_date` | DATETIME | True |
| `admin_observation` | TEXT | True |
| `temporary_password_generated` | BOOLEAN | False |
| `must_change_password_on_next_login` | BOOLEAN | False |

**PK:** id

**FK:** —

**Índices:** —

### `pliego_actos_admin` — 0 filas

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

### `pliego_analitica` — 0 filas

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

### `pliego_archivos` — 3 filas

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
| `contenido_bytes` | BLOB | True |
| `creado_en` | DATETIME | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_archivos_solicitud_id` [solicitud_id]

### `pliego_control_carga` — 0 filas

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

### `pliego_cronograma` — 4 filas

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

### `pliego_documentos` — 3 filas

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

### `pliego_edit_history` — 1 filas

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
| `edited_at` | DATETIME | True |
| `reason` | TEXT | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)
- (edited_by_user_id) → `users` (id)

**Índices:**
- `ix_pliego_edit_history_solicitud_id` [solicitud_id]

### `pliego_excel_cargas` — 1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `nombre_archivo` | VARCHAR | False |
| `version` | INTEGER | True |
| `url_path` | VARCHAR | True |
| `contenido_bytes` | BLOB | True |
| `cargado_por_id` | INTEGER | True |
| `creado_en` | DATETIME | True |
| `es_activa` | BOOLEAN | True |
| `observaciones` | TEXT | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)
- (cargado_por_id) → `users` (id)

**Índices:**
- `ix_pliego_excel_cargas_solicitud_id` [solicitud_id]

### `pliego_faltantes` — 15 filas

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

### `pliego_field_overrides` — 1 filas

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
| `edited_at` | DATETIME | True |
| `reason` | TEXT | True |
| `is_active` | BOOLEAN | False |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)
- (edited_by_user_id) → `users` (id)

**Índices:**
- `ix_pfo_pliego_entity` [solicitud_id, entity_type, entity_id, field_key]
- `ix_pliego_field_overrides_solicitud_id` [solicitud_id]

### `pliego_fusion_cabecera` — 0 filas

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

### `pliego_fusion_renglones` — 2 filas

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

### `pliego_garantias` — 7 filas

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

### `pliego_hallazgos` — 5 filas

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
| `datos_extra` | JSON | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_pliego_hallazgos_solicitud_id` [solicitud_id]

### `pliego_historial` — 3 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `solicitud_id` | INTEGER | False |
| `estado_anterior` | VARCHAR | True |
| `estado_nuevo` | VARCHAR | False |
| `comentario` | TEXT | True |
| `usuario_id` | INTEGER | True |
| `creado_en` | DATETIME | True |

**PK:** id

**FK:**
- (solicitud_id) → `pliego_solicitudes` (id)
- (usuario_id) → `users` (id)

**Índices:**
- `ix_pliego_historial_solicitud_id` [solicitud_id]

### `pliego_proceso` — 1 filas

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

### `pliego_renglones` — 2 filas

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

### `pliego_requisitos` — 19 filas

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

### `pliego_solicitudes` — 1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `client_request_id` | VARCHAR(64) | True |
| `titulo` | VARCHAR | False |
| `organismo` | VARCHAR | True |
| `nombre_licitacion` | VARCHAR | True |
| `numero_proceso` | VARCHAR | True |
| `expediente` | VARCHAR | True |
| `observaciones_usuario` | TEXT | True |
| `estado` | VARCHAR | True |
| `creado_por_id` | INTEGER | False |
| `admin_responsable_id` | INTEGER | True |
| `creado_en` | DATETIME | True |
| `actualizado_en` | DATETIME | True |
| `publicado_en` | DATETIME | True |
| `deleted_at` | DATETIME | True |
| `deleted_by_id` | INTEGER | True |
| `enviado_a_gpt_en` | DATETIME | True |
| `enviado_a_gpt_por_id` | INTEGER | True |
| `procesado_externamente_en` | DATETIME | True |
| `observaciones_procesamiento` | TEXT | True |
| `observaciones_admin` | TEXT | True |

**PK:** id

**FK:**
- (creado_por_id) → `users` (id)
- (admin_responsable_id) → `users` (id)
- (deleted_by_id) → `users` (id)
- (enviado_a_gpt_por_id) → `users` (id)

**Índices:**
- `ix_pliego_solicitudes_deleted_at` [deleted_at]
- `ix_pliego_solicitudes_estado` [estado]
- `ix_pliego_solicitudes_id` [id]
- `uq_pliego_solicitud_client_request_id` [client_request_id] (unique)

### `pliego_trazabilidad` — 34 filas

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

### `revision_sessions` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `tender_id` | VARCHAR | False |
| `user_id` | INTEGER | False |
| `field_path` | VARCHAR | False |
| `original_value` | VARCHAR | True |
| `corrected_value` | VARCHAR | True |
| `confidence_at_revision` | FLOAT | True |
| `created_at` | DATETIME | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `ix_revision_sessions_id` [id]
- `ix_revision_sessions_tender_id` [tender_id]
- `ix_revision_sessions_user_id` [user_id]

### `runs` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `upload_id` | INTEGER | True |
| `status` | VARCHAR | True |
| `started_at` | DATETIME | True |
| `ended_at` | DATETIME | True |
| `logs_path` | VARCHAR | True |

**PK:** id

**FK:**
- (upload_id) → `uploads` (id)

**Índices:** —

### `saved_views` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `view_id` | VARCHAR(64) | False |
| `name` | VARCHAR(120) | False |
| `is_default` | BOOLEAN | False |
| `payload` | JSON | False |
| `created_at` | DATETIME | False |
| `updated_at` | DATETIME | False |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `idx_savedviews_user` [user_id]
- `idx_savedviews_user_view_default` [user_id, view_id, is_default]
- `idx_savedviews_viewid` [view_id]
- `ix_saved_views_created_at` [created_at]
- `ix_saved_views_is_default` [is_default]
- `ix_saved_views_updated_at` [updated_at]
- `ix_saved_views_user_id` [user_id]
- `ix_saved_views_view_id` [view_id]

### `ticket_messages` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `ticket_id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `message` | TEXT | False |
| `is_internal` | BOOLEAN | True |
| `created_at` | DATETIME | True |

**PK:** id

**FK:**
- (ticket_id) → `tickets` (id)
- (user_id) → `users` (id)

**Índices:**
- `ix_ticket_messages_id` [id]
- `ix_ticket_messages_ticket_id` [ticket_id]

### `tickets` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | False |
| `title` | VARCHAR | False |
| `category` | VARCHAR | True |
| `priority` | VARCHAR | True |
| `status` | VARCHAR | True |
| `created_at` | DATETIME | True |
| `updated_at` | DATETIME | True |
| `modulo_origen` | VARCHAR(50) | True |
| `pliego_solicitud_id` | INTEGER | True |
| `contexto_extra` | TEXT | True |

**PK:** id

**FK:**
- (user_id) → `users` (id)
- (pliego_solicitud_id) → `pliego_solicitudes` (id)

**Índices:**
- `ix_tickets_id` [id]
- `ix_tickets_user_id` [user_id]

### `uploads` — 1 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `user_id` | INTEGER | True |
| `proceso_nro` | VARCHAR | True |
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
| `created_at` | DATETIME | True |
| `updated_at` | DATETIME | True |
| `proceso_key` | TEXT | True |
| `uploaded_by_name` | TEXT | True |
| `uploaded_by_email` | TEXT | True |
| `original_content` | BLOB | True |
| `normalized_content` | BLOB | True |
| `dashboard_json` | TEXT | True |

**PK:** id

**FK:** —

**Índices:**
- `idx_uploads_apertura` [apertura_fecha]
- `idx_uploads_buyer` [buyer_hint]
- `idx_uploads_created` [created_at]
- `idx_uploads_cuenta` [cuenta_nro]
- `idx_uploads_platform` [platform_hint]
- `idx_uploads_proceso` [proceso_nro]
- `idx_uploads_proceso_key` [proceso_key]
- `idx_uploads_province` [province_hint]
- `idx_uploads_status` [status]
- `ix_upload_user_proceso` [user_id, proceso_key]
- `uq_upload_user_proceso` [user_id, proceso_key] (unique)

### `usage_events` — 4,610 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `timestamp` | DATETIME | False |
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
- `idx_usage_events_user_time` [user_id, timestamp]
- `ix_usage_events_action_type` [action_type]
- `ix_usage_events_id` [id]
- `ix_usage_events_section` [section]
- `ix_usage_events_session_id` [session_id]
- `ix_usage_events_timestamp` [timestamp]
- `ix_usage_events_user_id` [user_id]
- `ix_usage_events_user_role` [user_role]

### `usage_sessions` — 0 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `session_id` | VARCHAR(64) | False |
| `user_id` | INTEGER | False |
| `user_role` | VARCHAR(32) | False |
| `start_time` | DATETIME | False |
| `end_time` | DATETIME | True |
| `duration_minutes` | FLOAT | True |
| `active_minutes` | FLOAT | True |
| `idle_minutes` | FLOAT | True |
| `files_uploaded` | INTEGER | False |
| `actions_count` | INTEGER | False |
| `sections_visited` | INTEGER | False |

**PK:** id

**FK:**
- (user_id) → `users` (id)

**Índices:**
- `idx_usage_sessions_user_start` [user_id, start_time]
- `ix_usage_sessions_end_time` [end_time]
- `ix_usage_sessions_id` [id]
- `ix_usage_sessions_session_id` [session_id] (unique)
- `ix_usage_sessions_start_time` [start_time]
- `ix_usage_sessions_user_id` [user_id]
- `ix_usage_sessions_user_role` [user_role]

### `users` — 3 filas

**Columnas**

| Columna | Tipo | Nullable |
|---|---|---|
| `id` | INTEGER | False |
| `email` | VARCHAR | True |
| `full_name` | VARCHAR | True |
| `password_hash` | VARCHAR | True |
| `role` | VARCHAR | True |
| `created_at` | DATETIME | True |
| `name` | TEXT | True |
| `business_unit` | TEXT | True |
| `access_scope` | VARCHAR | True |
| `must_change_password` | BOOLEAN | False |

**PK:** id

**FK:** —

**Índices:**
- `ix_users_email` [email] (unique)

---
_Backend analizado: sqlite / db=app.db._
