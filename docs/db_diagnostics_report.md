# Reporte de diagnóstico de base de datos — SIEM

> Generado por `scripts/db_diagnostics.py` (solo lectura).
> Fecha (UTC): 2026-06-06T20:58:26
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

---
_Backend analizado: sqlite / db=app.db._
