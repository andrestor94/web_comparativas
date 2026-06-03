# Actualizar el dataset de Dimensionamiento (Mercado Privado)

Procedimiento para reemplazar la información del módulo **Mercado Privado > Dimensionamiento**
con un dataset nuevo, tanto en local (SQLite) como en producción (Render / PostgreSQL).

La carga reutiliza el módulo de ingesta de producción
`web_comparativas.dimensionamiento.ingestion`, por lo que el comportamiento local y el de
Render son idénticos (dedup por `id_registro_unico`, reconstrucción del resumen mensual y del
snapshot del dashboard, registro de la corrida en `dimensionamiento_import_runs`).

## Tablas afectadas

- `dimensionamiento_records` — datos crudos normalizados (recarga total con `mode=replace`).
- `dimensionamiento_family_monthly_summary` — resumen mensual recalculado.
- `dimensionamiento_dashboard_snapshots` — snapshot del dashboard recalculado.
- `dimensionamiento_import_runs` — 1 fila nueva con métricas de la corrida.

No se tocan otras tablas ni otros módulos.

## Ubicación esperada del CSV (local)

```
web_comparativas/data/archivos dimensionamiento/dataset_unificado_valorizado_2025_2026 - 2.csv
```

> Nota: el nombre tiene espacios alrededor del guion (`... 2025_2026 - 2.csv`).

### Columnas obligatorias

El CSV debe contener, al menos, estas columnas (el delimitador se autodetecta `;`/`,`/`|`/tab):

```
fecha, plataforma, cliente_nombre_homologado, cliente_nombre_original, cuit, provincia,
cuenta_interna, codigo_articulo, descripcion, clasificacion_suizo, descripcion_articulo,
familia, unidad_negocio, subunidad_negocio, cantidad_demandada, resultado_participacion,
producto_nombre_original, id_registro_unico, fecha_procesamiento
```

`valorizacion_estimada` es opcional pero recomendada (alimenta los KPIs monetarios).
Cualquier columna adicional del CSV (p. ej. `cantidad_final`, `precio_unitario_estimado`,
`origen_precio`, etc.) se ignora automáticamente.

## Importar en LOCAL (SQLite, `web_comparativas/app.db`)

1. Hacer backup de la base (fuera de Git):

   ```powershell
   $ts = Get-Date -Format "yyyyMMdd_HHmmss"
   Copy-Item web_comparativas\app.db "_backups_db_local\app_before_dim_update_$ts.db"
   ```

2. Ejecutar el script (usa por defecto el CSV nuevo de la ruta de arriba):

   ```powershell
   .\venv_webcomparativas\Scripts\python.exe scripts\import_dimensionamiento_dataset.py
   ```

   Opcionalmente con otra ruta o modo:

   ```powershell
   .\venv_webcomparativas\Scripts\python.exe scripts\import_dimensionamiento_dataset.py --csv-path "ruta\al\otro.csv"
   .\venv_webcomparativas\Scripts\python.exe scripts\import_dimensionamiento_dataset.py --mode upsert
   ```

3. El script imprime: registros antes/después, resultado JSON de la ingesta y deltas.

## Importar en PRODUCCIÓN (Render / PostgreSQL)

Producción usa **PostgreSQL** (no SQLite). El mismo módulo de ingesta detecta el backend y, en
PostgreSQL, usa staging `UNLOGGED` + `COPY FROM STDIN` + merge/replace SQL.

Opciones:

- **Por URL del CSV** (recomendado para Render), como one-off job o shell:

  ```bash
  python -m web_comparativas.dimensionamiento.ingestion \
      --source-url "$DIMENSIONAMIENTO_CSV_URL" \
      --mode replace --force --require-postgres
  ```

- **Por archivo subido al servicio**:

  ```bash
  python scripts/import_dimensionamiento_dataset.py --csv-path "/ruta/en/render/dataset.csv"
  ```

- **Auto-ingesta al arranque si la tabla está vacía**: variable de entorno
  `DIMENSIONAMIENTO_STARTUP_MODE=ingest-if-empty` (o ruta `DIMENSIONAMIENTO_CSV_PATH` /
  `DIMENSIONAMIENTO_CSV_URL`). Por defecto el startup solo valida si ya hay datos.

> `--require-postgres` aborta si el target no es PostgreSQL: úsalo en prod para evitar
> cargar por error en una base equivocada.

## Nota técnica (solo SQLite local)

En el flujo en-proceso de SQLite, el borrado de registros de corridas previas y la
reconstrucción de `dimensionamiento_family_monthly_summary` pueden no persistir al
commitear junto con el refresh del snapshot. Por eso el script
`import_dimensionamiento_dataset.py` incluye un paso de **auto-reparación**: tras la
ingesta verifica si quedaron registros de corridas viejas o si el summary de la corrida
nueva quedó vacío y, de ser así, los repara en transacciones aisladas. En PostgreSQL
(producción) la ingesta persiste correctamente y este paso es un no-op.

## Validaciones posteriores

Tras la carga, validar en la base:

- `SELECT COUNT(*) FROM dimensionamiento_records;` (debe reflejar el dataset nuevo).
- `SELECT MIN(fecha), MAX(fecha) FROM dimensionamiento_records;` (rango de fechas esperado).
- `SELECT plataforma, COUNT(*) FROM dimensionamiento_records GROUP BY plataforma;`
- `SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary;` (> 0).
- Última corrida: `SELECT id, status, rows_processed, rows_inserted, rows_rejected FROM dimensionamiento_import_runs ORDER BY id DESC LIMIT 1;`

Y en la app (local `http://127.0.0.1:8000`): abrir **Mercado Privado > Dimensionamiento**,
verificar KPIs, filtros, multiselección, tabla de familias y gráficos sin errores 500/JS.

## Advertencias (Git)

- **No** commitear `web_comparativas/app.db` (ya cubierto por `*.db` en `.gitignore`).
- **No** commitear los backups de `_backups_db_local/` (cubiertos por `*.db`).
- **No** commitear el CSV de datos (cubierto por `*.csv` en `.gitignore`).
- Sí se versionan: `scripts/import_dimensionamiento_dataset.py` y esta documentación.
