# Dimensionamiento: arquitectura e ingesta

## Flujo

1. `CSV unificado` en `web_comparativas/data/dataset_unificado.csv`
2. `python -m web_comparativas.dimensionamiento.ingestion --mode replace`
3. Persistencia en tablas:
   - `dimensionamiento_records`
   - `dimensionamiento_family_monthly_summary`
   - `dimensionamiento_import_runs`
   - `dimensionamiento_import_errors`
4. El frontend consulta endpoints backend en `/api/mercado-privado/dimensiones/*`

## Estrategia de actualización

- Manual / one-off a PostgreSQL:
  - `python -m web_comparativas.dimensionamiento.ingestion --source-url "$DIMENSIONAMIENTO_CSV_URL" --mode replace --force --require-postgres`
  - o `ingestar_dimensionamiento.bat --source-url "https://...csv" --force --require-postgres`
- Startup del web service:
  - por defecto valida y loguea si `dimensionamiento_records` ya tiene datos
  - para bootstrap automático si la tabla está vacía: `DIMENSIONAMIENTO_STARTUP_MODE=ingest-if-empty`
  - modo legado de fuerza: `DIMENSIONAMIENTO_AUTO_INGEST=true`
- Ruta alternativa del CSV:
  - `DIMENSIONAMIENTO_CSV_PATH=/ruta/al/archivo.csv`
  - `DIMENSIONAMIENTO_CSV_URL=https://...`

## Notas de performance

- Lectura chunked de CSV con `pandas.read_csv(..., chunksize=...)`
- Validación de columnas obligatorias antes de procesar
- Upsert por `id_registro_unico`
- Recarga controlada con `mode=replace`
- Índices sobre fecha, plataforma, cliente, provincia, familia, unidad, subunidad y resultado
- Resumen mensual precalculado en `dimensionamiento_family_monthly_summary`
- El navegador ya no recibe datasets completos ni procesa archivos pesados

Notas extra de PostgreSQL:
- La carga grande ya no usa flush ORM por lotes para `dimensionamiento_records`.
- El flujo recomendado en Render es staging `UNLOGGED` + `COPY FROM STDIN` + merge/replace SQL.
- Al finalizar la ingesta se recalculan summary y snapshot del dashboard.

## Observabilidad

- Cada corrida queda en `dimensionamiento_import_runs`
- Filas inválidas se guardan en `dimensionamiento_import_errors`
- Métricas registradas:
  - procesadas
  - insertadas
  - actualizadas
  - rechazadas
- Logs explícitos de startup/ingesta:
  - `[DIM] Startup ingestion start`
  - `[DIM] Table row count = X`
  - `[DIM] Using source URL ...`
  - `[DIM] Download started`
  - `[DIM] Download completed`
  - `[DIM] CSV loaded with N rows`
  - `[DIM] Ingestion completed`
  - `[DIM] ERROR: ...`
