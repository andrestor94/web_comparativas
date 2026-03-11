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

- Manual local:
  - `ingestar_dimensionamiento.bat`
  - o `python -m web_comparativas.dimensionamiento.ingestion --mode replace`
- Auto al startup:
  - `DIMENSIONAMIENTO_AUTO_INGEST=true`
- Ruta alternativa del CSV:
  - `DIMENSIONAMIENTO_CSV_PATH=/ruta/al/archivo.csv`

## Notas de performance

- Lectura chunked de CSV con `pandas.read_csv(..., chunksize=...)`
- Validación de columnas obligatorias antes de procesar
- Upsert por `id_registro_unico`
- Recarga controlada con `mode=replace`
- Índices sobre fecha, plataforma, cliente, provincia, familia, unidad, subunidad y resultado
- Resumen mensual precalculado en `dimensionamiento_family_monthly_summary`
- El navegador ya no recibe datasets completos ni procesa archivos pesados

## Observabilidad

- Cada corrida queda en `dimensionamiento_import_runs`
- Filas inválidas se guardan en `dimensionamiento_import_errors`
- Métricas registradas:
  - procesadas
  - insertadas
  - actualizadas
  - rechazadas
