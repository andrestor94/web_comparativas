Ubique aquí el CSV unificado de Dimensionamiento.

Ruta esperada por defecto:

`web_comparativas/data/dataset_unificado.csv`

También puede usarse otra ruta con la variable de entorno:

`DIMENSIONAMIENTO_CSV_PATH=/ruta/al/archivo.csv`

Ingesta manual:

`python -m web_comparativas.dimensionamiento.ingestion --mode replace`

Auto-ingesta al arrancar la app:

`DIMENSIONAMIENTO_AUTO_INGEST=true`
