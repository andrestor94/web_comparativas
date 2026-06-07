# Comparación de esquema — SQLite local vs PostgreSQL producción

> Local: **sqlite / db=app.db** · Prod: **postgresql / db=web_comparativas_db**
> Generado por `scripts/db_compare.py` (offline, sin conexión a base).

## Resumen

- Tablas solo en LOCAL: **1**
- Tablas solo en PRODUCCIÓN: **15**
- Tablas en ambas: **51**

## Tablas solo en PRODUCCIÓN (no en local)

- `app_users` (-1 filas)  ← REVISAR
- `clients` (-1 filas)  ← REVISAR
- `forecast_articulo` (121701 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_base` (966072 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_cliente` (40092 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_dataset_base` (221424 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_fact_2026` (206246 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_imp_hist` (44861 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_main` (277452 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_negocio` (145 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_product_labs` (2914 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `forecast_valorizado` (702436 filas)  ← forecast_* datos base (ESPERABLE: en local son CSV/parquet)
- `order_items` (-1 filas)  ← REVISAR
- `orders` (-1 filas)  ← REVISAR
- `products` (-1 filas)  ← REVISAR

## Tablas solo en LOCAL (no en producción)

- `oportunidades` (794 filas)  ← REVISAR (¿fantasma/legado que prod no tiene?)

## Diferencias de columnas (tablas en ambas)

### `pliego_hallazgos`
- ⚠️ Tipo distinto en `datos_extra`: local=`JSON` vs prod=`JSONB`


## Clasificación de diferencias

**Esperables (no riesgosas):**

- Tablas `forecast_*` de datos base presentes solo en producción (en local son CSV/parquet).
- Tablas vacías en local que en prod tienen datos (entornos distintos).

**Potencialmente riesgosas (validar):**

- ⚠️ pliego_hallazgos.datos_extra: JSON (local) vs JSONB (prod)
- Tablas solo en local (`oportunidades`): confirmar si son fantasma/legado.

---
_Comparación estructural. No incluye filas de datos ni credenciales._
