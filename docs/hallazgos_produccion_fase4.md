# Hallazgos reales de producción (PostgreSQL Render) — Fase 4

> Fuente: diagnóstico read-only sobre `web_comparativas_db` (Render), snapshot
> `docs/db_snapshot_prod.json` + comparación `docs/db_local_vs_prod.md`.
> **Sin** filas de datos ni credenciales. Producción **no fue modificada**.

---

## 1. Resumen ejecutivo

- **66 tablas** en producción (vs 52 en SQLite local). 15 tablas existen solo en prod.
- **14 tablas `forecast_*`** (4 en ORM + 10 de datos base fuera del ORM).
- **Dos familias de forecast** conviven (hallazgo nuevo): una **vieja/usada** y otra
  **nueva tipada/sin referencias en el código** (ver §2).
- **Bloat de índices** real: `dimensionamiento_records` tiene **27 índices** y
  `dimensionamiento_family_monthly_summary` **26**. Eso explica el >1 GB: gran parte es
  índice, no dato.
- Tablas **legado ajenas al dominio** descubiertas en prod: `app_users`, `clients`,
  `products`, `orders`, `order_items` (parecen restos de un scaffold inicial; sin uso).
- `usage_events`: **68.752 filas** y creciendo (sin retención).
- Caveat de conteo: con `--estimate`, varias tablas muestran `rows=-1` = "nunca
  analizadas por PostgreSQL" (no necesariamente vacías). Para empties exactos, correr el
  diagnóstico **sin** `--estimate`.

---

## 2. Mapa real de tablas `forecast_*`

### 2.1 Familia VIEJA — **usada por la app** (`forecast_service.py`)
Cargadas por `migrate_forecast_csv_to_postgres.py` (`to_sql index=False` → **sin PK**,
columnas **TEXT**). Tienen **índices duplicados** por convención de nombres (`idx_fc_*`
y `ix_fc_*`).

| Tabla | Filas | Tamaño | PK | Cols | Índices | Duplicados detectados |
|---|---:|---:|---|---:|---:|---|
| `forecast_main` | 277.452 | 114.0 MB | ❌ | 27 (TEXT) | 9 | `idx_fc_main_codigo_serie`=`ix_fc_main_codigo_serie`; `idx_fc_main_perfil`=`ix_fc_main_perfil` |
| `forecast_valorizado` | 702.436 | 263.6 MB | ❌ | 18 | 11 | `idx_fc_val_cliente_id`=`ix_fc_val_cliente_id`; `idx_fc_val_fecha`=`ix_fc_val_fecha` |
| `forecast_fact_2026` | 206.246 | 46.5 MB | ❌ | 12 | 5 | — (revisar `idx_ff26_*` vs `ix_fc_fact2026_*`) |
| `forecast_imp_hist` | 44.861 | 6.9 MB | ❌ | 6 | 3 | — |
| `forecast_product_labs` | 2.914 | 0.7 MB | ❌ | 2 | 2 | `idx_fc_labs_cdg`=`ix_fc_labs_codigo` |

### 2.2 Familia NUEVA tipada — **sin referencias en el código de la app** ⚠️
Tienen **PK `id`**, tipos **VARCHAR/INTEGER/DOUBLE**, índices `ix_forecast_*` consistentes.
**0 referencias** en `web_comparativas/*.py` (grep). Parecen un pipeline/dataset nuevo
no cableado, o legado de un experimento.

| Tabla | Filas | Tamaño | PK | Rol aparente |
|---|---:|---:|---|---|
| `forecast_base` | 966.072 | 257.9 MB | ✅ id | Serie base tipada (¿reemplazo de `forecast_main`?) |
| `forecast_dataset_base` | 221.424 | 52.3 MB | ✅ id | Dataset de demanda (`qty_mes`) |
| `forecast_articulo` | 121.701 | 25.6 MB | ✅ id | Maestro de artículos |
| `forecast_cliente` | 40.092 | 8.9 MB | ✅ id | Maestro de clientes |
| `forecast_negocio` | 145 | 0.1 MB | ✅ id | Maestro de negocios |

> 🔴 **Para validación humana:** ¿la familia nueva es un pipeline en construcción, un
> reemplazo planeado de la vieja, o legado? Son ~1.35M filas / ~345 MB sin uso visible.
> **No tocar** hasta confirmar.

---

## 3. Riesgos principales (reales)

| # | Riesgo | Evidencia |
|---|---|---|
| RP1 | Ingesta Forecast **destructiva** (`if_exists="replace"`) sobre la familia vieja | `migrate_forecast_csv_to_postgres.py` |
| RP2 | Familia forecast nueva **sin uso visible** (~345 MB) | grep: 0 referencias en código |
| RP3 | **Índices duplicados** en forecast viejo (`idx_fc_*`==`ix_fc_*`) | snapshot: 5+ duplicados exactos |
| RP4 | **Bloat de índices** en dimensionamiento (27 + 26 índices) | snapshot: >1 GB, mayormente índice |
| RP5 | Forecast viejo **sin PK** + columnas TEXT (montos como texto) | snapshot: `forecast_main` y otros |
| RP6 | Tablas legado ajenas (`app_users, clients, products, orders, order_items`) | solo en prod, sin ORM |
| RP7 | `usage_events` sin retención (68.7k y creciendo) | snapshot |
| RP8 | Tablas fantasma confirmadas en prod (`chat_*, runs, dashboards, normalized_files, revision_sessions`) | snapshot |

---

## 4. Comparación SQLite local vs PostgreSQL producción

- **Solo en prod (15):** las 5 forecast nuevas + 5 legado ajenas (`app_users, clients,
  products, orders, order_items`) + algunas con datos reales que en local no se sembraron.
- **Solo en local (1):** ninguna relevante (artefacto de entorno).
- **En ambas (51):** el núcleo del ORM coincide estructuralmente.
- Las `forecast_*` de datos base **solo existen en prod** (esperable: en local son CSV/parquet).

> Detalle completo en `docs/db_local_vs_prod.md` (generado por `db_compare.py`).

---

## 5. Caveat técnico del diagnóstico

`--estimate` usa `pg_class.reltuples`; PostgreSQL devuelve **-1** para tablas nunca
analizadas (`ANALYZE`/autovacuum no corrió). Por eso "tablas vacías: 0" puede subestimar.
Para un conteo de vacías fiable, re-correr **sin** `--estimate` (más lento, exacto) o
ejecutar `ANALYZE` (eso es escritura de estadísticas — **no** lo hacemos en esta fase).
