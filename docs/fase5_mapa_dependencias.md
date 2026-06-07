# Fase 5 — Mapa de dependencias, clasificación y arquitectura objetivo

> **Estado:** Batch 1 (código read-only) aplicado en la rama `chore/db-phase-5-safe-optimizations`.
> Batch 2 (índices/estructura) y Batch 3 (renames/retención) **propuestos**, esperando backup.
> **Base de evidencia:** lectura del código real (no solo el diagnóstico estructural de Fase 4).
> **Regla:** tocar producción solo con evidencia, backup, trazabilidad, validación y rollback.

---

## 1. Mapa real: tabla → endpoint → dashboard

### Forecast (`forecast_service.py`, `routers/forecast_router.py`)
| Tabla | Filas | Lee/Escribe | Endpoints | Panel |
|---|---:|---|---|---|
| `forecast_valorizado` | 702k | Lee (SQL prod / parquet local) | `/api/chart-data`, `/api/client-table`, `/api/treemap-data`, `/api/filter-options` | Fuente canónica de todos los paneles |
| `forecast_main` | 277k | Lee | mismos | Metadata serie/negocio |
| `forecast_imp_hist` | 45k | Lee | `/api/chart-data` | Línea histórica real |
| `forecast_fact_2026` | 206k | Lee | `/api/chart-data`, `/api/client-table` | Línea real 2026 |
| `forecast_product_labs` | 3k | Lee | `/api/product-list`, `/api/filter-options` | Filtro laboratorio |
| `forecast_user_overrides` | — | **Lee+Escribe (ORM)** | `/api/save-client`, `/api/audit` | Curva "Ajustada" |
| `forecast_manual_clients/entries` | — | **Lee+Escribe (ORM)** | `/api/create-manual-client`, `/api/audit` | Detalle operativo |
| `forecast_change_requests` | — | **Lee+Escribe (ORM)** | `/api/approvals` | Aprobaciones (admin) |
| `forecast_base`, `forecast_dataset_base`, `forecast_articulo`, `forecast_cliente`, `forecast_negocio` | 1.35M | **0 referencias** | — | **Ninguno** |

> Confirmado por grep: la familia nueva tipada no la consulta ningún endpoint. Las únicas
> coincidencias textuales eran `forecast_base_consolidado.csv` (archivo) y el DataFrame
> `fact_forecast_base`, que **no** son esas tablas.

### Dimensionamiento / Mercado Privado (`dimensionamiento/*`, `routers/dimensiones_router.py`, `routers/mercado_privado_perfiles_router.py`)
| Tabla | Rol | Endpoints | Panel |
|---|---|---|---|
| `dimensionamiento_family_monthly_summary` (~260k) | Agregado mensual (fuente preferida) | `/kpis`, `/series`, `/results`, `/top-families`, `/geo`, `/clients-by-result`, `/family-consumption`, perfiles cliente/artículo (monto, plataforma, clientes, subnegocio…) | Dashboard Dimensiones |
| `dimensionamiento_records` (~319k) | Base cruda + **fallback** | mismos cuando summary "no usable"; **siempre** en `articulo/kpis`, `articulo/precio-evolucion`, `articulo/consumo-mensual`, `cliente/consumo-mensual` | KPIs/precio/consumo de artículo |
| `dimensionamiento_dashboard_snapshots` | Caché JSON | `/bootstrap` (sin filtros) | Carga inicial |
| `dimensionamiento_import_runs/errors` | Auditoría ingesta | `/status`, `/debug-snapshot` | Admin |

### Mercado Público / Perfiles (`routers/mercado_publico_perfiles_router.py`, `services.py`)
| Tabla | Filas | Rol | Endpoints |
|---|---:|---|---|
| `comparativa_rows` | 86k | Fuente del Reporte de Perfiles | `/articulos/*`, `/competidor/*`, `/cliente/*`, `/filtros*` |
| `uploads` | — | Proceso + BLOBs (`original_content`, `normalized_content`, `dashboard_json`) | carga/descarga/dashboard |
| `comments` | — | Hilos por upload | comentarios |

### Transversales / legado
| Tabla | Veredicto | Evidencia |
|---|---|---|
| `usage_events` (68k↑) | **Activa, sin retención** | Escritura: middleware + `/api/heartbeat` + `/api/track-activity`. Lectura: panel Live/presencia |
| `usage_sessions` | **SIN USO** (schema, nunca poblada) | 0 `.add()` en el código |
| `runs`, `normalized_files`, `dashboards` | **Solo DELETE de limpieza** | Solo en `_delete_upload_by_id` y reset masivo |
| `chat_channels/members/messages` | **Solo DELETE** (feature nunca hecha) | Sin router ni escritura |
| `revision_sessions` | **SIN USO** | Sin instanciación |
| `password_reset_requests` | **Activa** (flujo reset) | Modelo + flujo SIC |
| `app_users`, `clients`, `products`, `orders`, `order_items` | **NO están en el ORM** | Sin `class ...(Base)`; si existen en prod, son scaffold externo huérfano |

---

## 2. Clasificación por estado y riesgo

- **Activa crítica:** forecast viejo (5) + overrides (4), `dimensionamiento_records/summary/snapshots`, `comparativa_rows`, `uploads`, `comments`, pliegos (20), núcleo de identidad.
- **Activa secundaria:** `usage_events`, `import_runs/errors`, `notifications`, `tickets`.
- **Fuente base (no perder):** forecast viejo, `records`, `uploads`, `comparativa_rows`.
- **Derivada / recalculable:** `summary`, `dashboard_snapshots` (se regeneran desde records/uploads).
- **Sin uso / huérfana probable:** `usage_sessions`, `revision_sessions`, familia forecast nueva (a validar provenance antes de cualquier cosa).
- **Legacy probable (solo DELETE):** `runs`, `normalized_files`, `dashboards`, `chat_*`.
- **Legacy ajeno (fuera del ORM):** `app_users`, `clients`, `products`, `orders`, `order_items`.

> Ninguna se borra en Fase 5. Las huérfanas/legacy, una vez validadas, van a
> `zz_deprecated_*` (rename reversible), nunca `DROP`.

---

## 3. Arquitectura objetivo propuesta

### Forecast
1. **Corto plazo:** la familia **vieja** sigue siendo la fuente viva; se modela *read-only*
   en el ORM (scaffold en `forecast_models_proposed.py` de Fase 4).
2. **Familia nueva:** investigar provenance (conteos, rangos de fecha, *sample join*
   viejo↔nuevo) para decidir si es un refresh mejor o un experimento muerto. **No tocar**
   hasta confirmar. Es ~345 MB / 1.35M filas sin uso.
3. Si la nueva se valida como mejor fuente → **vista de compatibilidad** (`VIEW` con
   nombres/tipos viejos) y migración gradual de lecturas; recién después, deprecar la vieja.
4. **Índices duplicados** del viejo (`idx_fc_*` == `ix_fc_*`): desperdicio puro → drop de
   uno por par (Batch 2, con backup + `CONCURRENTLY`).
5. **Montos en TEXT:** no se hace `ALTER` sobre la tabla viva; se castea en query y se
   corrige el tipo en la fuente consolidada/nueva.

> Corrección al plan original: el "OR→IN" no es un win real (las cadenas OR son de ≤2-3
> términos). El costo real de forecast es traer ~700k filas a Python y `.iterrows()`.

### Dimensionamiento
- El `summary` casi no comprime (260k vs 319k) porque su clave incluye `cliente_visible`
  (alta cardinalidad). **Propuesta:** un **segundo summary más grueso, sin dimensión
  cliente**, para los paneles que no necesitan cliente (KPIs, series, geo, top-families).
  Tabla **aditiva** → poblar → validar → redirigir lecturas. No se toca el summary actual.
- **Índices:** `records` 27 / `summary` 26. Auditar `idx_scan` (pg_stat_user_indexes) +
  `EXPLAIN` de los endpoints reales → proponer retiro de los no usados/duplicados.

### Mercado Público / Privado
- Todas las medianas/percentiles a SQL (`percentile_cont` en Postgres, fallback Python en
  SQLite). La mediana **debe** leer `records`/`comparativa_rows` (no es sumable), pero se
  calcula en la DB y vuelve **un número**, no cientos de miles de filas por la red. **(Batch 1)**

---

## 4. Plan operativo por batches

### Batch 1 — APLICADO (solo código, sin tocar estructura ni datos)
| # | Tabla(s) | Cambio | Riesgo | Backup | Mejora | Validación | Reversa |
|---|---|---|---|---|---|---|---|
| 1.1 | `dimensionamiento_records` | `privado/articulo/kpis` y `precio-evolucion`: mediana Python → `percentile_cont` (PG) + fallback SQLite | Bajo (read-only) | No | No streamear ~319k filas/llamada | `phase5_parity_check.py` (PG) + SQLite idéntico | `git revert` |
| 1.2 | `comparativa_rows` | `articulos/kpis` mediana → SQL; `articulos/por-proveedor` mediana + último precio → `percentile_cont` + `DISTINCT ON` | Bajo | No | Elimina scan sin LIMIT del histórico por proveedor | `phase5_parity_check.py` | `git revert` |

**Diferidos de Batch 1 (documentados, no cambiados):**
- `articulo/consumo-mensual` y `cliente/consumo-mensual` (privado): ya agregan en SQL por
  familia+año+mes; la mediana en Python es sobre pocos totales anuales → no es cuello de
  botella. Cambiarlos suma riesgo sin beneficio.
- `articulos/evolucion-marca` (público): la agrupación normaliza la marca (`upper`+trim+
  colapso de espacios) y elige un "display" por orden de iteración en Python; replicar eso
  en SQL con paridad exacta es delicado → Batch 2 con cuidado.
- Forecast `.iterrows()`/agregaciones: DataFrames de pocos miles de filas (ms) en un
  archivo de 7000 líneas → bajo impacto / riesgo medio → Batch 2 con profiling.

### Batch 2 — PROPUESTO (gateado por backup verificado; ver `fase5_backup_rollback.md`)
- Segundo `summary` grueso para Dimensiones (tabla **aditiva**: crear→poblar→validar→redirigir).
- Índices compuestos nuevos en `comparativa_rows` (`CONCURRENTLY`, reversible).
- Drop de índices forecast **duplicados** exactos (`CONCURRENTLY`, con backup + DDL guardado).
- Auditoría `idx_scan` de los 27/26 índices de dimensionamiento → lista de candidatos a retiro.
- `evolucion-marca` y vectorización forecast con parity test dedicado.

### Batch 3 — PROPUESTO (aprobación explícita)
- Rename de huérfanas/legacy validadas a `zz_deprecated_*` (reversible, nunca DROP).
- Retención/archivado de `usage_events`.
- Decisión final de la familia forecast nueva (tras provenance).
- Baseline Alembic en prod (`stamp`), tras backup.

---

## 5. Validación de paridad (Batch 1)

- `percentile_cont(0.5)` ≡ `statistics.median` (interpolación lineal en la mediana =
  promedio de los dos centrales). Verificado numéricamente: 0 discrepancias en 2000
  pruebas (n par e impar) tras redondear a 2 decimales.
- `DISTINCT ON (proveedor) ... ORDER BY proveedor, fecha_apertura DESC, id ASC` ≡ "primera
  fila por proveedor" del loop Python ordenado igual.
- En **SQLite** la app usa la **misma ruta Python** que antes (paridad local exacta por
  construcción). El path PostgreSQL es matemáticamente equivalente y se valida con
  `scripts/phase5_parity_check.py` contra una **copia de producción** antes del deploy.

---

## 6. Pendientes de validación humana

1. **Familia forecast nueva** (`forecast_base`, …): ¿WIP, reemplazo planeado o legado? (gate de §3.2)
2. `app_users/clients/products/orders/order_items`: ¿se pueden deprecar? (confirmar que no
   son alimentadas por un proceso externo)
3. `chat_*` / `revision_sessions`: ¿features canceladas?
4. `runs/normalized_files/dashboards`: confirmar 0 escritura en el procesamiento actual.
5. Vía de **backup** de Render disponible (snapshot vs `pg_dump`) → gate del Batch 2.
