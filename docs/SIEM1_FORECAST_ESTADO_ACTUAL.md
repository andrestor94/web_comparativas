# Forecast SIEM 1.0 — cómo funciona HOY

Documento de referencia para SIEM 2.0. Levantado del código y de los datos, no de la
documentación previa.

**Fecha de relevamiento:** 2026-07-29
**Rama:** `main` (commit base `41defd7e`)

---

## ⚠️ ALCANCE DE LA VERIFICACIÓN — LEER PRIMERO

Hay tres niveles de evidencia en este documento y conviene no mezclarlos:

| Nivel | Qué verifiqué | Confianza |
|---|---|---|
| **A — Código** | Leído en `main`, con archivo y línea. Es el mismo código que Render deploya (Render deploya `main`). | Alta |
| **B — Datos fuente** | Archivos en `web_comparativas/data/forecast_data/` que son literalmente el insumo de las tablas de producción (los scripts de carga leen de ahí). Conté sobre los archivos. | Alta para estructura/grano; media para "cuántas filas hay en prod hoy" |
| **C — Base de producción** | **NO VERIFICADO.** No hay `DATABASE_URL` en el entorno (verificado: `echo $DATABASE_URL` → vacío; `.env` raíz y `web_comparativas/.env` no la contienen). No pude consultar el PostgreSQL de Render. |

**La base local (`web_comparativas/app.db`, SQLite 2,3 GB) NO contiene los datos del
Forecast.** Verificado: `sqlite_master` lista 70 tablas y de Forecast solo están las 4 de
metadatos (`forecast_user_overrides`, `forecast_change_requests`, `forecast_manual_clients`,
`forecast_manual_entries`). Las tablas de datos (`forecast_valorizado`, `forecast_main`,
`forecast_imp_hist`, `forecast_fact_2026`) **existen solo en PostgreSQL de producción**;
en local el servicio lee los archivos parquet/CSV directamente a memoria.

Además, los datos de las 4 tablas de metadatos en la base local **son de prueba**
(`tester@example.com`, `a@example.com`, `b@example.com` — 289 de 433 registros), y la tabla
`users` local tiene 4 usuarios. Esos números **no son los de producción**.

---

## 1. QUÉ ES Y PARA QUÉ SE USA

### Qué muestra, en criollo

El módulo pone en un solo gráfico cuatro curvas de plata (`web_comparativas/templates/forecast/index.html:2420-2445`
y el armado en `forecast_service.py:6183-6200`):

1. **Histórico real** — lo que se facturó en 2025 (`forecast_imp_hist`).
2. **Proyección del modelo** — lo que un modelo estadístico externo dice que se va a vender
   en 2026 (`forecast_valorizado.monto_yhat`), con banda inferior/superior (`monto_li`/`monto_ls`).
3. **Proyección + expectativa de crecimiento** — la misma proyección multiplicada por un
   porcentaje de crecimiento comercial (hoy fijo en **25 %**, ver bloque 7).
4. **Proyección ajustada por usuario** — lo mismo pero con los ajustes manuales que los
   comerciales cargaron cliente por cliente.

Y encima, **la facturación real de 2026 mes a mes** (`forecast_fact_2026`), para ver si la
realidad va acompañando la proyección.

Abajo, un treemap de composición por grupo/cliente y una grilla cliente × mes editable.

### Quién lo usa

El acceso se decide por dos condiciones que se aplican en AND (`policy.py:536-550`,
`policy.py:603-621`): la key `forecast` tiene que estar en el `module_access` del usuario
**y** el rol tiene que permitirla. O sea: **no es "todos los de rol X"**, es concesión
explícita por usuario.

Roles con capacidades diferenciadas (verificado en código):

| Capacidad | Dónde está la regla | Quién |
|---|---|---|
| Ver el módulo | `policy.py:603` `require_module("forecast")` | cualquiera con la key concedida |
| Editar ajustes de un cliente | `forecast_router.py:542-586` (`/api/save-client`) — **solo `require_module`, sin guard de rol** | cualquiera con acceso al módulo |
| Ver ajustes de **todos** los usuarios (consolidado) | `forecast_router.py:112-131` `_can_view_global_forecast_adjustments` | admin, auditor, gerente/manager |
| Ver pestaña Aprobaciones | `policy.py:207-216` `puede_ver_aprobaciones_forecast` | admin, gerente, auditor |
| Aprobar / rechazar | `policy.py:218-228` `puede_editar_aprobaciones_forecast` | **solo admin y gerente** |
| Auditoría de ajustes (`/api/audit*`) | `forecast_router.py:1021-1044` `_require_admin_only` | **solo admin** |
| Borrar clientes/entradas manuales | `forecast_router.py:767-772` `_require_admin` | solo admin |

### Qué se decide con eso

**NO VERIFICADO** — no hay en el repositorio ningún documento que declare el uso de negocio.
Lo que sí puedo afirmar del código: el módulo produce un **número de proyección anual 2026 en
pesos** y una **meta de avance** (`meta_completeness` = facturado/proyectado, `forecast_service.py:6193`),
y tiene un circuito formal de aprobación de las modificaciones de los comerciales. Eso es
consistente con un uso de presupuesto/meta comercial, pero es inferencia mía, no verificación.

### Frecuencia de uso

Verificado **solo en la base local** (`usage_events`, que en local tiene 4 usuarios y es una
copia de desarrollo — no sirve como medida de producción):

```sql
select section, count(*) from usage_events group by 1 order by 2 desc;
-- forecast → 2.956 eventos (la sección MÁS usada de todo SIEM en esa base)
select user_role, count(*) from usage_events where section like '%forecast%' group by 1;
-- admin 2.845 | auditor 79 | gerente 24 | analista 6 | supervisor 2
select substr(timestamp,1,7), count(*), count(distinct user_id) ... group by 1;
-- 2026-05: 1.566 (3 usuarios) | 2026-06: 1.188 (3) | 2026-07: 202 (1)
-- rango: 2026-05-14 → 2026-07-20, 4 usuarios distintos, 1 solo export
```

**Momento del año: NO VERIFICADO** como política. Lo único duro es que el dato tiene una
estacionalidad estructural: la proyección cubre **exactamente el año calendario 2026**
(2026-01 a 2026-12, 12 períodos) y el histórico va de 2024-01 a 2025-12. Es un ciclo anual.

---

## 2. TABLAS

### 2.1 Mapa general

| Tabla | Motor | Rol | Origen |
|---|---|---|---|
| `forecast_valorizado` | PG prod | **Proyección valorizada por cliente** (el corazón) | `fact_forecast_valorizado.parquet` |
| `forecast_main` | PG prod | Serie histórica + proyección a nivel serie, con `neg`/`subneg`/`precio` | `forecast_base_consolidado.csv` + `Articulos 1.csv` |
| `forecast_imp_hist` | PG prod | Facturación real **2025** (histórico en $) | `importe_historico.csv` |
| `forecast_fact_2026` | PG prod | Facturación real **2026** (contra la que se compara) | `facturacion_real_2026_sin_neg2.csv` |
| `forecast_product_labs` | PG prod | Mapa serie → laboratorios (filtro) | `dataset_base.csv` + `Articulos 1.csv` |
| `forecast_valorizado_summary` | PG prod | Agregado precalculado (perf) | derivada |
| `forecast_product_summary` | PG prod | Agregado precalculado (perf) | derivada |
| `forecast_user_overrides` | PG + SQLite | Ajustes % de los comerciales | ORM |
| `forecast_change_requests` | PG + SQLite | Registro de control / aprobaciones | ORM |
| `forecast_manual_clients` | PG + SQLite | Clientes agregados a mano | ORM |
| `forecast_manual_entries` | PG + SQLite | Líneas artículo-mes-importe de esos clientes | ORM |

Las 7 primeras se crean con `df.to_sql(..., if_exists="replace")` desde pandas
(`migrate_forecast_csv_to_postgres.py`) → **no tienen PK, ni UNIQUE, ni constraints**. Solo
índices creados a mano después de cada carga. Verificado línea por línea abajo.

---

### 2.2 `forecast_valorizado` — LA TABLA PRINCIPAL

**Para qué sirve:** guarda, para cada cliente y cada mes de 2026, cuánto proyecta el modelo
que ese cliente va a comprar de esa serie, en unidades y en pesos.

**DDL:** no hay DDL explícito. La tabla se crea por inferencia de pandas en
`migrate_forecast_csv_to_postgres.py:139` (`chunk.to_sql("forecast_valorizado", ..., if_exists=mode)`).
Columnas y tipos verificados leyendo el parquet fuente:

```
periodo                object          -- "2026-06"
fecha                  datetime64[ns]  -- 2026-06-01
codigo_serie           object          -- nombre de la FAMILIA (es la clave de producto)
perfil                 string          -- IPR/OES/DRO/FAR/... (15 valores)
cliente_id             string
yhat_cliente           int64           -- proyección en UNIDADES
li_cliente             int64           -- banda inferior, unidades
ls_cliente             int64           -- banda superior, unidades
monto_yhat             float64         -- proyección en PESOS  ← lo que ve el usuario
monto_li               float64
monto_ls               float64
nivel_agregacion       object          -- FAMILIA / ARTICULO
descripcion            object
clasificacion_serie    object          -- NORMAL / INTERMITENTE / NO ELEGIBLE
```

Al cargar se le agregan por merge (`migrate_forecast_csv_to_postgres.py:104-141`):
`fantasia`, `nombre_grupo` (desde `clientes.csv`), `neg`, `subneg` (desde `Negocios.csv` vía
`_apply_neg_names`).

**Índices:** `migrate_forecast_csv_to_postgres.py:150-152` crea 4 índices simples sobre
`fecha`, `perfil`, `codigo_serie`, `cliente_id`.

**Clave primaria: NO EXISTE. Clave única: NO EXISTE.** No hay `PRIMARY KEY` ni
`UNIQUE` en el script de carga. Consecuencia práctica: no hay upsert posible y no hay nada
que impida filas duplicadas — y de hecho las hay (ver abajo).

**GRANO — verificado contando, no asumido:**

```python
df = pd.read_parquet("fact_forecast_valorizado.parquet")   # 702.436 filas
df.duplicated(subset=["cliente_id","codigo_serie","fecha"]).sum()          # 31.573 dups
df.duplicated(subset=["cliente_id","codigo_serie","perfil","fecha"]).sum() #     18 dups
df.duplicated(subset=["codigo_serie","perfil","fecha"]).sum()              # 594.472 dups
```

→ **El grano es `cliente_id × codigo_serie × perfil × mes`.**
El `perfil` es parte del grano (sin él quedan 31.573 duplicados: un mismo cliente aparece bajo
más de un perfil comercial). Con perfil quedan **18 filas duplicadas residuales** — todas de
la serie `AGUJA DESCARTABLE 25X8 21GX1"`, 18 pares con valores distintos entre sí (ej.
cliente 13115, 2026-05, IPU: 9.713 y 13.598 unidades). Son un defecto de datos, no una
dimensión faltante.

**Filas hoy:** en el parquet fuente, **702.436**. En producción: **NO VERIFICADO** (sin
acceso a la base). El código lo asume así — comentario en `forecast_service.py:83`:
*"Canonical slim parquet (9MB, 702K rows … $121.7B)"*, y el total del parquet da
$121.742.106.031,20, que coincide.

---

### 2.3 `forecast_main`

**Para qué sirve:** la salida cruda del modelo (histórico + proyección) a nivel serie, sin
cliente. Es de donde salen `neg`, `subneg` y `precio`.

**Origen:** `forecast_base_consolidado.csv` procesado por `_process_dataframe`
(`forecast_service.py:2562`) + `_apply_neg_names` + `_apply_prices`
(`migrate_forecast_csv_to_postgres.py:33-40`).

Columnas del CSV fuente (verificado, `sep=";"`, `decimal=","`):
`periodo, codigo_serie, nivel_agregacion, perfil, Neg, Subneg, Familia, tipo, y, yhat, li, ls,
submodelo, clasificacion_serie, version_param` (15 columnas).
El procesamiento agrega `articulo` (= copia de `codigo_serie`), `descripcion`, `fecha`,
`precio` → 22 columnas en el DataFrame final (verificado en runtime: `df_main.shape == (277452, 22)`).

**Índices:** `migrate_forecast_csv_to_postgres.py:46-48` sobre `perfil`, `neg`, `subneg`, `codigo_serie`.
**PK/UNIQUE: no existen.**

**GRANO — verificado:**
```python
df.duplicated(subset=["periodo","codigo_serie","perfil","tipo"]).sum()  # 0 dups / 277.452
```
→ **`codigo_serie × perfil × mes × tipo`**, siendo `tipo ∈ {hist, forecast}`.

**Filas:** 277.452 (220.008 `hist` de 2024-01 a 2025-12 + 57.444 `forecast` de 2026-01 a
2026-12). 3.359 series distintas.

⚠️ Nota de tipos en producción: `forecast_service.py:3985-3987` documenta que
*"y/yhat are TEXT in production"* — pandas los infirió como texto al cargar. Por eso el
camino de "unidades" del gráfico está muerto en prod (ver bloque 12).

---

### 2.4 `forecast_imp_hist` — histórico real 2025

Origen `importe_historico.csv` (`migrate_forecast_csv_to_postgres.py:157-170`).
Columnas: `periodo, codigo_serie, perfil, imp_hist` + agregadas `tipo='hist'` y `fecha`.
Un solo índice, sobre `perfil`. Sin PK.

**GRANO:** `codigo_serie × perfil × mes`. **Sin cliente.**
Medido en runtime tras el filtro canónico que aplica el servicio: **38.758 filas**,
2.713 series, 2025-01 → 2025-12, **$98.028.987.936**.

El código documenta el filtro (`forecast_service.py:3963-3967`): sin restringir a las series
que existen en `forecast_valorizado`, la tabla devuelve *"44.861 rows / $109.1B (all series).
With it: 38.758 rows / $98.0B — the correct real-2025 baseline"*.

---

### 2.5 `forecast_fact_2026` — facturación real 2026

Origen `facturacion_real_2026_sin_neg2.csv` (`migrate_forecast_csv_to_postgres.py:172-205`
para la carga histórica, y `scripts/load_fact_2026_safe.py` para las recargas reales a prod).

Columnas del CSV (verificado, `sep=";"`, `decimal=","`, UTF-8 BOM):
`fecha; codigo_serie; perfil; cliente_id; familia; descripcion; nivel_agregacion;
articulo_codigo; y; imp_hist; tipo` (11 columnas).
Al cargar se agrega `tipocli` por join con `clientes.csv` (`load_fact_2026_safe.py:178-197`) y
se buckettea `fecha` a inicio de mes.

**Índices:** sobre `tipocli` y `cliente_id`. **Sin PK.**

**GRANO — verificado contando:**
```python
df.duplicated(subset=["fecha","cliente_id","codigo_serie","perfil"]).sum()   # 7.800 dups
df.duplicated(subset=["fecha","cliente_id","articulo_codigo","perfil"]).sum() #    9 dups
```
→ **`cliente × artículo (articulo_codigo) × perfil × mes`.**
**Este es el punto clave: la facturación real está a nivel ARTÍCULO. La proyección no.**
3.860 `articulo_codigo` distintos contra 3.311 `codigo_serie`.

**Filas en el archivo de hoy:** 313.197 · **$52.278.784.233,33** · Ene→Jun 2026 · 5.605
clientes · 1.959 filas con importe negativo (−$475,6 M) · 40 `codigo_serie` vacíos ·
`perfil='NAN'` presente.

⚠️ Ver bloque 12: el archivo en disco **no coincide** con lo último que se registró como
cargado a producción.

---

### 2.6 Las 4 tablas ORM (metadatos)

DDL real en `web_comparativas/models.py`. Row counts de la **base LOCAL** (dev, con datos de
prueba) — producción NO VERIFICADA.

#### `forecast_user_overrides` (`models.py:371-441`)

21 columnas. **PK:** `id`.
**UNIQUE real** (`models.py:379-391`):
`(user_id, source_module, context_key, client_selector, override_scope, subneg, codigo_serie, forecast_month)`
→ nombre `uq_forecast_user_override_scope`. 11 índices.

Columnas relevantes: `client_selector`, `override_scope` (`subnegocio|producto|celda`),
`subneg`, `codigo_serie`, `forecast_month`, `base_growth_pct`, `override_growth_pct`,
`effective_monthly_pct`, `effective_from_month`, `is_active`, timestamps y `created_by`/`updated_by`.

**GRANO:** una fila = un ajuste % de **un usuario** sobre **un alcance** (que según el scope
es cliente / cliente+subnegocio / cliente+serie+mes).
**Filas (local):** 57.

#### `forecast_change_requests` (`models.py:495-560`)

28 columnas. PK `id`. **Sin UNIQUE.** 6 índices.
Snapshot del cambio: quién, qué alcance, valor anterior/nuevo, impacto estimado, y el flujo
`status` (`pendiente|aprobado|rechazado`) + revisor. FK blanda a `forecast_user_overrides.id`
con `ondelete="SET NULL"`.
**Filas (local):** 433.

#### `forecast_manual_clients` (`models.py:445-464`) — 10 filas locales
#### `forecast_manual_entries` (`models.py:467-492`) — 15 filas locales

Grano de `forecast_manual_entries`: `client_id × codigo_serie × forecast_month`, con
`cantidad`, `costo_unitario`, `monto_total`. **Sin UNIQUE** → nada impide duplicar la misma
línea.

---

## 3. EL GRANO DE LA PROYECCIÓN — la pregunta central

### ¿Artículo o familia? → **FAMILIA**, casi sin excepción

Contado sobre el parquet que alimenta `forecast_valorizado`:

```python
df["nivel_agregacion"].value_counts()
# FAMILIA     702.400
# ARTICULO         36

df.groupby("nivel_agregacion")["codigo_serie"].nunique()
# ARTICULO        1
# FAMILIA     3.038
```

**99,995 % de las filas son FAMILIA.** Las 36 filas ARTICULO son **una sola serie**
(`INDOCIANINA VERDE 25 MG VERDYE`), 4 clientes, y su `monto_yhat` suma **$0**. Es ruido, no
una capacidad del sistema.

Y no es solo la columna: en `forecast_base_consolidado.csv` (la salida cruda del modelo) el
`nivel_agregacion` es **FAMILIA en las 277.452 filas, sin una sola excepción**. En
`dataset_base.csv` (221.424 filas) idem: **100 % FAMILIA**.

Confirmación estructural: `codigo_serie` **es** el nombre de la familia. En
`dataset_base.csv` las columnas `codigo_serie` y `Familia` son idénticas fila por fila
(verificado en el head: `(100)ASA 1 UL 8/1 PEEL` en ambas). Y en el código,
`_process_dataframe` (`forecast_service.py:2567-2568`) hace literalmente:

```python
if "codigo_serie" in df_input.columns and "articulo" not in df_input.columns:
    df_input["articulo"] = df_input["codigo_serie"].astype(str)
```

→ **la columna que en toda la app se llama `articulo` es, en realidad, la familia.**
Es el peor falso amigo del módulo.

### ¿Se desagrega a artículo en algún lado? → **NO**

Grepeé `nivel_agregacion` en todo el servicio (`forecast_service.py`, 12 apariciones) y en el
template. **Ninguna hace un split familia → artículos.** Los usos son:

- `forecast_service.py:2869` y `:3212` — para decidir contra qué columna de `Articulos 1.csv`
  buscar el laboratorio (`FAMILIA` → col `familia`, `ARTICULO`/`ITEM` → col `descrip`).
- `forecast_service.py:5247`, `:6920`, `:7522` — como *default de relleno*, y el default es
  **`"ARTICULO"`**, que es justo el valor equivocado dado que el dato es FAMILIA.
- `index.html:5875` — `{field:'nivel_agregacion', hide:true}` → columna **oculta** en la grilla.

**Conclusión: el Forecast de SIEM 1.0 nunca baja a artículo. La única cosa a nivel artículo en
todo el módulo es la facturación real (`forecast_fact_2026.articulo_codigo`), y no se usa para
desagregar la proyección: la comparación proyectado-vs-real se hace sumando ambos lados a
nivel total/mes.**

### ¿Alguna familia cruza más de un subnegocio? → **SÍ, 26**

Verificado sobre `dataset_base.csv`, que es la fuente del mapeo:

```python
key = df[["codigo_serie","Perfil","Neg","Subneg","Familia","nivel_agregacion"]].drop_duplicates()
g = key.groupby("codigo_serie").agg(nsub=("Subneg","nunique"), nneg=("Neg","nunique"))
(g["nsub"]>1).sum()   # 26 series cruzan más de un Subneg
(g["nneg"]>1).sum()   # 0  series cruzan más de un Neg
```

El cruce **siempre viene por el perfil**. Ejemplo verificado —
`AGUJA DESCARTABLE 16X5 25GX5/8"`, Neg 4 en todos los casos:
Subneg **5** para COM/DPM/DRO/IPR/IPU/OES/PRO/SAN, pero Subneg **1** para **OSP**.
Mismo patrón en `AGUJA DESCARTABLE 30X12 18GX1.1/4"` (IPU → Subneg 1, el resto → 5) y en
`AGUJA ESPINAL PUNCION LUMBAR 18GX3.1/2"` (DPM → 1, resto → 5).

**Pero eso se pierde en la carga.** Verificado en runtime sobre los DataFrames que la app
realmente usa:

```python
df_main.groupby("codigo_serie").agg(ns=("subneg","nunique"))    # series con >1 subneg: 0
df_valorizado.groupby("codigo_serie").agg(ns=("subneg","nunique")) # 0
```

La causa está en `migrate_forecast_csv_to_postgres.py:53-56`:
```python
neg_map = df_main[["codigo_serie"] + join_cols].drop_duplicates("codigo_serie")
```
El `drop_duplicates("codigo_serie")` **se queda con el primer subnegocio y descarta el resto**.
Idem `_apply_neg_names` (`forecast_service.py:2535`), que también hace
`.drop_duplicates(["UNIDAD","SUBUNIDAD"])`.

→ **Para 26 familias, el subnegocio que muestra el sistema es arbitrario (el primero que
apareció en el orden del archivo), no el correcto para cada perfil. Esto importa para SIEM 2.0
porque el ajuste de crecimiento por subnegocio es uno de los alcances principales del módulo.**

### ¿A qué nivel se muestra en pantalla?

| Pantalla | Nivel |
|---|---|
| Gráfico + KPIs | Total mes (sumatoria de todo el filtro) |
| Treemap | Grupo → Cliente (jerarquía de clientes, no de producto) |
| Grilla "Detalle Operativo" | Cliente × mes |
| Modal de cliente | Negocio → Subnegocio → **"artículo"** × mes ← **pero el "artículo" es la familia** |
| Filtro de productos | Lista de `descripcion` = familias |

---

## 4. DE DÓNDE VIENE EL DATO

### Quién produce la proyección

**Un modelo estadístico externo.** No está en este repositorio — verificado: no hay ningún
código de entrenamiento/forecasting en el repo; el servicio solo consume archivos ya
calculados. La evidencia de que es un modelo formal y versionado está en las propias columnas
de `forecast_base_consolidado.csv`:

```python
df["version_param"].value_counts()
# v4.4.0 (WMA+MeanReversion+BandCap)    277.452   (un solo valor)

df["submodelo"].value_counts()
# SM4_DRO_DPM       168.336
# SM1_IPU_OES        98.460
# SM5_IPR_OTHERS     10.656
```

→ Modelo **v4.4.0**, técnica *Weighted Moving Average + Mean Reversion + Band Cap*, con
**3 submodelos segmentados por familia de perfiles**. La numeración (SM1, SM4, SM5) sugiere
que existieron SM2 y SM3 — **NO VERIFICADO** qué son.

Que es externo se refuerza con `LEEME_Foresc_Filtrada.txt`, una nota operativa dejada en la
carpeta de datos: *"Colocar aquí el archivo 00_input/Foresc_Filtrada.csv (mismo nombre) para:
Etapa 3 (consumo ponderado por cliente) …"* → hay un pipeline por **etapas** que corre fuera
de SIEM y deja archivos.

### Cómo entra al sistema

**Archivos en disco + script de migración manual. No hay API, ni upload por pantalla, ni ETL
programado.** Verificado: el único endpoint de escritura de datos base es
`POST /forecast/api/reload` (`forecast_router.py:457-467`), que solo **relee los archivos** —
no ingesta nada.

Dos pipelines distintos:

**(a) Proyección + histórico** — `migrate_forecast_csv_to_postgres.py`, `python migrate_forecast_csv_to_postgres.py`
desde el Shell de Render. Recrea 5 tablas con `to_sql(if_exists="replace")` y las reindexa.
Prioriza el parquet sobre el CSV (`:66-70`): *"Fallback: incomplete copy (110K rows, 1838
series, ~$52B — DO NOT USE for production"*.

**(b) Facturación real 2026** — `scripts/load_fact_2026_safe.py`, pipeline propio de 9 pasos
con backup, staging y confirmación interactiva (`load_fact_2026_safe.py:1-34`).

### Formato exacto

| Archivo | Sep | Decimal | Encoding | Filas |
|---|---|---|---|---|
| `forecast_base_consolidado.csv` | `;` | `,` | UTF-8 BOM | 277.452 |
| `fact_forecast_valorizado.parquet` | — | — | — | 702.436 |
| `facturacion_real_2026_sin_neg2.csv` | `;` | `,` | UTF-8 BOM | 313.197 |
| `importe_historico.csv` | `,` | `.` | UTF-8 | — |
| `Articulos 1.csv` | `,` | `,` | **latin-1** | — |
| `clientes.csv` | `,` | — | **latin-1** | — |
| `dataset_base.csv` | `,` | `.` | UTF-8 | 221.424 |

Ejemplos reales (anonimizando cliente):

`forecast_base_consolidado.csv`
```
periodo;codigo_serie;nivel_agregacion;perfil;Neg;Subneg;Familia;tipo;y;yhat;li;ls;submodelo;clasificacion_serie;version_param
2024-01;(100)ASA 1 UL 8/1 PEEL;FAMILIA;IPU;4;6;(100)ASA 1 UL 8/1 PEEL;hist;0,000000;;;;SM1_IPU_OES;NO ELEGIBLE;v4.4.0 (WMA+MeanReversion+BandCap)
```

`fact_forecast_valorizado.parquet` (una fila)
```
periodo=2026-05 · fecha=2026-05-01 · codigo_serie=1/0917 CIRCUITO ARM RESPIRADOR 2T WESTMED (5703)
perfil=OES · cliente_id=NNNN · yhat_cliente=68 · li_cliente=0 · ls_cliente=136
monto_yhat=1179915.xx · monto_li=0.0 · monto_ls=2359831.xx
nivel_agregacion=FAMILIA · clasificacion_serie=INTERMITENTE
```

`facturacion_real_2026_sin_neg2.csv`
```
fecha;codigo_serie;perfil;cliente_id;familia;descripcion;nivel_agregacion;articulo_codigo;y;imp_hist;tipo
31/1/2026;APOSITO QUIRURGICO ESTERIL 10X20CM;FAR;NNNNNN;APOSITO QUIRURGICO ESTERIL 10X20CM;APOSITO QUIRURGICO ESTERIL 10X20CM X6 - SYRA;FAMILIA;8009152;36;18072,78;val
```

### Frecuencia y quién la ejecuta

**No hay automatización.** Verificado: `render.yaml` no define ninguna variable ni job de
Forecast; el reinicio del servicio no re-ingesta nada. Es un **proceso manual, ejecutado por
un operador con acceso al Shell de Render o con la connection string externa**. La cadencia
real: **NO VERIFICADO** (no hay registro de ejecuciones en el repo ni tabla de runs, a
diferencia de Dimensionamiento que sí tiene `dimensionamiento_import_runs`).

### Validaciones al cargar

**Pipeline (a) — proyección: prácticamente ninguna.** El único control es un `try/except` que
loguea y sigue (`migrate_forecast_csv_to_postgres.py:143-144`):
```python
except Exception as csv_err:
    logger.error("Error reading chunk: %s", csv_err)
```
→ **si un chunk de 25.000 filas falla, se pierde en silencio y la migración se declara
exitosa.** No hay conteo de filas esperado, ni checksum, ni comparación con la carga anterior.

**Pipeline (b) — facturación: sí tiene validaciones** (`load_fact_2026_safe.py:74-78, 143-166`):

| Control | Regla |
|---|---|
| Ancla de negocio | `DRO Abr-2026` debe caer entre **$2.000M y $4.000M** |
| Volumen mínimo | ≥ 100.000 filas |
| Filas DRO mínimas | ≥ 1.000 |
| Columna obligatoria | `perfil` debe existir |
| Rango | descarta todo lo anterior a 2026-01-01 |

**Qué pasa con las filas que fallan** (`load_fact_2026_safe.py:104-121`): el parser cuenta
`bad_rows` (líneas con cantidad de columnas distinta a la cabecera, tras un intento de
re-parseo), **las descarta y solo las loguea**. No se guardan en ninguna tabla de errores.
Verificado sobre el archivo actual: **`bad_rows = 0`**.
Si alguna validación falla, el script tira `ValueError` y **aborta antes de tocar producción**.

### Concepto de "ciclo" o versión de carga

**No existe como entidad del sistema.** No hay tabla de runs, ni `import_id`, ni columna de
versión de carga en las tablas de datos. Recargar el mismo período = **reemplazo destructivo
completo**:

- Pipeline (a): `to_sql(if_exists="replace")` → DROP + CREATE. **Sin backup.**
- Pipeline (b): sí hay resguardo — `CREATE TABLE forecast_fact_2026_backup_<TS> AS SELECT *`
  (`load_fact_2026_safe.py:273`), luego staging, luego `TRUNCATE + INSERT`. El rollback es
  restaurar desde la backup table.

Lo único parecido a una versión **vive dentro del dato**: la columna `version_param` del CSV
del modelo (hoy un único valor, `v4.4.0`). Pero **esa columna no se muestra en ninguna
pantalla ni se consulta en ningún query** (grep: 0 apariciones fuera del archivo).

---

## 5. FACTURACIÓN REAL

**Sí, el módulo compara contra facturación real, y de dos tablas distintas según el año:**

| Serie en pantalla | Tabla | Año | Grano | Total verificado |
|---|---|---|---|---|
| "Histórico" | `forecast_imp_hist` | 2025 | `serie × perfil × mes` (sin cliente) | $98.028.987.936 (38.758 filas, 12 meses) |
| "Fact. 2026" | `forecast_fact_2026` | 2026 | `cliente × artículo × perfil × mes` | $52.278.784.233 (313.197 filas, 6 meses) |

**Cómo se carga:** ver bloque 4. `forecast_imp_hist` por el pipeline (a); `forecast_fact_2026`
por `scripts/load_fact_2026_safe.py`.

**De dónde viene el dato crudo: NO VERIFICADO.** Los CSV están en la carpeta ya armados; no
hay en el repo el extractor que los genera desde el ERP.

**Detalle importante del filtro de perfil** (`forecast_service.py:3944-3948`): en
`forecast_fact_2026` **no se usa la columna `perfil`, se usa `tipocli`** (traída de
`clientes.csv` al cargar). El comentario del código explica por qué:

> *"Using JOIN via forecast_valorizado.perfil was incorrect: it excluded clients present in
> forecast_fact_2026 (tipocli=X) but absent in forecast_valorizado, causing $663M undercount
> for DRO Apr-2026 (73 clients lost)."*

**Corte de meses abiertos** (`forecast_service.py:354-375`, `_fact_2026_closed_month_cap`):
la línea del gráfico y el KPI de facturado solo suman **meses cerrados** (tope dinámico =
primer día del mes en curso). El cálculo de *accuracy*, en cambio, usa su propia definición
(`val_months_2026[:-1]`, `forecast_service.py:6215-6218`) → **dos definiciones distintas de
"mes cerrado" conviviendo en la misma función.**

### ¿Hay histórico? ¿Cuántos meses?

| Fuente | Rango | Meses |
|---|---|---|
| `forecast_main` (tipo=hist, unidades) | 2024-01 → 2025-12 | 24 |
| `forecast_imp_hist` (pesos) | 2025-01 → 2025-12 | **12** |
| `forecast_fact_2026` (pesos) | 2026-01 → 2026-06 | 6 |
| `forecast_valorizado` (proyección) | 2026-01 → 2026-12 | 12 |

**El histórico en pesos es de 12 meses.** Los 24 meses de `forecast_main` están en unidades y
en producción son de tipo TEXT, así que ese camino no rinde (ver bloque 12).

---

## 6. EL CÁLCULO

### Fórmula exacta, paso a paso

Para la curva principal que ve el usuario ("Proyección + expectativa"), mes `m`:

```
1.  base(m)   = SUM(forecast_valorizado.monto_yhat)  WHERE <filtros> AND fecha = m
                (o yhat_cliente si el toggle está en "unidades")

2.  Total_Adj(m) = base(m)                                   si m <= max_hist
                 = base(m) × (1 + growth_pct/100)            si m >  max_hist

    donde max_hist = MAX(fecha) de forecast_main WHERE tipo='hist'  = 2025-12-01 (verificado)
    y growth_pct = 25 (fijo, ver bloque 7)

3.  Para los ajustes del usuario, se calcula un DELTA solo sobre las filas con override:

    delta(m) = Σ_filas  base_val × ( _annual_eff − _eff_base )

    _annual_eff = 1 + override_growth_pct/100    (del override que ganó la precedencia)
    _eff_base   = 1.0                            si m <= max_hist
                = 1 + growth_pct/100             si m >  max_hist

4.  Total_User_Adj(m) = Total_Adj(m) + delta(m)

5.  + entradas manuales:  se SUMAN a Total_Forecast, Total_Adj, Total_User_Adj, Li y Ls
```

Nótese el paso 5: **una entrada manual se suma a la curva "modelo puro" también**, no solo a
la ajustada.

### Dónde vive en el código

| Camino | Archivo:línea | Función |
|---|---|---|
| **Producción (PostgreSQL)** | `forecast_service.py:3825` | `_pg_get_chart_data_inner` |
| ↳ crecimiento global | `forecast_service.py:4030-4034` | inline |
| ↳ delta de overrides | `forecast_service.py:4126-4147` | inline |
| ↳ inyección de manuales | `forecast_service.py:4176-4200` | inline |
| Local (parquet en memoria) | `forecast_service.py:5787` | `get_chart_data` |
| ↳ crecimiento global | `forecast_service.py:6063-6074` | `apply_growth` |
| Resolución del override | `forecast_service.py:821-866` | `_resolve_override_for_row` |
| Aplicación vectorizada | `forecast_service.py:882-1031` | `_apply_override_effects_to_dataframe` |
| KPIs 1-7 | `forecast_service.py:6125-6270` | inline |

### Cómo se aplica el crecimiento — **PLANO**, no capitalizado

Expresión real, `forecast_service.py:4030-4034` (camino de producción):

```python
# Line 3 — Proyección estándar comercial: modelo × (1 + growth_pct/100)
df_fcst["Total_Adj"] = df_fcst["Total_Forecast"]
if growth_pct != 0:
    g = 1.0 + growth_pct / 100.0
    future = df_fcst["fecha"] > max_hist
    df_fcst.loc[future, "Total_Adj"] = df_fcst.loc[future, "Total_Forecast"] * g
```

Y `forecast_service.py:6067-6073` (camino local), idéntico:
```python
growth_factor = 1.0 + (growth_pct / 100.0)
future = df_src["fecha"] > max_hist_date
df_src.loc[future, "Total_Adj"] = df_src.loc[future, col] * growth_factor
```

→ **Es un factor único `1,25` aplicado por igual a los 12 meses de 2026.** No capitaliza, no
rampa. Está explícitamente decidido así y hay un test que lo fija:
`tests/test_forecast_overrides.py:551` `test_global_growth_is_flat_not_quarter_ramped`, cuyo
docstring dice:

> *"La global se aplica PLANA (1 + g/100) … El camino local escalaba por trimestre → con la
> misma global, local proyectaba ~27% mas que prod sobre el mismo dato. Este test fija la
> convergencia local == prod."*

El servicio **sí tiene** helpers de capitalización — `_monthly_pct_from_annual_growth`
(`forecast_service.py:524`) y `_annual_growth_from_monthly_pct` (`:538`) — pero se usan para
**convertir entre la tasa que el usuario escribe (anual) y la que se guarda en la base
(mensual)**, no para proyectar.

### ⚠️ ¿El mismo cálculo está en más de un lugar? — **SÍ, y NO dan el mismo resultado**

Encontré **dos convenciones distintas conviviendo**:

**Grupo A — PLANO** (gráfico, KPIs, grilla de clientes, treemap):
`forecast_service.py:4032, 4127, 4630, 4633, 4643, 4645, 4653, 4655, 4723, 5988, 6023, 6027, 6069, 6428`

**Grupo B — CAPITALIZADO MENSUAL** (modal de detalle de cliente):
`forecast_service.py:5300-5301` (producción) y `forecast_service.py:7006-7008` (local):
```python
t  = (d.year - max_hist_date.year) * 12 + (d.month - max_hist_date.month)
rm = (1 + growth_pct / 100.0) ** (1 / 12.0) - 1
adj = orig * (1 + rm) ** t
```

**Divergencia calculada** (g=25 %, max_hist=2025-12, 12 meses de 2026):

```
Plano       : Σ factores = 12 × 1,25       = 15,000000
Capitalizado: Σ (1,25)^(t/12), t=1..12     = 13,569648
                                    gap    = −9,54 %
```

Sobre la base total del parquet ($121.742.106.031): **$152.177.632.539 (plano) vs
$137.666.457.962 (capitalizado) → una brecha de $14.511 millones.**

Y hay una **segunda divergencia, mucho más grande, en la misma pantalla** — ver bloque 12.1.

---

## 7. EXPECTATIVAS DE CRECIMIENTO / AJUSTES

### Dónde viven

Tabla **`forecast_user_overrides`** (`models.py:371-441`). Columnas en el bloque 2.6.

### Alcances y cuántos hay

`forecast_service.py:104-107` define tres:
```python
FORECAST_SCOPE_SUBNEG  = "subnegocio"
FORECAST_SCOPE_PRODUCT = "producto"
FORECAST_SCOPE_CELL    = "celda"
```

Conteo en la **base LOCAL de desarrollo** (producción NO VERIFICADA):

```sql
select override_scope, is_active, count(*) from forecast_user_overrides group by 1,2;
--  celda        activo    12
--  subnegocio   activo    40
--  subnegocio   inactivo   5
--  TOTAL 57  (0 de scope 'producto')

select case when forecast_month='' then 'VACIO' else 'MES' end,
       case when codigo_serie='' then 'VACIO' else 'COD' end,
       case when subneg=''       then 'VACIO' else 'SUB' end, count(*) ...
--  MES  + COD  + SUB   → 12   (celda: cliente+serie+mes)
--  VACIO+VACIO + SUB   → 41   (subnegocio: cliente+subneg)
--  VACIO+VACIO +VACIO  →  4   (WILDCARD: cliente entero, todos los subnegocios)
```

→ Hay un **cuarto alcance de facto que no está nombrado en las constantes**: el **wildcard de
grupo** (`subneg = ""`), que aplica a todos los subnegocios de un cliente. Se crea desde
`save_group_expectations` (`forecast_service.py:1929`) y se resuelve explícitamente en
`_resolve_override_for_row:855-862`. **4 registros locales.**

El scope `producto` está **declarado pero sin uso** (0 filas locales) y el propio código lo
marca como legacy (`forecast_service.py:840`): *"no effective_from_month restriction — kept
for legacy compat"*.

Rango de valores locales: `override_growth_pct ∈ [0, 90]`, `base_growth_pct ∈ [0, 25]`.
Vigencia: 42 desde `2026-06`, 15 desde `2026-07`.

### Regla de precedencia real — verificada en `_resolve_override_for_row` (`forecast_service.py:821-866`)

El orden es estricto y devuelve en el **primer match**:

```
1º  CELDA        maps["cell"][(selector, codigo, mes)]      → respeta effective_from_month
2º  PRODUCTO     maps["product"][(selector, codigo)]        → legacy
3º  SUBNEGOCIO   maps["subneg"][(selector, subneg)]         → respeta effective_from_month
4º  WILDCARD     maps["subneg"][(selector, "")]             → solo si subneg no vacío
5º  BASE         growth_pct global (25 %)
```

**Segundo eje de desempate — el selector de cliente** (`forecast_service.py:1030-1037` y
`:869-880`). Dentro de cada nivel se prueban los candidatos **en este orden**:

```python
for val in (fantasia_vals[j], cliente_id_vals[j], _cliente_vals[j], Cliente_vals[j]):
```
→ **`fantasia` (nombre) le gana a `cliente_id` (código).** Ojo: significa que los ajustes se
anclan al *nombre de fantasía*, no a un ID estable.

**Tercer eje — entre usuarios distintos** (`forecast_service.py:670-672`): cuando un
admin/auditor/gerente mira el consolidado, se traen los overrides de **todos** los usuarios y
se ordenan `updated_at ASC`, con el comentario *"so later saves win on conflict"*. →
**si dos comerciales ajustan el mismo cliente+subnegocio, en la vista global gana el que
guardó último.** Hay un test que lo cubre: `test_pill_matches_projection_when_two_users_override_the_same_client`.

**Regla adicional — las filas históricas nunca se ajustan** (`forecast_service.py:993-996`):
si `row_date <= max_hist_date`, se saltea el override y la fila queda en base.

### Vigencia

Regla del **día 20**, implementada en `get_forecast_effective_month`
(`forecast_service.py:545-567`):

```python
offset = 1 if today.day <= cutoff_day else 2      # cutoff_day = 20
```
- Guardo el día ≤ 20 → rige desde el **mes siguiente**.
- Guardo el día > 20 → rige desde el **mes subsiguiente**.

Ejemplos del propio docstring: `2026-05-12 → "2026-06"`, `2026-05-20 → "2026-06"`,
`2026-05-21 → "2026-07"`, `2026-12-21 → "2027-02"`.

**Se aplica en el backend, no solo en la UI** (`forecast_service.py:1872, 1892-1898`): el mes
efectivo lo calcula el servidor y los overrides de celda para meses anteriores se
**descartan en silencio** (`continue`, sin avisarle al usuario).

**Hasta cuándo aplica: no hay fecha de fin.** No existe una columna `effective_to_month`. Un
override rige desde su mes efectivo **hasta que alguien lo desactive**.

Los registros con `effective_from_month = NULL` (previos a la regla) **no tienen restricción**
— `efm is None or month_key >= efm` (`forecast_service.py:834`).

### Valor por defecto de crecimiento

**Hay dos defaults distintos, y no coinciden:**

| Capa | Valor | Dónde |
|---|---|---|
| Frontend | **25** | `index.html:3410` `const LOCKED_MAIN_GROWTH_PCT = 25;` |
| Backend (API) | **0.0** | `forecast_router.py:198, 287, 370` `growth_pct: float = Query(default=0.0)` |

→ La pantalla siempre manda 25; pero **cualquiera que pegue directo al endpoint sin el
parámetro obtiene la proyección sin crecimiento**.

### ¿Se puede cambiar desde la interfaz? — **NO**

El input existe pero está **deshabilitado** (`index.html:2429-2430`):
```html
<input type="number" id="growthInput" class="fc-growth-input"
       value="25" step="1" min="-50" max="500" placeholder="25" disabled>
```
Y el JS lo **fuerza en cada render**, pisando incluso lo que venga de una vista guardada
(`index.html:3521, 3551-3552`):
```javascript
STATE.growthPct = LOCKED_MAIN_GROWTH_PCT;
if (growthInput) growthInput.value = LOCKED_MAIN_GROWTH_PCT;
```

→ **El 25 % global está hardcodeado en el JavaScript. Cambiarlo requiere tocar código y
deployar.** Lo que sí puede cambiar el usuario es el % **por cliente / subnegocio / celda**,
que es lo que va a `forecast_user_overrides`.

El 25 aparece hardcodeado además en el backend de Aprobaciones:
`forecast_router.py:2727` `svc.get_chart_data(is_admin=True, growth_pct=25.0)` y
`:2747` `compute_approval_curve_impacts(growth_pct=25.0, ...)`. Son **tres lugares** que hay
que tocar en conjunto.

---

## 8. CIRCUITO DE APROBACIÓN

### Semántica — leer esto primero

Está declarada en `models.py:498-506`:

> *"Semántica = REGISTRO DE CONTROL: el override del cotizador se aplica **al instante**
> (lógica existente intacta). Esta tabla NO bloquea ni revierte el cambio; solo deja
> constancia."*

**El cambio del comercial impacta la proyección inmediatamente, sin esperar aprobación.** La
aprobación es *a posteriori*.

⚠️ Esa docstring **ya no describe el comportamiento real**. El commit `f9ce8040`
*"feat(forecast-aprobaciones): rechazar revierte el override real (compuerta de aprobación)"*
cambió el rechazo para que **sí revierta**. **Gana el código.**

### Estados y transiciones

`models.py:551` → `status ∈ {pendiente, aprobado, rechazado}`, default `pendiente`.
**No existe el estado "anulado"** (grep de `anul` en router y servicio: 0 resultados).

```
                 ┌──────────────┐
  save-client ──►│  pendiente   │
  save-group     └──┬────────┬──┘
  backfill          │        │
                    ▼        ▼
              aprobado   rechazado ──► desactiva el override + cascada a hermanas
```

**No hay transición de vuelta.** Los loops de aprobación masiva saltean todo lo que no esté
`pendiente` (`forecast_router.py:2145-2147`: *"los loops grupo/by-ids saltan las ya
decididas por su guard `status != 'pendiente'`"*).

### Quién puede qué

| Acción | Guard | Roles |
|---|---|---|
| **Proponer** (genera la CR) | ninguno más allá del acceso al módulo | cualquiera con `forecast` concedido |
| **Ver** aprobaciones | `_require_aprobaciones_view` → `policy.py:207` | admin, gerente, auditor |
| **Aprobar / Rechazar** | `_require_aprobaciones_edit` → `policy.py:218` | **admin y gerente únicamente** |
| Aprobar/rechazar por grupo | `forecast_router.py:2950, 2968` — mismo guard | admin, gerente |
| Aprobar/rechazar por IDs | `forecast_router.py:3020` — mismo guard | admin, gerente |
| **Anular** | — | **no existe** |

Las CR se generan automáticamente en `_record_change_request` (`forecast_service.py:1532`),
disparado desde los saves. Los `source` verificados en la base local: `save-client` (354),
`save-group` (27), `backfill` (52).

### Qué pasa exactamente al RECHAZAR

`_apply_review` (`forecast_router.py:2134-2181`), tres efectos:

**(a) Desactiva el override vinculado** — `svc.deactivate_override_by_id(session, cr.override_id, ...)`.
Es soft-delete: pone `is_active = False`. El alcance vuelve a la base del modelo.

**(b) Cascada a las CR hermanas** — todas las CR `pendiente` que apuntan al **mismo
`override_id`** pasan a `rechazado` con el mismo motivo:
```python
siblings = session.query(_CR).filter(_CR.override_id == cr.override_id)
                             .filter(_CR.status == "pendiente").all()
for sib in siblings: _stamp(sib)
```

**(c) Invalida la caché del DUEÑO, no del revisor** (`forecast_router.py:2237-2239`):
```python
# Caché del DUEÑO del override (cotizador), no del admin que rechaza.
if owner_uid is not None:
    svc._clear_cache_for_override_save(owner_uid)
```

**¿Afecta a otros ajustes?** Sí, al grano del override rechazado. Documentado en
`forecast_router.py:2142-2143`: *"Al grano del override (un subneg revierte sus celdas; un
override de celda más fino, separado, sobrevive)"*. Cubierto por
`tests/test_forecast_approvals_revert.py:253` `test_reject_subneg_scope_reverts_whole_subneg`.

**¿Recalcula?** No hay recálculo batch. Se limpia la caché de respuestas y el próximo request
recalcula desde cero.

**Motivo obligatorio al rechazar** (`forecast_router.py:2218-2220`): sin motivo → HTTP 400.

### Qué pasa al APROBAR

**Nada, salvo el sello.** `forecast_router.py:2159-2161`:
```python
if status != "rechazado":
    _stamp(cr)      # status, reviewed_by, reviewed_at, comment
    return None
```
El override **ya estaba aplicado** y sigue igual. Test que lo fija:
`test_forecast_approvals_revert.py:139` `test_approve_does_not_touch_override`.
**Aprobar no invalida caché** (devuelve `None` → no se llama a `_clear_cache_for_override_save`).

### ¿Se puede aprobar lo propio? — **SÍ**

**No hay ninguna verificación de `created_by_user_id != reviewer_id`** en todo el flujo
(revisado `_apply_review`, `api_approvals_approve`, `api_approvals_reject`, `_review_group`,
`_review_by_ids`). Un admin o gerente puede aprobar sus propias modificaciones.

Verificado además que **ya pasó** en la base local:
```sql
select count(*) from forecast_change_requests
 where reviewed_by_user_id is not null and reviewed_by_user_id = created_by_user_id;
-- 19
```

### Estado de la cola (base LOCAL, no producción)

```
status:      pendiente 371 | aprobado 62 | rechazado 0
change_type: ajuste 282 | baja_pct 93 | suba_pct 58
scope_type:  subnegocio 395 | celda 38
rango:       2026-05-20 → 2026-07-20
```

---

## 9. CLASIFICACIÓN Y METADATOS

### Marca de serie no elegible — **SÍ existe: `clasificacion_serie`**

Presente en `forecast_base_consolidado.csv`, en el parquet y por lo tanto en
`forecast_valorizado` y `forecast_main`. Tres valores.

En **`fact_forecast_valorizado.parquet`** (702.436 filas):

| Valor | Filas | Series | $ monto_yhat |
|---|---|---|---|
| NORMAL | 589.643 | 1.060 | $95.065.780.000 |
| INTERMITENTE | 79.280 | 2.292 | $26.676.320.000 |
| **NO ELEGIBLE** | **33.513** | **1.912** | **$0** |

En **`forecast_base_consolidado.csv`** (277.452 filas, incluye histórico):
INTERMITENTE 138.384 · NO ELEGIBLE 105.120 · NORMAL 33.948.

Hay además un archivo dedicado, **`series_excluidas.csv`** — 4.380 filas, 2.709 series
únicas, todas `NO ELEGIBLE`, con el motivo explícito:

```
motivo
pos_12 < 2               4.362     (menos de 2 meses con venta positiva en 12)
racha_zeros_final > 1       18     (racha de ceros al final de la serie)
```
Trae también las métricas de la decisión: `pos_12, zero_12, pos_rate_12, racha_zeros_final,
periodo_eval_ini, periodo_eval_fin, periodo_panel_max`.

⚠️ **NI `clasificacion_serie` NI `series_excluidas.csv` se usan en la aplicación.** Grep de
ambos términos en todos los `.py`, `.html` y `.js` del repo (excluyendo worktrees):
**cero resultados**. La marca viaja hasta la base y ahí muere. En la práctica no hace daño en
plata porque el modelo ya dejó esas filas en $0 — pero **sí infla los conteos**: 1.912 series
NO ELEGIBLE se cuentan en el `n_products` que muestra la pantalla
(`forecast_service.py:3884-3887` → `COUNT(DISTINCT codigo_serie)` sin filtrar por clasificación).

### Versión del modelo / de la carga

**Versión del MODELO: sí, en el dato.** Columna `version_param` de
`forecast_base_consolidado.csv` → `v4.4.0 (WMA+MeanReversion+BandCap)`, valor único en las
277.452 filas. Más `submodelo` (SM1/SM4/SM5). **Ninguna de las dos se consulta ni se muestra**
(grep: 0 apariciones en código).

**Versión de la CARGA: no existe.** No hay `import_run_id`, ni tabla de runs, ni timestamp de
carga en las tablas de datos. La única traza de "cuándo se cargó esto" es el nombre de las
backup tables de facturación (`forecast_fact_2026_backup_<YYYYMMDD_HHMMSS>`,
`load_fact_2026_safe.py:72`) — y eso solo para una de las cinco tablas.

---

## 10. PANTALLAS

Todo el módulo es **un solo archivo de 354 KB**: `web_comparativas/templates/forecast/index.html`.
Tres pestañas (`index.html:2341-2359`):

### Vista 1 — "Análisis General" (`tab-analisis`)

| Componente | Endpoint | Qué muestra |
|---|---|---|
| Fila de 7 KPIs | `/api/chart-data` | ver abajo |
| Gráfico de líneas (Plotly) | `/api/chart-data` | Histórico · Modelo (+banda Li/Ls) · +25 % · Ajustado usuario · Fact. 2026 |
| Treemap | `/api/treemap-data` | Composición ponderada, navegable Grupo → Cliente, con selector de período |
| Lista de productos | `/api/product-list` | familias para el filtro |

Los 7 KPIs (`forecast_service.py:6244-6270`):
1. Monto total proyectado anual 2026
2. Variación nominal vs 2025
3. Inflación esperada — **fija, hardcodeada**: `INFLATION_MO_PCT = 2.9` mensual → ~40,5 % anual (`forecast_service.py:6125-6126`)
4. Variación real (deflactada por ese 40,5 %)
5. Coincidencia del modelo (accuracy vs facturación real de meses cerrados)
6. Coincidencia de la expectativa (accuracy de la curva +25 %)
7. Facturado 2026 + `meta_completeness` (% de avance sobre la meta)

### Vista 2 — "Detalle Operativo" (`tab-detalle`)

- Grilla ag-Grid **cliente × 12 meses**, agrupada por `nombre_grupo`, con "pill" de crecimiento
  vigente por fila (`/api/client-table`).
- Toggle **$ / unidades** (`index.html:2543`, `toggleMoney`).
- Modal por cliente (`/api/client-detail`): árbol Negocio → Subnegocio → "artículo" (=familia)
  × mes, con celdas editables de %.
- Alta de clientes manuales y de artículos (`/api/create-manual-client`,
  `/api/add-articles-to-client`, `/api/article-search`, `/api/new-client-catalog`).

### Vista 3 — "Aprobaciones Forecast" (`tab-aprobaciones`)

Solo se renderiza si `can_view_approvals` (`index.html:2352`). El badge dice **"Control"** o
**"Solo lectura"** según `can_edit_approvals`.

- KPIs de la cola + medidor radial "Avance Meta" (`/api/approvals`)
- Vista agrupada y árbol, con selector de dimensión **Grupo | Perfil | Negocio | Subnegocio**
  (`/api/approvals/grouped`, `/api/approvals/tree`)
- Panel de detalle + modal de rechazo con motivo obligatorio
- Acciones individuales, por grupo y por IDs

### Sección — "Auditoría de ajustes" (`/api/audit*`) — **solo admin**

No es una pestaña visible del selector; es un conjunto de endpoints admin-only
(`forecast_router.py:1021-1044`).

### Filtros

**Barra global (`index.html:2276-2300`)** — aplican a las Vistas 1 y 2:
Período (desde/hasta) · Perfil (multi) · Negocio (multi) · Subnegocio (multi) ·
Productos (multi) · Laboratorio (checkbox + select de 1.612 valores) · toggle $/unidades ·
selector de período del treemap.

Opciones verificadas en runtime (`get_filter_options()`):
`profiles: 15 · neg: 4 · subneg: 36 · labs: 1.612 · min_date: 2026-01-01 · max_date: 2026-12-01`

**Filtros de Aprobaciones (`index.html:2964-3045`):** Estado · Comercial · y en "avanzados":
Fecha desde/hasta · Perfil · Negocio · Subnegocio · Tipo de modificación · Impacto · Alto.

**Filtros de Auditoría (`forecast_router.py:1386-1400`):** `date_from`, `date_to`, `comercial`,
`perfil`, `subneg`, `articulo`, `forecast_month`, `estado` (`activo|revertido|todos`),
`incluir_manuales`, con paginación (`page_size` máx 2.000).

### Exports — tres, con columnas verificadas

**(1) Proyección por cliente** — CSV generado en el navegador, sin tocar el server
(`index.html:5944-5970`). Delimitador `;`, nombre `proyeccion_<client_id>.csv`:
```
articulo;descripcion;unidad_medida;periodo;yhat_orig;pct;yhat_nuevo;monto
```

**(2) Aprobaciones** — CSV o XLSX, `GET /api/approvals/export?fmt=csv|xlsx`
(`forecast_router.py:3117`). 20 columnas (`_CR_EXPORT_LABELS`, `:3093-3114`):
```
Fecha y hora · Usuario · Grupo · Tipo de modificación · Campo modificado ·
Valor anterior (%) · Valor nuevo (%) · Diferencia (puntos) · Diferencia % ·
Impacto estimado (ARS) · Cuenta / Cliente · Perfil · Negocio · Subnegocio ·
Artículo · Período · Estado · Revisado por · Fecha de revisión · Observación / Motivo
```

**(3) Auditoría de ajustes** — CSV o XLSX, `GET /api/audit/export?fmt=csv|xlsx`
(`forecast_router.py:1453`), **admin-only**. 28 columnas (`_COL_ORDER_EXPORT`, `:1324-1332`):
```
Tipo de Registro · ID · Fecha de Actividad · Fecha Creación · Email Usuario ·
Nombre Usuario · Rol · Unidad de Negocio · Creado Por · Modificado Por · Cliente ·
ID Cliente (interno) · Código Artículo · Descripción Artículo · Perfil · Negocio ·
Subnegocio · Mes Forecast · Alcance · % Base Anual · % Ajuste Anual ·
% Mensual Efectivo · Diferencia % Anual · Valor Ajustado (ARS) · Vigente Desde (Mes) ·
Estado · Limitación del Dato · Origen Datos
```

Este export es notablemente honesto sobre sus propias limitaciones — las escribe **en cada
fila** (`forecast_router.py:1078-1090`):
- *"Valor absoluto no disponible: forecast_user_overrides almacena solo porcentajes de ajuste."*
- *"Descripción no disponible: no se almacena en forecast_user_overrides."*
- *"Fecha de reversión exacta no disponible… No existe tabla de historial de cambios."*

Topes: 30.000 overrides / 10.000 manuales por export; pool de 20.000 para paginación
(`forecast_router.py:1092-1096`).

---

## 11. PERMISOS

### Matriz (toda verificada en código, citas en el bloque 1)

| Capacidad | admin | gerente | auditor | supervisor | analista |
|---|:--:|:--:|:--:|:--:|:--:|
| Entrar al módulo | ● | ● | ● | ● | ● |
| Ver proyección y KPIs | ● | ● | ● | ● | ● |
| **Editar ajustes** (`/api/save-client`) | ● | ● | ● | ● | ● |
| Ver ajustes de **todos** los usuarios | ● | ● | ● | ○ | ○ |
| Ver Aprobaciones | ● | ● | ● | ○ | ○ |
| **Aprobar / Rechazar** | ● | ● | ○ | ○ | ○ |
| Auditoría de ajustes | ● | ○ | ○ | ○ | ○ |
| Borrar clientes/entradas manuales | ● | ○ | ○ | ○ | ○ |

● = sí · ○ = 403

Todas las filas "entrar/ver/editar" están además condicionadas a que **`forecast` esté en el
`module_access` del usuario** — no es automático por rol.

⚠️ **`/api/save-client`, `/api/save-group` y `/api/save-group-batch` no tienen guard de rol**
(`forecast_router.py:542-546, 587-591, 616-620`): solo `require_module("forecast")`. Un
auditor con la key concedida **puede modificar la proyección**, aunque su rol es de solo
lectura en el resto del sistema y no puede aprobar su propio cambio.

Role-sets canónicos (`visibility_service.py:26-33`):
```python
ADMIN_ROLES      = {"admin", "administrator", "administrador"}
AUDITOR_ROLES    = {"auditor", "visor", "viewer"}
MANAGER_ROLES    = {"gerente", "manager"}
SUPERVISOR_ROLES = {"supervisor"}
ANALYST_ROLES    = {"analista", "analyst"}
```
`_can_view_global_forecast_adjustments` (`forecast_router.py:112-131`) usa una lista propia,
normalizada con regex, que incluye variantes extra (`audit`, `aud`, `auditor_siem`).

### ¿Cada usuario ve solo su cartera? — **NO. TODOS VEN TODO.**

Este es un punto importante para SIEM 2.0. Verificado:

1. **No hay filtro de cartera sobre los datos.** Ninguno de los endpoints de datos
   (`/api/chart-data`, `/api/client-table`, `/api/treemap-data`, `/api/client-detail`,
   `/api/product-list`) recibe ni aplica un filtro por usuario/BU. Grep de `visibility`,
   `access_scope` y `cartera` en `forecast_router.py`: **0 resultados**. Las cláusulas WHERE
   que arma `_build_filter_sql` (`forecast_service.py:3023`) solo contemplan fecha, perfil,
   neg, subneg y productos.

2. **Lo único que sí se particiona por usuario son los AJUSTES**
   (`_fetch_override_records`, `forecast_service.py:644-673`):
   ```python
   if not all_users:
       q = q.filter(ForecastUserOverride.user_id == int(user_id))
   ```
   Un comercial ve **la proyección completa de la empresa**, pero solo **sus propios ajustes**.
   Admin, auditor y gerente ven los ajustes de todos (`all_users=True`).

3. Consecuencia operativa: **dos comerciales mirando la misma pantalla ven números distintos**
   en la línea "Ajustado por usuario", porque cada uno ve solo sus deltas. Y el admin ve una
   tercera cifra (la consolidada, con la regla de "último guardado gana").

---

## 12. PROBLEMAS CONOCIDOS

### 12.1 🔴 El modal de cliente y la grilla valorizan con precios distintos — brecha del 53 %

**Este es el bug más grande que encontré.** Dos pantallas del mismo módulo convierten
unidades a pesos por caminos diferentes:

- **Grilla, gráfico y treemap** usan `monto_yhat`, el importe precalculado en el parquet
  (`forecast_service.py:4001` → `val_col = "monto_yhat" if view_money else "yhat_cliente"`).
- **Modal de detalle de cliente** usa `yhat_cliente × precio`, donde `precio` sale de un mapa
  construido con `AVG(precio)` sobre `forecast_main` (`forecast_service.py:308-325`,
  `_get_precio_map_cached`). El `val_col` se elige en `forecast_service.py:5258`:
  ```python
  val_col = next((c for c in ("yhat_cliente", "yhat", "monto_yhat") if c in df_c.columns), None)
  ```
  → **`yhat_cliente` gana siempre; `monto_yhat` nunca se usa.** Y en `:5321`:
  `"money": round(adj * precio, 0)`.

**Medido sobre el dataset completo:**

```python
A = df_valorizado["monto_yhat"].sum()                                  # $121.742.106.031
pm = df_main.groupby("codigo_serie")["precio"].mean().to_dict()
B = (df_valorizado["yhat_cliente"] * df_valorizado["codigo_serie"].map(pm)).sum()
                                                                       # $ 56.737.814.770
B/A = 0,4660   →  −53,4 %
```

Y **99.980 filas (14 %) no tienen precio en `forecast_main`** → esas filas valen **$0** en el
modal, aunque representan **$9.965 millones** de `monto_yhat`.

**Verificación end-to-end sobre el cliente más grande** (`NO_ASIGNADO`, base
$15.624.605.367), corriendo el servicio real:

```
get_client_table(growth_pct=25) → $19.530.756.709    (= base × 1,25 exacto ✓)
get_client_detail(growth_pct=25) → $ 8.371.151.475
gap = −57,1 %
```

El −57,1 % es la suma de dos defectos: **−53,4 %** por el precio y **−9,5 %** por la
capitalización (bloque 6). **El usuario abre el detalle de un cliente y ve menos de la mitad
de lo que dice la fila que acaba de clickear.**

### 12.2 🔴 El archivo de facturación en disco no coincide con la última carga registrada

Medido con el **mismo parser del loader** (`csv.reader` + normalización de decimales),
sobre `facturacion_real_2026_sin_neg2.csv` (mtime **2026-07-08 11:25**):

```
filas          313.197        (bad_rows = 0)
SUM imp_hist   $52.278.784.233,33
meses          Ene→Jun 2026
negativos      1.959 filas, −$475.600.308
DRO Abr-2026   $2.546.766.468   (pasa el gate del loader)
```

Contra eso, el registro de la última carga a producción que tengo (2026-07-07: **313.187
filas / $51.763.562.611,19**) **no proviene de ningún archivo del repositorio** — grepeé
`313.187`/`51.763` en todos los `.md` y `.txt` y no aparece. Sale de una nota de sesión previa,
o sea **es un dato de segunda mano, no verificable acá**.

Con esa salvedad: el archivo actual tendría **10 filas y ~$515 M más** que lo cargado, y su
mtime (2026-07-08) es **posterior** al push. **Suposición (NO VERIFICADA): el CSV se reemplazó
después de la última carga y ese delta nunca llegó a producción.** Se cierra en 30 segundos
con acceso a la base: `SELECT COUNT(*), SUM(imp_hist) FROM forecast_fact_2026;`

### 12.3 🟠 Dos convenciones de crecimiento conviviendo

Ya detallado en el bloque 6: plano (grupo A, 14 sitios) vs capitalizado mensual (grupo B,
2 sitios, ambos en client-detail). Brecha **−9,54 %** ≈ **$14.511 M** sobre el total.
El test `test_global_growth_is_flat_not_quarter_ramped` fija la convención plana para
grilla/gráfico, pero **no cubre client-detail**.

### 12.4 🟠 El camino de "unidades" está muerto en producción

`forecast_service.py:3985-3987`, comentario del propio código:

> *"forecast_main fallback intentionally omitted: y/yhat are TEXT in production,
> SUM(COALESCE(y,0)) raises a type error caught by `_query_agg` → empty anyway."*

Y `:3988-3989`: *"Units path — forecast_main.y is TEXT in production so this will be empty;
kept for local/SQLite mode where y is numeric."*

→ **Con el toggle en "unidades", el histórico del gráfico sale vacío en producción**, y el
error se traga silenciosamente. Causa raíz: `to_sql` con inferencia de pandas sobre un CSV
leído con `dtype=str` (`migrate_forecast_csv_to_postgres.py:35`).

### 12.5 🟠 18 filas duplicadas en la proyección

Ver bloque 2.2. Todas de `AGUJA DESCARTABLE 25X8 21GX1"`, con valores distintos entre sí. Como
todos los queries hacen `SUM(...)`, **se suman las dos**. Nada lo impide: la tabla no tiene PK
ni UNIQUE.

### 12.6 🟠 26 familias pierden su subnegocio correcto

Ver bloque 3. El `drop_duplicates("codigo_serie")` de
`migrate_forecast_csv_to_postgres.py:56` colapsa el mapeo por perfil. Verificado: 26 series
cruzan subnegocio en el origen; 0 en los DataFrames que la app usa.

### 12.7 🟡 Dos definiciones de "mes cerrado" en la misma función

`_fact_2026_closed_month_cap()` (primer día del mes en curso) para la línea del gráfico y el
KPI de facturado; `val_months_2026[:-1]` (todos menos el último disponible) para el accuracy
(`forecast_service.py:6212-6218`). Con datos hasta junio y estando en julio, la primera
incluye junio y la segunda lo excluye → **el KPI 7 y los KPI 5/6 no hablan del mismo período.**

### 12.8 🟡 Las validaciones del pipeline principal son inexistentes

`migrate_forecast_csv_to_postgres.py:143-144`: un chunk que falle se loguea y la migración
sigue como si nada. Sin conteo esperado, sin checksum, sin comparación con la carga previa,
sin backup. Contrasta con el pipeline de facturación, que sí tiene backup + staging + gate.

### 12.9 🟡 Deuda declarada en el propio código

Textos que el sistema le muestra al usuario en cada fila del export de auditoría
(`forecast_router.py:1078-1090`):
- *"No existe tabla de historial de cambios"* → no se puede saber cuándo se revirtió algo,
  solo el `updated_at`.
- *"forecast_user_overrides almacena solo porcentajes"* → no hay valores absolutos.
- *"Descripción no disponible: no se almacena en forecast_user_overrides"*.
- `forecast_router.py:1088`: *"forecast_manual_entries no tiene campo updated_at propio"*.

### 12.10 🟡 Los ajustes se anclan al nombre de fantasía

`_resolve_override_for_row` prueba `fantasia` antes que `cliente_id`
(`forecast_service.py:869-880`). **Si cambia el nombre de fantasía de un cliente, sus ajustes
dejan de aplicar en silencio.** No hay FK ni validación.

### 12.11 🟡 Bugs cerrados (contexto histórico, ya corregidos en `main`)

- `aa49b16e` — *"quitar tope de fecha hardcodeado que cortaba la facturación real 2026 en
  abril"*. Estaba en 4 lugares; el gráfico cortaba en abril con datos hasta junio.
- `99cad738` — *"mostrar solo meses CERRADOS en facturación real 2026 (corte dinámico)"*.
- `799f3ef9` — clasificación del impacto del gauge de Aprobaciones por `override_id → status`
  en vez de cruce por valor.
- `f9ce8040` — rechazar ahora revierte el override real.
- `349274aa`, `cd8c46f3` — reconciliación del impacto de aprobaciones con la curva del gráfico
  (hubo desfasaje entre lo que decía Aprobaciones y lo que mostraba el gráfico).

### 12.12 Qué le molesta a los usuarios / qué piden

**NO VERIFICADO.** No hay tickets, encuestas ni notas de usuario en el repositorio que pueda
citar. Lo que sí es evidencia indirecta del código:

- La sucesión de commits de rediseño de Aprobaciones (`8b324ac6`, `9db70f15`, `bdb0c1b3`,
  `4713f99e`, `ccb3be24`) en poco tiempo sugiere iteración fuerte sobre esa pantalla.
- `606d7d48` *"scroll estable y animación suave al expandir clientes"* → fricción de UX
  reportada en la grilla.
- Una nota de performance pendiente registrada: la pantalla de aprobaciones agrupada tarda
  ~7 s y **no es la base** (la query corre en <1 ms) — es frontend/cold-start.

### 12.13 Qué NO hace el módulo

Verificado por ausencia en el código:

| No hace | Evidencia |
|---|---|
| **Proyectar a nivel artículo** | bloque 3 — todo es familia |
| Carga de datos por pantalla | solo `/api/reload`, que relee archivos |
| Versionado / escenarios comparables | no hay `import_run_id` ni tabla de runs |
| Historial de cambios de un override | declarado en `forecast_router.py:1084` |
| Fecha de fin de vigencia de un ajuste | no existe `effective_to_month` |
| Ajustes en valores absolutos ($) | solo porcentajes (`forecast_router.py:1078`) |
| Filtro por cartera del comercial | bloque 11 |
| Bloquear el cambio hasta aprobarlo | `models.py:501` — se aplica al instante |
| Estado "anulado" | grep `anul`: 0 resultados |
| Impedir la auto-aprobación | bloque 8 |
| Usar la marca de serie no elegible | bloque 9 |
| Mostrar la versión del modelo | `version_param` nunca se lee |
| Cambiar el % global desde la UI | `index.html:2430` `disabled` |
| Alertar cuando una carga falla parcialmente | `migrate_...py:143` |

---

## 13. VOLÚMENES REALES

Todo medido sobre los archivos fuente y sobre los DataFrames que el servicio arma en runtime
(`svc.get_data()`). **Producción NO VERIFICADA** — sin acceso a la base.

### Filas por tabla

| Tabla / fuente | Filas | Notas |
|---|---:|---|
| `forecast_valorizado` ← parquet | **702.436** | 18 dups residuales |
| `forecast_main` ← consolidado | **277.452** | 220.008 hist + 57.444 forecast |
| `forecast_fact_2026` ← CSV | **313.197** | ⚠️ ver 12.2 |
| `forecast_imp_hist` (tras filtro canónico) | **38.758** | 44.861 sin filtrar |
| `dataset_base.csv` | 221.424 | insumo |
| `series_excluidas.csv` | 4.380 | 2.709 series, **no usado** |
| `forecast_change_requests` (LOCAL/dev) | 433 | mayormente prueba |
| `forecast_user_overrides` (LOCAL/dev) | 57 | |
| `forecast_manual_clients` / `_entries` (LOCAL/dev) | 10 / 15 | |

### Cardinalidades en la proyección (`forecast_valorizado`)

| Dimensión | Cantidad |
|---|---:|
| `cliente_id` distintos | **6.490** |
| `fantasia` distintas | 6.096 |
| `nombre_grupo` distintos | **765** |
| `codigo_serie` (= **familias**) | **3.039** |
| **artículos** | **0** (una serie marcada ARTICULO, con $0) |
| `perfil` | **15** — COM, DPM, DRO, FAR, FIN, IPR, IPU, LAN, OES, OSP, OSU, PER, PRO, SAN, SIN |
| `neg` (negocios) | **5** (4 reales + `nan`, 51.542 filas sin mapear) |
| `subneg` (subnegocios) | **33** (32 reales + `nan`) |
| Períodos | **12** (2026-01 … 2026-12) |
| **Total proyectado** | **$121.742.106.031,20** |

Subnegocios por negocio (sobre `forecast_main`):
```
ACCESORIOS E INSUM MED-HOSPITALARIOS   →  9
CARDINAL HEALTH                        →  9
EQUIPAMIENTO MEDICO                    →  4
MEDICAMENTOS HOSPITALARIOS             → 14
```

En `forecast_main` hay **3.359** series (320 más que en `forecast_valorizado`): series con
histórico pero sin proyección valorizada por cliente.
En el CSV `forecast_fact_2026`: **5.605** clientes, **3.311** `codigo_serie`, **3.860**
`articulo_codigo`, 16 perfiles (incluye `NAN`).

### Rango de fechas cubierto

```
2024-01 ─────────────── 2025-12 │ 2026-01 ──────── 2026-06 ──────── 2026-12
├── forecast_main (hist, unidades, 24 meses) ─────┤
                  ├─ forecast_imp_hist ($, 12 m) ─┤
                                                  ├─ fact_2026 ($, 6 m) ┤
                                                  ├─ forecast_valorizado (proyección, 12 m) ──┤

max_hist = 2025-12-01   ← la frontera que separa "no ajustar" de "ajustar"
                          (verificado en runtime: df_main[tipo=='hist']['fecha'].max())
```

Cobertura total: **2024-01 → 2026-12 = 36 meses**, pero solo **12 tienen histórico en pesos**
y **12 tienen proyección**.

---

## LO QUE NO PUDE VERIFICAR

Todo lo de esta lista es **suposición o hueco declarado**, no dato.

### Sin acceso a la base de producción

1. **Row counts reales en PostgreSQL de producción** de las 7 tablas de datos y de las 4 de
   metadatos. Todo lo que reporto sale de los archivos fuente y de la base local de
   desarrollo. Motivo: `DATABASE_URL` vacía; `.env` raíz y `web_comparativas/.env` no la
   tienen. No intenté obtener credenciales.
2. **DDL efectivo en producción** — los tipos que reporto son los que pandas infiere del
   parquet/CSV. Lo único confirmado por el propio código es que `forecast_main.y`/`.yhat`
   quedaron TEXT (`forecast_service.py:3985`).
3. **Si `forecast_valorizado_summary` y `forecast_product_summary` existen hoy y están
   sincronizadas.** El gate `_forecast_summary_available()` (`forecast_service.py:3631`) cae a
   la tabla cruda si no existen, así que el módulo funciona igual — pero no sé cuál camino
   está activo ni si el kill-switch `FORECAST_USE_SUMMARY` está puesto.
4. **Si el delta de 12.2 llegó a producción.** El archivo local difiere de la última carga
   registrada; sin la base no puedo cerrar la pregunta.
5. **Cuántos usuarios de producción tienen `forecast` en su `module_access`** y con qué rol.
   La base local tiene 4 usuarios y es de desarrollo.
6. **Estado real de la cola de aprobaciones en producción.** Los 371 pendientes / 62 aprobados
   son de la base local, con datos de prueba.
7. **Si existen tablas huérfanas** (`forecast_fact_2026_staging`, backups viejas) ocupando
   espacio.

### Sin fuente en el repositorio

8. **Qué decide el negocio con este número.** Inferí "presupuesto/meta comercial" del
   `meta_completeness` y del circuito de aprobación, pero es inferencia.
9. **En qué momento del año/ciclo se usa.** Solo sé que el dato es anual calendario 2026.
10. **Frecuencia real de carga y quién la ejecuta.** No hay tabla de runs ni log de
    ejecuciones. Que los pipelines son manuales sí está verificado; la cadencia no.
11. **Quién produce la proyección** — sé que es externo, versionado `v4.4.0`, con 3
    submodelos. No sé qué área/herramienta lo corre, ni qué son SM2 y SM3 (existen SM1, SM4 y
    SM5; la numeración salteada sugiere que hubo otros).
12. **De dónde salen los CSV de facturación real** (qué extractor del ERP los genera).
13. **Qué les molesta a los usuarios y qué piden.** Cero fuente citable. Lo del bloque 12.12
    es lectura de commits, no testimonio.
14. **Si los 1.959 importes negativos de facturación son legítimos.** Aparecen bajo un archivo
    llamado `sin_neg2`, lo cual es al menos confuso. El loader no los rechaza.
15. **Por qué 51.542 filas (7,3 %) tienen `neg = 'nan'`** — no encontré la regla que las deja
    sin mapear.

### Verificado como ausente (no es hueco, es hallazgo)

16. `series_excluidas.csv` y la columna `clasificacion_serie`: **existen y no se usan** —
    grep sobre todo el repo, 0 resultados. Esto sí lo verifiqué; lo dejo acá porque es el tipo
    de cosa que la documentación previa podría afirmar que sí funciona.
17. `version_param` / `submodelo`: **existen en el dato y no se consultan** — idem.
18. Estado "anulado": **no existe** — grep `anul` en router y servicio, 0 resultados.

---

*Relevado el 2026-07-29 sobre `main` (`41defd7e`). Todas las citas `archivo:línea` refieren a
ese commit. Las consultas y conteos de este documento son reproducibles con los comandos
transcriptos en cada bloque.*
