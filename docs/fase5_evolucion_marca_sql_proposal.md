# Fase 5 — Propuesta EXPERIMENTAL: `evolucion-marca` a SQL

> **Estado:** PROPUESTA / EXPERIMENTAL. **No aplicada al router. No deploy. No merge.**
> No forma parte del Batch 1 cerrado. Etiqueta sugerida: **Batch 1.b (código)**, aceptable
> o descartable de forma independiente.
> **Condición de avance:** primero validar Batch 1 (4 endpoints) contra PostgreSQL con el
> usuario read-only. Esta propuesta solo pasa a "lista para deploy" si su parity test
> dedicado da **0 diferencias de agrupación** sobre los datos reales.

---

## 1. Endpoint y tablas afectadas

- **Endpoint:** `GET /api/mercado-publico/perfiles/articulos/evolucion-marca` → `articulos_evolucion_marca` en `web_comparativas/routers/mercado_publico_perfiles_router.py`.
- **Tabla:** `comparativa_rows` (única).
- **Panel:** evolución de la mediana de precio por **marca normalizada** a lo largo del tiempo (year/month).

---

## 2. Cómo se normaliza HOY la marca (Python)

```python
def _normalize_marca(m: str) -> str:
    if not m:                                   # None o "" → ""
        return ""
    return re.sub(r'\s+', ' ', m.strip()).upper()
```

Y en el endpoint:
```python
# WHERE marca IS NOT NULL (entre otros filtros)
marca_norm = _normalize_marca(r.marca or "")
if not marca_norm:                              # vacío tras normalizar → se descarta
    continue
if marca_norm not in marca_display:
    marca_display[marca_norm] = (r.marca or "").strip()   # display = original STRIPEADO
groups[(y, mo, marca_norm)].append(r.precio_unitario)
```

Pasos efectivos de normalización para AGRUPAR:
1. `strip()` — saca whitespace de los extremos (espacios, tabs, `\n`, y whitespace **Unicode** porque es `str`).
2. `re.sub(r'\s+', ' ', …)` — colapsa **toda** corrida de whitespace (incl. Unicode) a un solo espacio.
3. `.upper()` — mayúsculas **Unicode** de Python.

El **display** (texto que ve el front) es distinto del de agrupar: es `original.strip()` (solo
stripeado, **conserva** dobles espacios internos y la capitalización original), tomado del
**primer** registro encontrado por orden `(year, month, marca)`.

---

## 3. Casos especiales a vigilar

| Caso | Comportamiento hoy (Python) | ¿Lo reproduce el SQL propuesto? |
|---|---|---|
| `marca = NULL` | Excluido por `WHERE marca IS NOT NULL` | ✅ mismo WHERE |
| `marca = ""` o solo espacios | `marca_norm=""` → `continue` (se descarta) | ✅ `WHERE marca_norm <> ''` |
| Espacios extremos (`" ACME "`) | strip → `"ACME"` | ✅ `btrim(regexp_replace(...))` |
| Espacios internos múltiples (`"COCA  COLA"`) | colapsa → `"COCA COLA"` | ✅ `regexp_replace(marca,'\s+',' ','g')` |
| Mayús/minús (`"Acme"` vs `"ACME"`) | `.upper()` → agrupan juntas | ✅ `upper()` (ver ⚠️ abajo) |
| Acentos (`"Médix"`) | **No** se quitan; `.upper()`→`"MÉDIX"` | ✅ tampoco se quitan |
| Tabs/saltos de línea internos | `\s` los colapsa | ⚠️ depende de `\s` en PG |
| **Espacio no separable** U+00A0, espacios finos U+2009, etc. | Python `\s` (Unicode) **sí** los trata como espacio | ⚠️ PG `\s` = `[[:space:]]` suele ser **solo ASCII** |
| `ß` (eszett alemán) | `.upper()` → `"SS"` (¡cambia longitud!) | ⚠️ PG `upper('ß')` suele dar `"ß"` |
| Mapeos locale-específicos (turco `i`/`İ`) | `.upper()` Python | ⚠️ PG `upper()` depende de collation |

### ⚠️ Riesgo concreto de agrupación distinta
La equivalencia **no es 100% garantizable a nivel teórico** por 3 fuentes:
1. **Whitespace Unicode**: si una marca trae U+00A0 (NBSP) u otros, Python lo colapsa pero
   PG `\s` (POSIX) puede no hacerlo → quedarían normalizaciones distintas → **dos grupos**
   donde Python tenía uno (o viceversa).
2. **`upper()` Unicode**: `ß`→`SS` en Python pero no necesariamente en PG; collations ICU
   vs libc difieren en algunos caracteres.
3. **Orden del display**: `ORDER BY marca` usa la collation de PG, que puede ordenar distinto
   a Python para elegir el "primer" original (afecta solo el texto mostrado, no la mediana).

**Conclusión:** se mantiene como **EXPERIMENTAL**. La pregunta no es "¿son equivalentes para
todo input posible?" (no lo son), sino **"¿lo son para los datos reales de `comparativa_rows`?"**.
Eso lo decide el parity test sobre la base real. Si da 0 diferencias de agrupación, es seguro
para ESTE dataset; si aparece aunque sea una, se reporta y NO se deploya como está.

---

## 4. Query vieja vs query nueva

### Vieja (hoy): trae filas, normaliza y agrupa en Python
```python
q = select(_year, _month, ComparativaRow.marca, ComparativaRow.precio_unitario)
     .where(fecha_apertura not null, precio_unitario not null, marca not null + filtros)
     .order_by(_year, _month, ComparativaRow.marca)
rows = session.execute(q).all()        # ← trae todas las filas filtradas
# Python: _normalize_marca, skip vacío, groups[(y,mo,norm)].append(precio),
#         marca_display[norm] = primer original stripeado
# Output: por (y,mo,norm) ordenado → {year, month, month_label, marca: display, mediana_precio: median(precios)}
```

### Nueva (propuesta): normaliza, agrupa y mediana en SQL (PostgreSQL)
```sql
WITH norm AS (
  SELECT
    EXTRACT(year  FROM fecha_apertura)::int                          AS year,
    EXTRACT(month FROM fecha_apertura)::int                          AS month,
    upper(btrim(regexp_replace(marca, '\s+', ' ', 'g')))            AS marca_norm,
    regexp_replace(marca, '^\s+|\s+$', '', 'g')                      AS marca_stripped,  -- = .strip()
    marca                                                            AS marca_raw,
    precio_unitario
  FROM comparativa_rows
  WHERE fecha_apertura IS NOT NULL
    AND precio_unitario IS NOT NULL
    AND marca IS NOT NULL
    -- + mismos filtros del endpoint (descripcion/fecha/marca/proveedor/rubro/plataforma)
),
filt AS (SELECT * FROM norm WHERE marca_norm <> ''),
display AS (   -- primer original stripeado por marca_norm, orden (year, month, marca_raw)
  SELECT DISTINCT ON (marca_norm) marca_norm, marca_stripped AS display
  FROM filt
  ORDER BY marca_norm, year, month, marca_raw
)
SELECT f.year, f.month, f.marca_norm, d.display,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY f.precio_unitario) AS mediana
FROM filt f
JOIN display d USING (marca_norm)
GROUP BY f.year, f.month, f.marca_norm, d.display
ORDER BY f.year, f.month, f.marca_norm;   -- mismo orden que sorted(groups.items())
```
- En **SQLite (local)** no hay `percentile_cont` → el router mantendría la ruta Python
  actual (igual que en Batch 1). El cambio SQL aplica **solo en PostgreSQL**.
- `month_label` lo sigue armando el router en Python (presentación, idéntico en ambas rutas).

---

## 5. Criterio de paridad (qué debe coincidir)

1. **Mismo conjunto de marcas normalizadas** (claves `marca_norm`). ← lo más crítico.
2. **Mismos puntos por marca**: mismas combinaciones `(year, month, marca_norm)`.
3. **Mismos meses/fechas** por marca.
4. **Mismos valores de mediana**, con **tolerancia ≤ 0.01** (redondeo a 2 dec).
5. **Mismo display** por `marca_norm` (texto mostrado).
6. **Mismo orden** de salida `(year, month, marca_norm)` (el front podría depender de él).

**Diferencias aceptables:** ninguna en agrupación (1, 2, 3). En valores (4), solo ruido de
redondeo ≤ 0.01. En display (5), una diferencia de display **sin** cambio de agrupación es
"amarillo" (cosmético) → se revisa pero no necesariamente bloquea.
**Diferencias NO aceptables (bloquean):** cualquier `marca_norm` que aparezca en una ruta y
no en la otra, o que agrupe filas distintas, o medianas que difieran > 0.01.

---

## 6. Test de paridad dedicado

`scripts/phase5_evolucion_marca_parity_check.py` (separado del check de Batch 1). Read-only,
exige `--confirm-remote`. Calcula **las dos rutas** sobre la MISMA base y compara los 6 puntos
de arriba, **reportando explícitamente** toda diferencia de agrupación (no la oculta):

- `marca_norm` solo-en-python / solo-en-sql (grouping diff) → **FAIL**.
- claves `(year, month, marca_norm)` que difieren → **FAIL**.
- medianas con |dif| > 0.01 → **FAIL**.
- display distinto sin grouping diff → **WARN** (no bloquea por sí solo).
- orden distinto → **FAIL**.

Salida: `PASS` solo si no hay FAILs. Cualquier FAIL ⇒ queda como experimental, no se aplica.

---

## 7. Mejora esperada

- **Estructural:** deja de traer todas las filas filtradas (year, month, marca, precio) a
  Python; la DB agrupa y calcula la mediana. Devuelve ~`#(year,month,marca)` filas.
- **Magnitud:** moderada y dependiente del filtro. Con `descripcion` (un artículo) el volumen
  ya es acotado; el beneficio crece cuando el filtro es amplio (sin descripcion, muchas marcas).
- El parity test reporta `python=<ms>` vs `sql=<ms>` para tener el número real sobre prod.

---

## 8. Plan de rollback

- Hoy: **no hay nada que revertir** (no se tocó el router).
- Si en el futuro se aplica el diff: se hace en **commit separado** sobre la rama; rollback =
  `git revert <commit>` + redeploy. La rama SQLite mantiene la ruta Python (sin cambios), así
  que un revert no afecta el comportamiento local.
- No hay cambios de estructura ni de datos → no requiere backup de DB.

---

## 9. Prioridad

1. Validar Batch 1 (4 endpoints) contra PostgreSQL → 4 PASS / 0 FAIL.
2. Revisar resumen y decidir merge/deploy de Batch 1.
3. **Después**, correr el parity test de `evolucion-marca`. Si 0 diferencias de agrupación →
   evaluar aplicarlo como Batch 1.b / Batch 2 código. Si hay diferencias → queda experimental.
