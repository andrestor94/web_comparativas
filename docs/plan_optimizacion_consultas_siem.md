# Plan de optimización de consultas analíticas — SIEM

> **Fase 3 — preparación.** Propuestas **documentadas**, no implementadas. Ninguna se aplica
> en esta fase salvo que sea de **clase A** (mismo resultado, riesgo nulo) y se apruebe aparte.
> Cada cambio debe verificar **resultados idénticos** antes/después.

## Clasificación usada

- **A.** Cambios de query **sin cambiar resultados** (seguros).
- **B.** Mover cálculo de **Python → SQL** (mismo resultado, requiere verificación).
- **C.** Cambios que requieren **índices**.
- **D.** Cambios que requieren **tablas resumen / materializadas**.
- **E.** Cambios que **dependen de PostgreSQL** y necesitan **fallback SQLite**.

---

## 1. Mercado Público / Reporte de Perfiles
Archivo: `routers/mercado_publico_perfiles_router.py` (tabla `comparativa_rows`).

| Endpoint / línea aprox. | Problema | Clase | Propuesta |
|---|---|---|---|
| `/filtros` (compradores) | Traía todo el distinct a Python | **A** | ✅ Ya hecho en Fase 2 (`LIMIT 200` en SQL). |
| `articulos_por_proveedor` (~745) | Histórico de precios sin `LIMIT`, loop en Python | **B+C+E** | `MAX(precio) OVER (PARTITION BY proveedor ORDER BY fecha DESC)` (PG) con fallback a subquery+límite en SQLite. Índice `(proveedor, fecha_apertura)`. |
| `articulos/kpis` mediana (~532) | `statistics.median` sobre todos los precios | **B+E** | `percentile_cont(0.5) WITHIN GROUP (ORDER BY precio)` (PG); SQLite: subconsulta de mediana o aproximación documentada. |
| `articulos_evolucion_marca` (~825) | Agrupa+mediana en Python sin `LIMIT` | **B+D+E** | `GROUP BY year,month,marca` + `percentile_cont`; o tabla resumen mensual por marca. |

---

## 2. Mercado Privado / Dimensionamiento
Archivos: `routers/mercado_privado_perfiles_router.py`, `dimensionamiento/query_service.py`
(tablas `dimensionamiento_records` ~319k, `dimensionamiento_family_monthly_summary` ~260k).

| Endpoint / línea aprox. | Problema | Clase | Propuesta |
|---|---|---|---|
| `privado_articulo_kpis` (~213) | Mediana de ratios en Python, sin `LIMIT` | **B+E** | `percentile_cont` en SQL sobre `valorizacion/cantidad`. Fallback SQLite. |
| `privado_articulo_precio_evolucion` (~278) | Descarga todo, agrupa por mes en Python | **B+D** | `GROUP BY mes` en SQL; idealmente desde la summary. |
| `privado_articulo_consumo_mensual` (~444) | Diccionarios anidados + medianas en Python | **B+D** | Window functions o tabla resumen por familia/mes. |
| `query_service` fallback (`_resolve_aggregate_model`) | Si summary "no usable" escanea tabla cruda | **A** | ✅ Logging de razón/gravedad agregado en Fase 2. Falta: endurecer para que un fallback por *corrupción* sea más visible/alertable. |
| `_default_platform_values`, `_distinct_*` | `DISTINCT` sin `LIMIT` | **A+C** | `LIMIT` defensivo donde la cardinalidad lo permita; índices sobre las columnas de `DISTINCT`. |
| Dashboard bootstrap | Usa snapshot precalculado | **D** | ✅ Ya existe (`dimensionamiento_dashboard_snapshots`). Mantener y extender el patrón. |

---

## 3. Forecast (chart / treemap / overrides)
Archivo: `forecast_service.py` (tablas `forecast_valorizado` ~702k, overrides).

| Punto / línea aprox. | Problema | Clase | Propuesta |
|---|---|---|---|
| Treemap/chart (~3103) | `.iterrows()` / `groupby` sobre 702k filas | **B+D** | Agregar en SQL (`GROUP BY` server-side) o materializar agregados por perfil/cliente/mes. |
| `_fetch_override_records` (617) | `.all()` sin límite (path admin) | **A** | ✅ Instrumentado en Fase 2 (warning, sin LIMIT para no perder overrides). Futuro: paginar/agregar el path admin. |
| Lectura `forecast_valorizado` | Bien: ya filtra server-side y evita cargar global en Render | — | Mantener. Al integrarla al ORM, tipar columnas mejora claridad. |

---

## 4. Compatibilidad SQLite (clase E) — patrón recomendado

Las medianas/percentiles son el principal punto de divergencia:

```python
from web_comparativas.models import IS_POSTGRES

if IS_POSTGRES:
    mediana = func.percentile_cont(0.5).within_group(col.asc())
else:
    # SQLite: no tiene percentile_cont. Opciones:
    #   (a) subconsulta de mediana (ORDER BY + LIMIT/OFFSET sobre count),
    #   (b) aproximación documentada,
    #   (c) traer solo la columna necesaria con LIMIT y calcular en Python (último recurso).
    mediana = ...  # implementar con fallback y documentar la diferencia
```

> Regla: **PostgreSQL es la fuente de verdad**; el fallback SQLite puede ser aproximado, pero
> debe quedar **documentado** y nunca cambiar el resultado en producción.

---

## 5. Orden sugerido (cuando se autorice implementar)

1. **Clase A** restantes (LIMIT defensivos, endurecer logging de fallback). Riesgo nulo.
2. **Clase C** (índices) validados con `EXPLAIN ANALYZE` — primero local, luego prod con backup.
3. **Clase B** (Python→SQL) por endpoint, con test de **resultado idéntico** (snapshot antes/después).
4. **Clase D** (tablas resumen) reutilizando el patrón de `dimensionamiento_*_summary`/snapshots.
5. **Clase E** se resuelve dentro de B/D con el patrón de fallback de §4.

> Nada de B/D/E se implementa en Fase 3. Solo se documenta. Una mejora de clase A puede
> proponerse e implementarse aparte, con su propia rama + QA.
