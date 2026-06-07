# Fase 5 — Batch 1: resumen de validación (antes de merge/deploy)

> **Rama:** `chore/db-phase-5-safe-optimizations` · **Commit:** `0fb46cd7` · **NO mergeado, NO deployado.**
> **Naturaleza:** solo código read-only. Cero cambios de estructura o datos.
> **Regla:** en SQLite (local) la ruta de cálculo queda **idéntica** a la anterior; en
> PostgreSQL (prod) la mediana se calcula en la DB con `percentile_cont`. El merge/deploy
> recién se evalúa con **PASS** del parity check contra una base PostgreSQL equivalente a prod.

---

## 1. Qué endpoints mejoraron y qué se reemplazó

### 1.1 `mercado_privado_perfiles_router.py`

#### `POST/GET /api/mercado-privado/perfiles/articulo/kpis` → `privado_articulo_kpis`
- **Tabla que toca:** `dimensionamiento_records` (solo para la mediana; los otros KPIs usan `summary`/`records` vía `_resolve_model`).
- **Query vieja:** traía **una fila por registro** (`valorizacion_estimada`, `cantidad_demandada`) de `dimensionamiento_records` con los filtros aplicados, y calculaba en Python `statistics.median([val/cant ...])`. Hasta ~319k filas por la red por llamada.
- **Query nueva (PG):** `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY valorizacion_estimada / cantidad_demandada)` con el mismo `WHERE` (cant>0, val>0 + filtros). Devuelve **un número**.
- **Resultado devuelto:** `mediana_precio_unitario` (los otros campos del endpoint no cambian).
- **Antes vs después:** mismo valor (mediana redondeada a 2 decimales). En SQLite, código idéntico.

#### `POST/GET /api/mercado-privado/perfiles/articulo/precio-evolucion` → `privado_articulo_precio_evolucion`
- **Tabla:** `dimensionamiento_records`.
- **Query vieja:** traía una fila por registro con `month_expr`, agrupaba por mes en Python y hacía `statistics.median` por mes.
- **Query nueva (PG):** `SELECT month_expr, percentile_cont(0.5) WITHIN GROUP (ORDER BY val/cant) ... GROUP BY month_expr ORDER BY month_expr`. Una fila por mes.
- **Resultado devuelto:** `{months: [...], values: [...]}` — misma serie.
- **Antes vs después:** mismos meses (orden cronológico) y mismas medianas redondeadas.

### 1.2 `mercado_publico_perfiles_router.py`

#### `GET /api/mercado-publico/perfiles/articulos/kpis` → `articulos_kpis`
- **Tabla:** `comparativa_rows`.
- **Query vieja:** traía todos los `precio_unitario` filtrados y `statistics.median` en Python.
- **Query nueva (PG):** `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY precio_unitario)` con los mismos filtros.
- **Resultado devuelto:** campo `mediana` del KPI (resto sin cambios).
- **Antes vs después:** mismo valor redondeado a 2 decimales.

#### `GET /api/mercado-publico/perfiles/articulos/por-proveedor` → `articulos_por_proveedor`
- **Tabla:** `comparativa_rows` (vía subquery `participaciones`).
- **Query vieja:** `hist_q` traía **todos** los `precio_unitario` de los proveedores del filtro **sin LIMIT**, ordenados por fecha desc; en Python agrupaba por proveedor para (a) la mediana (`_median`) y (b) el "último precio" (primera fila por proveedor).
- **Query nueva (PG):** dos consultas agregadas:
  - Mediana por proveedor: `percentile_cont(0.5) WITHIN GROUP (ORDER BY precio_unitario) ... GROUP BY proveedor`.
  - Último precio: `SELECT DISTINCT ON (proveedor) proveedor, precio_unitario ... ORDER BY proveedor, fecha_apertura DESC, id ASC`.
- **Resultado devuelto:** por proveedor `{mediana_precio, ultimo_precio, veces_ganado, total_adjudicado, count, procesos, efectividad}` y el mismo orden final.
- **Antes vs después:** mismos valores. `DISTINCT ON` reproduce exactamente la "primera fila por proveedor" del loop ordenado igual.

---

## 2. Tablas que toca cada endpoint (resumen)

| Endpoint | Tabla(s) | ¿Cambió la lógica de filtros? |
|---|---|---|
| `privado .../articulo/kpis` | `dimensionamiento_records` (mediana) | No — mismos `WHERE` + `_apply_common_filters` |
| `privado .../articulo/precio-evolucion` | `dimensionamiento_records` | No |
| `publico .../articulos/kpis` | `comparativa_rows` | No — mismos `_apply_*` |
| `publico .../articulos/por-proveedor` | `comparativa_rows` (subq `participaciones`) | No — mismo subquery y filtros |

---

## 3. Tolerancia de diferencia aceptada (redondeo)

- Todas las salidas se redondean a **2 decimales** (igual que antes).
- `percentile_cont(0.5)` ≡ `statistics.median` (interpolación lineal en la mediana = promedio de los dos centrales). Verificado: **0 discrepancias en 2000 pruebas** (n par e impar) tras redondear a 2 decimales.
- El parity check usa `--tolerance 0.01` (diferencia absoluta máx. tras redondear). Cualquier diferencia ≤ 0.01 se considera ruido de redondeo flotante PG-numeric vs Python-float. **> 0.01 ⇒ FAIL** (frenar).

---

## 4. Baseline de tiempos (antes/después)

> **Honestidad:** no tengo tiempos de producción. El **baseline local (SQLite) no es
> representativo** porque la ruta nueva en SQLite es la misma Python de antes. Los tiempos
> reales los **captura el propio parity check** al correr contra la copia PostgreSQL: para
> cada métrica reporta `python=<ms>` (enfoque viejo) y `sql=<ms>` (enfoque nuevo).

Tabla a completar con la salida de `phase5_parity_check.py` contra la copia PG:

| Métrica | `python` (viejo) ms | `sql` (nuevo) ms | Filas evitadas por la red |
|---|---:|---:|---|
| privado/articulo/kpis · mediana | _(pendiente)_ | _(pendiente)_ | ~filas filtradas de records |
| privado/articulo/precio-evolucion | _(pendiente)_ | _(pendiente)_ | idem, agrupado por mes en SQL |
| publico/articulos/kpis · mediana | _(pendiente)_ | _(pendiente)_ | ~filas de comparativa_rows |
| publico/articulos/por-proveedor | _(pendiente)_ | _(pendiente)_ | histórico completo de proveedores |

**Mejora estructural esperada (independiente del número):** se elimina el transporte de
hasta cientos de miles de filas a Python por llamada; la DB devuelve un escalar (o una fila
por grupo). El beneficio crece con el volumen filtrado y con la latencia de red app↔DB.

---

## 5. Endpoints con mejora incierta / no validada

- **`por-proveedor`** es el cambio de mayor complejidad (dos sustituciones: `percentile_cont` + `DISTINCT ON`). La paridad está razonada y se cubre en el parity check, pero **es el que más conviene mirar** en el PASS por proveedor (campos `mediana_diffs` y `ultimo_diffs` deben ser 0).
- En `precio-evolucion`, parity depende de que `date_trunc('month', fecha)::date` agrupe igual que el `month_expr` del código (es el mismo helper). El parity check lo valida mes por mes.
- **No validado aún contra PG real:** los 4 endpoints. SQLite pasa por construcción (código idéntico); **la decisión de merge espera el PASS PostgreSQL.**

---

## 6. Estado de la rama y stash de Indicadores

**Rama Fase 5:** `chore/db-phase-5-safe-optimizations` (creada desde `chore/db-production-diagnostics-phase-4` + merge del logging de Fase 2 = superset). HEAD `0fb46cd7`. Sin push, sin merge, sin deploy.

**Trabajo de Indicadores Comerciales (parqueado, NO mezclado con Fase 5):**
- **Archivos en stash (3 tracked):** `web_comparativas/main.py`, `web_comparativas/templates/base.html`, `web_comparativas/templates/markets_home.html`.
- **Nombre exacto del stash:** `wip-indicadores-park-fase5` → referencia `stash@{0}`.
- **Rama desde la que se creó / desde la que se recupera:** `chore/db-audit-safe-performance-phase-2`.
- **Comando para recuperarlo:**
  ```powershell
  git checkout chore/db-audit-safe-performance-phase-2
  git stash pop   # aplica stash@{0} sobre la rama de Indicadores
  ```
  (verificá con `git stash list` que `stash@{0}` siga siendo "wip-indicadores-park-fase5" antes de hacer pop).
- **Archivos NUEVOS sin trackear que quedaron intactos en el árbol** (no entran en Fase 5):
  `web_comparativas/indicadores_db.py`, `indicadores_service.py`, `indicadores_inflacion_service.py`,
  `indicadores_laboratorios_service.py` (+ `.bak`), `routers/indicadores_router.py`,
  `static/css/indicadores.css`, `static/js/indicadores.js`, `indicadores_inflacion.js`,
  `indicadores_laboratorios.js`, `templates/indicadores/`, `data/Imagenes Suite SIEM/`,
  y la carpeta `Indicadores Comerciales/`.

---

## 7. Cómo correr la validación PostgreSQL (parity check)

### Comando exacto (Windows / PowerShell)
```powershell
# 1) Apuntar a una COPIA/STAGING o conexión de SOLO LECTURA equivalente a prod.
#    (la URL contiene credenciales → solo en la sesión, nunca en commits/chat)
$env:DATABASE_URL = "postgresql://USUARIO_RO:PASS@HOST.render.com/DBNAME?sslmode=require"

# 2) Correr el parity check (read-only, exige --confirm-remote para no-SQLite)
python -X utf8 scripts/phase5_parity_check.py --confirm-remote

# 3) Limpiar la credencial de la sesión al terminar
Remove-Item Env:\DATABASE_URL
```
(`-X utf8` evita problemas de encoding de la consola Windows con los íconos del reporte.)

### Respuestas a las preguntas operativas
- **Variable de entorno:** `DATABASE_URL` (formato `postgresql://...`; el script normaliza `postgres://`→`postgresql://`). Si no se setea, usa el SQLite local y todo da **SKIP**.
- **Qué URL usar:** **preferentemente una copia/staging** o una **External Database URL de Render con un usuario de SOLO LECTURA**. No es necesario (ni recomendado) un superusuario. Si solo existe la URL productiva, usarla con rol de solo lectura y en horario de bajo tráfico (el script no escribe, pero minimiza carga de lectura).
- **Permisos mínimos de esa URL:** `CONNECT` a la base + `USAGE` en el schema + `SELECT` sobre `dimensionamiento_records` y `comparativa_rows`. Nada más (no necesita INSERT/UPDATE/DDL).
- **Cómo se garantiza solo-lectura:** el script **solo emite `SELECT`** (medianas, conteos, `DISTINCT ON`). No ejecuta `INSERT/UPDATE/DELETE` ni `CREATE/ALTER/DROP`. No importa la app (no dispara migraciones de arranque): crea su propio engine. Para blindaje extra, usar un **rol de DB de solo lectura** (defensa en profundidad: el permiso lo impone la DB, no solo el script).
- **Output que indica PASS:** cada métrica imprime `✅ ... : PASS  (python=Xms · sql=Yms)` y el cierre:
  `Resumen: 4 PASS · 0 FAIL · 0 SKIP` y `RESULTADO: ✅ Paridad OK ...` con **exit code 0**.
  (Si la URL fuera SQLite, sería `4 SKIP` — eso **no** valida prod, solo confirma que el script corre.)
- **Output que es motivo para FRENAR:** cualquier `❌ ... : FAIL` y/o `RESULTADO: ❌ HAY DIFERENCIAS — NO deployar hasta resolver` (**exit code 1**). El detalle muestra qué métrica difirió y, en grupos (meses/proveedores), cuáles (`DIFF=[...]`). No mergear hasta entender y resolver la causa.
- **Duración aproximada:** depende del volumen filtrado. El script corre las 4 métricas **sin filtros** (peor caso: tablas completas). Con ~319k records y ~86k comparativa_rows, estimado **segundos a ~1-2 min** (la parte "vieja"/python es la que más tarda porque baja muchas filas; la "nueva"/sql es rápida). No bloquea escrituras.
- **¿Registra/expone datos sensibles?** **No.** No imprime `DATABASE_URL` ni credenciales (enmascara host/usuario/pass: solo `backend / db=<nombre>`). No vuelca filas: solo conteos, medianas agregadas y nombres de proveedor/mes ante una diferencia (para diagnóstico). No escribe archivos.

### Criterio de avance
**PASS PostgreSQL (4 PASS · 0 FAIL)** + revisión de este resumen → recién ahí se decide merge/deploy de Batch 1. Cualquier FAIL → frenar y diagnosticar.
