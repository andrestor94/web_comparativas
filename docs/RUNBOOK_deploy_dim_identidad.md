# Runbook — Deploy resolución de identidad de clientes (Dimensionamiento)

> ⚠️ **DESACTUALIZADO (jul 2026):** el endpoint `resolve-entities` de este runbook devuelve
> 410 y el backfill de arranque ya no existe (el server no calcula identidad; viaja como dato
> con `scripts/push_identity.py`). Usar **`RUNBOOK_prod_dim_identidad_jul2026.md`**. Solo
> sigue vigente la sección de ROLLBACK.

Reemplazá `<PROD_HOST>` por el host de Render y `<IMPORT_TOKEN>` por el valor de
`DIMENSIONAMIENTO_IMPORT_TOKEN` (el mismo token que usás para el push).
El **run 67 se preserva como rollback** — no se borra en ningún paso.

**Número esperado:** el run 67 se cargó con el mismo dataset que local (364.887 registros,
2.451 familias, 15 provincias, misma SUM de valorización). Prod **debe dar 256 / 158 / 98**.
Si da otra cosa, algo falló → rollback. ⚠️ Si ves **374**, NO es "el número viejo": es el
**fallback activo** (identidad sin resolver). Se distingue con el endpoint del paso 2, NO a ojo.

## 1. Merge + deploy (fuera de horario de uso)
- Merge de la rama a `main` y push. Render auto-despliega al detectar el push a `main`.
- En Render: panel → tu servicio → pestaña **Logs** (o **Events** para el estado del deploy).
- En el log de arranque, confirmá (ver detalle de líneas en el Apéndice):
  `SUCCESS: columnas de identidad...` · `Tabla resumen OK, no requiere reconstrucción` (B) ·
  `entidad_backfill: ... resolviendo...` y luego `entidad_backfill: COMPLETADO run <N> stats={... 'total': 256 ...}`.

## 2. (OBLIGATORIO) Verificar el estado de identidad por el endpoint — señal POSITIVA
El criterio de éxito NO es "la página no se rompió". Consultá el estado (una línea):
```
curl -s "https://<PROD_HOST>/api/mercado-privado/dimensiones/admin/estado-identidad" -H "X-Import-Token: <IMPORT_TOKEN>"
```
Debe devolver: `"registry_poblado": true`, `"modo_card": "identidad"`, `"entidades": 256`,
`"entidades_si": 158`, `"entidades_no": 98`, `"summary_filas_identidad_null": 0`,
`"records_filas_identidad_null": 0`, y `"ultima_resolucion"` con timestamp reciente.
- Si `"modo_card": "fallback"` o `"registry_poblado": false` → la card está mostrando el
  número provisorio. **NO avises a los usuarios.** Ejecutá el backfill manual:
  ```
  curl -s -X POST "https://<PROD_HOST>/api/mercado-privado/dimensiones/admin/resolve-entities" -H "X-Import-Token: <IMPORT_TOKEN>"
  ```
  Esperá 1-3 min (Postgres) y repetí el GET de estado hasta que dé `"identidad"`.

## 3. Verificar en la PÁGINA REAL (navegador, hard refresh Ctrl+Shift+R)
- La card debe mostrar **256** con desglose **158 clientes · 98 no clientes**.
- Como Admin, NO debe aparecer el pill amarillo "Identidad sin resolver". Si aparece →
  estás en fallback (volvé al paso 2).

## 4. Verificar que lo demás no se movió
- **RENGLONES 364.887**, **FAMILIAS 2.451**, **PROVINCIAS 15** sin cambios.
- **SUM(cantidad)** y **SUM(valorización)** idénticas a antes del deploy (la resolución no
  toca agregados de volumen ni dinero).

## 5. Verificar el dropdown "Cliente"
- **256 opciones**, sin duplicados por mayúsculas/acentos/plataforma.

## 6. Medir el ahorro de arranque (B)
- Comparar en Render el tiempo del deploy hasta "Live" antes vs después. B elimina la
  reconstrucción del summary (~300k filas) que corría en CADA arranque. Reportar la diferencia.

## 7. Recién ahora: avisar a los usuarios del cambio de 374 a 256.

---
## ROLLBACK (si el paso 2 o 3 no da)
La migración es **aditiva**: las columnas de entidad son nullable y el código viejo no las
usa. El run 67 está intacto. Traé los logs de arranque + la respuesta del endpoint de estado.
1. En Render: **Rollback** al deploy anterior (Deploys → deploy previo → "Rollback"),
   o revertir el merge y pushear:
   ```
   git revert -m 1 <SHA_DEL_MERGE> && git push origin main
   ```
2. Con el código viejo, la card vuelve a contar por `cliente_visible` (número anterior). Las
   columnas/tabla de identidad quedan en la base sin efecto (no molestan al código viejo).
3. No se toca el run 67 ni los datos: es solo rollback de código.

---
## Apéndice — líneas del log de arranque y qué significan
- `[MIGRATION] SUCCESS: columnas de identidad de clientes verificadas/creadas.` → esquema OK.
- `[MIGRATION] Tabla resumen OK, no requiere reconstrucción.` → **B**: no reconstruyó de más.
  (Un `ALERTA ... Reconstruyendo` una sola vez es aceptable si el summary estaba stale.)
- `[MIGRATION] entidad_backfill: registry VACIO para run <N> ... -> resolviendo...` → arranque
  en frío detectado; backfill automático en background empezó. Durante esta ventana la card
  muestra el número anterior (fallback), nunca 0.
- `[MIGRATION] entidad_backfill: COMPLETADO run <N> stats={... 'total': 256, 'si': 158, 'no': 98 ...}`
  → backfill terminado con el número resuelto.
- `[MIGRATION] entidad_populated: summary run <N> OK` → capa C: 0 filas para reparar.
- `[DIM][IDENTITY] registry ... card cae al conteo anterior` → un request pegó durante la
  ventana de fallback (esperado hasta que termine el backfill).
