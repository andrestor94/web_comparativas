# Fase 5 — Batch 2: scripts SQL (REVISABLES, NO EJECUTAR)

> ⚠️ **Estado: PROPUESTA.** Estos `.sql` están para **revisar**, no para ejecutar.
> Ningún proceso los corre automáticamente. **No** se conectan a producción.
> **No se ejecuta nada de Batch 2** hasta que:
> 1. haya un **backup verificado** (ver `docs/fase5_backup_rollback.md`), y
> 2. Batch 1 esté validado contra PostgreSQL (PASS) y mergeado/deployado, y
> 3. exista **autorización explícita** para cada script.

## Principios aplicados
- **Aditivo primero:** las tablas nuevas (coarse summary) **no reemplazan** a las actuales.
  Se crean, se pueblan y se validan; el código sigue leyendo las tablas viejas hasta una
  fase posterior.
- **Índices con `CONCURRENTLY`** donde aplica (no bloquea escrituras; **no** puede ir dentro
  de una transacción `BEGIN/COMMIT`).
- **Sin `DROP` irreversible:** el único `DROP` propuesto es de **índices duplicados exactos**,
  y es reversible recreándolos desde el DDL guardado. Va en archivo separado (`04`) y
  **comentado** (propuesta, no ejecución).
- **Renames a `zz_deprecated_*`:** NO están aquí. Son Batch 3, solo propuesta.
- Cada script trae cabecera con **objetivo · riesgo · backup requerido · rollback · validación**.

## Archivos
| # | Archivo | Objetivo | Riesgo | Ejecutable cuando |
|---|---|---|---|---|
| 01 | `01_dim_summary_grueso_aditivo.sql` | Tabla nueva agregada sin dimensión cliente | Bajo (aditivo) | Backup + OK |
| 02 | `02_comparativa_rows_indices.sql` | Índices candidatos para Mercado Público | Bajo (aditivo, CONCURRENTLY) | Backup + OK |
| 03 | `03_auditoria_indices_dimensionamiento.sql` | Auditoría READ-ONLY de índices (idx_scan, duplicados) | Nulo (solo SELECT) | Cuando quieras (read-only) |
| 04 | `04_forecast_indices_duplicados_PROPUESTA.sql` | Detectar + (propuesta) drop de índices forecast duplicados | Medio (drop reversible) | Backup + OK explícito |
| 05 | `05_validacion_antes_despues.sql` | Conteos/sumas/índices para comparar antes vs después | Nulo (solo SELECT) | Antes y después de cada cambio |
| 06 | `06_rollback.sql` | Reversa de 01/02/04 | — | Solo si hay que revertir |

## Orden sugerido de ejecución (cuando se autorice)
1. `05` (snapshot "antes") → `03` (auditoría) → backup (§ doc) →
2. `01` y/o `02` (aditivos) → `05` (snapshot "después") → validar →
3. `04` solo si la auditoría confirma duplicados y hay backup → `05` de nuevo.
4. `06` únicamente ante un problema.
