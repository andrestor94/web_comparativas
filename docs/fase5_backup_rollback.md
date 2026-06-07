# Fase 5 — Procedimiento de Backup y Rollback (producción Render/PostgreSQL)

> **Propósito:** dejar por escrito, paso a paso, cómo respaldar la base de producción y
> cómo revertir cada tipo de cambio **antes** de ejecutar cualquier intervención del
> Batch 2 (índices/estructura) o Batch 3 (renames/retención).
>
> **Regla de la fase:** se toca producción **solo** con evidencia, backup verificado,
> script revisable, ejecución en transacción cuando se pueda, validación antes/después y
> rollback probado. Los cambios del **Batch 1 son solo código** (medianas→SQL): NO
> requieren backup de datos — su rollback es `git revert` + redeploy.

---

## Checklist práctico (TL;DR)

**Opciones de backup**
- **Opción A — Snapshot de Render** (gestionado): rápido, sin descargar archivo. Ideal como
  punto de control y para PITR. (§1)
- **Opción B — `pg_dump` a archivo** (portátil): copia descargable, restaurable selectivo,
  sirve para restaurar en una base local y probar la migración antes. (§2)
- **Recomendada:** **A + B juntas** para Batch 2 estructural — snapshot de Render como red de
  seguridad gestionada **y** un `pg_dump` específico de las tablas afectadas para poder
  restaurar/probar offline. Para cambios solo-de-índice alcanza con A + capturar el DDL (§3).

**Pasos mínimos antes de ejecutar cualquier cosa de Batch 2/3**
- [ ] Backup tomado (A y/o B) — anotar fecha/hora UTC + ID/archivo.
- [ ] Backup **verificado** (existe y es restaurable — ver §2).
- [ ] DDL/estado afectado capturado (§3) y pegado en el changelog (§5).
- [ ] Script `.sql` revisado y aprobado (sin ejecutar).
- [ ] Plan de rollback escrito para ese cambio (§0 + §7).
- [ ] Validación antes/después definida (`phase5_parity_check.py` + conteos).

> Hasta tener una vía de backup **verificada**, Batch 2 (índices/estructura) queda
> *listo-pero-no-ejecutado*. Batch 1 (código) no depende de esto.

---

## 0. Qué requiere backup y qué no

| Cambio | Tipo | ¿Backup de datos? | Rollback |
|---|---|---|---|
| Batch 1 — medianas→SQL (código) | Código read-only | **No** (no toca datos/estructura) | `git revert <commit>` + redeploy |
| Crear índice nuevo (`CONCURRENTLY`) | Aditivo | Recomendado (snapshot) | `DROP INDEX CONCURRENTLY <idx>` |
| Crear tabla/vista consolidada nueva | Aditivo | Recomendado | `DROP TABLE/VIEW <obj>` |
| **Drop de índice** duplicado/no usado | Reversible si se guarda DDL | **Sí** + guardar `pg_get_indexdef` | Re-`CREATE INDEX CONCURRENTLY` con el DDL guardado |
| `ALTER TYPE` / normalización de columnas | Potencialmente destructivo | **Sí, obligatorio** | Restore desde dump (o columna espejo) |
| Rename a `zz_deprecated_*` | Reversible | **Sí** | `ALTER TABLE ... RENAME TO <original>` |
| Retención/purga `usage_events` | Destructivo (borra filas) | **Sí, obligatorio** | Restore desde dump |

> Para drops de índice, el "backup" más importante es **capturar su DDL** (ver §3): así
> el rollback es recrearlo, sin necesidad de restaurar toda la base.

---

## 1. Backup completo — Opción A: Snapshot de Render (recomendado)

Render Postgres (planes pagos) mantiene backups automáticos diarios y permite snapshots
manuales + *Point-in-Time Recovery*.

1. Entrar a **Render Dashboard → tu base de datos PostgreSQL**.
2. Pestaña **Backups** (o **Recovery**).
3. Verificar la fecha del último backup automático. Para un punto de control explícito
   antes del cambio: **"Create Backup" / "Manual Snapshot"** (si el plan lo permite) o
   anotar el timestamp para PITR.
4. Anotar en el changelog (§5): fecha/hora UTC del snapshot + ID si lo da.

> Render conserva los backups según el plan (p. ej. 7 días). PITR permite restaurar a un
> instante exacto. **Esto NO descarga un archivo a tu disco** — para tener una copia
> local/portátil usar la Opción B.

---

## 2. Backup completo — Opción B: `pg_dump` a archivo (copia portátil)

Necesitás la **External Database URL** de Render (Dashboard → base → *Connections* →
*External Database URL*) y `pg_dump` instalado (viene con PostgreSQL client tools).

> ⚠️ La External URL contiene credenciales: **no** la pegues en commits, logs ni chats.
> Pasala por variable de entorno.

### Windows (PowerShell)
```powershell
# 1) Cargar la URL externa SOLO en la sesión actual (no queda en disco)
$env:PGURL = "postgresql://USER:PASS@HOST.oregon-postgres.render.com/DBNAME?sslmode=require"

# 2) Dump en formato custom (comprimido, restaurable selectivo). Carpeta fuera del repo.
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
pg_dump $env:PGURL --format=custom --no-owner --no-acl `
  --file "C:\tmp\siem_backup_$ts.dump"

# 3) Verificar que el dump es legible y listar su contenido (sin restaurar)
pg_restore --list "C:\tmp\siem_backup_$ts.dump" | Select-Object -First 40

# 4) Limpiar la credencial de la sesión
Remove-Item Env:\PGURL
```

### Solo tablas afectadas (backup específico, más rápido)
```powershell
pg_dump $env:PGURL --format=custom --no-owner --no-acl `
  --table=dimensionamiento_records `
  --table=dimensionamiento_family_monthly_summary `
  --file "C:\tmp\siem_dim_$ts.dump"
```

### Verificación del backup (obligatoria antes de seguir)
- `pg_restore --list` no da error y muestra las tablas esperadas.
- Tamaño del `.dump` > 0 y coherente con el tamaño reportado por `db_diagnostics.py`.
- (Opcional fuerte) restaurar en una base **local** vacía y correr `phase5_parity_check.py`
  contra ella:
  ```powershell
  createdb siem_restore_test
  pg_restore --no-owner --no-acl --dbname siem_restore_test "C:\tmp\siem_backup_$ts.dump"
  $env:DATABASE_URL="postgresql:///siem_restore_test"; python scripts/phase5_parity_check.py --confirm-remote
  ```

> **Importante:** los `.dump` **no se commitean** (contienen datos productivos).
> Guardarlos fuera del repo (`C:\tmp\`) o en almacenamiento cifrado.

---

## 3. Antes de tocar índices — capturar el estado actual

El rollback de un `DROP INDEX` es recrearlo. Para eso hay que **guardar su definición**
exacta y la evidencia de uso (read-only, no modifica nada):

```sql
-- DDL exacto de todos los índices de una tabla (guardar la salida en el changelog)
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('dimensionamiento_records',
                    'dimensionamiento_family_monthly_summary',
                    'forecast_main', 'forecast_valorizado');

-- Evidencia de USO de cada índice (idx_scan = 0 ⇒ candidato a retiro)
SELECT relname AS tabla, indexrelname AS indice, idx_scan, idx_tup_read,
       pg_size_pretty(pg_relation_size(indexrelid)) AS tamano
FROM pg_stat_user_indexes
ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC;
```

`db_diagnostics.py` ya vuelca los índices por tabla; complementarlo con `idx_scan`
(arriba) es lo que habilita proponer retiros con evidencia.

---

## 4. Patrón seguro de ejecución (Batch 2/3)

1. **Backup** (§1 o §2) + **verificación** (§2).
2. **Capturar DDL/estado** afectado (§3) y pegarlo en el changelog (§5).
3. **Script SQL revisable** (un archivo `.sql` versionado en `docs/` o `scripts/`).
4. Ejecutar **en horario de bajo tráfico**. Para DDL de índices en tablas grandes usar
   `CREATE/DROP INDEX CONCURRENTLY` (no bloquea escrituras; **no** puede ir dentro de una
   transacción). Para cambios de datos, envolver en `BEGIN; ... ; COMMIT;` y revisar antes
   del commit.
5. **Validar después:** `phase5_parity_check.py` + conteos por tabla (`db_diagnostics.py`)
   + smoke test de cada módulo (Forecast, Mercado Público, Mercado Privado, Pliegos,
   Dimensionamiento) + tiempos vs baseline.
6. Registrar resultado en el changelog (§5). Si algo falla → ejecutar el **rollback**
   correspondiente (§0) y re-validar.

---

## 4bis. Qué hacer si falla una migración o un índice (rollback por caso)

| Falla | Síntoma | Acción inmediata | Rollback |
|---|---|---|---|
| `CREATE INDEX CONCURRENTLY` se interrumpe | Queda un índice **INVALID** (`pg_index.indisvalid = false`) | No reintentar encima | `DROP INDEX CONCURRENTLY <idx>;` y volver a crear |
| `CREATE INDEX` (sin CONCURRENTLY) bloquea escrituras | La tabla queda bloqueada / la app se traba | Cancelar la sesión (`pg_cancel_backend`) | Nada que revertir si no terminó; si terminó, `DROP INDEX` |
| `DROP INDEX` de un índice que sí se usaba | Endpoints más lentos tras el cambio | Confirmar con `EXPLAIN` | Re-`CREATE INDEX CONCURRENTLY` con el **DDL guardado** (§3) |
| Poblar tabla nueva (coarse summary) falla a mitad | Tabla parcial | La tabla es **aditiva**, nadie la lee aún | `TRUNCATE <tabla_nueva>;` y reintentar, o `DROP TABLE <tabla_nueva>;` |
| Cambio de datos (retención/normalización) con resultado inesperado | Conteos/sumas no coinciden post-cambio | **Detener**, no seguir | `ROLLBACK;` si está en transacción; si ya hubo `COMMIT`, **restaurar desde el dump** (§2) |
| La app muestra errores tras un cambio | 500 / datos vacíos | Revisar logs | Revertir el cambio (rollback de arriba) y, si es de código, `git revert` + redeploy |

**Principios:**
- Cambios de **datos** siempre dentro de `BEGIN; ... ; COMMIT;` y revisados **antes** del commit.
- DDL con `CONCURRENTLY` **no** puede ir en transacción → su seguro es el backup + el DDL guardado para recrear.
- Si hay cualquier duda → `ROLLBACK` / no commitear / restaurar desde dump. Nunca "arreglar a mano" sin script.

---

## 5. Changelog de intervenciones (llenar en cada cambio)

| Fecha | Cambio | Backup (export + verif.) | DDL previo guardado | Script | Validación | Resultado | Rollback |
|---|---|---|---|---|---|---|---|
| 2026-06-07 | **Batch 2 · Tanda 1** — `DROP INDEX CONCURRENTLY ix_dim_sum_familia_qty` (80 MB, índice muerto del summary fino) | Export Render 2026-06-07 ~15:00 (`.dir.tar.gz`, disponible) + PITR 7 días | `CREATE INDEX ix_dim_sum_familia_qty ON public.dimensionamiento_family_monthly_summary USING btree (familia, total_cantidad)` | `scripts/sql/phase5_batch2/07_summary_dead_indexes_cleanup_PROPUESTA.sql` (Tanda 1) | Pre: `idx_scan=0`, `idx_tup_read=0`. Post: índices summary **413 → 333 MB** (−80 MB); `filas_summary=259.702` sin cambios; PK + `uq_dim_family_monthly_summary` presentes; EXPLAIN Top-familias/Geo/Serie = `Parallel Seq Scan` (plan idéntico); smoke test UI OK, sin 500 | ✅ Éxito | No necesario |

---

## 6. Acceso pendiente de confirmar

- [ ] ¿Plan de Render permite snapshots manuales / PITR? (define Opción A)
- [ ] ¿Tenés la *External Database URL* y `pg_dump` disponible? (define Opción B)
- [ ] ¿Dónde se guardan los `.dump` de forma segura (cifrado/fuera del repo)?

Hasta confirmar al menos una vía de backup **verificada**, el Batch 2 (índices/estructura)
queda en estado *listo-pero-no-ejecutado*. El Batch 1 (código) no depende de esto.
