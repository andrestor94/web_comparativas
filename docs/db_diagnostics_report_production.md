# Reporte de diagnóstico de producción (PostgreSQL Render) — PENDIENTE DE GENERAR

> ⏳ **Estado: PLACEHOLDER.** Este archivo todavía **no** contiene datos reales de
> producción. Se completará ejecutando `scripts/db_diagnostics.py` contra el
> PostgreSQL de Render, en tu máquina, con la `DATABASE_URL` en una variable de
> entorno (nunca pegada en el chat ni commiteada).
>
> Hasta entonces, **no hay foto real de producción**. No se inventan números.

---

## Por qué está pendiente

El asistente **no tiene acceso a las credenciales de producción** (la `DATABASE_URL`
de Render) y, por política de seguridad de esta fase, **no debe** pedírtelas por chat
ni ejecutarlas. La ejecución read-only contra producción la corrés vos, localmente.

El script es **solo lectura** (SELECT/inspección; nunca DDL/DML), enmascara el motor
y **no imprime credenciales**, y exige `--confirm-remote` para bases no-SQLite.

---

## Runbook para generar este reporte (credential-safe)

> ⚠️ **Antes de correr el diagnóstico, hacé el backup** (ver
> [`backup_postgres_render_checklist.md`](backup_postgres_render_checklist.md)).
> El diagnóstico es read-only, pero la regla de la fase es: backup primero.

1. Activar el venv e instalar (si hace falta) las dependencias ya presentes
   (`SQLAlchemy`, `psycopg2-binary` ya están en `requirements.txt`).

2. Setear la URL **externa** de Render en una variable de entorno (NO en el comando,
   NO en git). En PowerShell:
   ```powershell
   $env:DATABASE_URL = "<External Database URL de Render>"
   ```
   En bash:
   ```bash
   export DATABASE_URL="<External Database URL de Render>"
   ```

3. Generar el reporte + snapshot JSON (read-only, estimación rápida de filas):
   ```bash
   python scripts/db_diagnostics.py --confirm-remote --estimate \
       --output docs/db_diagnostics_report_production.md \
       --json   docs/db_snapshot_prod.json
   ```
   - `--confirm-remote` es obligatorio para bases no-SQLite (guarda de seguridad).
   - `--estimate` evita `COUNT(*)` pesado en tablas grandes (usa `reltuples`).
   - Para conteos exactos (más lento), omitir `--estimate`.

4. Limpiar la variable de entorno al terminar:
   ```powershell
   Remove-Item Env:DATABASE_URL      # PowerShell
   # unset DATABASE_URL              # bash
   ```

5. Generar la comparación local vs producción. Los snapshots `.json` están
   **gitignoreados** (son artefactos regenerables); regenerá el local antes de comparar:
   ```bash
   # (a) snapshot local (usa el SQLite local por defecto)
   python scripts/db_diagnostics.py --json docs/db_snapshot_local.json \
       --output docs/db_diagnostics_report.md
   # (b) comparar local vs prod
   python scripts/db_compare.py \
       --local docs/db_snapshot_local.json \
       --prod  docs/db_snapshot_prod.json \
       --output docs/db_local_vs_prod.md
   ```

6. (Opcional) Compartir de vuelta `docs/db_diagnostics_report_production.md`,
   `docs/db_snapshot_prod.json` y `docs/db_local_vs_prod.md` — **no contienen
   credenciales ni filas de datos** — para completar el mapa real de `forecast_*`
   y el análisis de riesgos con datos reales.

---

## Qué contendrá este reporte una vez generado

- Motor detectado (PostgreSQL, enmascarado).
- Lista completa de tablas + conteo de filas (estimado o exacto).
- Tamaño por tabla (`pg_total_relation_size`).
- Columnas, tipos y nullabilidad por tabla.
- PK, FK (con tabla/columna destino) e índices (con columnas y unicidad) — apéndice.
- Tablas grandes, vacías y candidatas a revisión.
- Tablas `forecast_*` reales (las de datos base: `forecast_main`, `forecast_valorizado`,
  `forecast_imp_hist`, `forecast_fact_2026`, `forecast_product_labs`).
- Advertencias de compatibilidad SQLite ↔ PostgreSQL.

> La comparación local↔producción y el riesgo se documentan en `docs/db_local_vs_prod.md`
> (generado por `db_compare.py`) y se integran al plan de migración.
