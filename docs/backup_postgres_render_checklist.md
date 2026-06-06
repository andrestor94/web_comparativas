# Checklist de backup y restauración — PostgreSQL en Render

> **Leer y seguir con cuidado antes de tocar cualquier migración en producción.**
> PostgreSQL de Render es la **fuente de verdad**. Sin backup verificado, **no** se ejecuta
> ningún cambio estructural (DDL, índices en prod, integración de `forecast_*`, etc.).

---

## 0. Antes de empezar — qué NO hacer

- ❌ No ejecutar migraciones, `DROP`, `DELETE`, `TRUNCATE` ni el script de ingesta Forecast
  (`migrate_forecast_csv_to_postgres.py` usa `if_exists="replace"`: **destruye y recrea** tablas).
- ❌ No correr `alembic upgrade/downgrade/stamp` contra prod.
- ❌ No exponer ni pegar la `DATABASE_URL` en chats, commits, logs ni capturas.
- ❌ No trabajar sobre la única copia: siempre tener el dump + snapshot antes de tocar nada.

---

## 1. Snapshot de Render (interfaz)

1. Render Dashboard → tu servicio **PostgreSQL** → pestaña **Backups**.
2. Verificar la política de backups automáticos (Render hace snapshots diarios en planes pagos).
3. Crear un **backup manual** justo antes de la intervención (botón *Create Backup* / *Manual Backup*),
   si el plan lo permite.
4. Anotar fecha/hora y el ID del backup en la bitácora del cambio.

> En free-tier los backups automáticos pueden no estar disponibles → el `pg_dump` del paso 2
> es **obligatorio**.

---

## 2. `pg_dump` lógico a archivo (recomendado siempre)

Desde una máquina con `pg_dump` (misma versión mayor que el server o superior), usando la
**External Database URL** de Render (no la interna):

```bash
# La URL se toma de una variable de entorno para NO escribirla en el comando ni en el historial.
export PGURL="<External Database URL de Render>"   # NO commitear, NO compartir

# Dump comprimido (formato custom, ideal para pg_restore selectivo):
pg_dump "$PGURL" --format=custom --no-owner --no-privileges \
  --file="siem_backup_$(date +%Y%m%d_%H%M%S).dump"

# Alternativa SQL plano (legible, restaurable con psql):
# pg_dump "$PGURL" --no-owner --no-privileges --file="siem_backup_$(date +%Y%m%d_%H%M%S).sql"
```

> En PowerShell: `$env:PGURL = "<URL>"` y luego `pg_dump $env:PGURL ...`.
> Borrar la variable al terminar: `unset PGURL` / `Remove-Item Env:PGURL`.

---

## 3. Verificar que el backup existe y sirve

1. El archivo existe y **pesa lo razonable** (no 0 bytes):
   ```bash
   ls -lh siem_backup_*.dump
   ```
2. Listar el contenido del dump sin restaurar (sanity check):
   ```bash
   pg_restore --list siem_backup_*.dump | head -40
   ```
   Debe listar las tablas esperadas (`users`, `uploads`, `comparativa_rows`, `dimensionamiento_*`,
   `pliego_*`, `forecast_*`, etc.).
3. **Restaurar en una base de PRUEBA** (local o una instancia descartable) y comparar conteos:
   ```bash
   # En una base vacía de prueba:
   pg_restore --no-owner --dbname="postgresql://.../siem_test" siem_backup_*.dump
   ```

---

## 4. Guardar conteos ANTES (línea base)

Con **tu autorización**, correr el diagnóstico read-only contra producción:

```bash
DATABASE_URL="$PGURL" python scripts/db_diagnostics.py \
  --confirm-remote --estimate --output docs/db_diagnostics_prod_ANTES.md
```

- `--confirm-remote` es obligatorio (guarda de seguridad del script).
- `--estimate` evita `COUNT(*)` pesado en tablas grandes (usa `reltuples`).
- El reporte **no** contiene credenciales ni filas de datos: es seguro de commitear.

Guardar `docs/db_diagnostics_prod_ANTES.md` como evidencia.

---

## 5. Restauración en caso de emergencia

1. **No** seguir operando sobre la base afectada.
2. Opción A — Snapshot de Render: *Restore* desde el panel (crea/repone la base al punto del snapshot).
3. Opción B — Dump:
   ```bash
   # Restaurar a una base nueva y luego repuntar DATABASE_URL del servicio:
   pg_restore --no-owner --clean --if-exists --dbname="$PGURL_NUEVA" siem_backup_*.dump
   ```
4. Repuntar `DATABASE_URL` del servicio web si se restauró en instancia nueva.
5. Verificar conteos **DESPUÉS** con `db_diagnostics.py` y comparar contra `..._ANTES.md`.

---

## 6. Validación antes de tocar migraciones (gate final)

Marcar todo antes de ejecutar cualquier DDL en producción:

- [ ] Snapshot de Render creado y anotado.
- [ ] `pg_dump` generado, > 0 bytes, `pg_restore --list` OK.
- [ ] Dump **restaurado y verificado** en base de prueba.
- [ ] `docs/db_diagnostics_prod_ANTES.md` guardado (conteos línea base).
- [ ] Migración a aplicar **revisada a mano** y con `downgrade()` real.
- [ ] Ventana de mantenimiento acordada (bajo tráfico).
- [ ] Plan de rollback escrito y a mano.
- [ ] Autorización explícita registrada.

> Si **algún** ítem no está, **no** se ejecuta el cambio. Se posterga hasta completarlo.

---

## 7. Después del cambio

- [ ] `db_diagnostics.py` → `docs/db_diagnostics_prod_DESPUES.md`.
- [ ] Conteos coinciden con la línea base (o la diferencia es esperada y está documentada).
- [ ] Smoke test de los módulos: Forecast, Mercado Público, Mercado Privado, Pliegos, Dimensionamiento.
- [ ] Logs sin errores nuevos.
- [ ] Backup post-cambio (nuevo punto de restauración limpio).
