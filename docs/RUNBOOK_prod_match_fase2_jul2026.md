# RUNBOOK — Producción: Match (ambos mercados) + Fase 2 identidad + fix constraint + APP_SECRET

**Fecha:** julio 2026 · **Ejecuta:** Andrés · **Todos los comandos son de UNA línea para PowerShell de Windows** (usar `curl.exe`, NUNCA `curl` a secas — es alias de Invoke-WebRequest).

**Tokens que vas a necesitar:**
- `<TOKEN>` = valor de `DIMENSIONAMIENTO_IMPORT_TOKEN` (está en Render → tu servicio → Environment, y en tu `.env` local). Es el mismo para el push de identidad y el de Match.
- `<URL>` = URL base de prod, p.ej. `https://tu-app.onrender.com` (sin barra final).

**Orden recomendado:** Paso 1 (deploy) → Paso 2 (verificar constraint) → Paso 3 (Fase 2 identidad) → Paso 4 (datos de Match) → Paso 5 (encender Match + APP_SECRET en un solo redeploy) → Paso 6 (verificación final). Cada paso tiene su verificación y su rollback; se pueden ejecutar en días distintos.

---

## Paso 1 — Deploy del código (push a main)

El push a main dispara el deploy automático en Render. **El timing lo decidís vos** (los pasos 3 y 4 corren con la app en vivo sin molestar a los usuarios; el paso 5 redeploya e invalida sesiones → mejor fuera de horario pico).

```powershell
git push origin main
```

**Verificar en el log de arranque de Render** (Dashboard → servicio → Logs):
- `[MIGRATION] uq_dim_family_monthly_summary ya existe con las 12 columnas esperadas. (OK, skip)` — **este es el fix de la Parte C**. Si en cambio ves `[MIGRATION][WARN] uq_dim_family_monthly_summary existe con columnas [...]`, anotalo y seguí: el Paso 2 te dice qué hacer. Lo que NO tiene que aparecer nunca más es el `QueryCanceled ... statement timeout`.
- `[STARTUP] match precalc (solo lectura, sin computo server-side): match_negocio_map=0, match_demanda_desc=0.` — normal ANTES del push de datos de Match (Paso 4). Después del Paso 4 va a decir `3238` y `28598`.
- `[MIGRATION] Tables ensured via create_all.` — crea las tablas `match_*` (vacías, no pesa nada).
- Que NO haya `Traceback` ni `ERROR REAL` en el bloque de migraciones.

**Rollback:** Render → servicio → Deploys → botón **Rollback** al deploy anterior. En git: `git revert <hash>` y push (revertir, no force-push).

---

## Paso 2 — Constraint del summary (verificar; rebuild SOLO si hace falta)

El arranque ya no recrea la constraint. Verificá qué definición tiene prod:

```powershell
curl.exe -s "<URL>/api/mercado-privado/dimensiones/admin/estado-identidad" -H "X-Import-Token: <TOKEN>"
```

Mirá el campo nuevo `constraint_summary_cols`:
- **Si lista las 12 columnas** (`month, plataforma, cliente_nombre_homologado, cliente_visible, provincia, familia, unidad_negocio, subunidad_negocio, resultado_participacion, is_identified, is_client, import_run_id`) → **no hay nada que hacer**. Fin del paso.
- **Si la lista es distinta** (por ejemplo sin `import_run_id`): el push de datos de Dimensionamiento podría descartar filas en silencio entre corridas. Ejecutá el rebuild deliberado (construye el índice con `CREATE UNIQUE INDEX CONCURRENTLY`, sin bloquear la tabla, y hace un swap corto al final):

```powershell
curl.exe -s -X POST "<URL>/api/mercado-privado/dimensiones/admin/rebuild-summary-constraint" -H "X-Import-Token: <TOKEN>"
```

Duración estimada: 2–10 minutos (según tamaño del summary; el request queda esperando — si tu consola corta antes, el índice sigue construyéndose: repetí el mismo comando, limpia el residuo y reintenta solo). Respuesta esperada: `{"ok": true, "rebuilt": true, "cols_after": [las 12]}`. Si ya estaba bien: `{"ok": true, "rebuilt": false, ...}`.

**Rollback:** no aplica (si el rebuild falla, la constraint vieja queda intacta y el error vuelve en la respuesta).

---

## Paso 3 — Fase 2 de identidad (records del run 67)

Puebla `cliente_entidad_id` en las 364.887 filas de `dimensionamiento_records` del run 67 (los UPDATE filtran SIEMPRE por `import_run_id` y solo tocan filas NULL → reanudable). Corre desde tu PC, con el server de prod en vivo:

```powershell
python -m scripts.push_identity --url <URL> --token <TOKEN> --records --remote-run 67
```

- **Qué hace:** re-aplica FASE 1 (registry + summary; idempotente, los lotes ya aplicados tocan 0 filas) y después FASE 2: 186 CUITs en 24 lotes + 187 nombres en 24 lotes.
- **Duración estimada:** 3–10 minutos (la misma Fase 2 completa tardó 30 segundos contra el server local; en prod sumá la latencia de red de los ~50 requests y el Postgres chico de Render). Si un lote supera el timeout: reejecutá el MISMO comando — continúa donde quedó.
- **Salida esperada al final:** `✅ FASE 1 COMPLETA (run 67). summary_null=0 records_null=0`.

**Verificación:**

```powershell
curl.exe -s "<URL>/api/mercado-privado/dimensiones/admin/estado-identidad" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"records_filas_identidad_null": 0` (y sigue `"entidades": 256`, `"modo_card": "identidad"`, `"summary_filas_identidad_null": 0`).

**Rollback:** no hace falta — solo escribe una columna que estaba NULL; la card ya se sirve por summary y no cambia. Si algo saliera mal a mitad, el estado queda parcial y reanudable, nunca roto.

---

## Paso 4 — Datos de Match (propuestas + tablas precalculadas)

Sube desde tu base local: **64.223 propuestas** (33 lotes de 2.000), **3.238** filas de `match_negocio_map` (2 lotes) y **28.598** de `match_demanda_desc` (15 lotes). La corrida remota nace `pending_approval` y **solo se aprueba al final si el conteo coincide** — un push cortado jamás queda como corrida vigente. Se puede correr con el módulo todavía apagado (el push no depende de MATCH_ENABLED).

```powershell
python -m scripts.push_match_data --url <URL> --token <TOKEN>
```

- **Duración estimada:** 5–10 minutos (~52 requests).
- **Si un lote corta:** el script te imprime el número de corrida remota; reanudá con `python -m scripts.push_match_data --url <URL> --token <TOKEN> --resume-run <numero>`.
- **Salida esperada:** `✅ PUSH COMPLETO: corrida remota N APROBADA (64223 filas, 2252 artículos).`

**Verificación:**

```powershell
curl.exe -s "<URL>/api/mercado-privado/match/admin/estado" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"run_vigente_filas": 64223, "match_negocio_map": 3238, "match_demanda_desc": 28598`.

**Rollback:** con el módulo apagado nadie ve nada; si querés descartar la corrida subida, avisame y preparo el endpoint de rollback (hoy: se ignora — la próxima corrida aprobada la reemplaza como vigente).

---

## Paso 5 — Encender Match + APP_SECRET (un solo redeploy, fuera de horario pico)

En Render → servicio → **Environment → Add Environment Variable**, cargá LAS DOS juntas (así Render redeploya una sola vez):

1. **`MATCH_ENABLED`** = `1`

2. **`APP_SECRET`** = un secreto fuerte generado en tu PC con:

```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

Copiá el valor impreso (64 caracteres hex) y pegalo como valor de `APP_SECRET`. **Al guardar, Render redeploya automáticamente.**

⚠️ **Consecuencia de APP_SECRET:** se invalidan TODAS las sesiones activas — todos los usuarios (incluido vos) tienen que volver a loguearse. Hacelo fuera de horario pico (hoy había un auditor conectado). El warning `[SECURITY] APP_SECRET no definida` desaparece del log de arranque.

Nota: la app lee `APP_SECRET` del environment (verificado en `main.py::_get_app_secret`; nada lo pisa). Opcional a futuro: setear también `APP_ENV=production` hace que la app se NIEGUE a arrancar sin APP_SECRET (y marca las cookies como https-only) — si lo hacés, hacelo DESPUÉS de confirmar que APP_SECRET quedó bien, nunca junto.

**Rollback:** borrar `MATCH_ENABLED` (o ponerla en `0`) apaga el módulo al instante del próximo redeploy: desaparece del sidebar y la API responde 404, sin tocar datos. `APP_SECRET` no se rollbackea (solo re-invalidaría sesiones de nuevo).

---

## Paso 6 — Verificación final en la página real

1. Entrá a `<URL>` y logueate (sesión nueva por el APP_SECRET).
2. **Mercado Privado** → el sidebar muestra **Match (Nuevo)** → entrá: tablero con 64.223 propuestas / 2.252 artículos, filtros Negocio/Subnegocio en cascada, Papelera y export Excel.
3. **Mercado Público** → el mismo **Match** en el sidebar → entrá: misma pantalla y datos, con el sidebar marcando Mercado Público.
4. **Permisos:** SIC → Usuarios → editá un usuario de prueba: UNA sola casilla "Match" (bajo Mercado Privado). Tildarla lo habilita en AMBOS mercados; destildarla lo saca de ambos. Gerente/Auditor ven pero no pueden homologar/descartar (los botones devuelven 403).
5. **Dimensionamiento intacto:** la card de identidad sigue mostrando **256 ENTIDADES (158 clientes · 98 no clientes)** sin pill amarillo.
6. Log de arranque del último deploy: sin `QueryCanceled`, sin `[SECURITY] APP_SECRET`, y `match precalc ... match_negocio_map=3238, match_demanda_desc=28598`.

---

## Resumen de rollbacks

| Qué salió mal | Acción |
|---|---|
| El deploy del Paso 1 rompe algo | Render → Deploys → Rollback (botón) y/o `git revert` + push |
| Rebuild de constraint falla | Nada queda a medias; reintentar o dejar como estaba (el arranque ya no molesta) |
| Fase 2 corta a mitad | Reejecutar el mismo comando (reanudable, filas NULL solamente) |
| Push de Match corta | `--resume-run <n>`; la corrida nunca se aprueba incompleta |
| Match no debería verse | `MATCH_ENABLED=0` en Environment (redeploy) |
| Sesiones invalidadas molestan | Esperado tras APP_SECRET; avisar a los usuarios que re-logueen |
