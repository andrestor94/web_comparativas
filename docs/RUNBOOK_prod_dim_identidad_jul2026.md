# Runbook — Subir la identidad de clientes a PRODUCCIÓN (Dimensionamiento, julio 2026)

**Para:** Andrés (único con acceso a Render). **Todos los comandos son de UNA línea, para
PowerShell de Windows.** Usar siempre `curl.exe` (con el `.exe`): `curl` a secas es alias de
`Invoke-WebRequest` y no acepta `-H`.

**Situación:** prod (run 67) está en FALLBACK — la card muestra 374 + pill amarillo — porque
el registry de entidades está vacío. La causa raíz era un bug de migraciones (ya corregido en
el commit `34fc6720`, pusheado a `main`): en Postgres, el primer `ALTER TABLE ... ADD COLUMN`
sobre una columna que ya existía lanzaba "already exists" y **abortaba la transacción entera**;
las 8 sentencias siguientes (2 columnas del summary + 6 índices) caían en cascada con
`InFailedSqlTransaction`, y el log mentía "SUCCESS". Esa transacción abortada, sin rollback,
retenía locks sobre las tablas de dimensionamiento — por eso el push de identidad timeouteaba
(600 s), un DELETE de 256 filas tardaba 140 s y un COUNT 22 s. **No era falta de recursos.**
El deploy del fix reinicia el proceso, mata la transacción atascada y libera los locks.

**Número esperado al final:** card **256** (158 clientes · 98 no clientes), **sin** pill amarillo.
Si ves **374**, es el fallback todavía activo (no es "un número viejo").

---

## 1. Confirmar que el fix está deployado en Render

El fix ya está en `origin/main` (commits `8845f758`, `13d4ec19`, `34fc6720` — pusheados el
22/07). No hay nada que mergear.

1. Render → servicio **web-comparativas** → pestaña **Events** (o **Deploys**).
2. Mirar el commit del último deploy "Live":
   - Si es `34fc6720` (o posterior) → ya está, seguí al paso 2.
   - Si es anterior (p. ej. `6ad0cff1` o `c879e1ca`) → botón **Manual Deploy → Deploy latest
     commit** y esperá a que quede "Live".

## 2. Leer el log de arranque (Render → pestaña Logs)

Buscar el bloque de identidad. Con el fix, **cada sentencia loguea su resultado real**:

```
[MIGRATION] Verificando columnas de identidad de clientes en Dimensionamiento...
[MIGRATION] dimensionamiento_records.cliente_entidad_id: ya existe. (OK, idempotente)
[MIGRATION] dimensionamiento_family_monthly_summary.cliente_entidad_id: aplicado.   ← o "ya existe"
[MIGRATION] dimensionamiento_family_monthly_summary.es_cliente_entidad: aplicado.   ← o "ya existe"
[MIGRATION] ix_dim_records_entidad: aplicado.                                       ← ídem los 6 índices
...
[MIGRATION] SUCCESS: columnas/indices de identidad verificados/creados (todo OK o ya existente).
```

**Debe cumplirse:**
- CERO apariciones de `InFailedSqlTransaction` en todo el log de arranque.
- Las 2 columnas del summary y los 6 índices (`ix_dim_records_entidad`, `ix_dim_summary_entidad`,
  `ix_dim_summary_es_cliente_entidad`, `ix_dim_records_run_cuit`, `ix_dim_records_run_original`,
  `ix_dim_records_run_visible`) con "aplicado" o "ya existe" — ninguno fallido.
- La línea final `SUCCESS: columnas/indices de identidad ...`. Si en cambio aparece
  `[MIGRATION] ATENCION: alguna sentencia de identidad fallo con ERROR REAL` → **PARAR**, copiar
  el traceback que está arriba de esa línea y no hacer el push (ese traceback es el dato que
  antes se tragaba el log).

## 3. Conseguir el token

Render → servicio **web-comparativas** → pestaña **Environment** → fila
`DIMENSIONAMIENTO_IMPORT_TOKEN` → botón del ojito/copiar. Luego, en PowerShell (pegalo entre
las comillas):

```powershell
$TOKEN = "PEGAR_EL_TOKEN_ACA"
```

(Es el mismo token que ya está en el `.env` local como `DIMENSIONAMIENTO_IMPORT_TOKEN`; si
está ahí, los scripts lo levantan solos y podés omitir `--token`.)

## 4. Estado de identidad ANTES del push

```powershell
curl.exe -s "https://web-comparativas.onrender.com/api/mercado-privado/dimensiones/admin/estado-identidad" -H "X-Import-Token: $TOKEN"
```

**Esperado antes del push** (el esquema quedó sano, pero la identidad aún no viajó):

- `"run_activo": 67`
- `"registry_poblado": false` y `"modo_card": "fallback"`
- `"esquema_identidad": {...}` con **todo en `true`** (las 3 columnas y los 2 índices del
  summary). Si algo da `false`, el paso 2 no quedó bien — volver ahí.
- `"summary_filas_identidad_null"` ≈ 303944 y `"records_filas_identidad_null"` ≈ 364887.

## 5. Push de la identidad (se corre en TU PC — el server solo aplica)

Primero el dry-run (resuelve local, **no envía nada**):

```powershell
cd "C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok"; .\venv_webcomparativas\Scripts\python.exe -m scripts.push_identity --url https://web-comparativas.onrender.com --dry-run
```

Salida esperada del dry-run (verificada el 23/07 contra el run local 7):

```
🔎 Resolviendo identidad local (run 7, exclude_test=False)...
   → 256 entidades (158 clientes · 98 no clientes), visible_map=374, cuit_map=186, ori_map=187, ambiguas=0, filas=364887
🟡 --dry-run: no se envió nada.
```

(La advertencia `posible encoding roto ... 'CLINICA GENERAL OBST Y CIR NUESTRA SE#OR'` es un
`#` que viene así en el dataset — informativa, no bloquea.)

Si el dry-run dio eso, el push real:

```powershell
cd "C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok"; .\venv_webcomparativas\Scripts\python.exe -m scripts.push_identity --url https://web-comparativas.onrender.com --token $TOKEN --remote-run 67
```

**Qué esperar:** FASE 1 = 1 request de registry (256 entidades) + 38 lotes de summary (374
`cliente_visible` en lotes de 10), cada lote imprime `lote i/38: +N filas (quedan NULL: M)` con
M bajando hasta 0, y un `[finalize]` que refresca el snapshot. **Duración: ~1-3 min** si la
hipótesis del lock es correcta (cada lote son milisegundos-segundos de trabajo real). Cierra con:

```
✅ FASE 1 COMPLETA (run 67). summary_null=0  records_null=364887
✅   summary cubierto al 100%. La card debería mostrar el número resuelto.
ℹ️   records sigue sin poblar (364887 NULL) — es FASE 2, no hace falta para el número.
```

- **Si un lote corta** (timeout/red): no se pierde nada — cada lote commitea solo. Volvé a
  correr el MISMO comando; los lotes ya aplicados se saltean (tocan 0 filas).
- **Si algún lote tarda >60 s** (no debería, post-restart): ver "Plan B" abajo.

## 6. Estado de identidad DESPUÉS del push

Repetir el comando del paso 4. Esperado:

- `"registry_poblado": true` y `"modo_card": "identidad"`
- `"entidades": 256`, `"entidades_si": 158`, `"entidades_no": 98`
- `"summary_filas_identidad_null": 0`
- `"ultima_resolucion"` con timestamp de recién
- `"records_filas_identidad_null"` sigue ≈ 364887: **es normal** — records es FASE 2, la card
  no lo necesita. (Opcional, otro día, fuera de horario: mismo comando del paso 5 con
  `--records` al final; son ~47 lotes más.)

## 7. Verificación final en la PÁGINA (navegador, hard refresh Ctrl+Shift+R)

- https://web-comparativas.onrender.com → Mercado Privado → Dimensionamiento.
- Card **Entidades = 256**, desglose **158 clientes · 98 no clientes**, **sin** pill amarillo.
- Renglones 364.887, Familias 2.451, Provincias 15 sin cambios (la identidad no toca volumen
  ni plata).
- Dropdown "Cliente": 256 opciones, sin duplicados por mayúsculas/acentos/plataforma.

---

## Plan B — troceado más fino (ya implementado, solo si hace falta)

El push YA es troceado desde el cliente (`apply-identity-chunk`: el servidor solo aplica cada
lote y commitea por lote; cero cómputo server-side, cero background). Si aun después del
restart algún lote pesara, achicar el lote y/o subir el timeout — mismo comando con:

```powershell
cd "C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok"; .\venv_webcomparativas\Scripts\python.exe -m scripts.push_identity --url https://web-comparativas.onrender.com --token $TOKEN --remote-run 67 --summary-batch 4 --timeout 180
```

Se puede relanzar las veces que haga falta: siempre reanuda donde quedó.

## Rollback (si el paso 6 o 7 no dan)

- El fallback sigue activo por diseño: prod vuelve a mostrar **374, nunca 0**. No hay apuro.
- Las columnas de identidad son **aditivas** (nullable); el código viejo las ignora. No se
  tocó ningún dato del run 67 más allá de aplicar identidad.
- Código: Render → **Deploys** → deploy previo → **Rollback**; o
  `git revert <SHA> ; git push origin main` (auto-deploy). Igual que en
  `docs/RUNBOOK_deploy_dim_identidad.md` (ese runbook quedó desactualizado en lo demás: el
  endpoint `resolve-entities` que menciona hoy devuelve 410 y el backfill de arranque ya no
  existe — el server no calcula identidad).
- Traer siempre: log de arranque + respuesta del endpoint de estado.

## Pendiente anotado (no bloquea esto)

- ⚠️ El log de arranque de prod muestra `[SECURITY] APP_SECRET no definida — usando clave
  temporal de desarrollo`. Cargar `APP_SECRET` (aleatoria, ≥32 caracteres) en Render →
  Environment en otro momento. Ojo: al setearla se invalidan las sesiones activas (los
  usuarios vuelven a loguearse).
