# RUNBOOK — Producción: dataset de Dimensionamiento del 29/09/2026

**Corrida:** `dataset_unificado_valorizado_2025_2026.csv` (fecha_procesamiento 2026-09-29) · **Local validado: run 12** · **Prod vigente: run 69** · **Ejecuta:** Andrés

Todos los comandos: **una sola línea, PowerShell, parado en la raíz** (`...\web_comparativas_v2- ok`), con `.\.venv\Scripts\python.exe`. Usar siempre `curl.exe` (nunca `curl`). `<URL>` = URL base de prod sin barra final · `<TOKEN>` = `DIMENSIONAMIENTO_IMPORT_TOKEN`.

> **ANTES DE EMPEZAR — confirmar con el proveedor.** La valorización total **baja 37,7 %** ($226.900 M → **$141.337 M**) aunque hay 11 % más renglones. El 46 % de las filas (211.690) viene con `origen_precio = sin_precio` y valor 0, con una proporción que crece del 18 % (ene-25) al ~60 % (2026). Además 271.316 de los 408.011 renglones que ya estaban cambiaron de valor. Parece un cambio de metodología de precios, no un error de formato. Si el proveedor no lo confirma, **no correr este runbook**.

---

## Qué cambia vs el run 69 (prod 69 = mismo dataset que el run 11 local)

| Métrica | prod 69 (ago) | run 12 (29/09) | Δ |
|---|---|---|---|
| Renglones | 409.728 | **456.539** | +46.811 (408.011 en común, +48.528 nuevos, −1.717 que salen) |
| Meses | 20 (ene-25 → ago-26) | **20 (ene-25 → ago-26)** | igual: **no trae septiembre**. Todos los meses se reexpresan con ~+10 % de filas |
| Entidades | 260 (160 Sí · 100 No) | **258 (161 Sí · 97 No)** | ver detalle abajo |
| Renglones Clientes / No clientes | 363.152 / 46.576 | **387.046 / 69.493** | — |
| Familias | 2.547 | **2.545** | −2 |
| Provincias | 16 | **18** | +La Pampa, +San Juan (las dos cifras cuentan "Sin Dato") |
| CUITs distintos (reales) | 190 | **191** | +1 (más el literal `SIN DATO` en 61.486 filas) |
| Valorización | $226.899.760.147 | **$141.337.440.631** | **−$85.562 M (−37,7 %)** |
| Filas de summary | 341.384 | **381.668** | — |
| `match_negocio_map` | 10.792 | **10.792** | = |
| `match_demanda_desc` | ~30.397 | **32.225** | +1.828 |
| Ancla `cuenta_entidad_map` | **vacía** (por eso los usuarios con cartera ven 0) | **163 cuentas** | — |

**Entidades.** Salen: `ROI S A` (30661931066, Sí) y las 3 entidades de prueba que traía el 69: `OOSS prueba 2`, `Hospital General Bionexo - Argentina` y `Hospital Grupo 1`. Pasan de No a Sí, ahora con CUIT: `IPSST` (30633949227) e `Instituto Modelo Privado de Ginecología` (30586087750). Nuevas No: `Invitro (Fertilidad Buenos Aires SA)` (30715189948) y `Biovincularse` (sin CUIT). 0 reclasificaciones Sí↔No dentro de una misma entidad, 0 ambiguas y 0 filas rechazadas.

**Higiene del CSV (OK):** UTF-8 con BOM, fechas 100 % ISO `YYYY-MM-DD` (días del 01 al 31, sin inversión día/mes, máximo 2026-08-31, sin meses fantasma), 0 ids duplicados, 0 filas de prueba, `familia = 'SIN DATO'` en 0 filas.

---

## Paso 0 — Push y deploy (OBLIGATORIO, antes de subir nada)

Hay 1 commit sin pushear: `513e06b5 fix(dimensionamiento): recalcular el ancla cuenta_entidad_map cuando quedó vacía`. **Tiene que estar Live antes del Paso 2.** Sin este fix, el ancla puede persistirse vacía (el arranque de Render refresca el snapshot antes de que llegue la identidad), el refresh posterior la relee como válida y los usuarios con cartera quedan en 0, como hoy en el 69.

```powershell
git push origin main
```

Render → Deploys: esperar a que **`513e06b5`** quede "Live" (hay ~35-55 s de 502 durante el cambio; es normal). En el log de arranque no tiene que aparecer `Traceback` ni `ERROR REAL`.

**Verificación:**

```powershell
curl.exe -s "<URL>/api/mercado-privado/dimensiones/admin/estado-identidad" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"run_activo": 69` (todavía no cambió nada).

## Paso 1 — Subir el run 12 por chunks

```powershell
.\.venv\Scripts\python.exe scripts\upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> upload
```

- Sube el run local 12: **456.539 records en 23 lotes** y **381.668 filas de summary en 20 lotes**. No hay `scratch\upload_state.json` previo, así que arranca limpio y crea una corrida remota nueva. **Anotá el ID que imprime (`<N>`, seguramente 70).**
- Salida esperada: `✅ Corrida remota creada con ID: <N>` → lotes → `Validación esperada: records=456,539, summary_rows=381,668, valorización=141,337,440,631.35` → `🎉 Corrida remota activada: ID <N>` → `IDENTIDAD DE CLIENTES: PENDIENTE (esperado en este punto).`
- Duración: 25-40 min. **Si se corta:** repetí el MISMO comando (retoma; no uses `--fresh`). **Si da 422 "summary no reconcilia":** agregá `--resubir-summary` al mismo comando.
- El 69 sigue visible hasta el finalize, y el cambio es atómico.

**Verificación:** el curl del Paso 0 → `"run_activo": <N>`. Que `modo_card` diga `"fallback"` y que los `*_identidad_null` estén altos **es normal** hasta el Paso 2.

## Paso 2 — Identidad del run nuevo

Correlo enseguida después del Paso 1, sin reiniciar Render entre medio: cada arranque recalcula el ancla con la identidad que haya en ese momento. Si igual pasa (resize del disco, caída), el Paso 4 lo repara.

```powershell
.\.venv\Scripts\python.exe -m scripts.push_identity --url <URL> --token <TOKEN> --records --remote-run <N> --timeout 300
```

- Resuelve en local (run 12): `→ 258 entidades (161 clientes · 97 no clientes), visible_map=263, cuit_map=191, ori_map=70, ambiguas=0, filas=456539`.
- Salida final esperada: `✅ FASE 1 COMPLETA (run <N>). summary_null=0  records_null=0`. La advertencia `posible encoding roto ... NUESTRA SE#OR` viene así del dataset; es informativa.
- Duración: 5-12 min. Si se corta, repetí el mismo comando (solo toca filas NULL).

**Verificación** (curl del Paso 0): `"modo_card": "identidad"`, `"entidades": 258`, `"entidades_si": 161`, `"entidades_no": 97`, `"summary_filas_identidad_null": 0`, `"records_filas_identidad_null": 0`.

## Paso 3 — Precálculos de Match

```powershell
.\.venv\Scripts\python.exe -m scripts.push_match_data --url <URL> --token <TOKEN> --solo-precalc --timeout 300
```

- Local: `negocio_map=10792, demanda_desc=32225`. No toca propuestas, homologaciones, monodroga ni la corrida vigente. Duración: 2-5 min.

**Verificación:**

```powershell
curl.exe -s "<URL>/api/mercado-privado/match/admin/estado" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"match_negocio_map": 10792`, `"match_demanda_desc": 32225` y `run_vigente_filas` igual que antes (112.303, sin cambios).

## Paso 4 — Recalcular las anclas del run nuevo (SIEMPRE)

**Se corre siempre después del Paso 2, dé lo que dé.** En el run 70 el ancla quedó guardada con 23 cuentas en vez de 163. Cada arranque de Render regenera el snapshot y calcula el ancla con lo que haya en ese momento en records; si ese arranque cae con la identidad a medias, el ancla queda parcial. El fix `513e06b5` solo recalcula un ancla **vacía**, no una parcial. El backfill con `--force` la recalcula desde records sin leer la guardada.

Primero, la referencia local (solo lectura, base local):

```powershell
.\.venv\Scripts\python.exe backfill_dimensionamiento_dashboard_anchors.py --run-id <RUN_LOCAL>
```

Anotá `cuentas` de la línea `cuenta_entidad_map ACTUAL en la base: 'poblado (X cuentas)'` (29/09: 163).

Después, en prod. La línea borra las variables al final para que ningún script posterior apunte a prod por error:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; $env:APP_ENV="production"; .\.venv\Scripts\python.exe backfill_dimensionamiento_dashboard_anchors.py --run-id <N> --apply --force; Remove-Item Env:DATABASE_URL, Env:APP_ENV
```

- Duración: 1-2 min. Recalcula las 3 anclas (`platform_values`, `cuenta_entidad_map`, `global_totals`).
- Esperado: `[OK] cuenta_entidad_map guardado. X cuentas.`, con **el mismo X que la referencia local**. Si da menos, la identidad del Paso 2 no terminó: repetí el Paso 2 y después este paso.
- El Paso 5 (Restart) tiene que ir **después** de este paso, para que los workers suelten el ancla vieja de su caché en memoria.

## Paso 5 — Restart en Render

Render → servicio → **Manual Deploy → Restart service**. Limpia los cachés en memoria de todos los workers. Esperá "Live" (~35-55 s de 502).

## Paso 6 — Verificación en la página

1. `<URL>` → Mercado Privado → Dimensionamiento (Ctrl+Shift+R), como admin:
   - Card: **258 ENTIDADES · 161 clientes · 97 no clientes**
   - KPIs: **456.539** renglones · **2.545** familias · **18** provincias. En la vista Valorización: ≈ **$141.337 M**.
   - Rango de fechas **ene 25 — ago 26**, serie de **20 meses**, sin meses posteriores a ago-26.
   - Filtro ¿Cliente? Sí / No: **387.046 / 69.493** renglones.
2. **Usuario restringido:** "Ver como usuario" → **Yanina Sassone** (o que ella entre con su usuario). Tiene que ver datos, no 0. En local no se pudo replicar porque Yanina no existe en la base local; con el usuario restringido local (`dimensionamiento@suizo.com`) se ven 33 entidades y 104.215 renglones. Probá también Daniela Armilio.
3. Match desde los dos mercados: el tablero sigue con 112.303 propuestas y el desplegable de Negocio con 8 negocios.

---

## Mantenimiento de disco — poda de corridas viejas

`dimensionamiento_records` y el summary guardan las filas de **todas** las corridas. El 29/09 el Postgres se llenó (DiskFull) en el Paso 2 del run 70. `scripts\prune_dimensionamiento_runs.py` borra de esas dos tablas todo lo que no sea la corrida activa y la anterior. Detalles:

- Borra por lotes de 5.000 ids, con commit por lote.
- Es reanudable: si se corta, repetís el mismo comando y sigue desde lo que falta.
- Sin `--apply` es solo lectura.
- No toca la tabla de corridas ni ninguna otra tabla.
- No toca nada con id mayor al menor conservado (una corrida en staging queda a salvo).
- Se niega a borrar si hay un upload en curso o si `--keep` no coincide con activa + anterior.

**Cuándo:** una vez antes de la próxima carga (conservar 69 y 70). Después de cada carga verificada, conservar la nueva y la anterior (por ejemplo 70 y 71).

**1. Estado (solo lectura).** Muestra filas por corrida, tamaños y cuánto se libera:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py estado; Remove-Item Env:DATABASE_URL
```

Esperado: `Corrida activa: 70 · Conservar: [69, 70]`, una tabla por run con `CONSERVAR` / `borrar` y la línea `[ESTIMACIÓN]`. Si aparece `[WARN] Hay corridas 'running' posteriores a la activa`, no sigas: hay un upload a medias y hay que terminarlo o hacerle rollback.

**2. Borrar.** Necesita ≥ 2 GB libres de margen para WAL (mirá el uso de disco en Render):

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py borrar --apply --keep 69,70; Remove-Item Env:DATABASE_URL
```

- Salida: un bloque por grupo (`run 45: N filas (ids …)`), progreso cada 20 lotes y al final `✅ LISTO: N filas borradas`.
- Velocidad esperada: ~5-10 mil filas/s (el `estado` te dice cuántas son).
- Si se corta, reintenta solo hasta 6 veces. Si igual termina con error, repetí el mismo comando.
- Si el disco se acerca al límite mientras corre, cortá con Ctrl+C (el lote en curso se descarta entero) y repetí con `--pausa 1`.

**3. VACUUM (obligatorio después de borrar).** En Postgres, `DELETE` solo marca filas como muertas; el disco no se libera. El VACUUM común las limpia y deja ese espacio **reutilizable dentro de las tablas**, así el próximo upload lo ocupa sin crecer. No bloquea la app y casi no necesita espacio extra (~1 GB de margen alcanza):

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py vacuum --apply; Remove-Item Env:DATABASE_URL
```

Esperado: `muertas≈0` en las dos tablas. El tamaño que muestra Render **casi no baja**, y es normal. Probado en un Postgres local: después de borrar + VACUUM, insertar un run nuevo del tamaño de los borrados hizo crecer la tabla un 14 % en vez de un 50 %.

**4. VACUUM FULL (opcional, solo si necesitás devolver el espacio al disco).** Reescribe la tabla compacta. Tiene dos costos:

- Necesita libre ≈ **2 × lo que ocupa lo conservado** de esa tabla: la copia más el WAL. El `estado` y el propio comando lo calculan.
- Toma un **lock exclusivo**: Dimensionamiento y Oportunidades no cargan mientras dura. Hacelo fuera de horario y de a una tabla, primero el summary:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py vacuum-full --tabla dimensionamiento_family_monthly_summary; Remove-Item Env:DATABASE_URL
```

Sin flags solo muestra el espacio que necesita. Para correrlo, agregá `--apply --acepto-bloqueo` y repetí con `--tabla dimensionamiento_records`. Para el objetivo de no volver a llenar el disco, **alcanza con los pasos 2 y 3**.

---

## Rollback

| Problema | Acción |
|---|---|
| Push cortado a mitad (Paso 1) | Repetir el mismo comando (reanuda); nada visible cambió |
| El run nuevo quedó mal después del finalize | `.\.venv\Scripts\python.exe scripts\upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> rollback --remote-run-id <N>` → el <N> pasa a `failed` y **vuelve el 69** con su identidad. Después, Restart en Render. |
| Al volver al 69, los usuarios con cartera siguen en 0 (su ancla está vacía desde antes) | Con el fix ya deployado: `$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; $env:APP_ENV="production"; .\.venv\Scripts\python.exe backfill_dimensionamiento_dashboard_anchors.py --run-id 69 --apply --force; Remove-Item Env:DATABASE_URL, Env:APP_ENV` |
| La identidad no llega a 0 NULL (Paso 2) | Repetir el Paso 2; si sigue, mandar la salida |
| Match raro después del Paso 3 | Son tablas derivadas y las propuestas no se tocan. Para volver a los valores del 69: restaurar el backup local (abajo) y repetir el Paso 3 desde esa base |
| Local roto | Backup previo a esta carga: `C:\Users\andres.torres\OneDrive - Suizo Argentina S.A\Escritorio\backups_siem\app.db.bak-20260929_095957` (contiene el run 11 = prod 69) |

---

## Nota local — qué pasó en la carga del run 12

- El import (`--csv-path` explícito) insertó los 456.539 records sin rechazos, pero, igual que en agosto, bajo SQLite **no persistió el cierre de la corrida** (quedó en `running`), ni el summary, ni el registry. La auto-reparación chocó con la UNIQUE del summary. Se reparó en pasos aislados: cierre del run → `persist_records_and_registry` → `_rebuild_summary_table` → `ensure_entidad_columns_populated` → `refresh_default_dashboard_snapshot` (que calculó el ancla: 163 cuentas). Tardó ~70 min, contra ~10 de agosto.
- Durante el import alguien levantó el `.bat` (10:15). Lo reinicié para validar sin cachés viejos.
- El `.bat` de SIEM 1.0 levanta en **8010**. El 8020 lo usa SIEM 2.0.
