# RUNBOOK — Producción: dataset de Dimensionamiento de nov-2026

**Corrida:** `dataset_unificado_valorizado_2025_2026.csv` (fecha_procesamiento 11/11/2026 10:37, recibido 11/11/2026; reemplaza al del 06/11) · **Local validado: run 14** · **Prod vigente: confirmar en el Paso 0** (el último registrado es el 70) · **Ejecuta:** Andrés

Todos los comandos: **una sola línea, PowerShell, parado en la raíz** (`...\web_comparativas_v2- ok`), con `.\.venv\Scripts\python.exe`. Usar siempre `curl.exe` (nunca `curl`).

| Marcador | Qué es |
|---|---|
| `<URL>` | URL base de prod, sin barra final |
| `<TOKEN>` | `DIMENSIONAMIENTO_IMPORT_TOKEN` |
| `<DATABASE_URL_EXTERNAL>` | URL externa del Postgres de Render |
| `<ACTIVA>` / `<ANTERIOR>` | Corrida activa de prod y la previa. Salen del `estado` del Paso 1 (seguramente 70 y 69) |
| `<N>` | Corrida remota nueva. La imprime el Paso 2 (seguramente 71) |

> **ANTES DE EMPEZAR — confirmar con el proveedor.** Ninguno de estos puntos rompe la carga, pero conviene tenerlos resueltos antes de publicar:
> 1. **Octubre está incompleto.** Trae 22 meses (ene-25 → oct-26), pero octubre llega solo hasta el **25/10** (14.853 renglones, contra ~26.000 de un mes completo). En el gráfico, oct-26 se va a ver como una caída.
> 2. **La valorización sube 8,5 %** ($178.663 M → **$193.861 M**), toda por sep-26 y oct-26. De ene-25 a ago-26 no cambió nada: mismos 515.303 ids, mismas filas, misma valorización y mismas cantidades mes a mes.
> 3. **Hay filas de prueba, y dos aparecen como entidades en la card.** `Test Consolidado Fantasma` (2 filas) y, muy probablemente, `HOSPITAL MARCELA` (11 filas) suman 2 "no clientes". `hospital prueba 1` sigue creciendo: 1.500 filas y $457,7 M dentro de un cliente real (ver "Trampas del archivo").

---

## Qué cambia vs el run anterior (local 13)

| Métrica | run 13 (06/11) | run 14 (11/11) | Δ |
|---|---|---|---|
| Renglones | 515.303 | **555.763** | +40.460 (+7,9 %), todos de sep-26 y oct-26 |
| Meses | 20 (ene-25 → ago-26) | **22 (ene-25 → oct-26)** | +sep-26, +oct-26 (oct hasta el 25/10) |
| Entidades | 260 (162 Sí · 98 No) | **263 (164 Sí · 99 No)** | +2 Sí · +1 No (ver abajo) |
| Renglones Clientes / No clientes (filtro ¿Cliente?) | 428.566 / 86.737 | **464.156 / 91.607** | +35.590 / +4.870 |
| Familias | 2.834 | **2.845** | +11 (no sale ninguna) |
| Provincias | 18 | **18** | igual (las dos cifras cuentan "Sin Dato") |
| CUITs distintos (reales) | 192 | **194** | +2 (más el literal `SIN DATO` en 80.595 filas) |
| Valorización | $178.663.335.317 | **$193.860.529.247** | +$15.197 M (+8,5 %) |
| Filas con `origen_precio = sin_precio` | 241.250 | **264.074** | +22.824 (46,8 % → 47,5 %) |
| Filas con valorización 0 | 241.649 | **264.473** | +22.824 |
| Filas de summary | 426.461 | **460.506** | +34.045 |
| `match_negocio_map` | 10.875 | **10.884** | +9 |
| `match_demanda_desc` | 37.065 | **37.271** | +206 |
| Ancla `cuenta_entidad_map` | 165 cuentas | **167 cuentas** | +2 (= cuentas distintas en records) |
| Oportunidades (local) | 118 | **124** | +6 |

**Entidades.** 0 ambiguas y 0 filas rechazadas. Movimientos respecto del run 13:
- **Entran como Sí:** `CLINICA VIEDMA S A` (CUIT 30518407143, cuenta 247665) e `INSTITUTO PROVINCIAL DE SALUD DE SALTA` (CUIT 30707513094, 1 fila sin valorización).
- **Viedma pasa de No a Sí.** En el run 13 era `Clinica Viedma S.A.` (No, 982 filas, sin CUIT). Las 53 filas de oct-26 traen CUIT y cuenta, y la identidad unió ahí las 1.023 filas sin CUIT (982 hasta ago-26 más 41 de sep-26): queda una sola entidad Sí con 1.076 filas. Por eso cambia la partición Sí/No también en meses viejos.
- **Entran como No, y son de prueba:** `Test Consolidado Fantasma` (2 filas) y `HOSPITAL MARCELA` (11 filas). Ver "Trampas del archivo".
- `EN CASA - INTERNACION DOMICILIARIA` sigue siendo una sola entidad, con el CUIT sin el 0 inicial.

**Serie mensual.** El gráfico "Evolución mensual por negocio" apila solo las 5 unidades de negocio principales. Por eso suma 210 renglones menos que el total: Servicios Hospitalarios 148, Dispositivos Médicos 51, Cirugía Plástica 11. Con el run 13 pasaba igual (195). No es un error de datos. De ene-25 a ago-26, las barras son idénticas a las del run 13.

| Mes | Renglones (total) | Barra del gráfico | Valorización ($ M) | | Mes | Renglones (total) | Barra del gráfico | Valorización ($ M) |
|---|---|---|---|---|---|---|---|---|
| ene-25 | 25.210 | 25.201 | 10.743 | | dic-25 | 24.344 | 24.330 | 8.190 |
| feb-25 | 23.061 | 23.047 | 9.148 | | ene-26 | 24.822 | 24.811 | 7.396 |
| mar-25 | 26.605 | 26.598 | 8.705 | | feb-26 | 22.965 | 22.958 | 8.400 |
| abr-25 | 26.615 | 26.605 | 8.571 | | mar-26 | 26.831 | 26.815 | 7.981 |
| may-25 | 26.158 | 26.151 | 8.600 | | abr-26 | 25.481 | 25.474 | 10.078 |
| jun-25 | 27.539 | 27.528 | 9.252 | | may-26 | 25.346 | 25.336 | 8.792 |
| jul-25 | 27.539 | 27.529 | 10.473 | | jun-26 | 26.245 | 26.239 | 9.655 |
| ago-25 | 26.047 | 26.038 | 10.712 | | jul-26 | 26.234 | 26.223 | 8.419 |
| sep-25 | 26.136 | 26.124 | 9.669 | | ago-26 | 26.890 | 26.881 | 8.562 |
| oct-25 | 27.393 | 27.383 | 7.833 | | **sep-26** | **25.607** | **25.599** | **9.327** |
| nov-25 | 23.842 | 23.837 | 7.482 | | **oct-26** (hasta 25/10) | **14.853** | **14.846** | **5.870** |

## Trampas del archivo

- **Lo re-guardó Excel, como al del 06/11.** Fechas `D/M/YYYY` sin ceros (no ISO), decimales con coma y `fecha_procesamiento` sin segundos. El parser lee bien las 601 fechas distintas (validadas contra `%d/%m/%Y`). Esta vez **no hay copia ISO**: la de `Downloads\dataset_unificado_valorizado_2025_2026.csv` es el mismo archivo (mismo md5). De ene-25 a ago-26, los mismos 515.303 ids coinciden con la copia ISO del run 13 (`... 6.csv`), con las mismas filas y la misma valorización por mes. Prod recibe los valores ya parseados en local, así que no vuelve a interpretar fechas.
- **Daño de Excel: el mismo que el 06/11.** CUIT `03071016887` → `3071016887` en 2.496 filas (`EN CASA`, No; ninguna en sep/oct). 1 descripción `000` → `0`.
- **Filas de prueba que NO son entidades** (suman en los totales, pero no en la card):
  - `hospital prueba 1`: **1.500 filas y $457,7 M** dentro de `SOCIEDAD ESPAÑOLA DE BENEFICENCIA` (1.500 de las 1.518 filas de esa entidad), ahora hasta sep-26 (en el run 13: 1.437 filas y $431,9 M).
  - `hospital consolidador prueba 1`: **35 filas y $244 mil** dentro de `Grupo Nephron Hemodialisis` (en el run 13: 16 filas).
- **NUEVO — filas de prueba que SÍ son entidades** (suman 2 "no clientes" en la card; valorización $0):
  - `Test Consolidado Fantasma`: 2 filas MEDOX del 07/10/2026, paracetamol 500 mg, sin CUIT ni cuenta.
  - `HOSPITAL MARCELA`: 11 filas MEDOX del 9 al 14/10/2026, paracetamol y alcohol en gel, sin CUIT ni cuenta. **Hay que confirmarlo con el proveedor**, pero tiene el mismo patrón que la anterior.
- **NUEVO — familia con espacio no separable.** `APOSITO ALGODON/GASA 10X20 SIMPLE SOBRE` trae un NBSP en el nombre y convive con `... SOBR EST (DISC)`. Puede verse como un producto partido en Top familias. Hay 3.777 filas con NBSP en algún campo; en el run 13 eran 3.560, así que no es nuevo como fenómeno.
- **Familias con sufijo de laboratorio** (sin cambios). Algunos productos quedan partidos, por ejemplo `ALBUMINA HUMANA 20% X 50 ML FA` y `... FA - CSL BEHRING`.
- **Posible cliente duplicado** (sin cambios). `OBRA SOCIAL DE LA UNIV NAC SALTA` (Sí, 31 filas) y `Obra Social de la Universidad Nacional de Salta` (No, 413 filas, sin CUIT) no se unen porque los nombres no coinciden.
- **Los `id_registro_unico` no cambiaron**, a diferencia del 06/11: los 515.303 de ene-25 a ago-26 son exactamente los mismos, y solo entran los 40.460 de sep/oct.

**Higiene OK:** UTF-8 con BOM, separador `;`, 47 columnas (las mismas que el 06/11), 0 ids duplicados, `familia = 'SIN DATO'` en 0 filas, 0 fechas sin parsear. Los textos con `Ã` (`TAMAÃO`, `sÃ³dica`) vienen así del origen y ya estaban en el run 13.

---

## Paso 0 — Control previo (solo lectura)

No hay commits pendientes que sean requisito: `main` local = `origin/main` = `93bb33d6`, que ya incluye el fix `513e06b5`. En Render → Deploys, confirmá que **`93bb33d6`** esté "Live".

**Entre el Paso 2 y el Paso 7 no hagas push ni deploy de nada** (tampoco los cambios del $). Cada arranque de Render recalcula el ancla con la identidad que haya en ese momento.

```powershell
curl.exe -s "<URL>/api/mercado-privado/dimensiones/admin/estado-identidad" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"run_activo"` = la corrida vigente y `"modo_card": "identidad"`.

## Paso 1 — Liberar disco en prod (borrar corridas viejas)

Usa `scripts\prune_dimensionamiento_runs.py`. Borra de `dimensionamiento_records` y del summary todo lo anterior a la activa y a la previa. No toca la tabla de corridas ni ninguna otra tabla. Es reanudable y, sin `--apply`, solo lee. El 29/09 el disco de Render se llenó justamente en la identidad.

**1.1 Estado (solo lectura).** Anotá `<ANTERIOR>` y `<ACTIVA>`, y cuánto libera:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py estado; Remove-Item Env:DATABASE_URL
```

Esperado: `Corrida activa: <ACTIVA> · Conservar: [<ANTERIOR>, <ACTIVA>]`, una tabla por run con `CONSERVAR` / `borrar`, y la línea `[ESTIMACIÓN]`.

- Si aparece `[WARN] Hay corridas 'running' posteriores a la activa`: **frená**, hay un upload a medias.
- Si no hay nada marcado `borrar`: la poda ya se hizo. Saltá al Paso 2.

**1.2 Borrar.** Necesita **≥ 2 GB libres** de margen (mirá el uso de disco en Render). Si hay menos, ampliá el disco antes:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py borrar --apply --keep <ANTERIOR>,<ACTIVA>; Remove-Item Env:DATABASE_URL
```

- Salida final: `✅ LISTO: N filas borradas`. Velocidad esperada: ~5-10 mil filas/s.
- Si se corta, repetí el mismo comando y sigue desde lo que falta. Si `--keep` no coincide con la base, se niega y no borra nada.
- Si el disco se acerca al límite mientras corre: Ctrl+C y repetí con `--pausa 1` al final.

**1.3 VACUUM (obligatorio después de borrar).** Sin este paso, lo borrado no se reutiliza. No bloquea la app:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py vacuum --apply; Remove-Item Env:DATABASE_URL
```

Esperado: `muertas≈0` en las dos tablas. El tamaño que muestra Render **casi no baja, y es normal**: el espacio queda reutilizable dentro de las tablas para el Paso 2.

**Espacio del Paso 2:** el run nuevo tiene 555.763 records y 460.506 filas de summary. Es ≈8 % más grande que el run 13 y ≈22 % más grande que el run 12 (el 70 de prod). Si lo que libera `[ESTIMACIÓN]` no alcanza para cubrirlo, ampliá el disco antes de seguir. No uses `upload ... cleanup` (es otro camino) ni `vacuum-full` (bloquea tablas).

## Paso 2 — Subir el run 14 por chunks

```powershell
.\.venv\Scripts\python.exe scripts\upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> upload --run-id 14
```

- Sube **555.763 records en 28 lotes** y **460.506 filas de summary en 24 lotes**. No hay `scratch\upload_state.json` previo, así que arranca limpio. **Anotá el `<N>` que imprime.** (El `--dry-run` local ya pasó el 11/11: `Datos consistentes`, fechas 2025-01-01 → 2026-10-25.)
- Salida esperada: `✅ Corrida remota creada con ID: <N>` → lotes → `Validación esperada: records=555,763, summary_rows=460,506, valorización=193,860,529,247.28` → `🎉 Corrida remota activada: ID <N>` → `IDENTIDAD DE CLIENTES: PENDIENTE (esperado en este punto).`
- Duración: 30-45 min. **Si se corta:** repetí el MISMO comando (retoma; no uses `--fresh`). **Si da 422 "summary no reconcilia":** agregá `--resubir-summary` al mismo comando.

**Verificación:** el curl del Paso 0 → `"run_activo": <N>`. Que `modo_card` diga `"fallback"` y que los `*_identidad_null` estén altos **es normal** hasta el Paso 3.

## Paso 3 — Identidad del run nuevo

Correlo enseguida después del Paso 2, sin reiniciar Render entre medio.

```powershell
.\.venv\Scripts\python.exe -m scripts.push_identity --url <URL> --token <TOKEN> --records --remote-run <N> --timeout 300
```

- Resuelve en local (run 14): `→ 263 entidades (164 clientes · 99 no clientes), visible_map=271, cuit_map=194, ori_map=73, ambiguas=0, filas=555763`.
- Salida final esperada: `✅ FASE 1 COMPLETA (run <N>). summary_null=0  records_null=0`. La advertencia `posible encoding roto ... NUESTRA SE#OR` viene así del dataset y es informativa.
- Duración: 5-15 min. Si se corta, repetí el mismo comando (solo toca filas NULL).

**Verificación** (curl del Paso 0): `"modo_card": "identidad"`, `"entidades": 263`, `"entidades_si": 164`, `"entidades_no": 99`, `"summary_filas_identidad_null": 0`, `"records_filas_identidad_null": 0`.

## Paso 4 — Recalcular las anclas del run nuevo (SIEMPRE, con `--apply --force`)

**Se corre siempre después del Paso 3, dé lo que dé.** En el run 13 local, el ancla guardada quedó en 162 cuentas cuando desde records eran 165, y el refresh no la corrigió. En el run 70 de prod había quedado en 23 de 163. Solo `--force` la recalcula desde records.

Referencia local (solo lectura, base local):

```powershell
.\.venv\Scripts\python.exe backfill_dimensionamiento_dashboard_anchors.py --run-id 14
```

Esperado: `cuenta_entidad_map ACTUAL en la base: 'poblado (167 cuentas)'`. Son las 167 cuentas distintas que hay en records.

En prod. La línea borra las variables al final para que ningún script posterior apunte a prod por error:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; $env:APP_ENV="production"; .\.venv\Scripts\python.exe backfill_dimensionamiento_dashboard_anchors.py --run-id <N> --apply --force; Remove-Item Env:DATABASE_URL, Env:APP_ENV
```

- Duración: 1-2 min.
- Esperado: `[OK] cuenta_entidad_map guardado. 167 cuentas.` y `global_totals` con `total_rows: 555763`, `familias: 2845`, `provincias: 18`, `valorizacion: 193860529247.28`.
- Si da menos de 167, la identidad del Paso 3 no terminó: repetí el Paso 3 y después este paso.

## Paso 5 — Precálculos de Match

```powershell
.\.venv\Scripts\python.exe -m scripts.push_match_data --url <URL> --token <TOKEN> --solo-precalc --timeout 300
```

- Local: `negocio_map=10884, demanda_desc=37271`. No toca propuestas, homologaciones, monodroga ni la corrida vigente. Duración: 2-5 min.

**Verificación:**

```powershell
curl.exe -s "<URL>/api/mercado-privado/match/admin/estado" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"match_negocio_map": 10884`, `"match_demanda_desc": 37271`, y `run_vigente_filas` igual que antes de este paso.

## Paso 6 — VACUUM después de la identidad (SIEMPRE)

La identidad (Paso 3) reescribe todas las filas del run nuevo. En prod (run 71) quedaron ≈220 mil filas muertas en el summary y ≈275 mil en records. Sin este VACUUM, la vista con cartera ("Ver como usuario") daba **503**: `_fetch_summary_rows_for_bootstrap` se cortaba por statement timeout.

Es el mismo comando del Paso 1.3. El subcomando ya hace `VACUUM (ANALYZE)` de las dos tablas, así que también refresca las estadísticas del planner. No bloquea la app:

```powershell
$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; .\.venv\Scripts\python.exe scripts\prune_dimensionamiento_runs.py vacuum --apply; Remove-Item Env:DATABASE_URL
```

Esperado: `muertas≈0` en `dimensionamiento_family_monthly_summary` y en `dimensionamiento_records`. Si alguna sigue alta, repetí el comando antes del Restart.

## Paso 7 — Restart en Render

Render → servicio → **Manual Deploy → Restart service**. Limpia los cachés en memoria de todos los workers y hace que suelten el ancla vieja. Esperá "Live" (~35-55 s de 502). En el log de arranque no tiene que aparecer `Traceback` ni `ERROR REAL`.

## Paso 8 — Verificación en la página

1. `<URL>` → Mercado Privado → Dimensionamiento (Ctrl+Shift+R), como admin. Estos valores ya se verificaron en la página local:
   - Card: **263 ENTIDADES · 164 clientes · 99 no clientes**
   - KPIs: **555.763** renglones · **2.845** familias · **18** provincias
   - Valorización: **193,9MM** (`$ 193,9MM` solo si ya se deployaron los cambios del `$`; la página local se verificó con esos cambios en el working tree)
   - Rango **ene 25 — oct 26**, serie de **22 meses**, sin meses posteriores a oct-26. Oct-26 se ve bajo porque llega hasta el 25/10. Las barras coinciden con la columna "Barra del gráfico" de la tabla de arriba
   - Top familias por renglones: KETOROLAC 30 MG AMP (3.456), DEXAMETASONA 8 MG X 2 ML AMP (3.442), AMPICILINA + SULBACTAM (1/0.5) G X 10 ML AMP (3.368)
   - Filtro ¿Cliente? **Sí:** 164 entidades · 464.156 renglones. **No:** 99 entidades · 91.607 renglones
2. **Usuario restringido:** "Ver como usuario" → **Yanina Sassone** y **Daniela Armilio**. Tienen que ver datos, no 0. Referencia local (`dimensionamiento@suizo.com`, porque ellas no existen en local): 33 entidades, 120.216 renglones.
3. **Oportunidades:** tiene que cargar con datos (en local: 124 oportunidades del run 14). Se reconstruye sola en el arranque del Paso 7.
4. **Match**, desde los dos mercados: mismas propuestas que antes y el desplegable de Negocio con 8 negocios.

**Después de unos días sin problemas (opcional):** repetí los Pasos 1.2 y 1.3 con `--keep <ACTIVA>,<N>`. Eso borra `<ANTERIOR>`, que deja de hacer falta para un rollback, y libera espacio para la próxima carga.

---

## Rollback

| Problema | Acción |
|---|---|
| Upload cortado a mitad (Paso 2) | Repetir el mismo comando (reanuda). Nada visible cambió |
| El run nuevo quedó mal después del finalize | `.\.venv\Scripts\python.exe scripts\upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> rollback --remote-run-id <N>` → `<N>` pasa a `failed` y **vuelve `<ACTIVA>`**. Después, Restart en Render |
| Al volver a `<ACTIVA>`, los usuarios con cartera ven 0 | `$env:DATABASE_URL="<DATABASE_URL_EXTERNAL>"; $env:APP_ENV="production"; .\.venv\Scripts\python.exe backfill_dimensionamiento_dashboard_anchors.py --run-id <ACTIVA> --apply --force; Remove-Item Env:DATABASE_URL, Env:APP_ENV` y Restart |
| La identidad no llega a 0 NULL (Paso 3) | Repetir el Paso 3; si sigue, mandar la salida |
| Disco lleno durante el Paso 2 o 3 | Ampliar el disco en Render y repetir el mismo comando (ambos retoman) |
| "Ver como usuario" da 503 (statement timeout en el bootstrap) | Falta el VACUUM del Paso 6: correrlo y probar de nuevo (no hace falta Restart) |
| Match raro después del Paso 5 | Son tablas derivadas y las propuestas no se tocan. Restaurar el backup local y repetir el Paso 5 desde esa base |
| Local roto | Backup previo a esta carga: `C:\Users\andres.torres\OneDrive - Suizo Argentina S.A\Escritorio\backups_siem\app.db.bak-20261111_112845` (contiene el run 13) |

---

## Nota local — qué pasó en la carga del run 14

- **El server del 8010 tiene que estar cerrado durante toda la carga.** Los dos primeros intentos murieron con `database is locked` porque el `.bat` se abrió mientras corría el import: al arrancar, el server escribe en la base. Las dos veces se restauró el backup antes de reintentar. Las bases rotas quedaron apartadas en `backups_siem\fallido_20261111_1136` y `fallido_20261111_1140`, y se pueden borrar.
- El tercer intento (`--csv-path` explícito) insertó los 555.763 records sin rechazos y terminó con exit 0. La auto-reparación reconstruyó el summary (460.506). **El run quedó en `status='running'` con métricas en 0, como el run 12:** se cerró a mano (success, `rows_*` del log, `observed_columns`, `summary`, `platform_values`).
- Reparación en pasos aislados: `persist_records_and_registry` (263 entidades, 0 NULL) → `_rebuild_summary_table` → `ensure_entidad_columns_populated` → `refresh_default_dashboard_snapshot` → backfill local `--apply --force` (167 cuentas) → `rebuild_match_demanda_desc` → `rebuild_match_negocio_map`.
- **Ojo:** las dos primeras llamadas a `refresh_default_dashboard_snapshot` terminaron sin error, pero no persistieron nada: el snapshot siguió siendo el del run 13 y el ancla quedó en NULL. La tercera sí persistió. En local, verificá siempre después de ese paso que `dimensionamiento_dashboard_snapshots.import_run_id` sea el run nuevo.
- Verificado después de levantar el server, en `http://127.0.0.1:8010`.
