# RUNBOOK — Producción: dataset actualizado de Dimensionamiento (jul 2026)

**Fecha:** 24/07/2026 (v2, post-incidente del finalize) · **Ejecuta:** Andrés · Comandos de UNA línea para PowerShell (`curl.exe` explícito, nunca `curl` a secas).

> **INCIDENTE 24/07 (resuelto en código, pendiente de deploy):** el finalize del run 68 quedó bloqueado con 409 infinitos — un backend zombi de Postgres retuvo el lock tras un hipo de la instancia, y además el finalize viejo hacía >10 min de cómputo redundante en Render (resolvía identidad, reconstruía summary y regeneraba el snapshot que YA viajan desde tu PC). Ambas cosas están arregladas: el finalize ahora tarda segundos y reclama solo los locks huérfanos. **El run 68 quedó completo en staging (records 364.887 + summary 304.458): NO se re-sube nada, solo falta rematar el finalize tras el deploy.** El run 67 sigue activo e intacto.

## Paso 0 — Push a main y deploy (AHORA SÍ es requisito)

El fix del finalize tiene que estar desplegado antes de rematar el 68. Van TODOS los commits pendientes (los 4 del dataset + los 3 del incidente):

```powershell
git push origin main
```

**En el log de arranque de Render verificar:** `[MIGRATION] uq_dim_family_monthly_summary ya existe ... (OK, skip)`, sin `Traceback` ni `ERROR REAL`. (No hay migraciones nuevas en este deploy: el fix es de código.)

**Diagnóstico opcional del zombi** (antes y/o después del deploy, para verlo con tus ojos):

```powershell
curl.exe -s "<URL>/api/mercado-privado/dimensiones/admin/import/finalize-estado?run_id=68" -H "X-Import-Token: <TOKEN>"
```

Esperado ANTES de rematar: `"run_status": "running"`, `counts.records: 364887`, `counts.summary: 304458`, y `lock_holder` con el pid zombi (probablemente `"holder_huerfano": true`) — o `lock_holder: null` si el reinicio del deploy ya lo mató.

## Paso 1B — Rematar el finalize del run 68 (sin re-subir nada)

El MISMO comando de siempre; el estado local (`scratch/upload_state.json`, verificado apuntando al 68 con todo subido) hace que saltee los 35 chunks y vaya directo al finalize, que ahora tarda segundos. Si el zombi sigue vivo, el server lo detecta como huérfano, lo termina y reclama el lock solo:

```powershell
python scripts/upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> upload
```

- **Duración esperada:** ~1 minuto total (warmup + finalize de segundos).
- Salida esperada: "Registros ya subidos en su totalidad", ídem resúmenes, y el finalize OK con "IDENTIDAD DE CLIENTES: PENDIENTE (esperado)". Si en el log de Render aparece `marca HUÉRFANA detectada ... lock huérfano RECLAMADO`, es el fix haciendo su trabajo.
- **Si diera 422 "summary no reconcilia"** (no debería — los conteos del 68 coinciden exactos): `python scripts/upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> upload --resubir-summary` re-sube SOLO el summary (16 chunks, ~5 min) y remata.

**Verificación:** el mismo curl de arriba debe dar `"run_status": "success"`, y `estado-identidad` → `"run_activo": 68` (con `modo_card: "fallback"` hasta el Paso 2 — normal).

Después seguí con los **Pasos 2, 3 y 4 de abajo** usando `<RUN_NUEVO>` = **68**.

---

**Qué se sube:** el run 10 local, validado completo (ingestado del `dataset_unificado_valorizado_2025_2026.csv` actualizado con el parser de fechas corregido). Diferencias reales vs el run 67 vigente en prod: 2.564 filas re-clasificadas de familia ('SIN DATO' → producto real; familias 2.451 → **2.522**), 1 CUIT corregido con cero inicial (1.337 filas del mismo cliente) y 13 filas con centavos de valorización. Mismas 364.887 filas, mismos 18 meses (ene25–jun26), mismas 256 entidades (158 · 98), misma valorización total (~$204.478 M; difiere $0,33 por redondeo). Además se refrescan las 2 tablas precalculadas de Match, que estaban desactualizadas desde junio: `match_negocio_map` 3.238 → **3.446** y `match_demanda_desc` 28.598 → **29.641**.

**Deploy requerido (ver Paso 0):** los fixes del incidente del finalize deben estar desplegados antes de rematar el 68. El push incluye además los commits de higiene del dataset (parser de fechas, Capa C, `--solo-precalc`).

**Variables:** `<URL>` = URL base de prod (sin barra final) · `<TOKEN>` = `DIMENSIONAMIENTO_IMPORT_TOKEN` (Render → Environment, o tu `.env`).

**Red de seguridad:** el run 67 y su identidad quedan INTACTOS en prod durante todo el proceso. El run nuevo entra en staging (estado `running`, invisible) y recién pasa a vigente en el finalize del Paso 1. Rollback local adicional: `C:\Users\ANDRES.TORRES\Desktop\backups_siem\app.db.bak-20260723`.

---

## Paso 1 — Push del run nuevo por chunks (records + summary + finalize)

> **Para el incidente actual NO corresponde este paso** (el 68 ya está subido — usar el Paso 1B de arriba). Queda documentado para futuros reemplazos de dataset, ya con el finalize rápido.

Desde tu PC, venv activado, raíz del proyecto:

```powershell
python scripts/upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> upload
```

- Sube la última corrida local success (la 10): 364.887 records en 19 lotes de 20.000 + 304.458 filas de summary en 16 lotes, reanudable (estado en `scratch/upload_state.json`) y con timeout de 600s por request ya incorporado.
- **Duración estimada:** 15–30 minutos + unos minutos del finalize (reconstruye summary y snapshot server-side con timeout ilimitado en esa sesión — es el único cómputo que hace Render y es el flujo establecido).
- **Si corta:** reejecutá el MISMO comando — retoma del estado guardado (NO uses `--fresh` salvo querer arrancar de cero).
- Los usuarios siguen viendo el run 67 hasta el finalize; el cambio es atómico.

**Verificación:** anotá el número de corrida remota que imprime el script (`<RUN_NUEVO>`, seguramente 68) y:

```powershell
curl.exe -s "<URL>/api/mercado-privado/dimensiones/admin/estado-identidad" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"run_activo": <RUN_NUEVO>`. OJO: `modo_card` va a estar en `"fallback"` y los `*_identidad_null` altos — **es normal**, la identidad viaja en el Paso 2.

**Rollback:** antes del finalize no cambió nada visible; para descartar el run en staging o para VOLVER al 67 incluso después del finalize: `python scripts/upload_dimensionamiento_chunks.py --url <URL> --token <TOKEN> rollback --run-id <RUN_NUEVO>` (pone el run nuevo en `failed` → el vigente vuelve a ser el 67 con su identidad intacta).

## Paso 2 — Identidad del run nuevo (Fase 1 + Fase 2 juntas)

```powershell
python -m scripts.push_identity --url <URL> --token <TOKEN> --records --remote-run <RUN_NUEVO> --timeout 300
```

- **Duración estimada:** 3–10 minutos (~50 lotes chicos). Si corta, reejecutar el mismo comando (reanudable, solo toca filas NULL).
- **Salida esperada:** `✅ FASE 1 COMPLETA (run <RUN_NUEVO>). summary_null=0 records_null=0`.

**Verificación** (mismo curl del Paso 1). Esperado: `"modo_card": "identidad"`, `"entidades": 256`, `"entidades_si": 158`, `"entidades_no": 98`, `"summary_filas_identidad_null": 0`, `"records_filas_identidad_null": 0`.

## Paso 3 — Refrescar las tablas precalculadas de Match (SÍ hace falta esta vez)

Estaban desactualizadas desde junio (deuda conocida: se calcularon sobre un dataset anterior). Ya quedaron recalculadas en local sobre el run 10; el push sube SOLO esas dos tablas, sin re-subir las 64.223 propuestas ni tocar la corrida vigente de Match:

```powershell
python -m scripts.push_match_data --url <URL> --token <TOKEN> --solo-precalc --timeout 300
```

- **Duración estimada:** 2–5 minutos (2 lotes de negocio_map + 15 de demanda_desc).

**Verificación:**

```powershell
curl.exe -s "<URL>/api/mercado-privado/match/admin/estado" -H "X-Import-Token: <TOKEN>"
```

Esperado: `"match_negocio_map": 3446`, `"match_demanda_desc": 29641`, y `"run_vigente_filas": 64223` (sin cambios — las propuestas no se tocaron).

## Paso 4 — Verificación final en la página real

1. `<URL>` → Mercado Privado → Dimensionamiento:
   - Card: **256 ENTIDADES (158 clientes · 98 no clientes)**, sin pill amarillo.
   - Serie mensual: 18 meses, ene-2025 a jun-2026 (SIN meses posteriores a jun-26).
   - Filtro de Familias: ~2.522 opciones (antes 2.451) y menos 'SIN DATO'.
   - Filtro Clientes / No clientes: ambos devuelven datos (322.605 / 42.282 renglones).
   - Vistas Renglones y Valorización cargan; valorización total ≈ $204.478 M.
2. Match desde AMBOS mercados: los desplegables de Negocio muestran 8 negocios y el tablero sigue con 64.223 propuestas / 2.252 artículos.

## Resumen de rollbacks

| Problema | Acción |
|---|---|
| Push corta a mitad (Paso 1) | Reejecutar el mismo comando (reanuda); nada visible cambió |
| Run nuevo mal tras finalize | `upload_dimensionamiento_chunks.py rollback --run-id <RUN_NUEVO>` → vuelve el 67 con identidad intacta |
| Identidad no llega a 0 NULL | Reejecutar el push_identity del Paso 2; si persiste, mandame la salida |
| Match raro tras el Paso 3 | Reejecutar el Paso 3 (reset + repoblado idempotente); las propuestas nunca se tocan |
| Todo mal también en local | Backup: `Desktop\backups_siem\app.db.bak-20260723` |
