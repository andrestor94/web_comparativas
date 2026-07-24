# RUNBOOK — Producción: dataset actualizado de Dimensionamiento (jul 2026)

**Fecha:** 24/07/2026 · **Ejecuta:** Andrés · Comandos de UNA línea para PowerShell (`curl.exe` explícito, nunca `curl` a secas).

**Qué se sube:** el run 10 local, validado completo (ingestado del `dataset_unificado_valorizado_2025_2026.csv` actualizado con el parser de fechas corregido). Diferencias reales vs el run 67 vigente en prod: 2.564 filas re-clasificadas de familia ('SIN DATO' → producto real; familias 2.451 → **2.522**), 1 CUIT corregido con cero inicial (1.337 filas del mismo cliente) y 13 filas con centavos de valorización. Mismas 364.887 filas, mismos 18 meses (ene25–jun26), mismas 256 entidades (158 · 98), misma valorización total (~$204.478 M; difiere $0,33 por redondeo). Además se refrescan las 2 tablas precalculadas de Match, que estaban desactualizadas desde junio: `match_negocio_map` 3.238 → **3.446** y `match_demanda_desc` 28.598 → **29.641**.

**NO hace falta deploy de código**: el push es solo datos. (Hay 3 commits locales sin pushear — fix del parser de fechas, fix de Capa C y `--solo-precalc` — que conviene incluir en el PRÓXIMO push a main por higiene, pero este runbook no depende de ellos: el server de prod parsea las fechas de los chunks con su propio parser sano.)

**Variables:** `<URL>` = URL base de prod (sin barra final) · `<TOKEN>` = `DIMENSIONAMIENTO_IMPORT_TOKEN` (Render → Environment, o tu `.env`).

**Red de seguridad:** el run 67 y su identidad quedan INTACTOS en prod durante todo el proceso. El run nuevo entra en staging (estado `running`, invisible) y recién pasa a vigente en el finalize del Paso 1. Rollback local adicional: `C:\Users\ANDRES.TORRES\Desktop\backups_siem\app.db.bak-20260723`.

---

## Paso 1 — Push del run nuevo por chunks (records + summary + finalize)

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
