# NOTAS — Insumo para el futuro deploy de Oportunidades a producción

**Estado:** módulo funcionando en LOCAL (rama `feature/oportunidades-rescate`, jul-2026). Este archivo NO es el runbook: es la lista de lo que ese runbook va a necesitar cuando se decida el pase.

## Qué hay hoy

- Motor + tabla precalculada `oportunidades_summary` (run-scoped por `import_run_id`; en local: 143 filas sobre el run 10). Efectividad = GANADO/(GANADO+COMPRADO_OTRA_EMPRESA+EN_ESPERA); tipología ESTABLE/RECURRENTE/INTERMITENTE/PUNTUAL; ancla `oportunidad_id = sha1(cliente_visible|codigo_articulo)[:16]` (sin CUIT a propósito: 7,9% 'SIN DATO').
- Control de envíos a CRM: `crm_envios` (UNIQUE por oportunidad_id) + bitácora `crm_envio_eventos`; sello server-side; duplicados bloqueados; override solo Admin.
- Kill-switch `OPORTUNIDADES_ENABLED` — **default OFF** (cambiado de junio, misma convención que Match). En local está en `.env`; prod NO lo tiene → aunque el código llegue a prod por otro deploy, el módulo no aparece.

## Estado del placeholder de CRM (para la reunión con el equipo de CRM)

- **Armado:** payload completo por oportunidad (moneda/assigned con placeholders `PENDIENTE_CRM`/`PENDIENTE_MAPEO_CRM`, lead_source SIEM, sales_stage Prospecting), sello `enviado_por/enviado_por_id/enviado_at`, idempotencia por `oportunidad_id`, flujo ACK-antes-de-registrar, flag `CRM_ENVIO_PLACEHOLDER` (default ON → los envíos quedan `crm_status=SIMULADO`; `scripts/clear_crm_envios.py` los purga).
- **Falta:** `_enviar_real_a_crm` es un placeholder deliberado — no hay endpoint, auth ni contrato del CRM.
- **Definir en la reunión:** URL/credenciales del CRM y su gestión (env vars en Render), contrato del payload (campos obligatorios, formato de moneda, mapeo assigned_user), semántica del ACK (qué respuesta cuenta como éxito), política de reintentos, y si el CRM deduplica por su lado o confía en nuestro `oportunidad_id`.

## Pasos que va a necesitar el runbook de prod (borrador de orden)

1. **Código:** los commits de la rama van a main (deploy). El módulo queda dormido (kill-switch OFF, sin datos): riesgo cero.
2. **Esquema:** `oportunidades_summary` la crea `create_all`; `crm_envios`/`crm_envio_eventos` la migración idempotente `_ensure_crm_envios_table` (ya corre en startup — verificar su línea SUCCESS en el log de arranque).
3. **Datos:** `oportunidades_summary` se calcula LOCAL (`scripts/rebuild_oportunidades.py --run-id <run local espejo del activo de prod>`) y debe viajar como DATO — **falta construir el push por chunks** (patrón `push_match_data`: endpoint admin por token + script cliente, reset run-scoped, reanudable). Son ~150 filas: un solo lote alcanza, pero respetar el patrón. NUNCA correr el rebuild server-side en Render (lee todos los records del run).
4. **Encendido:** `OPORTUNIDADES_ENABLED=1` en Render Environment (redeploy) — recién ahí aparece el sidebar para quienes tengan la casilla.
5. **Permisos:** la hoja `mercado_privado.oportunidades` aparece sola en S.I.C.; usuarios NULL-legacy la ven por techo de rol; a los de module_access explícito hay que tildársela.
6. **Verificación:** página con datos reales, conteo esperado de oportunidades del run, envío de prueba en modo SIMULADO, y purga con `clear_crm_envios.py` si se ensució.
7. **CRM real:** recién cuando la integración exista: `CRM_ENVIO_PLACEHOLDER=0` + credenciales — deploy aparte, con su propia prueba.

## Nota de sincronía de datos

`oportunidades_summary` depende del run activo de Dimensionamiento: tras cada reemplazo de dataset en prod (nuevo run activo), hay que recalcular en local sobre el run espejo y re-pushear — agregarlo como paso al runbook estándar de actualización de dataset.
