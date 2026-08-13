# Oportunidades CRM: pendientes y deep-link

## Listado principal

`GET /api/mercado-privado/oportunidades/list` devuelve solamente oportunidades
pendientes para el entorno de CRM activo (`simulado`, `test` o `prod`). La consulta
base obtiene en una sola consulta los `crm_envios.oportunidad_id` del entorno activo y
excluye antes de serializar usando `opportunity_stable_id`, la misma funcion que crea el
`id_sistema_origen_c`; no reconstruye la identidad con un criterio alternativo de cliente/producto.
codigo con la misma normalizacion (`trim + lower`) que usa
`opportunity_stable_id`. La condicion incluye `crm_modo`, por lo que TEST y PROD
permanecen independientes.

La respuesta, sus totales y `completeness` nacen del mismo conjunto pendiente. Los
KPIs, busqueda, filtros y orden de la pantalla se calculan en el navegador, pero solo
sobre esas filas ya depuradas por el backend. La vista no implementa paginacion. Al
confirmar un envio, el frontend retira inmediatamente la fila de su conjunto local.

El repositorio de enviadas conserva su contrato normal: lista todos los envios y sus
contadores no dependen de la exclusion del listado principal.

## Deep-link del repositorio

La vista acepta:

`/mercado-privado/oportunidades/enviadas?oportunidad_id=<id_sistema_origen_c>`

El frontend no transforma el valor: lo lee de `URLSearchParams` y lo transmite con
`encodeURIComponent`. La resolucion puntual usa:

`GET /api/mercado-privado/oportunidades/enviadas/detalle?oportunidad_id=<valor>`

El endpoint compara `oportunidad_id` por igualdad exacta y agrega siempre
`crm_modo = <entorno activo>`. Responde `data.found`, `data.crm_modo` y, cuando existe,
`data.row`. No descarga el repositorio completo para buscar el registro.

Si encuentra la oportunidad, abre el mismo modal de detalle que se usa al seleccionar
una fila del repositorio. Si no existe en el entorno actual, presenta un estado explicito
de no encontrada y un enlace para volver al repositorio completo. La URL sin parametro
mantiene la carga normal. Como el entorno activo se determina en el servidor en cada
request, una recarga o cambio de configuracion vuelve a resolver el identificador sin
reutilizar coincidencias de otro entorno.

## Base de datos y seguridad

No hay cambios de esquema ni migraciones. Se conserva la unicidad por
`(oportunidad_id, crm_modo)`. El identificador llega como parametro enlazado de
SQLAlchemy (sin concatenacion SQL) y tiene un limite de 255 caracteres; los formatos
inesperados producen un resultado controlado.

## Pruebas

`tests/test_oportunidades_crm_pending_and_deeplink.py` cubre:

- nunca enviada, enviada solo a TEST, solo a PROD y a ambos entornos;
- paridad entre filas, total y completitud del conjunto pendiente;
- imposibilidad de que los filtros del cliente reincorporen una fila enviada;
- comparacion normalizada del origen para la exclusion SQL;
- coincidencia exacta, entorno incorrecto, cambio de entorno e identificador hostil;
- URL normal sin parametro, lookup puntual, codificacion y nueva resolucion frontend.

## Archivos del cambio

- `web_comparativas/routers/oportunidades_router.py`
- `web_comparativas/static/js/mercado_privado_oportunidades.js`
- `web_comparativas/static/js/mercado_privado_oportunidades_enviadas.js`
- `web_comparativas/templates/mercado_privado_oportunidades.html`
- `web_comparativas/templates/mercado_privado_oportunidades_enviadas.html`
- `tests/test_oportunidades_crm_pending_and_deeplink.py`
- `docs/OPORTUNIDADES_CRM_PENDIENTES_Y_DEEPLINK.md`
