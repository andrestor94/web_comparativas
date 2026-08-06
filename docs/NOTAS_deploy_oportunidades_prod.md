# NOTAS — Insumo para el futuro deploy de Oportunidades a producción

**Estado:** módulo funcionando en LOCAL (rama `feature/oportunidades-rescate`, jul-2026). Este archivo NO es el runbook: es la lista de lo que ese runbook va a necesitar cuando se decida el pase.

## Qué hay hoy

- Motor + tabla precalculada `oportunidades_summary` (run-scoped por `import_run_id`; en local: 143 filas sobre el run 10). Efectividad = GANADO/(GANADO+COMPRADO_OTRA_EMPRESA+EN_ESPERA); tipología ESTABLE/RECURRENTE/INTERMITENTE/PUNTUAL; ancla `oportunidad_id = sha1(cliente_visible|codigo_articulo)[:16]` (sin CUIT a propósito: 7,9% 'SIN DATO').
- Control de envíos a CRM: `crm_envios` (UNIQUE por oportunidad_id) + bitácora `crm_envio_eventos`; sello server-side; duplicados bloqueados; override solo Admin.
- Kill-switch `OPORTUNIDADES_ENABLED` — **default OFF** (cambiado de junio, misma convención que Match). En local está en `.env`; prod NO lo tiene → aunque el código llegue a prod por otro deploy, el módulo no aparece.

## Integración con el CRM (SuiteCRM V8) — IMPLEMENTADA (ago-2026)

`_enviar_real_a_crm` ya no es un placeholder. El circuito vive en `web_comparativas/dimensionamiento/crm_client.py` y corre, por cada envío, los 5 pasos probados en Postman:

1. `POST /Api/access_token` (client_credentials) → token nuevo por envío, **no se cachea**.
2. `GET /Api/V8/custom/reports/usuarios_rendidores` → matchea `user_name` contra el mail de SIEM sin dominio; si no matchea usa `CRM_USUARIO_FALLBACK_ID`.
3. `GET /Api/V8/custom/reports/Cuentas_por_numero_fusion?n_cuenta_c=<nro>` → `account_id`. 404 (o lista vacía) = cliente inexistente en el CRM → corta con HTTP 422 y mensaje claro, nunca un 500.
4. `POST /Api/V8/module` type `Opportunities` → guarda `data.id` en `crm_envios.crm_id`.
5. `POST /Api/V8/module` type `KNN_BitacoraMsj` (parent = la oportunidad recién creada).

**Nº de cuenta de fusión:** sale de `dimensionamiento_records.cuenta_interna` (columna del dataset), NO del cuit — el payload viejo mandaba el cuit en `n_cuenta`, que el CRM no resuelve. Se propaga al summary vía `oportunidades_summary.cuenta_interna`; para summaries viejos hay fallback a records. Cobertura en el run 10 local: **135/143** (8 quedan con `SIN DATO` y no se pueden enviar hasta que el dataset traiga la cuenta).

**Manejo de errores** (ninguno cae en 500): 404 del paso 3 → 422 "el cliente no existe en el CRM"; 400/422 → 422 con el `detail` textual del CRM; 401/403 → 502; 5xx y timeouts → 503 reintentable; falta de env vars → 503 nombrando la variable. **Ante cualquier falla NO se registra el envío**, así la oportunidad queda libre de reintentar sin pedir override.

**Excepción deliberada:** si falla el paso 5 (bitácora) el envío se considera exitoso igual — la oportunidad ya existe en el CRM y perder su id obligaría a reenviar, duplicándola del lado del CRM. Se devuelve `bitacora_error` y se anota en la nota del evento.

**Flags** (en `web_comparativas/.env`, que es el que carga la app — no el de la raíz):

| Variable | Default | Para qué |
|---|---|---|
| `CRM_ENVIO_PLACEHOLDER` | `1` (ON) | ON = simula, no toca el CRM (`crm_status=SIMULADO`). **Se mantiene a propósito** para poder seguir simulando después del go-live. |
| `CRM_MODO` | `test` | A qué CRM se envía cuando el placeholder está en 0: `test` \| `prod`. Evita pegarle a producción por error; queda registrado en `crm_envios.crm_modo` y se loguea en WARNING si es prod. |
| `CRM_BASE_URL` | — | `https://tstsc01.suizoargentina.com` en TEST. |
| `CRM_CLIENT_ID` / `CRM_CLIENT_SECRET` | — | Credenciales. **Nunca hardcodeadas ni commiteadas** (`.env` está en `.gitignore`). |
| `CRM_USUARIO_FALLBACK_ID` | — | Usuario del CRM al que se asigna si el mail de SIEM no matchea. |
| `CRM_DIAS_CIERRE_TENTATIVO` | vacío | Días a futuro para `date_closed`. Vacío → el campo **se omite** (SIEM no tiene fecha de cierre real). |

**UI:** una vez que hay `crm_id`, el botón "Enviar a CRM" pasa a "Ver en CRM" apuntando a `{CRM_BASE_URL}/index.php?action=DetailView&module=Opportunities&record=<ID_CRM>`.

**Formas de respuesta REALES** (verificadas en Postman, ago-2026 — el parser está ajustado a estas y conserva el fallback tolerante):

```
usuarios_rendidores        {"status": true, "data": [{"id": "...", "usuario": "jacqueline.gallo", "legajo_c": "1026123"}]}
Cuentas_por_numero_fusion  {"status": true, "data": {"id": "b1ca8e5c-...", "name": "SAD CORDOBA S.A (L.H)", "n_cuenta_c": "106920"}, "message": "", "status_code": 200}
creación (201)             {"data": {"type": "Opportunity", "id": "400ece16-...", "attributes": {...}}}
```

Ojo con dos detalles del contrato: en `Cuentas_por_numero_fusion` el `data` es un **objeto**, no una lista; y los reports traen un `status` propio **además** del HTTP, así que un `status:false` con HTTP 200 se trata como error (en la búsqueda de cuenta, como "cuenta inexistente").

**Asignación del usuario del CRM** (criterio vigente, ago-2026 — sin fallback silencioso):

1. **Match automático** por el campo `usuario` del CRM (el mail de Suizo sin dominio) contra el mail del usuario logueado en SIEM. Si coincide, se asigna a esa persona y el modal dice `asignado a vos (<usuario>)`.
2. **Sin match: NO se asigna a nadie automáticamente.** El modal muestra un **selector con todos los usuarios del CRM** para que quien envía elija explícitamente (arranca vacío a propósito: sin opción preseleccionada no hay envío distraído). Queda registrado como `crm_assigned_origen='manual'` junto con el usuario elegido, y `enviado_por` sigue guardando a quien disparó el envío.
3. `CRM_USUARIO_FALLBACK_ID` **ya no se aplica solo**: solo puede pre-sugerir una opción en el selector. La asignación automática a un tercero se eliminó porque hacía aparecer oportunidades en el CRM a nombre de gente que no las generó.

**Quién puede elegir el asignado (por rol, ago-2026):**

| Rol | Selector | Puede asignar a |
|---|---|---|
| Analista | No lo ve | **Solo a sí mismo** (su match del CRM) |
| Supervisor / Admin | Sí | Cualquier usuario del CRM |

El selector aparece **al abrir el modal**, no después de un envío rechazado: el Supervisor lo ve siempre (con su propio usuario preseleccionado si tiene match, editable), y el botón Confirmar arranca deshabilitado hasta que haya alguien elegido. El 422 del backend queda como defensa, no como camino normal — el usuario no debería llegar a verlo nunca.

El chequeo es **server-side**: un Analista que llame al endpoint con otro `assigned_user_id` recibe 422 (la UI que no le muestra el selector es cosmética). Si un Supervisor se elige a sí mismo, se registra como `match`, no como `manual`: elegir lo que ya correspondía no es una reasignación.

⚠️ **Cache-busting:** el `?v=` de los `<script>`/`<link>` de estos templates **hay que subirlo en cada cambio de JS/CSS** (hoy `oportunidades-v10-crm`). Si no, el navegador sigue sirviendo el archivo viejo y los cambios "no aparecen" sin ningún error a la vista — ya pasó con el selector de asignación.

**Ningún mensaje de error visible muestra códigos HTTP.** El `detail` que devuelve el backend viene redactado para leerse; el front tiene además un mapa de respaldo por status por si alguna respuesta llega sin él.

⚠️ **Forma del cuerpo de error en rutas `/api/`:** `main.py` tiene un `@app.exception_handler(HTTPException)` global que las reempaqueta como `{"error": <detail>, "status": <code>}` — **NO** como `{"detail": ...}`. Cualquier front que lea solo `detail` va a recibir `undefined` y mostrar un genérico, tapando el mensaje real (ya pasó: un 422 legítimo se veía como "faltan datos" y escondía que el CRM no había respondido). Leer siempre `detail || error`. Caso borde cubierto: **Analista cuyo usuario de SIEM no existe en el CRM** no tiene a quién asignar y tampoco puede elegir → el envío se bloquea con *"Tu usuario no está dado de alta en el CRM…"* en lugar de dejarlo con un botón muerto. A un Supervisor en la misma situación se le pide elegir, que sí puede.

La consulta al CRM se hace una sola vez por request (no por fila) y trae el match + la lista completa. Si el CRM no responde, no hay lista que ofrecer y el envío queda **bloqueado** con el motivo a la vista.

**La bitácora del CRM (paso 5)** es corta a propósito — todo el detalle de negocio ya viaja en `description` de la oportunidad y repetirlo solo ensucia el hilo:

```
Enviado desde SIEM por <mail SIEM>. Asignada a <usuario CRM>. dd/mm/aaaa HH:MM.
Enviado desde SIEM por <mail SIEM>. Asignada manualmente a <usuario CRM>. dd/mm/aaaa HH:MM.
```

Lo único que aporta y no está en ningún otro lado: `assigned_user_id` dice quién la TRABAJA, no quién la generó — sin esta línea, una oportunidad asignada a un tercero perdería todo rastro de su origen. "Asignada manualmente" aparece solo cuando el asignado difiere de quien envía.

La hora va en **hora argentina** (UTC-3 fijo; el país no tiene horario de verano desde 2009, así que no depende de tzdata, que en Windows no viene instalado). La app sigue guardando todo en UTC: la conversión es solo para esta línea, que la lee una persona dentro del CRM.

El texto lo compone el ROUTER (`crm_client.texto_bitacora`) con la hora real del envío, y `enviar_oportunidad` lo manda tal cual. El modal muestra exactamente ese texto: `/list` devuelve `crm_asignacion.bitacora_por_usuario`, un mapa `{id_usuario: texto}` precalculado con el mismo helper, así que al cambiar el selector el preview se actualiza sin que el front recomponga nada. **Si el front lo armara por su cuenta, cualquier retoque del texto habría que hacerlo en dos lados y tarde o temprano divergirían.**

> **CERTIFICADO (una línea):** al CRM le falta publicar el **certificado intermedio** de su cadena TLS (Sectigo); **lo arregla INFRA** en la config del servidor del CRM; si no se corrige antes de producción, SIEM no puede validar el certificado y **todos los envíos al CRM fallan** (503 "no se pudo conectar"), y la única alternativa sería mantener el bundle manual `CRM_CA_BUNDLE` —que hay que regenerar cada vez que el certificado rote— o apagar la verificación TLS, lo que expondría las credenciales.

**TLS: el CRM no manda la cadena intermedia.** Su certificado es un wildcard válido de Sectigo (`*.suizoargentina.com`), pero el servidor NO envía el intermedio. Los navegadores no lo notan (lo bajan solos por AIA); Python/requests no, y falla con `certificate verify failed: unable to get local issuer certificate`. Solución sin apagar la verificación: `python -m scripts.crm_ca_bundle` arma un bundle (CAs de certifi + el intermedio faltante) y se apunta `CRM_CA_BUNDLE` a ese archivo. **Lo correcto de fondo es que infra agregue el intermedio a la config del servidor**; mientras tanto el bundle hay que regenerarlo si el certificado rota. Existe `CRM_SSL_VERIFY=0` como puerta de emergencia, pero deja las credenciales expuestas a interceptación y avisa por WARNING en cada envío — no es el default y no debería usarse en prod.

**Sin envíos degradados** (ago-2026): si no se resuelve el usuario asignado (ni por match ni por fallback) o falta la cuenta de fusión, la oportunidad NO se envía: el modal muestra el motivo y el botón queda deshabilitado, y el endpoint devuelve 422 aunque se lo llame a mano. `assigned_user` nunca lleva un texto de error: o es un nombre real, o es `null` y el envío está bloqueado.

**Bloqueo de duplicados POR ENTORNO** (ago-2026): la clave única es `(oportunidad_id, crm_modo)`. Lo enviado a TEST no bloquea PROD, y lo simulado no bloquea nada real. El listado y el botón reflejan el entorno vigente (`CRM_MODO`), no un estado global. `crm_modo` es parte de la clave: si quedara NULL el bloqueo se apagaría en silencio (dos NULL son distintos entre sí en SQLite y en PG), por eso el código lo setea siempre y la migración rellena las filas viejas a `'simulado'`.

## Repositorio de oportunidades enviadas

Vista `/mercado-privado/oportunidades/enviadas` (API `GET /api/mercado-privado/oportunidades/enviadas`): lista TODO lo enviado con cliente, producto, monto, quién lo envió, a quién quedó asignado, fecha, entorno y link al CRM. Ordenable por cualquier columna y con búsqueda, ambas del lado del cliente.

Dos decisiones deliberadas: **no filtra por `crm_modo`** (el entorno es una columna, no un filtro implícito — el sentido es ver todo y a dónde fue) y **no se limita al run activo** (lee `crm_envios`, que sobrevive a los recálculos; las que ya no califican se marcan "fuera de la corrida actual"). El monto sale del `payload_snapshot` —el que se mandó de verdad, no uno recalculado hoy— y el producto del summary con el código de artículo como respaldo.

Comparte permiso y kill-switch con la vista principal a propósito: es de LECTURA para todos los que ven Oportunidades (Auditor y Gerente incluidos), así que no crea una hoja nueva que haya que tildar en S.I.C.

## Pasos que va a necesitar el runbook de prod (borrador de orden)

1. **Código:** los commits de la rama van a main (deploy). El módulo queda dormido (kill-switch OFF, sin datos): riesgo cero.
2. **Esquema:** `oportunidades_summary` la crea `create_all`; `crm_envios`/`crm_envio_eventos` la migración idempotente `_ensure_crm_envios_table` (ya corre en startup — verificar su línea SUCCESS en el log de arranque). Esa misma migración agrega ahora, en bases ya existentes, `crm_envios.crm_id/crm_account_id/crm_modo`, `crm_envio_eventos.crm_id` y `oportunidades_summary.cuenta_interna` (ALTER TABLE aditivos, no destructivos), **da de baja los índices únicos viejos** `ix_crm_envios_oportunidad_id` y `uq_crm_envios_oport` (bloqueo cross-entorno) y crea `uq_crm_envios_oport_modo`. Cada sentencia va en su propia transacción a propósito: en PG un error deja la transacción abortada y se llevaría puesto el resto (el `InFailedSqlTransaction` de Dimensionamiento). **Verificar post-deploy** que en prod quedó `uq_crm_envios_oport_modo` y que no sobrevive ningún único sobre `oportunidad_id` solo; si sobreviviera, el bloqueo seguiría siendo global sin dar ninguna señal.
3. **Datos:** `oportunidades_summary` se calcula LOCAL (`scripts/rebuild_oportunidades.py --run-id <run local espejo del activo de prod>`) y debe viajar como DATO — **falta construir el push por chunks** (patrón `push_match_data`: endpoint admin por token + script cliente, reset run-scoped, reanudable). Son ~150 filas: un solo lote alcanza, pero respetar el patrón. NUNCA correr el rebuild server-side en Render (lee todos los records del run).
4. **Encendido:** `OPORTUNIDADES_ENABLED=1` en Render Environment (redeploy) — recién ahí aparece el sidebar para quienes tengan la casilla.
5. **Permisos:** la hoja `mercado_privado.oportunidades` aparece sola en S.I.C.; usuarios NULL-legacy la ven por techo de rol; a los de module_access explícito hay que tildársela.
6. **Verificación:** página con datos reales, conteo esperado de oportunidades del run, envío de prueba en modo SIMULADO, y purga con `clear_crm_envios.py` si se ensució.
7. **CRM real:** deploy aparte, con su propia prueba. Orden sugerido: (a) cargar `CRM_BASE_URL`/`CRM_CLIENT_ID`/`CRM_CLIENT_SECRET`/`CRM_USUARIO_FALLBACK_ID` en Render con `CRM_MODO=test` y `CRM_ENVIO_PLACEHOLDER=1`; (b) pasar el placeholder a 0 y enviar UNA oportunidad contra TEST, verificando que aparezca en el CRM y que el botón cambie a "Ver en CRM"; (c) recién con eso OK, `CRM_MODO=prod`. Con el bloqueo por entorno, lo probado en TEST **no** bloquea el envío productivo: no hace falta purgar nada entre una etapa y la otra.

## Nota de sincronía de datos

`oportunidades_summary` depende del run activo de Dimensionamiento: tras cada reemplazo de dataset en prod (nuevo run activo), hay que recalcular en local sobre el run espejo y re-pushear — agregarlo como paso al runbook estándar de actualización de dataset.
