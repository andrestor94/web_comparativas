# Auditoría de identidad de cuentas CRM

Fecha de ejecución: 2026-08-06. Rama local auditada: `feature/oportunidades-crm-envio-real`. Alcance estrictamente read-only sobre datos y CRM TEST; no se crearon Opportunities.

## 1. Puente exacto implementado

1. `dataset_unificado_valorizado_2025_2026.csv.cliente_nombre_homologado` es la razón social usada. `cliente_nombre_original` queda solo como evidencia; `cliente_visible` no participa del puente.
2. Se aplica `web_comparativas.dimensionamiento.identity.canon`.
3. Se exige igualdad exacta con `clientes.csv.nombre` después de aplicar la misma función.
4. De las filas iguales se recupera `clientes.csv.codigo`.
5. Ese código se compara textualmente con `Operadores.xlsx.Codigo` para enriquecer `Vendedor` y `Nombre`.
6. El código es la cuenta candidata. Ni operador ni grupo comercial crean relaciones adicionales.

Código exacto de `canon`:

```python
def canon(value: str | None) -> str:
    if value is None:
        return ""
    s = unicodedata.normalize("NFKD", str(value))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Za-z0-9]+", " ", s.upper())
    return re.sub(r"\s+", " ", s).strip()
```

Comportamiento exacto: NFKD elimina tildes mediante descarte de marcas combinantes; luego descarta caracteres restantes no ASCII, pasa a mayúsculas, reemplaza cada secuencia no alfanumérica por un espacio, colapsa espacios y recorta extremos. No elimina `S.A.`, `S.R.L.` ni otros sufijos (solo su puntuación); no elimina sucursales; no elimina palabras cortas. No usa `contains`, `startswith`, similitud, fuzzy, prefijos ni coincidencia parcial. Para evitar nombres parecidos solo se acepta igualdad exacta de la clave completa. Además se bloquean claves sin término distintivo de más de dos caracteres, claves compartidas por más de un CUIT y CUIT con más de un nombre homologado incompatible.

## 2. Métricas reales

| Métrica | Valor |
|---|---:|
| Nombres canónicos únicos en dataset | 159 |
| Nombres canónicos únicos en clientes.csv | 28.135 |
| Coincidencias exactas | 159 |
| Uno a uno | 82 |
| Uno a varios | 77 |
| Varios CUIT a un nombre | 0 |
| CUIT con más de un nombre | 1 |
| Nombre con más de una cuenta del dataset | 0 |
| Nombre con más de un `clientes.codigo` | 77 |
| `clientes.codigo` con más de un nombre | 0 |
| Claves genéricas coincidentes | 3 |
| Oportunidades auditadas | 143 |
| Con alternativas por el puente | 101 |
| Ambiguas/bloqueadas | 3 oportunidades; 5 claves de identidad |
| Sin relación | 8 |
| Solo cuenta original | 31 |
| Máximo real de candidatas | 22 |
| Casos reales con más de 25 | 0 |

El CSV contiene las 159 claves auditadas y marca `top_30_revision=SI` para las treinta de mayor riesgo. Incluye CUIT, nombres fuente de ambos archivos, clave, cuentas, cardinalidades, decisión y motivo. Las cinco claves efectivamente bloqueadas están primeras; no se presenta una relación bloqueada como candidata automática.

## 3. Sanatorio Juan XXIII: cadena completa

Dataset, fila física 209027 (también hay la misma identidad en filas 227610 y 243562):

- `fecha=2025-12-01`
- `cliente_nombre_original=Sanatorio Juan XXIII S.R.L.`
- `cliente_nombre_homologado=SANATORIO JUAN XXIII S.R.L.`
- `cuit=30595201027`
- `cuenta_interna=32059`
- `plataforma=BIONEXO`
- `codigo_articulo=8111612`
- `descripcion=CEFTAZIDIMA 2 G + AVIBACTAM AMP X10 - PFIZER`
- `resultado=EN_ESPERA`

Clave producida: `SANATORIO JUAN XXIII S R L`.

`clientes.csv`:

- fila 21669: `codigo=8519`, `nombre=SANATORIO JUAN XXIII S.R.L.`, `fantasia=SANATORIO JUAN XXIII S.R.L (L.H)`, `grupo=0`, `cliente_grupo=8519`, `tipocli=IPR`;
- fila 21797: `codigo=32059`, mismo `nombre`, `fantasia=SANATORIO JUAN XXIII S.R.L (T.E)`.

`Operadores.xlsx`, fila 890: `Codigo=8519`, `Fantasia=SANATORIO JUAN XXIII S.R.L (L.H)`, `Vendedor=4071`, `Nombre=AYELEN PILUSO`. No existe fila de operador para 32059.

Consulta read-only real de 8519 en CRM TEST:

```json
{
  "exists": true,
  "crm_account_id": "29502a4f-5944-5cb1-5f50-68473bae4bdb",
  "crm_nombre": "SANATORIO JUAN XXIII S.R.L (L.H)",
  "crm_numero_cuenta": "8519",
  "crm_cuit": null,
  "crm_razon_social": "SANATORIO JUAN XXIII S.R.L (L.H)",
  "crm_documento": null
}
```

CRM TEST no expone CUIT ni documento en este reporte. Por ello 8519 queda como `ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO`, con `confirmacion_fiscal=false`; no se afirma identidad fiscal.

## 4. Confianza, ambigüedad y límite

Estados implementados: `CUENTA_ORIGINAL_CONFIRMADA`, `ALTERNATIVA_CONFIRMADA_POR_CUIT`, `ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO`, `RELACION_AMBIGUA`, `SIN_RELACION`, `ERROR_CONSULTA_CRM`.

Se bloquea la resolución automática por nombre compartido entre CUIT, varios nombres incompatibles para un CUIT, clave genérica, CUIT CRM divergente o más de 25 candidatas. Si CRM devuelve varias alternativas válidas y ninguna prioridad verificable, se muestra `RELACION_AMBIGUA` y se exige selección manual de un usuario autorizado; la primera, menor o más frecuente nunca gana. La selección manual queda registrada como `seleccion_origen=manual`.

Con más de 25 se conserva el conteo total, no se trunca la lista, no se consulta un subconjunto, se marca `MAS_DE_25_CUENTAS_CANDIDATAS`, se bloquea y no se envía. No existe un caso real actual (máximo 22); la prueba sintética usa 26 y comprueba esas invariantes.

## 5. Persistencia y repositorio de enviadas

No hubo cambio de esquema ni migración. En `crm_envios`, `crm_account_id` guarda el id CRM utilizado y `payload_snapshot` (TEXT con JSON) guarda: `cuenta_original`, `cuenta_utilizada`, `cuenta_criterio`, `cuenta_estado_confianza`, `cuenta_confianza_label`, `cuenta_confirmacion_fiscal`, `cuenta_seleccion_origen`, `crm_cuit_informado`, `crm_razon_social_informada`, `operador_codigo`, `operador_nombre`, `fuente_relacion_cuenta` y `cuentas_evaluadas`. Las columnas `crm_assigned_user_id`, `crm_assigned_usuario` y `crm_assigned_origen` guardan el usuario SuiteCRM y si fue `match` o `manual`.

`crm_envio_eventos.payload_snapshot` repite el snapshot en la bitácora append-only y `crm_envio_eventos.nota` conserva la traza textual. Los registros anteriores siguen siendo legibles: el serializador usa valores nulos/leyenda de registro anterior cuando esas claves JSON no existen. El repositorio de enviadas lee el JSON persistido y muestra original, utilizada, confianza/criterio, operador, origen automático/manual, fuente y conteo. La integración se verificó con base transaccional de prueba/rollback; no se generó un envío real.

## 6. Operador frente a usuario CRM

- `4071` es `Operadores.xlsx.Vendedor`.
- `AYELEN PILUSO` es `Operadores.xlsx.Nombre` y es información comercial de la cuenta.
- `ayelen.piluso` es un usuario real devuelto por `usuarios_rendidores` de SuiteCRM.
- El usuario SIEM de la validación fue `admin@suizo.com` (sesión Admin) y no tuvo match automático en SuiteCRM.
- En la prueba visual previa, `ayelen.piluso` fue una opción del selector; la asignación mencionada anteriormente fue manual.
- No existe transformación de `AYELEN PILUSO` a `ayelen.piluso` ni sugerencia derivada del operador. El match automático permitido usa únicamente el identificador del usuario SIEM contra la lista real de usuarios CRM. Analista solo puede asignarse a sí mismo; Admin/Supervisor pueden elegir manualmente a un usuario real.

## 7. POST real controlado y repositorio

Las pruebas usan `TestClient` contra la ruta HTTP real y sustituyen únicamente la frontera de CRM para impedir tráfico/escrituras externas. El backend vuelve a ejecutar la resolución en cada POST y no confía en `cuenta_seleccionada` de JavaScript.

| Caso | Resultado |
|---|---|
| cuenta válida candidata | acepta la resolución del servidor y persiste traza en transacción de prueba |
| `HACK` manipulada | 422, cero envíos/eventos, oportunidad reintentable |
| 6280 de otro CUIT | 422, cero escrituras |
| cuenta CRM existente pero no vinculada | 422, cero escrituras |
| válida en TEST pero no en PROD | 422 en el entorno actual, cero escrituras |
| relación cambiada desde apertura del modal | 422 tras re-resolver, cero escrituras |
| más de 25 | 422, sin truncar/consultar/enviar |
| error parcial de CRM | 503 funcional, cero escrituras |
| relación ambigua o inexistente | 422, cero escrituras |

Ningún rechazo produjo 500. La oportunidad permanece disponible porque `crm_envios` solo se inserta después del ACK exitoso. La base real conservó 6 `crm_envios` y 6 eventos; el último envío sigue siendo 2026-08-06 16:22:07, anterior a esta auditoría.

## 8. Muestras read-only reales

Uno a uno (cinco): 30654161786 AGRUPACION MEDICA INTEGRAL S R L → 10454; 30645337979 ALEXANDER FLEMING SA → 2178; 30707431543 AMERICAN SALUD S.R.L. → 6752; 30500974016 ASOCIACION ARGENTINA DE LOS ADVENTISTASA → 116413; 30545873024 ASOCIACION DE BENEFICIENCIA HOSPITAL SIR → 30702.

Varias cuentas (cinco): 30546128403 CEMIC → 10; 34546198105 DASPU → 6; 30550245309 OMINT → 6; 33561330609 FLENI → 5; 30710126107 LA CASA DEL MEDICO MUTUAL → 5.

Ambiguas (las cinco existentes): las dos claves del CUIT 30545875558 por múltiples nombres homologados; HOSPITAL PRIVADO CENTRO MEDICO (22 cuentas), CLINICA I M A SA e INSTITUTO MEDICO PRIVADO S A por claves sin términos distintivos.

Sin operador (cinco): CLIMO S.A. (3 cuentas), CLINICA BOEDO SRL (3), HOSPITAL ITALIANO DE LA PLATA ASOCIACION (3), OBRA SOCIAL DEL PERSONAL DE EMPRESA DE L (3), SOCIEDAD ITALIANA DE SOCORROS MUTUOS (3).

Sin relación (cinco de ocho): Fundación Cardiológica Correntina CUIT `SIN DATO`; Instituto Frenopático SA CUIT 30545842978; Hospital Italiano de La Plata CUIT `SIN DATO`; Sanatorio Colegiales CUIT `SIN DATO`; MEDISUR CUIT 30574921488.

## 9. Evidencias y reproducción

- Script read-only: `scripts/audit_crm_account_identity.py`.
- Métricas estructuradas: `docs/auditoria_identidad_cuentas_crm_metricas.json`.
- CSV UTF-8 BOM: `docs/auditoria_identidad_cuentas_crm.csv`.
- Captura: `docs/captura_modal_sanatorio_identidad_crm.png`.
- Suite específica de cierre: 35 passed.
- Suite principal `tests/`: 177 passed, 12 failed, 4 warnings. Las 12 fallas preexistentes/ajenas son 2 mocks desactualizados de `query_service`, 7 expectativas de permisos/redirección y 3 tests async sin `pytest-asyncio`.
- Suite desde raíz: interrumpida en colección por 5 tests heredados de Inflación que requieren `pyodbc`, dependencia ausente en el entorno.

La generación del CSV usó el fallback estándar UTF-8 BOM porque la herramienta avanzada de artefactos tabulares no estaba disponible en esta sesión; el script deja la reproducción completa y auditable.
## 10. Cierre técnico y regeneración

Los outputs derivados continúan ignorados por Git y se regeneran desde la raíz del checkout con:

```powershell
python scripts\audit_crm_account_identity.py --root . --output docs\auditoria_identidad_cuentas_crm.csv --metrics-output docs\auditoria_identidad_cuentas_crm_metricas.json
```

El script abre en modo lectura `dataset_unificado_valorizado_2025_2026.csv`, `clientes.csv`, `Operadores.xlsx` y la base local; solo escribe las dos rutas indicadas. El CSV UTF-8 BOM contiene los campos enumerados en esta auditoría y el JSON contiene las métricas agregadas. No se usará `git add -f`. La captura PNG es evidencia local y no se propone para el commit.

Ejemplo sanitizado de la persistencia nueva:

```json
{
  "cuenta_original": "32059",
  "cuenta_utilizada": "8519",
  "crm_account_id": "<uuid-cuenta-crm>",
  "criterio_resolucion": "unica_alternativa_valida",
  "nivel_confianza": "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO",
  "fuente_relacion": "dataset.cliente_nombre_homologado -> canon exacto -> clientes.nombre -> clientes.codigo -> Operadores.Codigo",
  "operador_codigo": "4071",
  "operador_nombre": "AYELEN PILUSO",
  "seleccion_cuenta": "automatica_unica_alternativa",
  "cantidad_candidatas": 2,
  "cantidad_evaluadas": 2
}
```

Se mantienen también las claves retrocompatibles `cuenta_criterio`, `cuenta_estado_confianza`, `fuente_relacion_cuenta`, `cuenta_seleccion_origen` y `cuentas_evaluadas`. `crm_account_id` se incorpora al snapshot después del ACK y también permanece en la columna `crm_envios.crm_account_id`. Los atributos reales enviados a SuiteCRM se construyen mediante una lista explícita en `crear_oportunidad`; las claves de auditoría no se envían como atributos, no alteran `n_cuenta`, son JSON serializable y no contienen tokens, credenciales ni secretos.

Caché: el índice maestro es inmutable y está protegido por `threading.Lock`; requests concurrentes del mismo worker comparten una única recarga. Cada worker Uvicorn tiene su propia copia y la refresca por `mtime_ns + size`. Una excepción de firma/lectura no reemplaza el último índice y el endpoint responde 503 controlado. La comparación de firma posterior a la lectura evita aceptar un archivo que cambie durante la carga; los productores deben publicar los maestros mediante reemplazo atómico para excluir un archivo parcial que sea sintácticamente válido. El índice del run usa un lock independiente y una consulta SQL `DISTINCT` por corrida. La validación CRM no se cachea: cada resolución obtiene configuración, sesión y token del entorno del servidor, por lo que TEST y PROD no comparten resultados.

Seguridad del GET: exige `mercado_privado.oportunidades`, limita el id a la corrida activa, devuelve 401 sin sesión y 404 para un id inexistente. El usuario del módulo ve el mismo conjunto que el listado (no existe segmentación adicional por fila). La respuesta solo contiene datos normalizados del caso; no incluye rutas, archivos completos, tokens ni secretos. `crm_modo` no es parámetro del endpoint y se obtiene del servidor.

Medición read-only local/CRM TEST: primera construcción 1.397 ms y una consulta SQL; segunda construcción 0,4 ms y cero SQL. AUREA MED (una candidata): 337 ms, un token y una consulta de cuenta. Sanatorio: 306 ms, un token y dos consultas de cuenta. Hospital Privado (22 potenciales, bloqueado): 0,5 ms, cero tokens y cero consultas CRM. No se relee el CSV fuente de 364.887 filas por modal y no hay N+1 local.
