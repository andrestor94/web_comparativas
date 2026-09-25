# Match: lo decidido sale de la lista + monodroga como guía (LOCAL, 25/09/2026)

Solo local (8010). No se subió nada a prod.

## 1. Lo decidido sale de la lista

**Panel derecho**
- Al homologar o descartar una candidata, la tarjeta sale de la lista y las pendientes suben.
- Aparece un aviso de 6 s con **Deshacer**, que revierte la decisión y vuelve a abrir ese artículo aunque ya haya salido de la lista.
- En la cabecera hay un botón **Ver decididas (N)** con las homologadas y descartadas de ese artículo, cada una con su Deshacer. El mismo botón vuelve a **Ver pendientes (N)**.
- A una descartada que ya salió de la papelera (pasaron más de 24 h o se eliminó definitivamente) no se le puede hacer Deshacer desde acá. Muestra "fuera de la papelera". Es la misma regla que ya aplica la papelera.

**Panel izquierdo**
- La lista trae solo los artículos que tienen al menos una candidata pendiente.
- Cuando se decide la última candidata, el artículo sale de la lista y se abre el siguiente de abajo. Si era el último de la página, se abre el de arriba.
- La fila muestra "1 de 2 pend." cuando ya hay candidatas decididas. Si no hay ninguna decidida, sigue mostrando "N desc.".
- El score, el nivel y el orden de la fila se calculan sobre todas las candidatas del artículo, así el artículo no se mueve mientras se trabaja.

**Qué no cambió:** el tablero (avance, pendientes, descartadas, artículos Suizo = 10.018), el ranking y la papelera usan las mismas consultas de antes. Una candidata cuenta como decidida con el mismo criterio que usa el tablero: el par descripción de portal + código.

## 2. Monodroga

- **Fuente:** Fusion, `dbo.vsl_art_alfabeta_full.monodroga` (catálogo Alfabeta). Es la misma vista que usa Indicadores.
- **Cobertura:** los 10.018 artículos están en la vista, pero solo **2.637 (26,3%)** tienen la monodroga cargada:

| Negocio | Artículos | Con monodroga |
|---|---:|---:|
| Tratamientos Especiales | 2.385 | 2.130 (89%) |
| Medicamentos Hospitalarios | 2.888 | 434 (15%) |
| Accesorios e Insumos | 4.718 | 69 (1,5%) |
| Otros (Cardinal, Ambulatorio, Servicios) | 27 | 4 |

- La monodroga se muestra solo cuando está cargada. En los demás artículos no aparece nada y no se completa con otro dato.
- El campo `familia` del maestro de artículos no se usa: es la descripción genérica del producto, no la monodroga.
- Algunos valores de Alfabeta no son drogas (por ejemplo "accesorio" o "soluc.parent/fisiol/glucos/otras"). Se muestran tal cual vienen.
- **Acentos:** el puente PowerShell a Fusion rompe los acentos ("codeína" llega como "code�na"). También pasa en el caché de Indicadores. Para la monodroga se lee en hexadecimal y se decodifica como cp1252, así que llega bien ("interferón", "trastuzumab deruxtecán").
- **Dónde se guarda:** en la tabla chica `match_monodroga_map` (código → monodroga). Se genera con `scripts/rebuild_match_monodroga_map.py`, que solo hace SELECT sobre Fusion. Hay que regenerarla después de cada import de Match.
- Se ve debajo del nombre de cada artículo en la lista y en la cabecera del panel derecho.

## Verificación en el 8010

Se usó el usuario `admin@local`, con un artículo real (ENHERTU, cód. 6752681, 2 candidatas):

| Paso | Resultado |
|---|---|
| Homologar la 1ª | Sale de la lista, 1 tarjeta restante, aviso "Homologada ✓ Deshacer", botón "Ver decididas (1)", fila "1 de 2 pend." |
| Tablero | homologadas 0→1, pendientes 112.303→112.302, artículos Suizo 10.018 sin cambios, ranking con 1 usuario |
| Descartar la última | ENHERTU sale de la lista y se abre solo PEMBROX (el siguiente). La lista baja a 10.017 artículos y la papelera sube a 1 |
| Deshacer desde el aviso | ENHERTU vuelve a la lista y se reabre con 1 pendiente |
| Deshacer desde "Ver decididas" | Vuelven las 2 candidatas y la fila muestra otra vez "2 desc." |
| Estado final | Tablero, papelera (0) y lista (10.018) igual que al inicio. `match_homologaciones` idéntica a la de antes de la prueba |
| Monodroga | 16 de los 50 artículos de la página 1 la tienen. Con el filtro Tratamientos Especiales, 49 de 50 |
| Consola del navegador | Sin errores |

Las decisiones de prueba quedan registradas en la bitácora local (`match_homologacion_eventos`, que solo agrega filas). `push_match_data` no sube decisiones a prod.

**Rendimiento de `/articulos`** (local, 112k propuestas): la consulta tarda entre 670 y 1.280 ms, contra 580 a 1.060 ms de antes (+15–20% por el cruce con decisiones).

**Tests:** `tests/test_match_cola_pendientes.py` (nuevo, 5 tests) y `tests/test_match_dataset_refresh.py` pasan. En `tests/test_fusion_name_matching.py` fallan 2 tests, de cartera y visibilidad (`clientes_visibles_para`). No tienen relación con este cambio.

## Capturas

1. Lista con la monodroga debajo del nombre: ![](match_ui_20260925/01_lista_con_monodroga.png)
2. Cabecera del panel derecho con la monodroga: ![](match_ui_20260925/02_detalle_monodroga_en_cabecera.png)
3. Homologada: sale de la lista, aviso con Deshacer y "Ver decididas (1)": ![](match_ui_20260925/03_homologada_sale_aviso_deshacer.png)
4. Ver decididas: ![](match_ui_20260925/04_ver_decididas.png)
5. Artículo resuelto: sale de la izquierda y se abre el siguiente: ![](match_ui_20260925/05_articulo_resuelto_abre_siguiente.png)
6. Deshacer desde el aviso: el artículo vuelve: ![](match_ui_20260925/06_deshacer_desde_aviso_vuelve.png)
7. Deshacer desde "Ver decididas": ![](match_ui_20260925/07_deshacer_desde_decididas.png)
8. Filtro Tratamientos Especiales (49 de 50 con monodroga): ![](match_ui_20260925/08_tratamientos_especiales.png)

## Archivos

- `web_comparativas/match/models.py`: modelo `MatchMonodrogaMap`.
- `web_comparativas/match/service.py`: `listar_articulos` trae solo artículos con pendientes y suma `n_pendientes` y `monodroga`. `detalle_articulo` trae también las decididas (con `recuperable`) y la monodroga.
- `web_comparativas/templates/mercado_privado_match.html`: la interfaz.
- `scripts/rebuild_match_monodroga_map.py`: script nuevo.
- `web_comparativas/routers/match_router.py`: kind `monodroga-map` en `/admin/apply-data-chunk` y el conteo en `/admin/estado`.
- `scripts/push_match_data.py`: flag `--solo-monodroga`.
- `tests/test_match_cola_pendientes.py`: tests nuevos. También se actualizó el fixture de `tests/test_match_dataset_refresh.py`.

## Para llevarlo a prod

1. Deploy del código. La tabla `match_monodroga_map` se crea vacía en el arranque. Mientras esté vacía la monodroga no aparece y lo demás funciona igual.
2. Render no tiene acceso a Fusion, así que el mapa se sube desde local con `push_match_data --solo-monodroga` (kind `monodroga-map` en `/admin/apply-data-chunk`; `/admin/estado` muestra `match_monodroga_map`). Se probó contra el 8010 local: 2 lotes, 2.637 filas, acentos intactos y los otros mapas sin tocar. El push completo de Match no toca esta tabla.
3. El paso "regenerar y subir la monodroga después de cada import" está en `docs/RUNBOOK_prod_match_fase2_jul2026.md`, en la sección "Después de cada import de Match — monodroga".
