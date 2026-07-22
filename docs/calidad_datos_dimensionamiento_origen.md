# Dimensionamiento — problemas de calidad de datos a corregir en el origen

Detectamos varios problemas en el dataset unificado (Bionexo / Portada / Medox) que
obligan al sistema a "adivinar" qué filas corresponden a la misma entidad. Los listamos
abajo con ejemplos concretos para que puedan corregirse desde la generación del archivo.
Corregir esto en el origen mejora la exactitud del tablero y evita reprocesos.

## 1. La misma entidad aparece homologada en unas filas y sin homologar en otras
Es el problema más importante. Una misma institución entra por varias plataformas y solo
en algunas quedó con su nombre homologado; en las otras figura el nombre "original" tal
cual vino, y sin identificar. Eso hace que la misma entidad se cuente dos veces: una como
"cliente" y otra como "no cliente".

Ejemplos verificados:
- **Clínica Pergamino S.A.** — homologada y con CUIT en Portada, pero en Bionexo aparece
  como texto suelto, sin homologar y sin CUIT.
- **Clínica Roca S.A.** — homologada en Medox (con CUIT), sin homologar en Bionexo.
- **DASPU** — miles de filas homologadas y decenas sin homologar, bajo el mismo nombre.

**Pedido:** completar la homologación a nivel de fila en TODAS las plataformas, de modo que
todas las filas de una misma entidad lleguen con el mismo nombre homologado y su CUIT.

## 2. Nombres homologados cortados (truncados)
Hay nombres homologados que llegan cortados a una longitud fija, lo que impide reconocer la
entidad completa: **36 nombres cortados a 40 caracteres** y **12 cortados a 30 caracteres**.

Ejemplo: `SOCIEDAD ESPAÑOLA DE BENEFICEN` y `SOCIEDAD ESPAÑOLA DE BENEFICENCIA Y MUTU`
son la misma entidad, pero el primero llegó truncado.

**Pedido:** exportar el nombre homologado completo, sin límite de caracteres.

## 3. Un nombre con la "Ñ" corrompida
Al menos un nombre llegó con la letra Ñ reemplazada por el símbolo `#`:
`CLINICA GENERAL OBST Y CIR NUESTRA SE#OR` (debería decir "NUESTRA SEÑORA…").
Es un problema de codificación de caracteres (encoding) al generar el archivo.

**Pedido:** exportar en UTF-8 conservando acentos y la Ñ.

## 4. Registros de prueba mezclados con datos reales (Medox)
Aparecen filas de testeo mezcladas con datos productivos, con nombres como
`OOSS prueba 1`, `OOSS prueba 2`, `hospital prueba 1`. Además algunas de esas filas de
prueba quedaron asociadas a un CUIT real, lo que las mezcla con entidades legítimas.

**Pedido:** no incluir registros de prueba/test/demo en el dataset productivo.

## 5. CUIT ausente en una parte de las filas
Una porción de las filas llega sin CUIT (`SIN DATO`). El CUIT es el dato más confiable para
identificar unívocamente a cada entidad; sin él, la identificación depende del nombre, que
es mucho más frágil (ver puntos 1 a 3).

**Pedido:** completar el CUIT en la mayor cantidad de filas posible, priorizando las
entidades que hoy llegan sin homologar.

---
**Impacto de corregir esto:** el conteo de entidades/clientes del tablero deja de depender
de heurísticas y pasa a ser exacto y directo, y desaparecen los duplicados por plataforma.
