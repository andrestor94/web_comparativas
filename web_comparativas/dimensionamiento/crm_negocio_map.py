"""Mapa Negocio / Subnegocio del dataset de SIEM -> campos booleanos del CRM.

Contexto (pedido de Hugo, especificado por Matías): al crear la Oportunidad de Venta
en el CRM (Mercado Privado > Oportunidades), además de los campos que ya viajan hoy,
hay que encender el Negocio y el Subnegocio del artículo. En el CRM son **campos
booleanos independientes**: se mandan como string ``"1"`` para encender, y los que no
aplican se OMITEN del payload (nunca ``"0"``). Los nombres van SIEMPRE en minúscula.
Formato verificado en Postman contra el CRM de desarrollo (201 Created).

Los números entre paréntesis del spec (200/400/601/…) son códigos de rubro/línea de
FUSIÓN y NO existen en ningún campo del dataset de SIEM (`dimensionamiento_records`
sólo trae la etiqueta de texto en `unidad_negocio` / `subunidad_negocio`). Por eso el
mapeo es por TEXTO NORMALIZADO (sin acentos, minúscula, espacios colapsados) contra
una tabla estática, no por código.

Decisión (confirmada): el Negocio/Subnegocio se DERIVA del artículo. El modal de
envío sólo lo muestra para revisión y permite override manual; no es carga
obligatoria. Si una etiqueta no matchea la tabla (p. ej. una `unidad_negocio` nueva
en un run futuro), el envío NO se bloquea: viaja sin estos campos y queda registrado
(ver `resolver_negocio_subnegocio(...)["no_mapeado"]` y los logs en el motor / router).

⚠️ PROVISORIO — VA 407/408: en el dataset "Insumos medicos de Valor Agregado" y
"Farmacopea / galénicos de Valor Agregado" cuelgan de la unidad
"ACCESORIOS E INSUM MED-HOSPITALARIOS", pero el spec de Matías los lista bajo
`medicamentos_c`. Hasta que Matías confirme, se dejan colgando de
`accesorios_insumos_c` (lo que dice el dataset). Cuando responda: cambiar el
`.parent` de esas dos entradas en `_SUBNEGOCIOS` de "accesorios_insumos_c" a
"medicamentos_c" y listo (una línea cada una).
"""
from __future__ import annotations

import unicodedata
from typing import Any, NamedTuple


def _norm(texto: str | None) -> str:
    """Normaliza una etiqueta para el matcheo: sin acentos, minúscula, espacios
    colapsados. Conserva la puntuación (`.`, `/`, `-`, `,`) porque forma parte de
    algunas etiquetas del dataset ("Trat.esp. alto costo", "Cardio-thoracic")."""
    if not texto:
        return ""
    plano = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode("ascii")
    return " ".join(plano.lower().split())


class _Sub(NamedTuple):
    field: str      # campo booleano del CRM (minúscula, con sufijo _c)
    parent: str     # campo booleano del Negocio al que pertenece en el spec del CRM


# ── NEGOCIO: etiqueta normalizada de `unidad_negocio` -> campo booleano del CRM ──
# Sólo se usa como fallback cuando el Subnegocio NO matchea (ver resolver_...): si el
# subnegocio matchea, el Negocio sale de su `.parent`, que es la agrupación real del
# CRM (resuelve, p. ej., que "SERVICIOS HOSPITALARIOS" del dataset es gerenciamiento).
_NEGOCIOS: dict[str, str] = {
    "medicamentos hospitalarios": "medicamentos_c",
    "accesorios e insum med-hospitalarios": "accesorios_insumos_c",
    "tratamientos especiales": "tratamientos_especiales_c",
    "cardinal health": "cardinal_health_c",
    "servicios hospitalarios": "gerenciamiento_c",
    # No aparecen (todavía) como `unidad_negocio` en el dataset — se listan para dejar
    # explícito que existen del lado del CRM: "diagnosticorapido_c", "home_care_c",
    # "equipamiento_medico_c". Si un run futuro los trae, agregar la etiqueta acá.
}

# ── SUBNEGOCIO: etiqueta normalizada de `subunidad_negocio` -> (campo CRM, negocio) ──
_SUBNEGOCIOS: dict[str, _Sub] = {
    # Tratamientos especiales (200)
    "trat.esp. alto costo": _Sub("alto_costo_c", "tratamientos_especiales_c"),
    "uso compasivo": _Sub("uso_compasivo_c", "tratamientos_especiales_c"),
    "diabetes": _Sub("diabetes_c", "tratamientos_especiales_c"),
    "alimentos y suplementos especiales": _Sub("alimentos_suple_especiales_c", "tratamientos_especiales_c"),

    # Acces. e Insumos Med. (400)
    "insumos medico - hospitalarios": _Sub("insumos_medicos_401_c", "accesorios_insumos_c"),
    "accesorios": _Sub("accesorios_402_c", "accesorios_insumos_c"),
    "otros": _Sub("otros_403_c", "accesorios_insumos_c"),
    "farmacopea / galenicos / otros": _Sub("farmacopea_galenicos_otros_4_c", "accesorios_insumos_c"),
    "productos importados": _Sub("productos_importados_405_c", "accesorios_insumos_c"),
    "diagnostico": _Sub("diagnostico_406_c", "accesorios_insumos_c"),

    # Medicamentos (600)
    #   VA 407/408: PROVISORIO bajo accesorios_insumos_c (ver nota del módulo). El campo
    #   del subnegocio ya es el definitivo; sólo el `.parent` está pendiente de Matías.
    "insumos medicos de valor agregado": _Sub("insumos_medicos_va_407_c", "accesorios_insumos_c"),
    "farmacopea / galenicos de valor agregado": _Sub("farmacopea_galenicos_va_408_c", "accesorios_insumos_c"),
    "ampollas generales": _Sub("ampollas_generales_601_c", "medicamentos_c"),
    "anestesicos": _Sub("anestesicos_602_c", "medicamentos_c"),
    "antibioticos": _Sub("antibioticos_603_c", "medicamentos_c"),
    "comprimidos generales": _Sub("comprimidos_generales_604_c", "medicamentos_c"),
    "estupefacientes y psicotropicos": _Sub("estupefa_psicotropico_605_c", "medicamentos_c"),
    "hemoderivados": _Sub("hemoderivados_606_c", "medicamentos_c"),
    "medios de contraste": _Sub("medios_contraste_607_c", "medicamentos_c"),
    "nutricion enteral": _Sub("nutricion_enteral_608_c", "medicamentos_c"),
    "nutricion general": _Sub("nutricion_general_609_c", "medicamentos_c"),
    "psicofarmacos": _Sub("psicofarmacos_610_c", "medicamentos_c"),
    "soluciones parenterales": _Sub("soluciones_parentales_611_c", "medicamentos_c"),
    "suspensiones, gotas y otros": _Sub("suspenciones_gotas_otros_612_c", "medicamentos_c"),
    "nutricion enteral de valor agregado": _Sub("nutricion_enteral_va_613_c", "medicamentos_c"),
    "nutricion general de valor agregado": _Sub("nutricion_general_va_614_c", "medicamentos_c"),
    "diagnostico rapido": _Sub("diagnostico_rapido_409_c", "medicamentos_c"),

    # Equipamiento Médico (800)
    "mobiliario": _Sub("mobiliario_802_c", "equipamiento_medico_c"),
    "seca": _Sub("seca_805_c", "equipamiento_medico_c"),
    "baxter": _Sub("baxter_806_c", "equipamiento_medico_c"),

    # Gerenciamiento (900)
    "gerenciamiento de convenios especiales": _Sub("geren_conv_especiales_901_c", "gerenciamiento_c"),
    "gerenciamiento convenios hosp.": _Sub("geren_conv_hosp_902_c", "gerenciamiento_c"),
    "gerenciamiento convenios hospit. fisico": _Sub("geren_conv_hop_fisico_903_c", "gerenciamiento_c"),

    # Cardinal Health (1500)
    "cardinal health": _Sub("cardinal_health_1501_c", "cardinal_health_c"),
    "compression": _Sub("compression_1502_c", "cardinal_health_c"),
    "cardio-thoracic": _Sub("cardio_thoracic_1503_c", "cardinal_health_c"),
    "enteral feeding": _Sub("enteral_feeding_1504_c", "cardinal_health_c"),
    "electrodes": _Sub("electrodes_1505_c", "cardinal_health_c"),
    "perinatal": _Sub("perinatal_1507_c", "cardinal_health_c"),
    "suction gi": _Sub("suction_gi_1508_c", "cardinal_health_c"),
    "wound care": _Sub("wound_care_1509_c", "cardinal_health_c"),
}


def resolver_negocio_subnegocio(
    unidad_negocio: str | None,
    subunidad_negocio: str | None,
) -> dict[str, Any]:
    """Resuelve el Negocio y el Subnegocio del CRM para un artículo.

    Devuelve un dict con:
      - ``negocio_field`` / ``subnegocio_field``: nombre del campo booleano del CRM
        (o ``None`` si esa etiqueta no matcheó la tabla).
      - ``negocio_label`` / ``subnegocio_label``: la etiqueta cruda del dataset (para
        mostrarla en el modal).
      - ``campos``: SÓLO los campos que aplican, cada uno con valor ``"1"`` (string).
        Es lo que se mergea al payload del CRM. Vacío si nada matcheó.
      - ``sin_mapear``: lista de ``(nivel, etiqueta_cruda)`` para los niveles que no
        matchearon (nivel ∈ {"negocio", "subnegocio"}).
      - ``no_mapeado``: ``True`` si ``sin_mapear`` no está vacío.

    Cuando el Subnegocio matchea, el Negocio se toma de su agrupación en el spec del
    CRM (``_Sub.parent``), no de la etiqueta ``unidad_negocio`` — así se resuelven los
    desajustes de nombre del dataset (p. ej. "SERVICIOS HOSPITALARIOS" -> gerenciamiento).
    """
    sub = _SUBNEGOCIOS.get(_norm(subunidad_negocio))
    negocio_field = _NEGOCIOS.get(_norm(unidad_negocio))
    subnegocio_field = sub.field if sub else None
    if sub:
        negocio_field = sub.parent  # la agrupación del CRM manda

    campos: dict[str, str] = {}
    if negocio_field:
        campos[negocio_field] = "1"
    if subnegocio_field:
        campos[subnegocio_field] = "1"

    sin_mapear: list[tuple[str, str]] = []
    if (unidad_negocio or "").strip() and not negocio_field:
        sin_mapear.append(("negocio", unidad_negocio))
    if (subunidad_negocio or "").strip() and not subnegocio_field:
        sin_mapear.append(("subnegocio", subunidad_negocio))

    return {
        "negocio_field": negocio_field,
        "subnegocio_field": subnegocio_field,
        "negocio_label": unidad_negocio or None,
        "subnegocio_label": subunidad_negocio or None,
        "campos": campos,
        "sin_mapear": sin_mapear,
        "no_mapeado": bool(sin_mapear),
    }


def campos_crm_negocio(
    unidad_negocio: str | None,
    subunidad_negocio: str | None,
) -> dict[str, str]:
    """Atajo: sólo el dict ``{campo_crm: "1"}`` listo para mergear al payload."""
    return resolver_negocio_subnegocio(unidad_negocio, subunidad_negocio)["campos"]
