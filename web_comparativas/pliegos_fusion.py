"""
pliegos_fusion.py
Validación Fusion independiente del campo listo_para_fusion de LicIA.
Implementa la matriz de campos obligatorios de Verónica y calcula el estado_fusion
internamente, sin confiar en Control_Carga ni Fusion_Cabecera ciegamente.
"""
from __future__ import annotations

import io
import re
import unicodedata
import datetime as dt
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Marcadores de valor faltante — nunca deben tratarse como dato válido
# ---------------------------------------------------------------------------
FUSION_MISSING_MARKERS = {
    "", "-", "--", "n/a", "na", "nan", "none",
    "no encontrado", "no encontrada", "no identificado", "no identificada",
    "sin dato", "sin datos", "sin dato en pliego", "sin informacion", "sin información",
    "pendiente", "a definir", "a confirmar", "por definir", "por confirmar",
    "no aplica", "no disponible", "no informado",
}


def _norm(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    nfkd = unicodedata.normalize("NFKD", text)
    return re.sub(r"\s+", " ", "".join(ch for ch in nfkd if not unicodedata.combining(ch))).strip().lower()


def _safe(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s.lower() in {"nan", "none"} else s


def is_fusion_missing(value) -> bool:
    """True cuando el valor debe considerarse faltante para Fusion."""
    return _norm(value) in FUSION_MISSING_MARKERS


def _pick(*values) -> str:
    """Devuelve el primer valor no-faltante de la lista de candidatos."""
    for v in values:
        s = _safe(v)
        if s and not is_fusion_missing(s):
            return s
    return ""


def _build_licia_texto(estado_licia: dict) -> str:
    """Construye texto legible del estado informado por el análisis automático."""
    if not estado_licia or not isinstance(estado_licia, dict):
        return ""

    def _yn(val) -> str:
        n = _norm(val)
        if n in {"si", "sí", "yes", "true", "1"}:
            return "Sí"
        if n in {"no", "false", "0"}:
            return "No"
        return ""

    parts = []

    listo = _yn(estado_licia.get("listo_para_fusion", ""))
    if listo:
        parts.append(f"Listo: {listo}")

    nivel = _safe(estado_licia.get("nivel_riesgo", ""))
    if nivel:
        parts.append(f"Riesgo: {nivel.title()}")

    rev = _yn(estado_licia.get("requiere_revision_analista", ""))
    if rev == "Sí":
        parts.append("Requiere revisión")

    motivo = _safe(estado_licia.get("motivo_estado", ""))
    if motivo and len(motivo) < 60:
        parts.append(motivo)

    return " · ".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# Catálogos de normalización para Fusion
# ---------------------------------------------------------------------------
PROCEDIMIENTO_CATALOGO = {
    "licitacion publica": "Licitación Pública",
    "licitacion privada": "Licitación Privada",
    "concurso publico": "Concurso Público",
    "concurso privado": "Concurso Privado",
    "contratacion directa": "Contratación Directa",
    "contratacion por cotejo": "Cotejo",
    "cotejo de precios": "Cotejo",
    "cotejo": "Cotejo",
    "licitacion": "Licitación Pública",
    "concurso": "Concurso Público",
}

MODALIDAD_CATALOGO = {
    "sin modalidad": "Sin Modalidad",
    "convenio marco": "Convenio Marco",
    "compra consolidada": "Compra Consolidada",
    "iniciativa privada": "Iniciativa Privada",
    "subasta inversa": "Subasta Inversa",
    "sin especificar": "Sin Especificar",
}

TIPO_COTIZACION_CATALOGO = {
    "total": "Total",
    "parcial": "Parcial",
    "por renglon": "Por Renglón",
    "por renglón": "Por Renglón",
    "global": "Global",
}

TIPO_ADJUDICACION_CATALOGO = {
    "total": "Total",
    "parcial": "Parcial",
    "por renglon": "Por Renglón",
    "por renglón": "Por Renglón",
}


def _normalize_catalog(value: str, catalog: dict, field_key: str) -> dict:
    norm = _norm(value)
    # Buscar coincidencia exacta
    if norm in catalog:
        return {"valor_normalizado": catalog[norm], "requiere_validacion": False}
    # Buscar si el valor contiene alguna clave
    for key, mapped in catalog.items():
        if key in norm:
            return {"valor_normalizado": mapped, "requiere_validacion": False}
    # Sin coincidencia: marcar para revisión manual
    return {"valor_normalizado": value, "requiere_validacion": True}


# ---------------------------------------------------------------------------
# Resolución de campos Fusion con prioridad y fallback
# ---------------------------------------------------------------------------

def _get_fusion_cabecera(caso) -> dict:
    if caso.fusion_cabecera and caso.fusion_cabecera.datos:
        return {k.lower().strip(): v for k, v in caso.fusion_cabecera.datos.items()}
    return {}


def _get_proceso(caso) -> dict:
    if caso.datos_proceso and caso.datos_proceso.datos:
        return {k.lower().strip(): v for k, v in caso.datos_proceso.datos.items()}
    return {}


def _get_control_carga(caso) -> dict:
    if caso.control_carga and caso.control_carga.datos:
        datos = caso.control_carga.datos
        if isinstance(datos, list) and datos:
            return {k.lower().strip(): v for k, v in datos[0].items()}
        if isinstance(datos, dict):
            return {k.lower().strip(): v for k, v in datos.items()}
    return {}


def _from_proceso(proceso: dict, *keys) -> str:
    for key in keys:
        for k, v in proceso.items():
            if k == key.lower() or k.replace(" ", "_") == key.lower() or k.replace("_", " ") == key.lower():
                val = _safe(v)
                if val and not is_fusion_missing(val):
                    return val
    return ""


def _from_trazabilidad(caso, *campos) -> str:
    campos_norm = [_norm(c) for c in campos]
    for t in (caso.trazabilidad or []):
        campo_n = _norm(t.campo)
        if any(campo_n == cn or campo_n.startswith(cn) or cn.startswith(campo_n) for cn in campos_norm if cn):
            val = _safe(t.valor_extraido)
            if val and not is_fusion_missing(val):
                return val
    return ""


def _from_cronograma(caso, *palabras_clave) -> str:
    palabras = [_norm(p) for p in palabras_clave]
    for hito in (caso.cronograma or []):
        hito_norm = _norm(hito.hito)
        if any(p in hito_norm for p in palabras):
            fecha = _safe(hito.fecha)
            hora = _safe(hito.hora)
            if fecha and not is_fusion_missing(fecha):
                return f"{fecha} {hora}".strip() if hora and not is_fusion_missing(hora) else fecha
    return ""


def _from_cronograma_campo(caso, *palabras_clave, campo: str = "fecha") -> str:
    palabras = [_norm(p) for p in palabras_clave]
    for hito in (caso.cronograma or []):
        hito_norm = _norm(hito.hito)
        if any(p in hito_norm for p in palabras):
            if campo == "hito":
                return _safe(hito.hito)
            elif campo == "fecha":
                val = _safe(hito.fecha)
                return val if val and not is_fusion_missing(val) else ""
    return ""


def _from_analitica(caso, *campos_relacionados) -> str:
    analitica = caso.analitica
    if not analitica:
        return ""
    datos = analitica.datos
    if not datos:
        return ""
    rows = datos if isinstance(datos, list) else []
    campos_norm = [_norm(c) for c in campos_relacionados]
    for row in rows:
        if not isinstance(row, dict):
            continue
        campo_rel = _norm(row.get("campo_relacionado", ""))
        if any(campo_rel == cn or cn in campo_rel for cn in campos_norm if cn):
            val = _safe(row.get("valor_analitico", ""))
            if val and not is_fusion_missing(val):
                return val
    return ""


def _from_hallazgos(caso, *campos_dashboard) -> str:
    campos_norm = [_norm(c) for c in campos_dashboard]
    for h in (caso.hallazgos or []):
        extra = getattr(h, "datos_extra", {}) or {}
        campo_dash = _norm(extra.get("campo_dashboard_sugerido", ""))
        if any(campo_dash == cn or cn in campo_dash for cn in campos_norm if cn):
            val = _safe(extra.get("valor_sugerido", ""))
            if val and not is_fusion_missing(val):
                return val
    return ""


# ---------------------------------------------------------------------------
# Resolvedor por campo Fusion
# ---------------------------------------------------------------------------

def _resolver_campo_fusion(caso, field_key: str, fc: dict, proc: dict) -> dict:
    """
    Resuelve un campo Fusion usando la cadena de prioridad definida por Verónica.
    Retorna dict con: valor, fuente, requiere_validacion, valor_normalizado
    """
    valor = ""
    fuente = ""
    valor_normalizado = ""
    requiere_validacion = False

    if field_key == "unidad_ejecutora":
        valor = _pick(
            fc.get("unidad_ejecutora_fusion"),
            _from_proceso(proc, "unidad_operativa"),
            _from_proceso(proc, "organismo_contratante"),
            _from_trazabilidad(caso, "unidad_operativa", "unidad_ejecutora"),
        )
        fuente = "Fusion_Cabecera" if fc.get("unidad_ejecutora_fusion") and not is_fusion_missing(fc["unidad_ejecutora_fusion"]) else "Proceso"

    elif field_key == "numero_proceso":
        valor = _pick(
            fc.get("numero_proceso_fusion"),
            _from_proceso(proc, "numero_proceso"),
            _from_trazabilidad(caso, "numero_proceso", "nro_proceso"),
        )
        fuente = "Fusion_Cabecera" if fc.get("numero_proceso_fusion") and not is_fusion_missing(fc.get("numero_proceso_fusion", "")) else "Proceso"

    elif field_key == "nombre_proceso":
        valor = _pick(
            fc.get("nombre_proceso_fusion"),
            _from_proceso(proc, "nombre_proceso"),
        )
        fuente = "Fusion_Cabecera" if fc.get("nombre_proceso_fusion") and not is_fusion_missing(fc.get("nombre_proceso_fusion", "")) else "Proceso"

    elif field_key == "procedimiento_seleccion":
        raw = _pick(
            _from_proceso(proc, "tipo_proceso"),
            _from_trazabilidad(caso, "procedimiento_seleccion", "tipo_proceso"),
        )
        if raw:
            norm_result = _normalize_catalog(raw, PROCEDIMIENTO_CATALOGO, field_key)
            valor = raw
            valor_normalizado = norm_result["valor_normalizado"]
            requiere_validacion = norm_result["requiere_validacion"]
            fuente = "Proceso"

    elif field_key == "objeto_contratacion":
        valor = _pick(
            fc.get("objeto_proceso_fusion"),
            _from_proceso(proc, "objeto_contratacion"),
            _from_trazabilidad(caso, "objeto_contratacion"),
        )
        fuente = "Fusion_Cabecera" if fc.get("objeto_proceso_fusion") and not is_fusion_missing(fc.get("objeto_proceso_fusion", "")) else "Proceso"

    elif field_key == "fecha_apertura":
        valor = _pick(
            fc.get("fecha_apertura_fusion"),
            _from_proceso(proc, "fecha_apertura"),
            _from_cronograma(caso, "apertura"),
            _from_trazabilidad(caso, "fecha_apertura"),
        )
        fuente = "Fusion_Cabecera" if fc.get("fecha_apertura_fusion") and not is_fusion_missing(fc.get("fecha_apertura_fusion", "")) else ("Cronograma" if not _from_proceso(proc, "fecha_apertura") else "Proceso")

    elif field_key == "tipo_cotizacion":
        raw = _pick(
            fc.get("tipo_cotizacion_fusion"),
            _from_proceso(proc, "tipo_cotizacion"),
            _from_trazabilidad(caso, "tipo_cotizacion"),
        )
        if raw:
            norm_result = _normalize_catalog(raw, TIPO_COTIZACION_CATALOGO, field_key)
            valor = raw
            valor_normalizado = norm_result["valor_normalizado"]
            requiere_validacion = norm_result["requiere_validacion"]
            fuente = "Fusion_Cabecera" if fc.get("tipo_cotizacion_fusion") and not is_fusion_missing(fc.get("tipo_cotizacion_fusion", "")) else "Proceso"

    elif field_key == "tipo_adjudicacion":
        raw = _pick(
            _from_proceso(proc, "tipo_adjudicacion"),
            _from_analitica(caso, "tipo_adjudicacion"),
            _from_hallazgos(caso, "tipo_adjudicacion"),
        )
        if raw:
            norm_result = _normalize_catalog(raw, TIPO_ADJUDICACION_CATALOGO, field_key)
            valor = raw
            valor_normalizado = norm_result["valor_normalizado"]
            requiere_validacion = norm_result["requiere_validacion"]
            fuente = "Proceso"

    elif field_key == "cant_oferta_permitidas":
        # Buscar con múltiples aliases
        proc_val = ""
        for alias in ["cant_oferta_permitidas", "cantidad_ofertas_permitidas", "acepta_mas_de_una_oferta",
                      "oferta_permitida", "cantidad_ofertas", "ofertas_permitidas", "cant ofertas", "ofertas"]:
            proc_val = _from_proceso(proc, alias)
            if proc_val:
                break
        if not proc_val:
            proc_val = _from_trazabilidad(caso, "cant_oferta_permitidas", "cantidad_ofertas_permitidas", "ofertas_permitidas")
        valor = proc_val
        fuente = "Proceso" if proc_val else ""

    elif field_key == "plazo_mantenimiento_oferta":
        valor = _pick(
            _from_proceso(proc, "plazo_mantenimiento_oferta"),
            _from_cronograma(caso, "mantenimiento de oferta", "mantenimiento oferta"),
            _from_trazabilidad(caso, "plazo_mantenimiento_oferta", "mantenimiento_oferta"),
        )
        fuente = "Proceso"

    elif field_key == "acepta_redeterminacion":
        raw = _pick(
            _from_proceso(proc, "acepta_redeterminacion"),
            _from_trazabilidad(caso, "acepta_redeterminacion"),
        )
        # Normalizar booleano
        if raw:
            n = _norm(raw)
            token = re.split(r"[\s,;:/()]+", n)[0] if n else ""
            if token in {"si", "s", "yes", "true", "1"}:
                valor = "Sí"
            elif token in {"no", "false", "0"}:
                valor = "No"
            else:
                valor = raw
                requiere_validacion = True
            fuente = "Proceso"

    elif field_key == "fecha_inicio_consulta":
        valor = _pick(
            _from_cronograma(caso, "inicio consulta", "inicio de consulta", "apertura consulta", "inicio consultas"),
            _from_proceso(proc, "fecha_inicio_consulta", "inicio_consultas", "inicio_consulta"),
            _from_trazabilidad(caso, "fecha_inicio_consulta", "inicio_consultas", "inicio_consulta"),
        )
        fuente = "Cronograma" if valor and _from_cronograma(caso, "inicio consulta", "inicio de consulta", "apertura consulta", "inicio consultas") else "Proceso"

    elif field_key == "fecha_final_consulta":
        valor = _pick(
            _from_cronograma(caso, "cierre consulta", "fin consulta", "final consulta", "limite consulta", "cierre de consulta", "fin de consulta"),
            _from_proceso(proc, "fecha_final_consulta", "cierre_consultas", "fin_consulta"),
            _from_trazabilidad(caso, "fecha_final_consulta", "cierre_consultas", "limite_consultas"),
        )
        fuente = "Cronograma" if valor and _from_cronograma(caso, "cierre consulta", "fin consulta", "final consulta", "limite consulta") else "Proceso"

    elif field_key == "monto":
        raw = _pick(
            _from_proceso(proc, "presupuesto_oficial"),
            _from_trazabilidad(caso, "presupuesto_oficial", "monto"),
            _from_analitica(caso, "presupuesto_oficial", "monto"),
        )
        valor = raw
        fuente = "Proceso"

    elif field_key == "moneda":
        valor = _pick(
            fc.get("moneda_fusion"),
            _from_proceso(proc, "moneda"),
            _from_trazabilidad(caso, "moneda"),
        )
        fuente = "Fusion_Cabecera" if fc.get("moneda_fusion") and not is_fusion_missing(fc.get("moneda_fusion", "")) else "Proceso"

    elif field_key == "duracion_contrato":
        valor = _pick(
            _from_proceso(proc, "duracion_contrato"),
            _from_trazabilidad(caso, "duracion_contrato"),
        )
        fuente = "Proceso"

    elif field_key == "acepta_prorroga":
        raw = _pick(
            _from_proceso(proc, "acepta_prorroga"),
            _from_trazabilidad(caso, "acepta_prorroga"),
        )
        if raw:
            n = _norm(raw)
            token = re.split(r"[\s,;:/()]+", n)[0] if n else ""
            if token in {"si", "s", "yes", "true", "1"}:
                valor = "Sí"
            elif token in {"no", "false", "0"}:
                valor = "No"
            else:
                valor = raw
                requiere_validacion = True
            fuente = "Proceso"

    elif field_key == "expediente":
        valor = _pick(
            fc.get("expediente_fusion"),
            _from_proceso(proc, "expediente"),
            _from_trazabilidad(caso, "expediente"),
        )
        fuente = "Fusion_Cabecera" if fc.get("expediente_fusion") and not is_fusion_missing(fc.get("expediente_fusion", "")) else "Proceso"

    elif field_key == "modalidad":
        raw = _pick(
            fc.get("modalidad_fusion"),
            _from_proceso(proc, "modalidad"),
            _from_trazabilidad(caso, "modalidad"),
        )
        if raw:
            norm_result = _normalize_catalog(raw, MODALIDAD_CATALOGO, field_key)
            valor = raw
            valor_normalizado = norm_result["valor_normalizado"]
            requiere_validacion = norm_result["requiere_validacion"]
            fuente = "Fusion_Cabecera" if fc.get("modalidad_fusion") and not is_fusion_missing(fc.get("modalidad_fusion", "")) else "Proceso"

    return {
        "valor": valor,
        "fuente": fuente,
        "valor_normalizado": valor_normalizado or valor,
        "requiere_validacion": requiere_validacion,
        "completo": bool(valor and not is_fusion_missing(valor)),
    }


# ---------------------------------------------------------------------------
# Campos obligatorios Fusion (matriz de Verónica)
# ---------------------------------------------------------------------------
FUSION_CAMPOS_OBLIGATORIOS = [
    {"key": "unidad_ejecutora",         "label": "Unidad Ejecutora",             "categoria": "fusion_obligatorio"},
    {"key": "numero_proceso",           "label": "N° Proceso",                   "categoria": "fusion_obligatorio"},
    {"key": "nombre_proceso",           "label": "Nombre Proceso",               "categoria": "fusion_obligatorio"},
    {"key": "procedimiento_seleccion",  "label": "Procedimiento Selección",      "categoria": "fusion_obligatorio"},
    {"key": "objeto_contratacion",      "label": "Objeto Contratación",          "categoria": "fusion_obligatorio"},
    {"key": "fecha_apertura",           "label": "Fecha Acto Apertura",          "categoria": "fusion_obligatorio"},
    {"key": "tipo_cotizacion",          "label": "Tipo Cotización",              "categoria": "fusion_obligatorio"},
    {"key": "tipo_adjudicacion",        "label": "Tipo Adjudicación",            "categoria": "fusion_obligatorio"},
    {"key": "cant_oferta_permitidas",   "label": "Cant. Oferta Permitidas",      "categoria": "fusion_obligatorio"},
    {"key": "plazo_mantenimiento_oferta","label": "Plazo Mantenimiento Oferta", "categoria": "fusion_obligatorio"},
    {"key": "acepta_redeterminacion",   "label": "Acepta Redeterminación",       "categoria": "fusion_obligatorio"},
    {"key": "fecha_inicio_consulta",    "label": "Fecha Inicio Consulta",        "categoria": "fusion_obligatorio"},
    {"key": "fecha_final_consulta",     "label": "Fecha Final Consulta",         "categoria": "fusion_obligatorio"},
    {"key": "monto",                    "label": "Monto",                        "categoria": "fusion_obligatorio"},
    {"key": "moneda",                   "label": "Moneda",                       "categoria": "fusion_obligatorio"},
    {"key": "duracion_contrato",        "label": "Duración Contrato",            "categoria": "fusion_obligatorio"},
    {"key": "acepta_prorroga",          "label": "Acepta Prórroga",              "categoria": "fusion_obligatorio"},
    {"key": "expediente",               "label": "Expediente",                   "categoria": "fusion_obligatorio"},
    {"key": "modalidad",                "label": "Modalidad",                    "categoria": "fusion_obligatorio"},
]

FUSION_CAMPOS_COMPLEMENTARIOS = [
    {"key": "ampliacion",               "label": "Ampliación",                   "categoria": "pliego_complementario"},
    {"key": "alternativa",              "label": "Alternativa",                  "categoria": "pliego_complementario"},
    {"key": "lugar_entrega",            "label": "Lugar y Cond. de Entrega",     "categoria": "pliego_complementario"},
    {"key": "garantia_oferta",          "label": "Garantía de Mant. de Oferta", "categoria": "pliego_complementario"},
    {"key": "garantia_anticipo",        "label": "Garantía de Anticipo",         "categoria": "pliego_complementario"},
    {"key": "garantia_cumplimiento",    "label": "Garantía de Cumplimiento",     "categoria": "pliego_complementario"},
    {"key": "garantia_impugnacion_pliego","label":"Garantía de Impug. Pliego",   "categoria": "pliego_complementario"},
    {"key": "garantia_impugnacion_preadj","label":"Garantía de Impug. Preadj.", "categoria": "pliego_complementario"},
    {"key": "garantia_contragarantia",  "label": "Garantía - Contragarantía",    "categoria": "pliego_complementario"},
    {"key": "exigencia_desestimacion",  "label": "Exigencia c/ Causal Desestimación","categoria": "pliego_complementario"},
    {"key": "lleva_muestras",           "label": "¿Lleva Muestras?",            "categoria": "pliego_complementario"},
    {"key": "fecha_limite_muestras",    "label": "Fecha Límite Muestras",        "categoria": "pliego_complementario"},
]

FUSION_CAMPOS_NO_OBLIGATORIOS = [
    {"key": "obj_gasto",                "label": "Obj. Gas.",                    "categoria": "no_obligatorio"},
    {"key": "codigo_item",              "label": "Cod. Item",                    "categoria": "no_obligatorio"},
]

FUSION_EXPORT_PROCESO_COLUMNS = [
    "Unidad Ejecutora",
    "N° Procesos",
    "Nombre Proceso",
    "Procedimiento Selección",
    "Objeto Contratación",
    "Fecha Acto Apertura",
    "Tipo Cotización",
    "Tipo Adjudicación",
    "Cant. Oferta Permitidas",
    "Plazo Mantenimiento Oferta",
    "Acepta Redeterminación",
    "Fecha Inicio Consulta",
    "Fecha Final Consulta",
    "Monto",
    "Moneda",
    "Duración Contrato",
    "Acepta Prórroga",
    "Expediente",
    "Modalidad",
    "Ampliación",
    "Alternativa",
    "Lugar y Cond. de Entrega",
    "Garantía de Mantenimiento de Oferta",
    "Garantía de Anticipo Financiero",
    "Garantía de Cumplimiento de Contrato",
    "Garantía de Impugnación al Pliego",
    "Garantía de Impugnación a la Preadjudicación",
    "Garantía de Incorporar Contragarantía",
    "Exigencia con Causal de Desestimación",
    "¿Lleva Muestras?",
    "Fecha limite para presentación de muestras",
]

FUSION_EXPORT_RENGLON_COLUMNS = [
    "Item",
    "Obj. Gas.",
    "Cod. Item",
    "Descripción",
    "Cant",
]

_FUSION_PROCESS_FIELD_BY_COLUMN = {
    "Unidad Ejecutora": "unidad_ejecutora",
    "N° Procesos": "numero_proceso",
    "Nombre Proceso": "nombre_proceso",
    "Procedimiento Selección": "procedimiento_seleccion",
    "Objeto Contratación": "objeto_contratacion",
    "Fecha Acto Apertura": "fecha_apertura",
    "Tipo Cotización": "tipo_cotizacion",
    "Tipo Adjudicación": "tipo_adjudicacion",
    "Cant. Oferta Permitidas": "cant_oferta_permitidas",
    "Plazo Mantenimiento Oferta": "plazo_mantenimiento_oferta",
    "Acepta Redeterminación": "acepta_redeterminacion",
    "Fecha Inicio Consulta": "fecha_inicio_consulta",
    "Fecha Final Consulta": "fecha_final_consulta",
    "Monto": "monto",
    "Moneda": "moneda",
    "Duración Contrato": "duracion_contrato",
    "Acepta Prórroga": "acepta_prorroga",
    "Expediente": "expediente",
    "Modalidad": "modalidad",
}


# ---------------------------------------------------------------------------
# Cálculo de renglones Fusion
# ---------------------------------------------------------------------------

def _resolver_renglones_fusion(caso) -> list[dict]:
    """
    Prioriza Fusion_Renglones; enriquece con datos de Renglones cuando falte fuente/observaciones.
    Nunca devuelve lista vacía si hay renglones en alguna hoja.
    """
    renglones_out = []

    # Índice de renglones por orden para enriquecimiento
    renglones_idx: dict[int, object] = {}
    for r in (caso.renglones or []):
        renglones_idx[r.orden] = r

    fusion_renglones = caso.fusion_renglones or []
    renglones_base = caso.renglones or []

    # Preferir Fusion_Renglones si existen
    fuente_principal = fusion_renglones if fusion_renglones else renglones_base

    for idx, fr in enumerate(fuente_principal):
        if fusion_renglones:
            orden_str = _safe(fr.numero_renglon) or str(idx + 1)
            try:
                orden_int = int(float(orden_str))
            except (ValueError, TypeError):
                orden_int = idx + 1

            # Enriquecer con hoja Renglones si existe
            renglon_base = renglones_idx.get(orden_int)
            extra = fr.datos_extra or {}

            row = {
                "item": orden_str,
                "obj_gasto": _safe(extra.get("obj_gasto", "")) or _safe(extra.get("objeto_gasto", "")) or _safe(extra.get("obj_gas", "")),
                "codigo_item": _safe(fr.codigo_item),
                "descripcion": _safe(fr.descripcion),
                "cantidad": _safe(fr.cantidad),
                "unidad": _safe(fr.unidad),
                "precio_unitario": _safe(fr.precio_unitario_estimado) or _safe(extra.get("precio_unitario", "")),
                "importe_total": _safe(extra.get("importe_total", "")),
                "lugar_entrega": _safe(extra.get("lugar_entrega", "")),
                "plazo_entrega": _safe(extra.get("plazo_entrega", "")),
                "periodicidad": _safe(extra.get("periodicidad", "")),
                "marca": _safe(extra.get("marca", "")),
                "destino_efector": _safe(extra.get("destino_efector", "")),
                "observaciones": _safe(extra.get("observaciones", "")),
                "fuente": _safe(extra.get("fuente", "Fusion_Renglones")),
                "estado": _safe(extra.get("estado", "")),
            }
            # Enriquecer fuente/observaciones desde hoja Renglones
            if renglon_base:
                rb_extra = renglon_base.datos_extra or {}
                if not row["fuente"]:
                    row["fuente"] = _safe(rb_extra.get("fuente", "Renglones"))
                if not row["observaciones"]:
                    row["observaciones"] = _safe(renglon_base.obs_tecnicas)
                if not row["lugar_entrega"]:
                    row["lugar_entrega"] = _safe(rb_extra.get("lugar_entrega", ""))
                if not row["plazo_entrega"]:
                    row["plazo_entrega"] = _safe(rb_extra.get("plazo_entrega", ""))
                if not row["periodicidad"]:
                    row["periodicidad"] = _safe(rb_extra.get("periodicidad", ""))
                if not row["destino_efector"]:
                    row["destino_efector"] = _safe(renglon_base.destino_efector)
        else:
            # Solo hoja Renglones
            extra = fr.datos_extra or {}
            row = {
                "item": _safe(fr.orden) or str(idx + 1),
                "obj_gasto": _safe(extra.get("obj_gasto", "")) or _safe(extra.get("objeto_gasto", "")) or _safe(extra.get("obj_gas", "")),
                "codigo_item": _safe(fr.codigo_item),
                "descripcion": _safe(fr.descripcion),
                "cantidad": _safe(fr.cantidad),
                "unidad": _safe(fr.unidad),
                "precio_unitario": _safe(extra.get("precio_unitario", "")),
                "importe_total": _safe(extra.get("importe_total", "")),
                "lugar_entrega": _safe(extra.get("lugar_entrega", "")),
                "plazo_entrega": _safe(extra.get("plazo_entrega", "")),
                "periodicidad": _safe(extra.get("periodicidad", "")),
                "marca": _safe(extra.get("marca", "")),
                "destino_efector": _safe(fr.destino_efector),
                "observaciones": _safe(fr.obs_tecnicas),
                "fuente": _safe(extra.get("fuente", "Renglones")),
                "estado": _safe(fr.estado),
            }

        if row["descripcion"] or row["item"]:
            renglones_out.append(row)

    return renglones_out


# ---------------------------------------------------------------------------
# Función principal: calcular_estado_fusion
# ---------------------------------------------------------------------------

def _clean_export_value(value) -> str:
    text = _safe(value)
    return "" if is_fusion_missing(text) else text


def _format_export_date(value) -> str:
    text = _clean_export_value(value)
    if not text:
        return ""
    if isinstance(value, dt.datetime):
        return value.strftime("%d/%m/%Y %H:%M") if (value.hour or value.minute) else value.strftime("%d/%m/%Y")
    if isinstance(value, dt.date):
        return value.strftime("%d/%m/%Y")

    try:
        parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    except Exception:
        parsed = None
    if parsed is not None and not pd.isna(parsed):
        has_time = bool(getattr(parsed, "hour", 0) or getattr(parsed, "minute", 0))
        return parsed.strftime("%d/%m/%Y %H:%M") if has_time else parsed.strftime("%d/%m/%Y")
    return text


def _format_export_bool(value) -> str:
    text = _clean_export_value(value)
    if not text:
        return ""
    norm = _norm(text)
    token = re.split(r"[\s,;:/()]+", norm)[0] if norm else ""
    if token in {"si", "s", "yes", "true", "1"}:
        return "Sí"
    if token in {"no", "false", "0"}:
        return "No"
    return text


def _format_export_value(column: str, value) -> str:
    if column in {
        "Fecha Acto Apertura",
        "Fecha Inicio Consulta",
        "Fecha Final Consulta",
        "Fecha limite para presentación de muestras",
    }:
        return _format_export_date(value)
    if column in {
        "Acepta Redeterminación",
        "Acepta Prórroga",
        "Ampliación",
        "Alternativa",
        "¿Lleva Muestras?",
    }:
        return _format_export_bool(value)
    return _clean_export_value(value)


def _lookup_process_value(caso, proc: dict, *aliases) -> str:
    return _pick(
        _from_proceso(proc, *aliases),
        _from_trazabilidad(caso, *aliases),
        _from_analitica(caso, *aliases),
        _from_hallazgos(caso, *aliases),
    )


def _garantia_value(caso, *keywords) -> str:
    keyword_norms = [_norm(k) for k in keywords]
    for garantia in (caso.garantias or []):
        tipo_norm = _norm(garantia.tipo)
        if not any(k and k in tipo_norm for k in keyword_norms):
            continue
        pieces = []
        for value in (garantia.requerida, garantia.porcentaje, garantia.base_calculo, garantia.plazo, garantia.formas_admitidas):
            clean = _clean_export_value(value)
            if clean:
                pieces.append(clean)
        return " - ".join(pieces) if pieces else _clean_export_value(garantia.tipo)
    return ""


def _lleva_muestras_value(caso, proc: dict) -> str:
    raw = _lookup_process_value(caso, proc, "lleva_muestras", "muestras", "requiere_muestras")
    if raw:
        return raw
    for requisito in (caso.requisitos or []):
        text = " ".join(_safe(v) for v in (requisito.categoria, requisito.descripcion, requisito.obligatorio))
        if "muestra" in _norm(text):
            return "Sí"
    return ""


def _fecha_limite_muestras_value(caso, proc: dict) -> str:
    return _pick(
        _from_cronograma(caso, "muestra", "presentacion de muestras", "presentación de muestras"),
        _lookup_process_value(caso, proc, "fecha_limite_muestras", "fecha_limite_presentacion_muestras", "fecha limite para presentacion de muestras"),
    )


def _build_fusion_process_export_row(caso, fusion_ctx: dict) -> dict:
    proc = _get_proceso(caso)
    campos_fusion = fusion_ctx.get("campos_fusion", {})
    row = {}

    for column in FUSION_EXPORT_PROCESO_COLUMNS:
        field_key = _FUSION_PROCESS_FIELD_BY_COLUMN.get(column)
        value = ""
        if field_key:
            field_data = campos_fusion.get(field_key, {})
            value = field_data.get("valor_normalizado") or field_data.get("valor") or ""
        elif column == "Ampliación":
            value = _lookup_process_value(caso, proc, "ampliacion", "ampliación", "acepta_ampliacion")
        elif column == "Alternativa":
            value = _lookup_process_value(caso, proc, "alternativa", "oferta_alternativa", "acepta_alternativa")
        elif column == "Lugar y Cond. de Entrega":
            value = _lookup_process_value(caso, proc, "lugar_entrega", "lugar y cond de entrega", "condiciones_entrega", "plazo_entrega", "periodicidad")
        elif column == "Garantía de Mantenimiento de Oferta":
            value = _garantia_value(caso, "mantenimiento", "oferta")
        elif column == "Garantía de Anticipo Financiero":
            value = _garantia_value(caso, "anticipo")
        elif column == "Garantía de Cumplimiento de Contrato":
            value = _garantia_value(caso, "cumplimiento", "contrato")
        elif column == "Garantía de Impugnación al Pliego":
            value = _garantia_value(caso, "impugnacion", "impugnación", "pliego")
        elif column == "Garantía de Impugnación a la Preadjudicación":
            value = _garantia_value(caso, "impugnacion", "impugnación", "preadjudicacion", "preadjudicación")
        elif column == "Garantía de Incorporar Contragarantía":
            value = _garantia_value(caso, "contragarantia", "contragarantía", "contra garantia")
        elif column == "Exigencia con Causal de Desestimación":
            value = _lookup_process_value(caso, proc, "exigencia_desestimacion", "causal_desestimacion", "causal de desestimacion")
        elif column == "¿Lleva Muestras?":
            value = _lleva_muestras_value(caso, proc)
        elif column == "Fecha limite para presentación de muestras":
            value = _fecha_limite_muestras_value(caso, proc)
        row[column] = _format_export_value(column, value)

    return row


def _build_fusion_renglones_export_rows(caso, fusion_ctx: dict) -> list[dict]:
    rows = []
    for row in fusion_ctx.get("renglones_fusion", []) or []:
        extra = row.get("datos_extra", {}) if isinstance(row.get("datos_extra", {}), dict) else {}
        obj_gasto = _pick(
            row.get("obj_gasto"),
            row.get("objeto_gasto"),
            row.get("obj_gas"),
            extra.get("obj_gasto"),
            extra.get("objeto_gasto"),
            extra.get("obj_gas"),
            extra.get("obj. gas."),
        )
        rows.append({
            "Item": _clean_export_value(row.get("item")),
            "Obj. Gas.": _clean_export_value(obj_gasto),
            "Cod. Item": _clean_export_value(row.get("codigo_item")),
            "Descripción": _clean_export_value(row.get("descripcion")),
            "Cant": _clean_export_value(row.get("cantidad")),
        })
    return rows


def export_fusion_excel_bytes(caso) -> bytes:
    """Genera el Excel final para carga en Fusion, sin hojas internas de LicIA."""
    fusion_ctx = calcular_estado_fusion(caso)
    proceso_row = _build_fusion_process_export_row(caso, fusion_ctx)
    renglones_rows = _build_fusion_renglones_export_rows(caso, fusion_ctx)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([proceso_row], columns=FUSION_EXPORT_PROCESO_COLUMNS).to_excel(
            writer,
            index=False,
            sheet_name="Datos del proceso",
        )
        pd.DataFrame(renglones_rows, columns=FUSION_EXPORT_RENGLON_COLUMNS).to_excel(
            writer,
            index=False,
            sheet_name="Renglones del pliego",
        )

        for sheet in writer.book.worksheets:
            sheet.freeze_panes = "A2"
            sheet.auto_filter.ref = sheet.dimensions
            for cell in sheet[1]:
                cell.font = cell.font.copy(bold=True)
            for column_cells in sheet.columns:
                max_len = max(len(str(cell.value or "")) for cell in column_cells)
                width = min(max(max_len + 2, 10), 42)
                sheet.column_dimensions[column_cells[0].column_letter].width = width

    buffer.seek(0)
    return buffer.getvalue()


def calcular_estado_fusion(caso) -> dict:
    """
    Calcula el estado Fusion del caso de forma independiente a LicIA.
    Retorna:
    - estado_fusion: 'listo_para_fusion' | 'no_listo_para_fusion' | 'requiere_validacion_manual'
    - estado_visualizacion: 'listo_para_visualizacion' | 'requiere_revision' | 'en_reproceso' | 'incompleto'
    - estado_licia: valor informativo leído del Excel
    - faltantes_criticos: lista de campos obligatorios faltantes
    - campos_fusion: dict con todos los campos resueltos y sus estados
    - renglones_fusion: lista de renglones procesados
    - contradiccion_licia: bool — LicIA dice listo pero SIEM detecta faltantes
    - mensaje_contradiccion: str
    - resumen: dict con conteos
    """
    fc = _get_fusion_cabecera(caso)
    proc = _get_proceso(caso)
    ctrl = _get_control_carga(caso)

    # Estado informativo de LicIA
    licia_listo = _safe(
        fc.get("listo_para_fusion") or ctrl.get("listo_para_fusion")
    )
    licia_listo_visualizacion = _safe(
        fc.get("listo_para_visualizacion") or ctrl.get("listo_para_visualizacion")
    )
    licia_requiere_revision = _safe(
        fc.get("requiere_revision_analista") or ctrl.get("requiere_revision_analista")
    )
    estado_licia = {
        "listo_para_fusion": licia_listo,
        "listo_para_visualizacion": licia_listo_visualizacion,
        "requiere_revision_analista": licia_requiere_revision,
        "nivel_riesgo": _safe(ctrl.get("nivel_riesgo_integracion", "")),
        "motivo_estado": _safe(ctrl.get("motivo_estado", "")),
        "accion_recomendada": _safe(ctrl.get("accion_recomendada", "")),
        "faltantes_documentales": _safe(ctrl.get("faltantes_documentales", "")),
        "contradicciones_detectadas": _safe(ctrl.get("contradicciones_detectadas", "")),
    }

    # Resolver cada campo obligatorio
    campos_fusion = {}
    faltantes_criticos = []
    campos_completos = 0
    requiere_validacion_manual = False

    for campo_meta in FUSION_CAMPOS_OBLIGATORIOS:
        key = campo_meta["key"]
        resultado = _resolver_campo_fusion(caso, key, fc, proc)
        resultado["key"] = key
        resultado["label"] = campo_meta["label"]
        resultado["categoria"] = campo_meta["categoria"]

        if resultado["completo"]:
            campos_completos += 1
            if resultado["requiere_validacion"]:
                requiere_validacion_manual = True
                resultado["estado"] = "requiere_validacion_manual"
            else:
                resultado["estado"] = "completo"
        else:
            resultado["estado"] = "faltante_critico_fusion"
            faltantes_criticos.append({
                "key": key,
                "label": campo_meta["label"],
                "categoria": campo_meta["categoria"],
                "hoja_origen": resultado.get("fuente", "—"),
            })

        campos_fusion[key] = resultado

    # Renglones
    renglones_fusion = _resolver_renglones_fusion(caso)
    tiene_renglones = len(renglones_fusion) > 0

    # Calcular estado_fusion SIEM (independiente de LicIA)
    total_obligatorios = len(FUSION_CAMPOS_OBLIGATORIOS)
    if not faltantes_criticos and tiene_renglones and not requiere_validacion_manual:
        estado_fusion = "listo_para_fusion"
    elif requiere_validacion_manual or (faltantes_criticos and campos_completos >= total_obligatorios - 3):
        estado_fusion = "requiere_validacion_manual"
    else:
        estado_fusion = "no_listo_para_fusion"

    # Estado visualización
    if caso.datos_proceso and (caso.renglones or caso.fusion_renglones):
        if faltantes_criticos or requiere_validacion_manual:
            estado_visualizacion = "requiere_revision"
        else:
            estado_visualizacion = "listo_para_visualizacion"
    elif caso.datos_proceso:
        estado_visualizacion = "requiere_revision"
    else:
        estado_visualizacion = "incompleto"

    # Detectar contradicción con LicIA
    licia_dice_listo = _norm(licia_listo) in {"si", "sí", "yes", "true", "1"}
    contradiccion_licia = licia_dice_listo and bool(faltantes_criticos)
    mensaje_contradiccion = (
        "El Excel de LicIA indica listo para Fusion, pero SIEM detectó faltantes obligatorios según la matriz Fusion."
        if contradiccion_licia else ""
    )

    # Clasificar faltantes/dudas para separar documentales de campos Fusion
    faltantes_documentales = []
    faltantes_campos_fusion = []
    dudas_operativas = []

    for f in (caso.faltantes or []):
        campo_norm = _norm(f.campo_objetivo)
        es_campo_fusion = any(
            campo_norm == _norm(c["key"]) or campo_norm in _norm(c["key"]) or _norm(c["key"]) in campo_norm
            for c in FUSION_CAMPOS_OBLIGATORIOS
        )
        criticidad_norm = _norm(f.criticidad or "")
        if "documental" in (f.motivo or "").lower() or "documento" in campo_norm:
            faltantes_documentales.append(f)
        elif es_campo_fusion and criticidad_norm in {"alta", "critica", "crítica", "alto", "high"}:
            faltantes_campos_fusion.append(f)
        else:
            dudas_operativas.append(f)

    return {
        "estado_fusion": estado_fusion,
        "estado_visualizacion": estado_visualizacion,
        "estado_licia": estado_licia,
        "estado_licia_texto": _build_licia_texto(estado_licia),
        "faltantes_criticos": faltantes_criticos,
        "campos_fusion": campos_fusion,
        "campos_completos": campos_completos,
        "campos_total_obligatorios": total_obligatorios,
        "renglones_fusion": renglones_fusion,
        "tiene_renglones": tiene_renglones,
        "cantidad_renglones": len(renglones_fusion),
        "requiere_validacion_manual": requiere_validacion_manual,
        "contradiccion_licia": contradiccion_licia,
        "mensaje_contradiccion": mensaje_contradiccion,
        "faltantes_documentales": faltantes_documentales,
        "faltantes_campos_fusion": faltantes_campos_fusion,
        "dudas_operativas": dudas_operativas,
        "resumen": {
            # aliases usados en templates
            "campos_completos": campos_completos,
            "campos_totales": total_obligatorios,
            "renglones": len(renglones_fusion),
            # claves completas
            "campos_obligatorios_completos": campos_completos,
            "campos_obligatorios_faltantes": len(faltantes_criticos),
            "campos_requieren_validacion": sum(1 for c in campos_fusion.values() if c.get("estado") == "requiere_validacion_manual"),
            "cantidad_renglones": len(renglones_fusion),
            "faltantes_documentales": len(faltantes_documentales),
            "requiere_revision_analista": bool(licia_requiere_revision and _norm(licia_requiere_revision) in {"si", "sí", "yes", "true", "1"}),
            "contradicciones_detectadas": _safe(ctrl.get("contradicciones_detectadas", "0")),
        },
    }
