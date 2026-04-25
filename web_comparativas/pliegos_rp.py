from __future__ import annotations

import io
import re
import unicodedata
from collections import OrderedDict
from pathlib import Path
from typing import Any

import pandas as pd

from web_comparativas.models import PliegoSolicitud

GROUP_GENERAL = "Datos generales del proceso"
GROUP_DETAIL = "Detalle"
GROUP_MATRIX = "Matriz Requerimiento del Pliego"

RP_GENERAL_FIELDS = [
    "Origen",
    "Unidad Ejecutora",
    "N° Procesos",
    "Nombre Proceso",
    "Procedimiento Selección",
    "Objeto Contratación",
    "Fecha Acto Apertura",
    "Ult. Actualización",
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
    "Nro. Expediente",
    "Modalidad",
    "Link",
    "Alternativa",
]

RP_DETAIL_FIELDS = [
    "Item",
    "Obj. Gas.",
    "Cod. Item",
    "Descripción",
    "Cant",
    "Ampliación",
]

RP_MATRIX_FIELDS = [
    "Lugar y Cond. de Entrega",
    "Garantía de Mantenimiento de Oferta",
    "Garantía de Anticipo Financiero",
    "Garantía de Cumplimiento de Contrato",
    "Garantía de Impugnación al Pliego",
    "Garantía de Impugnación a la Preadjudicación",
    "Garantía de Incorporar Contragarantía",
    "Fecha limite para presentación de muestras",
    "Fecha limite para disposición de oferta",
    "Exigencia con Causal de Desestimación",
    "Plazo de Pago",
    "Multa por Incumplimiento",
    "Costo de Pliego",
    "¿Lleva Muestras?",
    "Retira en",
]

RP_GROUPS = OrderedDict(
    [
        (GROUP_GENERAL, RP_GENERAL_FIELDS),
        (GROUP_DETAIL, RP_DETAIL_FIELDS),
        (GROUP_MATRIX, RP_MATRIX_FIELDS),
    ]
)

MISSING_MARKERS = {
    "",
    "-",
    "--",
    "n/a",
    "na",
    "nan",
    "none",
    "no encontrado",
    "no encontrada",
    "no identificado",
    "no identificada",
    "sin dato",
    "sin datos",
    "sin informacion",
    "sin información",
}

FIELD_SPECS = OrderedDict(
    [
        (
            "Origen",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.organismo_contratante | PliegoSolicitud.organismo",
                "transformation": "Renombrar organismo contratante como Origen",
                "resolver": "process_direct",
                "keys": ["organismo_contratante", "organismo"],
                "case_attrs": ["organismo"],
            },
        ),
        (
            "Unidad Ejecutora",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.unidad_operativa_adquisiciones | 01_Proceso.unidad_operativa",
                "transformation": "Renombrar UOA a Unidad Ejecutora",
                "resolver": "process_direct",
                "keys": ["unidad_operativa_adquisiciones", "unidad_operativa", "unidad_contratante", "uoa"],
            },
        ),
        (
            "N° Procesos",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.numero_proceso",
                "transformation": "Renombrar numero_proceso a N° Procesos",
                "resolver": "process_direct",
                "keys": ["numero_proceso", "nro_proceso", "codigo_oficial"],
                "case_attrs": ["numero_proceso"],
            },
        ),
        (
            "Nombre Proceso",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.nombre_proceso | PliegoSolicitud.nombre_licitacion",
                "transformation": "Renombrar denominación actual a Nombre Proceso",
                "resolver": "process_direct",
                "keys": ["nombre_proceso", "denominacion", "titulo_proceso"],
                "case_attrs": ["nombre_licitacion", "titulo"],
            },
        ),
        (
            "Procedimiento Selección",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.tipo_proceso",
                "transformation": "Renombrar tipo_proceso a Procedimiento Selección",
                "resolver": "process_direct",
                "keys": ["tipo_proceso", "clase_proceso", "tipo_licitacion", "tipo_compra"],
            },
        ),
        (
            "Objeto Contratación",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.objeto_contratacion",
                "transformation": "Renombrar objeto_contratacion al campo RP",
                "resolver": "process_direct",
                "keys": ["objeto_contratacion", "objeto", "descripcion_objeto"],
            },
        ),
        (
            "Fecha Acto Apertura",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "REGLA NUEVA",
                "source_current": "02_Cronograma.fecha + detalle del hito apertura | 10_Trazabilidad.fecha_apertura",
                "transformation": "Tomar hito apertura y combinar fecha/hora o detalle literal",
                "resolver": "cronograma_event",
                "event_tokens": ["apertura"],
            },
        ),
        (
            "Ult. Actualización",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "REGLA NUEVA",
                "source_current": "PliegoSolicitud.actualizado_en | PliegoSolicitud.publicado_en",
                "transformation": "Formatear timestamp interno como fecha RP",
                "resolver": "case_datetime",
                "attrs": ["actualizado_en", "publicado_en"],
            },
        ),
        (
            "Tipo Cotización",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.tipo_cotizacion",
                "transformation": "Renombrar tipo_cotizacion a Tipo Cotización",
                "resolver": "process_direct",
                "keys": ["tipo_cotizacion", "forma_cotizacion"],
            },
        ),
        (
            "Tipo Adjudicación",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.tipo_adjudicacion",
                "transformation": "Renombrar tipo_adjudicacion a Tipo Adjudicación",
                "resolver": "process_direct",
                "keys": ["tipo_adjudicacion", "forma_adjudicacion"],
            },
        ),
        (
            "Cant. Oferta Permitidas",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "FALTANTE REAL",
                "source_current": "Sin fuente estructurada persistida",
                "transformation": "No se mapea sin dato explícito",
                "resolver": "missing",
            },
        ),
        (
            "Plazo Mantenimiento Oferta",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.plazo_mantenimiento_oferta",
                "transformation": "Renombrar plazo_mantenimiento_oferta al campo RP",
                "resolver": "process_direct",
                "keys": ["plazo_mantenimiento_oferta", "mantenimiento_oferta"],
            },
        ),
        (
            "Acepta Redeterminación",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.acepta_redeterminacion",
                "transformation": "Normalizar SI/NO al nombre RP",
                "resolver": "process_direct",
                "keys": ["acepta_redeterminacion", "redeterminacion_precios"],
                "normalize_yes_no": True,
            },
        ),
        (
            "Fecha Inicio Consulta",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "REGLA NUEVA",
                "source_current": "02_Cronograma.fecha + detalle del hito inicio_consultas",
                "transformation": "Tomar hito de inicio de consultas y priorizar fecha exacta",
                "resolver": "cronograma_event",
                "event_tokens": ["inicio_consultas", "inicio consultas", "consultas"],
            },
        ),
        (
            "Fecha Final Consulta",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "REGLA NUEVA",
                "source_current": "02_Cronograma.fecha + detalle del hito fin_consultas",
                "transformation": "Tomar hito de cierre de consultas y priorizar fecha exacta",
                "resolver": "cronograma_event",
                "event_tokens": ["fin_consultas", "fin consultas", "consultas"],
            },
        ),
        (
            "Monto",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.monto_estimado | 10_Trazabilidad.monto_estimado",
                "transformation": "Renombrar monto_estimado al campo RP Monto",
                "resolver": "process_direct",
                "keys": ["monto_estimado", "presupuesto_oficial", "monto_oficial", "presupuesto"],
                "format_money": True,
            },
        ),
        (
            "Moneda",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "OK",
                "source_current": "01_Proceso.moneda",
                "transformation": "Sin transformación semántica, solo estandarización básica",
                "resolver": "process_direct",
                "keys": ["moneda", "divisa", "currency"],
            },
        ),
        (
            "Duración Contrato",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.duracion_contrato",
                "transformation": "Renombrar duracion_contrato a Duración Contrato",
                "resolver": "process_direct",
                "keys": ["duracion_contrato", "plazo_contrato", "duracion_del_contrato"],
            },
        ),
        (
            "Acepta Prórroga",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.acepta_prorroga",
                "transformation": "Normalizar SI/NO al nombre RP",
                "resolver": "process_direct",
                "keys": ["acepta_prorroga", "prorroga"],
                "normalize_yes_no": True,
            },
        ),
        (
            "Nro. Expediente",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "RENOMBRAR",
                "source_current": "01_Proceso.expediente | PliegoSolicitud.expediente",
                "transformation": "Renombrar expediente a Nro. Expediente",
                "resolver": "process_direct",
                "keys": ["expediente", "nro_expediente", "numero_expediente"],
                "case_attrs": ["expediente"],
            },
        ),
        (
            "Modalidad",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "OK",
                "source_current": "01_Proceso.modalidad",
                "transformation": "Sin transformación semántica, solo renombre visual RP",
                "resolver": "process_direct",
                "keys": ["modalidad", "modalidad_contratacion"],
            },
        ),
        (
            "Link",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "REGLA NUEVA",
                "source_current": "06_Documentos.url_o_referencia",
                "transformation": "Tomar la primera URL explícita informada en documentos",
                "resolver": "document_link",
            },
        ),
        (
            "Alternativa",
            {
                "group": GROUP_GENERAL,
                "mapping_status": "REGLA NUEVA",
                "source_current": "05_Renglones.permite_equivalente",
                "transformation": "Consolidar equivalencias permitidas desde renglones",
                "resolver": "renglon_yes_no",
                "keys": ["permite_equivalente"],
            },
        ),
        (
            "Item",
            {
                "group": GROUP_DETAIL,
                "mapping_status": "RENOMBRAR",
                "source_current": "05_Renglones.renglon_nro | PliegoRenglon.numero_renglon",
                "transformation": "Renombrar el identificador del renglón al campo RP Item",
                "resolver": "missing",
            },
        ),
        (
            "Obj. Gas.",
            {
                "group": GROUP_DETAIL,
                "mapping_status": "FALTANTE REAL",
                "source_current": "Sin fuente estructurada actual",
                "transformation": "No se mapea sin dato explícito",
                "resolver": "missing",
            },
        ),
        (
            "Cod. Item",
            {
                "group": GROUP_DETAIL,
                "mapping_status": "RENOMBRAR",
                "source_current": "05_Renglones.codigo_item | PliegoRenglon.codigo_item",
                "transformation": "Renombrar código de ítem al nombre RP",
                "resolver": "missing",
            },
        ),
        (
            "Descripción",
            {
                "group": GROUP_DETAIL,
                "mapping_status": "OK",
                "source_current": "05_Renglones.descripcion | PliegoRenglon.descripcion",
                "transformation": "Sin transformación semántica",
                "resolver": "missing",
            },
        ),
        (
            "Cant",
            {
                "group": GROUP_DETAIL,
                "mapping_status": "RENOMBRAR",
                "source_current": "05_Renglones.cantidad | PliegoRenglon.cantidad",
                "transformation": "Renombrar cantidad a Cant",
                "resolver": "missing",
            },
        ),
        (
            "Ampliación",
            {
                "group": GROUP_DETAIL,
                "mapping_status": "REGLA NUEVA",
                "source_current": "08_Hallazgos_extra modificación de cantidades",
                "transformation": "Propagar regla general de ampliación al detalle RP",
                "resolver": "missing",
            },
        ),
        (
            "Lugar y Cond. de Entrega",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "05_Renglones.lugar_entrega + plazo_entrega + periodicidad | 01_Proceso.lugar_recepcion_fisica",
                "transformation": "Consolidar lugar, plazo y periodicidad en una única celda RP",
                "resolver": "delivery_condition",
            },
        ),
        (
            "Garantía de Mantenimiento de Oferta",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "04_Garantias.tipo_garantia = mantenimiento de oferta",
                "transformation": "Filtrar la garantía por tipo y resumir condiciones",
                "resolver": "garantia_match",
                "match_tokens": ["mantenimiento", "oferta"],
            },
        ),
        (
            "Garantía de Anticipo Financiero",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "04_Garantias.tipo_garantia = anticipo financiero | 08_Hallazgos_extra anticipo",
                "transformation": "Buscar garantía o hallazgo asociado al anticipo financiero",
                "resolver": "anticipo_garantia",
            },
        ),
        (
            "Garantía de Cumplimiento de Contrato",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "04_Garantias.tipo_garantia = cumplimiento",
                "transformation": "Filtrar la garantía por cumplimiento de contrato",
                "resolver": "garantia_match",
                "match_tokens": ["cumplimiento", "contrato"],
            },
        ),
        (
            "Garantía de Impugnación al Pliego",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "04_Garantias.tipo_garantia con referencia explícita al pliego",
                "transformation": "Tomar solo garantías que mencionen impugnación del pliego",
                "resolver": "garantia_match",
                "match_tokens": ["impugnacion", "pliego"],
            },
        ),
        (
            "Garantía de Impugnación a la Preadjudicación",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "04_Garantias.tipo_garantia con referencia explícita a preadjudicación",
                "transformation": "Tomar solo garantías que mencionen preadjudicación",
                "resolver": "garantia_match",
                "match_tokens": ["preadjudic"],
            },
        ),
        (
            "Garantía de Incorporar Contragarantía",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "04_Garantias.tipo_garantia = contragarantía",
                "transformation": "Filtrar garantía de contragarantía y resumirla",
                "resolver": "garantia_match",
                "match_tokens": ["contragar"],
            },
        ),
        (
            "Fecha limite para presentación de muestras",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "03_Requisitos.lleva_muestra + detalle_muestra | 08_Hallazgos_extra muestras",
                "transformation": "Buscar fecha o condición temporal asociada a muestras",
                "resolver": "samples_deadline",
            },
        ),
        (
            "Fecha limite para disposición de oferta",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "02_Cronograma.presentacion_ofertas",
                "transformation": "Tomar fecha exacta o detalle de presentación de ofertas",
                "resolver": "cronograma_event",
                "event_tokens": ["presentacion_ofertas", "presentacion ofertas", "recepcion", "oferta"],
            },
        ),
        (
            "Exigencia con Causal de Desestimación",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "03_Requisitos.descripcion_requisito | 08_Hallazgos_extra.descripcion | 10_Trazabilidad",
                "transformation": "Detectar menciones explícitas a desestimación, rechazo o inadmisibilidad",
                "resolver": "text_search",
                "keywords": ["desestim", "inadmis", "rechaz"],
            },
        ),
        (
            "Plazo de Pago",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "01_Proceso.condiciones_pago | 08_Hallazgos_extra anticipo/pago",
                "transformation": "Usar condiciones de pago o hallazgo financiero explícito",
                "resolver": "payment_term",
            },
        ),
        (
            "Multa por Incumplimiento",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "08_Hallazgos_extra sanción/penalidad/multa | 10_Trazabilidad",
                "transformation": "Detectar penalidades explícitas sin inferir contenido nuevo",
                "resolver": "text_search",
                "keywords": ["multa", "penal", "incumpl"],
            },
        ),
        (
            "Costo de Pliego",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "01_Proceso.requiere_pago_pliego | 01_Proceso.valor_pliego",
                "transformation": "Parsear importe embebido o reutilizar valor_pliego explícito",
                "resolver": "pliego_cost",
            },
        ),
        (
            "¿Lleva Muestras?",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "REGLA NUEVA",
                "source_current": "03_Requisitos.lleva_muestra | 05_Renglones.muestras",
                "transformation": "Consolidar SI/NO desde requisitos y renglones",
                "resolver": "samples_yes_no",
            },
        ),
        (
            "Retira en",
            {
                "group": GROUP_MATRIX,
                "mapping_status": "FALTANTE REAL",
                "source_current": "Sin fuente estructurada actual con retiro explícito",
                "transformation": "No se mapea sin indicación textual de retiro",
                "resolver": "text_search",
                "keywords": ["retira en", "retiro", "retirar"],
            },
        ),
    ]
)

RP_PROCESS_KEYS_USED = {
    "organismo_contratante",
    "organismo",
    "unidad_operativa_adquisiciones",
    "unidad_operativa",
    "unidad_contratante",
    "uoa",
    "numero_proceso",
    "nro_proceso",
    "codigo_oficial",
    "nombre_proceso",
    "denominacion",
    "titulo_proceso",
    "tipo_proceso",
    "clase_proceso",
    "tipo_licitacion",
    "tipo_compra",
    "objeto_contratacion",
    "objeto",
    "descripcion_objeto",
    "tipo_cotizacion",
    "forma_cotizacion",
    "tipo_adjudicacion",
    "forma_adjudicacion",
    "plazo_mantenimiento_oferta",
    "mantenimiento_oferta",
    "acepta_redeterminacion",
    "redeterminacion_precios",
    "monto_estimado",
    "presupuesto_oficial",
    "monto_oficial",
    "presupuesto",
    "moneda",
    "divisa",
    "currency",
    "duracion_contrato",
    "plazo_contrato",
    "duracion_del_contrato",
    "acepta_prorroga",
    "prorroga",
    "expediente",
    "nro_expediente",
    "numero_expediente",
    "modalidad",
    "modalidad_contratacion",
    "requiere_pago_pliego",
    "valor_pliego",
    "condiciones_pago",
    "pago",
    "anticipo_financiero",
    "lugar_recepcion_fisica",
}


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none"} else text


def _strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value or "")
        if not unicodedata.combining(ch)
    )


def _normalize_key(value: Any) -> str:
    text = _strip_accents(_safe_str(value).lower())
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _strip_accents(_safe_str(value).lower())).strip()


def _is_missing_value(value: Any) -> bool:
    text = _normalize_text(value)
    return not text or text in MISSING_MARKERS or text.startswith("no encontrado") or text.startswith("no encontrada")


def _normalize_yes_no(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    tokens = text.split()
    if any(token in {"si", "sí", "yes", "true", "1"} for token in tokens[:3]) or text.startswith("si") or text.startswith("sí"):
        return "Sí"
    if any(token in {"no", "false", "0"} for token in tokens[:3]) or text.startswith("no"):
        return "No"
    return ""


def _parse_decimal(value: Any) -> float | None:
    text = _safe_str(value)
    if not text:
        return None
    cleaned = re.sub(r"[^0-9,.\-]", "", text)
    if not cleaned or cleaned in {"-", ".", ","}:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif cleaned.count(".") > 1:
        cleaned = cleaned.replace(".", "")
    elif cleaned.count(",") > 1:
        parts = cleaned.split(",")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    elif "," in cleaned:
        whole, frac = cleaned.rsplit(",", 1)
        cleaned = f"{whole}.{frac}" if len(frac) <= 2 else whole + frac
    try:
        return float(cleaned)
    except ValueError:
        return None


def _format_number(value: float) -> str:
    decimals = 0 if abs(value - int(value)) < 0.000001 else 2
    fmt = f"{value:,.{decimals}f}"
    return fmt.replace(",", "X").replace(".", ",").replace("X", ".")


def _format_money(value: Any, currency: str = "ARS") -> str:
    parsed = _parse_decimal(value)
    if parsed is None:
        return _safe_str(value)
    prefix = "$ " if (currency or "ARS").upper() == "ARS" else f"{currency.upper()} "
    return f"{prefix}{_format_number(parsed)}"


def _extract_money_from_text(value: Any) -> str:
    text = _safe_str(value)
    match = re.search(r"([0-9][0-9\.\,]+)", text)
    if not match:
        return ""
    parsed = _parse_decimal(match.group(1))
    return str(parsed) if parsed is not None else ""


def _format_datetime_value(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%d/%m/%Y %H:%M")
    return _safe_str(value)


def _join_unique(values: list[str], sep: str = " | ") -> str:
    result = []
    seen = set()
    for value in values:
        text = _safe_str(value)
        norm = _normalize_text(text)
        if not text or norm in seen:
            continue
        seen.add(norm)
        result.append(text)
    return sep.join(result)


def _resolve_active_excel_path(caso: PliegoSolicitud) -> Path | None:
    cargas = [
        carga
        for carga in (caso.cargas_excel or [])
        if getattr(carga, "es_activa", False) and _safe_str(getattr(carga, "url_path", ""))
    ]
    if not cargas:
        return None
    cargas.sort(key=lambda item: getattr(item, "version", 0), reverse=True)
    url_path = _safe_str(cargas[0].url_path)
    if not url_path:
        return None
    base_dir = Path(__file__).resolve().parent
    return base_dir / url_path.lstrip("/\\")


def _looks_like_template_record(record: dict[str, Any]) -> bool:
    values = [_normalize_text(value) for value in record.values() if _safe_str(value)]
    if not values:
        return True
    joined = " ".join(values)
    template_tokens = {
        "id proceso",
        "id requisito",
        "id doc",
        "aaaa-mm-dd",
        "campo esperado",
        "pagina/seccion",
        "sí/no",
        "si/no",
        "archivo origen",
        "texto normalizado",
        "descripcion normalizada",
        "encontrado/ambiguo",
        "alto/medio/bajo",
    }
    hits = sum(1 for token in template_tokens if token in joined)
    return hits >= 2


def _load_active_excel_records(caso: PliegoSolicitud) -> dict[str, list[dict[str, str]]]:
    path = _resolve_active_excel_path(caso)
    if not path or not path.exists():
        return {}
    try:
        xl = pd.ExcelFile(path)
    except Exception:
        return {}
    records: dict[str, list[dict[str, str]]] = {}
    for sheet_name in xl.sheet_names:
        try:
            df = xl.parse(sheet_name, dtype=str).fillna("")
        except Exception:
            continue
        rows = []
        for row in df.to_dict("records"):
            clean = {str(key): _safe_str(value) for key, value in row.items() if _safe_str(key)}
            if not clean or _looks_like_template_record(clean):
                continue
            rows.append(clean)
        records[_normalize_key(sheet_name)] = rows
    return records


def _sheet_rows(records: dict[str, list[dict[str, str]]], *sheet_names: str) -> list[dict[str, str]]:
    for sheet_name in sheet_names:
        found = records.get(_normalize_key(sheet_name))
        if found:
            return found
    return []


def _build_process_data(caso: PliegoSolicitud, records: dict[str, list[dict[str, str]]]) -> dict[str, str]:
    data = dict(caso.datos_proceso.datos) if (caso.datos_proceso and caso.datos_proceso.datos) else {}
    
    # Si el DB está vacío, intentar con el Excel
    if not data or all(_is_missing_value(v) for v in data.values()):
        process_rows = _sheet_rows(records, "01_Proceso", "Proceso")
        if process_rows:
            first = process_rows[0]
            for key, value in first.items():
                if key not in data or _is_missing_value(data.get(key)):
                    data[key] = value
    return data


def _normalized_dict(data: dict[str, Any]) -> dict[str, str]:
    return {_normalize_key(key): _safe_str(value) for key, value in (data or {}).items()}


def _get_from_process(process_data: dict[str, Any], keys: list[str]) -> tuple[str, str]:
    normalized = _normalized_dict(process_data)
    for key in keys:
        value = normalized.get(_normalize_key(key), "")
        if not _is_missing_value(value):
            return value, key
    return "", ""


def _record_value(record: dict[str, Any], *keys: str) -> str:
    normalized = _normalized_dict(record)
    for key in keys:
        value = normalized.get(_normalize_key(key), "")
        if not _is_missing_value(value):
            return value
    return ""


def _row_text(record: dict[str, Any]) -> str:
    return " ".join(_normalize_text(value) for value in record.values() if _safe_str(value))


def _find_rows_by_any_keyword(rows: list[dict[str, Any]], keywords: list[str]) -> list[dict[str, Any]]:
    result = []
    normalized_keywords = [_normalize_text(keyword) for keyword in keywords]
    for row in rows:
        haystack = _row_text(row)
        if any(keyword in haystack for keyword in normalized_keywords):
            result.append(row)
    return result


def _format_garantia_row(row: dict[str, Any]) -> str:
    parts = []
    requerida = _normalize_yes_no(_record_value(row, "requerida"))
    if requerida:
        parts.append(requerida)
    porcentaje = _record_value(row, "porcentaje", "monto_fijo")
    if porcentaje:
        parts.append(porcentaje)
    base = _record_value(row, "base_calculo", "base")
    if base:
        parts.append(f"Base: {base}")
    plazo = _record_value(row, "plazo_constitucion", "plazo")
    if plazo:
        parts.append(f"Plazo: {plazo}")
    formas = _record_value(row, "formas_admitidas")
    if formas:
        parts.append(f"Formas: {formas}")
    if not parts:
        parts.append(_record_value(row, "tipo_garantia", "tipo"))
    return "; ".join(part for part in parts if part)


def _resolve_detail_ampliacion(caso: PliegoSolicitud, records: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    hallazgos_rows = _sheet_rows(records, "08_Hallazgos_extra", "Hallazgos_Extra")
    for row in hallazgos_rows:
        haystack = _row_text(row)
        if "cantidad" in haystack and ("aument" in haystack or "disminu" in haystack or "modific" in haystack):
            return {"value": _record_value(row, "descripcion", "titulo"), "requires_manual": True}
    for hallazgo in caso.hallazgos or []:
        haystack = _normalize_text(" ".join(filter(None, [hallazgo.categoria, hallazgo.hallazgo, hallazgo.accion_sugerida])))
        if "cantidad" in haystack and ("aument" in haystack or "disminu" in haystack or "modific" in haystack):
            return {"value": hallazgo.hallazgo or hallazgo.accion_sugerida or "", "requires_manual": True}
    return {}


def _finalize_field(label: str, resolved: dict[str, Any]) -> dict[str, Any]:
    spec = FIELD_SPECS.get(label, {})
    mapping_status = resolved.get("mapping_status") or spec.get("mapping_status", "REGLA NUEVA")
    source_current = resolved.get("source_current") or spec.get("source_current", "")
    transformation = resolved.get("transformation") or spec.get("transformation", "")
    raw_value = _safe_str(resolved.get("value", ""))
    normalized = _normalize_text(raw_value)
    ambiguous = bool(resolved.get("ambiguous")) or normalized in {"ambiguo", "contradictorio"}
    requires_manual = bool(resolved.get("requires_manual")) or normalized in {"pendiente validacion", "pendiente validación"}
    has_meaningful_value = bool(raw_value and normalized not in MISSING_MARKERS and normalized not in {"ambiguo", "contradictorio"})
    coverage_status = "COMPLETO"
    display_value = raw_value
    export_value = raw_value
    if not has_meaningful_value:
        if ambiguous:
            coverage_status = "AMBIGUO"
            display_value = raw_value or "Ambiguo"
            export_value = raw_value if raw_value and normalized not in {"ambiguo"} else ""
        elif requires_manual:
            coverage_status = "VALIDACION_MANUAL"
            display_value = raw_value or "Validación manual"
            export_value = raw_value
        else:
            coverage_status = "FALTANTE_REAL"
            display_value = "Faltante real"
            export_value = ""
    else:
        if ambiguous:
            coverage_status = "AMBIGUO"
        elif requires_manual:
            coverage_status = "VALIDACION_MANUAL"
        if resolved.get("format_money"):
            display_value = _format_money(raw_value, resolved.get("currency", "ARS"))
            export_value = display_value
        elif resolved.get("normalize_yes_no"):
            normalized_yes_no = _normalize_yes_no(raw_value)
            if normalized_yes_no:
                display_value = normalized_yes_no
                export_value = normalized_yes_no
    return {
        "label": label,
        "group": spec.get("group", ""),
        "value": raw_value,
        "display_value": display_value,
        "export_value": export_value,
        "coverage_status": coverage_status,
        "mapping_status": mapping_status,
        "source_current": source_current,
        "transformation": transformation,
        "notes": _safe_str(resolved.get("notes", "")),
    }


def _resolve_process_direct(spec: dict[str, Any], process_data: dict[str, Any], caso: PliegoSolicitud, currency: str) -> dict[str, Any]:
    value, key_used = _get_from_process(process_data, spec.get("keys", []))
    if _is_missing_value(value):
        for attr in spec.get("case_attrs", []):
            case_value = _safe_str(getattr(caso, attr, ""))
            if not _is_missing_value(case_value):
                value = case_value
                key_used = attr
                break
    resolved = {
        "value": value,
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "format_money": spec.get("format_money", False),
        "normalize_yes_no": spec.get("normalize_yes_no", False),
        "currency": currency,
    }
    if key_used and key_used not in spec.get("keys", []):
        resolved["requires_manual"] = True
    return resolved


def _build_event_value(row: dict[str, Any]) -> tuple[str, bool]:
    date_value = _record_value(row, "fecha")
    hour_value = _record_value(row, "hora")
    detail_value = _record_value(row, "detalle", "observaciones")
    parts = [part for part in [date_value, hour_value] if part]
    if parts:
        return " ".join(parts), not bool(date_value)
    if detail_value:
        return detail_value, True
    place_value = _record_value(row, "lugar_o_medio", "lugar_medio")
    if place_value:
        return place_value, True
    return "", True


def _event_matches(hito_text: str, event_tokens: list[str]) -> bool:
    normalized_tokens = [_normalize_text(token) for token in event_tokens]
    if "consultas" in normalized_tokens and "consulta" in hito_text:
        return True
    return all(token in hito_text for token in normalized_tokens if token != "consultas")


def _resolve_cronograma_event(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> dict[str, Any]:
    cronograma_rows = _sheet_rows(records, "02_Cronograma", "Cronograma")
    event_tokens = spec.get("event_tokens", [])
    matches = []
    for row in cronograma_rows:
        hito = _normalize_text(_record_value(row, "tipo_hito", "hito"))
        if _event_matches(hito, event_tokens):
            matches.append(row)
    if not matches:
        for hito in caso.cronograma or []:
            row = {
                "hito": hito.hito,
                "fecha": hito.fecha,
                "hora": hito.hora,
                "lugar_medio": hito.lugar_medio,
                "observaciones": hito.estado_dato,
            }
            if _event_matches(_normalize_text(hito.hito), event_tokens):
                matches.append(row)
    if not matches:
        return {
            "mapping_status": spec.get("mapping_status"),
            "source_current": spec.get("source_current"),
            "transformation": spec.get("transformation"),
        }
    values = []
    manual = False
    for row in matches:
        current_value, current_manual = _build_event_value(row)
        if current_value:
            values.append(current_value)
            manual = manual or current_manual
    return {
        "value": _join_unique(values),
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len({_normalize_text(value) for value in values if value}) > 1,
        "requires_manual": manual,
    }


def _resolve_case_datetime(spec: dict[str, Any], caso: PliegoSolicitud) -> dict[str, Any]:
    for attr in spec.get("attrs", []):
        value = getattr(caso, attr, None)
        if value is not None:
            return {
                "value": _format_datetime_value(value),
                "mapping_status": spec.get("mapping_status"),
                "source_current": spec.get("source_current"),
                "transformation": spec.get("transformation"),
            }
    return {
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
    }


def _resolve_document_link(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> dict[str, Any]:
    document_rows = _sheet_rows(records, "06_Documentos", "Documentos")
    for row in document_rows:
        value = _record_value(row, "url_o_referencia")
        if value.startswith("http://") or value.startswith("https://"):
            return {
                "value": value,
                "mapping_status": spec.get("mapping_status"),
                "source_current": spec.get("source_current"),
                "transformation": spec.get("transformation"),
            }
    for archivo in caso.archivos or []:
        value = _safe_str(getattr(archivo, "url_path", ""))
        if value.startswith("http://") or value.startswith("https://"):
            return {
                "value": value,
                "mapping_status": spec.get("mapping_status"),
                "source_current": "PliegoArchivo.url_path",
                "transformation": spec.get("transformation"),
            }
    return {
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
    }


def _resolve_renglon_yes_no(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> dict[str, Any]:
    values = []
    for row in _sheet_rows(records, "05_Renglones", "Renglones"):
        for key in spec.get("keys", []):
            value = _record_value(row, key)
            normalized = _normalize_yes_no(value)
            if normalized:
                values.append(normalized)
            elif not _is_missing_value(value):
                values.append(value)
    if not values:
        for renglon in caso.renglones or []:
            extra = renglon.datos_extra or {}
            for key in spec.get("keys", []):
                value = _safe_str(extra.get(key, ""))
                normalized = _normalize_yes_no(value)
                if normalized:
                    values.append(normalized)
                elif not _is_missing_value(value):
                    values.append(value)
    joined = _join_unique(values)
    unique = {_normalize_text(value) for value in values if value}
    return {
        "value": joined,
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len(unique) > 1,
        "normalize_yes_no": len(unique) <= 1,
    }


def _resolve_delivery_condition(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], process_data: dict[str, Any], caso: PliegoSolicitud) -> dict[str, Any]:
    values = []
    for row in _sheet_rows(records, "05_Renglones", "Renglones"):
        parts = []
        place = _record_value(row, "lugar_entrega")
        deadline = _record_value(row, "plazo_entrega")
        periodicity = _record_value(row, "periodicidad")
        if place:
            parts.append(place)
        if deadline:
            parts.append(f"Plazo: {deadline}")
        if periodicity:
            parts.append(f"Periodicidad: {periodicity}")
        if parts:
            values.append(" | ".join(parts))
    if not values:
        process_place, _ = _get_from_process(process_data, ["lugar_recepcion_fisica", "lugar_entrega"])
        if process_place:
            values.append(process_place)
    if not values:
        for renglon in caso.renglones or []:
            extra = renglon.datos_extra or {}
            parts = []
            if _safe_str(extra.get("lugar_entrega", "")):
                parts.append(_safe_str(extra.get("lugar_entrega", "")))
            if _safe_str(extra.get("plazo_entrega", "")):
                parts.append(f"Plazo: {_safe_str(extra.get('plazo_entrega', ''))}")
            if _safe_str(extra.get("periodicidad", "")):
                parts.append(f"Periodicidad: {_safe_str(extra.get('periodicidad', ''))}")
            if parts:
                values.append(" | ".join(parts))
    return {
        "value": _join_unique(values),
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len({_normalize_text(value) for value in values if value}) > 1,
        "requires_manual": len(values) > 1,
    }


def _resolve_garantia_match(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> dict[str, Any]:
    values = []
    garantia_rows = _sheet_rows(records, "04_Garantias", "Garantias")
    for row in garantia_rows:
        haystack = _normalize_text(_record_value(row, "tipo_garantia", "tipo"))
        if all(_normalize_text(token) in haystack for token in spec.get("match_tokens", [])):
            values.append(_format_garantia_row(row))
    if not values:
        for garantia in caso.garantias or []:
            haystack = _normalize_text(garantia.tipo)
            if all(_normalize_text(token) in haystack for token in spec.get("match_tokens", [])):
                values.append(
                    _join_unique(
                        [
                            _normalize_yes_no(garantia.requerida),
                            garantia.porcentaje or "",
                            f"Base: {garantia.base_calculo}" if _safe_str(garantia.base_calculo) else "",
                            f"Plazo: {garantia.plazo}" if _safe_str(garantia.plazo) else "",
                        ],
                        sep="; ",
                    )
                )
    return {
        "value": _join_unique(values),
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len({_normalize_text(value) for value in values if value}) > 1,
    }


def _resolve_anticipo_garantia(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> dict[str, Any]:
    rows = _find_rows_by_any_keyword(_sheet_rows(records, "04_Garantias", "Garantias"), ["anticipo", "contragar"])
    values = [_format_garantia_row(row) for row in rows if _format_garantia_row(row)]
    if not values:
        hallazgo_rows = _find_rows_by_any_keyword(_sheet_rows(records, "08_Hallazgos_extra", "Hallazgos_Extra"), ["anticipo financiero"])
        values = [_record_value(row, "descripcion", "titulo") for row in hallazgo_rows if _record_value(row, "descripcion", "titulo")]
    if not values:
        for garantia in caso.garantias or []:
            haystack = _normalize_text(garantia.tipo)
            if "anticipo" in haystack or "contragar" in haystack:
                values.append(
                    _join_unique(
                        [
                            _normalize_yes_no(garantia.requerida),
                            garantia.porcentaje or "",
                            f"Base: {garantia.base_calculo}" if _safe_str(garantia.base_calculo) else "",
                        ],
                        sep="; ",
                    )
                )
    return {
        "value": _join_unique(values),
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len({_normalize_text(value) for value in values if value}) > 1,
        "requires_manual": bool(values),
    }


def _resolve_samples_deadline(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> dict[str, Any]:
    requirement_rows = _sheet_rows(records, "03_Requisitos", "Requisitos")
    values = []
    for row in requirement_rows:
        lleva = _normalize_yes_no(_record_value(row, "lleva_muestra"))
        detail = _record_value(row, "detalle_muestra", "momento_presentacion", "observaciones")
        if lleva == "Sí" and detail:
            values.append(detail)
    hallazgo_rows = _find_rows_by_any_keyword(_sheet_rows(records, "08_Hallazgos_extra", "Hallazgos_Extra"), ["muestras"])
    for row in hallazgo_rows:
        description = _record_value(row, "descripcion", "titulo")
        if description:
            values.append(description)
    for requisito in caso.requisitos or []:
        haystack = _normalize_text(" ".join(filter(None, [requisito.descripcion, requisito.momento_presentacion, requisito.medio_presentacion])))
        if "muestra" in haystack:
            values.append(requisito.momento_presentacion or requisito.descripcion or "")
    return {
        "value": _join_unique(values),
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len({_normalize_text(value) for value in values if value}) > 1,
        "requires_manual": bool(values),
    }


def _resolve_text_search(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud, process_data: dict[str, Any]) -> dict[str, Any]:
    keywords = spec.get("keywords", [])
    values = []
    for sheet_name in ("03_Requisitos", "08_Hallazgos_extra", "10_Trazabilidad"):
        for row in _find_rows_by_any_keyword(_sheet_rows(records, sheet_name), keywords):
            value = _record_value(row, "descripcion", "titulo", "valor_extraido", "campo_objetivo", "descripcion_requisito")
            if value:
                values.append(value)
    for key, value in process_data.items():
        haystack = f"{_normalize_text(key)} {_normalize_text(value)}"
        if any(_normalize_text(keyword) in haystack for keyword in keywords):
            values.append(_safe_str(value))
    for hallazgo in caso.hallazgos or []:
        haystack = _normalize_text(" ".join(filter(None, [hallazgo.categoria, hallazgo.hallazgo, hallazgo.accion_sugerida])))
        if any(_normalize_text(keyword) in haystack for keyword in keywords):
            values.append(hallazgo.hallazgo or hallazgo.accion_sugerida or "")
    for requisito in caso.requisitos or []:
        haystack = _normalize_text(" ".join(filter(None, [requisito.descripcion, requisito.categoria])))
        if any(_normalize_text(keyword) in haystack for keyword in keywords):
            values.append(requisito.descripcion or "")
    for traza in caso.trazabilidad or []:
        haystack = _normalize_text(" ".join(filter(None, [traza.campo, traza.valor_extraido, traza.observacion])))
        if any(_normalize_text(keyword) in haystack for keyword in keywords):
            values.append(traza.valor_extraido or traza.observacion or "")
    return {
        "value": _join_unique(values),
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len({_normalize_text(value) for value in values if value}) > 1,
        "requires_manual": bool(values),
    }


def _resolve_payment_term(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], process_data: dict[str, Any], caso: PliegoSolicitud) -> dict[str, Any]:
    value, _ = _get_from_process(process_data, ["condiciones_pago", "forma_pago", "pago", "condicion_pago"])
    requires_manual = False
    if not value:
        value, _ = _get_from_process(process_data, ["anticipo_financiero", "anticipo"])
        requires_manual = bool(value)
    if not value:
        hallazgo_rows = _find_rows_by_any_keyword(_sheet_rows(records, "08_Hallazgos_extra", "Hallazgos_Extra"), ["anticipo", "pago"])
        value = _join_unique([_record_value(row, "descripcion", "titulo") for row in hallazgo_rows])
        requires_manual = bool(value)
    if not value:
        for hallazgo in caso.hallazgos or []:
            haystack = _normalize_text(" ".join(filter(None, [hallazgo.categoria, hallazgo.hallazgo])))
            if "anticipo" in haystack or "pago" in haystack:
                value = hallazgo.hallazgo or ""
                requires_manual = True
                break
    return {
        "value": value,
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "requires_manual": requires_manual,
    }


def _resolve_pliego_cost(spec: dict[str, Any], process_data: dict[str, Any], currency: str) -> dict[str, Any]:
    value, _ = _get_from_process(process_data, ["valor_pliego"])
    if value:
        return {
            "value": value,
            "mapping_status": spec.get("mapping_status"),
            "source_current": spec.get("source_current"),
            "transformation": spec.get("transformation"),
            "format_money": True,
            "currency": currency,
        }
    compound, _ = _get_from_process(process_data, ["requiere_pago_pliego", "pago_pliego", "compra_pliego"])
    parsed = _extract_money_from_text(compound)
    return {
        "value": parsed,
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "format_money": True,
        "currency": currency,
        "requires_manual": bool(parsed and not _normalize_yes_no(compound)),
    }


def _resolve_samples_yes_no(spec: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> dict[str, Any]:
    values = []
    for row in _sheet_rows(records, "03_Requisitos", "Requisitos"):
        sample = _normalize_yes_no(_record_value(row, "lleva_muestra"))
        if sample:
            values.append(sample)
    for row in _sheet_rows(records, "05_Renglones", "Renglones"):
        sample = _normalize_yes_no(_record_value(row, "muestras"))
        if sample:
            values.append(sample)
        elif "sí;" in _normalize_text(_record_value(row, "muestras")):
            values.append("Sí")
    for renglon in caso.renglones or []:
        extra = renglon.datos_extra or {}
        sample = _normalize_yes_no(_safe_str(extra.get("muestras", "")))
        if sample:
            values.append(sample)
    unique = {_normalize_text(value) for value in values if value}
    value = _join_unique(values)
    return {
        "value": value,
        "mapping_status": spec.get("mapping_status"),
        "source_current": spec.get("source_current"),
        "transformation": spec.get("transformation"),
        "ambiguous": len(unique) > 1,
        "normalize_yes_no": len(unique) == 1,
    }


def _build_detail_rows(records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    hallazgo_ampliacion = _resolve_detail_ampliacion(caso, records)
    
    # Prioridad 1: Base de Datos (si hay renglones registrados)
    if caso.renglones:
        for index, renglon in enumerate(caso.renglones, start=1):
            rows.append(
                {
                    "Item": _finalize_field(
                        "Item",
                        {
                            "value": _safe_str(renglon.numero_renglon) or str(renglon.orden or index),
                            "mapping_status": "REGLA NUEVA" if not _safe_str(renglon.numero_renglon) else "RENOMBRAR",
                            "source_current": "PliegoRenglon.numero_renglon | PliegoRenglon.orden",
                            "transformation": "Usar número de renglón o posición persistida",
                            "requires_manual": not bool(_safe_str(renglon.numero_renglon)),
                        },
                    ),
                    "Obj. Gas.": _finalize_field("Obj. Gas.", {}),
                    "Cod. Item": _finalize_field(
                        "Cod. Item",
                        {
                            "value": renglon.codigo_item or "",
                            "mapping_status": "RENOMBRAR",
                            "source_current": "PliegoRenglon.codigo_item",
                            "transformation": "Renombrar código de ítem al nombre RP",
                        },
                    ),
                    "Descripción": _finalize_field(
                        "Descripción",
                        {
                            "value": renglon.descripcion or "",
                            "mapping_status": "OK",
                            "source_current": "PliegoRenglon.descripcion",
                            "transformation": "Sin transformación semántica",
                        },
                    ),
                    "Cant": _finalize_field(
                        "Cant",
                        {
                            "value": renglon.cantidad or "",
                            "mapping_status": "RENOMBRAR",
                            "source_current": "PliegoRenglon.cantidad",
                            "transformation": "Renombrar cantidad a Cant",
                        },
                    ),
                    "Ampliación": _finalize_field(
                        "Ampliación",
                        {
                            "value": hallazgo_ampliacion.get("value", ""),
                            "mapping_status": "REGLA NUEVA",
                            "source_current": "08_Hallazgos_extra modificación de cantidades",
                            "transformation": "Propagar regla general de ampliación/reducción al detalle RP",
                            "requires_manual": hallazgo_ampliacion.get("requires_manual", False),
                        },
                    ),
                }
            )
        return rows

    # Prioridad 2: Excel en disco (legacy fallback)
    excel_rows = _sheet_rows(records, "05_Renglones", "Renglones")
    if excel_rows:
        for index, row in enumerate(excel_rows, start=1):
            item_value = _record_value(row, "renglon_nro", "numero_renglon")
            item_requires_manual = False
            if _is_missing_value(item_value):
                item_value = str(index)
                item_requires_manual = True
            rows.append(
                {
                    "Item": _finalize_field(
                        "Item",
                        {
                            "value": item_value,
                            "mapping_status": "REGLA NUEVA" if item_requires_manual else "RENOMBRAR",
                            "source_current": "05_Renglones.renglon_nro | orden de fila",
                            "transformation": "Usar número de renglón o la posición cargada",
                            "requires_manual": item_requires_manual,
                        },
                    ),
                    "Obj. Gas.": _finalize_field("Obj. Gas.", {}),
                    "Cod. Item": _finalize_field(
                        "Cod. Item",
                        {
                            "value": _record_value(row, "codigo_item"),
                            "mapping_status": "RENOMBRAR",
                            "source_current": "05_Renglones.codigo_item",
                            "transformation": "Renombrar código de ítem al nombre RP",
                        },
                    ),
                    "Descripción": _finalize_field(
                        "Descripción",
                        {
                            "value": _record_value(row, "descripcion"),
                            "mapping_status": "OK",
                            "source_current": "05_Renglones.descripcion",
                            "transformation": "Sin transformación semántica",
                        },
                    ),
                    "Cant": _finalize_field(
                        "Cant",
                        {
                            "value": _record_value(row, "cantidad"),
                            "mapping_status": "RENOMBRAR",
                            "source_current": "05_Renglones.cantidad",
                            "transformation": "Renombrar cantidad a Cant",
                        },
                    ),
                    "Ampliación": _finalize_field(
                        "Ampliación",
                        {
                            "value": hallazgo_ampliacion.get("value", ""),
                            "mapping_status": "REGLA NUEVA",
                            "source_current": "08_Hallazgos_extra modificación de cantidades",
                            "transformation": "Propagar regla general de ampliación/reducción al detalle RP",
                            "requires_manual": hallazgo_ampliacion.get("requires_manual", False),
                        },
                    ),
                }
            )
        return rows

    for index, renglon in enumerate(caso.renglones or [], start=1):
        rows.append(
            {
                "Item": _finalize_field(
                    "Item",
                    {
                        "value": _safe_str(renglon.numero_renglon) or str(renglon.orden or index),
                        "mapping_status": "REGLA NUEVA" if not _safe_str(renglon.numero_renglon) else "RENOMBRAR",
                        "source_current": "PliegoRenglon.numero_renglon | PliegoRenglon.orden",
                        "transformation": "Usar número de renglón o posición persistida",
                        "requires_manual": not bool(_safe_str(renglon.numero_renglon)),
                    },
                ),
                "Obj. Gas.": _finalize_field("Obj. Gas.", {}),
                "Cod. Item": _finalize_field(
                    "Cod. Item",
                    {
                        "value": renglon.codigo_item or "",
                        "mapping_status": "RENOMBRAR",
                        "source_current": "PliegoRenglon.codigo_item",
                        "transformation": "Renombrar código de ítem al nombre RP",
                    },
                ),
                "Descripción": _finalize_field(
                    "Descripción",
                    {
                        "value": renglon.descripcion or "",
                        "mapping_status": "OK",
                        "source_current": "PliegoRenglon.descripcion",
                        "transformation": "Sin transformación semántica",
                    },
                ),
                "Cant": _finalize_field(
                    "Cant",
                    {
                        "value": renglon.cantidad or "",
                        "mapping_status": "RENOMBRAR",
                        "source_current": "PliegoRenglon.cantidad",
                        "transformation": "Renombrar cantidad a Cant",
                    },
                ),
                "Ampliación": _finalize_field(
                    "Ampliación",
                    {
                        "value": hallazgo_ampliacion.get("value", ""),
                        "mapping_status": "REGLA NUEVA",
                        "source_current": "08_Hallazgos_extra modificación de cantidades",
                        "transformation": "Propagar regla general de ampliación/reducción al detalle RP",
                        "requires_manual": hallazgo_ampliacion.get("requires_manual", False),
                    },
                ),
            }
        )
    return rows


def _resolve_field(label: str, spec: dict[str, Any], process_data: dict[str, Any], records: dict[str, list[dict[str, str]]], caso: PliegoSolicitud, currency: str) -> dict[str, Any]:
    resolver = spec.get("resolver")
    if resolver == "process_direct":
        return _finalize_field(label, _resolve_process_direct(spec, process_data, caso, currency))
    if resolver == "cronograma_event":
        return _finalize_field(label, _resolve_cronograma_event(spec, records, caso))
    if resolver == "case_datetime":
        return _finalize_field(label, _resolve_case_datetime(spec, caso))
    if resolver == "document_link":
        return _finalize_field(label, _resolve_document_link(spec, records, caso))
    if resolver == "renglon_yes_no":
        return _finalize_field(label, _resolve_renglon_yes_no(spec, records, caso))
    if resolver == "delivery_condition":
        return _finalize_field(label, _resolve_delivery_condition(spec, records, process_data, caso))
    if resolver == "garantia_match":
        return _finalize_field(label, _resolve_garantia_match(spec, records, caso))
    if resolver == "anticipo_garantia":
        return _finalize_field(label, _resolve_anticipo_garantia(spec, records, caso))
    if resolver == "samples_deadline":
        return _finalize_field(label, _resolve_samples_deadline(spec, records, caso))
    if resolver == "text_search":
        return _finalize_field(label, _resolve_text_search(spec, records, caso, process_data))
    if resolver == "payment_term":
        return _finalize_field(label, _resolve_payment_term(spec, records, process_data, caso))
    if resolver == "pliego_cost":
        return _finalize_field(label, _resolve_pliego_cost(spec, process_data, currency))
    if resolver == "samples_yes_no":
        return _finalize_field(label, _resolve_samples_yes_no(spec, records, caso))
    return _finalize_field(label, {})


def _build_validation(entries: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        "COMPLETO": 0,
        "AMBIGUO": 0,
        "VALIDACION_MANUAL": 0,
        "FALTANTE_REAL": 0,
    }
    for entry in entries:
        counts[entry["coverage_status"]] = counts.get(entry["coverage_status"], 0) + 1
    total = len(entries)
    complete_pct = int(round((counts["COMPLETO"] / total) * 100)) if total else 0
    return {
        "total_campos_rp": total,
        "completos": counts["COMPLETO"],
        "ambiguos": counts["AMBIGUO"],
        "requieren_validacion_manual": counts["VALIDACION_MANUAL"],
        "faltantes_reales": counts["FALTANTE_REAL"],
        "porcentaje_completitud": complete_pct,
    }


def _group_validation(entries: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for group_name in RP_GROUPS:
        group_entries = [entry for entry in entries if entry.get("group") == group_name]
        result[group_name] = _build_validation(group_entries)
    return result


def _detail_export_rows(detail_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    export_rows = []
    for row in detail_rows:
        export_rows.append({field: row[field]["export_value"] for field in RP_DETAIL_FIELDS})
    return export_rows


def build_rp_output(caso: PliegoSolicitud) -> dict[str, Any]:
    records = _load_active_excel_records(caso)
    process_data = _build_process_data(caso, records)
    currency, _ = _get_from_process(process_data, ["moneda", "divisa", "currency"])
    currency = currency or "ARS"

    general_fields = [_resolve_field(label, FIELD_SPECS[label], process_data, records, caso, currency) for label in RP_GENERAL_FIELDS]
    matrix_fields = [_resolve_field(label, FIELD_SPECS[label], process_data, records, caso, currency) for label in RP_MATRIX_FIELDS]
    detail_rows = _build_detail_rows(records, caso)

    detail_entries = []
    if detail_rows:
        for row in detail_rows:
            detail_entries.extend(row[field] for field in RP_DETAIL_FIELDS)
    else:
        for field in RP_DETAIL_FIELDS:
            detail_entries.append(
                _finalize_field(
                    field,
                    {
                        "mapping_status": FIELD_SPECS.get(field, {}).get("mapping_status", "FALTANTE REAL"),
                        "source_current": FIELD_SPECS.get(field, {}).get("source_current", ""),
                        "transformation": FIELD_SPECS.get(field, {}).get("transformation", ""),
                    },
                )
            )

    all_entries = general_fields + detail_entries + matrix_fields
    process_extra_fields = {
        key: value
        for key, value in process_data.items()
        if not _is_missing_value(value) and _normalize_key(key) not in RP_PROCESS_KEYS_USED
    }

    mapping_matrix = []
    for label in RP_GENERAL_FIELDS + RP_DETAIL_FIELDS + RP_MATRIX_FIELDS:
        spec = FIELD_SPECS.get(label, {})
        instance = next((entry for entry in all_entries if entry["label"] == label), None)
        mapping_matrix.append(
            {
                "campo_rp": label,
                "grupo": spec.get("group", ""),
                "fuente_actual": spec.get("source_current", ""),
                "transformacion": spec.get("transformation", ""),
                "estado": spec.get("mapping_status", "REGLA NUEVA"),
                "estado_resultado": instance.get("coverage_status", "FALTANTE_REAL") if instance else "FALTANTE_REAL",
            }
        )

    return {
        "schema": RP_GROUPS,
        "general_fields": general_fields,
        "detail_fields": RP_DETAIL_FIELDS,
        "detail_rows": detail_rows,
        "matrix_fields": matrix_fields,
        "validation": _build_validation(all_entries),
        "validation_by_group": _group_validation(all_entries),
        "mapping_matrix": mapping_matrix,
        "general_export": {field["label"]: field["export_value"] for field in general_fields},
        "detail_export": _detail_export_rows(detail_rows),
        "matrix_export": {field["label"]: field["export_value"] for field in matrix_fields},
        "process_extra_fields": process_extra_fields,
        "active_excel_sheets": list(records.keys()),
        "dual_data": _get_raw_dual_data(caso),
    }


# Campos críticos de Fusión que bloquean listo_para_fusion si tienen estado incompleto.
# La clave es el label RP tal como aparece en FIELD_SPECS.
_FUSION_CRITICAL_FIELDS: list[str] = [
    "N° Procesos",
    "Unidad Ejecutora",
    "Nombre Proceso",
    "Objeto Contratación",
    "Procedimiento Selección",
    "Tipo Cotización",
    "Tipo Adjudicación",
    "Fecha Acto Apertura",
    "Monto",
    "Moneda",
]
# Campos críticos adicionales solo cuando existen renglones
_FUSION_CRITICAL_RENGLON_FIELDS: list[str] = [
    "Descripción",
    "Cant",
]
# Estados que bloquean Fusión en un campo crítico
_FUSION_BLOCKING_STATUSES: set[str] = {
    "FALTANTE_REAL",
    "AMBIGUO",
    "VALIDACION_MANUAL",
}


def _compute_fusion_status(caso: PliegoSolicitud, n_renglones: int) -> tuple[str, str]:
    """
    Evalúa si el caso está listo para carga en Fusión.
    Bloquea con 'NO' o 'PARCIAL' si hay campos críticos con estados problemáticos.
    Retorna (listo_para_fusion, motivo_bloqueo).
    """
    if n_renglones == 0:
        return "NO", "Sin renglones registrados"

    try:
        rp = build_rp_output(caso)
    except Exception:
        return "PARCIAL", "Error al evaluar campos críticos"

    gen_idx = {f["label"]: f for f in rp["general_fields"]}
    # Incluir renglones solo si existen
    detail_idx: dict[str, dict] = {}
    if rp["detail_rows"]:
        for row in rp["detail_rows"]:
            for lbl, cell in row.items():
                if lbl in _FUSION_CRITICAL_RENGLON_FIELDS:
                    # Pesimista: si algún renglón falla en ese campo, se bloquea
                    prev = detail_idx.get(lbl)
                    if prev is None or cell.get("coverage_status", "FALTANTE_REAL") in _FUSION_BLOCKING_STATUSES:
                        detail_idx[lbl] = cell

    blocking: list[str] = []

    for label in _FUSION_CRITICAL_FIELDS:
        field = gen_idx.get(label)
        if field is None:
            blocking.append(label)
            continue
        if field.get("coverage_status", "FALTANTE_REAL") in _FUSION_BLOCKING_STATUSES:
            blocking.append(label)

    if n_renglones > 0:
        for label in _FUSION_CRITICAL_RENGLON_FIELDS:
            field = detail_idx.get(label)
            if field is None or field.get("coverage_status", "FALTANTE_REAL") in _FUSION_BLOCKING_STATUSES:
                blocking.append(f"{label} (renglones)")

    # Fecha apertura extra: hora 00:00:00 cuenta como bloqueo parcial
    fecha_field = gen_idx.get("Fecha Acto Apertura", {})
    display_fecha = fecha_field.get("display_value", "") or ""
    if display_fecha.endswith("00:00:00") and "Fecha Acto Apertura" not in blocking:
        blocking.append("Fecha Acto Apertura (hora no identificada)")

    if not blocking:
        return "SI", ""
    motivo = "Campos pendientes: " + ", ".join(blocking)
    # Si los únicos bloqueos son hora ambigua o validacion_manual → PARCIAL; si hay FALTANTE_REAL → NO
    statuses_criticos = {
        gen_idx.get(lbl, {}).get("coverage_status", "FALTANTE_REAL")
        for lbl in _FUSION_CRITICAL_FIELDS
        if lbl in blocking and lbl in gen_idx
    }
    if "FALTANTE_REAL" in statuses_criticos:
        return "NO", motivo
    return "PARCIAL", motivo


def _get_raw_dual_data(caso: PliegoSolicitud) -> dict[str, list[dict[str, Any]]]:
    """Obtiene los datos crudos de las 14 hojas para el export dual."""
    data = {}

    # 1. Proceso
    proceso_data = {}
    if caso.datos_proceso and caso.datos_proceso.datos:
        proceso_data = dict(caso.datos_proceso.datos)
    data["Proceso"] = [proceso_data] if proceso_data else []

    # 2. Cronograma
    data["Cronograma"] = [
        {
            "hito": c.hito,
            "fecha": c.fecha,
            "hora": c.hora,
            "lugar_medio": c.lugar_medio,
            "estado_dato": c.estado_dato,
            "fuente": c.fuente,
        }
        for c in (caso.cronograma or [])
    ]

    # 3. Requisitos
    data["Requisitos"] = [
        {
            "categoria": r.categoria,
            "descripcion": r.descripcion,
            "obligatorio": r.obligatorio,
            "momento_presentacion": r.momento_presentacion,
            "medio_presentacion": r.medio_presentacion,
            "vigencia": r.vigencia,
            "estado_dato": r.estado_dato,
            "fuente": r.fuente,
        }
        for r in (caso.requisitos or [])
    ]

    # 4. Garantias
    data["Garantias"] = [
        {
            "tipo": g.tipo,
            "requerida": g.requerida,
            "porcentaje": g.porcentaje,
            "base_calculo": g.base_calculo,
            "plazo": g.plazo,
            "formas_admitidas": g.formas_admitidas,
            "estado_dato": g.estado_dato,
            "fuente": g.fuente,
        }
        for g in (caso.garantias or [])
    ]

    # 5. Renglones
    data["Renglones"] = [
        {
            "orden": r.orden,
            "numero_renglon": r.numero_renglon,
            "codigo_item": r.codigo_item,
            "descripcion": r.descripcion,
            "cantidad": r.cantidad,
            "unidad": r.unidad,
            "destino_efector": r.destino_efector,
            "entrega_parcial": r.entrega_parcial,
            "obs_tecnicas": r.obs_tecnicas,
            "estado": r.estado,
        }
        for r in (caso.renglones or [])
    ]

    # 6. Documentos
    data["Documentos"] = [
        {
            "nombre": d.nombre,
            "tipo": d.tipo,
            "rol": d.rol,
            "obligatorio": d.obligatorio,
            "estado_lectura": d.estado_lectura,
            "fecha": d.fecha,
        }
        for d in (caso.documentos_pliego or [])
    ]

    # 7. Actos_Administrativos
    data["Actos_Administrativos"] = [
        {
            "tipo_acto": a.tipo_acto,
            "numero": a.numero,
            "numero_especial": a.numero_especial,
            "fecha": a.fecha,
            "organismo_emisor": a.organismo_emisor,
            "descripcion": a.descripcion,
        }
        for a in (caso.actos_admin or [])
    ]

    # 8. Hallazgos_Extra
    data["Hallazgos_Extra"] = [
        {
            "categoria": h.categoria,
            "hallazgo": h.hallazgo,
            "impacto": h.impacto,
            "accion_sugerida": h.accion_sugerida,
            "fuente": h.fuente,
        }
        for h in (caso.hallazgos or [])
    ]

    # 9. Faltantes_y_Dudas
    data["Faltantes_y_Dudas"] = [
        {
            "campo_objetivo": f.campo_objetivo,
            "motivo": f.motivo,
            "detalle": f.detalle,
            "criticidad": f.criticidad,
            "accion_recomendada": f.accion_recomendada,
            "estado": f.estado,
        }
        for f in (caso.faltantes or [])
    ]

    # 10. Trazabilidad
    data["Trazabilidad"] = [
        {
            "campo": t.campo,
            "valor_extraido": t.valor_extraido,
            "documento_fuente": t.documento_fuente,
            "pagina_seccion": t.pagina_seccion,
            "tipo_evidencia": t.tipo_evidencia,
            "observacion": t.observacion,
        }
        for t in (caso.trazabilidad or [])
    ]

    # 11. Fusion_Cabecera
    # Si hay registro propio, usarlo. Si no, usar datos_proceso como fallback
    # (datos_proceso contiene toda la metadata del proceso y es la fuente
    #  natural de cabecera para la fusión con SIEM).
    if caso.fusion_cabecera and caso.fusion_cabecera.datos:
        data["Fusion_Cabecera"] = [dict(caso.fusion_cabecera.datos)]
    elif caso.datos_proceso and caso.datos_proceso.datos:
        data["Fusion_Cabecera"] = [dict(caso.datos_proceso.datos)]
    else:
        data["Fusion_Cabecera"] = []

    # 12. Fusion_Renglones
    data["Fusion_Renglones"] = [
        {
            "numero_renglon": r.numero_renglon,
            "codigo_item": r.codigo_item,
            "descripcion": r.descripcion,
            "cantidad": r.cantidad,
            "unidad": r.unidad,
            "precio_unitario_estimado": r.precio_unitario_estimado,
            **(dict(r.datos_extra) if r.datos_extra else {}),
        }
        for r in (caso.fusion_renglones or [])
    ]

    # 13. SIEM_Analitica
    # Solo se puebla si existe un registro pliego_analitica generado externamente.
    # Si está vacía, indica que la analítica SIEM no fue generada aún para este caso.
    data["SIEM_Analitica"] = [dict(caso.analitica.datos)] if (caso.analitica and caso.analitica.datos) else []

    # 14. Control_Carga
    # Si hay registro propio, usarlo. Si no, generar fila de control desde el
    # estado actual del caso para que la hoja nunca quede completamente vacía.
    if caso.control_carga and caso.control_carga.datos:
        data["Control_Carga"] = [dict(caso.control_carga.datos)]
    else:
        n_renglones = len(data["Renglones"])
        n_fusion_ren = len(data["Fusion_Renglones"])
        tiene_cabecera = bool(data["Fusion_Cabecera"])
        tiene_analitica = bool(data["SIEM_Analitica"])
        listo, motivo = _compute_fusion_status(caso, n_renglones)
        data["Control_Carga"] = [{
            "caso_id": caso.id,
            "titulo": caso.titulo or caso.nombre_licitacion or "",
            "estado_caso": caso.estado or "",
            "renglones_registrados": n_renglones,
            "fusion_renglones_listos": n_fusion_ren,
            "fusion_cabecera_lista": "SI" if tiene_cabecera else "NO",
            "siem_analitica_lista": "SI" if tiene_analitica else "NO",
            "listo_para_fusion": listo,
            "motivo_bloqueo": motivo,
        }]

    return data


def export_rp_excel_bytes(rp_output: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([rp_output["general_export"]], columns=RP_GENERAL_FIELDS).to_excel(
            writer,
            index=False,
            sheet_name=GROUP_GENERAL,
        )
        pd.DataFrame(rp_output["detail_export"], columns=RP_DETAIL_FIELDS).to_excel(
            writer,
            index=False,
            sheet_name=GROUP_DETAIL,
        )
        pd.DataFrame([rp_output["matrix_export"]], columns=RP_MATRIX_FIELDS).to_excel(
            writer,
            index=False,
            sheet_name=GROUP_MATRIX,
        )
    buffer.seek(0)
    return buffer.getvalue()


# =============================================================================
# CAPA CANÓNICA – Pantalla principal y Excel descargable
# Filtra, ordena y formatea solo los campos pedidos por el usuario.
# No modifica la lógica base de extracción/consolidación.
# =============================================================================

# A. Datos del proceso
MAIN_GENERAL_FIELDS = [
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
    "Nro. Expediente",
    "Modalidad",
    "Link",
]

# B. Detalle de renglones
MAIN_DETAIL_FIELDS = [
    "Item",
    "Obj. Gas.",
    "Cod. Item",
    "Descripción",
    "Cant",
]

# C. Campos complementarios visibles en pantalla principal
MAIN_COMPLEMENTARY_FIELDS = [
    "Lugar y Cond. de Entrega",
    "Fecha limite para presentación de muestras",
    "Retira en",
]

# ---------- Tablas de normalización ERP ----------

_MONEDA_ERP: dict[str, str] = {
    "ars": "ARS - Peso Argentino",
    "peso": "ARS - Peso Argentino",
    "peso argentino": "ARS - Peso Argentino",
    "pesos": "ARS - Peso Argentino",
    "pesos argentinos": "ARS - Peso Argentino",
    "usd": "USD - Dólar estadounidense",
    "dolar": "USD - Dólar estadounidense",
    "dólar": "USD - Dólar estadounidense",
    "dolares": "USD - Dólar estadounidense",
    "dolar estadounidense": "USD - Dólar estadounidense",
    "eur": "EUR - Euro",
    "euro": "EUR - Euro",
    "euros": "EUR - Euro",
}

_PROCEDIMIENTO_ERP: dict[str, str] = {
    # Licitación pública y variantes
    "licitacion publica": "Licitación pública",
    "licitacion publica nacional": "Licitación pública nacional",
    "licitacion publica internacional": "Licitación pública internacional",
    "licitacion publica nacional e internacional": "Licitación pública nacional e internacional",
    "licitacion publica abreviada": "Licitación pública abreviada",
    "lp": "Licitación pública",
    "licitacion": "Licitación pública",
    # Licitación privada
    "licitacion privada": "Licitación privada",
    # Contratación directa y variantes
    "contratacion directa": "Contratación directa",
    "contratacion directa por excepcion": "Contratación directa por excepción",
    "compra directa": "Contratación directa",
    "contratacion por excepcion": "Contratación directa por excepción",
    "excepcion": "Contratación directa por excepción",
    # Contratación menor / compra menor
    "contratacion menor": "Contratación menor",
    "compra menor": "Compra menor",
    # Concurso de precios
    "concurso de precios": "Concurso de precios",
    "concurso": "Concurso de precios",
    # Compulsa abreviada
    "compulsa abreviada": "Compulsa abreviada",
    "compulsa": "Compulsa abreviada",
    # Urgencia / emergencia
    "contratacion de urgencia": "Contratación de urgencia",
    "urgencia": "Contratación de urgencia",
    "contratacion de emergencia": "Contratación de emergencia",
    "emergencia": "Contratación de emergencia",
    # Precio testigo
    "precio testigo": "Precio testigo",
    # Subasta inversa
    "subasta inversa": "Subasta inversa",
    "subasta": "Subasta inversa",
}

_COTIZACION_ERP: dict[str, str] = {
    # Plural: por renglones
    "por renglones parcial": "Por renglones: parcial",
    "renglones parcial": "Por renglones: parcial",
    "por renglones total": "Por renglones: total",
    "renglones total": "Por renglones: total",
    "por renglones": "Por renglones: parcial",   # sin especificar → parcial por defecto
    # Singular: por renglón
    "por renglon parcial": "Por renglones: parcial",
    "renglon parcial": "Por renglones: parcial",
    "por renglon total": "Por renglones: total",
    "renglon total": "Por renglones: total",
    "por renglon": "Por renglones: parcial",
    # Global
    "global": "Global",
    "por oferta global": "Por oferta global",
    "oferta global": "Por oferta global",
    # Lote
    "lote": "Por lote",
    "por lote": "Por lote",
    "por lotes": "Por lote",
    "lotes": "Por lote",
    # Adjudicación — misma familia de valores ERP
    "adjudicacion por renglones parcial": "Por renglones: parcial",
    "adjudicacion por renglones total": "Por renglones: total",
    "adjudicacion global": "Global",
    "adjudicacion por lote": "Por lote",
}

# ---------- Funciones de formato ERP ----------

def _fmt_erp_fecha(val: Any) -> str:
    """Normaliza fecha al formato ERP: DD/MM/YYYY HH:MM:SS.
    Retorna "" si no se puede parsear ningún patrón de fecha.
    """
    text = _safe_str(val).strip()
    if not text or _is_missing_value(text):
        return ""
    # Ya tiene formato DD/MM/YYYY…
    if re.match(r"\d{2}/\d{2}/\d{4}", text):
        if re.search(r"\d{2}:\d{2}:\d{2}", text):
            return text
        if re.search(r"\d{2}:\d{2}", text):
            return text + ":00"
        return text + " 00:00:00"
    # Formato YYYY-MM-DD[T HH:MM[:SS]]
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?", text)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        h = m.group(4) or "00"
        mi = m.group(5) or "00"
        s = m.group(6) or "00"
        return f"{d}/{mo}/{y} {h}:{mi}:{s}"
    # Fecha+hora separados por espacio (DD/MM/YYYY HH:MM sin segundos)
    if re.match(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}$", text):
        return text + ":00"
    # Intento de extracción desde texto narrativo (ej: "Apertura: 15/03/2024 14:30hs")
    m2 = re.search(r"(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})", text)
    if m2:
        d2 = m2.group(1).zfill(2)
        mo2 = m2.group(2).zfill(2)
        y2 = m2.group(3) if len(m2.group(3)) == 4 else "20" + m2.group(3)
        rest = text[m2.end():]
        t_m = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", rest)
        if t_m:
            h2 = t_m.group(1).zfill(2)
            mi2 = t_m.group(2)
            s2 = t_m.group(3) or "00"
            return f"{d2}/{mo2}/{y2} {h2}:{mi2}:{s2}"
        return f"{d2}/{mo2}/{y2} 00:00:00"
    # No se encontró ningún patrón de fecha → indicar al caller que no se pudo normalizar
    return ""


def _fmt_erp_monto(val: Any) -> str:
    """Normaliza monto al formato ERP: 148.869.642,00 (sin prefijo de moneda)"""
    parsed = _parse_decimal(val)
    if parsed is None:
        return _safe_str(val)
    fmt = f"{parsed:,.2f}"          # "148,869,642.00"
    return fmt.replace(",", "X").replace(".", ",").replace("X", ".")  # "148.869.642,00"


def _fmt_erp_moneda(val: Any) -> str:
    """Normaliza moneda al formato ERP: ARS - Peso Argentino"""
    text = _safe_str(val).strip()
    if not text:
        return text
    return _MONEDA_ERP.get(_normalize_text(text), text)


def _fmt_erp_duracion(val: Any) -> str:
    """Normaliza duración al formato ERP: 6 Meses, 180 Días corridos, 15 Días hábiles"""
    text = _safe_str(val).strip()
    if not text:
        return text
    low = _normalize_text(text)
    m = re.match(r"(\d+)\s*(.+)", low)
    if m:
        num = m.group(1)
        unit = m.group(2).strip()
        if "mes" in unit:
            return f"{num} Meses"
        if "habil" in unit:
            return f"{num} Días hábiles"
        if "corrid" in unit:
            return f"{num} Días corridos"
        if "semana" in unit:
            return f"{num} Semanas"
        if "ano" in unit or "año" in unit:
            return f"{num} Años"
        if "dia" in unit or "día" in unit:
            return f"{num} Días"
    return text


def _fmt_erp_procedimiento(val: Any) -> str:
    """Normaliza Procedimiento Selección al estilo ERP."""
    text = _safe_str(val).strip()
    if not text:
        return text
    # Strip punctuation before lookup (dashes, parens, slashes, colons)
    key = re.sub(r"[:\-\(\)/,]", " ", _normalize_text(text))
    key = re.sub(r"\s+", " ", key).strip()
    return _PROCEDIMIENTO_ERP.get(key, text)


def _fmt_erp_tipo_cot(val: Any) -> str:
    """Normaliza Tipo Cotización / Tipo Adjudicación al estilo ERP."""
    text = _safe_str(val).strip()
    if not text:
        return text
    # Strip punctuation and normalize before lookup (colons, dashes, parens, slashes)
    key = re.sub(r"[:\-\(\)/,]", " ", _normalize_text(text))
    key = re.sub(r"\s+", " ", key).strip()
    return _COTIZACION_ERP.get(key, text)


def _fmt_erp_cant_oferta(val: Any) -> str:
    """Normaliza Cant. Oferta Permitidas al estilo ERP."""
    text = _safe_str(val).strip()
    if not text:
        return text
    low = _normalize_text(text)
    if (
        low in {"1", "una", "uno"}
        or "no acepta mas de una" in low
        or "una sola" in low
        or "solo una" in low
        or "no acepta mas" in low
    ):
        return "No acepta más de una oferta"
    return text


def _fmt_erp_si_no(val: Any) -> str:
    """Normaliza Si/No al estilo ERP (sin tilde).
    Retorna "" si el valor no es normalizable a Sí/No.
    """
    norm = _normalize_yes_no(val)
    if norm == "Sí":
        return "Si"
    if norm == "No":
        return "No"
    return ""  # No normalizable → el caller lo escalará a VALIDACION_MANUAL


def _fmt_erp_cant(val: Any) -> str:
    """Normaliza Cant con 2 decimales: 4.00"""
    parsed = _parse_decimal(val)
    if parsed is None:
        return _safe_str(val)
    return f"{parsed:.2f}"


def _fmt_erp_lugar_entrega(val: Any) -> str:
    """Para Lugar y Cond. de Entrega: muestra solo el primer segmento, máx 250 chars."""
    text = _safe_str(val).strip()
    if not text:
        return text
    # El resolver puede unir varias filas con " | "; tomamos solo el primero
    first = text.split(" | ")[0].strip()
    return first[:250] + "…" if len(first) > 250 else first


def _fmt_erp_fecha_muestra(val: Any) -> str:
    """Intenta parsear la fecha límite de muestras; si no es parseable retorna ""."""
    return _fmt_erp_fecha(val)


# ---------------------------------------------------------------------------
# Valores dummy / placeholder que deben descartarse de la pantalla principal
# ---------------------------------------------------------------------------
_DUMMY_PHRASES: frozenset[str] = frozenset(
    {
        "prueba", "test", "testing", "mock", "dummy", "placeholder",
        "texto mock", "valor de prueba", "dato de prueba", "ejemplo",
        "completar", "pendiente", "en blanco", "xxx", "aaa", "bbb",
        "campo esperado", "texto normalizado", "descripcion normalizada",
        "si/no", "si / no", "alto/medio/bajo",
    }
)


def _is_canonical_dummy(value: Any) -> bool:
    """Detecta valores ficticio/placeholder que no deben mostrarse en pantalla."""
    text = _normalize_text(value)
    return text in _DUMMY_PHRASES


# Mapa label → función de formato ERP
_ERP_FORMATTERS: dict[str, Any] = {
    "Fecha Acto Apertura":                        _fmt_erp_fecha,
    "Fecha Inicio Consulta":                       _fmt_erp_fecha,
    "Fecha Final Consulta":                        _fmt_erp_fecha,
    "Monto":                                       _fmt_erp_monto,
    "Moneda":                                      _fmt_erp_moneda,
    "Duración Contrato":                           _fmt_erp_duracion,
    "Plazo Mantenimiento Oferta":                  _fmt_erp_duracion,
    "Procedimiento Selección":                     _fmt_erp_procedimiento,
    "Tipo Cotización":                             _fmt_erp_tipo_cot,
    "Tipo Adjudicación":                           _fmt_erp_tipo_cot,
    "Cant. Oferta Permitidas":                     _fmt_erp_cant_oferta,
    "Acepta Redeterminación":                      _fmt_erp_si_no,
    "Acepta Prórroga":                             _fmt_erp_si_no,
    "Cant":                                        _fmt_erp_cant,
    "Lugar y Cond. de Entrega":                    _fmt_erp_lugar_entrega,
    "Fecha limite para presentación de muestras":  _fmt_erp_fecha_muestra,
}

# Campos donde un valor no-normalizable debe escalarse a VALIDACION_MANUAL
# (en lugar de mostrarse como texto libre)
_ERP_STRICT_FIELDS: frozenset[str] = frozenset(
    {
        "Fecha Acto Apertura",
        "Fecha Inicio Consulta",
        "Fecha Final Consulta",
        "Acepta Redeterminación",
        "Acepta Prórroga",
        "Fecha limite para presentación de muestras",
    }
)


def _apply_erp_format(label: str, field_data: dict[str, Any]) -> dict[str, Any]:
    """Aplica formato ERP, detecta dummies y escala campos estrictos no normalizables."""
    raw = field_data.get("value", "")
    current_status = field_data.get("coverage_status", "FALTANTE_REAL")

    # 1. Valores dummy → descartar como FALTANTE_REAL
    if current_status != "FALTANTE_REAL" and raw and _is_canonical_dummy(raw):
        result = dict(field_data)
        result["coverage_status"] = "FALTANTE_REAL"
        result["display_value"] = "Faltante real"
        result["export_value"] = ""
        return result

    fmt_fn = _ERP_FORMATTERS.get(label)
    if not fmt_fn:
        return field_data
    if current_status == "FALTANTE_REAL":
        return field_data
    if not raw or _is_missing_value(raw):
        return field_data

    formatted = fmt_fn(raw)

    # 2. Formatter no pudo normalizar (retornó "")
    if not formatted:
        if label in _ERP_STRICT_FIELDS:
            # Escalar a VALIDACION_MANUAL y limpiar display para no mostrar texto libre
            result = dict(field_data)
            if result["coverage_status"] not in {"AMBIGUO", "VALIDACION_MANUAL"}:
                result["coverage_status"] = "VALIDACION_MANUAL"
            result["display_value"] = ""
            result["export_value"] = ""
            return result
        # Campo no estricto: dejar el valor original intacto
        return field_data

    # 3. Formatter normalizó con éxito
    if formatted != raw:
        result = dict(field_data)
        result["display_value"] = formatted
        result["export_value"] = formatted
        # Para fechas: si la hora es 00:00:00 marcar como ambigua (puede ser default, no real)
        if label == "Fecha Acto Apertura" and formatted.endswith("00:00:00"):
            result["hora_no_identificada"] = True
        return result

    # Fecha sin cambio de formato: verificar igual si la hora es 00:00:00
    if label == "Fecha Acto Apertura" and re.search(r"00:00:00", _safe_str(formatted)):
        result = dict(field_data)
        result["hora_no_identificada"] = True
        return result

    return field_data


def _canonical_validation(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Valida solo los campos de la pantalla principal."""
    counts: dict[str, int] = {"COMPLETO": 0, "AMBIGUO": 0, "VALIDACION_MANUAL": 0, "FALTANTE_REAL": 0}
    for e in entries:
        k = e.get("coverage_status", "FALTANTE_REAL")
        counts[k] = counts.get(k, 0) + 1
    total = len(entries)
    pct = int(round(counts["COMPLETO"] / total * 100)) if total else 0
    return {
        "total_campos": total,
        "completos": counts["COMPLETO"],
        "ambiguos": counts["AMBIGUO"],
        "requieren_validacion_manual": counts["VALIDACION_MANUAL"],
        "faltantes_reales": counts["FALTANTE_REAL"],
        "porcentaje_completitud": pct,
    }


def build_canonical_output(caso: PliegoSolicitud) -> dict[str, Any]:
    """
    Capa canónica para la pantalla principal.
    Llama a build_rp_output() y filtra/formatea solo los campos del listado
    aprobado. No modifica la lógica base de extracción ni consolidación.
    """
    base = build_rp_output(caso)

    gen_idx = {f["label"]: f for f in base["general_fields"]}
    mat_idx = {f["label"]: f for f in base["matrix_fields"]}

    # A. Datos del proceso (20 campos, sin Origen / Ult. Actualización / Alternativa)
    main_general = [
        _apply_erp_format(lbl, gen_idx[lbl])
        for lbl in MAIN_GENERAL_FIELDS
        if lbl in gen_idx
    ]

    # B. Detalle de renglones (5 columnas, sin Ampliación)
    main_detail_rows: list[dict[str, Any]] = []
    for row in base["detail_rows"]:
        filtered: dict[str, Any] = {}
        for lbl in MAIN_DETAIL_FIELDS:
            cell = row.get(lbl) or _finalize_field(lbl, {})
            filtered[lbl] = _apply_erp_format(lbl, cell)
        main_detail_rows.append(filtered)

    # C. Campos complementarios (3 campos del grupo Matriz)
    main_complementary = [
        _apply_erp_format(lbl, mat_idx[lbl])
        for lbl in MAIN_COMPLEMENTARY_FIELDS
        if lbl in mat_idx
    ]

    # Validación solo sobre campos de la pantalla principal
    all_entries: list[dict[str, Any]] = list(main_general)
    for row in main_detail_rows:
        all_entries.extend(row[lbl] for lbl in MAIN_DETAIL_FIELDS if lbl in row)
    all_entries.extend(main_complementary)

    validation = _canonical_validation(all_entries)

    return {
        "general_fields": main_general,
        "detail_fields": MAIN_DETAIL_FIELDS,
        "detail_rows": main_detail_rows,
        "complementary_fields": main_complementary,
        "validation": validation,
        # El output base queda disponible para la pantalla secundaria si se necesita
        "_base": base,
    }


_DUAL_SHEET_ORDER = [
    "Proceso", "Cronograma", "Requisitos", "Garantias", "Renglones",
    "Documentos", "Actos_Administrativos", "Hallazgos_Extra",
    "Faltantes_y_Dudas", "Trazabilidad",
    "Fusion_Cabecera", "Fusion_Renglones", "SIEM_Analitica", "Control_Carga",
]


def export_dual_excel_bytes(caso: "PliegoSolicitud") -> bytes:
    """
    Exporta el workbook dual completo con exactamente 14 hojas en orden fijo.
    Itera sobre _DUAL_SHEET_ORDER — no sobre dual_data.keys() — para garantizar
    que ninguna hoja se omita aunque _get_raw_dual_data no la incluya.
    """
    dual_data = _get_raw_dual_data(caso)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name in _DUAL_SHEET_ORDER:
            rows = dual_data.get(sheet_name, [])
            if rows:
                df = pd.DataFrame(rows)
            else:
                df = pd.DataFrame(columns=_get_default_columns_for_sheet(sheet_name))
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer.getvalue()


def export_canonical_excel_bytes(canonical_output: dict[str, Any]) -> bytes:
    """
    Exporta a Excel. Si detecta datos duales, genera las 14 hojas.
    Si no, genera las 3 hojas canónicas.
    """
    dual_data = canonical_output.get("_base", {}).get("dual_data")

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        if dual_data:
            # Export Dual (14 sheets)
            for sheet_name, rows in dual_data.items():
                df = pd.DataFrame(rows)
                # Si la hoja está vacía, al menos crearla con columnas si es posible
                if df.empty:
                    # Definir columnas mínimas por sheet si fuera necesario, 
                    # por ahora permitimos vacías o con los campos definidos en _get_raw_dual_data
                    df = pd.DataFrame(columns=_get_default_columns_for_sheet(sheet_name))
                
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        else:
            # Export Canonical (3 sheets)
            gen_export = {f["label"]: f["export_value"] for f in canonical_output["general_fields"]}
            detail_export = [
                {lbl: row[lbl]["export_value"] for lbl in MAIN_DETAIL_FIELDS if lbl in row}
                for row in canonical_output["detail_rows"]
            ]
            comp_export = {f["label"]: f["export_value"] for f in canonical_output["complementary_fields"]}

            pd.DataFrame([gen_export], columns=MAIN_GENERAL_FIELDS).to_excel(
                writer, index=False, sheet_name="Datos del proceso"
            )
            pd.DataFrame(detail_export or [{}], columns=MAIN_DETAIL_FIELDS).to_excel(
                writer, index=False, sheet_name="Detalle"
            )
            pd.DataFrame([comp_export], columns=MAIN_COMPLEMENTARY_FIELDS).to_excel(
                writer, index=False, sheet_name="Complementarios"
            )
            
    buffer.seek(0)
    return buffer.getvalue()


def _get_default_columns_for_sheet(sheet_name: str) -> list[str]:
    """Retorna las columnas por defecto para una hoja si está vacía."""
    cols = {
        "Proceso": ["numero_proceso", "objeto", "organismo"],
        "Cronograma": ["hito", "fecha", "hora", "lugar_medio", "estado_dato", "fuente"],
        "Requisitos": ["categoria", "descripcion", "obligatorio", "momento_presentacion", "medio_presentacion", "vigencia", "estado_dato", "fuente"],
        "Garantias": ["tipo", "requerida", "porcentaje", "base_calculo", "plazo", "formas_admitidas", "estado_dato", "fuente"],
        "Renglones": ["orden", "numero_renglon", "codigo_item", "descripcion", "cantidad", "unidad", "destino_efector", "entrega_parcial", "obs_tecnicas", "estado"],
        "Documentos": ["nombre", "tipo", "rol", "obligatorio", "estado_lectura", "fecha"],
        "Actos_Administrativos": ["tipo_acto", "numero", "numero_especial", "fecha", "organismo_emisor", "descripcion"],
        "Hallazgos_Extra": ["categoria", "hallazgo", "impacto", "accion_sugerida", "fuente"],
        "Faltantes_y_Dudas": ["campo_objetivo", "motivo", "detalle", "criticidad", "accion_recomendada", "estado"],
        "Trazabilidad": ["campo", "valor_extraido", "documento_fuente", "pagina_seccion", "tipo_evidencia", "observacion"],
        "Fusion_Cabecera": ["tipo_proceso", "numero_proceso", "expediente", "nombre_proceso", "objeto_contratacion", "organismo_contratante"],
        "Fusion_Renglones": ["numero_renglon", "codigo_item", "descripcion", "cantidad", "unidad", "precio_unitario_estimado"],
        "SIEM_Analitica": ["campo", "valor", "fuente", "observacion"],
        "Control_Carga": ["caso_id", "titulo", "estado_caso", "renglones_registrados", "fusion_renglones_listos", "fusion_cabecera_lista", "siem_analitica_lista", "listo_para_fusion", "motivo_bloqueo"],
    }
    return cols.get(sheet_name, [])
