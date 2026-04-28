from __future__ import annotations

import re
import unicodedata
from pathlib import Path

from web_comparativas.models import PliegoSolicitud

MISSING_VALUE_MARKERS = {
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
    "sin dato en pliego",
    "sin informacion",
    "sin información",
    "a definir",
    "a confirmar",
    "por definir",
    "por confirmar",
    "no aplica",
    "no disponible",
    "no informado",
    "aaaa-mm-dd",
    "hh:mm",
    "id proceso",
    "id único",
    "id unico",
    "archivo origen",
    "pagina/seccion",
    "página/sección",
    "encontrado/ambiguo",
    "abierto/cerrado",
    "literal/inferido/calculado",
    "texto/tabla/imagen",
    # Palabras de estado usadas literalmente como valor — deben tratarse como dato faltante
    "ambiguo",
    "ambigua",
    "contradictorio",
    "contradictoria",
    "pendiente validacion",
    "pendiente validación",
}
HIGH_CRIT_VALUES = {"alta", "critica", "crítica", "alto", "high"}
SUMMARY_FIELD_ALIASES = {
    "tipo_proceso": [
        "tipo_proceso", "procedimiento_seleccion", "clase_proceso",
        "tipo_licitacion", "tipo_compra", "procedimiento_de_seleccion",
        "tipo_de_proceso", "tipo_de_licitacion",
    ],
    "numero_proceso": [
        "numero_proceso", "nro_proceso", "numero_de_proceso", "codigo_oficial",
        "nro_de_proceso", "numero_proceso_compra",
    ],
    "expediente": [
        "expediente", "nro_expediente", "numero_expediente",
        "expediente_numero", "nro_exp",
    ],
    "nombre_proceso": [
        "nombre_proceso", "titulo_proceso", "denominacion_proceso", "denominacion",
        "nombre_de_proceso", "nombre_licitacion",
    ],
    "objeto_contratacion": [
        "objeto_contratacion", "objeto", "detalle_productos_servicios",
        "descripcion_objeto", "objeto_de_contratacion", "objeto_de_la_contratacion",
    ],
    "organismo_contratante": [
        "organismo_contratante", "organismo", "entidad_contratante",
        "reparticion", "organismo_de_aplicacion", "entidad", "organismo_comprador",
    ],
    "unidad_operativa": [
        "unidad_operativa", "unidad_operativa_adquisiciones", "unidad_contratante",
        "uoa", "unidad_ejecutora", "unidad_de_compra",
    ],
    "rubro": [
        "rubro", "categoria", "clase", "rubro_objeto", "familia", "categoria_producto",
    ],
    "moneda": ["moneda", "divisa", "currency", "tipo_moneda"],
    "modalidad": [
        "modalidad", "modalidad_contratacion", "modalidad_de_contratacion",
        "tipo_modalidad",
    ],
    "etapa": [
        "etapa", "fase", "etapa_proceso", "fase_proceso",
        "estado_etapa", "etapa_de_proceso",
    ],
    "alcance": [
        "alcance", "alcance_territorial", "alcance_geografico",
        "alcance_de_la_contratacion",
    ],
    "presupuesto_oficial": [
        "presupuesto_oficial", "monto", "monto_estimado", "monto_oficial",
        "presupuesto", "valor_total_estimado", "importe_total",
        "monto_total_estimado", "presupuesto_total",
    ],
    "requiere_pago_pliego": [
        "requiere_pago_pliego", "pago_pliego", "compra_pliego",
        "costo_pliego", "pliego_de_pago",
    ],
    "valor_pliego": [
        "valor_pliego", "precio_pliego", "monto_pliego", "costo_pliego",
        "valor_del_pliego", "importe_pliego",
    ],
    "plazo_mantenimiento_oferta": [
        "plazo_mantenimiento_oferta", "mantenimiento_oferta",
        "plazo_de_mantenimiento", "mantenimiento_de_oferta",
    ],
    "condiciones_pago": [
        "condiciones_pago", "facturacion", "forma_pago", "pago",
        "condicion_pago", "condiciones_de_pago", "forma_de_pago",
        "plazo_de_pago", "plazo_pago",
    ],
    "anticipo_financiero": [
        "anticipo_financiero", "anticipo", "anticipo_financiero_porcentaje",
        "porcentaje_anticipo",
    ],
    "contragarantia": [
        "contragarantia", "contragarantía", "contra_garantia",
        "garantia_de_contragarantia",
    ],
    "duracion_contrato": [
        "duracion_contrato", "plazo_contrato", "duracion_del_contrato",
        "duracion", "plazo_de_contrato", "vigencia_contrato",
    ],
    "fecha_estimada_inicio": [
        "fecha_estimada_inicio", "fecha_inicio", "fecha_probable_inicio",
        "inicio_contrato", "fecha_de_inicio", "inicio_estimado",
    ],
    "tipo_adjudicacion": [
        "tipo_adjudicacion", "forma_adjudicacion", "tipo_de_adjudicacion",
        "adjudicacion_parcial", "adjudicacion_total",
    ],
    "tipo_cotizacion": [
        "tipo_cotizacion", "forma_cotizacion", "tipo_de_cotizacion",
        "cotizacion_parcial", "cotizacion_total",
    ],
    "lugar_entrega": [
        "lugar_entrega", "lugares_entrega",
        "domicilio_entrega", "lugar_de_entrega", "destino_efector",
        "lugar_recepcion", "lugar_recepcion_fisica",
    ],
    "plazo_entrega": [
        "plazo_entrega", "tiempo_entrega", "plazo_de_entrega",
        "dias_entrega",
    ],
    "periodicidad": [
        "periodicidad", "periodicidad_recepcion", "frecuencia_entrega",
        "frecuencia",
    ],
    "supervisor": [
        "supervisor", "responsable", "responsable_tecnico",
        "responsable_de_compra", "funcionario_responsable",
    ],
    "contactos": [
        "contactos", "contacto", "email_contacto", "correo_contacto",
        "telefono_contacto", "correo", "email", "tel_contacto",
        "datos_contacto",
    ],
    "observaciones": [
        "observaciones", "observaciones_generales", "notas", "nota",
        "observacion",
    ],
    "id_documental": [
        "proceso_id", "id_documental", "codigo_pliego", "codigo_documental",
        "documento_particular", "id_proceso", "identificador_documental",
    ],
    "jurisdiccion": [
        "jurisdiccion", "ministerio", "area", "secretaria",
        "subsecretaria", "organismos_jurisdiccion",
    ],
    "documento_generado": [
        "documento_generado", "documento_contrato", "instrumento_generado",
        "tipo_documento_generado",
    ],
    "recepcion_digital": [
        "recepcion_digital", "presentacion_digital",
        "oferta_digital", "presentacion_electronica",
    ],
    "acepta_redeterminacion": [
        "acepta_redeterminacion", "redeterminacion_precios",
        "redeterminacion", "acepta_redeterminacion_precios",
    ],
    "acepta_actualizacion_precios": [
        "acepta_actualizacion_precios", "actualizacion_precios",
        "actualizacion_de_precios", "indexacion_precios",
    ],
    "acepta_prorroga": [
        "acepta_prorroga", "prorroga", "acepta_prorroga_contrato",
        "prorroga_contrato",
    ],
    "fecha_apertura": [
        "fecha_apertura", "fecha_hora_apertura", "apertura",
        "fecha_acto_apertura", "fecha_de_apertura",
    ],
    "fecha_inicio_consulta": [
        "fecha_inicio_consulta", "inicio_consultas", "inicio_consulta",
        "fecha_desde_consultas", "apertura_consultas", "inicio consulta",
        "inicio de consultas", "fecha inicio consultas",
    ],
    "fecha_final_consulta": [
        "fecha_final_consulta", "cierre_consultas", "fin_consulta",
        "fecha_hasta_consultas", "limite_consultas", "cierre consulta",
        "fin de consultas", "fecha limite consultas", "fecha cierre consultas",
    ],
    "cant_oferta_permitidas": [
        "cant_oferta_permitidas", "cantidad_ofertas_permitidas",
        "acepta_mas_de_una_oferta", "oferta_permitida", "cantidad_ofertas",
        "ofertas_permitidas", "cant ofertas permitidas", "numero de ofertas",
    ],
    "unidad_ejecutora": [
        "unidad_ejecutora", "unidad_ejecutora_fusion", "unidad_operativa",
        "unidad_operativa_adquisiciones", "unidad_contratante", "uoa",
        "unidad de compra",
    ],
}
SUMMARY_FIELD_META = {
    "tipo_proceso": {"label": "Tipo de proceso", "icon": "bi-tag"},
    "numero_proceso": {"label": "Numero de proceso", "icon": "bi-hash"},
    "expediente": {"label": "Expediente", "icon": "bi-folder2"},
    "nombre_proceso": {"label": "Nombre del proceso", "icon": "bi-file-text"},
    "objeto_contratacion": {"label": "Objeto de contratacion", "icon": "bi-bullseye"},
    "organismo_contratante": {"label": "Organismo contratante", "icon": "bi-building"},
    "unidad_operativa": {"label": "Unidad operativa", "icon": "bi-diagram-3"},
    "rubro": {"label": "Rubro", "icon": "bi-collection"},
    "moneda": {"label": "Moneda", "icon": "bi-currency-exchange"},
    "modalidad": {"label": "Modalidad", "icon": "bi-sliders"},
    "etapa": {"label": "Etapa", "icon": "bi-signpost-2"},
    "alcance": {"label": "Alcance", "icon": "bi-globe"},
    "presupuesto_oficial": {"label": "Presupuesto oficial", "icon": "bi-cash-stack"},
    "requiere_pago_pliego": {"label": "Requiere pago de pliego", "icon": "bi-wallet2"},
    "valor_pliego": {"label": "Valor del pliego", "icon": "bi-receipt"},
    "plazo_mantenimiento_oferta": {"label": "Plazo mant. oferta", "icon": "bi-hourglass"},
    "condiciones_pago": {"label": "Condiciones de pago", "icon": "bi-credit-card"},
    "anticipo_financiero": {"label": "Anticipo financiero", "icon": "bi-bank"},
    "contragarantia": {"label": "Contragarantia", "icon": "bi-shield-check"},
    "duracion_contrato": {"label": "Duracion del contrato", "icon": "bi-calendar-range"},
    "fecha_estimada_inicio": {"label": "Fecha estimada de inicio", "icon": "bi-calendar-event"},
    "tipo_adjudicacion": {"label": "Tipo de adjudicacion", "icon": "bi-award"},
    "tipo_cotizacion": {"label": "Tipo de cotizacion", "icon": "bi-percent"},
    "lugar_entrega": {"label": "Lugar de entrega", "icon": "bi-geo-alt"},
    "plazo_entrega": {"label": "Plazo de entrega", "icon": "bi-clock"},
    "periodicidad": {"label": "Periodicidad", "icon": "bi-arrow-repeat"},
    "supervisor": {"label": "Supervisor / Responsable", "icon": "bi-person-badge"},
    "contactos": {"label": "Contacto", "icon": "bi-telephone"},
    "observaciones": {"label": "Observaciones", "icon": "bi-chat-text"},
    "id_documental": {"label": "ID documental", "icon": "bi-upc-scan"},
    "jurisdiccion": {"label": "Jurisdiccion", "icon": "bi-building-gear"},
    "documento_generado": {"label": "Documento generado", "icon": "bi-file-earmark-check"},
    "recepcion_digital": {"label": "Recepcion digital", "icon": "bi-cloud-arrow-up"},
    "acepta_redeterminacion": {"label": "Acepta redeterminacion", "icon": "bi-graph-up-arrow"},
    "acepta_actualizacion_precios": {"label": "Acepta actualizacion de precios", "icon": "bi-arrow-repeat"},
    "acepta_prorroga": {"label": "Acepta prorroga", "icon": "bi-arrow-clockwise"},
    "fecha_apertura": {"label": "Fecha de apertura", "icon": "bi-calendar2-check"},
    "fecha_inicio_consulta": {"label": "Fecha inicio consulta", "icon": "bi-calendar-plus"},
    "fecha_final_consulta": {"label": "Fecha final consulta", "icon": "bi-calendar-x"},
    "cant_oferta_permitidas": {"label": "Cant. oferta permitidas", "icon": "bi-123"},
    "unidad_ejecutora": {"label": "Unidad ejecutora", "icon": "bi-building"},
}
SUMMARY_GROUPS = {
    "informacion_basica": [
        "tipo_proceso", "numero_proceso", "expediente", "nombre_proceso",
        "objeto_contratacion", "organismo_contratante", "unidad_operativa",
        "rubro", "moneda", "modalidad", "etapa", "alcance",
    ],
    "condiciones_comerciales": [
        "presupuesto_oficial", "requiere_pago_pliego", "valor_pliego",
        "plazo_mantenimiento_oferta", "condiciones_pago", "anticipo_financiero",
        "contragarantia", "duracion_contrato", "fecha_estimada_inicio",
        "tipo_adjudicacion", "tipo_cotizacion",
    ],
    "entrega_responsables": [
        "lugar_entrega", "plazo_entrega", "periodicidad", "supervisor", "contactos", "observaciones",
    ],
    "informacion_adicional": [
        "id_documental", "jurisdiccion", "documento_generado", "recepcion_digital",
        "acepta_redeterminacion", "acepta_actualizacion_precios", "acepta_prorroga", "fecha_apertura",
    ],
}
SUMMARY_KPI_KEYS = ["presupuesto_oficial", "valor_pliego", "plazo_mantenimiento_oferta", "duracion_contrato"]
SUMMARY_TOP_LEVEL_ALIASES = {
    "organismo": "organismo_contratante",
    "objeto": "objeto_contratacion",
    "pago": "condiciones_pago",
    "anticipo": "anticipo_financiero",
    "fecha_inicio": "fecha_estimada_inicio",
    "contacto": "contactos",
}


def _safe_str(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none"} else text


def _strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value or "")
        if not unicodedata.combining(ch)
    )


def _normalize_key(value) -> str:
    text = _strip_accents(_safe_str(value).lower())
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _normalize_text(value) -> str:
    return re.sub(r"\s+", " ", _strip_accents(_safe_str(value).lower())).strip()


def _alias_matches(candidate_key: str, alias_key: str) -> bool:
    return candidate_key == alias_key or candidate_key.startswith(f"{alias_key}_") or alias_key.startswith(f"{candidate_key}_")


def _is_explicit_missing(value: str) -> bool:
    text = _normalize_text(value)
    return (
        not text
        or text in MISSING_VALUE_MARKERS
        or text.startswith("no encontrado")
        or text.startswith("no encontrada")
        or text.startswith("no identificado")
        or text.startswith("no identificada")
    )


def _field_state_from_value(value: str, fallback: str = "Encontrado") -> str:
    text = _normalize_text(value)
    if _is_explicit_missing(text):
        return "Pendiente validacion"
    if "contradict" in text:
        return "Contradictorio"
    if "ambigu" in text:
        return "Ambiguo"
    if "inferid" in text:
        return "Inferido"
    return fallback


def _state_key(state: str) -> str:
    return {
        "Encontrado": "encontrado",
        "Inferido": "inferido",
        "Ambiguo": "ambiguo",
        "Contradictorio": "contradictorio",
        "No encontrado": "no_encontrado",
        "Pendiente validacion": "pendiente",
    }.get(state, "pendiente")


def is_presentable_value(value) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    text = _safe_str(value)
    return bool(text and not _is_explicit_missing(text))


def _has_usable_field_value(*values) -> bool:
    return any(is_presentable_value(value) for value in values)


def _normalize_yes_no(value: str) -> str:
    text = _normalize_text(value)
    token = re.split(r"[\s,;:/()]+", text)[0] if text else ""
    if token in {"si", "s", "yes", "true", "1"}:
        return "Si"
    if token in {"no", "false", "0"}:
        return "No"
    return ""


def _parse_decimal_text(value: str):
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
    elif "." in cleaned:
        whole, frac = cleaned.rsplit(".", 1)
        if frac.isdigit() and len(frac) == 3 and whole.replace("-", "").isdigit():
            cleaned = cleaned.replace(".", "")
    elif cleaned.count(".") > 1:
        cleaned = cleaned.replace(".", "")
    elif "," in cleaned:
        whole, frac = cleaned.rsplit(",", 1)
        if frac.isdigit() and len(frac) == 3 and whole.replace("-", "").isdigit():
            cleaned = cleaned.replace(",", "")
        else:
            parts = cleaned.split(",")
            cleaned = "".join(parts[:-1]) + "." + parts[-1] if len(parts[-1]) in {1, 2} else "".join(parts)
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_money_amount(value: str):
    matches = re.findall(r"(?:\$|ars|usd)?\s*([0-9][0-9.,]*)", _normalize_text(value))
    if not matches:
        return _parse_decimal_text(value)
    for raw in reversed(matches):
        parsed = _parse_decimal_text(raw)
        if parsed is not None:
            return parsed
    return None


def _extract_percentage(value: str) -> str:
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*%", _safe_str(value))
    return f"{match.group(1).replace('.', ',')}%" if match else ""


def _format_number(value: float) -> str:
    decimals = 0 if abs(value - int(value)) < 0.000001 else 2
    fmt = f"{value:,.{decimals}f}"
    return fmt.replace(",", "X").replace(".", ",").replace("X", ".")


def _format_currency_value(value: str, currency: str = "ARS") -> str:
    parsed = _parse_decimal_text(value)
    if parsed is None:
        return _safe_str(value)
    prefix = "$ " if (currency or "ARS").upper() == "ARS" else f"{currency.upper()} "
    return f"{prefix}{_format_number(parsed)}"


def _format_field_value(field_key: str, value: str, currency: str = "ARS") -> str:
    text = _safe_str(value)
    if not text:
        return ""
    if field_key in {"presupuesto_oficial", "valor_pliego"}:
        return _format_currency_value(text, currency)
    if field_key in {"requiere_pago_pliego", "recepcion_digital", "acepta_redeterminacion", "acepta_actualizacion_precios", "acepta_prorroga"}:
        yes_no = _normalize_yes_no(text)
        if yes_no:
            return yes_no
    return text


def _trace_hint(source_name: str, trace: dict, note: str = "") -> str:
    parts = [f"Fuente: {source_name}"]
    if trace.get("fuente_documento"):
        parts.append(f"Documento: {trace['fuente_documento']}")
    if trace.get("pagina_seccion"):
        parts.append(f"Pagina/seccion: {trace['pagina_seccion']}")
    if trace.get("metodo_extraccion"):
        parts.append(f"Metodo: {trace['metodo_extraccion']}")
    if trace.get("texto_evidencia"):
        parts.append(f"Evidencia: {trace['texto_evidencia']}")
    if note:
        parts.append(note)
    return " | ".join(p for p in parts if p)


def _make_candidate(value: str, *, source_name: str, priority: int, source_type: str, state: str | None = None, trace: dict | None = None, note: str = ""):
    raw_value = _safe_str(value)
    if not raw_value or _is_explicit_missing(raw_value):
        return None
    return {
        "value": raw_value,
        "source_name": source_name,
        "priority": priority,
        "source_type": source_type,
        "state": state or _field_state_from_value(raw_value, "Encontrado"),
        "trace": trace or {},
        "note": note,
    }


_VALID_TRACE_STATES = {
    "Encontrado", "Inferido", "Ambiguo", "Contradictorio",
    "No encontrado", "Pendiente validacion", "Pendiente validación",
}
_TRACE_STATE_NORMALIZE = {
    "encontrado": "Encontrado",
    "inferido": "Inferido",
    "ambiguo": "Ambiguo",
    "contradictorio": "Contradictorio",
    "no encontrado": "No encontrado",
    "pendiente validacion": "Pendiente validacion",
    "pendiente validación": "Pendiente validacion",
    "pendiente": "Pendiente validacion",
}


def _parse_trace_observation(observacion: str) -> dict:
    data = {}
    for raw_line in _safe_str(observacion).splitlines():
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        norm_key = _normalize_key(key)
        value_stripped = value.strip()
        if norm_key in {"metodo", "metodo_extraccion"}:
            data["metodo_extraccion"] = value_stripped
        elif norm_key in {"texto_evidencia", "evidencia"}:
            data["texto_evidencia"] = value_stripped
        elif norm_key in {"normalizacion", "normalizacion_aplicada"}:
            data["normalizacion_aplicada"] = value_stripped
        elif norm_key == "observacion":
            data["observacion"] = value_stripped
        elif norm_key in {"estado", "estado_extraccion"}:
            # Normalizar el estado al vocabulario canónico
            normalized = _TRACE_STATE_NORMALIZE.get(value_stripped.lower().strip())
            if normalized:
                data["estado"] = normalized
            elif value_stripped in _VALID_TRACE_STATES:
                data["estado"] = value_stripped
    return data


def _collect_process_candidates(datos: dict, field_key: str) -> list[dict]:
    aliases_raw = [field_key] + SUMMARY_FIELD_ALIASES.get(field_key, [])
    alias_norm_to_original = {_normalize_key(a): a for a in aliases_raw}
    aliases = set(alias_norm_to_original.keys())
    candidates = []
    for raw_key, raw_value in (datos or {}).items():
        norm_key = _normalize_key(raw_key)
        if not norm_key or not any(_alias_matches(norm_key, alias) for alias in aliases):
            continue
        # Detectar qué alias coincidió para incluirlo en la metadata de depuración
        matched_alias_norm = next((a for a in aliases if _alias_matches(norm_key, a)), norm_key)
        matched_alias_display = alias_norm_to_original.get(matched_alias_norm, raw_key)
        candidate = _make_candidate(raw_value, source_name="01_Proceso", priority=1, source_type="proceso")
        if candidate:
            candidate["alias_matched"] = matched_alias_display
            candidate["column_original"] = raw_key
            candidates.append(candidate)
    return candidates


def _resolve_active_excel_path(caso: PliegoSolicitud) -> Path | None:
    cargas = [carga for carga in (caso.cargas_excel or []) if getattr(carga, "es_activa", False) and _safe_str(getattr(carga, "url_path", ""))]
    if not cargas:
        return None
    cargas.sort(key=lambda item: getattr(item, "version", 0), reverse=True)
    url_path = _safe_str(cargas[0].url_path)
    if not url_path:
        return None
    base_dir = Path(__file__).resolve().parent
    return base_dir / url_path.lstrip("/\\")


def _looks_like_process_template_row(row: dict) -> bool:
    process_id = _normalize_text(row.get("proceso_id", ""))
    if process_id.startswith("id "):
        return True
    joined = " ".join(_normalize_text(value) for value in row.values() if _safe_str(value))
    template_tokens = {
        "codigo oficial",
        "n expediente",
        "denominacion",
        "objeto",
        "rubro",
        "ars/usd",
        "oc abierta/cerrada",
        "nacional/internacional",
        "por oferta/por renglon",
        "total/parcial",
        "texto del pliego",
        "texto o fecha",
        "tel./mail/area",
        "nombre/cargo",
    }
    return sum(1 for token in template_tokens if token in joined) >= 3


def _load_process_data_from_excel(caso: PliegoSolicitud) -> dict:
    excel_path = _resolve_active_excel_path(caso)
    if not excel_path or not excel_path.exists():
        return {}
    try:
        import pandas as pd
    except Exception:
        return {}
    try:
        xl = pd.ExcelFile(excel_path)
    except Exception:
        return {}
    sheet_name = next((name for name in xl.sheet_names if _normalize_key(name) in {"01_proceso", "proceso"}), None)
    if not sheet_name:
        sheet_name = next((name for name in xl.sheet_names if "proceso" in _normalize_key(name)), None)
    if not sheet_name:
        return {}
    try:
        df = xl.parse(sheet_name, dtype=str).fillna("")
    except Exception:
        return {}
    for row in df.to_dict("records"):
        clean_row = {str(key): _safe_str(value) for key, value in row.items() if _safe_str(key)}
        if not clean_row or _looks_like_process_template_row(clean_row):
            continue
        if sum(1 for value in clean_row.values() if is_presentable_value(value)) >= 4:
            return clean_row
    return {}


def _collect_trace_candidates(caso: PliegoSolicitud, field_key: str) -> list[dict]:
    aliases_raw = [field_key] + SUMMARY_FIELD_ALIASES.get(field_key, [])
    alias_norm_to_original = {_normalize_key(a): a for a in aliases_raw}
    aliases = set(alias_norm_to_original.keys())
    candidates = []
    for traza in caso.trazabilidad or []:
        campo_norm = _normalize_key(traza.campo)
        if not campo_norm or not any(_alias_matches(campo_norm, alias) for alias in aliases):
            continue
        extra_trace = _parse_trace_observation(traza.observacion or "")
        trace = {
            "campo_objetivo": traza.campo or "",
            "fuente_documento": traza.documento_fuente or "",
            "pagina_seccion": traza.pagina_seccion or "",
            "tipo_evidencia": traza.tipo_evidencia or "",
            "metodo_extraccion": extra_trace.get("metodo_extraccion", ""),
            "texto_evidencia": extra_trace.get("texto_evidencia", ""),
            "normalizacion_aplicada": extra_trace.get("normalizacion_aplicada", ""),
            "observacion": extra_trace.get("observacion", "") or (traza.observacion or ""),
        }
        # Prioridad 1: usar el estado explícito parseado del observacion ("Estado: Inferido")
        parsed_estado = extra_trace.get("estado", "")
        if parsed_estado:
            resolved_state = parsed_estado
        else:
            # Prioridad 2: derivar el estado del contenido del texto
            state_source = " ".join(filter(None, [
                traza.valor_extraido or "",
                trace.get("metodo_extraccion", ""),
                trace.get("observacion", ""),
            ]))
            resolved_state = _field_state_from_value(state_source, "Encontrado")

        # Detectar qué alias fue el que coincidió
        matched_alias_norm = next((a for a in aliases if _alias_matches(campo_norm, a)), campo_norm)
        matched_alias_display = alias_norm_to_original.get(matched_alias_norm, traza.campo or "")

        candidate = _make_candidate(
            traza.valor_extraido,
            source_name="10_Trazabilidad",
            priority=2,
            source_type="trazabilidad",
            state=resolved_state,
            trace=trace,
        )
        if candidate:
            candidate["alias_matched"] = matched_alias_display
            candidate["column_original"] = traza.campo or ""
            candidates.append(candidate)
    return candidates


def _collect_case_candidates(caso: PliegoSolicitud, field_key: str) -> list[dict]:
    return []


def _collect_renglon_values(caso: PliegoSolicitud, extra_keys: list[str], fallback_attrs: list[str]) -> list[str]:
    values = []
    for renglon in caso.renglones or []:
        extra = renglon.datos_extra or {}
        for key in extra_keys:
            value = _safe_str(extra.get(key, ""))
            if value and not _is_explicit_missing(value):
                values.append(value)
        for attr in fallback_attrs:
            value = _safe_str(getattr(renglon, attr, ""))
            if value and not _is_explicit_missing(value):
                values.append(value)
    return values


def _collapse_unique(values: list[str]) -> list[str]:
    result = []
    seen = set()
    for value in values:
        norm = _normalize_text(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        result.append(_safe_str(value))
    return result


def _candidate_from_renglones(field_key: str, values: list[str]) -> list[dict]:
    unique = _collapse_unique(values)
    if not unique:
        return []
    if field_key in {"lugar_entrega", "contactos"}:
        display = " / ".join(unique[:4])
        state = "Inferido"
    elif len(unique) == 1:
        display = unique[0]
        state = "Inferido"
    else:
        display = " | ".join(unique[:4])
        state = "Ambiguo"
    candidate = _make_candidate(
        display,
        source_name="05_Renglones",
        priority=3,
        source_type="tematica",
        state=state,
        note="Consolidado desde renglones",
    )
    return [candidate] if candidate else []


def _thematic_candidates(caso: PliegoSolicitud, field_key: str) -> list[dict]:
    if field_key == "lugar_entrega":
        return _candidate_from_renglones(field_key, _collect_renglon_values(caso, ["lugar_entrega"], ["destino_efector"]))
    if field_key == "plazo_entrega":
        return _candidate_from_renglones(field_key, _collect_renglon_values(caso, ["plazo_entrega"], []))
    if field_key == "periodicidad":
        return _candidate_from_renglones(field_key, _collect_renglon_values(caso, ["periodicidad"], []))
    if field_key == "fecha_apertura":
        values = []
        for hito in caso.cronograma or []:
            if "apertura" not in _normalize_text(hito.hito):
                continue
            if is_presentable_value(hito.fecha):
                parts = [_safe_str(hito.fecha)]
                if is_presentable_value(hito.hora):
                    parts.append(_safe_str(hito.hora))
                values.append(" ".join(parts))
        return _candidate_from_renglones(field_key, values)
    if field_key == "contragarantia":
        # Solo coincide con "contragar" — incluye "contra garantia" (con espacio)
        for garantia in caso.garantias or []:
            tipo = _normalize_text(garantia.tipo)
            tipo_sin_espacio = tipo.replace(" ", "")
            if "contragar" not in tipo_sin_espacio:
                continue
            pieces = []
            if _safe_str(garantia.requerida):
                pieces.append(_safe_str(garantia.requerida))
            if _safe_str(garantia.porcentaje):
                pct_raw = _safe_str(garantia.porcentaje)
                if re.match(r"^\d", pct_raw.replace(",", "").replace(".", "").strip()):
                    pieces.append(f"{pct_raw}%")
                else:
                    pieces.append(pct_raw)
            if _safe_str(garantia.base_calculo):
                pieces.append(f"sobre {_safe_str(garantia.base_calculo)}")
            estado_raw = _normalize_text(garantia.estado_dato or "")
            estado_final = _TRACE_STATE_NORMALIZE.get(estado_raw) or ("Inferido" if pieces else "Pendiente validacion")
            candidate = _make_candidate(
                "; ".join(pieces) or _safe_str(garantia.tipo),
                source_name="04_Garantias",
                priority=3,
                source_type="tematica",
                state=estado_final,
                note=_safe_str(garantia.tipo),
            )
            if candidate:
                candidate["alias_matched"] = "contragarantia"
                candidate["column_original"] = "tipo"
            return [candidate] if candidate else []
    if field_key == "anticipo_financiero":
        # Fuente 1: hoja Garantias con tipo que contenga "anticipo"
        for garantia in caso.garantias or []:
            tipo = _normalize_text(garantia.tipo)
            if "anticipo" not in tipo:
                continue
            pieces = []
            if _safe_str(garantia.requerida):
                pieces.append(_safe_str(garantia.requerida))
            if _safe_str(garantia.porcentaje):
                pct_raw = _safe_str(garantia.porcentaje)
                # Solo agregar "%" si es un valor numérico; si es texto descriptivo, usarlo tal cual
                if re.match(r"^\d", pct_raw.replace(",", "").replace(".", "").strip()):
                    pieces.append(f"{pct_raw}%")
                else:
                    pieces.append(pct_raw)
            if _safe_str(garantia.base_calculo):
                pieces.append(f"sobre {_safe_str(garantia.base_calculo)}")
            valor = "; ".join(pieces) or _safe_str(garantia.tipo)
            # Estado: mapeo directo del estado_dato al vocabulario canónico
            estado_raw = _normalize_text(garantia.estado_dato or "")
            estado_final = _TRACE_STATE_NORMALIZE.get(estado_raw) or ("Inferido" if pieces else "Pendiente validacion")
            candidate = _make_candidate(
                valor,
                source_name="04_Garantias",
                priority=3,
                source_type="tematica",
                state=estado_final,
                note=_safe_str(garantia.tipo),
            )
            if candidate:
                candidate["alias_matched"] = "anticipo_financiero"
                candidate["column_original"] = "tipo"
            return [candidate] if candidate else []
        # Fuente 2: hoja Hallazgos_Extra que mencione anticipo con porcentaje
        for hallazgo in caso.hallazgos or []:
            raw_text = " ".join(filter(None, [hallazgo.categoria, hallazgo.hallazgo, hallazgo.accion_sugerida]))
            if "anticipo" not in _normalize_text(raw_text):
                continue
            percentage = _extract_percentage(raw_text)
            candidate = _make_candidate(
                percentage or _safe_str(hallazgo.hallazgo),
                source_name="08_Hallazgos_Extra",
                priority=3,
                source_type="tematica",
                state="Encontrado" if percentage else "Inferido",
            )
            if candidate:
                candidate["alias_matched"] = "anticipo_financiero"
                candidate["column_original"] = "hallazgo"
            return [candidate] if candidate else []
    if field_key == "tipo_adjudicacion":
        for hallazgo in caso.hallazgos or []:
            normalized = _normalize_text(" ".join(filter(None, [hallazgo.categoria, hallazgo.hallazgo])))
            valor = None
            if "adjudicacion parcial" in normalized or "adjudicacion por item" in normalized:
                valor = "Parcial"
            elif "adjudicacion total" in normalized:
                valor = "Total"
            if valor:
                candidate = _make_candidate(valor, source_name="08_Hallazgos_Extra", priority=3, source_type="tematica", state="Inferido")
                if candidate:
                    candidate["alias_matched"] = "tipo_adjudicacion"
                    candidate["column_original"] = "hallazgo"
                return [candidate] if candidate else []
    if field_key == "id_documental":
        values = [_safe_str(acto.numero) for acto in caso.actos_admin or [] if _safe_str(acto.numero)]
        cands = _candidate_from_renglones(field_key, values)
        for c in cands:
            c.setdefault("alias_matched", "id_documental")
            c.setdefault("column_original", "numero")
        return cands
    return []


def _find_matching_faltante(caso: PliegoSolicitud, field_key: str):
    aliases = {_normalize_key(field_key), *(_normalize_key(alias) for alias in SUMMARY_FIELD_ALIASES.get(field_key, []))}
    matches = []
    for faltante in caso.faltantes or []:
        campo_norm = _normalize_key(faltante.campo_objetivo)
        if campo_norm and any(_alias_matches(campo_norm, alias) for alias in aliases):
            matches.append(faltante)
    if not matches:
        return None
    matches.sort(key=lambda item: 0 if _normalize_text(item.criticidad) in HIGH_CRIT_VALUES else 1)
    return matches[0]


def _comparable_field_value(field_key: str, value: str):
    if field_key in {"presupuesto_oficial", "valor_pliego"}:
        parsed = _extract_money_amount(value)
        return round(parsed, 2) if parsed is not None else _normalize_text(value)
    if field_key in {"requiere_pago_pliego", "recepcion_digital", "acepta_redeterminacion", "acepta_actualizacion_precios", "acepta_prorroga"}:
        yes_no = _normalize_yes_no(value)
        return yes_no or _normalize_text(value)
    return _normalize_text(value)


def _pick_best_candidate(field_key: str, candidates: list[dict]):
    usable = [candidate for candidate in candidates if candidate and not _is_explicit_missing(candidate["value"])]
    if not usable:
        return None
    usable.sort(key=lambda item: (item["priority"], 0 if item["state"] == "Encontrado" else 1))
    selected = dict(usable[0])
    unique_values = []
    seen = set()
    for candidate in usable:
        comparable = _comparable_field_value(field_key, candidate["value"])
        if comparable in seen:
            continue
        seen.add(comparable)
        unique_values.append(candidate["value"])
    normalized_values = [_normalize_text(value) for value in unique_values]
    if len(normalized_values) > 1:
        compact = []
        for value in normalized_values:
            if any(value != other and value and value in other for other in normalized_values):
                continue
            compact.append(value)
        normalized_values = compact or normalized_values
    # Solo detectar conflicto si hay valores distintos DENTRO del mismo nivel de prioridad
    # Si el mejor candidato es prioridad 1 (Proceso) con Encontrado, no lo degradamos por fuentes temáticas
    best_priority = selected["priority"]
    same_priority_values = [
        _comparable_field_value(field_key, c["value"])
        for c in usable if c["priority"] == best_priority and not _is_explicit_missing(c["value"])
    ]
    same_priority_unique = len(set(same_priority_values))
    if same_priority_unique > 1 and selected["state"] not in {"Ambiguo", "Contradictorio"}:
        bool_like = {"requiere_pago_pliego", "recepcion_digital", "acepta_redeterminacion", "acepta_actualizacion_precios", "acepta_prorroga"}
        selected["state"] = "Contradictorio" if field_key in bool_like else "Ambiguo"
        selected["note"] = (selected.get("note", "") + " | " if selected.get("note") else "") + "Multiples valores detectados"
    return selected


def _build_missing_field(field_key: str, missing_hint) -> dict:
    reason = _normalize_text(getattr(missing_hint, "motivo", "") if missing_hint else "")
    state = "Pendiente validacion"
    if "contradict" in reason:
        state = "Contradictorio"
    elif "ambigu" in reason:
        state = "Ambiguo"
    elif "no encontrado" in reason or "ausente" in reason:
        state = "No encontrado"
    note = _safe_str(getattr(missing_hint, "detalle", "") if missing_hint else "")
    trace = {
        "fuente_documento": "",
        "pagina_seccion": "",
        "tipo_evidencia": "",
        "metodo_extraccion": "",
        "texto_evidencia": "",
        "observacion": note,
    } if missing_hint else {}
    meta = SUMMARY_FIELD_META[field_key]
    return {
        "key": field_key,
        "label": meta["label"],
        "icon": meta["icon"],
        "value": "",
        "has_value": False,
        "display_value": "No identificado",
        "state": state,
        "state_key": _state_key(state),
        "source_name": "09_Faltantes_y_dudas" if missing_hint else "",
        "trace": trace,
        "note": note,
        "trace_hint": _trace_hint("09_Faltantes_y_dudas", trace, note) if missing_hint else "",
    }


def _build_field(field_key: str, candidate, missing_hint, currency: str = "ARS") -> dict:
    if not candidate:
        return _build_missing_field(field_key, missing_hint)
    meta = SUMMARY_FIELD_META[field_key]
    display_value = _format_field_value(field_key, candidate["value"], currency)
    has_value = _has_usable_field_value(
        candidate.get("raw_value"),
        candidate.get("value"),
        display_value,
    )
    if not has_value:
        return _build_missing_field(field_key, missing_hint)
    note = candidate.get("note", "")
    return {
        "key": field_key,
        "label": meta["label"],
        "icon": meta["icon"],
        "value": candidate["value"],
        "has_value": True,
        "display_value": display_value,
        "state": candidate["state"],
        "state_key": _state_key(candidate["state"]),
        "source_name": candidate["source_name"],
        "trace": candidate.get("trace", {}),
        "note": note,
        "trace_hint": _trace_hint(candidate["source_name"], candidate.get("trace", {}), note),
    }


def _parse_compound_pliego_value(raw_value: str) -> dict:
    text = _safe_str(raw_value)
    if not text or _is_explicit_missing(text):
        return {}
    return {
        "raw_value": text,
        "requiere_pago_pliego": _normalize_yes_no(text),
        "valor_pliego_amount": _extract_money_amount(text),
    }


def _resolve_compound_pliego_fields(caso: PliegoSolicitud, datos: dict, currency: str):
    raw_candidates = _collect_process_candidates(datos, "requiere_pago_pliego")
    raw_candidates.extend(_collect_trace_candidates(caso, "requiere_pago_pliego"))
    raw_source = _pick_best_candidate("requiere_pago_pliego", raw_candidates)
    parsed = _parse_compound_pliego_value(raw_source["value"]) if raw_source else {}
    direct_value = _pick_best_candidate(
        "valor_pliego",
        _collect_process_candidates(datos, "valor_pliego") + _collect_trace_candidates(caso, "valor_pliego"),
    )
    result = {}
    if raw_source and parsed.get("requiere_pago_pliego"):
        result["requiere_pago_pliego"] = _build_field(
            "requiere_pago_pliego",
            dict(raw_source, raw_value=parsed.get("raw_value", raw_source["value"]), value=parsed["requiere_pago_pliego"]),
            _find_matching_faltante(caso, "requiere_pago_pliego"),
            currency,
        )
    elif raw_source:
        result["requiere_pago_pliego"] = _build_field(
            "requiere_pago_pliego",
            dict(raw_source, state="Pendiente validacion"),
            _find_matching_faltante(caso, "requiere_pago_pliego"),
            currency,
        )
    if parsed.get("valor_pliego_amount") is not None:
        result["valor_pliego"] = _build_field(
            "valor_pliego",
            dict(raw_source, raw_value=parsed.get("raw_value", raw_source["value"]), value=str(parsed["valor_pliego_amount"])),
            _find_matching_faltante(caso, "valor_pliego"),
            currency,
        )
    elif direct_value:
        result["valor_pliego"] = _build_field(
            "valor_pliego",
            direct_value,
            _find_matching_faltante(caso, "valor_pliego"),
            currency,
        )
    elif raw_source:
        result["valor_pliego"] = _build_field(
            "valor_pliego",
            dict(raw_source, state="Pendiente validacion"),
            _find_matching_faltante(caso, "valor_pliego"),
            currency,
        )
    return result


def _resolve_summary_field(caso: PliegoSolicitud, datos: dict, field_key: str, currency: str = "ARS") -> dict:
    candidates = []
    candidates.extend(_collect_process_candidates(datos, field_key))
    candidates.extend(_collect_trace_candidates(caso, field_key))
    candidates.extend(_thematic_candidates(caso, field_key))
    candidates.extend(_collect_case_candidates(caso, field_key))
    return _build_field(field_key, _pick_best_candidate(field_key, candidates), _find_matching_faltante(caso, field_key), currency)


def _score_from_state(state: str, *, has_value: bool) -> float:
    if not has_value:
        return 0.0
    return {
        "Encontrado": 1.0,
        "Inferido": 0.7,
        "Ambiguo": 0.4,
        "Contradictorio": 0.2,
        "No encontrado": 0.0,
        "Pendiente validacion": 0.0,
    }.get(state, 0.0)


def _metric_from_field(campos: dict, key: str) -> dict:
    campo = campos[key]
    has_value = bool(campo.get("has_value"))
    return {
        "label": key,
        "score": _score_from_state(campo["state"], has_value=has_value),
        "state": campo["state"],
        "has_value": has_value,
    }


def _metric_from_collection(items, *, label: str) -> dict:
    count = len(items or [])
    return {
        "label": label,
        "score": 1.0 if count else 0.0,
        "state": "Encontrado" if count else "Pendiente validacion",
        "has_value": bool(count),
    }


def _metric_from_cronograma_critico(caso: PliegoSolicitud) -> dict:
    critical_hits = 0
    for hito in caso.cronograma or []:
        if "apertura" in _normalize_text(hito.hito) and is_presentable_value(hito.fecha):
            critical_hits += 1
            break
    return {
        "label": "cronograma_critico_minimo",
        "score": 1.0 if critical_hits else 0.0,
        "state": "Encontrado" if critical_hits else "Pendiente validacion",
        "has_value": bool(critical_hits),
    }


def _build_completeness_metrics(campos: dict, caso: PliegoSolicitud) -> dict:
    executive_items = [
        _metric_from_field(campos, "tipo_proceso"),
        _metric_from_field(campos, "nombre_proceso"),
        _metric_from_field(campos, "objeto_contratacion"),
        _metric_from_field(campos, "organismo_contratante"),
        _metric_from_field(campos, "unidad_operativa"),
        _metric_from_field(campos, "modalidad"),
        _metric_from_field(campos, "moneda"),
        _metric_from_field(campos, "presupuesto_oficial"),
        _metric_from_field(campos, "valor_pliego"),
        _metric_from_field(campos, "plazo_mantenimiento_oferta"),
        _metric_from_field(campos, "duracion_contrato"),
        _metric_from_field(campos, "lugar_entrega"),
        _metric_from_field(campos, "plazo_entrega"),
        _metric_from_field(campos, "contactos"),
        _metric_from_collection(caso.renglones, label="renglones"),
        _metric_from_collection(caso.requisitos, label="requisitos"),
        _metric_from_collection(caso.garantias, label="garantias"),
        _metric_from_cronograma_critico(caso),
    ]
    technical_keys = [
        "tipo_proceso", "numero_proceso", "expediente", "nombre_proceso", "objeto_contratacion",
        "organismo_contratante", "unidad_operativa", "rubro", "moneda", "modalidad", "alcance",
        "presupuesto_oficial", "valor_pliego", "plazo_mantenimiento_oferta", "duracion_contrato",
        "tipo_adjudicacion", "tipo_cotizacion", "lugar_entrega", "plazo_entrega", "periodicidad",
        "supervisor", "contactos", "fecha_estimada_inicio", "recepcion_digital", "acepta_redeterminacion",
        "acepta_actualizacion_precios", "acepta_prorroga", "fecha_apertura", "jurisdiccion",
        "documento_generado", "condiciones_pago", "anticipo_financiero", "contragarantia",
    ]
    technical_items = [_metric_from_field(campos, key) for key in technical_keys]
    critical = [row for row in caso.faltantes if _normalize_text(row.criticidad) in HIGH_CRIT_VALUES]

    def build_metric(items: list[dict]) -> dict:
        total = len(items)
        found = sum(1 for item in items if item["state"] == "Encontrado" and item["has_value"])
        inferred = sum(1 for item in items if item["state"] == "Inferido" and item["has_value"])
        ambiguous = sum(1 for item in items if item["state"] == "Ambiguo")
        contradictory = sum(1 for item in items if item["state"] == "Contradictorio")
        missing = sum(1 for item in items if not item["has_value"])
        score_sum = sum(item["score"] for item in items)
        percentage = int(round((score_sum / total) * 100)) if total else 0

        if critical:
            percentage = min(percentage, 89)
        if critical:
            status = "Pendiente validacion"
        elif contradictory or missing:
            status = "Con observaciones"
        elif ambiguous:
            status = "Con observaciones"
        else:
            status = "Completo"

        max_percentage = 99 if (critical or contradictory or missing or ambiguous) else 100
        percentage = max(0, min(percentage, max_percentage))
        return {
            "porcentaje": percentage,
            "estado": status,
            "campos_esperados": total,
            "campos_encontrados": found,
            "campos_inferidos": inferred,
            "campos_ambiguos": ambiguous,
            "campos_contradictorios": contradictory,
            "campos_pendientes": missing,
            "faltantes_criticos": len(critical),
        }

    return {
        "ejecutiva": build_metric(executive_items),
        "tecnica": build_metric(technical_items),
    }


def build_resumen_licitacion(caso: PliegoSolicitud) -> dict:
    datos = dict(caso.datos_proceso.datos) if (caso.datos_proceso and caso.datos_proceso.datos) else {}
    excel_process_data = _load_process_data_from_excel(caso)
    if excel_process_data:
        for key, value in excel_process_data.items():
            if not is_presentable_value(datos.get(key)):
                datos[key] = value
    campos = {"moneda": _resolve_summary_field(caso, datos, "moneda", "ARS")}
    moneda_base = campos["moneda"]["display_value"] or "ARS"
    compound = _resolve_compound_pliego_fields(caso, datos, moneda_base)

    for field_key in SUMMARY_FIELD_META:
        if field_key in campos:
            continue
        if field_key in compound:
            campos[field_key] = compound[field_key]
            continue
        campos[field_key] = _resolve_summary_field(caso, datos, field_key, moneda_base)

    completitudes = _build_completeness_metrics(campos, caso)
    completitud_ejecutiva = completitudes["ejecutiva"]
    completitud_tecnica = completitudes["tecnica"]
    criticos = [row for row in caso.faltantes if _normalize_text(row.criticidad) in HIGH_CRIT_VALUES]
    known_fields = set()
    for field_key, aliases in SUMMARY_FIELD_ALIASES.items():
        known_fields.add(field_key)
        known_fields.update(aliases)

    resumen = {
        "titulo": caso.titulo or "",
        "campos": campos,
        "grupos": {group: [campos[key] for key in keys] for group, keys in SUMMARY_GROUPS.items()},
        "kpis": [campos[key] for key in SUMMARY_KPI_KEYS],
        "cantidad_renglones": len(caso.renglones),
        "cantidad_requisitos": len(caso.requisitos),
        "cantidad_hallazgos": len(caso.hallazgos),
        "cantidad_criticos": len(criticos),
        "fecha_carga": caso.publicado_en or caso.actualizado_en,
        "estado": caso.estado,
        "completitud": completitud_ejecutiva,
        "completitud_ejecutiva": completitud_ejecutiva,
        "completitud_tecnica": completitud_tecnica,
        "_campos_conocidos": known_fields,
        "_datos_raw": datos,
    }

    for field_key in SUMMARY_FIELD_META:
        resumen[field_key] = campos[field_key]["value"]
    for alias_key, field_key in SUMMARY_TOP_LEVEL_ALIASES.items():
        resumen[alias_key] = campos[field_key]["value"]
    return resumen


def calcular_completitud(caso: PliegoSolicitud) -> int:
    return build_resumen_licitacion(caso)["completitud_ejecutiva"]["porcentaje"]


def build_debug_matrix(caso: PliegoSolicitud) -> list[dict]:
    """
    Genera la matriz de depuración técnica por campo canónico.
    Usa el mismo pipeline de build_resumen_licitacion para garantizar que los scores
    y estados son idénticos a los que se muestran en la UI de completitud.

    Columnas:
    - campo_canonico      : clave interna del campo
    - label_ui            : etiqueta visible en el dashboard
    - fuente_resuelta     : nombre de la hoja/fuente que aportó el valor
    - hoja_origen         : tipo de fuente (proceso, trazabilidad, tematica)
    - columna_origen      : nombre de columna exacta en la fuente
    - alias_detectado     : alias que coincidió con el campo canónico
    - valor_resuelto      : valor final adoptado
    - estado_resuelto     : estado del dato (Encontrado, Inferido, Ambiguo, etc.)
    - puntaje_aplicado    : peso numérico según el estado (0.0 - 1.0)
    - candidatos_proceso  : cuántos candidatos vinieron de 01_Proceso
    - candidatos_trazab   : cuántos candidatos vinieron de 10_Trazabilidad
    - candidatos_tematicos: cuántos vinieron de hojas temáticas
    - motivo_si_no_resuelve: explicación cuando el campo queda vacío
    """
    # Usar el mismo pipeline que build_resumen_licitacion para garantizar consistencia
    resumen = build_resumen_licitacion(caso)
    campos = resumen["campos"]
    datos = resumen.get("_datos_raw", {})

    matrix = []
    for field_key, meta in SUMMARY_FIELD_META.items():
        campo = campos.get(field_key, {})

        # Recalcular candidatos para metadata de debug
        process_cands = _collect_process_candidates(datos, field_key)
        trace_cands = _collect_trace_candidates(caso, field_key)
        thematic_cands = _thematic_candidates(caso, field_key)
        all_cands = process_cands + trace_cands + thematic_cands
        best_for_debug = _pick_best_candidate(field_key, all_cands)

        # Usar el estado y score del campo ya resuelto (pipeline unificado)
        has_value = bool(campo.get("has_value"))
        state = campo.get("state", "No encontrado")
        score = _score_from_state(state, has_value=has_value)

        # Construir el motivo cuando no se resuelve
        motivo = ""
        if not has_value:
            faltante = _find_matching_faltante(caso, field_key)
            if faltante:
                partes = [f"Registrado como faltante ({faltante.criticidad or 'sin criticidad'})"]
                if faltante.motivo:
                    partes.append(faltante.motivo)
                if faltante.detalle:
                    partes.append(faltante.detalle)
                motivo = " | ".join(partes)
            elif not all_cands:
                motivo = "Sin candidatos en ninguna fuente (Proceso, Trazabilidad ni hojas temáticas)"
            else:
                motivo = "Candidatos encontrados pero todos con valor vacío o marcador de faltante"

        best = best_for_debug
        matrix.append({
            "campo_canonico": field_key,
            "label_ui": meta["label"],
            "fuente_resuelta": campo.get("source_name", "") if has_value else "",
            "hoja_origen": campo.get("trace", {}).get("source_type", best.get("source_type", "") if best else "") if has_value else "",
            "columna_origen": best.get("column_original", "") if best and has_value else "",
            "alias_detectado": best.get("alias_matched", "") if best and has_value else "",
            "valor_resuelto": campo.get("value", "") if has_value else "",
            "estado_resuelto": state,
            "puntaje_aplicado": score,
            "candidatos_proceso": len(process_cands),
            "candidatos_trazab": len(trace_cands),
            "candidatos_tematicos": len(thematic_cands),
            "motivo_si_no_resuelve": motivo,
        })

    return matrix
