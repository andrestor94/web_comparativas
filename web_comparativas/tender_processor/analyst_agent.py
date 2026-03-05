"""
AGENTE 4: ANALISTA DE LICITACIONES (Rule-Based Expert System)
==============================================================
Objetivo: Simular el análisis de un analista de licitaciones experto.
Funciona 100% offline, sin APIs ni LLMs. Usa lógica de negocio,
heurísticas y reglas configurables para:
  1. Auditar completitud e integridad de datos extraídos
  2. Evaluar riesgos (plazos, penalidades, garantías)
  3. Generar un resumen ejecutivo con notas de analista
  4. Detectar inconsistencias y alertas tempranas
"""

import re
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger("wc.analyst_agent")

# ─────────────────────────────────────────────────────────────────
# CONFIGURABLE THRESHOLDS (Umbrales configurables)
# ─────────────────────────────────────────────────────────────────
THRESHOLDS = {
    # Delivery
    "delivery_days_high_risk": 5,       # ≤ 5 días = riesgo alto
    "delivery_days_medium_risk": 10,    # ≤ 10 días = riesgo medio

    # Penalties
    "penalty_pct_severe": 2.0,          # ≥ 2% diario = severo
    "penalty_pct_moderate": 1.0,        # ≥ 1% diario = moderado

    # Guarantees
    "guarantee_total_high": 10.0,       # > 10% acumulado = alto
    "guarantee_total_medium": 5.0,      # > 5% acumulado = medio

    # Offer Maintenance
    "maintenance_days_short": 15,       # < 15 = muy corto
    "maintenance_days_normal": 30,      # 30 = normal
    "maintenance_days_long": 60,        # > 60 = largo

    # Payment
    "payment_advance_high": 80.0,       # > 80% anticipo = favorable
    "payment_advance_low": 30.0,        # < 30% anticipo = riesgo

    # Budget
    "budget_large": 100_000_000,        # > 100M = licitación grande
    "budget_medium": 10_000_000,        # > 10M = mediana
    
    # Vencimiento producto
    "shelf_life_min_months": 12,        # < 12 meses = riesgo
}


class AnalystAgent:
    """
    Agente Analista de Licitaciones - Sistema Experto basado en reglas.
    No requiere APIs, LLMs ni servicios de pago.
    """

    def __init__(self, thresholds: Dict = None):
        self.thresholds = thresholds or THRESHOLDS

    def analyze(self, tender_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Punto de entrada principal.
        Recibe el JSON consolidado del orquestador y produce un informe analítico.
        """
        logger.info("Running Analyst Agent (Rule-Based Expert System)...")

        try:
            report = {
                "completeness_audit": self._audit_completeness(tender_data),
                "risk_assessment": self._assess_risks(tender_data),
                "executive_summary": self._generate_summary(tender_data),
                "analyst_notes": self._generate_notes(tender_data),
                "score": 0,
                "score_label": "",
            }

            # Calculate overall score (0-100)
            report["score"] = self._calculate_score(report)
            report["score_label"] = self._score_to_label(report["score"])

            logger.info(f"Analyst Agent complete. Score: {report['score']}/100 ({report['score_label']})")
            return report
        except Exception as e:
            logger.error(f"AnalystAgent CRITICAL ERROR: {e}", exc_info=True)
            return {
                "completeness_audit": {
                    "checks": [], "total_fields": 0, "present_fields": 0,
                    "completeness_pct": 0, "missing_critical": [], "missing_optional": [],
                    "low_confidence_warnings": [f"Error en análisis: {str(e)}"],
                    "reliability_score": 0
                },
                "risk_assessment": {
                    "risks": [], "opportunities": [], "overall_risk": "N/D",
                    "high_count": 0, "medium_count": 0, "low_count": 0
                },
                "executive_summary": {},
                "analyst_notes": [f"⚠️ Error en el agente analista: {str(e)}"],
                "score": 0,
                "score_label": "Error en análisis",
            }

    # ─────────────────────────────────────────────────────────────
    # 1. COMPLETENESS AUDIT
    # ─────────────────────────────────────────────────────────────

    def _audit_completeness(self, data: Dict) -> Dict[str, Any]:
        """Verify all critical fields are present and valid."""
        
        checks = []
        missing_critical = []
        missing_optional = []

        # Define what to check: (path, label, is_critical)
        field_checks = [
            # Critical fields
            ("summary.process_number", "Número de proceso", True),
            ("summary.estimated_total_amount.value.amount", "Presupuesto oficial", True),
            ("basic_info.object", "Objeto de la licitación", True),
            ("basic_info.selection_procedure", "Procedimiento de selección", True),
            ("basic_info.pliego_value.value.amount", "Valor del pliego", True),
            ("basic_info.contact_email", "Email de contacto", True),
            
            # Important fields
            ("basic_info.opening_date", "Fecha de apertura", True),
            ("basic_info.offer_maintenance_term_days_business", "Mantenimiento de oferta", False),
            ("basic_info.payment_terms", "Condiciones de pago", True),
            
            # Guarantees
            ("guarantees.offer_maintenance_pct", "Garantía mantenimiento oferta", False),
            ("guarantees.contract_compliance_pct", "Garantía cumplimiento contrato", False),
            
            # Delivery
            ("delivery.modalidad", "Modalidad de entrega", False),
            ("delivery.plazo", "Plazo de entrega", False),
            ("delivery.horario", "Horario de entrega", False),
            ("delivery.condiciones", "Condiciones de entrega", False),
            
            # Penalties
            ("penalties.mora", "Penalidad por mora", False),
            ("penalties.multas", "Multas", False),
            ("penalties.jurisdiccion", "Jurisdicción", False),

            # Schedule
            ("schedule.consultation_deadline_relative", "Plazo de consultas", False),
            ("schedule.samples_rule", "Regla de muestras", False),
            
            # Additional UI crucial fields
            ("delivery.locations", "Lugares de entrega", True),
        ]

        missing_critical = []
        low_confidence = []
        present_count = 0
        
        for path, label, is_critical in field_checks:
            raw_item = self._get_nested(data, path)
            # Check presence
            val = self._get_val(raw_item)
            is_present = val is not None and val != "" and val != "No informado"
            
            # Special check for numeric zero (pliego_value amount = 0 means not found)
            if path.endswith(".amount") and val == 0.0:
                is_present = False

            # Check confidence if available
            confidence_warning = False
            if is_present and isinstance(raw_item, dict) and "confidence" in raw_item:
                conf = raw_item.get("confidence", 1.0)
                if conf < 0.8:
                    low_confidence.append(f"{label} (Confianza: {int(conf*100)}%)")
                    confidence_warning = True

            status = "✅" if is_present else ("❌" if is_critical else "⚠️")
            if confidence_warning:
                 status = "⚠️ (Dudoso)"

            checks.append({
                "field": label,
                "status": status,
                "present": is_present,
                "critical": is_critical,
            })

            if is_present:
                 present_count += 1
            else:
                if is_critical:
                    missing_critical.append(label)
                else:
                    missing_optional.append(label)

        # Check items/renglones
        items = data.get("items", [])
        items_count = len(items) if isinstance(items, list) else 0
        checks.append({
            "field": "Renglones de productos",
            "status": "✅" if items_count > 0 else "❌",
            "present": items_count > 0,
            "critical": True,
            "count": items_count,
        })
        if items_count == 0:
            missing_critical.append("Renglones de productos")

        # Check requirements
        reqs = self._get_nested(data, "requirements.value") or []
        req_count = len(reqs)
        checks.append({
            "field": "Requisitos",
            "status": "✅" if req_count > 0 else "⚠️",
            "present": req_count > 0,
            "critical": False,
            "count": req_count,
        })

        total = len(checks)
        present = sum(1 for c in checks if c["present"])
        pct = round((present / total) * 100, 1) if total > 0 else 0
        
        # Reliability penalty
        reliability_penalty = len(low_confidence) * 5
        reliability_score = max(0, 100 - reliability_penalty)

        return {
            "checks": checks,
            "total_fields": total,
            "present_fields": present,
            "completeness_pct": pct,
            "missing_critical": missing_critical,
            "missing_optional": missing_optional,
            "low_confidence_warnings": low_confidence,
            "reliability_score": reliability_score
        }

    # ─────────────────────────────────────────────────────────────
    # 2. RISK ASSESSMENT
    # ─────────────────────────────────────────────────────────────

    def _assess_risks(self, data: Dict) -> Dict[str, Any]:
        """Evaluate risks across delivery, penalties, guarantees, and payment."""
        
        risks = []
        
        # --- A. Delivery Risk ---
        plazo_text = self._get_nested(data, "delivery.plazo.value") or ""
        delivery_days = self._extract_days(plazo_text)
        
        if delivery_days:
            if delivery_days <= self.thresholds["delivery_days_high_risk"]:
                risks.append({
                    "category": "Entrega",
                    "level": "ALTO",
                    "icon": "🔴",
                    "description": f"Plazo de entrega muy ajustado: {delivery_days} días hábiles. "
                                   f"Riesgo logístico elevado para cumplir con múltiples puntos de entrega.",
                    "recommendation": "Evaluar capacidad logística antes de cotizar. Considerar stock de seguridad.",
                })
            elif delivery_days <= self.thresholds["delivery_days_medium_risk"]:
                risks.append({
                    "category": "Entrega",
                    "level": "MEDIO",
                    "icon": "🟡",
                    "description": f"Plazo de entrega moderado: {delivery_days} días hábiles.",
                    "recommendation": "Verificar disponibilidad de stock y transporte.",
                })
            else:
                risks.append({
                    "category": "Entrega",
                    "level": "BAJO",
                    "icon": "🟢",
                    "description": f"Plazo de entrega holgado: {delivery_days} días hábiles.",
                    "recommendation": "Sin observaciones.",
                })

        # Multiple delivery locations
        locations = self._get_nested(data, "delivery.locations") or []
        if len(locations) >= 3:
            risks.append({
                "category": "Logística",
                "level": "MEDIO",
                "icon": "🟡",
                "description": f"Se requiere entrega en {len(locations)} puntos diferentes. "
                               f"Esto incrementa complejidad y costos logísticos.",
                "recommendation": "Incluir costo de flete para cada punto de entrega en la cotización.",
            })

        # --- B. Penalty Risk ---
        multas_text = self._get_str_val(self._get_nested(data, "penalties.multas"))
        penalty_pct = self._extract_percentage(multas_text)
        
        if penalty_pct:
            if penalty_pct >= self.thresholds["penalty_pct_severe"]:
                risks.append({
                    "category": "Penalidades",
                    "level": "ALTO",
                    "icon": "🔴",
                    "description": f"Multas severas detectadas: {penalty_pct}% por día de mora. "
                                   f"El monto acumulado puede superar rápidamente la garantía de cumplimiento.",
                    "recommendation": "Asegurar cadena de suministro robusta. "
                                      "Considerar cláusula de fuerza mayor en la oferta si es permitido.",
                })
            elif penalty_pct >= self.thresholds["penalty_pct_moderate"]:
                risks.append({
                    "category": "Penalidades",
                    "level": "MEDIO",
                    "icon": "🟡",
                    "description": f"Multas moderadas: {penalty_pct}% por día de mora.",
                    "recommendation": "Monitorear plazos de entrega con atención.",
                })

        # Mora automática
        mora_text = self._get_str_val(self._get_nested(data, "penalties.mora"))
        if "automát" in mora_text.lower():
            risks.append({
                "category": "Mora",
                "level": "MEDIO",
                "icon": "🟡",
                "description": "La mora se constituye automáticamente sin necesidad de intimación. "
                               "No hay margen para justificar retrasos.",
                "recommendation": "Planificar entregas con margen de seguridad mínimo de 2 días hábiles.",
            })

        # --- C. Guarantee Risk ---
        guarantee_total = 0
        glist = self._get_nested(data, "guarantees.garantias_list") or []
        for g in glist:
            pct_str = self._get_str_val(g.get("porcentaje"))
            pct = self._extract_percentage(pct_str)
            if pct and pct < 100:  # Exclude 100% contra-garantía
                guarantee_total += pct

        if guarantee_total > self.thresholds["guarantee_total_high"]:
            risks.append({
                "category": "Garantías",
                "level": "ALTO",
                "icon": "🔴",
                "description": f"Carga de garantías acumulada elevada: {guarantee_total}% total. "
                               f"Requiere inmovilización significativa de capital o línea de caución.",
                "recommendation": "Verificar disponibilidad de pólizas de caución con anticipación.",
            })
        elif guarantee_total > self.thresholds["guarantee_total_medium"]:
            risks.append({
                "category": "Garantías",
                "level": "MEDIO",
                "icon": "🟡",
                "description": f"Carga de garantías moderada: {guarantee_total}% total.",
                "recommendation": "Solicitar cotización de póliza de caución con anticipación.",
            })

        # Contra-garantía anticipo
        for g in glist:
            tipo = self._get_str_val(g.get("tipo", "")).lower()
            pct_s = self._get_str_val(g.get("porcentaje", ""))
            if "anticipo" in tipo and "100" in pct_s:
                risks.append({
                    "category": "Anticipo",
                    "level": "MEDIO",
                    "icon": "🟡",
                    "description": "Se requiere contra-garantía del 100% del anticipo. "
                                   "Esto implica un costo financiero adicional por la póliza.",
                    "recommendation": "Incluir el costo de la póliza de caución en el precio unitario.",
                })
                break

        # --- D. Payment Risk ---
        payment_terms = self._get_str_val(self._get_nested(data, "basic_info.payment_terms.value"))
        anticipo_pct = self._extract_percentage(payment_terms)
        
        if anticipo_pct:
            if anticipo_pct >= self.thresholds["payment_advance_high"]:
                risks.append({
                    "category": "Pago",
                    "level": "BAJO",
                    "icon": "🟢",
                    "description": f"Condiciones de pago favorables: anticipo del {anticipo_pct}%. "
                                   f"Reduce significativamente el riesgo financiero.",
                    "recommendation": "Condición favorable. Considerar ofrecer mejores precios.",
                })
            elif anticipo_pct <= self.thresholds["payment_advance_low"]:
                risks.append({
                    "category": "Pago",
                    "level": "ALTO",
                    "icon": "🔴",
                    "description": f"Anticipo bajo: solo {anticipo_pct}%. "
                                   f"Alta exposición financiera del proveedor.",
                    "recommendation": "Evaluar capacidad de financiamiento propio.",
                })

        # --- E. Vencimiento / Shelf Life ---
        vencimiento = self._get_str_val(self._get_nested(data, "delivery.vencimiento_minimo.value"))
        months = self._extract_months(vencimiento)
        if months and months >= 12:
            risks.append({
                "category": "Producto",
                "level": "MEDIO",
                "icon": "🟡",
                "description": f"Se requiere vencimiento mínimo de {months} meses. "
                               f"Esto limita el stock disponible para cotizar.",
                "recommendation": "Verificar fechas de vencimiento del stock actual antes de cotizar.",
            })

        # --- F. Budget Scale ---
        budget = self._get_nested(data, "summary.estimated_total_amount.value.amount")
        if budget:
            if budget >= self.thresholds["budget_large"]:
                risks.append({
                    "category": "Escala",
                    "level": "MEDIO",
                    "icon": "🟡",
                    "description": f"Licitación de gran envergadura: ${budget:,.0f}. "
                                   f"Requiere evaluación detallada de capacidad de producción/abastecimiento.",
                    "recommendation": "Evaluar si se dispone de capacidad para cubrir la totalidad o considerar UT.",
                })

        # --- G. Opportunities (Green Flags) ---
        opportunities = []
        
        # 1. Free Participation
        pliego_val = self._get_nested(data, "basic_info.pliego_value.value.amount") or 0
        if pliego_val == 0:
            opportunities.append({
                "category": "Administrativo",
                "description": "Pliego sin costo. Baja barrera de entrada.",
                "icon": "🟢"
            })

        # 2. Advance Payment (Financial Benefit)
        # Re-check anticipo calculated in D
        # anticipo_pct is already calculated in section D. Payment Risk
        if anticipo_pct and anticipo_pct > 0:
             opportunities.append({
                "category": "Financiero",
                "description": f"Anticipo del {anticipo_pct}%. Mejora el flujo de caja.",
                "icon": "🟢"
            })

        # 3. No Technical Guarantee
        gar_tec = self._get_str_val(self._get_nested(data, "penalties.garantia_tecnica"))
        if "no aplica" in gar_tec.lower():
             opportunities.append({
                "category": "Técnico",
                "description": "No requiere Garantía Técnica. Menor complejidad administrativa.",
                "icon": "🟢"
            })

        # 4. Centralized Delivery
        locs = self._get_nested(data, "delivery.locations") or []
        if isinstance(locs, list) and len(locs) == 1:
             opportunities.append({
                "category": "Logística",
                "description": "Entrega centralizada en un único punto. Reduce costos logísticos.",
                "icon": "🟢"
            })

        # Calculate risk summary
        high_count = sum(1 for r in risks if r["level"] == "ALTO")
        medium_count = sum(1 for r in risks if r["level"] == "MEDIO")
        low_count = sum(1 for r in risks if r["level"] == "BAJO")

        if high_count >= 2:
            overall = "ALTO"
        elif high_count >= 1 or medium_count >= 3:
            overall = "MEDIO-ALTO"
        elif medium_count >= 1:
            overall = "MEDIO"
        else:
            overall = "BAJO"

        return {
            "risks": risks,
            "opportunities": opportunities,
            "overall_risk": overall,
            "high_count": high_count,
            "medium_count": medium_count,
            "low_count": low_count,
        }

    # ─────────────────────────────────────────────────────────────
    # 3. EXECUTIVE SUMMARY
    # ─────────────────────────────────────────────────────────────

    def _generate_summary(self, data: Dict) -> Dict[str, Any]:
        """Generate a structured executive summary of the tender."""
        
        proc_num = self._get_nested(data, "summary.process_number.value") or "N/D"
        proc_name = self._get_nested(data, "summary.process_name.value") or "N/D"
        organismo = self._get_nested(data, "summary.uoa.value") or "N/D"
        objeto = self._get_nested(data, "basic_info.object.value") or "N/D"
        budget = self._get_nested(data, "summary.estimated_total_amount.value.amount")
        procedure = self._get_nested(data, "basic_info.selection_procedure.value") or "N/D"
        pliego_val = self._get_nested(data, "basic_info.pliego_value.value.amount") or 0
        email = self._get_nested(data, "basic_info.contact_email.value") or "N/D"
        maint_days = self._get_nested(data, "basic_info.offer_maintenance_term_days_business.value")
        
        # Items count
        items = data.get("items", [])
        items_count = len(items) if isinstance(items, list) else 0
        
        # Locations
        locations = self._get_nested(data, "delivery.locations") or []
        loc_names = [l.get("name", "").replace("*", "").strip() for l in locations]
        
        # Guarantees summary
        glist = self._get_nested(data, "guarantees.garantias_list") or []
        guarantee_summary = []
        for g in glist:
            tipo = self._get_str_val(g.get("tipo"))
            pct = self._get_str_val(g.get("porcentaje"))
            guarantee_summary.append(f"{tipo}: {pct}")

        # Budget classification
        budget_class = "No definido"
        if budget:
            if budget >= self.thresholds["budget_large"]:
                budget_class = "Gran envergadura"
            elif budget >= self.thresholds["budget_medium"]:
                budget_class = "Mediana envergadura"
            else:
                budget_class = "Pequeña envergadura"

        return {
            "titulo": f"{procedure} N° {proc_num}",
            "organismo": organismo,
            "objeto_resumido": self._truncate(objeto, 200),
            "presupuesto": f"${budget:,.2f}" if budget else "N/D",
            "clasificacion_presupuesto": budget_class,
            "valor_pliego": f"${pliego_val:,.0f}" if pliego_val else "Gratuito",
            "procedimiento": procedure,
            "email_contacto": email,
            "mantenimiento_oferta": f"{maint_days} días hábiles" if maint_days else "N/D",
            "cantidad_renglones": items_count,
            "puntos_entrega": loc_names,
            "garantias_resumen": guarantee_summary,
        }

    # ─────────────────────────────────────────────────────────────
    # 4. ANALYST NOTES
    # ─────────────────────────────────────────────────────────────

    def _generate_notes(self, data: Dict) -> List[str]:
        """Generate actionable analyst observations."""
        
        notes = []
        
        # --- Scale observation ---
        budget = self._get_nested(data, "summary.estimated_total_amount.value.amount")
        if budget and budget >= self.thresholds["budget_large"]:
            notes.append(
                f"📊 Licitación de gran escala (${budget:,.0f}). "
                f"Evaluar capacidad de abastecimiento completo o posibilidad de cotizar renglones parciales."
            )

        # --- Logistics complexity ---
        locations = self._get_nested(data, "delivery.locations") or []
        if len(locations) >= 3:
            loc_names = ", ".join(l.get("name", "").replace("*", "").strip() for l in locations[:3])
            notes.append(
                f"🚚 Alta complejidad logística: entrega en {len(locations)} puntos ({loc_names}). "
                f"Incluir costos de flete diferenciados por destino."
            )
        elif len(locations) == 1:
            notes.append(f"🚚 Entrega centralizada en un único punto. Logística simplificada.")

        # --- Penalty severity ---
        multas = self._get_str_val(self._get_nested(data, "penalties.multas"))
        pct = self._extract_percentage(multas)
        if pct and pct >= 3:
            notes.append(
                f"⚠️ Régimen de penalidades severo ({pct}% diario). "
                f"La acumulación de multas podría superar el margen de ganancia en menos de 15 días de mora."
            )

        # --- Payment advantage ---
        payment = self._get_str_val(self._get_nested(data, "basic_info.payment_terms.value"))
        anticipo = self._extract_percentage(payment)
        if anticipo and anticipo >= 80:
            notes.append(
                f"💰 Condición de pago muy favorable: anticipo del {anticipo}%. "
                f"Esto permite financiar la producción/adquisición con fondos del organismo."
            )
        elif anticipo and anticipo < 30:
            notes.append(
                f"💰 Condición de pago desfavorable: solo {anticipo}% de anticipo. "
                f"El proveedor deberá financiar la mayor parte de la operación."
            )

        # --- Factura type ---
        tipo_factura = self._get_str_val(self._get_nested(data, "payment.tipo_factura"))
        if "Factura B" in tipo_factura:
            notes.append(
                "📋 Se requiere Factura B. Confirmar que el régimen fiscal del oferente permite emitir este tipo."
            )

        # --- Maintenance period ---
        maint = self._get_nested(data, "basic_info.offer_maintenance_term_days_business.value")
        if maint:
            if isinstance(maint, int):
                if maint >= self.thresholds["maintenance_days_long"]:
                    notes.append(
                        f"⏳ Mantenimiento de oferta prolongado: {maint} días hábiles. "
                        f"Considerar variación de costos en el período."
                    )
                elif maint <= self.thresholds["maintenance_days_short"]:
                    notes.append(
                        f"⏳ Mantenimiento de oferta corto: {maint} días hábiles. "
                        f"La adjudicación debería resolverse rápidamente."
                    )

        # --- Samples requirement ---
        samples = self._get_nested(data, "schedule.samples_rule.value") or ""
        if "48" in samples and "muestra" in samples.lower():
            notes.append(
                "🧪 Se requiere presentación de muestras dentro de las 48 hs hábiles post-apertura. "
                "Tener muestras preparadas antes de la apertura."
            )

        # --- Shelf life ---
        vencimiento = self._get_str_val(self._get_nested(data, "delivery.vencimiento_minimo.value"))
        months = self._extract_months(vencimiento)
        if months and months >= 12:
            notes.append(
                f"📦 Vencimiento mínimo exigido: {months} meses. "
                f"Verificar fechas de lotes disponibles antes de comprometer stock."
            )

        # --- Número de renglones ---
        items = data.get("items", [])
        if len(items) > 50:
            notes.append(
                f"📋 Licitación con {len(items)} renglones. "
                f"Alta carga administrativa para la preparación de la oferta."
            )
        elif len(items) > 0:
            notes.append(
                f"📋 {len(items)} renglones a cotizar."
            )

        # --- Garantía técnica ---
        gar_tec = self._get_str_val(self._get_nested(data, "penalties.garantia_tecnica"))
        if "no aplica" in gar_tec.lower():
            notes.append("✅ No se requiere garantía técnica adicional.")

        return notes

    # ─────────────────────────────────────────────────────────────
    # SCORING
    # ─────────────────────────────────────────────────────────────

    def _calculate_score(self, report: Dict) -> int:
        """
        Calculate an overall opportunity score (0-100).
        Higher = more attractive opportunity, lower risk.
        """
        score = 100

        # Penalize for missing critical fields (-5 each)
        missing_critical = report["completeness_audit"].get("missing_critical", [])
        score -= len(missing_critical) * 5

        # Penalize for high risks (-15 each) and medium risks (-5 each)
        risk_data = report["risk_assessment"]
        score -= risk_data.get("high_count", 0) * 15
        score -= risk_data.get("medium_count", 0) * 5

        # Bonus for low risks (+3 each, up to 9)
        low_bonus = min(risk_data.get("low_count", 0) * 3, 9)
        score += low_bonus

        # Completeness bonus
        completeness = report["completeness_audit"].get("completeness_pct", 0)
        if completeness >= 90:
            score += 5
        elif completeness < 60:
            score -= 10

        return max(0, min(100, score))

    def _score_to_label(self, score: int) -> str:
        if score >= 80:
            return "Oportunidad Atractiva"
        elif score >= 60:
            return "Oportunidad Viable con Cautela"
        elif score >= 40:
            return "Requiere Evaluación Detallada"
        else:
            return "Alto Riesgo - Evaluar Cuidadosamente"

    # ─────────────────────────────────────────────────────────────
    # UTILITY HELPERS
    # ─────────────────────────────────────────────────────────────

    def _get_nested(self, data: Dict, path: str) -> Any:
        """Safely navigate nested dict with dot notation."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def _get_val(self, item: Any) -> Any:
        """Extract value from item, handling rich text dicts (with 'value' key) or direct values."""
        if isinstance(item, dict) and "value" in item:
            return item["value"]
        if isinstance(item, dict) and "provenance" in item: # Safety fallback?
             return item.get("value")
        return item

    def _get_str_val(self, item: Any) -> str:
        """Get string representation of value, safely."""
        val = self._get_val(item)
        return str(val) if val is not None else ""


    def _extract_days(self, text: str) -> Optional[int]:
        """Extract number of days from text like 'cinco (5) días hábiles'."""
        if not text:
            return None
        # Try parenthetical number first: (5)
        m = re.search(r'\((\d+)\)\s*d[ií]as', text)
        if m:
            return int(m.group(1))
        # Try standalone number
        m = re.search(r'(\d+)\s*d[ií]as', text)
        if m:
            return int(m.group(1))
        # Try written numbers
        written = {
            "uno": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5,
            "seis": 6, "siete": 7, "ocho": 8, "nueve": 9, "diez": 10,
            "quince": 15, "veinte": 20, "treinta": 30,
        }
        for word, num in written.items():
            if word in text.lower():
                return num
        return None

    def _extract_percentage(self, text: str) -> Optional[float]:
        """Extract the first percentage from text."""
        if not text:
            return None
        m = re.search(r'(\d+(?:[.,]\d+)?)\s*%', text)
        if m:
            return float(m.group(1).replace(",", "."))
        # Try written: "TRES POR CIENTO"
        written_pcts = {
            "uno por ciento": 1, "dos por ciento": 2, "tres por ciento": 3,
            "cuatro por ciento": 4, "cinco por ciento": 5,
            "diez por ciento": 10, "quince por ciento": 15,
        }
        lower = text.lower()
        for word, pct in written_pcts.items():
            if word in lower:
                return float(pct)
        return None

    def _extract_months(self, text: str) -> Optional[int]:
        """Extract months from text like '18 meses'."""
        if not text:
            return None
        m = re.search(r'(\d+)\s*meses', text)
        if m:
            return int(m.group(1))
        return None

    def _truncate(self, text: str, max_len: int = 200) -> str:
        """Truncate text to max length with ellipsis."""
        if not text:
            return ""
        if len(text) <= max_len:
            return text
        return text[:max_len-3] + "..."
