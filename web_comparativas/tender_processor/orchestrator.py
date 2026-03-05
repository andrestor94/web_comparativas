import logging
import hashlib
import time
from typing import Dict, List, Any
from .document_router import DocumentRouter
from .legal_agent import LegalAgent
from .product_agent import ProductAgent
from .analyst_agent import AnalystAgent

logger = logging.getLogger("wc.tender_processor")


class TenderProcessor:
    """
    Orquestador de Multi-Agentes Secuenciales (HYBRID MODE)
    Arquitectura: Regex Primary + LLM Enrichment Opcional
    """
    def __init__(self):
        self.router = DocumentRouter()
        self.legal_agent = LegalAgent()          # PRIMARY: Regex (< 1 seg)
        self.product_agent = ProductAgent()
        self.analyst_agent = AnalystAgent()

    def process_files(self, files: Dict[str, bytes]) -> Dict[str, Any]:
        """
        Main entry point.
        files: { 'filename.pdf': bytes, ... }
        """
        logger.info(f"Starting Hybrid Multi-Agent processing for {len(files)} files.")
        
        if not files:
            return {"error": "No files provided."}

        try:
            return self._process_internal(files)
        except Exception as e:
            logger.error(f"CRITICAL: TenderProcessor unhandled error: {e}", exc_info=True)
            return self._build_error_result(str(e), list(files.keys()))

    def _process_internal(self, files: Dict[str, bytes]) -> Dict[str, Any]:
        t0 = time.time()
        
        # 1. ROUTER (Ingestion & Classification)
        logger.info(f"Routing {len(files)} files via DocumentRouter...")
        routed = self.router.process_and_classify(files)
        
        # Log Audit
        for log in routed.get("audit_log", []):
            logger.info(f"[ROUTER] {log}")
            
        priority = routed.get("particular")
        support = routed.get("general")
        
        # Fallback Logic
        if not priority:
            if routed.get("support"):
                priority = routed["support"][0]
                logger.warning(f"No Priority Doc identified. Using fallback: {priority[0]}")
            elif routed.get("spreadsheets"):
                 return {"error": "No valid PDF/Image documents found to process text."}
            else:
                return {"error": "No valid documents found to process."}

        # 2. LEGAL / HEADER — Regex Primary (< 1 segundo)
        logger.info(f"Running Legal Agent (Regex) on {priority[0]}...")
        t_legal = time.time()
        legal_data = self.legal_agent.process(priority, support_doc=support)
        logger.info(f"Legal Agent completed in {time.time()-t_legal:.2f}s")
        

        
        # 4. PRODUCTS
        logger.info(f"Running Product Agent (Plumber/Table) on {priority[0]}...")
        t_prod = time.time()
        items = self.product_agent.process(priority)
        logger.info(f"Product Agent completed in {time.time()-t_prod:.2f}s")
        
        # 4. CONSOLIDATE
        def _get_val(val):
            return val.get("value") if isinstance(val, dict) and "value" in val else val

        proc_name = legal_data.get("objeto", "")
        sel_proc = legal_data.get("selection_procedure")
        proc_num = legal_data.get("process_number")
        
        proc_name_val = _get_val(proc_name) or ""
        sel_proc_val = _get_val(sel_proc)
        proc_num_val = _get_val(proc_num)
        
        if sel_proc_val and proc_num_val:
            proc_name_val = f"{sel_proc_val} N° {proc_num_val}"
        elif proc_num_val:
             if len(proc_name_val) > 100:
                 proc_name_val = f"Licitación N° {proc_num_val}"

        # Helper to wrap values if they aren't already rich ExtractedField dicts
        def _smart_wrap(val, source="legal", is_inferred=False):
            if isinstance(val, dict) and "value" in val and "provenance" in val:
                # If we're forcing inferred, override it in the dict
                if is_inferred:
                    val["is_inferred"] = True
                return val
            if val is None: return {"value": None, "is_inferred": is_inferred}
            return {"value": val, "is_inferred": is_inferred}

        # Override wrapped process_name with the computed one
        if proc_name_val != _get_val(proc_name):
            proc_name = _smart_wrap(proc_name_val, is_inferred=True)

        # Generate a unique ID for this processing run
        tender_id = hashlib.md5(f"{proc_num_val or ''}-{time.time()}".encode()).hexdigest()[:12]

        final_json = {
            "tender_id": tender_id,
            "summary": {
                "process_number": _smart_wrap(proc_num), 
                "process_name": _smart_wrap(proc_name),
                "expediente": _smart_wrap(legal_data.get("expediente", "")),
                "uoa": _smart_wrap(legal_data.get("organismo", "")),
                "status": {"value": "OPEN"},
                "stage": {"value": "Publicado"},
                "estimated_total_amount": {
                     "value": {
                         "amount": _get_val(legal_data.get("presupuesto_oficial")),
                         "currency": "ARS" 
                     }
                }
            },
            "basic_info": {
                "object": _smart_wrap(legal_data.get("objeto", "")),
                "opening_date": _smart_wrap(legal_data.get("fecha_apertura")),
                "submission_deadline": _smart_wrap(legal_data.get("fecha_apertura")), 
                "place": _smart_wrap(legal_data.get("lugar_apertura")),
                
                "selection_procedure": _smart_wrap(legal_data.get("selection_procedure") or "No informado"),
                "pliego_value": _smart_wrap(legal_data.get("pliego_value", {"amount": 0, "currency": "ARS"})),
                "offer_maintenance_term_days_business": _smart_wrap(legal_data.get("offer_maintenance_term_days_business")),
                "contact_email": _smart_wrap(legal_data.get("contact_email")),
                
                "physical_address": _smart_wrap(legal_data.get("delivery_place") or legal_data.get("lugar_apertura")),
                "payment_terms": _smart_wrap(legal_data.get("payment_terms"))
            },
            "guarantees": self._build_guarantees(legal_data),
            "schedule": {
                "consultation_deadline_relative": _smart_wrap(legal_data.get("consultation_deadline")), 
                "samples_rule": _smart_wrap(legal_data.get("samples_rule")),
                "mantenimiento_oferta_texto": _smart_wrap(legal_data.get("mantenimiento_oferta_texto")),
            },
            "delivery": self._build_delivery(legal_data),
            "penalties": self._build_penalties(legal_data),
            "payment": self._build_payment(legal_data),
            "requirements": {
                "value": [
                    {
                        "code": r.get("nro", ""),
                        "description": r.get("descripcion_literal", ""),
                        "mandatory": True, 
                        "type": r.get("subseccion", "III. Administrativos"),
                        "proof_document": r.get("archivo_adjuntar", "")
                    }
                    for r in legal_data.get("requirements", [])
                ]
            },
            "sobres": self._build_sobres(legal_data.get("requirements", [])),
            "items": items if items else [],
            "provenance": {
               "warnings": []
            }
        }
        
        # Auto-populate delivery locations from product items if empty
        if not final_json["delivery"].get("locations") and items:
            seen = set()
            locations = []
            for item in items:
                loc_name = item.get("delivery_location", "")
                cat_prog = item.get("programmatic_category", "")
                key = f"{cat_prog}|{loc_name}"
                if loc_name and key not in seen:
                    seen.add(key)
                    locations.append({
                        "name": loc_name,
                        "address": "",
                        "phones": [],
                        "cat_prog": cat_prog
                    })
            final_json["delivery"]["locations"] = locations
        
        # Add Warnings
        if not legal_data.get("presupuesto_oficial"):
            final_json["provenance"]["warnings"].append("Presupuesto oficial no detectado.")
        if not items:
            final_json["provenance"]["warnings"].append("No se detectaron renglones de productos.")
        if legal_data.get("_error"):
            final_json["provenance"]["warnings"].append(f"Error en agente legal: {legal_data['_error']}")
        
        # 5. ANALYST AGENT (Rule-based Expert System)
        logger.info("Running Analyst Agent (Rule-Based Expert System)...")
        try:
            analysis = self.analyst_agent.analyze(final_json)
        except Exception as e:
            logger.error(f"Analyst Agent error: {e}", exc_info=True)
            analysis = {
                "completeness_audit": {"checks": [], "total_fields": 0, "present_fields": 0,
                                       "completeness_pct": 0, "missing_critical": [], "missing_optional": [],
                                       "low_confidence_warnings": [], "reliability_score": 0},
                "risk_assessment": {"risks": [], "opportunities": [], "overall_risk": "N/D",
                                    "high_count": 0, "medium_count": 0, "low_count": 0},
                "executive_summary": {},
                "analyst_notes": [],
                "score": 0,
                "score_label": "Error en análisis",
            }
        
        # PROMOTE analysis sub-keys to top-level so template can access them directly
        final_json["analysis"] = analysis
        final_json["risk_assessment"] = analysis.get("risk_assessment", {})
        final_json["completeness_audit"] = analysis.get("completeness_audit", {})
        final_json["executive_summary"] = analysis.get("executive_summary", {})
        final_json["analyst_notes"] = analysis.get("analyst_notes", [])
        final_json["score"] = analysis.get("score", 0)
        final_json["score_label"] = analysis.get("score_label", "")
            
        # 6. LLM AUDITOR (Intelligent Risk & Consistency Check)
        logger.info("Running LLM Auditor (Intelligent Risk Assessment)...")
        try:
            from .llm_auditor import LLMAuditor
            auditor = LLMAuditor()
            from .pdf_utils import extract_text_pages_robust
            pages_text = extract_text_pages_robust(priority[1], filename=priority[0])
            llm_insights = auditor.audit_tender(final_json, pages_text)
            
            if llm_insights:
                # Merge insights into final JSON
                final_json["ia_insights"] = llm_insights.get("ia_insights", [])
                
                # Merge hidden risks into risk_assessment
                hidden_risks_dict_list = llm_insights.get("ia_hidden_risks", [])
                
                # In RAG format, it's a list of dicts: [ {"Riesgo GRAVE X": "Evidencia..."} ]
                for risk_dict in hidden_risks_dict_list:
                    if isinstance(risk_dict, dict):
                        for risk_title, risk_evidence in risk_dict.items():
                            final_json["risk_assessment"]["risks"].append({
                                "category": "Riesgo Oculto (IA)",
                                "level": "ALTO", 
                                "icon": "🤖",
                                "description": str(risk_title),
                                "recommendation": f"Evidencia (RAG): {risk_evidence}"
                            })
                    elif isinstance(risk_dict, str):
                         final_json["risk_assessment"]["risks"].append({
                                "category": "Riesgo Oculto (IA)",
                                "level": "ALTO", 
                                "icon": "🤖",
                                "description": risk_dict,
                                "recommendation": "Revisar cláusula específica en el pliego orginal."
                            })

                if hidden_risks_dict_list:
                    # Update counts and overall risk level
                    final_json["risk_assessment"]["high_count"] = final_json["risk_assessment"].get("high_count", 0) + len(hidden_risks_dict_list)
                    final_json["risk_assessment"]["overall_risk"] = "ALTO (IA Alert)"
                    
                    # Penalize score for AI discovered risks
                    new_score = max(0, final_json.get("score", 100) - (len(hidden_risks_dict_list) * 15))
                    final_json["score"] = new_score
                    
                    # Update score label
                    if new_score >= 80: label = "Oportunidad Atractiva"
                    elif new_score >= 60: label = "Oportunidad Viable con Cautela"
                    elif new_score >= 40: label = "Requiere Evaluación Detallada"
                    else: label = "Alto Riesgo - Evaluar Cuidadosamente"
                    final_json["score_label"] = label
                
                # Add AI executive summary
                ai_sum = llm_insights.get("ia_executive_summary")
                if ai_sum:
                    final_json["executive_summary"]["ai_summary"] = ai_sum
                    
                # Override core fields with AI Enriched Data (RAG Contextualized)
                enriched_data = llm_insights.get("ia_enriched_data", {})
                if enriched_data:
                    if enriched_data.get("delivery") and "no se detalla" not in enriched_data.get("delivery", "").lower():
                        if "delivery" not in final_json: final_json["delivery"] = {}
                        if "plazo" not in final_json["delivery"]: final_json["delivery"]["plazo"] = {}
                        final_json["delivery"]["plazo"]["value"] = enriched_data["delivery"]
                        
                    if enriched_data.get("payment") and "no se detalla" not in enriched_data.get("payment", "").lower():
                        if "payment" not in final_json: final_json["payment"] = {}
                        if "general" not in final_json["payment"]: final_json["payment"]["general"] = {}
                        final_json["payment"]["general"]["value"] = enriched_data["payment"]
                        
                    if enriched_data.get("penalties") and "no se detalla" not in enriched_data.get("penalties", "").lower():
                        if "penalties" not in final_json: final_json["penalties"] = {}
                        if "multas" not in final_json["penalties"]: final_json["penalties"]["multas"] = {}
                        final_json["penalties"]["multas"]["value"] = enriched_data["penalties"]
                        
                    if enriched_data.get("guarantees") and "no se detalla" not in enriched_data.get("guarantees", "").lower():
                        if "guarantees" not in final_json: final_json["guarantees"] = {}
                        # Mantenimiento de oferta in web_comparativas is often shown first in guarantees widget
                        if "offer_maintenance_pct" not in final_json["guarantees"]: final_json["guarantees"]["offer_maintenance_pct"] = {}
                        final_json["guarantees"]["offer_maintenance_pct"]["value"] = enriched_data["guarantees"]
                    
        except Exception as e:
            logger.error(f"LLM Auditor error (non-blocking): {e}", exc_info=True)
            
        # Final schema validation — ensure no missing keys
        self._ensure_schema(final_json)
        
        elapsed = time.time() - t0
        logger.info(f"Processing complete in {elapsed:.2f}s")
        return final_json

    # ─────────────────────────────────────────────────────────────
    # BUILDERS: safe construction of each section
    # ─────────────────────────────────────────────────────────────

    def _safe_extracted(self, val):
        """Converts ExtractedField dict or raw value to a safe template-ready dict."""
        if val is None:
            return {"value": None}
        if isinstance(val, dict) and "value" in val:
            return val
        return {"value": val}

    def _build_sobres(self, raw_requirements: list) -> list:
        """
        Groups requirements by envelope (Sobre) for the 'Sobres a Entregar' tab.
        Returns a list of dicts: [{"name": "Sobre N° 1", "documents": [...]}, ...]
        """
        import re
        sobres_map = {}  # key: sobre_name, value: list of docs
        
        for r in raw_requirements:
            code = r.get("nro", "")
            desc = r.get("descripcion_literal", "")
            adj = r.get("archivo_adjuntar", "")
            
            # Detect sobre number from the code pattern (Art12-S1-*, Art12-S2-*)
            sobre_match = re.search(r'Art\d+-S(\d+)', code)
            if sobre_match:
                sobre_num = sobre_match.group(1)
                sobre_name = f"Sobre N° {sobre_num}"
            elif "sobre n" in desc.lower() or "sobre n" in code.lower():
                # Fallback: try to detect from description
                m = re.search(r'[Ss]obre\s*N?\s*[°º]?\s*(\d+)', desc)
                if m:
                    sobre_name = f"Sobre N° {m.group(1)}"
                else:
                    continue  # Not a sobre-related requirement
            else:
                continue  # Not a sobre-related requirement
            
            if sobre_name not in sobres_map:
                sobres_map[sobre_name] = []
            
            # Clean up description (remove "Documentación del Sobre N° X:" prefix)
            clean_desc = re.sub(r'^Documentación del Sobre N°\s*\d+:\s*', '', desc).strip()
            if not clean_desc:
                clean_desc = desc
                
            sobres_map[sobre_name].append({
                "code": code,
                "description": clean_desc,
                "proof_document": adj,
                "checked": False  # For future checklist functionality
            })
        
        # Sort by sobre number and return as list
        result = []
        for name in sorted(sobres_map.keys()):
            result.append({
                "name": name,
                "documents": sobres_map[name],
                "count": len(sobres_map[name])
            })
        
        return result

    def _build_guarantees(self, legal_data: Dict) -> Dict:
        """Build guarantees section with safe defaults."""
        garantias = legal_data.get("garantias", {})
        return {
            "offer_maintenance_pct": self._safe_extracted(garantias.get("mantenimiento_oferta")),
            "contract_compliance_pct": self._safe_extracted(garantias.get("cumplimiento_contrato")),
            "impugnation_pct": {"value": "5%"},
            "advance_counter_guarantee": {"value": "100%"},
            "garantias_list": garantias.get("garantias_list", []),
        }

    def _build_delivery(self, legal_data: Dict) -> Dict:
        """Build delivery section, FLATTENED (no circular delivery.delivery)."""
        delivery_data = legal_data.get("delivery", {})
        return {
            "terms": self._safe_extracted(delivery_data.get("terms")),
            "modalidad": self._safe_extracted(delivery_data.get("modalidad")),
            "plazo": self._safe_extracted(delivery_data.get("plazo")),
            "horario": self._safe_extracted(delivery_data.get("horario")),
            "condiciones": self._safe_extracted(delivery_data.get("condiciones")),
            "vencimiento_minimo": self._safe_extracted(delivery_data.get("vencimiento_minimo")),
            "locations": delivery_data.get("locations", []),
        }

    def _build_penalties(self, legal_data: Dict) -> Dict:
        """Build penalties with safe ExtractedField wrapping."""
        raw = legal_data.get("penalties", {})
        result = {}
        for key in ["mora", "multas", "penalidades_generales", "jurisdiccion", "anticorrupcion", "garantia_tecnica"]:
            val = raw.get(key)
            if val is not None:
                result[key] = self._safe_extracted(val)
            else:
                result[key] = {"value": None}
        return result

    def _build_payment(self, legal_data: Dict) -> Dict:
        """Build payment section with safe defaults."""
        raw = legal_data.get("payment", {})
        result = {}
        for key in ["general", "anticipo", "saldo", "tipo_factura"]:
            val = raw.get(key)
            if val is not None:
                result[key] = self._safe_extracted(val)
            else:
                result[key] = {"value": None}
        return result

    def _ensure_schema(self, data: Dict):
        """
        Validates and fills defaults for ALL fields expected by the template.
        This is the LAST SAFETY NET before data reaches the UI.
        """
        # Summary defaults
        summary = data.setdefault("summary", {})
        for key in ["process_number", "process_name", "expediente", "uoa", "status", "stage"]:
            summary.setdefault(key, {"value": None})
        summary.setdefault("estimated_total_amount", {"value": {"amount": None, "currency": "ARS"}})

        # Basic info defaults
        basic = data.setdefault("basic_info", {})
        for key in ["object", "opening_date", "submission_deadline", "place",
                     "selection_procedure", "pliego_value", "offer_maintenance_term_days_business",
                     "contact_email", "physical_address", "payment_terms"]:
            basic.setdefault(key, {"value": None})

        # Guarantees defaults
        guarantees = data.setdefault("guarantees", {})
        for key in ["offer_maintenance_pct", "contract_compliance_pct", "impugnation_pct", "advance_counter_guarantee"]:
            guarantees.setdefault(key, {"value": None})
        guarantees.setdefault("garantias_list", [])

        # Schedule defaults
        schedule = data.setdefault("schedule", {})
        for key in ["consultation_deadline_relative", "samples_rule", "mantenimiento_oferta_texto"]:
            schedule.setdefault(key, {"value": None})

        # Delivery defaults
        delivery = data.setdefault("delivery", {})
        for key in ["terms", "modalidad", "plazo", "horario", "condiciones", "vencimiento_minimo"]:
            delivery.setdefault(key, {"value": None})
        delivery.setdefault("locations", [])

        # Penalties defaults
        penalties = data.setdefault("penalties", {})
        for key in ["mora", "multas", "penalidades_generales", "jurisdiccion", "anticorrupcion", "garantia_tecnica"]:
            penalties.setdefault(key, {"value": None})

        # Payment defaults
        payment = data.setdefault("payment", {})
        for key in ["general", "anticipo", "saldo", "tipo_factura"]:
            payment.setdefault(key, {"value": None})

        # Other defaults
        data.setdefault("requirements", {"value": []})
        data.setdefault("items", [])
        data.setdefault("provenance", {"warnings": []})
        data.setdefault("tender_id", "N/A")

        # Analysis defaults
        data.setdefault("risk_assessment", {"risks": [], "opportunities": []})
        data.setdefault("completeness_audit", {"reliability_score": 0, "low_confidence_warnings": []})
        data.setdefault("analyst_notes", [])
        data.setdefault("score", 0)
        data.setdefault("score_label", "")

    def _build_error_result(self, error_msg: str, filenames: List[str]) -> Dict[str, Any]:
        """Build a complete but empty result with just the error info."""
        result = {
            "error": error_msg,
            "tender_id": "error",
            "summary": {},
            "basic_info": {},
            "guarantees": {},
            "schedule": {},
            "delivery": {},
            "penalties": {},
            "payment": {},
            "requirements": {"value": []},
            "items": [],
            "provenance": {"warnings": [f"Error fatal: {error_msg}"]},
            "risk_assessment": {"risks": [], "opportunities": []},
            "completeness_audit": {"reliability_score": 0, "low_confidence_warnings": []},
            "analyst_notes": [],
            "score": 0,
            "score_label": "Error",
        }
        self._ensure_schema(result)
        return result
