import re
import logging
from typing import Dict, Any, Tuple, List, Optional
from .pdf_utils import extract_text_all_pages, extract_text_pages_robust, find_text_bbox
from .models import ExtractedField

logger = logging.getLogger("wc.legal_agent")

class LegalAgent:
    """
    AGENTE 2: LEGAL Y CABECERA (Legacy Ported Logic)
    Objetivo: Extraer cabecera y requisitos usando Regex robusto del script legacy.
    """
    def __init__(self):
        pass

    def process(self, priority_doc: Tuple[str, bytes], support_doc: Tuple[str, bytes] = None) -> Dict[str, Any]:
        try:
            return self._process_internal(priority_doc, support_doc)
        except Exception as e:
            logger.error(f"LegalAgent CRITICAL ERROR: {e}", exc_info=True)
            # Return a minimal valid structure so downstream never crashes
            return self._empty_result(str(e))

    def _empty_result(self, error_msg: str = "") -> Dict[str, Any]:
        """Returns a minimal valid dict so orchestrator/template never crash."""
        return {
            "process_number": None,
            "presupuesto_oficial": None,
            "fecha_apertura": None,
            "objeto": "",
            "lugar_apertura": None,
            "selection_procedure": None,
            "pliego_value": {"currency": "ARS", "amount": 0.0},
            "contact_email": None,
            "offer_maintenance_term_days_business": None,
            "consultation_deadline": None,
            "samples_rule": None,
            "mantenimiento_oferta_texto": None,
            "payment_terms": "No informado",
            "garantias": {"garantias_list": []},
            "penalties": {},
            "delivery": {"terms": {"value": "No informado"}},
            "delivery_term": {"value": "No informado"},
            "payment": {},
            "requirements": [],
            "_error": error_msg,
        }

    def _process_internal(self, priority_doc: Tuple[str, bytes], support_doc: Tuple[str, bytes] = None) -> Dict[str, Any]:
        filename, content = priority_doc
        
        # Store raw bytes for bounding box lookups
        self.current_pdf_bytes = content
        
        # 1. Full Text for Regex
        full_text = extract_text_all_pages(content, filename=filename)
        
        # 2. Pages List for Provenance
        self.current_pages = extract_text_pages_robust(content, filename=filename)
        self.current_filename = filename
        
        # Support text (Pliego General)
        support_text = ""
        support_filename = ""
        if support_doc:
            support_filename = support_doc[0]
            support_text = extract_text_all_pages(support_doc[1], filename=support_filename)

        # Extract fields
        info = self._extract_process_info_legacy(full_text)
        reqs = self._extract_requisitos_legacy(full_text)
        
        # Article-Based Extractions with ExtractedField provenance
        garantias = self._extract_garantias(full_text)
        penalties = self._extract_penalties(full_text)
        delivery = self._extract_delivery_detailed(full_text)
        payment = self._extract_payment_terms_detailed(full_text)
        
        # Merge Support doc data (fills gaps)
        if support_text:
             self._merge_garantias(garantias, self._extract_garantias(support_text))
             
        # Format Output
        return self._format_output(info, reqs, garantias, penalties, delivery, payment)

    def _locate_page(self, snippet: str) -> int:
        """Finds which page contains the snippet. Returns 1-based index or None."""
        if not snippet or len(snippet) < 5: return None
        
        # Normalize snippet for search
        snip_clean = self._clean_whitespace(snippet).lower()
        if len(snip_clean) > 50: snip_clean = snip_clean[:50]
        
        for i, page_text in enumerate(self.current_pages):
            if snip_clean in self._clean_whitespace(page_text).lower():
                return i + 1
        return None

    def _make_field(self, value: Any, evidence: str = "", section: str = "General", is_inferred: bool = False) -> ExtractedField:
        page = self._locate_page(evidence)
        bbox = None
        if page and hasattr(self, 'current_pdf_bytes') and evidence:
             bbox = find_text_bbox(self.current_pdf_bytes, page, evidence)
             
        return ExtractedField(
            value=value,
            source=self.current_filename,
            page=page,
            section=section,
            evidence_text=evidence,
            confidence=1.0 if page else 0.8,
            is_inferred=is_inferred,
            bbox=bbox
        )

    # =========================================================================
    #  LEGACY HELPERS (Ported from licitacion_pipeline_v2.py)
    # =========================================================================

    def _clean_whitespace(self, s: str) -> str:
        s = (s or "").replace("\u00a0", " ")
        s = re.sub(r"[ \t]+", " ", s)
        s = re.sub(r"\s*\n\s*", "\n", s)
        return s.strip()

    def _clean_one_line(self, s: str) -> str:
        return re.sub(r"\s+", " ", self._clean_whitespace(s)).strip()

    def _find_between(self, text: str, start_regex: str, end_regex: str) -> Optional[str]:
        m = re.search(start_regex + r"(.*?)" + end_regex, text, flags=re.IGNORECASE | re.DOTALL)
        return self._clean_whitespace(m.group(1)) if m else None

    def _find_line_after_label(self, text: str, label_regex: str) -> Optional[str]:
        m = re.search(label_regex + r"\s*:\s*([^\r\n]+)", text, flags=re.IGNORECASE)
        return self._clean_one_line(m.group(1)) if m else None

    def _extract_process_number(self, text: str) -> Optional[str]:
        # Legacy regex: PLIE-###
        m = re.search(r"\bPLIE-\d+-\d{4}\b", text, flags=re.IGNORECASE)
        if m: return m.group(0)
        # Fallback: Expte typical patterns
        m2 = re.search(r"(?i)(?:expediente|expte)\.?\s*[:\.]?\s*([0-9\-\./\s]+)", text)
        return m2.group(1).strip() if m2 else None

    def _normalize_money(self, num_str: str) -> Optional[float]:
        if not num_str: return None
        s = re.sub(r"[^0-9,\.]", "", num_str)
        # 1.000,00 -> 1000.00 logic
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma != -1 and last_dot != -1:
            if last_comma > last_dot: s = s.replace(".", "").replace(",", ".")
            else: s = s.replace(",", "")
        else:
            if "," in s and "." not in s:
                if re.search(r",\d{2}$", s): s = s.replace(",", ".")
                else: s = s.replace(",", "")
            if "." in s and "," not in s:
                if not re.search(r"\.\d{2}$", s): s = s.replace(".", "")
        try:
            return float(s)
        except:
            return None

    def _parse_pct(self, s: str) -> Optional[int]:
        if not s: return None
        m = re.search(r"(\d{1,3})\s*%", s)
        if m: return int(m.group(1))
        m = re.search(r"\(\s*(\d{1,3})\s*%\s*\)", s)
        if m: return int(m.group(1))
        return None

    def _first_sentence_or_line(self, s: str) -> str:
        one = self._clean_one_line(s)
        if len(one) > 260: return one[:260].rstrip() + "..."
        return one

    def _classify_requirement(self, desc: str) -> str:
        d = (desc or "").lower()
        tec_kw = ["anmat", "muestra", "vencimiento", "lote", "habilitación", "habilitacion", "certificado", "director técnico", "director tecnico", "trazabilidad", "cufe"]
        eco_kw = ["garant", "póliza", "poliza", "anticipo", "multa", "cotización", "cotizacion", "precio", "oferta económica", "oferta economica"]
        if any(k in d for k in tec_kw): return "II. Técnicos"
        if any(k in d for k in eco_kw): return "I. Económicos y financieros"
        return "III. Administrativos"

    def _extract_bullets(self, block: str) -> List[Tuple[str, str]]:
        _BULLET_RE = re.compile(r"^\s*([a-zA-Z])\)\s*(.+)\s*$")
        lines = (block or "").splitlines()
        items = []
        cur_k = None
        cur_parts = []
        
        for raw in lines:
            line = raw.rstrip("\r\n")
            m = _BULLET_RE.match(line)
            if m:
                if cur_k is not None: items.append((cur_k, cur_parts))
                cur_k = m.group(1).lower()
                cur_parts = [line.strip()]
            else:
                if cur_k is not None and line.strip():
                    cur_parts.append(line.strip())
        if cur_k is not None: items.append((cur_k, cur_parts)) # Flush last
        
        out = []
        for k, parts in items:
            out.append((k, self._clean_one_line(" ".join(parts))))
        return out

    def _intro_before_first_bullet(self, block: str) -> str:
        if not block: return ""
        m = re.search(r"(?m)^\s*[a-zA-Z]\)\s+", block)
        if not m: return ""
        return self._clean_one_line(block[:m.start()])

    def _split_sobre2_bullet_zone(self, block: str) -> Tuple[str, str]:
        if not block: return "", ""
        cut_re = re.compile(r"(?im)^(En la oferta econ[oó]mica|La omisi[oó]n|RECHAZO\b|RECHAZO Y/O|RECHAZO Y/O DESESTIMACIÓN)")
        m = cut_re.search(block)
        if not m: return block, ""
        return block[:m.start()], block[m.start():]

    def _paragraph_requirements_from_rest(self, rest: str) -> List[str]:
        if not rest: return []
        start_re = re.compile(r"(?im)^(En la oferta econ[oó]mica|La omisi[oó]n|RECHAZO\b|RECHAZO Y/O|RECHAZO Y/O DESESTIMACIÓN)")
        lines = rest.splitlines()
        chunks = []
        cur = []
        for raw in lines:
            line = raw.strip()
            if not line: continue
            if start_re.match(line):
                if cur: chunks.append(cur)
                cur = [line]
            else:
                if cur: cur.append(line)
                else: cur = [line]
        if cur: chunks.append(cur)
        return [self._clean_one_line(" ".join(parts)) for parts in chunks]

    # =========================================================================
    #  CORE LOGIC
    # =========================================================================
    
    def _extract_article(self, full_text: str, art_num: str, next_patterns: list = None) -> str:
        """Extract the full content of a specific article by its number.
        art_num can be '3', '35.1', etc.
        next_patterns: list of regex patterns that mark the end of the article.
        """
        # Build the start pattern
        start = rf'Art[ií]culo\s*{re.escape(art_num)}\.?\s*[^\n]*\n'
        # Build end patterns
        if not next_patterns:
            # Default: next article number (increment integer part)
            base = art_num.split('.')[0]
            try:
                next_num = str(int(base) + 1)
                next_patterns = [rf'Art[ií]culo\s*{next_num}\.']
            except ValueError:
                next_patterns = [r'Art[ií]culo\s*\d+']
        
        end_combined = '|'.join(next_patterns)
        m = re.search(start + rf'(.*?)(?={end_combined}|$)', full_text, re.DOTALL | re.IGNORECASE)
        if m:
            return self._clean_whitespace(m.group(1))
        return ""
    
    def _extract_garantias(self, full_text: str) -> Dict[str, Any]:
        """Extract guarantees including summary keys and list, all as ExtractedField."""
        g = {}
        garantias_list = []
        
        # 1. Mantenimiento de Oferta
        art17 = self._extract_article(full_text, '17', [r'Art[ií]culo\s*18\.'])
        if art17:
            m_pct = re.search(r'(\d+(?:[.,]\d+)?)\s*%', art17)
            pct_val = m_pct.group(1) + "%" if m_pct else "Ver Art. 17"
            
            # Create field matches
            f_tipo = self._make_field("Mantenimiento de oferta", art17, "Art. 17")
            f_pct = self._make_field(pct_val, art17, "Art. 17")
            
            g_item = {
                "tipo": f_tipo,
                "porcentaje": f_pct,
                "base": self._make_field("Presupuesto Oficial (estimado)", art17, "Art. 17"),
                "monto_estimado": None,
                "fuente": self._make_field("Pliego Particular Art. 17", art17, "Art. 17")
            }
            garantias_list.append(g_item)
            
            # Summary key for Orchestrator/Legacy
            g["mantenimiento_oferta"] = f_pct
            
        # 2. Cumplimiento de Contrato
        art24 = self._extract_article(full_text, '24', [r'Art[ií]culo\s*25\.'])
        if art24:
            m_pct = re.search(r'(\d+(?:[.,]\d+)?)\s*%', art24)
            pct_val = m_pct.group(1) + "%" if m_pct else "10%"
            
            f_tipo = self._make_field("Cumplimiento de contrato", art24, "Art. 24")
            f_pct = self._make_field(pct_val, art24, "Art. 24")
            
            g_item = {
                "tipo": f_tipo,
                "porcentaje": f_pct,
                "base": self._make_field("Monto total de adjudicación", art24, "Art. 24"),
                "monto_estimado": None,
                "fuente": self._make_field("Pliego Particular Art. 24", art24, "Art. 24")
            }
            garantias_list.append(g_item)
            g["cumplimiento_contrato"] = f_pct

        # 3. Contragarantía (Anticipo)
        art30 = self._extract_article(full_text, '30', [r'Art[ií]culo\s*31\.'])
        if art30 and re.search(r'(?i)anticipo', art30):
             label = "Contragarantía por Anticipo"
             f_tipo = self._make_field(label, art30, "Art. 30")
             f_pct = self._make_field("100% del anticipo", art30, "Art. 30")
             g_item = {
                "tipo": f_tipo,
                "porcentaje": f_pct,
                "base": self._make_field("Monto del anticipo", art30, "Art. 30"),
                "monto_estimado": None,
                "fuente": self._make_field("Pliego Particular Art. 30", art30, "Art. 30")
             }
             garantias_list.append(g_item)

        g["garantias_list"] = garantias_list
        return g

    def _extract_penalties(self, full_text: str) -> Dict[str, ExtractedField]:
        """Extract penalties with ExtractedField provenance."""
        pen = {}
        
        try:
            # Mora - Art.35
            art35 = self._extract_article(full_text, '35', [r'Art[ií]culo\s*35\.1', r'Art[ií]culo\s*36\.'])
            if art35:
                pen["mora"] = self._make_field(art35, art35, "Art. 35")
            
            # Multas - Art.35.1
            art35_1_match = re.search(r'Art[ií]culo\s*35\.1\.?\s*[^\n]*\n(.*?)(?=Art[ií]culo\s*36\.|$)', full_text, re.DOTALL | re.IGNORECASE)
            if art35_1_match:
                text = self._clean_whitespace(art35_1_match.group(1))
                pen["multas"] = self._make_field(text, text, "Art. 35.1")
            
            # Penalidades Generales - Art.32  (BUG FIX: was missing extraction)
            art32 = self._extract_article(full_text, '32', [r'Art[ií]culo\s*33\.'])
            if art32:
                pen["penalidades_generales"] = self._make_field(art32, art32, "Art. 32")
                
            # Jurisdiccion - Art.54 or similar
            art_juris = self._extract_article(full_text, '54', [r'Art[ií]culo\s*55\.'])
            if not art_juris:
                # Fallback search
                m_juris = re.search(r"(?i)(?:competencia|jurisdicci[oó]n)\s*(?:judicial|tribunales).*?(\.|:|;)", full_text)
                if m_juris: art_juris = self._clean_one_line(m_juris.group(0))
                
            if art_juris:
                 pen["jurisdiccion"] = self._make_field(art_juris, art_juris, "Jurisdicción")

            # Anticorrupcion - Art.60
            art_anti = self._extract_article(full_text, '60', [r'ANEXO', r'Art[ií]culo'])
            if art_anti:
                 pen["anticorrupcion"] = self._make_field(art_anti, art_anti, "Art. 60")
        except Exception as e:
            logger.error(f"Error extracting penalties: {e}", exc_info=True)
             
        return pen

    def _extract_payment_terms_detailed(self, full_text: str) -> Dict[str, ExtractedField]:
        """Extract payment terms into structured ExtractedFields."""
        pay = {}
        art30 = self._extract_article(full_text, '30', [r'Art[ií]culo\s*31\.'])
        
        if art30:
            pay["general"] = self._make_field(art30, art30, "Art. 30")
            
            # Anticipo
            m_anticipo = re.search(r'(?i)(\d{1,3}\s*%.*?anticipo.*?\.)', art30, re.DOTALL)
            if m_anticipo:
                val = self._clean_one_line(m_anticipo.group(1))
                pay["anticipo"] = self._make_field(val, val, "Art. 30")
            
            # Saldo
            m_saldo = re.search(r'(?i)((?:El\s+)?saldo.*?(?:conformado|factura).*?\.)', art30, re.DOTALL)
            if m_saldo:
                val = self._clean_one_line(m_saldo.group(1))
                pay["saldo"] = self._make_field(val, val, "Art. 30")
        else:
             # Fallback
             pay["general"] = self._make_field("No informado", "", "N/A")
             
        return pay

    def _extract_delivery_detailed(self, full_text: str) -> Dict[str, ExtractedField]:
        """Extract structured delivery info from Art.28 and Art.29 with provenance."""
        d = {}
        
        # Art.28
        art28 = self._extract_article(full_text, '28', [r'Art[ií]culo\\s*29\\.'])
        if art28:
            m_mod = re.search(r'(?i)((?:entrega|realiz).*?(?:total|parcial).*?(?:\.(?=\s)|$))', art28, re.DOTALL)
            if m_mod:
                val = self._clean_one_line(m_mod.group(1))
                d["modalidad"] = self._make_field(val, val, "Art. 28")
                
            m_plazo = re.search(r'(?i)((?:dentro|en\s+un\s+plazo).*?(?:hábiles|corridos).*?(?:\.(?=\s)|$))', art28, re.DOTALL)
            if m_plazo:
                val = self._clean_one_line(m_plazo.group(1))
                d["plazo"] = self._make_field(val, val, "Art. 28")

        # Art.29
        art29 = self._extract_article(full_text, '29', [r'Art[ií]culo\\s*30\\.'])
        if art29:
             m_hora = re.search(r'(?i)(lunes\s+a\s+viernes.*?\d{1,2}.*?hs\.?)', art29)
             if m_hora:
                 val = self._clean_one_line(m_hora.group(1))
                 d["horario"] = self._make_field(val, val, "Art. 29")

             # Extract delivery locations with addresses and phone numbers
             locations = []
             loc_pattern = r'\*\s*([^(\n]+?)\s*\(([^)]+?(?:tel[eé]fono|fono)\s*[\d\-/]+(?:\s*/\s*[\d\-/]+)*)\)'
             for m_loc in re.finditer(loc_pattern, art29, re.IGNORECASE):
                 raw_name = self._clean_one_line(m_loc.group(1))
                 raw_detail = m_loc.group(2)

                 phone_match = re.search(r'(?:Nro\.?\s*(?:de\s+)?tel[eé]fono\s*)([\d\-/\s]+)', raw_detail, re.IGNORECASE)
                 phones = []
                 address = raw_detail
                 if phone_match:
                     phone_str = phone_match.group(1).strip()
                     phones = [p.strip() for p in re.split(r'[/]', phone_str) if p.strip()]
                     address = raw_detail[:phone_match.start()].strip().rstrip(',').rstrip('\u2013').rstrip('-').strip()

                 locations.append({
                     "name": raw_name,
                     "address": address,
                     "phones": phones
                 })

             if locations:
                 d["locations"] = locations

             # Conditions summary
             d["condiciones"] = self._make_field("Ver Art. 29", art29, "Art. 29")

        # Art.41 - Vencimiento mínimo
        art41 = self._extract_article(full_text, '41', [r'ANEXO', r'Página'])
        if art41:
            m_venc = re.search(r'(?i)(\d+)\s*(?:\()?.{0,30}meses', art41)
            if m_venc:
                val = f"{m_venc.group(1)} meses"
                d["vencimiento_minimo"] = self._make_field(val, art41, "Art. 41")
 
        # Summary 'terms' for legacy compatibility
        term_parts = []
        if d.get("modalidad"): term_parts.append(d["modalidad"].value)
        if d.get("plazo"): term_parts.append(d["plazo"].value)
        
        summary_val = " / ".join(term_parts) if term_parts else "Ver detalle"
        d["terms"] = self._make_field(summary_val, "", "Resumen")

        return d

    def _format_output(self, info, reqs, garantias, penalties, delivery, payment) -> Dict[str, Any]:
        """Assemble the final dictionary structure and serialize ExtractedFields."""
        
        def to_serializable(obj):
            if isinstance(obj, ExtractedField):
                return obj.to_dict()
            if isinstance(obj, list):
                return [to_serializable(x) for x in obj]
            if isinstance(obj, dict):
                return {k: to_serializable(v) for k, v in obj.items()}
            return obj

        # Assemble logic identical to previous but serialize everything
        data = {
            "process_number": to_serializable(info.get("process_number")),
            "presupuesto_oficial": to_serializable(info.get("presupuesto_oficial")),
            "fecha_apertura": to_serializable(info.get("fecha_apertura")),
            "objeto": to_serializable(info.get("objeto")),
            "lugar_apertura": to_serializable(info.get("lugar_apertura")),
            "selection_procedure": to_serializable(info.get("selection_procedure")),
            "pliego_value": to_serializable(info.get("pliego_value")),
            "contact_email": to_serializable(info.get("contact_email")),
            "offer_maintenance_term_days_business": to_serializable(info.get("offer_maintenance_term_days_business")),
            "consultation_deadline": to_serializable(info.get("consultation_deadline")),
            "samples_rule": to_serializable(info.get("samples_rule")),
            "mantenimiento_oferta_texto": to_serializable(info.get("mantenimiento_oferta_texto")),
            "payment_terms": to_serializable(info.get("payment_terms")),
            
            # Complex Sections
            "garantias": to_serializable(garantias),
            "penalties": to_serializable(penalties),
            "delivery": to_serializable(delivery),
            "payment": to_serializable(payment),
            
            # Requirements (Legacy)
            "requirements": reqs
        }
        return data

    def _extract_process_info_legacy(self, full_text: str) -> Dict[str, Any]:
        info = {}
        
        # 1. Nro Proceso
        pnum = self._extract_process_number(full_text)
        if pnum: info["process_number"] = self._make_field(pnum, pnum, "Cabecera")
        
        # 2. Objeto
        obj = self._find_between(full_text, r"Artículo\s*1\.?\s*Objeto", r"Artículo\s*2\.")
        if obj: 
            val = self._clean_one_line(obj)
            info["objeto"] = self._make_field(val, obj, "Art. 1 - Objeto")
        else:
            # Fallback
            p_obj = re.search(r"(?i)(?:objeto|concepto)\s*(?:del llamado)?[:\.]?\s*(.{10,300})", full_text)
            if p_obj: 
                val = self._clean_one_line(p_obj.group(1))
                info["objeto"] = self._make_field(val, p_obj.group(0), "Objeto")

        # 3. Presupuesto
        pres = self._find_between(full_text, r"Artículo\s*2\.?\s*Presupuesto\s+Oficial", r"Artículo\s*3\.")
        if pres:
            m2 = re.search(r"\$\s*([0-9\.,]+)", pres)
            if m2:
                val = self._normalize_money(m2.group(1))
                info["presupuesto_oficial"] = self._make_field(val, pres, "Art. 2 - Presupuesto")
        
        # 4. Lugar
        maddr = re.search(r"Dirección\s+de\s+Compras\s+y\s+Licitaciones.*?sita\s+en\s+([^\n]+)", full_text, flags=re.IGNORECASE)
        if maddr:
            val = self._clean_one_line(maddr.group(1))
            info["lugar_apertura"] = self._make_field(val, maddr.group(0), "Lugar de apertura")
        else:
            # Fallback generic
            m_lugar = re.search(r"(?i)(?:lugar|sitio)\s+de\s+(?:apertura|presentaci[oó]n).*?[:\.]?\s*((?:(?!Art[ií]culo).){10,150})", full_text)
            if m_lugar: 
                val = self._clean_one_line(m_lugar.group(1))
                info["lugar_apertura"] = self._make_field(val, m_lugar.group(0), "Lugar de apertura")
            
        # 6. Dates & Deadline
        p_date = re.search(r"(?i)(?:apertura).*?(\d{1,2}\s*[/]\s*\d{1,2}\s*[/]\s*\d{2,4})", full_text)
        if not p_date:
             p_date = re.search(r"(?i)(?:apertura).*?(\d{1,2}\s+de\s+[a-z]+\s+de\s+\d{4})", full_text)
        
        if p_date: 
            val = self._clean_one_line(p_date.group(1))
            info["fecha_apertura"] = self._make_field(val, p_date.group(0), "Fechas")
        
        # Detailed Delivery
        info["delivery"] = self._extract_delivery_detailed(full_text)
        info["delivery_term"] = info["delivery"].get("terms") # Verification legacy
        
        # Improved Consultation Deadline
        p_consultas = re.search(r"(?i)(?:consultas?|aclaraciones).*?(?:hasta|vencimiento|l[íi]mite|tope|cierre).*?(\d{1,2}\s*[/]\s*\d{1,2}\s*[/]\s*\d{2,4}(?:\s+a\s+las\s+\d{1,2}:\d{2})?)", full_text, flags=re.DOTALL)
        if p_consultas: 
             val = self._clean_one_line(p_consultas.group(1))
             info["consultation_deadline"] = self._make_field(val, p_consultas.group(0), "Fechas de consultas")
        else:
             p_rel = re.search(r"(?i)(?:consultas?).*?(?:hasta|antelaci[oó]n).*?(\d+\s*(?:hs\.?|horas|días|dias)\s+antes.*?(?:apertura|acto))", full_text, flags=re.DOTALL)
             if p_rel: 
                 val = self._clean_one_line(p_rel.group(1))
                 info["consultation_deadline"] = self._make_field(val, p_rel.group(0), "Fechas de consultas")
             else:
                 p_consultas_old = re.search(r"(?i)consultas\s+al\s+pliego.*?(?:hasta|antes\s+de)\s*([^.\n]{10,200})", full_text)
                 if p_consultas_old: 
                     val = self._clean_one_line(p_consultas_old.group(1))
                     info["consultation_deadline"] = self._make_field(val, p_consultas_old.group(0), "Fechas de consultas")

        # Samples Rule (Muestras) - Art.38
        art38 = self._extract_article(full_text, '38', [r'Art[ií]culo\s*39\.', r'Articulo\s*39\.'])
        if art38:
            info["samples_rule"] = self._make_field(art38, art38, "Art. 38 - Muestras")
        else:
            p_muestras = re.search(r"(?i)Art[ií]culo?\s*\d+\.?\s*Muestras\s*[:\.]?\s*((?:(?!Art[ií]culo\s*\d).){10,3000})", full_text, flags=re.DOTALL)
            if p_muestras:
                val = self._clean_whitespace(p_muestras.group(1))
                info["samples_rule"] = self._make_field(val, p_muestras.group(0), "Muestras")
        
        # 7. Payment Terms 
        info["payment_terms"] = self._make_field("No informado", "", "N/A")
        info["payment"] = {}
        
        art30 = self._extract_article(full_text, '30', [r'Art[ií]culo\s*31\.'])
        if art30:
            info["payment_terms"] = self._make_field(art30, art30, "Art. 30")
            
            m_anticipo = re.search(r'(?i)(\d{1,3}\s*%.*?anticipo.*?\.)', art30, re.DOTALL)
            if not m_anticipo:
                m_anticipo = re.search(r'(?i)(\d{1,3}\s*%.*?(?:póliza|cauci[oó]n).*?\.)', art30, re.DOTALL)
            if m_anticipo:
                val = self._clean_one_line(m_anticipo.group(1))
                info["payment"]["anticipo"] = self._make_field(val, m_anticipo.group(0), "Art. 30")
            
            m_saldo = re.search(r'(?i)((?:El\s+)?saldo.*?(?:conformado|factura).*?\.)', art30, re.DOTALL)
            if m_saldo:
                val = self._clean_one_line(m_saldo.group(1))
                info["payment"]["saldo"] = self._make_field(val, m_saldo.group(0), "Art. 30")
            
            m_factura = re.search(r'(?i)((?:La\s+)?factura.*?Factura\s+[A-Z].*?\.)', art30, re.DOTALL)
            if m_factura:
                val = self._clean_one_line(m_factura.group(1))
                info["payment"]["tipo_factura"] = self._make_field(val, m_factura.group(0), "Art. 30")
        else:
            payment_patterns = [
                r"(?i)(?:condici[oó]n|forma|plazo|t[ée]rminos?)\s+de\s+pago\s*[:\.]?\s*((?:(?!Art[ií]culo\s*\d).){4,3000})",
                r"(?i)(?:facturaci[oó]n)\s*[:\.]?\s*((?:(?!Art[ií]culo\s*\d).){4,3000})",
            ]
            for pat in payment_patterns:
                m_pago = re.search(pat, full_text, flags=re.DOTALL)
                if m_pago:
                    val = self._clean_whitespace(m_pago.group(1))
                    val = re.sub(r"^[:\.\-\s]+", "", val)
                    if len(val) > 4:
                        info["payment_terms"] = self._make_field(val, m_pago.group(0), "Condición de pago")
                        break

        # 8. Penalties
        info["penalties"] = self._extract_penalties(full_text)

        # 9. Missing Fields
        # A. Procedimiento
        header_text = full_text[:2000] 
        proc_val = None
        if re.search(r"Licitaci[oó]n\s+P[uú]blica", header_text, re.IGNORECASE):
            proc_val = "Licitación Pública"
        elif re.search(r"Licitaci[oó]n\s+Privada", header_text, re.IGNORECASE):
            proc_val = "Licitación Privada"
        elif re.search(r"Concurso\s+de\s+Precios", header_text, re.IGNORECASE):
             proc_val = "Concurso de Precios"
        else:
            m_proc = re.search(r"(?i)(?:tipo|procedimiento)\s+de\s+(?:selecci[oó]n|contrataci[oó]n).*?[:\.]?\s*([^.\n]{5,100})", full_text)
            if m_proc: proc_val = self._clean_one_line(m_proc.group(1))
        
        if proc_val:
             info["selection_procedure"] = self._make_field(proc_val, proc_val, "Cabecera")

        # B. Valor Pliego 
        info["pliego_value"] = self._make_field({"currency": "ARS", "amount": 0.0}, "", "Valor Pliego")
        
        art3 = self._extract_article(full_text, '3', [r'Art[ií]culo\s*4\.'])
        pliego_found = False
        if art3:
            m_num = re.search(r'\$\s*([0-9][0-9\.,]+)', art3)
            if m_num:
                amt = self._normalize_money(m_num.group(1))
                if amt is not None and amt > 0:
                    info["pliego_value"] = self._make_field({"currency": "ARS", "amount": amt}, art3, "Art. 3")
                    pliego_found = True
        
        if not pliego_found:
            patterns_pliego = [
                r'(?i)valor\s+del\s+presente\s+[Pp]liego.*?\$\s*([0-9][0-9\.,]+)',
                r'(?i)valor\s+(?:del|de\s+este)?\s*pliego.*?\$\s*([0-9][0-9\.,]+)',
                r'(?i)(?:valor|costo|precio)\s+(?:del|de)?\s*pliego.*?\$\s*([0-9][0-9\.,]+)',
            ]
            for pat in patterns_pliego:
                m_val = re.search(pat, full_text, re.DOTALL)
                if m_val:
                    amt = self._normalize_money(m_val.group(1))
                    if amt is not None and amt > 0:
                        info["pliego_value"] = self._make_field({"currency": "ARS", "amount": amt}, m_val.group(0), "Valor Pliego")
                        pliego_found = True
                        break

        # C. Mantenimiento Oferta
        art16 = self._extract_article(full_text, '16', [r'Art[ií]culo\s*17\.'])
        if art16:
            m_mant = re.search(r'(?i)(?:(\d+)|(?:treinta|sesenta|noventa|quince))\s*\(?(\d+)?\)?\s*(?:d[ií]as)', art16)
            if m_mant:
                val = m_mant.group(1) or m_mant.group(2)
                if val:
                    info["offer_maintenance_term_days_business"] = self._make_field(int(val), art16, "Art. 16")
            
            if "offer_maintenance_term_days_business" not in info:
                map_num = {"treinta": 30, "sesenta": 60, "noventa": 90, "quince": 15}
                for word, num in map_num.items():
                    if word in art16.lower():
                        info["offer_maintenance_term_days_business"] = self._make_field(num, art16, "Art. 16")
                        break
            
            info["mantenimiento_oferta_texto"] = self._make_field(art16, art16, "Art. 16")
        else:
            m_mant = re.search(r'(?i)(?:mantenimiento|validez)\s+de\s+(?:la\s+)?oferta.*?(?:(\d+)|(treinta|sesenta|noventa|quince))\s*(?:d[ií]as)?', full_text)
            if m_mant:
                val = m_mant.group(1)
                num = None
                if val and val.isdigit():
                    num = int(val)
                elif m_mant.group(2):
                    map_num = {"treinta": 30, "sesenta": 60, "noventa": 90, "quince": 15}
                    num = map_num.get(m_mant.group(2).lower(), m_mant.group(2))
                
                if num is not None:
                     info["offer_maintenance_term_days_business"] = self._make_field(num, m_mant.group(0), "Mantenimiento Oferta")

        # D. Contacto
        m_email = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", full_text)
        if m_email:
             info["contact_email"] = self._make_field(m_email.group(0), m_email.group(0), "Contacto")
        
        return info

    def _extract_requisitos_legacy(self, text: str) -> List[Dict]:
        reqs = []
        
        # Art 10
        art10 = self._find_between(text, r"Artículo\s*10\.-\s*Condiciones\s+para\s+ser\s+oferente", r"Artículo\s*11\.-")
        if art10:
            for k, desc in self._extract_bullets(art10):
                full_desc = f"Condiciones para ser oferente: {desc}"
                reqs.append({"nro": f"Art10-{k}", "subseccion": self._classify_requirement(desc), "descripcion_literal": full_desc, "archivo_adjuntar": ""})

        # Art 11
        art11 = self._find_between(text, r"Artículo\s*11\.-\s*Impedimentos\s+para\s+ser\s+Oferentes", r"Artículo\s*12\.")
        if art11:
            for k, desc in self._extract_bullets(art11):
                full_desc = f"Impedimentos para ser oferentes: {desc}"
                reqs.append({"nro": f"Art11-{k}", "subseccion": self._classify_requirement(desc), "descripcion_literal": full_desc, "archivo_adjuntar": ""})

        # Sobre 1
        sobre1 = self._find_between(text, r"Sobre\s*N\s*[°º]\s*1\s*:", r"Sobre\s*N\s*[°º]\s*2\s*:")
        if sobre1:
            intro = self._intro_before_first_bullet(sobre1)
            if intro: reqs.append({"nro": "Art12-S1-Intro", "subseccion": self._classify_requirement(intro), "descripcion_literal": intro, "archivo_adjuntar": ""})
            for k, desc in self._extract_bullets(sobre1):
                adj = re.sub(r"^[a-zA-Z]\)\s*", "", desc).strip()
                full_desc = f"Documentación del Sobre N° 1: {desc}"
                reqs.append({"nro": f"Art12-S1-{k}", "subseccion": self._classify_requirement(desc), "descripcion_literal": full_desc, "archivo_adjuntar": adj})

        # Sobre 2
        sobre2 = self._find_between(text, r"Sobre\s*N\s*[°º]\s*2\s*:", r"Artículo\s*13\.")
        if sobre2:
            intro = self._intro_before_first_bullet(sobre2)
            if intro: reqs.append({"nro": "Art12-S2-Intro", "subseccion": self._classify_requirement(intro), "descripcion_literal": intro, "archivo_adjuntar": ""})
            
            bullet_zone, rest = self._split_sobre2_bullet_zone(sobre2)
            
            for k, desc in self._extract_bullets(bullet_zone):
                adj = re.sub(r"^[a-zA-Z]\)\s*", "", desc).strip()
                full_desc = f"Documentación del Sobre N° 2: {desc}"
                reqs.append({"nro": f"Art12-S2-{k}", "subseccion": self._classify_requirement(desc), "descripcion_literal": full_desc, "archivo_adjuntar": adj})
                
            extra_pars = self._paragraph_requirements_from_rest(rest)
            idx = 1
            for p in extra_pars:
                full_desc = f"Otras consideraciones: {p}"
                reqs.append({"nro": f"Art12-S2-P{idx}", "subseccion": "III. Administrativos", "descripcion_literal": full_desc, "archivo_adjuntar": ""})
                idx += 1

        return reqs

    def _merge_garantias(self, target: Dict, source: Dict):
        """Merge garantías data from support document into target. Uses correct keys."""
        try:
            # Merge summary keys (mantenimiento_oferta, cumplimiento_contrato)
            if not target.get("mantenimiento_oferta") and source.get("mantenimiento_oferta"):
                target["mantenimiento_oferta"] = source["mantenimiento_oferta"]
            if not target.get("cumplimiento_contrato") and source.get("cumplimiento_contrato"):
                target["cumplimiento_contrato"] = source["cumplimiento_contrato"]
            
            # Merge garantias_list (append items not already present)
            target_list = target.get("garantias_list", [])
            source_list = source.get("garantias_list", [])
            target_tipos = set()
            for g in target_list:
                tipo = g.get("tipo")
                if isinstance(tipo, dict):
                    tipo = tipo.get("value", "")
                elif isinstance(tipo, ExtractedField):
                    tipo = tipo.value
                target_tipos.add(str(tipo).lower())
            
            for g in source_list:
                tipo = g.get("tipo")
                if isinstance(tipo, dict):
                    tipo = tipo.get("value", "")
                elif isinstance(tipo, ExtractedField):
                    tipo = tipo.value
                if str(tipo).lower() not in target_tipos:
                    target_list.append(g)
            
            target["garantias_list"] = target_list
        except Exception as e:
            logger.error(f"Error merging garantías: {e}", exc_info=True)
