import re
import logging
import io
import pdfplumber
import json
from typing import List, Dict, Any, Tuple
from .pdf_utils import extract_text_all_pages, extract_text_pages_robust
from .ollama_client import OllamaClient


logger = logging.getLogger("wc.product_agent")

# ─────────────────────────────────────────────────────────────────
# KNOWN UNITS for matching vertical format items
# ─────────────────────────────────────────────────────────────────
KNOWN_UNITS = {
    "UN", "UNIDAD", "UNIDADES", "UNI", "U",
    "CAJA", "CAJAS", "CJ",
    "ENVASE", "ENVASES", "ENV",
    "LITRO", "LITROS", "LT", "L",
    "ML", "CC",
    "KG", "GR", "GRAMOS", "G",
    "MTS", "METROS", "M", "MT",
    "ROLLO", "ROLLOS", "RL",
    "PACK", "PACKS",
    "PAR", "PARES",
    "BLISTER", "BLISTERS",
    "FRASCO", "FRASCOS",
    "AMPOLLA", "AMPOLLAS",
    "TUBO", "TUBOS",
    "BIDON", "BIDONES",
    "BOLSA", "BOLSAS",
    "SOBRE", "SOBRES",
    "LATA", "LATAS",
    "COMPRIMIDO", "COMPRIMIDOS",
    "RESMA", "RESMAS",
}


class ProductAgent:
    """
    AGENTE 3: PRODUCTOS (Tabla de Renglones)
    Objetivo: Extraer renglones usando pdfplumber (tablas) o fallback a Regex.
    Soporta múltiples formatos de tabla de pliegos argentinos.
    """
    def __init__(self, ollama_url="http://localhost:11434", model="deepseek-r1:8b"):
        self.client = OllamaClient(base_url=ollama_url, default_model=model)

    def process(self, priority_doc: Tuple[str, bytes]) -> List[Dict]:
        try:
            return self._process_internal(priority_doc)
        except Exception as e:
            logger.error(f"ProductAgent CRITICAL ERROR: {e}", exc_info=True)
            return []

    def _process_internal(self, priority_doc: Tuple[str, bytes]) -> List[Dict]:
        fname, content = priority_doc
        
        # 1. Extract text pages
        pages_text = extract_text_pages_robust(content)
        if not pages_text:
            logger.warning("No pages text extracted from document.")
            pages_text = []
        
        full_text = "\n".join(pages_text) if pages_text else ""
        header_info = self._extract_header_info(full_text)
        
        # 2. Try PDF Plumber (Best for Digital Tables with visible lines)
        items = self._extract_with_plumber(content, fname)
        
        if items:
            logger.info(f"PDFPlumber extracted {len(items)} items.")
        else:
            # 3. Try Vertical Format extraction (most common in Argentine pliegos)
            logger.info("PDFPlumber found no items. Trying vertical format extraction...")
            items = self._extract_vertical_format(pages_text, fname)
            
            if items:
                logger.info(f"Vertical format extracted {len(items)} items.")
            else:
                # 4. Final Fallback: LLM (Ollama)
                logger.info("Vertical format found no items. Trying Intelligent LLM Fallback...")
                items = self._extract_with_llm(pages_text, fname)
        
        # 5. Merge Header Info & Defaults
        for i in items:
            if not i.get("category_code"): i["category_code"] = "NO ENCONTRADO"
            if not i.get("programmatic_category"): 
                i["programmatic_category"] = header_info.get("cat_prog", "NO ENCONTRADO")
            if not i.get("delivery_location"):
                i["delivery_location"] = header_info.get("efector", "NO ENCONTRADO")
            
        logger.info(f"ProductAgent found {len(items)} items total.")
        return items

    # ─────────────────────────────────────────────────────────────────
    # EXTRACTION METHOD LLM: OLLAMA FALLBACK
    # ─────────────────────────────────────────────────────────────────

    def _extract_with_llm(self, pages_text: List[str], fname: str) -> List[Dict]:
        """Usa a Ollama para entender tablas de renglones completamente destrozadas o texto corrido."""
        if not pages_text:
            return []
            
        # Para no explotar el contexto, buscamos las páginas que parezcan tener renglones
        relevant_text = ""
        for page in pages_text:
            page_low = page.lower()
            if any(k in page_low for k in ["renglon", "renglón", "item", "cantidad", "especificacion", "detalle", "precio"]):
                relevant_text += page + "\n"
                
        if len(relevant_text) < 50:
             relevant_text = "\n".join(pages_text[:8]) # Fallback total
             
        if len(relevant_text) > 25000:
             relevant_text = relevant_text[:25000]

        sys_prompt = '''Eres un especialista en extracción de datos de compras públicas.
Extrae TODOS los artículos (renglones) solicitados en la licitación.
RESPONDE ÚNICAMENTE CON UN JSON que contenga la clave "items" siendo esto una lista de objetos.
Esquema de salida exacto:
{
  "items": [
    {
      "renglon": "1",
      "cantidad": 100,
      "unidad": "U",
      "descripcion": "Gasas estériles 10x10"
    }
  ]
}
No inventes datos.'''

        prompt = f"TEXTO DEL PLIEGO:\n\n{relevant_text}\n\nExtrae la tabla."
        
        logger.info("ProductAgent: Enviando tabla difusa al LLM...")
        llm_response = self.client.generate_json(prompt=prompt, system_prompt=sys_prompt)
        
        items_out = []
        if llm_response and "items" in llm_response and isinstance(llm_response["items"], list):
            for i, r in enumerate(llm_response["items"]):
                items_out.append({
                    "line_number": str(r.get("renglon", i+1)),
                    "description": r.get("descripcion", ""),
                    "quantity": self._clean_number(str(r.get("cantidad", ""))) if r.get("cantidad") else 0,
                    "unit": str(r.get("unidad", "U")),
                    "category_code": "",
                    "category_name": "General",
                    "provenance": {
                        "source": fname,
                        "page": 1,
                        "method": "llm_fallback"
                    }
                })
        return items_out

    # ─────────────────────────────────────────────────────────────────
    # HEADER INFO EXTRACTION
    # ─────────────────────────────────────────────────────────────────

    def _extract_header_info(self, text: str) -> Dict[str, str]:
        info = {}
        # Pattern: Cat. Prog. 16.00.00 Hospital houssay)
        m = re.search(r"Cat\.?\s*Prog\.?\s*([\d\.]+)\s*(.*?)(?:\)|$|\n)", text, flags=re.IGNORECASE)
        if m:
            info["cat_prog"] = m.group(1).strip()
            info["efector"] = self._clean_one_line(m.group(2)).replace(")", "").strip()
        else:
            # Fallback for Efector only
            m_hosp = re.search(r"Hospital\s+Municipal\s+[^\n]+Houssay", text, flags=re.IGNORECASE)
            if m_hosp: info["efector"] = self._clean_one_line(m_hosp.group(0))
            
        return info

    # ─────────────────────────────────────────────────────────────────
    # EXTRACTION METHOD 1: PDFPLUMBER (structured tables with lines)
    # ─────────────────────────────────────────────────────────────────

    def _extract_with_plumber(self, content: bytes, fname: str) -> List[Dict]:
        items = []
        try:
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages:
                    # Try multiple strategies
                    for strategy in [
                        {"vertical_strategy": "lines", "horizontal_strategy": "lines", "intersection_y_tolerance": 5},
                        {"vertical_strategy": "text", "horizontal_strategy": "text"},
                    ]:
                        tables = page.extract_tables(table_settings=strategy)
                        
                        for table in (tables or []):
                            if not table or len(table) < 2:
                                continue
                                
                            # Heuristic: Check if table looks like Renglones
                            header = [str(c).lower().strip() for c in table[0]]
                            
                            # Map columns flexibly
                            col_map = self._map_columns(header)
                            
                            if col_map.get("qty") is not None and col_map.get("desc") is not None:
                                for row in table[1:]:
                                    item = self._parse_plumber_row(row, col_map, fname, page.page_number)
                                    if item:
                                        items.append(item)
                            
                        if items:
                            break  # Don't try next strategy if we found items

        except Exception as e:
            logger.error(f"Plumber extraction error: {e}")
            
        return items

    def _map_columns(self, header: List[str]) -> Dict[str, int]:
        """
        Flexibly map column headers to expected fields.
        Handles many variations: cant/cantidad, desc/descripcion/detalle, 
        renglon/item/nro, unidad/uni/u.m., codigo, etc.
        """
        col_map = {}
        
        for i, col in enumerate(header):
            if not col:
                continue
            col_lower = col.lower().strip()
            
            # Quantity
            if any(kw in col_lower for kw in ["cant", "cantidad", "qty"]):
                col_map["qty"] = i
            # Description
            elif any(kw in col_lower for kw in ["desc", "detalle", "especif", "producto", "artículo", "articulo"]):
                col_map["desc"] = i
            # Unit
            elif any(kw in col_lower for kw in ["uni", "medida", "u.m"]):  
                col_map["unit"] = i
            # Line number / Renglón
            elif any(kw in col_lower for kw in ["reng", "item", "nro", "n°", "nº", "reng"]):
                col_map["line_num"] = i
            # Code
            elif any(kw in col_lower for kw in ["cod", "código", "codigo"]):
                col_map["code"] = i
            # Price
            elif any(kw in col_lower for kw in ["precio", "unit", "p.u"]):
                col_map["price"] = i
        
        return col_map

    def _parse_plumber_row(self, row, col_map, fname, page_num) -> Dict:
        """Parse a single row from a pdfplumber table given the column mapping."""
        try:
            idx_qty = col_map["qty"]
            idx_desc = col_map["desc"]
            
            if len(row) <= max(idx_qty, idx_desc):
                return None
                
            qty = row[idx_qty]
            desc = row[idx_desc]
            
            if not qty or not desc:
                return None
                
            if not self._is_number(qty):
                return None
            
            clean_qty = self._clean_number(qty)
            
            # Get optional fields
            unit = "u"
            if "unit" in col_map and col_map["unit"] < len(row):
                unit = row[col_map["unit"]] or "u"
            
            line_num = ""
            if "line_num" in col_map and col_map["line_num"] < len(row):
                line_num = row[col_map["line_num"]] or ""
            elif len(row) > 0:
                line_num = row[0] or ""
            
            code = ""
            if "code" in col_map and col_map["code"] < len(row):
                code = row[col_map["code"]] or ""
            
            return {
                "line_number": str(line_num).strip(),
                "description": str(desc).replace("\n", " ").strip(),
                "quantity": clean_qty,
                "unit": str(unit).strip(),
                "category_code": str(code).strip(),
                "category_name": "General",
                "provenance": {
                    "source": fname,
                    "page": page_num,
                    "method": "table_extraction"
                }
            }
        except Exception:
            return None

    # ─────────────────────────────────────────────────────────────────
    # EXTRACTION METHOD 2: VERTICAL FORMAT (3-line blocks)
    # Pattern: [quantity]\n[unit]\n[description, possibly multiline]
    # Grouped by Cat.Prog. sections
    # ─────────────────────────────────────────────────────────────────

    def _extract_vertical_format(self, pages_text: List[str], fname: str) -> List[Dict]:
        """
        Extracts items from the common Argentine pliego format where
        each item is laid out as:
          <quantity>          (e.g. "192" or "2,808")
          <unit>              (e.g. "UN")
          UN <description>    (e.g. "UN CERA PARA HUESOS TIPO HORSEY")
        
        Items are grouped under Cat.Prog. headers like:
          Cat. Prog. 16.00.00 Hospital houssay)
          Cant. Uni.
          Descripción
        """
        items = []
        current_cat_prog = ""
        current_efector = ""
        renglon_counter = 0
        
        for page_idx, page_text in enumerate(pages_text):
            page_num = page_idx + 1
            lines = page_text.split("\n")
            
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                
                # Check for Cat.Prog. section header
                cat_match = re.match(
                    r"Cat\.?\s*Prog\.?\s*([\d\.]+)\s*\(?(.*?)(?:\)|$)",
                    line, re.IGNORECASE
                )
                if cat_match:
                    current_cat_prog = cat_match.group(1).strip()
                    current_efector = cat_match.group(2).strip()
                    i += 1
                    # Skip "Cant. Uni." and "Descripción" header lines
                    while i < len(lines):
                        next_line = lines[i].strip().lower()
                        if next_line in ["cant. uni.", "descripción", "descripcion", 
                                         "cant.  uni.", "cant uni", ""]:
                            i += 1
                        else:
                            break
                    continue
                
                # Skip noise lines
                if self._is_noise_line(line):
                    i += 1
                    continue
                
                # Try to match vertical format: qty, then unit, then description
                if self._looks_like_quantity(line):
                    qty_str = line.strip()
                    
                    # Look ahead for unit line
                    if i + 1 < len(lines):
                        unit_line = lines[i + 1].strip()
                        
                        if self._looks_like_unit(unit_line):
                            # Look ahead for description (may span multiple lines)
                            desc_lines = []
                            j = i + 2
                            while j < len(lines):
                                desc_line = lines[j].strip()
                                
                                # Stop if we hit another quantity (start of next item)
                                if self._looks_like_quantity(desc_line):
                                    break
                                # Stop if we hit a section header
                                if re.match(r"Cat\.?\s*Prog\.?", desc_line, re.IGNORECASE):
                                    break
                                # Stop if we hit a page marker
                                if re.match(r"^Página\s+\d+\s+de\s+\d+", desc_line, re.IGNORECASE):
                                    break
                                if re.match(r"^PLIE-\d+-\d+$", desc_line):
                                    break
                                # Stop on empty or noise
                                if not desc_line:
                                    break
                                
                                desc_lines.append(desc_line)
                                j += 1
                            
                            if desc_lines:
                                description = " ".join(desc_lines)
                                # Remove leading "UN " if the unit is already captured
                                if unit_line.upper() in ("UN", "UNIDAD", "UNIDADES"):
                                    description = re.sub(r"^UN\s+", "", description, flags=re.IGNORECASE)
                                
                                renglon_counter += 1
                                clean_qty = self._clean_number(qty_str)
                                
                                items.append({
                                    "line_number": str(renglon_counter),
                                    "description": description.strip()[:500],
                                    "quantity": clean_qty,
                                    "unit": unit_line.upper().strip(),
                                    "category_code": "",
                                    "category_name": "General",
                                    "programmatic_category": current_cat_prog or "NO ENCONTRADO",
                                    "delivery_location": current_efector or "NO ENCONTRADO",
                                    "provenance": {
                                        "source": fname,
                                        "page": page_num,
                                        "method": "vertical_format"
                                    }
                                })
                                
                                i = j  # Skip past description lines
                                continue
                    
                i += 1
        
        return items

    def _looks_like_quantity(self, line: str) -> bool:
        """Check if a line looks like a standalone quantity number."""
        line = line.strip()
        if not line:
            return False
        # Must be a number, possibly with thousand separators (dots or commas)
        # e.g. "192", "2,808", "3.816", "12"
        # But NOT article numbers like "Art 35" or page numbers like "Página 12"
        if re.match(r"^\d{1,3}([.,]\d{3})*$", line):
            return True
        if re.match(r"^\d+$", line) and len(line) <= 6:
            return True
        return False

    def _looks_like_unit(self, line: str) -> bool:
        """Check if a line looks like a unit of measure."""
        line = line.strip().upper()
        if not line:
            return False
        return line in KNOWN_UNITS

    def _is_noise_line(self, line: str) -> bool:
        """Check if a line is a noise/header/footer line to skip."""
        if not line.strip():
            return True
        # Page markers
        if re.match(r"^Página\s+\d+\s+de\s+\d+", line, re.IGNORECASE):
            return True
        # Document reference numbers
        if re.match(r"^PLIE-\d+-\d+$", line):
            return True
        # Article headers (don't confuse with item data)
        if re.match(r"^Art[ií]culo\s+\d+", line, re.IGNORECASE):
            return True
        # "ANEXO" headers
        if re.match(r"^ANEXO\s", line, re.IGNORECASE):
            return True
        return False

    # ─────────────────────────────────────────────────────────────────
    # EXTRACTION METHOD 3: LEGACY REGEX (Renglón N° X format)
    # ─────────────────────────────────────────────────────────────────

    def _extract_renglones_legacy(self, pages_text: List[str], fname: str) -> List[Dict]:
        """Extract lines using regex on text, iterating per page."""
        renglones = []
        
        patterns = [
            # Pattern 1: Renglon X ... Cant Y ... Desc Z
            r"Rengl[oó]n\s*N?[°o]?\s*(\d+).*?Cant(?:\.|idad)?\s*:\s*(\d+(?:[.,]\d+)?).*?Desc(?:ripci[oó]n)?\s*:\s*(.*?)(?:Unitario|$)",
            # Pattern 2: Item X ...
            r"Item\s*(\d+)\s+(\d+(?:[.,]\d+)?)\s+(.*?)$",
            # Pattern 3: Generic Start with Number + Quantity + Unit
            r"^\s*(\d+)\s+(\d+(?:[.,]\d+)?)\s+(?:unid|u|caja|envase|ml|mts|gramos|litros)\.?\s+(.*?)$",
            # Pattern 4: Tabular line — Nro | Codigo | Desc | Qty | Unit
            r"^\s*(\d{1,3})\s+(?:[A-Z0-9\-\.]{3,}\s+)?(.{10,}?)\s+(\d+(?:[.,]\d+)?)\s*$",
        ]

        for i, text in enumerate(pages_text):
             page_num = i + 1
             for pat in patterns:
                 matches = re.findall(pat, text, re.DOTALL | re.IGNORECASE | re.MULTILINE)
                 for m in matches:
                      if len(m) >= 3:
                         nro = m[0]
                         cant = self._clean_number(m[1])
                         desc = m[2].strip()[:200]
                         
                         if not any(r["nro"] == nro for r in renglones):
                             renglones.append({
                                 "nro": nro,
                                 "cantidad": cant,
                                 "descripcion": desc,
                                 "unidad": "u",
                                 "codigo_item": "",
                                 "page": page_num
                             })
                             
        return renglones

    # ─────────────────────────────────────────────────────────────────
    # UTILITIES
    # ─────────────────────────────────────────────────────────────────

    def _clean_whitespace(self, s: str) -> str:
        s = (s or "").replace("\u00a0", " ")
        s = re.sub(r"[ \t]+", " ", s)
        return s.strip()

    def _clean_one_line(self, s: str) -> str:
        return re.sub(r"\s+", " ", self._clean_whitespace(s)).strip()

    def _is_number(self, s: Any) -> bool:
        if not s: return False
        try:
            # Handle Argentine number formats: "2,808" or "3.816"
            cleaned = str(s).strip().replace(".", "").replace(",", ".")
            float(cleaned)
            return True
        except:
            return False

    def _clean_number(self, s: Any) -> float:
        return self._normalize_int(str(s))

    def _normalize_int(self, num_str: str) -> float:
        if not num_str: return 0.0
        s = re.sub(r"[^0-9,\.]", "", str(num_str))
        # Handle Argentine format: 1.000,00 or 2,808
        # If has both dot and comma, dot is thousand separator
        if "." in s and "," in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            # Could be "2,808" (thousand) or "1,5" (decimal)
            parts = s.split(",")
            if len(parts) == 2 and len(parts[1]) == 3:
                # Likely thousand separator: 2,808 → 2808
                s = s.replace(",", "")
            else:
                # Likely decimal: 1,5 → 1.5
                s = s.replace(",", ".")
        elif "." in s:
            # Could be "3.816" (thousand) or "1.5" (decimal)
            parts = s.split(".")
            if len(parts) == 2 and len(parts[1]) == 3:
                # Likely thousand separator: 3.816 → 3816
                s = s.replace(".", "")
            # Otherwise keep as is (decimal)
        
        try:
            return float(s)
        except:
            return 0.0
