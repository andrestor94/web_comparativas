import logging
from typing import Dict, Any, List, Tuple, Optional
from .pdf_utils import extract_text_first_page_robust, convert_to_searchable_pdf

logger = logging.getLogger("wc.document_router")

class DocumentRouter:
    """
    AGENTE 1: DOCUMENT ROUTER (The Gatekeeper)
    Objetivo: 
      1. Ingestar múltiples archivos (PDF, IMG, XLS).
      2. Normalizar a FORMATO UNIFICADO (Searchable PDF).
      3. Clasificar por contenido (General, Particular, Anexos, etc.).
    """
    
    def process_and_classify(self, files_map: Dict[str, bytes]) -> Dict[str, Any]:
        """
        Input: { 'file.pdf': bytes, 'image.png': bytes }
        Output: {
           'particular': (filename, bytes),
           'general': (filename, bytes) or None,
           'support': [(filename, bytes)], # Anexos, circulares
           'spreadsheets': [(filename, bytes)],
           'audit_log': [...]
        }
        """
        logger.info(f"Routing {len(files_map)} files...")
        
        c = {
            "particular": None,
            "general": None,
            "support": [],
            "spreadsheets": [],
            "audit_log": []
        }
        
        # Keywords
        KW_PARTICULAR = ["particular", "pbcp", "condiciones particulares", "clausulas particulares", "especificaciones técnicas"]
        KW_GENERAL = ["general", "pbcg", "condiciones generales", "regimen de contrataciones"]
        KW_CIRCULAR = ["circular", "aclaratoria"]
        
        best_particular_score = 0
        
        for fname, content in files_map.items():
            ext = fname.lower().split('.')[-1]
            
            # 1. Spreadsheets -> Pass through
            if ext in ['xlsx', 'xls', 'csv']:
                c['spreadsheets'].append((fname, content))
                c['audit_log'].append(f"File {fname}: Spreadsheet detected.")
                continue
                
            # 2. PDF/Images -> Convert to Searchable PDF
            # Only convert if necessary (images) or distinctively scanned?
            # For efficiency, we first check if it's an image.
            # If PDF, we use as is for classification, but we might upgrade it later?
            # Actually, extract_text_robust in pdf_utils now handles images via convert_to_searchable.
            # But the Router should ideally standardize.
            
            # Let's verify content for classification
            # If image, we MUST convert to classify.
            searchable_content = content
            was_converted = False
            
            if ext in ['jpg', 'jpeg', 'png', 'bmp', 'tiff']:
                 logger.info(f" Converting image {fname} to searchable PDF...")
                 conv = convert_to_searchable_pdf(content, fname)
                 if conv:
                     searchable_content = conv
                     was_converted = True
            
            # Rename if converted to avoid downstream confusion
            current_fname = fname
            if was_converted:
                current_fname = fname + ".pdf"
            
            # Extract text for classification (first page is enough usually)
            # Use extract_text_first_page_robust which handles scanned first pages
            # Note: extract_text_first_page_robust takes bytes.
            first_page_text = extract_text_first_page_robust(searchable_content).lower()
            
            # Tag classification
            is_particular = any(k in first_page_text for k in KW_PARTICULAR)
            is_general = any(k in first_page_text for k in KW_GENERAL)
            is_circular = any(k in first_page_text for k in KW_CIRCULAR) or "circular" in fname.lower()
            
            classified_as = "support"
            
            # Scoring for Particular (priority)
            score = 0
            if is_particular: score += 10
            if "pliego" in fname.lower() and "particular" in fname.lower(): score += 5
            
            # Routing logic
            if is_particular:
                # If we have a better candidate, downgrade current to support
                if score > best_particular_score:
                    if c['particular']:
                         c['support'].append(c['particular'])
                    c['particular'] = (current_fname, searchable_content)
                    best_particular_score = score
                    classified_as = "PARTICULAR"
                else:
                    c['support'].append((current_fname, searchable_content))
            
            elif is_general:
                if c['general'] is None:
                    c['general'] = (current_fname, searchable_content)
                    classified_as = "GENERAL"
                else:
                    c['support'].append((current_fname, searchable_content))
            
            else:
                c['support'].append((current_fname, searchable_content))
                
            c['audit_log'].append(f"File {fname}: Classified as {classified_as} (Converted: {was_converted})")
            
        # Fallback: If no particular found, take the largest PDF from support?
        if c['particular'] is None and c['support']:
             # Heuristic: longest text usually
             # For now, just take the first one
             c['particular'] = c['support'][0]
             c['support'] = c['support'][1:]
             c['audit_log'].append("Fallback: Promoted first support file to PARTICULAR")
             
        return c
