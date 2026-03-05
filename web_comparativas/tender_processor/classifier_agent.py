from typing import Dict, List, Any
from .pdf_utils import extract_text_first_page_robust

class ClassifierAgent:
    """
    AGENTE 1: CLASIFICADOR (The Router)
    Objetivo: Identificar cuál archivo tiene los datos duros.
    """
    
    def classify_documents(self, files_map: Dict[str, bytes]) -> Dict[str, Any]:
        """
        Input: Dict {filename: bytes}
        Output: { 
           'PRIORITARIO': (filename, bytes) or None,
           'SOPORTE': (filename, bytes) or None,
           'OTHERS': [] 
        }
        """
        result = {
            "PRIORITARIO": None,
            "SOPORTE": None,
            "OTHERS": []
        }
        
        # Keywords
        KW_PRIORITY = ["particular", "anexo", "pliego de bases y condiciones particulares", "pbcp"]
        KW_SUPPORT = ["general", "pliego de bases y condiciones generales", "pbcg"]
        
        for fname, content in files_map.items():
            text_p1 = extract_text_first_page_robust(content).lower()
            
            # 1. Check Priority (Winner takes all)
            if any(k in text_p1 for k in KW_PRIORITY):
                if result["PRIORITARIO"] is None:
                    result["PRIORITARIO"] = (fname, content)
                    continue
                # If we already have one, maybe this is 'better'? 
                # stick to first found for simplicity or append to others
            
            # 2. Check Support
            if any(k in text_p1 for k in KW_SUPPORT):
                if result["SOPORTE"] is None:
                    result["SOPORTE"] = (fname, content)
                    continue
            
            result["OTHERS"].append((fname, content))
            
        # Fallback: If no priority found, but we have others, take the largest or first one?
        if result["PRIORITARIO"] is None and result["OTHERS"]:
            # Let's assume the first PDF is the one if classification failed
             result["PRIORITARIO"] = result["OTHERS"][0]
             
        return result
