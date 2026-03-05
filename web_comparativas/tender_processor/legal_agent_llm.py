import logging
import re
from typing import Dict, Any, List, Optional
from .ollama_client import OllamaClient

logger = logging.getLogger("wc.llm_enricher")


class LLMEnricher:
    """
    Enriquecedor LLM focalizado.
    NO es un extractor completo. Solo se invoca para campos que el regex dejó vacíos.
    Envía prompts chicos (~2K chars) que procesan en 30-60 seg en CPU.
    """
    def __init__(self, ollama_url="http://localhost:11434", model="qwen2.5:7b"):
        self.client = OllamaClient(
            base_url=ollama_url,
            default_model=model,
            timeout=120  # Max 2 min
        )

    def enrich_missing(self, legal_data: Dict, missing_fields: List[str],
                       pages_text: List[str], filename: str) -> Dict:
        """
        Intenta extraer solo los campos faltantes usando LLM.
        Modifica legal_data in-place y lo retorna.
        """
        if not missing_fields or not pages_text:
            return legal_data

        # Construir un fragmento breve de texto (solo cabecera + primeras páginas)
        # Máximo 3000 chars para que sea rápido
        text_sample = "\n".join(pages_text[:3])
        if len(text_sample) > 3000:
            text_sample = text_sample[:3000]

        # Definir los campos a extraer
        fields_desc = {
            "numero_expediente": "Número de expediente o proceso (ej: PLIE-123-2024, EX-2023-456)",
            "presupuesto_oficial": "Monto del presupuesto oficial en pesos (solo el número)",
            "fecha_apertura": "Fecha de apertura (DD/MM/YYYY)",
            "objeto_contratacion": "Objeto de la contratación (descripción breve)",
            "email_contacto": "Email de contacto",
            "procedimiento_seleccion": "Tipo de procedimiento (Licitación Pública, Privada, etc)",
            "lugar_apertura": "Dirección del lugar de apertura",
        }

        # Solo pedir los campos que faltan
        requested = {k: v for k, v in fields_desc.items() if k in missing_fields}
        
        if not requested:
            return legal_data

        fields_list = "\n".join([f'  "{k}": "{v}"' for k, v in requested.items()])

        prompt = (
            f"Del siguiente fragmento de un pliego de licitación, extrae SOLO estos campos:\n"
            f"{{{fields_list}}}\n\n"
            f"TEXTO:\n{text_sample}\n\n"
            f"Responde SOLO con un JSON con las claves solicitadas. Si no encuentras un dato, usa null."
        )

        system = "Eres un extractor de datos. Responde SOLO con JSON válido, sin explicaciones."

        logger.info(f"LLM Enricher: pidiendo {len(requested)} campos con {len(text_sample)} chars de texto...")
        result = self.client.generate_json(prompt=prompt, system_prompt=system)

        if not result:
            logger.warning("LLM Enricher: no obtuvo respuesta válida.")
            return legal_data

        # Mapear de vuelta al formato de legal_data
        field_map = {
            "numero_expediente": "process_number",
            "presupuesto_oficial": "presupuesto_oficial",
            "fecha_apertura": "fecha_apertura",
            "objeto_contratacion": "objeto",
            "email_contacto": "contact_email",
            "procedimiento_seleccion": "selection_procedure",
            "lugar_apertura": "lugar_apertura",
        }

        enriched_count = 0
        for llm_key, native_key in field_map.items():
            if llm_key in requested and result.get(llm_key):
                val = result[llm_key]
                if val and str(val).lower() not in ["null", "none", ""]:
                    if not legal_data.get(native_key):
                        legal_data[native_key] = val
                        enriched_count += 1
                        logger.info(f"  LLM Enriched: {native_key} = {str(val)[:100]}")

        logger.info(f"LLM Enricher completó: {enriched_count} campos enriquecidos de {len(requested)} solicitados.")
        return legal_data
