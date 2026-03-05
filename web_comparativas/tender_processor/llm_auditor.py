import logging
import re
import time
from typing import Dict, Any, List
from .ollama_client import OllamaClient

logger = logging.getLogger("wc.llm_auditor")

# Keyword groups for instant page filtering (no embeddings needed)
TOPIC_KEYWORDS = {
    "penalties": [
        "penalidad", "multa", "mora", "rescisión", "rescision", "resolución",
        "incumplimiento", "sanción", "sancion", "retención", "retencion",
        "caducidad", "suspensión", "suspension", "inhabilitación"
    ],
    "delivery": [
        "entrega", "plazo", "recepción", "recepcion", "provisión", "provision",
        "lugar de entrega", "depósito", "deposito", "muestra", "remito",
        "días hábiles", "dias habiles", "calendario", "cronograma"
    ],
    "payment": [
        "pago", "factura", "facturación", "facturacion", "moneda", "anticipo",
        "sellado", "retención", "retencion", "fondo de reparo", "certificado",
        "plazo de pago", "conformación", "conformacion", "tesorería"
    ],
    "guarantees": [
        "garantía", "garantia", "mantenimiento de oferta", "cumplimiento",
        "póliza", "poliza", "caución", "caucion", "adjudicación", "seguro",
        "aval bancario", "fianza"
    ]
}


def _keyword_search(pages_text: List[str], keywords: List[str], max_chars: int = 2000) -> str:
    """
    Pure Python keyword search. Instant execution (0ms).
    Finds the most relevant paragraphs from all pages matching the given keywords.
    """
    scored_paragraphs = []

    for page_idx, page in enumerate(pages_text):
        page_lower = page.lower()
        # Split page into paragraphs (by double newline or single newline with blank)
        paragraphs = re.split(r'\n\s*\n|\n(?=[A-ZÁÉÍÓÚ])', page)

        for para in paragraphs:
            para_stripped = para.strip()
            if len(para_stripped) < 30:
                continue  # Skip tiny fragments

            para_lower = para_stripped.lower()
            hit_count = sum(1 for kw in keywords if kw in para_lower)

            if hit_count > 0:
                scored_paragraphs.append({
                    "text": para_stripped,
                    "page": page_idx + 1,
                    "score": hit_count
                })

    # Sort by relevance (hit count) descending
    scored_paragraphs.sort(key=lambda x: x["score"], reverse=True)

    # Build result string respecting max_chars budget
    result_parts = []
    total_chars = 0
    for sp in scored_paragraphs:
        entry = f"[Pág.{sp['page']}]: {sp['text']}"
        if total_chars + len(entry) > max_chars:
            break
        result_parts.append(entry)
        total_chars += len(entry)

    return "\n\n---\n\n".join(result_parts)


class LLMAuditor:
    """
    Agente Auditor + Enriquecedor de Licitaciones (LLM).
    Usa búsqueda por palabras clave (instantánea) para encontrar
    los párrafos relevantes del pliego y luego envía solo esos
    fragmentos al LLM para análisis profundo y enriquecimiento.
    """
    def __init__(self, ollama_url="http://localhost:11434", model="qwen2.5:7b"):
        self.client = OllamaClient(
            base_url=ollama_url,
            default_model=model,
            timeout=300  # qwen2.5:7b needs 120-240s on this hardware for JSON generation
        )

    def audit_tender(self, extracted_data: Dict[str, Any], pages_text: List[str]) -> Dict[str, Any]:
        """
        Enriquece y audita los datos extraídos usando búsqueda por keywords + LLM.
        No bloquea el pipeline si falla.
        """
        if not self.client.check_health() or not pages_text:
            return {}

        t0 = time.time()

        # 1. Prepare context from deterministic extraction
        context_json = {
            "objeto": extracted_data.get("basic_info", {}).get("object", {}).get("value"),
            "presupuesto": extracted_data.get("summary", {}).get("estimated_total_amount", {}).get("value", {}).get("amount"),
            "garantias_exigidas": [g.get("tipo", {}).get("value") for g in extracted_data.get("guarantees", {}).get("garantias_list", [])],
            "plazo_entrega": extracted_data.get("delivery", {}).get("plazo", {}).get("value"),
            "condiciones_pago": extracted_data.get("payment", {}).get("general", {}).get("value"),
            "penalidades": extracted_data.get("penalties", {}).get("multas", {}).get("value"),
            "cantidad_renglones": len(extracted_data.get("items", [])),
        }

        # 2. Keyword-based paragraph extraction (INSTANT — 0ms, pure Python)
        all_keywords = []
        for kw_list in TOPIC_KEYWORDS.values():
            all_keywords.extend(kw_list)

        text_sample = _keyword_search(pages_text, all_keywords, max_chars=3000)

        if not text_sample:
            # Fallback: just use first 2 pages if no keywords matched
            text_sample = "\n".join(pages_text[:2])[:3000]

        search_time = time.time() - t0
        logger.info(f"LLM Auditor: Keyword search completed in {search_time:.3f}s, found {len(text_sample)} chars of evidence.")

        # 3. Build prompt (COMPACT — optimized for speed)
        prompt = (
            f"Analiza esta licitación y devuelve SOLO JSON válido.\n\n"
            f"DATOS BASE:\n{context_json}\n\n"
            f"TEXTO:\n{text_sample}\n\n"
            f"Responde con este JSON exacto:\n"
            f"{{\n"
            f'  "ia_insights": ["inconsistencia o dato relevante"],\n'
            f'  "ia_hidden_risks": [{{"descripción del riesgo": "evidencia textual"}}],\n'
            f'  "ia_executive_summary": "Resumen de 2 líneas riesgo/beneficio.",\n'
            f'  "ia_enriched_data": {{\n'
            f'    "delivery": "Detalle preciso del lugar, plazo y modalidad de entrega.",\n'
            f'    "payment": "Detalle de moneda, plazos y condiciones de pago.",\n'
            f'    "penalties": "Detalle de multas por mora y causales de rescisión.",\n'
            f'    "guarantees": "Detalle de garantías exigidas."\n'
            f'  }}\n'
            f"}}\n"
            f"Solo JSON. Sin markdown. No inventar datos ausentes."
        )

        system = "Auditor IA de licitaciones. Solo JSON."

        logger.info(f"LLM Auditor: Enviando {len(text_sample)} chars al LLM para análisis...")
        result = self.client.generate_json(prompt=prompt, system_prompt=system)

        total_time = time.time() - t0
        logger.info(f"LLM Auditor: Proceso total completado en {total_time:.1f}s")

        if not result or "ia_executive_summary" not in result:
            logger.warning("LLM Auditor: No se obtuvieron insights válidos.")
            return {}

        logger.info(f"LLM Auditor: Análisis completado con éxito. Detectó {len(result.get('ia_hidden_risks', []))} riesgos ocultos.")
        return result
