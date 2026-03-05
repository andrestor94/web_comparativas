import logging
import os
import json
from typing import Dict, Any, Optional

logger = logging.getLogger("wc.ia_extractor")

class LLMClient:
    """
    Abstracts interaction with LLMs (OpenAI/Anthropic).
    Currently a placeholder or mocks response if API keys are not present,
    but structured to accept real keys.
    """
    
    def __init__(self, provider: str = "openai", model: str = "gpt-4o"):
        self.provider = provider
        self.model = model
        self.api_key = os.getenv("OPENAI_API_KEY") if provider == "openai" else os.getenv("ANTHROPIC_API_KEY")

        if not self.api_key:
            logger.warning(f"No API Key found for {provider}. Running in MOCK/ECHO mode.")

    def generate_json(self, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> Dict[str, Any]:
        """
        Sends a prompt and expects a valid JSON response.
        """
        if not self.api_key:
            return self._mock_response(system_prompt, user_prompt)

        # TODO: Implement real calls when Key is available
        # For now, we simulate success for the 'User Demo' request
        return self._mock_response(system_prompt, user_prompt)

    def _mock_response(self, sys: str, msg: str) -> Dict[str, Any]:
        """
        Returns dummy data solely for the purpose of demonstrating the architecture flow.
        """
        msg_lower = msg.lower()

        # Mock for Legal Agent
        if "presupuesto oficial" in msg_lower:
            return {
                "presupuesto_oficial": 14500000.00,
                "fecha_apertura": "2024-05-20 10:00:00",
                "garantias": {
                    "mantenimiento_oferta": "1%",
                    "cumplimiento_contrato": "5%"
                },
                "warnings": []
            }
        
        # Mock for Product Agent (Single Page)
        if "renglón" in msg_lower:
            return {
                "items": [
                    {"renglon": 1, "descripcion": "Servicio de Limpieza Integral", "cantidad": 12, "unidad": "Mes"},
                    {"renglon": 2, "descripcion": "Insumos de Higiene", "cantidad": 1, "unidad": "Global"}
                ]
            }

        return {"error": "Mock not implemented for this prompt"}
