import requests
import json
import logging
import re
from typing import Dict, Any, Optional

logger = logging.getLogger("wc.ollama_client")

class OllamaClient:
    """
    Cliente nativo para comunicarse con un servidor local de Ollama.
    Diseñado para forzar respuestas en formato JSON estructurado
    y manejar errores de manera robusta sin depender de servicios externos.
    """
    
    def __init__(self, base_url: str = "http://localhost:11434", default_model: str = "qwen2.5:7b", timeout: int = 120):
        self.base_url = base_url.rstrip('/')
        self.default_model = default_model
        self.timeout = timeout
        
    def check_health(self) -> bool:
        """Verifica si el servidor Ollama está corriendo y responde."""
        try:
            response = requests.get(f"{self.base_url}/", timeout=5)
            # Ollama root endpoint usually returns "Ollama is running"
            return response.status_code == 200
        except requests.RequestException as e:
            logger.warning(f"Ollama server no está disponible en {self.base_url}: {e}")
            return False
            
    def get_available_models(self) -> list:
        """Obtiene la lista de modelos descargados localmente."""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            if response.status_code == 200:
                data = response.json()
                return [model["name"] for model in data.get("models", [])]
        except requests.RequestException as e:
            logger.error(f"Error al obtener modelos de Ollama: {e}")
        return []

    def generate_json(self, prompt: str, system_prompt: str = "", model: str = None) -> Optional[Dict[str, Any]]:
        """
        Envía un prompt a Ollama forzando que la salida sea un JSON válido.
        Usa la capacidad nativa 'format: json' de la API de Ollama.
        Incluye retry automático si el primer intento falla.
        """
        target_model = model or self.default_model
        
        # Intentar hasta 2 veces con temperatura creciente
        temperatures = [0.1, 0.3]
        
        for attempt, temp in enumerate(temperatures):
            payload = {
                "model": target_model,
                "prompt": prompt,
                "system": system_prompt,
                "stream": False,
                "options": {
                    "temperature": temp,
                    "num_ctx": 8192  # Enricher sends small prompts; 8K is sufficient
                }
            }
            
            # OLLAMA FIX: DeepSeek-r1 is a reasoning model. If we force "format": "json", 
            # it gets stuck trying to output <think> before the JSON and returns empty/invalid strings.
            # Qwen2.5 SÍ soporta format:json correctamente con contexto amplio.
            if "deepseek" not in target_model.lower():
                payload["format"] = "json"
            
            logger.info(f"Ollama -> Solicitando extracción a modelo [{target_model}] (intento {attempt+1}, temp={temp})")
            
            try:
                response = requests.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=self.timeout
                )
                
                if response.status_code != 200:
                    logger.error(f"Ollama API error: {response.status_code} - {response.text}")
                    continue  # Reintentar
                    
                result_data = response.json()
                raw_text = result_data.get("response", "")
                
                parsed = self._parse_json_safely(raw_text)
                if parsed:
                    # Verificar que no sea un JSON vacío o con todos los valores null
                    non_null = sum(1 for v in parsed.values() if v is not None and v != "" and v != [] and v != {})
                    if non_null >= 1:  # Al menos 1 campo con dato real (enricher pide pocos)
                        return parsed
                    logger.warning(f"Ollama devolvió JSON con solo {non_null} campos no-null. Reintentando...")
                    continue
                    
                logger.warning(f"Ollama no devolvió JSON parseable en intento {attempt+1}")
                
            except requests.exceptions.Timeout:
                logger.error(f"Ollama timeout ({self.timeout}s) en intento {attempt+1}")
                continue
            except requests.RequestException as e:
                logger.error(f"Ollama connection error en intento {attempt+1}: {e}")
                continue
        
        logger.error("Ollama: Todos los intentos fallaron.")
        return None
            
    def _parse_json_safely(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Intenta parsear el string devuelto como JSON.
        A veces los modelos DeepSeek agregan etiquetas <think> antes del JSON,
        por lo que hacemos una limpieza agresiva.
        """
        if not text:
            return None
            
        # Extract everything between the first { and the last }
        text = text.strip()
        
        # Strip potential deepseek thought blocks <think>...</think>
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
        
        # Find JSON boundaries
        start_idx = text.find('{')
        end_idx = text.rfind('}')
        
        if start_idx == -1 or end_idx == -1:
            logger.error("No JSON structure found in Ollama output")
            logger.debug(f"Raw output: {text}")
            return None
            
        json_str = text[start_idx:end_idx+1]
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode failed on Ollama output: {e}")
            logger.debug(f"Attempted to parse: {json_str}")
            return None
