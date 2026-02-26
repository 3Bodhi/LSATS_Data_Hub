import requests
from typing import Dict, List, Any
from .base_ai_adapter import BaseAIAdapter
from ..models.ai_response import AIResponse

class OllamaAdapter(BaseAIAdapter):
    """Ollama local AI adapter."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('base_url', 'http://localhost:11434')
        self.default_model = config.get('model', 'gemma3:27b')
        self.timeout = config.get('timeout', 30)

    def generate_completion(self, prompt: str, **kwargs) -> AIResponse:
        """Generate completion using Ollama."""
        model = kwargs.get('model', self.default_model)

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            **kwargs.get('extra_params', {})
        }

        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            response_data = response.json()
            raw_text = response_data.get('response', '').strip()

            return AIResponse(
                raw_response=raw_text,
                success=True,
                model_used=model,
                provider='ollama'
            )

        except requests.exceptions.RequestException as e:
            return AIResponse(
                raw_response="",
                success=False,
                error_message=f"Ollama request failed: {str(e)}",
                provider='ollama'
            )
        except Exception as e:
            return AIResponse(
                raw_response="",
                success=False,
                error_message=f"Unexpected error: {str(e)}",
                provider='ollama'
            )

    def generate_structured_response(self, prompt: str, format_type: str = "json", **kwargs) -> AIResponse:
        """Generate structured response with better parsing."""

        # Add format instructions to prompt
        format_instructions = {
            "list": "Return your response as a valid Python list. Do not include any text outside the list.",
            "json": "Return your response as valid JSON. Do not include any text outside the JSON.",
            "python_list": "Return only a Python list with no additional text or explanation. Example: [\"item1\", \"item2\"]"
        }

        instruction = format_instructions.get(format_type, "")
        enhanced_prompt = f"{prompt}\n\n{instruction}" if instruction else prompt

        response = self.generate_completion(enhanced_prompt, **kwargs)

        if response.success:
            # Parse based on format type
            if format_type in ["list", "python_list"]:
                response.parse_as_list()
            elif format_type == "json":
                response.parse_as_json()

        return response

    def is_available(self) -> bool:
        """Check if Ollama is running."""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False

    def get_available_models(self) -> List[str]:
        """Get available Ollama models."""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return [model['name'] for model in data.get('models', [])]
        except:
            pass
        return []

    def chat_completion(self, messages: List[Dict[str, str]], **kwargs) -> AIResponse:
        """Generate chat completion using Ollama's chat endpoint."""
        model = kwargs.get('model', self.default_model)

        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            **kwargs.get('extra_params', {})
        }

        try:
            response = requests.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            response_data = response.json()
            raw_text = response_data.get('message', {}).get('content', '').strip()

            return AIResponse(
                raw_response=raw_text,
                success=True,
                model_used=model,
                provider='ollama'
            )

        except requests.exceptions.RequestException as e:
            return AIResponse(
                raw_response="",
                success=False,
                error_message=f"Ollama chat request failed: {str(e)}",
                provider='ollama'
            )
        except Exception as e:
            return AIResponse(
                raw_response="",
                success=False,
                error_message=f"Unexpected error: {str(e)}",
                provider='ollama'
            )
