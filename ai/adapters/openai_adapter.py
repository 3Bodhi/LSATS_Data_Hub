from openai import OpenAI
from typing import Dict, List, Any
from .base_ai_adapter import BaseAIAdapter
from ..models.ai_response import AIResponse

class OpenAIAdapter(BaseAIAdapter):
    """OpenAI API adapter (works with OpenAI, local OpenAI-compatible servers, etc.)."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client = OpenAI(
            api_key=config.get('api_key', 'not-needed-for-local'),
            base_url=config.get('base_url')  # Can be local OpenAI-compatible server
        )
        self.default_model = config.get('model', 'gpt-3.5-turbo')

    def generate_completion(self, prompt: str, **kwargs) -> AIResponse:
        """Generate completion using OpenAI API."""
        model = kwargs.get('model', self.default_model)

        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=kwargs.get('temperature', 0.7),
                max_tokens=kwargs.get('max_tokens', 1000)
            )

            raw_text = response.choices[0].message.content

            token_usage = None
            if hasattr(response, 'usage') and response.usage:
                token_usage = {
                    'prompt_tokens': response.usage.prompt_tokens,
                    'completion_tokens': response.usage.completion_tokens,
                    'total_tokens': response.usage.total_tokens
                }

            return AIResponse(
                raw_response=raw_text,
                success=True,
                model_used=model,
                provider='openai',
                token_usage=token_usage
            )

        except Exception as e:
            return AIResponse(
                raw_response="",
                success=False,
                error_message=f"OpenAI request failed: {str(e)}",
                provider='openai'
            )

    def generate_structured_response(self, prompt: str, format_type: str = "json", **kwargs) -> AIResponse:
        """Generate structured response."""
        format_instructions = {
            "list": "Return your response as a valid Python list enclosed in square brackets. Include nothing else. Example: [\"item1\", \"item2\"]",
            "json": "Return your response as valid JSON. Include nothing else.",
            "python_list": "Return only a Python list with no additional text or explanation. Example: [\"item1\", \"item2\"]"
        }

        instruction = format_instructions.get(format_type, "")
        enhanced_prompt = f"{prompt}\n\n{instruction}" if instruction else prompt

        response = self.generate_completion(enhanced_prompt, **kwargs)

        if response.success:
            if format_type in ["list", "python_list"]:
                response.parse_as_list()
            elif format_type == "json":
                response.parse_as_json()

        return response

    def chat_completion(self, messages: List[Dict[str, str]], **kwargs) -> AIResponse:
        """Generate chat completion using OpenAI API."""
        model = kwargs.get('model', self.default_model)

        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=kwargs.get('temperature', 0.7),
                max_tokens=kwargs.get('max_tokens', 1000)
            )

            raw_text = response.choices[0].message.content

            token_usage = None
            if hasattr(response, 'usage') and response.usage:
                token_usage = {
                    'prompt_tokens': response.usage.prompt_tokens,
                    'completion_tokens': response.usage.completion_tokens,
                    'total_tokens': response.usage.total_tokens
                }

            return AIResponse(
                raw_response=raw_text,
                success=True,
                model_used=model,
                provider='openai',
                token_usage=token_usage
            )

        except Exception as e:
            return AIResponse(
                raw_response="",
                success=False,
                error_message=f"OpenAI chat request failed: {str(e)}",
                provider='openai'
            )

    def is_available(self) -> bool:
        """Check if the API is available."""
        try:
            # Simple test call - try to list models
            self.client.models.list()
            return True
        except:
            return False

    def get_available_models(self) -> List[str]:
        """Get available models."""
        try:
            models = self.client.models.list()
            return [model.id for model in models.data]
        except:
            return []
