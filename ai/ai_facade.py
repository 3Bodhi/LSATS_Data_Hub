from typing import List, Optional, Dict, Any
from .config import AIConfig
from .adapters.ollama_adapter import OllamaAdapter
from .adapters.openai_adapter import OpenAIAdapter
from .models.ai_response import AIResponse

class AIFacade:
    """High-level AI service facade providing generic AI capabilities."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize AI facade with configuration.

        Args:
            config: Optional configuration dict. If None, loads from environment.
        """
        self.config = config or AIConfig.get_config()
        self.adapter = self._create_adapter()

    def _create_adapter(self):
        """Create appropriate adapter based on configuration."""
        provider = self.config['provider']

        if provider == 'ollama':
            return OllamaAdapter(self.config)
        elif provider == 'openai':
            return OpenAIAdapter(self.config)
        # elif provider == 'anthropic':
        #     return AnthropicAdapter(self.config)
        else:
            raise ValueError(f"Unsupported AI provider: {provider}")

    def generate_completion(self, prompt: str, **kwargs) -> AIResponse:
        """
        Generate basic text completion.

        Args:
            prompt: The input prompt
            **kwargs: Additional parameters (model, temperature, etc.)

        Returns:
            AIResponse with the completion
        """
        return self.adapter.generate_completion(prompt, **kwargs)

    def generate_structured_response(self, prompt: str, format_type: str = "json", **kwargs) -> AIResponse:
        """
        Generate structured response (list, json, etc.).

        Args:
            prompt: The input prompt
            format_type: "json", "list", or "python_list"
            **kwargs: Additional parameters

        Returns:
            AIResponse with parsed structured data
        """
        return self.adapter.generate_structured_response(prompt, format_type, **kwargs)

    def generate_chat_completion(self, messages: List[Dict[str, str]], **kwargs) -> AIResponse:
        """
        Generate chat-style completion for conversation interfaces.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            **kwargs: Additional parameters

        Returns:
            AIResponse with the chat completion
        """
        if hasattr(self.adapter, 'chat_completion'):
            return self.adapter.chat_completion(messages, **kwargs)
        else:
            # Fallback: convert chat to single prompt
            prompt = "\n".join([f"{msg['role']}: {msg['content']}" for msg in messages])
            return self.generate_completion(prompt, **kwargs)

    # Utility methods
    def is_service_available(self) -> bool:
        """Check if the AI service is available."""
        return self.adapter.is_available()

    def get_current_provider(self) -> str:
        """Get the currently configured provider."""
        return self.config['provider']

    def get_current_model(self) -> str:
        """Get the currently configured model."""
        return self.config['model']

    def get_available_models(self) -> List[str]:
        """Get available models for current provider."""
        return self.adapter.get_available_models()

    def get_config_info(self) -> Dict[str, Any]:
        """Get current configuration information."""
        return {
            'provider': self.get_current_provider(),
            'model': self.get_current_model(),
            'available': self.is_service_available(),
            'available_models': self.get_available_models()
        }

    @staticmethod
    def get_example_configs() -> Dict[str, Dict[str, Any]]:
        """Get example configurations for all supported providers."""
        return AIConfig.get_provider_configs()
