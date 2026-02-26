from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from ..models.ai_response import AIResponse

class BaseAIAdapter(ABC):
    """Abstract base class for AI service adapters."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def generate_completion(self, prompt: str, **kwargs) -> AIResponse:
        """Generate a completion from the AI service."""
        pass

    @abstractmethod
    def generate_structured_response(self, prompt: str, format_type: str = "json", **kwargs) -> AIResponse:
        """Generate a structured response (list, json, etc.)."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the AI service is available."""
        pass

    @abstractmethod
    def get_available_models(self) -> List[str]:
        """Get list of available models."""
        pass
