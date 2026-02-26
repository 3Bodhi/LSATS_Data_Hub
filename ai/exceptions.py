class AIServiceError(Exception):
    """Base exception for AI service errors."""
    pass

class AIProviderNotAvailableError(AIServiceError):
    """Raised when the AI provider is not available."""
    pass

class AIModelNotFoundError(AIServiceError):
    """Raised when the specified model is not found."""
    pass

class AIResponseParsingError(AIServiceError):
    """Raised when response parsing fails."""
    pass
