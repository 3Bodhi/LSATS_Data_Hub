import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class AIConfig:
    """Centralized AI configuration management."""

    @staticmethod
    def get_config() -> Dict[str, Any]:
        """Get AI configuration from environment variables."""
        provider = os.getenv('AI_PROVIDER', 'ollama').lower()

        base_config = {
            'provider': provider,
            'model': os.getenv('AI_MODEL', 'gemma3:27b'),
            'timeout': int(os.getenv('AI_TIMEOUT', '30')),
        }

        if provider == 'ollama':
            base_config.update({
                'base_url': os.getenv('AI_BASE_URL', 'http://localhost:11434'),
            })
        elif provider == 'openai':
            base_config.update({
                'api_key': os.getenv('AI_API_KEY'),
                'base_url': os.getenv('AI_BASE_URL'),  # Optional for custom endpoints
                'model': os.getenv('AI_MODEL', 'gpt-3.5-turbo'),
            })
        elif provider == 'anthropic':
            base_config.update({
                'api_key': os.getenv('AI_API_KEY'),
                'model': os.getenv('AI_MODEL', 'claude-3-sonnet-20240229'),
            })

        return base_config

    @staticmethod
    def get_provider_configs() -> Dict[str, Dict[str, Any]]:
        """Get example configurations for all providers."""
        return {
            'ollama': {
                'AI_PROVIDER': 'ollama',
                'AI_MODEL': 'gemma3:27b',
                'AI_BASE_URL': 'http://localhost:11434',
                'AI_TIMEOUT': '30'
            },
            'openai': {
                'AI_PROVIDER': 'openai',
                'AI_MODEL': 'gpt-3.5-turbo',
                'AI_API_KEY': 'your_openai_key_here',
                'AI_TIMEOUT': '30'
            },
            'openai_local': {
                'AI_PROVIDER': 'openai',
                'AI_MODEL': 'gemma3:27b',
                'AI_BASE_URL': 'http://localhost:11434/v1',
                'AI_API_KEY': 'not-needed',
                'AI_TIMEOUT': '30'
            },
            'anthropic': {
                'AI_PROVIDER': 'anthropic',
                'AI_MODEL': 'claude-3-sonnet-20240229',
                'AI_API_KEY': 'your_anthropic_key_here',
                'AI_TIMEOUT': '30'
            }
        }
