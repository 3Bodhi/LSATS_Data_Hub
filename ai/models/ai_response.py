from dataclasses import dataclass
from typing import Any, Optional, Dict, List
import json
import ast
import re

@dataclass
class AIResponse:
    """Standardized AI response format."""
    raw_response: str
    parsed_data: Optional[Any] = None
    success: bool = True
    error_message: Optional[str] = None
    model_used: Optional[str] = None
    provider: Optional[str] = None
    token_usage: Optional[Dict[str, int]] = None

    def parse_as_list(self) -> List[Any]:
        """Safely parse response as Python list."""
        if self.parsed_data and isinstance(self.parsed_data, list):
            return self.parsed_data

        try:
            # Try literal_eval first (safer than eval)
            result = ast.literal_eval(self.raw_response.strip())
            if isinstance(result, list):
                self.parsed_data = result
                return result
        except (ValueError, SyntaxError):
            pass

        # Fallback: try to extract list from text using regex
        try:
            list_match = re.search(r'\[.*?\]', self.raw_response, re.DOTALL)
            if list_match:
                result = ast.literal_eval(list_match.group())
                if isinstance(result, list):
                    self.parsed_data = result
                    return result
        except (ValueError, SyntaxError):
            pass

        # Final fallback: try to extract quoted strings as list items
        try:
            quoted_items = re.findall(r'"([^"]*)"', self.raw_response)
            if quoted_items:
                self.parsed_data = quoted_items
                return quoted_items
        except:
            pass

        return []

    def parse_as_json(self) -> Dict[str, Any]:
        """Safely parse response as JSON."""
        if self.parsed_data and isinstance(self.parsed_data, dict):
            return self.parsed_data

        try:
            result = json.loads(self.raw_response.strip())
            self.parsed_data = result
            return result
        except json.JSONDecodeError:
            # Try to extract JSON from text
            try:
                json_match = re.search(r'\{.*\}', self.raw_response, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    self.parsed_data = result
                    return result
            except json.JSONDecodeError:
                pass
            return {}
