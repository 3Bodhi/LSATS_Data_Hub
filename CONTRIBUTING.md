# Contributing to LSATS Data Hub

Thank you for considering contributing to LSATS Data Hub! This document provides guidelines and instructions for contributing to this project.

## Code of Conduct

Please be respectful and considerate of others when contributing to this project. All contributors are expected to follow standard professional conduct guidelines.

## How to Contribute

### Reporting Bugs

If you find a bug, please create an issue on the GitHub repository with the following information:

1. Clear and descriptive title
2. Steps to reproduce the bug
3. Expected behavior
4. Actual behavior
5. Environment information (Python version, operating system, etc.)
6. Any additional context

### Suggesting Enhancements

For feature requests or enhancements, create an issue with:

1. Clear description of the feature or enhancement
2. Rationale for why it should be added
3. Any implementation details you might have in mind

### Pull Requests

1. Fork the repository
2. Create a new branch for your changes
3. Make your changes
4. Run tests (if applicable)
5. Submit a pull request

### Development Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install the package in development mode:
   ```bash
   pip install -e .[all]
   ```

## Adding New Data Sources

When adding a new data source to LSATS Data Hub, follow this structure:

1. Create a new directory for the data source (e.g., `new_source/`)
2. Create the following files and directories:
   - `new_source/__init__.py` - Export the facade
   - `new_source/api/__init__.py` - Export all API classes
   - `new_source/api/new_source_api.py` - Base API class
   - `new_source/facade/__init__.py` - Export the facade
   - `new_source/facade/new_source_facade.py` - Facade implementation

3. Update the root `__init__.py` to include the new data source

For more detailed architecture guidance, see the README.md file.

## Style Guide

- Follow PEP 8 for Python code
- Use meaningful variable and function names
- Keep methods small and focused on a single responsibility

### Type Hints and Documentation

**Type hints are required for all functions and methods.** This is crucial for ensuring forward compatibility with agentic implementations (smolagents library and MCP server implementation).

All functions and methods must include docstrings with the following format:

```python
def add(a: int, b: int) -> int:
    """
    Add two numbers.

    Args:
        a (int): The first number
        b (int): The second number

    Returns:
        int: The sum of the two numbers
    """
    return a + b
```

For methods with no return value, use:

```python
def process_data(data: List[Dict[str, Any]]) -> None:
    """
    Process the input data.

    Args:
        data (List[Dict[str, Any]]): The data to process

    Returns:
        None
    """
    # Implementation here
```

For classes, document attributes in the class docstring:

```python
class DataProcessor:
    """
    Processes data from various sources.

    Attributes:
        sources (List[str]): List of data source identifiers
        cache_enabled (bool): Whether caching is enabled
    """
```

## Testing

We use the standard Python `unittest` framework for testing. Please include tests for new features when possible.

To run the tests:

```bash
python -m unittest discover tests
```

## Documentation

Please document any new features or changes to existing features in the appropriate places:

- README.md for high-level overview
- Docstrings for API documentation
- Example scripts for usage examples

## Questions?

If you have any questions about contributing, please feel free to create an issue asking for clarification.
