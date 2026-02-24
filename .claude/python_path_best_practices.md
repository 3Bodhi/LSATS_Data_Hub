# Python Path Best Practices

A comprehensive guide to managing file paths in Python scripts for robust, portable code.

## Core Principles

1. **Use `pathlib` over `os.path`** - Modern, object-oriented, platform-independent
2. **Anchor to `__file__`** - Make paths relative to script location, not working directory
3. **Keep paths as Path objects** - Convert to strings only when necessary
4. **Document navigation levels** - Make parent directory traversal clear and maintainable

## Essential Patterns

### 1. Basic Script-Relative Paths

Always base paths on the script's location using `__file__`:

```python
from pathlib import Path

# Get the directory containing the current script
SCRIPT_DIR = Path(__file__).parent

# Access files relative to script
config_file = SCRIPT_DIR / 'config.json'
data_dir = SCRIPT_DIR / 'data'
```

### 2. Project Root Pattern

For projects with deep directory structures, calculate project root once:

```python
from pathlib import Path

# Method 1: Explicit parent navigation (clear and documented)
SCRIPT_DIR = Path(__file__).parent
# Script location: src/services/database/scripts/ingest.py
# Project root is 4 levels up: project/
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent.parent

# Method 2: Using parents[] (more concise)
PROJECT_ROOT = Path(__file__).resolve().parents[4]  # 4 levels up

# Method 3: Search for marker file (most robust)
def find_project_root(marker_files=('.git', 'setup.py', 'pyproject.toml')):
    """Find project root by searching for marker files."""
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if any((parent / marker).exists() for marker in marker_files):
            return parent
    raise RuntimeError("Could not find project root")

PROJECT_ROOT = find_project_root()

# Define all project paths from root
DATA_DIR = PROJECT_ROOT / 'data'
CONFIG_DIR = PROJECT_ROOT / 'config'
LOGS_DIR = PROJECT_ROOT / 'logs'
CACHE_DIR = PROJECT_ROOT / '.cache'
```

### 3. Configurable Paths with Defaults

Allow environment variable overrides while providing sensible defaults:

```python
import os
from pathlib import Path

# Default to script-relative, allow override via environment
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = PROJECT_ROOT / 'data'
DATA_DIR = Path(os.getenv('DATA_DIR', DEFAULT_DATA_DIR))

# With validation
if not DATA_DIR.exists():
    raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")
```

### 4. Cross-Platform Path Construction

Use `/` operator for readability and platform independence:

```python
# Good ✓ - Works on Windows, macOS, Linux
config_path = PROJECT_ROOT / 'config' / 'settings.json'
log_file = LOGS_DIR / 'app.log'

# Avoid ✗ - Platform-specific separators
config_path = PROJECT_ROOT + '\\config\\settings.json'  # Windows only
```

### 5. Path Resolution and Normalization

Use `.resolve()` to get absolute, normalized paths:

```python
# Resolve symlinks and convert to absolute path
absolute_path = (PROJECT_ROOT / 'data' / 'file.csv').resolve()

# Normalize paths with .. references
normalized = (SCRIPT_DIR / '../config/settings.json').resolve()
```

## Common Patterns by Use Case

### Accessing Data Files

```python
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / 'data'

def load_data_file(filename: str):
    """Load data file with proper path resolution."""
    file_path = DATA_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    with open(file_path, 'r') as f:
        return f.read()
```

### Creating Output Directories

```python
from pathlib import Path

OUTPUT_DIR = PROJECT_ROOT / 'output' / 'reports'

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR

# Usage
output_dir = ensure_output_dir()
report_file = output_dir / f'report_{datetime.now():%Y%m%d}.csv'
```

### Finding Files with Patterns

```python
from pathlib import Path
import glob

DATA_DIR = PROJECT_ROOT / 'data'

# Method 1: Using pathlib.glob()
def find_excel_files():
    """Find all Excel files in data directory."""
    return list(DATA_DIR.glob('*.xlsx'))

# Method 2: Using glob module with Path
def find_latest_report():
    """Find newest report file."""
    pattern = str(DATA_DIR / 'report_*.csv')
    files = glob.glob(pattern)

    if not files:
        return None

    # Return Path object, not string
    return Path(max(files, key=os.path.getmtime))
```

### Logging with Path-Based Log Files

```python
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[3]
LOG_DIR = PROJECT_ROOT / 'logs'

# Detect layer from script path for organized logging
script_path = Path(__file__)
if any(parent.name == 'bronze' for parent in script_path.parents):
    log_subdir = LOG_DIR / 'bronze'
elif any(parent.name == 'silver' for parent in script_path.parents):
    log_subdir = LOG_DIR / 'silver'
else:
    log_subdir = LOG_DIR

# Create log directory
log_subdir.mkdir(parents=True, exist_ok=True)

# Configure logging
log_file = log_subdir / f'{script_path.stem}.log'
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
```

## Validation and Error Handling

### Check Path Existence

```python
from pathlib import Path

data_file = DATA_DIR / 'input.csv'

# Method 1: Check before operation
if data_file.exists():
    process_file(data_file)
else:
    logger.error(f"File not found: {data_file}")
    raise FileNotFoundError(f"Required file missing: {data_file}")

# Method 2: Try/except pattern
try:
    with open(data_file) as f:
        data = f.read()
except FileNotFoundError:
    logger.error(f"File not found: {data_file}")
    raise
```

### Validate Path Type

```python
from pathlib import Path

def validate_directory(path: Path) -> Path:
    """Validate that path exists and is a directory."""
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Not a directory: {path}")

    return path.resolve()

def validate_file(path: Path, extensions: list = None) -> Path:
    """Validate that path exists, is a file, and has valid extension."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    if not path.is_file():
        raise IsADirectoryError(f"Not a file: {path}")

    if extensions and path.suffix not in extensions:
        raise ValueError(f"Invalid file type: {path.suffix}. Expected: {extensions}")

    return path.resolve()
```

## Working with sys.path

### Adding Project to Python Path

```python
import sys
from pathlib import Path

# Add project root to Python path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

# Now you can do absolute imports from project root
from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.api.ticket_api import TicketAPI
```

## String Conversion Best Practices

### When to Convert to Strings

Keep paths as `Path` objects throughout your code, convert to strings only when required:

```python
from pathlib import Path

# Good ✓ - Keep as Path objects
config_dir = PROJECT_ROOT / 'config'
settings_file = config_dir / 'settings.json'

# Work with Path objects
if settings_file.exists():
    with open(settings_file) as f:  # open() accepts Path objects
        config = json.load(f)

# Convert to string only for APIs that require it
connection_string = f"sqlite:///{str(DB_FILE)}"
db_adapter = DatabaseAdapter(connection_url=str(DB_FILE))

# Or use os.fspath() for official conversion
import os
legacy_api_call(os.fspath(settings_file))
```

## Common Anti-Patterns to Avoid

### ❌ DON'T: Hardcode Absolute Paths

```python
# Bad - Breaks on other machines/environments
DATA_DIR = '/Users/username/projects/myapp/data'
```

### ❌ DON'T: Use Relative Paths Without Anchoring

```python
# Bad - Depends on where script is executed from
config_file = 'config/settings.json'
```

### ❌ DON'T: Mix os.path and pathlib

```python
# Bad - Inconsistent, harder to read
from pathlib import Path
import os

base = Path(__file__).parent
config = os.path.join(base, 'config', 'settings.json')  # Mixed APIs
```

### ❌ DON'T: Use String Concatenation for Paths

```python
# Bad - Platform-specific, error-prone
config_path = base_dir + '/' + 'config' + '/' + 'settings.json'
```

### ❌ DON'T: Ignore Platform Differences

```python
# Bad - Windows-specific
log_file = logs_dir + '\\app.log'
```

## Advanced Patterns

### Finding Files Recursively

```python
from pathlib import Path

def find_all_python_files(root: Path):
    """Recursively find all Python files."""
    return list(root.rglob('*.py'))

def find_test_files(root: Path):
    """Find all test files matching pattern."""
    return list(root.rglob('test_*.py')) + list(root.rglob('*_test.py'))
```

### Temporary File Locations

```python
from pathlib import Path
import tempfile

# System temp directory
temp_dir = Path(tempfile.gettempdir())

# Project-specific temp directory
PROJECT_TEMP = PROJECT_ROOT / '.tmp'
PROJECT_TEMP.mkdir(exist_ok=True)

# Create temporary file in project temp
temp_file = PROJECT_TEMP / f'temp_{uuid.uuid4()}.json'
```

### Relative Path from One Path to Another

```python
from pathlib import Path

source = Path('/project/src/services/api.py')
target = Path('/project/config/settings.json')

# Get relative path from source to target
relative = target.relative_to(source.parent.parent)
# Result: ../config/settings.json
```

### Path Iteration and Filtering

```python
from pathlib import Path

def get_recent_logs(log_dir: Path, days: int = 7):
    """Get log files modified in last N days."""
    from datetime import datetime, timedelta

    cutoff = datetime.now() - timedelta(days=days)

    return [
        f for f in log_dir.glob('*.log')
        if f.is_file() and datetime.fromtimestamp(f.stat().st_mtime) > cutoff
    ]
```

## Testing Path-Dependent Code

### Use Fixtures for Test Paths

```python
import pytest
from pathlib import Path

@pytest.fixture
def test_data_dir(tmp_path):
    """Create temporary test data directory."""
    data_dir = tmp_path / 'test_data'
    data_dir.mkdir()
    return data_dir

@pytest.fixture
def sample_file(test_data_dir):
    """Create sample test file."""
    file_path = test_data_dir / 'sample.txt'
    file_path.write_text('test content')
    return file_path

def test_load_data(sample_file):
    """Test data loading with temporary file."""
    content = sample_file.read_text()
    assert content == 'test content'
```

### Mock Path Operations

```python
from unittest.mock import Mock, patch
from pathlib import Path

def test_file_processing(monkeypatch):
    """Test file processing with mocked paths."""
    mock_path = Mock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.read_text.return_value = 'test data'

    # Your test code here
```

## Configuration File Pattern

Store path configuration in a dedicated module:

```python
# config/paths.py
from pathlib import Path
import os

def get_project_root():
    """Get project root by searching for marker files."""
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / '.git').exists() or (parent / 'setup.py').exists():
            return parent
    raise RuntimeError("Could not find project root")

PROJECT_ROOT = get_project_root()

# Data directories
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'

# Configuration
CONFIG_DIR = PROJECT_ROOT / 'config'
ENV_FILE = PROJECT_ROOT / '.env'

# Logging
LOGS_DIR = PROJECT_ROOT / 'logs'

# Cache
CACHE_DIR = PROJECT_ROOT / '.cache'

# Ensure critical directories exist
for directory in [DATA_DIR, LOGS_DIR, CACHE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
```

Usage in other modules:

```python
# scripts/process_data.py
from config.paths import DATA_DIR, LOGS_DIR

input_file = DATA_DIR / 'raw' / 'input.csv'
log_file = LOGS_DIR / 'process.log'
```

## Summary Checklist

When writing a new script that handles file paths:

- [ ] Import `pathlib.Path` at the top
- [ ] Define `PROJECT_ROOT` or `SCRIPT_DIR` as anchor point
- [ ] Use `Path(__file__)` to anchor to script location
- [ ] Document how many levels up you navigate (comments)
- [ ] Use `/` operator for path joining
- [ ] Keep paths as `Path` objects, convert to `str` only when needed
- [ ] Validate path existence before operations
- [ ] Use `.mkdir(parents=True, exist_ok=True)` for directory creation
- [ ] Consider allowing environment variable overrides
- [ ] Test that paths work when script is executed from different directories

## References

- [Python pathlib documentation](https://docs.python.org/3/library/pathlib.html)
- [Real Python: Python's pathlib Module](https://realpython.com/python-pathlib/)
- [PEP 428: The pathlib module](https://peps.python.org/pep-0428/)

---

**Last Updated**: December 2025
