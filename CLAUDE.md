# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LSATS Data Hub is a Python package for cross-referencing and querying data from LSA Technology Services sources (Google Workspace, Active Directory, TeamDynamix, MCommunity, etc.). The primary use case is computer compliance management through automated ticket creation and follow-ups.

## Architecture Pattern: Adapter-Facade-Service

The codebase follows a strict three-layer pattern:

1. **API Adapters** (`*/api/*_api.py`): Wrap raw API requests for specific web services
   - Example: `teamdynamix/api/ticket_api.py`, `teamdynamix/api/user_api.py`
   - Handle HTTP requests, authentication, rate limiting
   - One adapter per API resource type

2. **Facades** (`*/facade/*_facade.py`): Organize and compose adapter calls
   - Example: `teamdynamix/facade/teamdynamix_facade.py`
   - Provide higher-level functions using multiple primitive API calls
   - Example: `get_user_assets_by_uniqname()` combines user lookup + asset retrieval

3. **Services** (`scripts/`): Orchestrate complex workflows across multiple facades
   - Example: `scripts/compliance/compliance_ticket_automator.py`
   - Use resources from several facades (TeamDynamix + Google Sheets + LDAP)
   - Implement business logic and data transformations

**When adding new functionality:**
- New API integration → Create adapter in `<source>/api/`
- Organizing related API calls → Add to or create facade in `<source>/facade/`
- Cross-system workflow → Create service script in `scripts/<category>/`

## Code Style Standards

All API adapter functions (`*_api.py` files) follow strict coding standards:

### Type Annotations
- **All function parameters must be strongly typed** using Python's `typing` module
- **All return types must be explicitly declared**
- Use `Optional[T]` for nullable returns, `Union[T1, T2]` for multiple possible types
- Common types: `Dict[str, Any]`, `List[Dict[str, Any]]`, `int`, `str`, `bool`, `BinaryIO`

### Docstring Format (Google/MCP Style)
All functions must include docstrings with:
1. **Brief description** (one line) of what the function does
2. **Args section**: Each parameter with name and description
3. **Returns section** (if applicable): Description of return value
4. **Notes section** (optional): Important caveats or warnings

**Example:**
```python
def get_ticket(self, id: int) -> Dict[str, Any]:
    """
    Gets a ticket by ID.

    Args:
        id: The ticket ID.
    
    Returns:
        Dictionary containing the full ticket information.
    """
    return self.get(f'tickets/{id}')
```

**Multi-parameter example:**
```python
def search_users_by_uniqname(self, uniqname: str, isActive: bool = True) -> Optional[List[Dict[str, Any]]]:
    """
    Searches for users by uniqname in various fields. Returns list of account objects.

    Args:
        uniqname: The uniqname to search for.
        isActive: Whether to only return active users.
    
    Returns:
        List of user dictionaries if found, None if no match.
    
    Notes:
        Searches in order: Username, AlternateID, then SearchText fields.
    """
    # Implementation...
```

### When to Apply These Standards
- **Required**: All new functions in `*_api.py` adapter files
- **Recommended**: Facade methods and service functions
- **Encouraged**: Helper functions and utilities

### Validation
Before committing API adapter changes, verify:
- [ ] All parameters have type annotations
- [ ] Return type is declared (use `-> None` if no return)
- [ ] Docstring includes function description
- [ ] All parameters documented in Args section
- [ ] Return value documented if function returns data

## Development Commands

### Installation
```bash
# Development mode (changes reflected immediately)
pip install -e .

# With all optional dependencies
pip install -e .[all]

# Specific feature sets
pip install -e .[teamdynamix]  # TeamDynamix only
pip install -e .[google]       # Google APIs only
pip install -e .[ai]           # AI features only
```

### Environment Setup
```bash
# Copy template and configure
cp .env.example .env
# Edit .env with your credentials

# Key variables to set:
# - TDX_BASE_URL: Sandbox uses /SBTDWebApi/, Production uses /TDWebApi/
# - TDX_API_TOKEN: Get from TDX login SSO endpoint
# - SPREADSHEET_ID: From Google Sheets URL
# - SHEET_NAME: Current month (case-sensitive)
```

### Running Compliance Scripts

All compliance commands support `--dry-run` and `--log <file>`:

```bash
# Generate new compliance tickets
compliance-automator --dry-run --log compliance_automator.log

# Send second outreach (follow-up)
compliance-update --dry-run --log compliance_update.log

# Send third outreach (escalate to CAs)
compliance-third-outreach --dry-run --log compliance_third_outreach.log
```

**PowerShell wrapper (Windows):**
```powershell
# Install PowerShell module (run from project root)
.\install.ps1

# Interactive menu
Show-ComplianceMenu
```

### Testing
```bash
# Run tests (when available)
python -m pytest tests/

# Test specific module
python -m pytest tests/teamdynamix/
```

## Critical Code Locations

### TeamDynamix Integration
- **Facade entry point**: `teamdynamix/facade/teamdynamix_facade.py:TeamDynamixFacade`
  - Initializes all API adapters (users, assets, tickets, CIs, etc.)
  - Provides convenience methods like `get_user_assets_by_uniqname()`
  
- **API adapters**: `teamdynamix/api/`
  - `ticket_api.py`: Ticket CRUD, feed entries, CI associations
  - `user_api.py`: User search and attribute lookup
  - `asset_api.py`: Asset queries and CI linking
  - `configuration_item_api.py`: CI (Configuration Item) management
  - `teamdynamix_api.py`: Base class with request/retry logic

### Google Sheets Integration
- **Adapter**: `google_drive/sheets/sheets_api.py:GoogleSheetsAdapter`
- **Model**: `google_drive/sheets/sheets_api.py:Sheet`
- Handles OAuth flow, caching (`token.json`), and batch operations

### Compliance Automation
- **Entry points** (console scripts): `scripts/compliance/`
  - `compliance_ticket_automator.py`: Reads spreadsheet → creates TDX tickets
  - `compliance_ticket_second_outreach.py`: Updates existing tickets
  - `compliance_ticket_third_outreach.py`: Escalates by adding CAs

- **Common pattern**: Load Google Sheet → Filter/transform data → Batch update TDX tickets

### AI Integration (Optional)
- **Facade**: `ai/ai_facade.py:AIFacade`
- **Adapters**: `ai/adapters/` (OpenAI, Ollama)
- Used for lab notes generation and content analysis

## Key Data Flows

### Compliance Ticket Creation Flow
1. Load environment (`.env`) → detect sandbox vs production
2. Initialize `GoogleSheetsAdapter` → read compliance spreadsheet
3. Initialize `TeamDynamixFacade` → get TDX ticket APIs
4. For each non-compliant computer row:
   - Check if ticket already exists (via spreadsheet column)
   - Create ticket via `TicketAPI.create_ticket()`
   - Update spreadsheet with ticket ID and link
5. Write back to Google Sheets in batch

### Ticket Outreach Flow
1. Read spreadsheet → filter for tickets needing outreach
2. For each ticket:
   - Check last activity via `get_ticket_feed()`
   - Calculate days since last response
   - Add feed entry with template message
   - Optionally add CA group members (third outreach)
3. Update spreadsheet status columns

## Environment Detection

Scripts auto-detect sandbox vs production from `TDX_BASE_URL`:
- Contains `SB` or `sandbox` → SANDBOX mode
- Otherwise → PRODUCTION mode

Ticket URL generation automatically transforms:
- `/SBTDWebApi/api` → `/SBTDNext/` (sandbox tickets)
- `/TDWebApi/api` → `/TDNext/` (production tickets)

## Configuration Files

- **`.env`**: Runtime configuration (API tokens, spreadsheet IDs)
- **`credentials.json`**: Google OAuth client credentials (not in git)
- **`token.json`**: Google OAuth access token (auto-generated, not in git)
- **`teamdynamix/api/ci_defaults.json`**: Default values for CI creation
- **`setup.py`**: Package dependencies and console script entry points

## Important Notes

- **Always use dry-run first**: All compliance scripts support `--dry-run` to preview changes
- **Sandbox testing**: Use `TDX_BASE_URL` with `/SBTDWebApi/` for safe testing
- **Sheet name**: Must exactly match the Google Sheets tab name (case-sensitive)
- **API token expiry**: TDX tokens expire; get fresh token from SSO endpoint if auth fails
- **Batch operations**: Google Sheets updates are batched to avoid rate limits
- **Error handling**: Most API adapters have retry logic with exponential backoff

## Module Organization

```
teamdynamix/          # TeamDynamix integration
├── api/              # Low-level API adapters
└── facade/           # High-level facade

google_drive/         # Google Workspace integration
└── sheets/           # Google Sheets adapter

ai/                   # AI service integration
├── adapters/         # Provider-specific adapters (OpenAI, Ollama)
└── models/           # Response models

ldap/                 # LDAP/Active Directory integration
├── adapters/         # LDAP connection adapters
└── facade/           # LDAP query facade

database/             # PostgreSQL integration (optional)
├── adapters/         # Database connection adapters
├── models/           # SQLAlchemy models
└── service/          # Data ingestion services

scripts/              # Executable services
├── compliance/       # Compliance automation
├── lab_notes/        # Lab management
└── database/         # Database ingestion

docker/               # Docker/PostgreSQL setup
```

## Common Gotchas

1. **Uniqname vs UID**: TeamDynamix uses numeric UIDs internally; most facades accept uniqnames and do the lookup
2. **Feed vs Comments**: TDX "feed" includes all activity (comments, status changes, assignments)
3. **CI vs Asset**: Configuration Items (CIs) can contain multiple assets; labs are modeled as CIs
4. **Date formats**: TDX returns ISO 8601 with 'Z' suffix; convert to datetime objects for comparison
5. **PowerShell execution policy**: May need to unblock scripts: `Unblock-File -Path .\install.ps1`
