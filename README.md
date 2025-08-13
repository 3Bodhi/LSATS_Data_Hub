# LSATS Data Hub

## Introduction

LSATS Data Hub is a comprehensive set of Python modules designed to simplify complex and interlocking queries of LSA Technology Services data sources. Its purpose is to make it easier for end users to cross-reference data and generate insights from the LSATS myriad of data sources such as Google Workspace, Active Directory, TeamDynamix, MCommunity, and more.

The packages are designed using an adapter-facade-service paradigm:
- **API Adapters** wrap API requests from specific web applications to simplify API calls
- **Facades** organize a single API's functions and provide higher-level functions utilizing multiple primitive API requests
- **Services** orchestrate complex functions that use resources from several facades

This paradigm helps keep the codebase modular and maintainable as the organization cycles through data sources and as data sources update.

## Quick Start

### Windows Installation (Recommended)

The easiest way to get started on Windows is using the automated installer:

1. **Download or clone the repository:**
   ```powershell
   git clone https://github.com/3Bodhi/LSATS_Data_Hub.git
   cd LSATS_Data_Hub
   ```

   > **Note:** Place the project folder somewhere safe where it isn't likely to get deleted.

2. **Run the installer:**
   ```powershell
   .\install.ps1
   ```
   > **Note:** Certain versions may have unsigned powershell scripts which are not allowed to run under standard execution policies. Best practices here would be to unblock the powershell scripts using the Unblock-File command:
 ```powershell
    Unblock-File -Path .\install.ps1
    Unblock-File -Path .\scripts\compliance\ComplianceHelper\ComplianceHelper.psm1 # The module containg the automations install.ps1 installs
 ```
   > Alternatively, you can allow set the policy to run all unsigned scripts:
  ```powershell
    Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope LocalMachine
  ```

The installer will:
- Check and install Python if needed
- Create a virtual environment
- Install all dependencies
- Set up the ComplianceHelper PowerShell module
- Configure environment variables interactively
- Test the installation

3. **Start using the Compliance Helper:**
   ```powershell
   Show-ComplianceMenu
   ```

### Manual Installation

If you prefer manual installation or are using a non-Windows system:

1. **Create a virtual environment (recommended):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. **Install the package:**
   ```bash
   pip install .
   ```

## Configuration

### Environment Variables

Create a `.env` file from the `.env.example` file to configure your environment variables. The Windows installer will help you set this up interactively.

Key variables:
- `TDX_BASE_URL`: TeamDynamix API endpoint (sandbox or production)
- `TDX_API_TOKEN`: Your TeamDynamix API token
- `SPREADSHEET_ID`: Google Sheets spreadsheet ID
- `SHEET_NAME`: Current sheet name (usually the month)

### TeamDynamix Setup

TeamDynamix uses an API token you can receive from the login endpoint:
- **Production:** https://teamdynamix.umich.edu/TDWebApi/api/auth/loginsso
- **Sandbox (testing):** https://teamdynamix.umich.edu/SBTDWebApi/api/auth/loginsso

### Google API Setup

A `credentials.json` file and OAuth setup is required to access the Google Sheets API. The credentials.json file will be made available in PasswordState. For now, contact myodhes@umich.edu[myodhes@umich.edu](mailto:myodhes@umich.edu?subject=Credentials.json%Request) to retrieve the credentials.json file. Alternatively, you can generate your own project and crentials.json by following the directions in the [Google Sheets API quickstart guide](https://developers.google.com/sheets/api/quickstart/python).

Place the `credentials.json` file in the project root directory.

The `SPREADSHEET_ID` can be found in the Google Sheets URL:
`https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=0`

## Computer Compliance Management

### Compliance Helper Menu (Recommended)

The primary interface for compliance management is the interactive PowerShell menu:

```powershell
Show-ComplianceMenu
```

The menu provides:

#### **Compliance Commands:**
1. **🎫 Generate Compliance Tickets (Automator)** - Creates new compliance tickets for non-compliant computers
2. **📧 Send Second Outreach (Update)** - Sends follow-up notifications for unresponsive tickets
3. **🚨 Send Third Outreach (Escalate to CAs)** - Adds Computing Associates and sends escalation notifications

#### **Configuration Options:**
4. **🔧 View Environment Configuration** - Display current settings
5. **🧪 Test Commands (Show Help)** - Test all compliance commands
6. **⚙️ Modify Environment Configuration** - Full environment setup wizard
7. **📄 Update Spreadsheet Name** - Quick sheet name update (e.g., to current month)
8. **🔄 Toggle Sandbox/Production** - Quick environment switching

#### **Environment Status Display:**
- Virtual Environment status
- Configuration file presence
- TDX Environment (SANDBOX/PRODUCTION)
- Google Credentials status
- Current Sheet Name

### Direct Command Usage

You can also run compliance commands directly:

#### PowerShell Functions (After Installation)
```powershell
# Generate compliance tickets
Invoke-ComplianceAutomator

# Send second outreach
Update-Compliance

# Send third outreach with CA escalation
Invoke-ComplianceEscalation
```

#### Python Commands (With Virtual Environment)
```bash
# Generate compliance tickets
compliance-automator --dry-run --log compliance_automator.log

# Send second outreach
compliance-update --dry-run --log compliance_update.log

# Send third outreach with CA escalation
compliance-third-outreach --dry-run --log compliance_third_outreach.log
```

### Command Options

All compliance commands support:
- `--dry-run`: Preview mode (no actual changes)
- `--log <filename>`: Enable logging to specified file
- `--help`: Show command help

## Safety Features

### Environment Protection
- **Sandbox Mode**: Safe testing environment that doesn't create real tickets
- **Production Warnings**: Scripts indicate when working with live data
- **Environment Toggle**: Easily switch between sandbox and production
- **Dry-run Mode**: Preview changes before executing

## Troubleshooting

### Common Issues

**Virtual environment issues:**
```powershell
# Re-run the installer
.\install.ps1
```

**Credentials not working:**
1. Verify `credentials.json` is in the project root
2. Check that your API token is current
3. Ensure you're using the correct environment (sandbox vs production)

### Getting Help

- Run `Show-ComplianceMenu` and use option 5 to test all commands
- Check the Environment Status in the menu for configuration issues
- Use `--help` flag with any compliance command for usage information
- Slack or email myodhes@umich.edu

## Development

For detailed development and installation instructions, see [INSTALL.md](INSTALL.md).

Typically, you will want to clone with git and install with:
 ```bash
   pip install -e .
   ```
This will allow you develop and test dynamically.

### Contributing

This project follows the adapter-facade-service paradigm to maintain modularity and maintainability. When adding new features:

1. Create adapters for new API integrations
2. Build facades for organizing related functionality
3. Develop services for complex multi-system operations
