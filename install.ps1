# LSATS Data Hub Windows Installation Script
# This script automates the installation of LSATS Data Hub on Windows

param(
    [string]$InstallPath = $PWD.Path,
    [switch]$SkipGitClone = $false
)

# Script configuration
$RequiredPythonVersion = "3.6"
$ProjectName = "LSATS Data Hub"
$RepoUrl = "https://github.com/3Bodhi/LSATS_Data_Hub.git"
$VenvName = ".venv"

# Color functions for better output
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

# Function to check if running as administrator
function Test-Administrator {
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# Function to compare version strings
function Compare-Version {
    param($Version1, $Version2)
    $v1 = [version]($Version1 -replace '[^\d.]', '')
    $v2 = [version]($Version2 -replace '[^\d.]', '')
    return $v1.CompareTo($v2)
}

# Function to check Python installation
function Test-Python {
    try {
        $pythonCmd = Get-Command python -ErrorAction Stop
        $versionOutput = & python --version 2>&1
        if ($versionOutput -match "Python (\d+\.\d+\.\d+)") {
            $installedVersion = $matches[1]
            Write-Info "Python $installedVersion found at: $($pythonCmd.Path)"

            if ((Compare-Version $installedVersion $RequiredPythonVersion) -ge 0) {
                return @{
                    Installed = $true
                    Version = $installedVersion
                    Path = $pythonCmd.Path
                }
            } else {
                Write-Warning "Python version $installedVersion is below required version $RequiredPythonVersion"
                return @{ Installed = $false }
            }
        }
    } catch {
        Write-Warning "Python not found in PATH"
    }

    # Also check py launcher
    try {
        $pyCmd = Get-Command py -ErrorAction Stop
        $versionOutput = & py -3 --version 2>&1
        if ($versionOutput -match "Python (\d+\.\d+\.\d+)") {
            $installedVersion = $matches[1]
            Write-Info "Python $installedVersion found via py launcher"

            if ((Compare-Version $installedVersion $RequiredPythonVersion) -ge 0) {
                return @{
                    Installed = $true
                    Version = $installedVersion
                    Path = "py -3"
                    UsePy = $true
                }
            }
        }
    } catch {
        # py launcher not found
    }

    return @{ Installed = $false }
}

# Function to install Python
function Install-Python {
    Write-Warning "`nPython $RequiredPythonVersion or higher is required but not found."
    Write-Info "You have the following options:"
    Write-Info "1. Download and install Python manually from https://www.python.org/downloads/"
    Write-Info "2. Let this script download and install Python for you (requires admin rights)"
    Write-Info "3. Exit and install Python yourself"

    $choice = Read-Host "`nEnter your choice (1-3)"

    switch ($choice) {
        "1" {
            Start-Process "https://www.python.org/downloads/"
            Write-Info "`nPlease download and install Python, making sure to:"
            Write-Info "- Check 'Add Python to PATH' during installation"
            Write-Info "- Restart this script after installation"
            exit 0
        }
        "2" {
            if (-not (Test-Administrator)) {
                Write-Error "This option requires administrator privileges."
                Write-Info "Please restart PowerShell as Administrator and run this script again."
                exit 1
            }

            Write-Info "Downloading Python installer..."
            $pythonUrl = "https://www.python.org/ftp/python/3.12.0/python-3.12.0-amd64.exe"
            $installerPath = "$env:TEMP\python-installer.exe"

            try {
                Invoke-WebRequest -Uri $pythonUrl -OutFile $installerPath
                Write-Info "Installing Python..."
                Start-Process -FilePath $installerPath -ArgumentList "/quiet", "InstallAllUsers=1", "PrependPath=1" -Wait
                Remove-Item $installerPath

                Write-Success "Python installed successfully!"
                Write-Info "Please restart PowerShell and run this script again."
                exit 0
            } catch {
                Write-Error "Failed to download/install Python: $_"
                exit 1
            }
        }
        "3" {
            Write-Info "Exiting. Please install Python $RequiredPythonVersion or higher and run this script again."
            exit 0
        }
        default {
            Write-Error "Invalid choice. Exiting."
            exit 1
        }
    }
}

# Function to setup project
function Setup-Project {
    param($PythonInfo)

    # Determine Python command
    $pythonCmd = if ($PythonInfo.UsePy) { "py -3" } else { "python" }
    $pipCmd = if ($PythonInfo.UsePy) { "py -3 -m pip" } else { "python -m pip" }

    # Clone repository if needed
    if (-not $SkipGitClone) {
        if (Test-Path "$InstallPath\setup.py") {
            Write-Info "Project files already downloaded and exist at $InstallPath"
            $response = Read-Host "Use existing files? (Y/n)"
            if ($response -eq 'n') {
                Write-Error "Please choose a different installation path or use -SkipGitClone"
                exit 1
            }
        } else {
            Write-Info "`nCloning repository..."
            if (Get-Command git -ErrorAction SilentlyContinue) {
                git clone $RepoUrl $InstallPath
                if ($LASTEXITCODE -ne 0) {
                    Write-Error "Failed to clone repository"
                    exit 1
                }
                Set-Location $InstallPath
            } else {
                Write-Warning "Git is not installed. Please either:"
                Write-Info "1. Install Git from https://git-scm.com/download/win"
                Write-Info "2. Download the repository manually from GitHub and extract to: $InstallPath"
                Write-Info "   Then run this script again with -SkipGitClone flag"
                exit 1
            }
        }
    } else {
        Set-Location $InstallPath
    }

    # Verify we're in the right directory
    if (-not (Test-Path "setup.py")) {
        Write-Error "setup.py not found in current directory. Are you in the right folder?"
        exit 1
    }

    # Create virtual environment
    Write-Info "`nCreating virtual environment..."
    Invoke-Expression "$pythonCmd -m venv $VenvName"

    # Activate virtual environment
    Write-Info "Activating virtual environment..."
    $activateScript = "$VenvName\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
    } else {
        Write-Error "Failed to create virtual environment"
        exit 1
    }

    # Upgrade pip
    Write-Info "`nUpgrading pip..."
    python -m pip install --upgrade pip

    # Install the package with all dependencies
    Write-Info "`nInstalling $ProjectName with all dependencies..."
    pip install -e ".[all]"

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to install packages"
        exit 1
    }

    Write-Success "`n$ProjectName installed successfully!"
}

# Function to configure environment
function Configure-Environment {
    Write-Info "`n=== Environment Configuration ==="

    # Copy .env.example to .env if it doesn't exist
    if (-not (Test-Path ".env")) {
        if (Test-Path ".env.example") {
            Copy-Item ".env.example" ".env"
            Write-Success "Created .env file from .env.example"
        } else {
            Write-Error ".env.example not found!"
            return
        }
    } else {
        Write-Info ".env file already exists"
    }

    Write-Info "`nWould you like to configure the .env file now? (Y/n)"
    $response = Read-Host

    if ($response -ne 'n') {
        # Read current .env content
        $envContent = Get-Content ".env" -Raw

        Write-Info "`n=== TeamDynamix Configuration ==="
        Write-Info "Environment options:"
        Write-Info "  - Type 'sb' or 'sandbox' for sandbox environment"
        Write-Info "  - Type 'prod' or 'production' for production environment"
        Write-Info "  - Or enter a custom URL"

        $tdxInput = Read-Host "Enter environment (default: sandbox)"

        # Convert shortcuts to full URLs
        switch ($tdxInput.ToLower()) {
            { $_ -in @("", "sb", "sandbox") } {
                $tdxUrl = "https://teamdynamix.umich.edu/SBTDWebApi/api"
                $loginUrl = "https://teamdynamix.umich.edu/SBTDWebApi/api/auth/loginsso"
                Write-Info "Using SANDBOX environment"
            }
            { $_ -in @("prod", "production") } {
                $tdxUrl = "https://teamdynamix.umich.edu/TDWebApi/api"
                $loginUrl = "https://teamdynamix.umich.edu/TDWebApi/api/auth/loginsso"
                Write-Warning "Using PRODUCTION environment - real tickets will be created!"
            }
            default {
                $tdxUrl = $tdxInput
                $loginUrl = $tdxInput -replace "/api$", "/api/auth/loginsso"
            }
        }

        Write-Info "`nTo get your API token, visit:"
        Write-Host $loginUrl -ForegroundColor Blue -NoNewline
        Write-Host " (Ctrl+Click to open)" -ForegroundColor Gray

        # Try to open in browser
        $openBrowser = Read-Host "`nOpen this URL in your browser? (Y/n)"
        if ($openBrowser -ne 'n') {
            Start-Process $loginUrl
        }

        $tdxToken = Read-Host "`nEnter TDX_API_TOKEN"

        Write-Info "`n=== Google Sheets Configuration ==="
        Write-Info "`nHINT: This is the section after 'https://docs.google.com/spreadsheets/d/' in the URL."
        $spreadsheetId = Read-Host "Enter SPREADSHEET_ID"
        $date = (Get-Date).ToString("MMMM")
        $sheetName = Read-Host "Enter SHEET_NAME (e.g., '$date')"

        # Update .env content
        $envContent = $envContent -replace 'TDX_BASE_URL = ".*"', "TDX_BASE_URL = `"$tdxUrl`""
        if ($tdxToken) {
            $envContent = $envContent -replace 'TDX_API_TOKEN = #.*', "TDX_API_TOKEN = `"$tdxToken`""
        }
        if ($spreadsheetId) {
            $envContent = $envContent -replace 'SPREADSHEET_ID = #.*', "SPREADSHEET_ID = `"$spreadsheetId`""
        }
        if ($sheetName) {
            $envContent = $envContent -replace 'SHEET_NAME = #.*', "SHEET_NAME = `"$sheetName`""
        }

        # Save updated content
        Set-Content ".env" $envContent
        Write-Success ".env file updated!"
    }

    # Check for credentials.json
    if (-not (Test-Path "credentials.json")) {
        Write-Warning "`ncredentials.json not found!"
        Write-Info "You need to:"
        Write-Info "1. Go to https://developers.google.com/sheets/api/quickstart/python"
        Write-Info "2. Create a Google Cloud project and enable Sheets API"
        Write-Info "3. Download credentials.json"
        Write-Info "4. Place it in: $PWD"
    } else {
        Write-Success "credentials.json found!"
    }
}

# Function to test installation
function Test-Installation {
    Write-Info "`nTesting installation..."

    $commands = @("compliance-automator", "compliance-update", "compliance-third-outreach")
    $allWorking = $true

    foreach ($cmd in $commands) {
        try {
            $testOutput = & $cmd --help 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Success "✓ $cmd is working"
            } else {
                Write-Warning "⚠ $cmd returned non-zero exit code"
                $allWorking = $false
            }
        } catch {
            Write-Warning "⚠ $cmd not found or not working: $_"
            $allWorking = $false
        }
    }

    if ($allWorking) {
        Write-Success "`nAll compliance commands are working correctly!"
    } else {
        Write-Warning "`nSome commands are not available. Make sure the virtual environment is activated."
    }

    # Create activation shortcuts
    Write-Info "`nCreating activation shortcuts..."

    # Create batch file for easy activation
    $activateBat = @"
@echo off
call "$InstallPath\$VenvName\Scripts\activate.bat"
cd /d "$InstallPath"
"@
    Set-Content -Path "$InstallPath\activate.bat" -Value $activateBat

    Write-Success "Created activation shortcuts!"
}

# Function to install PowerShell module
function Install-ComplianceModule {
    Write-Info "`nInstalling Compliance PowerShell Module..."

    $moduleSource = "$InstallPath\scripts\compliance\ComplianceHelper"
    $moduleParentPath = Split-Path $moduleSource

    # Ensure module source directory exists
    if (-not (Test-Path $moduleSource)) {
        Write-Warning "Module source directory not found at: $moduleSource"
        Write-Info "Creating module directory structure..."
        New-Item -ItemType Directory -Path $moduleSource -Force | Out-Null

        Write-Warning "Module files need to be placed in: $moduleSource"
        Write-Info "Please ensure ComplianceHelper.psm1 and ComplianceHelper.psd1 are in that directory"
        return
    }

    # PRIMARY METHOD: Add to PSModulePath (keeps module in project directory)
    Write-Info "Adding module path to PSModulePath..."
    try {
        Add-ToPSModulePath -Path $moduleParentPath
        Write-Success "Module installed via PSModulePath: $moduleParentPath"

        # Test if the module can be found
        $availableModule = Get-Module -ListAvailable -Name "ComplianceHelper" -ErrorAction SilentlyContinue
        if ($availableModule) {
            Write-Success "✓ Module is discoverable in PowerShell"
        } else {
            Write-Warning "Module added to path but not immediately discoverable. May require PowerShell restart."
        }

    } catch {
        Write-Warning "Could not add to PSModulePath: $($_.Exception.Message)"
        Write-Info "Falling back to copying to Documents folder..."

        # FALLBACK METHOD: Copy to Documents folder
        $moduleDestination = Join-Path ([Environment]::GetFolderPath("MyDocuments")) "PowerShell\Modules\ComplianceHelper"

        # Ensure module destination directory exists
        if (-not (Test-Path $moduleDestination)) {
            New-Item -ItemType Directory -Path $moduleDestination -Force | Out-Null
        }

        try {
            Copy-Item "$moduleSource\*" -Destination $moduleDestination -Recurse -Force
            Write-Success "Module installed to: $moduleDestination"
        } catch {
            Write-Error "Failed to install module: $($_.Exception.Message)"
            return
        }
    }
}

# Function to add module path to PSModulePath
function Add-ToPSModulePath {
    param($Path)

    $currentPSModulePath = [Environment]::GetEnvironmentVariable("PSModulePath", "User")

    if ($currentPSModulePath -notlike "*$Path*") {
        $newPSModulePath = "$currentPSModulePath;$Path"
        [Environment]::SetEnvironmentVariable("PSModulePath", $newPSModulePath, "User")

        # Update current session
        $env:PSModulePath = "$env:PSModulePath;$Path"

        Write-Success "Added $Path to PSModulePath"
    } else {
        Write-Info "Module path already in PSModulePath"
    }
}

# Function to add module import to PowerShell profile
function Add-ModuleToProfile {
    Write-Info "`nConfiguring PowerShell profile..."

    $profilePath = $PROFILE.CurrentUserAllHosts
    $profileDir = Split-Path $profilePath

    # Create profile directory if it doesn't exist
    if (-not (Test-Path $profileDir)) {
        New-Item -ItemType Directory -Path $profileDir -Force | Out-Null
    }

    # Check if module import already exists in profile
    $importLine = "Import-Module ComplianceHelper -ErrorAction SilentlyContinue"

    if (Test-Path $profilePath) {
        $profileContent = Get-Content $profilePath
        if ($profileContent -contains $importLine) {
            Write-Info "Module import already exists in PowerShell profile"
            return
        }
    }

    # Add module import to profile
    try {
        Add-Content -Path $profilePath -Value "`n# LSATS Compliance Helper Module"
        Add-Content -Path $profilePath -Value $importLine
        Write-Success "Added module import to PowerShell profile: $profilePath"
    } catch {
        Write-Warning "Could not modify PowerShell profile. You may need to manually import the module."
        Write-Info "To manually import: Import-Module ComplianceHelper"
    }
}

# Function to add scripts directory to PATH
function Add-ScriptsToPath {
    Write-Info "`nAdding scripts directory to PATH..."

    $scriptsPath = "$InstallPath\scripts\compliance"
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")

    if ($currentPath -notlike "*$scriptsPath*") {
        $newPath = "$currentPath;$scriptsPath"
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")

        # Update current session
        $env:Path = "$env:Path;$scriptsPath"

        Write-Success "Added $scriptsPath to PATH"
        Write-Warning "Restart terminal for PATH changes to take full effect"
    } else {
        Write-Info "Scripts directory already in PATH"
    }
}

# Function to create simple wrapper scripts (optional fallback)
function Create-SimpleWrappers {
    Write-Info "`nCreating simple wrapper scripts..."

    $wrapperDir = "$InstallPath\scripts\compliance"

    # Ensure directory exists
    if (-not (Test-Path $wrapperDir)) {
        New-Item -ItemType Directory -Path $wrapperDir -Force | Out-Null
    }

    # Create wrapper for compliance-helper that calls the module
    $helperWrapper = @"
#Requires -Version 5.1
# Simple wrapper to call ComplianceHelper module
param(
    [Parameter(ValueFromRemainingArguments=`$true)]
    [string[]]`$Arguments
)

try {
    Import-Module ComplianceHelper -ErrorAction Stop
    Show-ComplianceMenu @Arguments
} catch {
    Write-Error "Failed to load ComplianceHelper module: `$(`$_.Exception.Message)"
    Write-Host "Try running: Import-Module ComplianceHelper" -ForegroundColor Yellow
    exit 1
}
"@

    Set-Content -Path "$wrapperDir\compliance-helper.ps1" -Value $helperWrapper -Encoding UTF8
    Write-Success "Created compliance-helper.ps1 wrapper"
}

# Function to setup compliance commands (replaces Create-WrapperScripts)
function Setup-ComplianceCommands {
    Write-Info "`nSetting up Compliance Commands..."

    # Install PowerShell module
    Install-ComplianceModule

    # Add module to PowerShell profile for auto-loading
    Add-ModuleToProfile

    # Add scripts directory to PATH (for any additional scripts)
    Add-ScriptsToPath

    # Create simple wrapper scripts as fallback
    Create-SimpleWrappers

    Write-Success "Compliance commands configured!"
    Write-Info "`nAvailable commands:"
    Write-Info "  - Invoke-ComplianceAutomator"
    Write-Info "  - Update-Compliance"
    Write-Info "  - Invoke-ComplianceEscalation"
    Write-Info "  - Show-ComplianceMenu"
    Write-Info "`nOr use: compliance-helper.ps1 for interactive menu"
}

# Main installation flow
function Main {
    Clear-Host
    Write-Host "======================================" -ForegroundColor Cyan
    Write-Host "  LSATS Data Hub Windows Installer   " -ForegroundColor Cyan
    Write-Host "======================================" -ForegroundColor Cyan
    Write-Info "`nInstallation Path: $InstallPath"

    # Check Python
    Write-Info "`nChecking Python installation..."
    $pythonInfo = Test-Python

    if (-not $pythonInfo.Installed) {
        Install-Python
    } else {
        Write-Success "Python $($pythonInfo.Version) is installed and ready!"
    }

    # Setup project
    Setup-Project -PythonInfo $pythonInfo

    # Configure environment
    Configure-Environment

    # Verify installation
    Test-Installation

    # Setup compliance commands (REPLACES Create-WrapperScripts)
    Setup-ComplianceCommands

    # Final instructions
    Write-Host "`n======================================" -ForegroundColor Green
    Write-Host "    Installation Complete!            " -ForegroundColor Green
    Write-Host "======================================" -ForegroundColor Green

    Write-Info "`nNext steps:"
    Write-Info "1. Make sure credentials.json is in place (if using Google Sheets)"
    Write-Info "2. Review and complete .env configuration"

    Write-Success "`n✓ Compliance commands are available!"
    Write-Info "Start a new PowerShell session and try:"
    Write-Info "  - Show-ComplianceMenu"
    Write-Info "  - Invoke-ComplianceAutomator --help"
    Write-Info "  - Get-Command *Compliance*"

    Write-Info "`nWould you like to test the module now? (Y/n)"
    $response = Read-Host
    if ($response -ne 'n') {
        Write-Info "Testing module import..."
        try {
            Import-Module ComplianceHelper -Force
            Write-Success "✓ Module loaded successfully!"
            Write-Info "Running Show-ComplianceMenu..."
            Show-ComplianceMenu
        } catch {
            Write-Warning "Module test failed: $($_.Exception.Message)"
            Write-Info "Try restarting PowerShell and run: Import-Module ComplianceHelper"
        }
    }
}

# Run the installer
try {
    Main
} catch {
    Write-Error "An error occurred: $_"
    exit 1
}
