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
            Write-Info "Project files already exist in $InstallPath"
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
        $spreadsheetId = Read-Host "Enter SPREADSHEET_ID"
        $sheetName = Read-Host "Enter SHEET_NAME (e.g., 'March')"

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

# Function to verify installation
function Test-Installation {
    Write-Info "`n=== Verifying Installation ==="

    # Test if commands are available
    $commands = @("compliance-automator", "compliance-update", "compliance-third-outreach")
    $allGood = $true

    foreach ($cmd in $commands) {
        if (Get-Command $cmd -ErrorAction SilentlyContinue) {
            Write-Success "✓ $cmd is available"
        } else {
            Write-Error "✗ $cmd not found"
            $allGood = $false
        }
    }

    if ($allGood) {
        Write-Success "`nAll commands installed successfully!"
    } else {
        Write-Warning "`nSome commands are not available. Make sure the virtual environment is activated."
    }

    # Create activation shortcuts
    Write-Info "`nCreating activation shortcuts..."

    # Create batch file for easy activation
    $activateBat = @"
@echo off
call "$InstallPath\$VenvName\Scripts\activate.bat"
"@
    Set-Content -Path "$InstallPath\activate.bat" -Value $activateBat

    # Create PowerShell activation script
    $activatePs1 = @"
& "$InstallPath\$VenvName\Scripts\Activate.ps1"
"@
    Set-Content -Path "$InstallPath\activate-compliance.ps1" -Value $activatePs1

    Write-Success "Created activation shortcuts!"
}

# Function to create self-activating wrapper scripts
function Create-WrapperScripts {
    Write-Info "`nCreating self-activating wrapper scripts..."

    # Create a directory for wrapper scripts
    $wrapperDir = "$InstallPath\compliance-scripts"
    if (-not (Test-Path $wrapperDir)) {
        New-Item -ItemType Directory -Path $wrapperDir | Out-Null
    }

    # Define the commands and their wrapper scripts
    $commands = @{
        "compliance-automator" = "compliance-automator.ps1"
        "compliance-update" = "compliance-update.ps1"
        "compliance-third-outreach" = "compliance-third-outreach.ps1"
    }

    foreach ($cmd in $commands.Keys) {
        $wrapperContent = @"
# Self-activating wrapper for $cmd
`$scriptPath = Split-Path -Parent `$MyInvocation.MyCommand.Path
`$projectPath = Split-Path -Parent `$scriptPath
`$venvPath = Join-Path `$projectPath ".venv"
`$activateScript = Join-Path `$venvPath "Scripts\Activate.ps1"

# Check if virtual environment exists
if (-not (Test-Path `$activateScript)) {
    Write-Host "Virtual environment not found at: `$venvPath" -ForegroundColor Red
    Write-Host "Please run the installation script first." -ForegroundColor Red
    exit 1
}

# Create a new PowerShell process with activated venv and run the command
`$arguments = `$args -join ' '
`$command = "& '`$activateScript'; $cmd `$arguments"
powershell.exe -NoProfile -Command `$command
"@

        $wrapperPath = Join-Path $wrapperDir $commands[$cmd]
        Set-Content -Path $wrapperPath -Value $wrapperContent
        Write-Success "Created wrapper: $($commands[$cmd])"
    }

    # Create a batch file for easy access from CMD
    foreach ($cmd in $commands.Keys) {
        $batchContent = @"
@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0\$($commands[$cmd])" %*
"@
        $batchPath = Join-Path $wrapperDir "$cmd.bat"
        Set-Content -Path $batchPath -Value $batchContent
    }

    Write-Success "Created self-activating wrapper scripts in: $wrapperDir"

    # Add to PATH suggestion
    Write-Info "`nTo run compliance commands from anywhere:"
    Write-Info "1. Add this directory to your PATH: $wrapperDir"
    Write-Info "   OR"
    Write-Info "2. Copy the wrapper scripts to a directory already in your PATH"

    Write-Info "`nWould you like to add the wrapper directory to your PATH now? (Y/n)"
    $response = Read-Host

    if ($response -ne 'n') {
        Add-ToPath -Path $wrapperDir
    }
}

# Function to add directory to PATH
function Add-ToPath {
    param($Path)

    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")

    if ($currentPath -notlike "*$Path*") {
        $newPath = "$currentPath;$Path"
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")

        # Also update current session
        $env:Path = "$env:Path;$Path"

        Write-Success "Added $Path to USER PATH"
        Write-Warning "Please restart your terminal for PATH changes to take full effect"
    } else {
        Write-Info "Directory already in PATH"
    }
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

    # Create wrapper scripts
    Create-WrapperScripts

    # Final instructions
    Write-Host "`n======================================" -ForegroundColor Green
    Write-Host "    Installation Complete!            " -ForegroundColor Green
    Write-Host "======================================" -ForegroundColor Green

    Write-Info "`nNext steps:"
    Write-Info "1. Make sure credentials.json is in place (if using Google Sheets)"
    Write-Info "2. Review and complete .env configuration"

    if ($env:Path -like "*$InstallPath\compliance-scripts*") {
        Write-Success "`n✓ Compliance commands are available globally!"
        Write-Info "You can now run commands from anywhere:"
        Write-Info "  - compliance-automator --help"
        Write-Info "  - compliance-update --dry-run"
        Write-Info "  - compliance-third-outreach --log"
    } else {
        Write-Info "`n3. To activate the virtual environment manually:"
        Write-Info "   - PowerShell: . $InstallPath\activate-compliance.ps1"
        Write-Info "   - CMD: $InstallPath\activate.bat"
        Write-Info "`n   OR use the self-activating scripts in:"
        Write-Info "   $InstallPath\compliance-scripts"
    }

    Write-Success "`nThe wrapper scripts automatically activate the virtual environment!"

    # Ask if user wants to test now
    Write-Info "`nWould you like to test a command now? (Y/n)"
    $response = Read-Host
    if ($response -ne 'n') {
        Write-Info "Running: compliance-automator --help"
        if (Test-Path "$InstallPath\compliance-scripts\compliance-automator.ps1") {
            & "$InstallPath\compliance-scripts\compliance-automator.ps1" --help
        } else {
            & "$InstallPath\$VenvName\Scripts\Activate.ps1"
            compliance-automator --help
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
