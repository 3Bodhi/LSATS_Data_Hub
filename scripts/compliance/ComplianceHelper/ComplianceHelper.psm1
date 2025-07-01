# LSATS Data Hub Compliance Helper PowerShell Module
# Provides native PowerShell access to compliance automation tools
#Requires -Version 5.1

# Module variables for paths
$script:ProjectRoot = if ($PSScriptRoot) { Split-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) -Parent } else { Get-Location }
$script:VenvPath = Join-Path $script:ProjectRoot ".venv"
$script:ActivateScript = Join-Path $script:VenvPath "Scripts\Activate.ps1"

# Helper functions for output formatting
function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Info {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Cyan
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

# Function to activate virtual environment and run Python command
function Invoke-PythonCommand {
    param(
        [string]$Command,
        [string[]]$Arguments = @(),
        [string]$Description = "Running command"
    )

    Write-Info "⚡ $Description..."

    if (-not (Test-Path $script:ActivateScript)) {
        Write-Error "Virtual environment not found at: $script:VenvPath"
        Write-Info "Run install.ps1 to set up the environment."
        return $false
    }

    try {
        # Activate virtual environment and run command
        & $script:ActivateScript
        & $Command @Arguments
        return $LASTEXITCODE -eq 0
    }
    catch {
        Write-Error "Failed to run $Command : $_"
        return $false
    }
}

# Function to get environment variables from .env file
function Get-EnvVariables {
    $envFile = Join-Path $script:ProjectRoot ".env"
    $envVars = @{}

    if (Test-Path $envFile) {
        $content = Get-Content $envFile
        foreach ($line in $content) {
            if ($line -match '^([^#=]+)=(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim(' "')
                $envVars[$key] = $value
            }
        }
    }

    return $envVars
}

# Function to update environment variable in .env file
function Set-EnvVariable {
    param(
        [string]$Key,
        [string]$Value
    )

    $envFile = Join-Path $script:ProjectRoot ".env"

    if (-not (Test-Path $envFile)) {
        Write-Error ".env file not found at: $envFile"
        return $false
    }

    $content = Get-Content $envFile
    $updated = $false
    $newContent = @()

    foreach ($line in $content) {
        if ($line -match "^$Key\s*=") {
            $newContent += "$Key = `"$Value`""
            $updated = $true
        } else {
            $newContent += $line
        }
    }

    if (-not $updated) {
        $newContent += "$Key = `"$Value`""
    }

    Set-Content -Path $envFile -Value $newContent -Encoding UTF8
    return $true
}

# Function to get script options interactively
function Get-ScriptOptions {
    param(
        [string]$ScriptName,
        [string]$Description,
        [string]$DefaultLogFile
    )

    Write-Host "`n======================================" -ForegroundColor Green
    Write-Host "    $Description" -ForegroundColor Green
    Write-Host "======================================" -ForegroundColor Green

    $options = @()

    # Ask for dry run
    Write-Host "`nRun in dry-run mode (preview only, no actual changes)? (Y/n): " -NoNewline -ForegroundColor Yellow
    $dryRun = Read-Host
    if ($dryRun -ne 'n') {
        $options += "--dry-run"
    }

    # Ask for verbose output
    Write-Host "Enable verbose output? (y/N): " -NoNewline -ForegroundColor Yellow
    $verbose = Read-Host
    if ($verbose -eq 'y') {
        $options += "--verbose"
    }

    # Ask for log file
    Write-Host "Log file path (Enter for default '$DefaultLogFile'): " -NoNewline -ForegroundColor Yellow
    $logFile = Read-Host
    if ([string]::IsNullOrWhiteSpace($logFile)) {
        $logFile = $DefaultLogFile
    }
    $options += "--log", $logFile

    return $options
}

# Function to invoke a compliance script with options
function Invoke-ComplianceScript {
    param(
        [string]$Command,
        [string[]]$Arguments,
        [string]$Description
    )

    Write-Host "`n$('-' * 50)" -ForegroundColor Gray
    Write-Host "Executing: $Command $($Arguments -join ' ')" -ForegroundColor White
    Write-Host "$('-' * 50)" -ForegroundColor Gray

    $success = Invoke-PythonCommand -Command $Command -Arguments $Arguments -Description $Description

    if ($success) {
        Write-Success "`n$Description completed successfully!"
    } else {
        Write-Error "`n$Description failed or was interrupted."
    }

    Read-Host "`nPress Enter to continue"
}

# Main compliance automation function
function Invoke-ComplianceAutomator {
    param(
        [Parameter(ValueFromRemainingArguments=$true)]
        [string[]]$Arguments
    )

    if ($Arguments.Count -eq 0) {
        $Arguments = Get-ScriptOptions -ScriptName "compliance-automator" -Description "Generate Compliance Tickets" -DefaultLogFile "compliance_automator.log"
    }

    Invoke-PythonCommand -Command "compliance-automator" -Arguments $Arguments -Description "Generate Compliance Tickets"
}

# Compliance update function
function Update-Compliance {
    param(
        [Parameter(ValueFromRemainingArguments=$true)]
        [string[]]$Arguments
    )

    if ($Arguments.Count -eq 0) {
        $Arguments = Get-ScriptOptions -ScriptName "compliance-update" -Description "Send Second Outreach" -DefaultLogFile "compliance_update.log"
    }

    Invoke-PythonCommand -Command "compliance-update" -Arguments $Arguments -Description "Send Second Outreach"
}

# Compliance escalation function
function Invoke-ComplianceEscalation {
    param(
        [Parameter(ValueFromRemainingArguments=$true)]
        [string[]]$Arguments
    )

    if ($Arguments.Count -eq 0) {
        $Arguments = Get-ScriptOptions -ScriptName "compliance-third-outreach" -Description "Escalate with CAs" -DefaultLogFile "compliance_third_outreach.log"
    }

    Invoke-PythonCommand -Command "compliance-third-outreach" -Arguments $Arguments -Description "Escalate with CAs"
}

# Function to modify environment configuration (similar to install.ps1)
function Update-EnvironmentConfiguration {
    Write-Host "`n======================================" -ForegroundColor Cyan
    Write-Host "    Environment Configuration        " -ForegroundColor Cyan
    Write-Host "======================================" -ForegroundColor Cyan

    $envFile = Join-Path $script:ProjectRoot ".env"

    if (-not (Test-Path $envFile)) {
        Write-Error ".env file not found at: $envFile"
        Write-Info "Run install.ps1 to create initial configuration."
        return
    }

    Write-Info "`nCurrent project path: $script:ProjectRoot"
    Write-Info "Configuring: $envFile"

    # Get current environment variables
    $currentEnv = Get-EnvVariables

    Write-Info "`n=== TeamDynamix Configuration ==="
    Write-Info "Current environment: $($currentEnv['TDX_BASE_URL'])"
    Write-Info "`nEnvironment options:"
    Write-Info "  - Type 'sb' or 'sandbox' for sandbox environment"
    Write-Info "  - Type 'prod' or 'production' for production environment"
    Write-Info "  - Or enter a custom URL"
    Write-Info "  - Press Enter to keep current setting"

    $tdxInput = Read-Host "`nEnter environment"

    if (-not [string]::IsNullOrWhiteSpace($tdxInput)) {
        # Convert shortcuts to full URLs
        switch ($tdxInput.ToLower()) {
            { $_ -in @("sb", "sandbox") } {
                $tdxUrl = "https://teamdynamix.umich.edu/SBTDWebApi/api"
                $loginUrl = "https://teamdynamix.umich.edu/SBTDWebApi/api/auth/loginsso"
                Write-Info "Setting SANDBOX environment"
            }
            { $_ -in @("prod", "production") } {
                $tdxUrl = "https://teamdynamix.umich.edu/TDWebApi/api"
                $loginUrl = "https://teamdynamix.umich.edu/TDWebApi/api/auth/loginsso"
                Write-Warning "Setting PRODUCTION environment - real tickets will be created!"
            }
            default {
                $tdxUrl = $tdxInput
                $loginUrl = $tdxInput -replace "/api$", "/api/auth/loginsso"
            }
        }

        Set-EnvVariable -Key "TDX_BASE_URL" -Value $tdxUrl

        Write-Info "`nTo get your API token, visit:"
        Write-Host $loginUrl -ForegroundColor Blue -NoNewline
        Write-Host " (Ctrl+Click to open)" -ForegroundColor Gray

        # Try to open in browser
        $openBrowser = Read-Host "`nOpen this URL in your browser? (Y/n)"
        if ($openBrowser -ne 'n') {
            try {
                Start-Process $loginUrl
            } catch {
                Write-Warning "Could not open browser automatically. Please copy the URL above."
            }
        }

        $tdxToken = Read-Host "`nEnter TDX_API_TOKEN (or press Enter to keep current)"
        if (-not [string]::IsNullOrWhiteSpace($tdxToken)) {
            Set-EnvVariable -Key "TDX_API_TOKEN" -Value $tdxToken
        }
    }

    Write-Info "`n=== Google Sheets Configuration ==="
    Write-Info "Current spreadsheet ID: $($currentEnv['SPREADSHEET_ID'])"
    Write-Info "Current sheet name: $($currentEnv['SHEET_NAME'])"

    Write-Info "`nHINT: Spreadsheet ID is the section after 'https://docs.google.com/spreadsheets/d/' in the URL."
    $spreadsheetId = Read-Host "Enter SPREADSHEET_ID (or press Enter to keep current)"
    if (-not [string]::IsNullOrWhiteSpace($spreadsheetId)) {
        Set-EnvVariable -Key "SPREADSHEET_ID" -Value $spreadsheetId
    }

    $currentMonth = (Get-Date).ToString("MMMM")
    $sheetName = Read-Host "Enter SHEET_NAME (current month would be '$currentMonth', or press Enter to keep current)"
    if (-not [string]::IsNullOrWhiteSpace($sheetName)) {
        Set-EnvVariable -Key "SHEET_NAME" -Value $sheetName
    }

    # Check credentials file
    $credentialsFile = Join-Path $script:ProjectRoot "credentials.json"
    Write-Info "`n=== Google Credentials ==="
    if (Test-Path $credentialsFile) {
        Write-Success "credentials.json found at: $credentialsFile"
    } else {
        Write-Warning "credentials.json not found at: $credentialsFile"
        Write-Info "If you plan to use Google Sheets integration, you'll need to:"
        Write-Info "1. Follow the Google Sheets API setup guide"
        Write-Info "2. Download your credentials.json file"
        Write-Info "3. Place it in the project root directory: $script:ProjectRoot"
    }

    Write-Success "`nEnvironment configuration updated!"
    Write-Info "Changes have been saved to: $envFile"
}

# Function to update just the spreadsheet name
function Update-SpreadsheetName {
    Write-Host "`n======================================" -ForegroundColor Cyan
    Write-Host "    Update Spreadsheet Name          " -ForegroundColor Cyan
    Write-Host "======================================" -ForegroundColor Cyan

    $envFile = Join-Path $script:ProjectRoot ".env"

    if (-not (Test-Path $envFile)) {
        Write-Error ".env file not found at: $envFile"
        Write-Info "Run install.ps1 to create initial configuration."
        return
    }

    # Get current sheet name
    $currentEnv = Get-EnvVariables
    $currentSheetName = $currentEnv['SHEET_NAME']

    Write-Info "Current sheet name: $currentSheetName"

    # Calculate current month
    $currentMonth = (Get-Date).ToString("MMMM")

    Write-Info "`nWould you like to update the sheet name to the current month ($currentMonth)? (Y/n)"
    $useCurrentMonth = Read-Host

    if ($useCurrentMonth -ne 'n') {
        $newSheetName = $currentMonth
        Write-Info "Setting sheet name to: $newSheetName"
    } else {
        $newSheetName = Read-Host "`nEnter the new sheet name"
        if ([string]::IsNullOrWhiteSpace($newSheetName)) {
            Write-Warning "No sheet name entered. Operation cancelled."
            return
        }
    }

    # Update the sheet name
    if (Set-EnvVariable -Key "SHEET_NAME" -Value $newSheetName) {
        Write-Success "Sheet name updated to: $newSheetName"
        Write-Info "Changes have been saved to: $envFile"
    } else {
        Write-Error "Failed to update sheet name."
    }
}

<#
.SYNOPSIS
Shows an interactive menu for compliance automation tasks.

.DESCRIPTION
Provides a user-friendly menu interface for managing compliance automation tasks,
including configuration management and command execution.

.PARAMETER ProjectPath
Optional path to the project directory. Defaults to auto-detected path.

.EXAMPLE
Show-ComplianceMenu
#>
function Show-ComplianceMenu {
    [CmdletBinding()]
    param(
        [string]$ProjectPath = $script:ProjectRoot
    )

    # Update project path if provided
    if ($ProjectPath -ne $script:ProjectRoot) {
        $script:ProjectRoot = $ProjectPath
        $script:VenvPath = Join-Path $script:ProjectRoot ".venv"
        $script:ActivateScript = Join-Path $script:VenvPath "Scripts\Activate.ps1"
    }

    do {
        Clear-Host
        Write-Host "======================================" -ForegroundColor Cyan
        Write-Host "    LSATS Compliance Helper Menu     " -ForegroundColor Cyan
        Write-Host "======================================" -ForegroundColor Cyan

        Write-Host "`nProject Path: " -NoNewline -ForegroundColor Gray
        Write-Host $script:ProjectRoot -ForegroundColor White

        # Check environment status
        $envFile = Join-Path $script:ProjectRoot ".env"
        $credentialsFile = Join-Path $script:ProjectRoot "credentials.json"

        Write-Host "`nEnvironment Status:" -ForegroundColor Yellow
        Write-Host "  Virtual Environment: " -NoNewline -ForegroundColor Gray
        if (Test-Path $script:ActivateScript) {
            Write-Host "✓ Ready" -ForegroundColor Green
        } else {
            Write-Host "✗ Not Found" -ForegroundColor Red
        }

        Write-Host "  Configuration (.env): " -NoNewline -ForegroundColor Gray
        if (Test-Path $envFile) {
            Write-Host "✓ Found" -ForegroundColor Green
        } else {
            Write-Host "✗ Missing" -ForegroundColor Red
        }

        Write-Host "  Google Credentials: " -NoNewline -ForegroundColor Gray
        if (Test-Path $credentialsFile) {
            Write-Host "✓ Found" -ForegroundColor Green
        } else {
            Write-Host "⚠ Missing" -ForegroundColor Yellow
        }

        Write-Host "`n======================================" -ForegroundColor White
        Write-Host "Available Commands:" -ForegroundColor White
        Write-Host "`n1. 🎫 Generate Compliance Tickets (Automator)" -ForegroundColor Cyan
        Write-Host "   Creates new compliance tickets for non-compliant computers"

        Write-Host "`n2. 📧 Send Second Outreach (Update)" -ForegroundColor Cyan
        Write-Host "   Sends follow-up notifications for unresponsive tickets"

        Write-Host "`n3. 🚨 Send Third Outreach (Escalate to CAs)" -ForegroundColor Cyan
        Write-Host "   Adds Computing Associates and sends escalation notifications"

        Write-Host "`n======================================" -ForegroundColor White
        Write-Host "Other Options:" -ForegroundColor White
        Write-Host "`n4. 🔧 View Environment Configuration" -ForegroundColor Yellow
        Write-Host "5. 🧪 Test Commands (Show Help)" -ForegroundColor Yellow
        Write-Host "6. ⚙️  Modify Environment Configuration" -ForegroundColor Yellow
        Write-Host "7. 📄 Update Spreadsheet Name" -ForegroundColor Yellow
        Write-Host "`nQ. ❌ Quit" -ForegroundColor Red

        $choice = Read-Host "`nEnter your choice [1-7, Q]"

        switch ($choice.ToUpper()) {
            "1" {
                $options = Get-ScriptOptions -ScriptName "compliance-automator" -Description "Creates compliance tickets for non-compliant computers" -DefaultLogFile "compliance_automator.log"
                Invoke-ComplianceScript -Command "compliance-automator" -Arguments $options -Description "Generate Compliance Tickets"
            }

            "2" {
                $options = Get-ScriptOptions -ScriptName "compliance-update" -Description "Sends second outreach notifications for non-responsive tickets" -DefaultLogFile "compliance_update.log"
                Invoke-ComplianceScript -Command "compliance-update" -Arguments $options -Description "Send Second Outreach"
            }

            "3" {
                $options = Get-ScriptOptions -ScriptName "compliance-third-outreach" -Description "Sends third outreach with CA notifications" -DefaultLogFile "compliance_third_outreach.log"
                Invoke-ComplianceScript -Command "compliance-third-outreach" -Arguments $options -Description "Escalate with CAs"
            }

            "4" {
                Clear-Host
                Write-Host "======================================" -ForegroundColor Cyan
                Write-Host "    Environment Configuration        " -ForegroundColor Cyan
                Write-Host "======================================" -ForegroundColor Cyan

                Write-Host "`nProject Directory: $script:ProjectRoot" -ForegroundColor White
                Write-Host "Virtual Environment: $script:VenvPath" -ForegroundColor White

                $envVars = Get-EnvVariables
                if ($envVars.Count -gt 0) {
                    Write-Host "`nEnvironment Variables (.env):" -ForegroundColor Yellow
                    foreach ($key in $envVars.Keys) {
                        if ($key -like "*TOKEN*" -or $key -like "*PASSWORD*") {
                            Write-Host "  $key = [HIDDEN]" -ForegroundColor Gray
                        } else {
                            Write-Host "  $key = $($envVars[$key])" -ForegroundColor Gray
                        }
                    }
                } else {
                    Write-Warning "No environment variables found in .env file"
                    Write-Host "Run install.ps1 to configure environment." -ForegroundColor Yellow
                }

                Read-Host "`nPress Enter to continue"
            }

            "5" {
                Clear-Host
                Write-Host "======================================" -ForegroundColor Cyan
                Write-Host "    Testing Commands                  " -ForegroundColor Cyan
                Write-Host "======================================" -ForegroundColor Cyan

                Write-Host "`nTesting compliance-automator --help:" -ForegroundColor Yellow
                try {
                    Invoke-ComplianceAutomator "--help"
                } catch {
                    Write-Error "Failed to run compliance-automator: $($_.Exception.Message)"
                }

                Write-Host "`n" + "="*50
                Write-Host "`nTesting compliance-update --help:" -ForegroundColor Yellow
                try {
                    Update-Compliance "--help"
                } catch {
                    Write-Error "Failed to run compliance-update: $($_.Exception.Message)"
                }

                Write-Host "`n" + "="*50
                Write-Host "`nTesting compliance-third-outreach --help:" -ForegroundColor Yellow
                try {
                    Invoke-ComplianceEscalation "--help"
                } catch {
                    Write-Error "Failed to run compliance-third-outreach: $($_.Exception.Message)"
                }

                Read-Host "`nPress Enter to continue"
            }

            "6" {
                Update-EnvironmentConfiguration
                Read-Host "`nPress Enter to continue"
            }

            "7" {
                Update-SpreadsheetName
                Read-Host "`nPress Enter to continue"
            }

            "Q" {
                Write-Host "`nGoodbye!" -ForegroundColor Green
                return
            }

            default {
                Write-Warning "Invalid choice. Please try again."
                Start-Sleep -Seconds 1
            }
        }

    } while ($true)
}

# Export public functions
Export-ModuleMember -Function @(
    'Invoke-ComplianceAutomator',
    'Update-Compliance',
    'Invoke-ComplianceEscalation',
    'Show-ComplianceMenu'
)
