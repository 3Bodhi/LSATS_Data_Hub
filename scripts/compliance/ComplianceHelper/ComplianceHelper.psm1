# LSATS Data Hub Compliance Helper PowerShell Module
# Provides native PowerShell access to compliance automation tools

# Module variables - Auto-detect project path
$script:ModuleRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:ProjectRoot = Split-Path -Parent (Split-Path -Parent $script:ModuleRoot)
$script:VenvPath = Join-Path $script:ProjectRoot ".venv"
$script:ActivateScript = Join-Path $script:VenvPath "Scripts\Activate.ps1"

# Color functions for consistent output
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }
function Write-Menu { Write-Host $args -ForegroundColor White }

# Private helper function to execute Python commands with venv
function Invoke-PythonWithVenv {
    param(
        [string]$Command,
        [string[]]$Arguments = @()
    )

    # Save current location
    $originalLocation = Get-Location

    try {
        # Validate venv exists
        if (-not (Test-Path $script:ActivateScript)) {
            throw "Virtual environment not found at: $script:VenvPath. Please run install.ps1 first."
        }

        # Change to project directory
        Set-Location $script:ProjectRoot

        # Activate virtual environment and run command
        $argumentString = $Arguments -join ' '
        $fullCommand = "& '$script:ActivateScript'; $Command $argumentString"

        Invoke-Expression $fullCommand

    } catch {
        Write-Error "Error executing $Command`: $($_.Exception.Message)"
        throw
    } finally {
        # Always restore original location
        Set-Location $originalLocation
    }
}

# Private helper function to read .env file
function Get-EnvVariables {
    $envFile = Join-Path $script:ProjectRoot ".env"
    $envVars = @{}

    if (Test-Path $envFile) {
        Get-Content $envFile | ForEach-Object {
            if ($_ -match '^([^#][^=]*)\s*=\s*(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim().Trim('"').Trim("'")
                $envVars[$key] = $value
            }
        }
    }
    return $envVars
}

# Private helper function to show environment status
function Show-EnvironmentStatus {
    $envVars = Get-EnvVariables

    Write-Info "`n=== Current Environment Status ==="

    $tdxUrl = $envVars['TDX_BASE_URL']
    if ($tdxUrl -like "*SB*" -or $tdxUrl -like "*sandbox*") {
        Write-Info "Environment: SANDBOX (testing)" -ForegroundColor Green
    } else {
        Write-Warning "Environment: PRODUCTION (live data!)"
    }

    $sheetName = $envVars['SHEET_NAME']
    Write-Info "Current sheet: $sheetName"

    $spreadsheetId = $envVars['SPREADSHEET_ID']
    if ($spreadsheetId) {
        Write-Info "Spreadsheet ID: $($spreadsheetId.Substring(0, [Math]::Min(10, $spreadsheetId.Length)))..."
    }
}

# Private helper function to validate prerequisites
function Test-Prerequisites {
    $allGood = $true

    # Check if .env exists
    $envFile = Join-Path $script:ProjectRoot ".env"
    if (-not (Test-Path $envFile)) {
        Write-Error "✗ .env file not found at: $envFile"
        $allGood = $false
    } else {
        Write-Success "✓ .env file found"
    }

    # Check credentials.json
    $credentialsPath = Join-Path $script:ProjectRoot "credentials.json"
    if (-not (Test-Path $credentialsPath)) {
        Write-Warning "⚠ credentials.json not found at: $credentialsPath"
        Write-Info "  This is required for Google Sheets access"
    } else {
        Write-Success "✓ credentials.json found"
    }

    # Check virtual environment
    if (-not (Test-Path $script:ActivateScript)) {
        Write-Error "✗ Virtual environment not found at: $script:VenvPath"
        Write-Info "  Please run install.ps1 first"
        $allGood = $false
    } else {
        Write-Success "✓ Virtual environment found"
    }

    return $allGood
}

# Private helper function to walk user through script options
function Get-ScriptOptions {
    param(
        [string]$ScriptName,
        [string]$Description,
        [string]$DefaultLogFile
    )

    Write-Info "`n" + "="*50
    Write-Info "Configuring: $ScriptName"
    Write-Info "Description: $Description"
    Write-Info "="*50

    $options = @()

    # Ask about dry run
    Write-Host "`n🔍 " -NoNewline -ForegroundColor Blue
    Write-Host "Dry Run Mode" -ForegroundColor White
    Write-Host "   This will show you what the script would do without making any actual changes."
    Write-Host "   Recommended for first-time use or testing." -ForegroundColor Gray

    do {
        $dryRunInput = Read-Host "`nEnable dry-run mode? [Y/n]"
        $dryRunInput = $dryRunInput.ToLower()
        if ($dryRunInput -eq "" -or $dryRunInput -eq "y" -or $dryRunInput -eq "yes") {
            $options += "--dry-run"
            Write-Success "✓ Dry-run mode enabled"
            break
        } elseif ($dryRunInput -eq "n" -or $dryRunInput -eq "no") {
            Write-Warning "⚠ Live mode enabled - real changes will be made!"
            break
        } else {
            Write-Warning "Please enter Y, N, or press Enter for default (Y)"
        }
    } while ($true)

    # Ask about logging
    Write-Host "`n📝 " -NoNewline -ForegroundColor Blue
    Write-Host "Logging" -ForegroundColor White
    Write-Host "   Save a detailed log of all actions taken by the script."
    Write-Host "   Useful for troubleshooting and record-keeping." -ForegroundColor Gray

    do {
        $loggingInput = Read-Host "`nEnable logging? [Y/n]"
        $loggingInput = $loggingInput.ToLower()
        if ($loggingInput -eq "" -or $loggingInput -eq "y" -or $loggingInput -eq "yes") {
            Write-Host "`nLog file options:" -ForegroundColor Yellow
            Write-Host "  1. Use default: $DefaultLogFile"
            Write-Host "  2. Specify custom log file name"
            Write-Host "  3. Use timestamped log file"

            do {
                $logChoice = Read-Host "`nChoose log option [1-3]"
                switch ($logChoice) {
                    "1" {
                        $options += "--log"
                        $options += $DefaultLogFile
                        Write-Success "✓ Logging to: $DefaultLogFile"
                        $loggingDone = $true
                        break
                    }
                    "2" {
                        $customLog = Read-Host "Enter custom log file name"
                        if ($customLog) {
                            $options += "--log"
                            $options += $customLog
                            Write-Success "✓ Logging to: $customLog"
                            $loggingDone = $true
                        } else {
                            Write-Warning "Please enter a valid file name"
                        }
                        break
                    }
                    "3" {
                        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
                        $timestampedLog = "$($ScriptName)_$timestamp.log"
                        $options += "--log"
                        $options += $timestampedLog
                        Write-Success "✓ Logging to: $timestampedLog"
                        $loggingDone = $true
                        break
                    }
                    default {
                        Write-Warning "Please choose 1, 2, or 3"
                    }
                }
            } while (-not $loggingDone)
            break
        } elseif ($loggingInput -eq "n" -or $loggingInput -eq "no") {
            Write-Info "✓ Logging disabled"
            break
        } else {
            Write-Warning "Please enter Y, N, or press Enter for default (Y)"
        }
    } while ($true)

    return $options
}

# Private helper function to confirm and execute script
function Invoke-ComplianceScript {
    param(
        [string]$Command,
        [string[]]$Arguments,
        [string]$Description
    )

    Write-Host "`n" + "="*60 -ForegroundColor Cyan
    Write-Host "READY TO EXECUTE" -ForegroundColor Cyan
    Write-Host "="*60 -ForegroundColor Cyan

    Write-Info "Script: $Command"
    Write-Info "Description: $Description"

    if ($Arguments.Count -gt 0) {
        Write-Info "Arguments: $($Arguments -join ' ')"
    } else {
        Write-Info "Arguments: None"
    }

    Write-Host "`nPress Enter to continue or Ctrl+C to cancel..." -ForegroundColor Yellow
    Read-Host

    Write-Info "Executing script..."
    Write-Warning "You can press Ctrl+C at any time to stop the script"
    Write-Host ""

    try {
        Invoke-PythonWithVenv -Command $Command -Arguments $Arguments
        Write-Success "`n✓ Script completed successfully!"
    } catch {
        Write-Error "`n✗ Script failed: $($_.Exception.Message)"
    }

    Write-Host "`nPress Enter to return to menu..." -ForegroundColor Gray
    Read-Host
}

<#
.SYNOPSIS
Generates compliance tickets from specified spreadsheet data.

.DESCRIPTION
Runs the compliance-automator tool to generate all compliance tickets from the configured spreadsheet.

.PARAMETER Arguments
Additional arguments to pass to the compliance-automator command.

.EXAMPLE
Invoke-ComplianceAutomator --help
Invoke-ComplianceAutomator --dry-run --log
#>
function Invoke-ComplianceAutomator {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromRemainingArguments=$true)]
        [string[]]$Arguments
    )

    Write-Info "Running compliance automator..."
    Invoke-PythonWithVenv -Command "compliance-automator" -Arguments $Arguments
}

<#
.SYNOPSIS
Runs ticket second outreach, resending descriptions.

.DESCRIPTION
Updates existing compliance tickets with second outreach messaging.

.PARAMETER Arguments
Additional arguments to pass to the compliance-update command.

.EXAMPLE
Update-Compliance --help
Update-Compliance --dry-run --log
#>
function Update-Compliance {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromRemainingArguments=$true)]
        [string[]]$Arguments
    )

    Write-Info "Running compliance update (second outreach)..."
    Invoke-PythonWithVenv -Command "compliance-update" -Arguments $Arguments
}

<#
.SYNOPSIS
Escalates compliance tickets by adding CAs and sending third outreach.

.DESCRIPTION
Resends descriptions and adds Computing Associates (CAs) to tickets for escalation.

.PARAMETER Arguments
Additional arguments to pass to the compliance-third-outreach command.

.EXAMPLE
Invoke-ComplianceEscalation --help
Invoke-ComplianceEscalation --dry-run --log
#>
function Invoke-ComplianceEscalation {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromRemainingArguments=$true)]
        [string[]]$Arguments
    )

    Write-Info "Running compliance escalation (third outreach with CA notification)..."
    Invoke-PythonWithVenv -Command "compliance-third-outreach" -Arguments $Arguments
}

<#
.SYNOPSIS
Displays an interactive menu for compliance operations.

.DESCRIPTION
Provides a user-friendly menu interface for managing compliance automation tasks.
Auto-detects project path and walks users through script configuration.

.EXAMPLE
Show-ComplianceMenu
#>
function Show-ComplianceMenu {
    [CmdletBinding()]
    param()

    do {
        Clear-Host
        Write-Host "======================================" -ForegroundColor Cyan
        Write-Host "    LSATS Compliance Helper Menu     " -ForegroundColor Cyan
        Write-Host "======================================" -ForegroundColor Cyan

        Write-Host "`nProject Path: " -NoNewline -ForegroundColor Gray
        Write-Host $script:ProjectRoot -ForegroundColor White

        # Show environment status
        Show-EnvironmentStatus

        # Check prerequisites
        Write-Host "`n" + "-"*40 -ForegroundColor Gray
        Write-Host "Environment Check:" -ForegroundColor Yellow

        $prereqsOk = Test-Prerequisites

        if (-not $prereqsOk) {
            Write-Host "`n" + "-"*40 -ForegroundColor Gray
            Write-Warning "⚠ Some prerequisites are missing. Some functions may not work properly."
            Write-Info "Run install.ps1 to fix configuration issues."
        }

        Write-Host "`n======================================" -ForegroundColor White
        Write-Host "Available Scripts:" -ForegroundColor White
        Write-Host "`n1. 🎫 Generate Compliance Tickets (Automator)" -ForegroundColor Cyan
        Write-Host "   Creates new compliance tickets for non-compliant computers"

        Write-Host "`n2. 📧 Send Second Outreach (Update)" -ForegroundColor Cyan
        Write-Host "   Sends follow-up notifications for unresponsive tickets"

        Write-Host "`n3. 🚨 Escalate with CAs (Third Outreach)" -ForegroundColor Cyan
        Write-Host "   Adds Computing Associates and sends escalation notifications"

        Write-Host "`n======================================" -ForegroundColor White
        Write-Host "Other Options:" -ForegroundColor White
        Write-Host "`n4. 🔧 View Environment Configuration" -ForegroundColor Yellow
        Write-Host "5. 🧪 Test Commands (Show Help)" -ForegroundColor Yellow
        Write-Host "`nQ. ❌ Quit" -ForegroundColor Red

        $choice = Read-Host "`nEnter your choice [1-5, Q]"

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
                    Write-Info "Run install.ps1 to configure environment."
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
