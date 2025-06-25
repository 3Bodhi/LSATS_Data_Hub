# LSATS Data Hub Compliance Helper PowerShell Module
# Provides native PowerShell access to compliance automation tools

# Module variables
$script:ModuleRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:ProjectRoot = Split-Path -Parent (Split-Path -Parent $script:ModuleRoot)
$script:VenvPath = Join-Path $script:ProjectRoot ".venv"
$script:ActivateScript = Join-Path $script:VenvPath "Scripts\Activate.ps1"

# Color functions for consistent output
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

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
        Write-Host "`n1. Generate Compliance Tickets (Automator)" -ForegroundColor Cyan
        Write-Host "2. Send Second Outreach (Update)" -ForegroundColor Cyan
        Write-Host "3. Escalate with CAs (Third Outreach)" -ForegroundColor Cyan
        Write-Host "`n4. View Environment Configuration" -ForegroundColor Yellow
        Write-Host "5. Test Commands (Help)" -ForegroundColor Yellow
        Write-Host "`nQ. Quit" -ForegroundColor Gray
        Write-Host "======================================" -ForegroundColor White

        $choice = Read-Host "`nEnter your choice"

        switch ($choice.ToUpper()) {
            "1" {
                Write-Host "`nRunning Compliance Automator..." -ForegroundColor Green
                Write-Host "Add --dry-run to test without making changes" -ForegroundColor Yellow
                $args = Read-Host "Enter arguments (or press Enter for default)"

                if ([string]::IsNullOrWhiteSpace($args)) {
                    Invoke-ComplianceAutomator
                } else {
                    Invoke-ComplianceAutomator ($args -split ' ')
                }

                Read-Host "`nPress Enter to continue"
            }

            "2" {
                Write-Host "`nRunning Second Outreach..." -ForegroundColor Green
                Write-Host "Add --dry-run to test without making changes" -ForegroundColor Yellow
                $args = Read-Host "Enter arguments (or press Enter for default)"

                if ([string]::IsNullOrWhiteSpace($args)) {
                    Update-Compliance
                } else {
                    Update-Compliance ($args -split ' ')
                }

                Read-Host "`nPress Enter to continue"
            }

            "3" {
                Write-Host "`nRunning Third Outreach with CA Escalation..." -ForegroundColor Green
                Write-Host "Add --dry-run to test without making changes" -ForegroundColor Yellow
                $args = Read-Host "Enter arguments (or press Enter for default)"

                if ([string]::IsNullOrWhiteSpace($args)) {
                    Invoke-ComplianceEscalation
                } else {
                    Invoke-ComplianceEscalation ($args -split ' ')
                }

                Read-Host "`nPress Enter to continue"
            }

            "4" {
                Clear-Host
                Write-Host "======================================" -ForegroundColor Cyan
                Write-Host "    Environment Configuration        " -ForegroundColor Cyan
                Write-Host "======================================" -ForegroundColor Cyan

                $envVars = Get-EnvVariables

                if ($envVars.Count -gt 0) {
                    Write-Host "`nCurrent .env settings:" -ForegroundColor Yellow
                    foreach ($key in $envVars.Keys | Sort-Object) {
                        $value = $envVars[$key]
                        if ($key -like "*PASSWORD*" -or $key -like "*SECRET*" -or $key -like "*TOKEN*") {
                            $value = "***HIDDEN***"
                        }
                        Write-Host "  $key = $value" -ForegroundColor White
                    }
                } else {
                    Write-Host "`nNo .env file found or file is empty." -ForegroundColor Red
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
