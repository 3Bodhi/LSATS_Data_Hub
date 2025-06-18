# LSATS Data Hub Compliance Helper Script
# This script provides easy access to manage and run compliance automation tools

param(
    [string]$ProjectPath = $PWD.Path
)

# Color functions for better output
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }
function Write-Menu { Write-Host $args -ForegroundColor White }

# Global variables
$VenvName = ".venv"
$EnvFile = ".env"

# Function to read .env file and return as hashtable
function Read-EnvFile {
    param($FilePath)

    $envVars = @{}
    if (Test-Path $FilePath) {
        Get-Content $FilePath | ForEach-Object {
            if ($_ -match '^([^#][^=]*)\s*=\s*(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim().Trim('"').Trim("'")
                $envVars[$key] = $value
            }
        }
    }
    return $envVars
}

# Function to write .env file from hashtable
function Write-EnvFile {
    param($FilePath, $EnvVars)

    $content = @()
    foreach ($key in $EnvVars.Keys) {
        $content += "$key = `"$($EnvVars[$key])`""
    }
    Set-Content -Path $FilePath -Value $content
}

# Function to activate virtual environment
function Enable-VirtualEnvironment {
    $activateScript = Join-Path $ProjectPath "$VenvName\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
        return $true
    } else {
        Write-Error "Virtual environment not found at: $ProjectPath\$VenvName"
        Write-Error "Please run the installation script first."
        return $false
    }
}

# Function to test TDX API key validity
function Test-TDXApiKey {
    param($BaseUrl, $ApiKey)

    if (-not $ApiKey -or $ApiKey -eq "#api token from https://teamdynamix.umich.edu/SBTDWebApi/api/auth/loginsso") {
        return $false
    }

    try {
        $headers = @{
            'Authorization' = "Bearer $ApiKey"
            'Content-Type' = 'application/json'
        }

        # Test with a simple API call (get accounts)
        $testUrl = "$BaseUrl//accounts"
        $response = Invoke-RestMethod -Uri $testUrl -Headers $headers -Method Get -TimeoutSec 10
        return $true
    } catch {
        Write-Warning "API key validation failed: $($_.Exception.Message)"
        return $false
    }
}

# Function to verify prerequisites
function Test-Prerequisites {
    $envVars = Read-EnvFile (Join-Path $ProjectPath $EnvFile)
    $allGood = $true

    Write-Info "=== Checking Prerequisites ==="

    # Check if .env exists
    if (-not (Test-Path (Join-Path $ProjectPath $EnvFile))) {
        Write-Error "✗ .env file not found"
        Write-Info "Please run the installation script first or create .env from .env.example"
        return $false
    }

    # Check TDX API key
    $tdxUrl = $envVars['TDX_BASE_URL']
    $tdxToken = $envVars['TDX_API_TOKEN']

    if (-not $tdxUrl) {
        Write-Error "✗ TDX_BASE_URL not configured in .env"
        $allGood = $false
    } elseif (-not (Test-TDXApiKey -BaseUrl $tdxUrl -ApiKey $tdxToken)) {
        Write-Error "✗ TDX API key is invalid or not configured"
        Write-Info "To get a new API token, visit:"
        $loginUrl = $tdxUrl -replace "/api$", "/api/auth/loginsso"
        Write-Host $loginUrl -ForegroundColor Blue

        $response = Read-Host "Would you like to open this URL in your browser? (Y/n)"
        if ($response -ne 'n') {
            Start-Process $loginUrl
        }

        $newToken = Read-Host "Enter your new TDX_API_TOKEN (or press Enter to skip)"
        if ($newToken) {
            $envVars['TDX_API_TOKEN'] = $newToken
            Write-EnvFile (Join-Path $ProjectPath $EnvFile) $envVars

            if (Test-TDXApiKey -BaseUrl $tdxUrl -ApiKey $newToken) {
                Write-Success "✓ New API key validated successfully"
            } else {
                Write-Error "✗ New API key is still invalid"
                $allGood = $false
            }
        } else {
            $allGood = $false
        }
    } else {
        Write-Success "✓ TDX API key is valid"
    }

    # Check credentials.json
    $credentialsPath = Join-Path $ProjectPath "credentials.json"
    if (-not (Test-Path $credentialsPath)) {
        Write-Error "✗ credentials.json not found"
        Write-Info "You need to either:"
        Write-Info "1. Create a Google Cloud project and download credentials:"
        Write-Host "   https://developers.google.com/sheets/api/quickstart/python" -ForegroundColor Blue
        Write-Info "2. Get credentials.json from your script administrator"
        Write-Info "3. Place the file at: $credentialsPath"

        $response = Read-Host "Would you like to open the Google API setup page? (Y/n)"
        if ($response -ne 'n') {
            Start-Process "https://developers.google.com/sheets/api/quickstart/python"
        }

        Read-Host "Press Enter after you have placed credentials.json in the project folder"

        if (Test-Path $credentialsPath) {
            Write-Success "✓ credentials.json found"
        } else {
            Write-Error "✗ credentials.json still not found"
            $allGood = $false
        }
    } else {
        Write-Success "✓ credentials.json found"
    }

    # Check virtual environment
    if (-not (Test-Path (Join-Path $ProjectPath "$VenvName\Scripts\Activate.ps1"))) {
        Write-Error "✗ Virtual environment not found"
        Write-Info "Please run the installation script first"
        $allGood = $false
    } else {
        Write-Success "✓ Virtual environment found"
    }

    return $allGood
}

# Function to show current environment status
function Show-EnvironmentStatus {
    $envVars = Read-EnvFile (Join-Path $ProjectPath $EnvFile)

    Write-Info "`n=== Current Environment Status ==="

    $tdxUrl = $envVars['TDX_BASE_URL']
    if ($tdxUrl -like "*SB*" -or $tdxUrl -like "*sandbox*") {
        Write-Info "Environment: SANDBOX (testing)"
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

# Function to run compliance scripts submenu
function Show-ComplianceScriptsMenu {
    while ($true) {
        Clear-Host
        Write-Host "======================================" -ForegroundColor Cyan
        Write-Host "     Compliance Scripts Menu         " -ForegroundColor Cyan
        Write-Host "======================================" -ForegroundColor Cyan

        Show-EnvironmentStatus

        Write-Menu "`nAvailable Scripts:"
        Write-Menu "1. Compliance Automator (Create tickets)"
        Write-Menu "2. Second Outreach (Follow-up notifications)"
        Write-Menu "3. Third Outreach (CA notifications)"
        Write-Menu "4. Back to main menu"

        $choice = Read-Host "`nEnter your choice (1-4)"

        $scriptCmd = $null
        $defaultLog = $null

        switch ($choice) {
            "1" {
                $scriptCmd = "compliance-automator"
                $defaultLog = "compliance_automator.log"
                $description = "This script creates compliance tickets for non-compliant computers"
            }
            "2" {
                $scriptCmd = "compliance-update"
                $defaultLog = "compliance_update.log"
                $description = "This script sends second outreach notifications for non-responsive tickets"
            }
            "3" {
                $scriptCmd = "compliance-third-outreach"
                $defaultLog = "compliance_third_outreach.log"
                $description = "This script sends third outreach with CA notifications"
            }
            "4" {
                return
            }
            default {
                Write-Warning "Invalid choice. Please try again."
                Start-Sleep 2
                continue
            }
        }

        if ($scriptCmd) {
            Write-Info "`n$description"

            # Ask about dry run
            $dryRun = Read-Host "`nRun in dry-run mode (recommended for testing)? (Y/n)"
            $dryRunFlag = if ($dryRun -ne 'n') { " --dry-run" } else { "" }

            # Ask about logging
            $logging = Read-Host "Enable logging? (Y/n)"
            $logFlag = ""
            if ($logging -ne 'n') {
                $logFile = Read-Host "Log file name (press Enter for default: $defaultLog)"
                if (-not $logFile) {
                    $logFile = $defaultLog
                }
                $logFlag = " --log `"$logFile`""
            }

            # Construct and run command
            $fullCommand = "$scriptCmd$dryRunFlag$logFlag"

            Write-Info "`nRunning: $fullCommand"
            Write-Warning "Press Ctrl+C to cancel if needed"
            Read-Host "Press Enter to continue"

            # Activate virtual environment and run command
            if (Enable-VirtualEnvironment) {
                Invoke-Expression $fullCommand
            }

            Write-Info "`nScript completed. Press Enter to continue..."
            Read-Host
        }
    }
}

# Function to switch environment
function Switch-Environment {
    $envVars = Read-EnvFile (Join-Path $ProjectPath $EnvFile)
    $currentUrl = $envVars['TDX_BASE_URL']

    Write-Info "`n=== Switch Environment ==="
    $isSandbox = $currentUrl -like "*SB*" -or $currentUrl -like "*sandbox*"

    if ($isSandbox) {
        Write-Info "Currently in: SANDBOX"
        Write-Warning "Switch to PRODUCTION? (Real tickets will be created!)"
        $confirm = Read-Host "Type 'PRODUCTION' to confirm"

        if ($confirm -eq 'PRODUCTION') {
            $envVars['TDX_BASE_URL'] = "https://teamdynamix.umich.edu/TDWebApi/api"
            # Clear the API token as it will be different for production
            $envVars['TDX_API_TOKEN'] = ""
            Write-Success "Switched to PRODUCTION environment"
            Write-Warning "You will need to get a new API token for production!"
        }
    } else {
        Write-Info "Currently in: PRODUCTION"
        $confirm = Read-Host "Switch to SANDBOX? (Y/n)"

        if ($confirm -ne 'n') {
            $envVars['TDX_BASE_URL'] = "https://teamdynamix.umich.edu/SBTDWebApi/api"
            # Clear the API token as it will be different for sandbox
            $envVars['TDX_API_TOKEN'] = ""
            Write-Success "Switched to SANDBOX environment"
            Write-Info "You will need to get a new API token for sandbox!"
        }
    }

    Write-EnvFile (Join-Path $ProjectPath $EnvFile) $envVars
    Read-Host "Press Enter to continue"
}

# Function to change sheet name
function Change-SheetName {
    $envVars = Read-EnvFile (Join-Path $ProjectPath $EnvFile)

    Write-Info "`n=== Change Sheet Name ==="
    Write-Info "Current sheet name: $($envVars['SHEET_NAME'])"

    $newSheetName = Read-Host "Enter new sheet name"
    if ($newSheetName) {
        $envVars['SHEET_NAME'] = $newSheetName
        Write-EnvFile (Join-Path $ProjectPath $EnvFile) $envVars
        Write-Success "Sheet name updated to: $newSheetName"
    }

    Read-Host "Press Enter to continue"
}

# Function to safely update environment file with regex patterns
function Update-EnvContent {
    param($FilePath, $Updates)

    $content = Get-Content $FilePath -Raw

    foreach ($update in $Updates.GetEnumerator()) {
        $pattern = $update.Key
        $replacement = $update.Value
        $content = $content -replace $pattern, $replacement
    }

    Set-Content -Path $FilePath -Value $content
}

# Function to completely reconfigure environment (reuse from install script)
function Complete-Reconfiguration {
    Write-Warning "`n=== Complete Environment Reconfiguration ==="
    Write-Warning "This will reset all environment variables!"

    $confirm = Read-Host "Are you sure? (Y/n)"
    if ($confirm -eq 'n') {
        return
    }

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
    Write-Info "`nHINT: This is the section after the /spreadsheets/d/ part in the URL."
    $spreadsheetId = Read-Host "Enter SPREADSHEET_ID"

    # Fix the date interpolation issue
    $currentDate = (Get-Date).ToString("MMMM")
    $defaultSheetPrompt = "Enter SHEET_NAME (e.g., " + $currentDate + ")"
    $sheetName = Read-Host $defaultSheetPrompt

    # Create updates hashtable for safe regex replacement
    $envPath = Join-Path $ProjectPath $EnvFile
    $updates = @{}

    # Use simpler regex patterns
    $updates['TDX_BASE_URL = ".*"'] = "TDX_BASE_URL = `"$tdxUrl`""

    if ($tdxToken) {
        $updates['TDX_API_TOKEN = .*'] = "TDX_API_TOKEN = `"$tdxToken`""
    }
    if ($spreadsheetId) {
        $updates['SPREADSHEET_ID = .*'] = "SPREADSHEET_ID = `"$spreadsheetId`""
    }
    if ($sheetName) {
        $updates['SHEET_NAME = .*'] = "SHEET_NAME = `"$sheetName`""
    }

    # Apply updates
    Update-EnvContent -FilePath $envPath -Updates $updates
    Write-Success ".env file completely reconfigured!"

    Read-Host "Press Enter to continue"
}

# Main menu function
function Show-MainMenu {
    while ($true) {
        Clear-Host
        Write-Host "======================================" -ForegroundColor Cyan
        Write-Host "   LSATS Compliance Helper Script    " -ForegroundColor Cyan
        Write-Host "======================================" -ForegroundColor Cyan

        Show-EnvironmentStatus

        Write-Menu "`nOptions:"
        Write-Menu "1. Run Compliance Scripts"
        Write-Menu "2. Switch Environment (Sandbox/Production)"
        Write-Menu "3. Change Sheet Name"
        Write-Menu "4. Complete Environment Reconfiguration"
        Write-Menu "5. Recheck Prerequisites"
        Write-Menu "6. Exit"

        $choice = Read-Host "`nEnter your choice (1-6)"

        switch ($choice) {
            "1" { Show-ComplianceScriptsMenu }
            "2" { Switch-Environment }
            "3" { Change-SheetName }
            "4" { Complete-Reconfiguration }
            "5" {
                Write-Info "Rechecking prerequisites..."
                Test-Prerequisites | Out-Null
                Read-Host "Press Enter to continue"
            }
            "6" {
                Write-Info "Goodbye!"
                exit 0
            }
            default {
                Write-Warning "Invalid choice. Please try again."
                Start-Sleep 2
            }
        }
    }
}

# Main execution
function Main {
    # Change to project directory
    Set-Location $ProjectPath

    # Check if we are in the right directory
    if (-not (Test-Path "setup.py")) {
        Write-Error "This does not appear to be the LSATS Data Hub project directory."
        Write-Error "Please run this script from the project root directory."
        exit 1
    }

    Write-Info "LSATS Data Hub Compliance Helper"
    Write-Info "Project Path: $ProjectPath"

    # Check prerequisites
    if (-not (Test-Prerequisites)) {
        Write-Error "`nPrerequisites not met. Please fix the issues above and run the script again."
        Read-Host "Press Enter to exit"
        exit 1
    }

    # Activate virtual environment
    if (-not (Enable-VirtualEnvironment)) {
        exit 1
    }

    Write-Success "`nAll prerequisites met! Starting main menu..."
    Start-Sleep 2

    # Show main menu
    Show-MainMenu
}

# Run the script
try {
    Main
} catch {
    Write-Error "An error occurred: $_"
    Read-Host "Press Enter to exit"
    exit 1
}
