# LSATS Data Hub
## Introduction
LSATS Data Hub is a set of python modules designed to simplify
complex and interlocking queries of LSA Technology Services data sources. Its purpose
is to make it easier for end users to cross reference data and generate insights from
the LSATS myriad of Data sources such as Google Workspace, Active Directory, TeamDynamix,
MCommunity, etc. The packages are designed in an adapter-facade-service paradigm. API adapters
wrap API requests from a specific web applications to simplify API calls. Facades organize
a single APIs functions and provide higher level functions utilizing multiple primitive API
requests. Services orchestrate complex functions that use resources from several facades. This
paradigm should help keep the codebase modular and maintainable as the organization cycles through
data sources and data sources update.

# Setup

## Quick Start
### Windows
#### Clone the repository/download the project.
Note: Place the project folder somewhere safe where it isn't likely to get deleted.
```powershell
git clone https://github.com/3Bodhi/LSATS_Data_Hub.git
cd lsats_data_hub
```
<<<<<<< HEAD
# OPTIONAL but recommended: Create a virtual environment
=======
Alternatively, download the script [here](https://github.com/3Bodhi/LSATS_Data_Hub/archive/refs/heads/main.zip) or click 'Download zip' from the green 'Code' button at the top. Extract the zip and then navigate into the folder.
#### Run the installer
If you are already in an elevated powershell window simply run `install.ps1`. If you are in File Explorer, make sure you are in the project folder, right click and select 'Open in Terminal'. Key in 'i' in the terminal and press the tab key until you see `.\install.ps1` and press enter.

The installer will guide you through the process of installing the compliance automation scripts, installing Python, verifying the project folder exists and configuring the .env file. Lastly, it adds the compliance automation commands to be used from anywhere on the command prompt. If you do not have a credentials.json file provided to you, jump to [Google API Setup](#Google-API-Setup)

Note: If you choose to have the script install Python for you, you will need to close your terminal and restart the script.
Info: You can manually configure the .env file by opening it in Notepad or your favorite Text Editor.

### MacOS/Linux

#### Clone the repository/download the project.
Note: Place the project folder somewhere safe where it isn't likely to get deleted.
```powershell
git clone https://github.com/3Bodhi/LSATS_Data_Hub.git
cd lsats_data_hub
```

#### OPTIONAL but recommended: Create a virtual environment
>>>>>>> f1ef6c5 (Updated README for installer script.)
```bash
python -m venv .venv
source .venv/bin/activate
```
#### Install the package
```bash
pip install .
```

## Configuration
Create a `.env` file from the `.env.example` file to configure your environment variables. On Windows The install Powershell script will have already done this for you.

#### TeamDynamix Setup
TeamDynamix uses an API token you can receive from the [/loginsso endpoint](https://teamdynamix.umich.edu/TDWebApi/). For testing, you can use the [sandbox API endpoint](https://teamdynamix.umich.edu/SBTDWebApi/).

#### Google API Setup
A credentials.json file and OAuth setup is required to access the Google Sheets API. You can follow the directions [here](https://developers.google.com/sheets/api/quickstart/python) for a quickstart.

Place the credentials.json file in the base project directory.

#### Sheet Setup
The SHEET_ID and SUB_SHEET_ID can be easily found in the URL, which follows the format:
'https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit?gid=SUB_SHEET_ID#gid=SUB_SHEET_ID'. You can use the plaintext name for the SHEET_NAME variable. Be aware of case and extra spaces!

<<<<<<< HEAD
### Computer Compliance Management
#### Compliance Helper
The installed script, `compliance-helper`
#### Directly Invoke Complaince Commands
The following scripts are currently in production use for managing computer compliance.
They can be called using python or, once installed, directly from anywhere in your shell.
call file directly:
```bash
python3 /scripts/compliance/compliance_ticket_automator.py --log
=======
They can be called using python or, once installed, directly from anywhere in your shell.

Call from the command line:
```bash
compliance-automator --help
```
```bash
compliance-update --dry-run
```
```bash
compliance-third-outreach --dry-run --log
```
Call scripts directly:
```bash
python3 /scripts/compliance/compliance_ticket_automator.py --dry-run --log
python3 /scripts/compliance/compliance_ticket_second_outreach.py --log --dry-run
python3 /scripts/compliance/compliance_ticket_third_outreach.py --dry-run
>>>>>>> Stashed changes
=======
### Using Computer Compliance Management Scripts
The following scripts are currently in production use for managing computer compliance.
They can be called using python or, once installed, directly from anywhere in your shell.

Call from the command line:
```bash
compliance-automator --help
compliance-update --dry-run
compliance-third-outreach --dry-run --log
```
Call scripts directly:
```bash
python3 /scripts/compliance/compliance_ticket_automator.py --log
python3 /scripts/compliance/compliance_ticket_second_outreach.py --log --dry-run
python3 /scripts/compliance/compliance_ticket_third_outreach.py --dry-run
>>>>>>> f1ef6c5 (Updated README for installer script.)
```

All commands have --help flag you can run to see the available flag options.

#### compliance_ticket_automator.py | compliance-automator
Creates tickets in TeamDynamix for computers requiring compliance updates:
- Processes spreadsheet data of non-compliant computers
- Automatically creates tickets for affected users
- Associates relevant computer assets with tickets
- Builds detailed notifications with compliance information

#### compliance_ticket_second_outreach.py | compliance-update
Handles follow-up communications for compliance tickets:
- Identifies tickets with no response (still awaiting input) after first outreach
- Automatically sends second notification to users

#### compliance_ticket_third_outreach.py | compliance-third-outreach
Handles follow-up communications for compliance tickets:
- Identifies tickets with no response (still awaiting input) after first outreach
- Adds Chief Administrato to ticket
- Automatically sends notification to user and CA.


## ROADMAP
1. ✅ TeamDynamix API adapter for key TDX API calls
2. ✅ Basic Google Sheets integration
3. ⬜ MCommunity API adapter
4. ⬜ Enhance Lab_Note creation
   - Add 'clif note'-like attachment files to describe lab
   - Incorporate lab members from MCommunity groups
5. ⬜ Expand data source integrations:
   - Active Directory
   - Tenable/ThreatDown
   - Finance API
   - KeyServer
   - Izzy/Jamf
   - Additional Google Workspace tools (docs, gmail, etc)
6. ⬜ Build SSO_Manager to better handle authentication
7. ⬜ Create cache/GraphQL database to improve performance/reduce query load
8. ⬜ Add comprehensive test suite
9. ⬜ Create user-friendly documentation site
