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

## Setup
For detailed installation instructions, please see [INSTALL.md](INSTALL.md).

### Quick Start
```bash
# Clone the repository
git clone https://github.com/yourusername/lsats-data-hub.git
cd lsats-data-hub

# Install the package
pip install .

# For development mode (changes to source are immediately available)
pip install -e .
```

### Configuration
Create a `.env` file from the `.env.example` file to configure your environment variables.

#### TeamDynamix Setup
TeamDynamix uses an API token you can receive from the [/loginsso endpoint](https://teamdynamix.umich.edu/TDWebApi/). For testing, you can use the [sandbox API endpoint](https://teamdynamix.umich.edu/SBTDWebApi/).

#### Google API Setup
A credentials.json file and OAuth setup is required to access the Google Sheets API. You can follow the directions [here](https://developers.google.com/sheets/api/quickstart/python) for a quickstart.

The SHEET_ID and SUB_SHEET_ID can be easily found in the URL, which follows the format:
'https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit?gid=SUB_SHEET_ID#gid=SUB_SHEET_ID'. You can use the plaintext name for the SHEET_NAME variable.

## Examples
The repository contains several example scripts demonstrating the use of the library:

### create_LabNote.py
Creates a Configuration Item (CI) in TeamDynamix for a PI's lab. It aggregates:
- Assets owned by the PI
- Tickets where the PI is the requestor

Future enhancements will include integration with MCommunity/AD to find lab members and their related tickets/assets.

```bash
# After installation, you can run it with:
create-lab-note <uniqname>
```

### example_tdx_sheet_post.py
Shows how to combine multiple data sources with AI capabilities to generate insights:
- Extracts requestor information and compares it to Google Sheet data
- Uses AI to generate personalized outreach emails
- Posts information back to TeamDynamix tickets

## Production Scripts

### Computer Compliance Management
The following scripts are currently in production use for managing computer compliance:

#### compliance_ticket_automator.py
Creates tickets in TeamDynamix for computers requiring compliance updates:
- Processes spreadsheet data of non-compliant computers
- Automatically creates tickets for affected users
- Associates relevant computer assets with tickets
- Builds detailed notifications with compliance information

#### compliance_ticket_second_outreach.py
Handles follow-up communications for compliance tickets:
- Identifies tickets with no response after first outreach
- Automatically sends second notification to users
- Updates tracking spreadsheet with response statuses
- Handles various ticket statuses (Resolved, Cancelled, etc.)


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
