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
This project contains folders for each data source which can be imported into a project
to provide access to its underlying facade or service. Options are currently limited to
TeamDynamix and the Google Sheets API. Create a .env file from the .env.example file
to get thing running.

A credentials.json file and Oauth setup is required to access
the google sheets API. You can follow the directions [here](https://developers.google.com/sheets/api/quickstart/python) for a quickstart.
the whole document SHEET_ID and SUB_SHEET_ID and can be easily found in the url which breaks down to
'https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit?gid=SUB_SHEET_ID#gid=SUB_SHEET_ID'. You can
use the plaintext name for the SHEET_NAME variable.

Teamdynamix uses an api token you can receive from the [/loginsso endpoint](https://teamdynamix.umich.edu/TDWebApi/).
You can use the plaintext of the name of the sub-sheet for sheet name.
the [sandbox api endpoint](https://teamdynamix.umich.edu/SBTDWebApi/) for testing.

## Example files
Currently this repo contains two examples file of potential uses
for this library, create_LabNote.py and example_tdx_sheet_post.py.

### create_LabNote.py
create_labNote.py takes a the uniqname of a PI and returns a CI for their lab.
Currently it returns all assets in which they are owner and all tickets in
which they are requestor. MCommunity/AD integrations will allow for a more
accurate set of tickets and assets by being able to loop in tickets from
lab coordinators/members found in groups named after the PI. As the TDX API
is fleshed out, KB articles for the labs as well as generated attachements will
also be added.

### example_tdx_sheet_post.py
This file is an potential exampe use case of orchestrating multiple data
sources to make an insight more readily available to our customers or end users.
A script such as this could watch a ticket queue. When a ticket comes in,
it will take the ticket_ID, obtain the requestor's email and compare it to the
'The List' Google Sheet to see if the user has any problem computers. In then
feeds a small dataframe cut from this sheet to an LLM to generate an outreach email
that is then posted into the TDX ticket. This AI prompt could be tuned
for the requestor or the tech, providing them insight on the User's ticket history,
lab environment, problem computers, potential fixes, etc.

In this program, I serve use an AI using the openai Api and a LM Studio locally hosted AI.
Install LM-Studio and start a server. Modify your .env to the correct port and enpoint you set
and model you chose. Model must alos be changed in the completion function.

## ROADMAP
1. Complete TDX API ADAPTER for all TDX API calls
2. Set create_lab to pull both financial owner and primary user for asset_list
3. Add Mcommunity API adapter
4. Add and refactor Sheets API adapter.
5. Improve Lab_Note creation.
    - add 'clif note'-like attachemt files to describe lab
    - Fine tune asset/ticket ingestion.
5. Add More Data sources.
    - Active Directory
    - Tenable/ThreatDown
    - Finance API
    - KeyServer
    - Izzy/Jamf
    - Other Google Workspace tools (docs, gmail, etc)
    - etc
6. Build SSO_Manager to better handle authentication.
7. Create cache/graphql database to improve performance/reduce query load.
