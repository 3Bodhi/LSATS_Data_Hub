# Security and Analytics Guide for LSATS Data Hub

This guide covers security best practices for database traffic encryption, Kerberos authentication integration, and end-user analytics options for managers.

## Table of Contents
1. [Database Traffic Encryption](#database-traffic-encryption)
2. [Kerberos Authentication Integration](#kerberos-authentication-integration)
3. [Manager Analytics UI Options](#manager-analytics-ui-options)

---

## Database Traffic Encryption

### Current State

The LSATS Data Hub PostgreSQL database currently runs without SSL/TLS encryption. This is acceptable for the current deployment scenario where:
- Database and ingestion scripts run on the same VM
- Traffic uses localhost (loopback interface) or Docker bridge network
- No network packets traverse physical network interfaces
- VM disk encryption protects data at rest

### When Encryption is Required

SSL/TLS encryption becomes **mandatory** when:
- Exposing database to remote connections (non-localhost)
- Allowing remote access to pgAdmin web interface
- Database server and application servers are on different VMs
- Compliance requirements mandate encryption in transit

### Implementation: SSL/TLS with Internal PKI

#### Why Internal PKI (Not Let's Encrypt or Self-Signed)

**Use University of Michigan's Internal Certificate Authority:**
- UMich has Active Directory Certificate Services (AD CS) infrastructure
- All domain-joined machines already trust the UMich CA root certificate
- Supports certificate lifecycle management (renewal, revocation via OCSP/CRL)
- Integrated with Active Directory for automated enrollment
- No external dependencies or public DNS requirements

**Avoid Let's Encrypt for internal services:**
- Requires publicly accessible DNS records
- 90-day expiration requires automation
- No integration with Active Directory
- External dependency for internal infrastructure

**Avoid self-signed certificates:**
- No revocation mechanism if compromised
- No centralized auditing
- Manual distribution overhead
- Each service creates separate trust chains

#### Step 1: Request Certificates from UMich PKI

Contact UMich ITS or LSA IT PKI team to request:
- **Server certificate** for PostgreSQL: `vm-hostname.umich.edu`
- **Server certificate** for pgAdmin: `pgadmin.lsa.umich.edu` (if exposing remotely)
- **Certificate format:** PEM-encoded (`.crt` and `.key` files)
- **Include Subject Alternative Names (SANs)** if multiple hostnames needed

Typical request includes:
```
Common Name (CN): vm-hostname.umich.edu
Organization (O): LSA Technology Services
Organizational Unit (OU): LSATS
SAN: DNS:vm-hostname.umich.edu, DNS:lsats-db.lsa.umich.edu
Key Type: RSA 2048-bit or higher
Validity: 1-2 years (standard UMich policy)
```

#### Step 2: Configure PostgreSQL with SSL

**Update `docker-compose.yml`:**

```yaml
services:
  lsats-postgres:
    image: postgres:15-alpine
    container_name: lsats-database
    environment:
      POSTGRES_DB: lsats_db
      POSTGRES_USER: lsats_user
      POSTGRES_PASSWORD: ${DB_PASSWORD:-lsats_dev_password}
      POSTGRES_INITDB_ARGS: "--auth-host=scram-sha-256"  # Stronger auth than md5
    volumes:
      - lsats_postgres_data:/var/lib/postgresql/data
      - ./docker/postgres/init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
      - ./docker/postgres/schemas.sql:/docker-entrypoint-initdb.d/02-schemas.sql:ro
      - ./docker/postgres/views:/docker-entrypoint-initdb.d/views:ro
      
      # SSL certificate files from UMich PKI
      - ./docker/postgres/certs/server.crt:/var/lib/postgresql/server.crt:ro
      - ./docker/postgres/certs/server.key:/var/lib/postgresql/server.key:ro
      - ./docker/postgres/certs/umich-ca.crt:/var/lib/postgresql/ca.crt:ro
    
    command: >
      postgres
      -c ssl=on
      -c ssl_cert_file=/var/lib/postgresql/server.crt
      -c ssl_key_file=/var/lib/postgresql/server.key
      -c ssl_ca_file=/var/lib/postgresql/ca.crt
      -c ssl_min_protocol_version=TLSv1.2
    
    ports:
      - "5432:5432"
    networks:
      - lsats_network
```

**Update connection strings in `.env`:**

```bash
# For remote connections (requires SSL)
DATABASE_URL=postgresql://lsats_user:password@vm-hostname.umich.edu:5432/lsats_db?sslmode=require

# For local ingestion scripts on same VM (can use localhost without SSL overhead)
DATABASE_URL=postgresql://lsats_user:password@localhost:5432/lsats_db
```

**SSL modes explained:**
- `sslmode=disable` - No SSL (current state, only safe for localhost)
- `sslmode=require` - Require SSL, but don't verify certificate
- `sslmode=verify-ca` - Require SSL and verify certificate signed by trusted CA
- `sslmode=verify-full` - Require SSL, verify CA, and verify hostname matches certificate

#### Step 3: Configure pgAdmin with HTTPS

**Update `docker-compose.yml`:**

```yaml
  lsats-pgadmin:
    image: dpage/pgadmin4:latest
    container_name: lsats-pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: myodhes@umich.edu
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_PASSWORD:-admin}
      PGADMIN_LISTEN_PORT: 443
      PGADMIN_ENABLE_TLS: "True"
    volumes:
      - lsats_pgadmin_data:/var/lib/pgadmin
      
      # SSL certificates for HTTPS
      - ./docker/pgadmin/certs/server.crt:/certs/server.cert:ro
      - ./docker/pgadmin/certs/server.key:/certs/server.key:ro
    
    ports:
      - "8443:443"  # HTTPS instead of HTTP
    depends_on:
      lsats-postgres:
        condition: service_healthy
    networks:
      - lsats_network
    profiles:
      - tools
```

### Alternative: SSH Tunnel (Easiest for Remote Access)

Instead of exposing pgAdmin publicly with SSL, use SSH tunneling:

**Update `docker-compose.yml` to bind localhost only:**

```yaml
  lsats-pgadmin:
    ports:
      - "127.0.0.1:8080:80"  # Only accessible from localhost
```

**Users create SSH tunnel:**

```bash
# From user's local machine
ssh -L 8080:localhost:8080 uniqname@vm-hostname.umich.edu

# Then access pgAdmin at http://localhost:8080
# All traffic encrypted through SSH tunnel
```

**Benefits:**
- No certificate management needed for pgAdmin
- PostgreSQL doesn't need SSL for pgAdmin connection (uses Docker network)
- Leverages existing VM SSH access controls
- All traffic encrypted via SSH

**Recommended architecture:**
- Ingestion scripts → localhost → PostgreSQL (no SSL overhead)
- pgAdmin container → Docker network → PostgreSQL (no SSL overhead)
- Remote users → SSH tunnel → pgAdmin (encrypted via SSH)

---

## Kerberos Authentication Integration

Kerberos provides password-less authentication for domain-joined Windows machines using Active Directory tickets.

### Overview of UMich Authentication Architecture

**Two Separate Authentication Systems:**

1. **On-Premises Active Directory (Kerberos)**
   - Domain-joined Windows computers
   - Uses Windows SSPI (Security Support Provider Interface)
   - Authenticates against UMich AD domain controllers
   - Provides Kerberos tickets for internal resources
   - Used for: File shares, internal SQL databases, domain applications

2. **Microsoft 365 / Azure AD (OAuth 2.0)**
   - Cloud-based identity service
   - Uses OAuth tokens and SAML federation
   - Integrated with Shibboleth SSO portal
   - Used for: Office 365, Canvas, Google Workspace

**Azure AD Connect** likely syncs on-prem AD to cloud, enabling single sign-on between both systems.

### Kerberos for PostgreSQL Connections

#### Use Case: Ingestion Scripts and Database Clients

**Benefits:**
- No passwords in connection strings
- Uses Windows domain authentication tickets
- Strong mutual authentication
- Centralized access control via Active Directory groups

#### Step 1: Request Service Principal and Keytab

Contact UMich Active Directory administrators to:

1. **Create service account** for PostgreSQL:
   ```
   Account name: postgres-lsats-service
   UPN: postgres-lsats-service@umich.edu
   Purpose: PostgreSQL database service authentication
   ```

2. **Register Service Principal Name (SPN):**
   ```powershell
   setspn -A postgres/vm-hostname.umich.edu postgres-lsats-service
   setspn -A postgres/lsats-db.lsa.umich.edu postgres-lsats-service
   ```

3. **Generate keytab file:**
   ```powershell
   ktutil
   addent -password -p postgres/vm-hostname.umich.edu@UMICH.EDU -k 1 -e aes256-cts-hmac-sha1-96
   wkt /path/to/postgres.keytab
   ```

   Request keytab file from AD administrators.

#### Step 2: Configure PostgreSQL for Kerberos

**Update `docker-compose.yml`:**

```yaml
  lsats-postgres:
    volumes:
      # Add Kerberos configuration
      - ./docker/postgres/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./docker/postgres/kerberos/postgres.keytab:/etc/postgresql/postgres.keytab:ro
    
    environment:
      # Kerberos configuration
      KRB5_KTNAME: /etc/postgresql/postgres.keytab
    
    command: >
      postgres
      -c ssl=on
      -c ssl_cert_file=/var/lib/postgresql/server.crt
      -c ssl_key_file=/var/lib/postgresql/server.key
      -c krb_server_keyfile=/etc/postgresql/postgres.keytab
      -c krb_caseins_users=on
```

**Create `docker/postgres/kerberos/krb5.conf`:**

```ini
[libdefaults]
    default_realm = UMICH.EDU
    dns_lookup_realm = false
    dns_lookup_kdc = true
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    
[realms]
    UMICH.EDU = {
        kdc = kerberos.umich.edu
        admin_server = kerberos.umich.edu
    }

[domain_realm]
    .umich.edu = UMICH.EDU
    umich.edu = UMICH.EDU
```

**Create `docker/postgres/pg_hba.conf` (mount as volume):**

```conf
# TYPE  DATABASE    USER        ADDRESS         METHOD

# Kerberos authentication for domain users
hostgssenc  all     all         0.0.0.0/0       gss include_realm=0 krb_realm=UMICH.EDU

# Fallback to password authentication over SSL
hostssl     all     all         0.0.0.0/0       scram-sha-256

# Local connections (for Docker containers)
host        all     all         172.16.0.0/12   scram-sha-256
```

**Mount pg_hba.conf in docker-compose.yml:**

```yaml
volumes:
  - ./docker/postgres/pg_hba.conf:/var/lib/postgresql/data/pg_hba.conf
```

#### Step 3: Configure Python Scripts for Kerberos

**Install required packages:**

```bash
pip install psycopg2-binary  # Ensure GSSAPI support compiled in
```

**Update connection string in `.env`:**

```bash
# Kerberos connection (no password needed)
DATABASE_URL=postgresql://myodhes@vm-hostname.umich.edu/lsats_db?gssencmode=require

# User's Kerberos ticket automatically used
# No password in connection string
```

**Python script usage (no code changes needed):**

```python
from database.adapters.postgres_adapter import create_postgres_adapter

# DATABASE_URL environment variable used
# User's Kerberos ticket from Windows login automatically applied
db_adapter = create_postgres_adapter()

# Connection succeeds without password prompt
df = db_adapter.query_to_dataframe("SELECT * FROM silver.users LIMIT 10")
```

**Verify Kerberos ticket on Windows:**

```powershell
# Check current Kerberos tickets
klist

# Should show:
# Ticket cache: LSA
# Default principal: myodhes@UMICH.EDU
#
# Valid starting     Expires            Service principal
# 01/21/26 08:00:00  01/21/26 18:00:00  krbtgt/UMICH.EDU@UMICH.EDU
```

#### Step 4: Access Control via Active Directory Groups

**Create AD security groups for database access:**

```
Group: LSATS-DB-Admins
Members: myodhes, other_admin

Group: LSATS-DB-ReadOnly
Members: analyst1, analyst2, manager1
```

**Map AD groups to PostgreSQL roles:**

```sql
-- Create roles matching AD groups
CREATE ROLE "LSATS-DB-Admins" WITH LOGIN;
GRANT ALL PRIVILEGES ON SCHEMA bronze, silver, gold TO "LSATS-DB-Admins";

CREATE ROLE "LSATS-DB-ReadOnly" WITH LOGIN;
GRANT USAGE ON SCHEMA silver, gold TO "LSATS-DB-ReadOnly";
GRANT SELECT ON ALL TABLES IN SCHEMA silver, gold TO "LSATS-DB-ReadOnly";

-- Individual users inherit group permissions
-- When myodhes@UMICH.EDU connects, PostgreSQL checks AD group membership
```

**Configure group mapping in `pg_ident.conf`:**

```conf
# MAPNAME       SYSTEM-USERNAME                 PG-USERNAME
kerb            /^(.*)@UMICH\.EDU$              \1
```

### Kerberos for API Service Accounts

#### Use Case: Automated Scripts Running as Service Accounts

For scripts running as Windows service or scheduled task using service account:

**Step 1: Create dedicated service account**

Request from UMich IT:
```
Account: lsats-ingest-svc
Purpose: Automated data ingestion scripts
Password: (long random password, never expires)
```

**Step 2: Register SPN for service account**

```powershell
setspn -A postgres/vm-hostname.umich.edu lsats-ingest-svc
```

**Step 3: Run script as service account**

**Option A: Windows Scheduled Task**
```
Task Scheduler → Create Task
General → Run as: UMICH\lsats-ingest-svc
Actions → Start program: C:\Python39\python.exe
Arguments: scripts\database\bronze\tdx\002_ingest_tdx_users.py
```

**Option B: Windows Service**
```python
# Use pywin32 to create Windows service
# Service runs as lsats-ingest-svc account
# Inherits Kerberos ticket from service account
```

**Step 4: Grant database permissions**

```sql
CREATE ROLE "lsats-ingest-svc" WITH LOGIN;
GRANT INSERT, UPDATE ON bronze.raw_entities TO "lsats-ingest-svc";
GRANT INSERT, UPDATE ON meta.ingestion_runs TO "lsats-ingest-svc";
```

### Kerberos Limitations with pgAdmin

**pgAdmin Kerberos support is poor** due to web-based architecture:

**The problem:**
1. pgAdmin runs as web server (Python/Flask)
2. User's browser connects via HTTP(S)
3. Kerberos ticket from Windows session doesn't flow through browser
4. Would require SPNEGO/Negotiate HTTP authentication
5. Would require Kerberos credential delegation to PostgreSQL
6. Extremely complex setup with fragile results

**Recommendation: Use password authentication for pgAdmin**

```yaml
# pgAdmin uses traditional password over SSL
# Keep it simple for admin-only tool
lsats-pgadmin:
  # Users log in with database password
  # Access restricted via SSH tunnel or VPN
```

**For Kerberos-enabled database access, use desktop clients instead** (see Manager Analytics UI Options below).

### Recommended Kerberos Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    LSATS Database Access                    │
└─────────────────────────────────────────────────────────────┘

Ingestion Scripts (Python)
  ↓ Kerberos (gssencmode=require)
  ↓ User: lsats-ingest-svc@UMICH.EDU
PostgreSQL

Analysts (Azure Data Studio / DBeaver)
  ↓ Kerberos (Windows Authentication)
  ↓ User: analyst@UMICH.EDU
PostgreSQL

Admins (pgAdmin)
  ↓ SSH Tunnel → Password over SSL
  ↓ User: lsats_user (password)
PostgreSQL

Power BI / Excel (via ODBC)
  ↓ Kerberos (Windows Authentication)
  ↓ User: manager@UMICH.EDU
PostgreSQL
```

**Summary:**
- ✅ **Use Kerberos for:** Python scripts, desktop clients, BI tools
- ❌ **Don't use Kerberos for:** pgAdmin (use SSH tunnel + password)
- ✅ **Access control:** AD security groups mapped to PostgreSQL roles

---

## Manager Analytics UI Options

For end users (managers, analysts) who are comfortable with spreadsheets and need self-service analytics.

### Option 1: Excel with Power Query ⭐ RECOMMENDED

**Best for:** Windows users, spreadsheet-heavy workflows, familiar interface

#### Overview

Excel Power Query connects directly to PostgreSQL and refreshes data on demand. Users work in familiar Excel environment with full pivot table and charting capabilities.

#### Prerequisites

**Required software (IT provides):**
- Microsoft Excel 2016 or newer
- PostgreSQL ODBC Driver (64-bit)
- Domain-joined Windows machine (for Kerberos)

**Database preparation:**
- Create views in `gold` schema with user-friendly column names
- Set up row-level security if needed
- Configure Kerberos or SSL authentication

#### Implementation Steps

##### Step 1: Install PostgreSQL ODBC Driver

**On each user's Windows machine:**

1. Download **PostgreSQL ODBC Driver** (64-bit):
   - https://www.postgresql.org/ftp/odbc/versions/msi/
   - Latest version: `psqlodbc_x64.msi`

2. Install driver (requires admin rights)

3. Verify installation:
   ```
   Control Panel → Administrative Tools → ODBC Data Sources (64-bit)
   → Drivers tab → PostgreSQL Unicode should be listed
   ```

##### Step 2: Create System DSN (Data Source Name)

**Option A: Manual Setup (per user)**

```
1. Open ODBC Data Sources (64-bit)
2. System DSN tab → Add
3. Select "PostgreSQL Unicode(x64)" → Finish
4. Configure connection:
   
   Data Source: LSATS_DataHub
   Description: LSATS Data Hub Analytics
   Database: lsats_db
   Server: vm-hostname.umich.edu
   Port: 5432
   User Name: [leave blank for Kerberos]
   Password: [leave blank for Kerberos]
   
   SSL Mode: require
   
   [Advanced Options]
   ☑ Use Kerberos Authentication
   
5. Test connection → Should succeed without password
6. Save
```

**Option B: Automated Deployment (via Group Policy)**

Create `LSATS_ODBC.reg` file:

```reg
Windows Registry Editor Version 5.00

[HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBC.INI\LSATS_DataHub]
"Driver"="C:\\Program Files\\psqlODBC\\1500\\bin\\psqlodbc35w.dll"
"Description"="LSATS Data Hub Analytics"
"Database"="lsats_db"
"Servername"="vm-hostname.umich.edu"
"Port"="5432"
"SSLmode"="require"
"UseKerberosAuth"="1"
"Protocol"="7.4-1"
"ReadOnly"="1"
"ShowOidColumn"="0"
"FakeOidIndex"="0"
"ShowSystemTables"="0"
```

Deploy via Group Policy:
```
Computer Configuration → Preferences → Windows Settings
→ Registry → New → Registry Wizard
→ Import LSATS_ODBC.reg
```

##### Step 3: Create SQL Views for Business Users

**In PostgreSQL, create user-friendly views:**

```sql
-- gold/views_for_excel.sql

-- Compliance Dashboard View
CREATE OR REPLACE VIEW gold.compliance_dashboard AS
SELECT 
    d.dept_name AS "Department",
    u.display_name AS "Employee Name",
    u.uniqname AS "Uniqname",
    c.computer_name AS "Computer",
    c.operating_system AS "OS",
    c.last_check_date AS "Last Compliance Check",
    c.compliance_status AS "Status",
    c.days_non_compliant AS "Days Non-Compliant",
    t.ticket_id AS "Ticket ID",
    t.status_name AS "Ticket Status",
    t.created_date AS "Ticket Created"
FROM silver.departments d
JOIN silver.users u ON u.department_id = d.dept_id
LEFT JOIN silver.computers c ON c.assigned_user_uniqname = u.uniqname
LEFT JOIN silver.compliance_status c ON c.computer_id = c.computer_id
LEFT JOIN silver.tickets t ON t.requestor_uid = u.tdx_user_uid
WHERE d.is_active = true
ORDER BY d.dept_name, u.display_name;

GRANT SELECT ON gold.compliance_dashboard TO "LSATS-DB-ReadOnly";

-- User Activity Summary
CREATE OR REPLACE VIEW gold.user_activity_summary AS
SELECT
    u.display_name AS "Employee",
    u.uniqname AS "Uniqname",
    d.dept_name AS "Department",
    COUNT(DISTINCT c.computer_id) AS "Computer Count",
    COUNT(DISTINCT t.ticket_id) AS "Total Tickets",
    COUNT(DISTINCT t.ticket_id) FILTER (WHERE t.status_name = 'Open') AS "Open Tickets",
    MAX(t.created_date) AS "Last Ticket Date"
FROM silver.users u
JOIN silver.departments d ON u.department_id = d.dept_id
LEFT JOIN silver.computers c ON c.assigned_user_uniqname = u.uniqname
LEFT JOIN silver.tickets t ON t.requestor_uid = u.tdx_user_uid
GROUP BY u.display_name, u.uniqname, d.dept_name
ORDER BY d.dept_name, u.display_name;

GRANT SELECT ON gold.user_activity_summary TO "LSATS-DB-ReadOnly";

-- Ticket Summary by Department
CREATE OR REPLACE VIEW gold.tickets_by_department AS
SELECT
    d.dept_name AS "Department",
    t.type_name AS "Ticket Type",
    t.status_name AS "Status",
    COUNT(*) AS "Count",
    AVG(EXTRACT(DAY FROM (COALESCE(t.completed_date, NOW()) - t.created_date))) AS "Avg Days to Close"
FROM silver.tickets t
JOIN silver.users u ON t.requestor_uid = u.tdx_user_uid
JOIN silver.departments d ON u.department_id = d.dept_id
WHERE t.created_date >= NOW() - INTERVAL '90 days'
GROUP BY d.dept_name, t.type_name, t.status_name
ORDER BY d.dept_name, t.type_name, t.status_name;

GRANT SELECT ON gold.tickets_by_department TO "LSATS-DB-ReadOnly";
```

**Key principles for Excel-friendly views:**
- Use friendly column aliases with spaces: `AS "Employee Name"`
- Pre-calculate common metrics (don't make users write formulas)
- Filter to relevant data (active users, recent tickets)
- Include all columns users might want to pivot/filter on
- Avoid complex joins (do them in the view)

##### Step 4: Create Excel Template Workbook

**Template structure:**

```
Workbook: LSATS_Compliance_Dashboard.xlsx

Sheet 1: "README"
  - Instructions for refreshing data
  - Contact info for help
  - Last updated timestamp

Sheet 2: "Compliance_Data" (hidden from users)
  - Power Query connection to gold.compliance_dashboard
  - Raw data table
  - Auto-refresh on open

Sheet 3: "Overview Dashboard"
  - Pivot table: Count of computers by Status
  - Pivot table: Non-compliant computers by Department
  - Charts visualizing compliance metrics
  - Slicers for Department, Status

Sheet 4: "Department Detail"
  - Pivot table: Detailed view of all computers
  - Filters for Department, Employee, Status
  - Conditional formatting for non-compliant items

Sheet 5: "Ticket Status"
  - Pivot table: Ticket counts by Department and Status
  - Charts showing ticket trends
  - Slicer for date ranges

Sheet 6: "My Analysis" (blank)
  - Users create their own pivot tables here
  - Connected to same data source
```

**Create Power Query connection:**

```
1. Open Excel
2. Data tab → Get Data → From Other Sources → From ODBC
3. Select "LSATS_DataHub" DSN
4. Navigator shows database objects
5. Select "gold" schema → "compliance_dashboard" view
6. Click "Load" (loads to table) or "Transform Data" (opens Power Query Editor)

Power Query Editor (optional transformations):
- Change data types if needed
- Filter rows (e.g., only my department)
- Add calculated columns
- Click "Close & Load"

7. Table appears in worksheet
8. Right-click table → Table Properties:
   - ☑ Refresh data when opening the file
   - ☑ Enable background refresh
   - Refresh every: 60 minutes (optional)
```

**Create pivot table from Power Query table:**

```
1. Click anywhere in the data table
2. Insert tab → PivotTable
3. Select "Existing Worksheet" → specify location
4. Build pivot table:
   - Rows: Department, Employee Name
   - Columns: Status
   - Values: Count of Computer
   - Filters: Last Compliance Check (date range)
```

**Add slicers for interactivity:**

```
1. Click pivot table
2. PivotTable Analyze tab → Insert Slicer
3. Select fields: Department, Status, OS
4. Slicers appear as filter buttons
5. Users click to filter entire dashboard
```

##### Step 5: Distribute Template to Users

**Option A: SharePoint/OneDrive (Recommended)**

```
1. Upload LSATS_Compliance_Dashboard.xlsx to SharePoint
2. Share with LSATS-Analysts security group
3. Users open file in Excel Online or Desktop
4. Data refreshes automatically (cloud-connected)
5. Users can create personal copies for custom analysis
```

**SharePoint benefits:**
- Version control (single source of truth)
- Automatic updates when you improve template
- Users always have latest version
- Can use Excel Online (no desktop install needed)

**Option B: Network File Share**

```
\\lsa.umich.edu\shares\LSATS\Analytics\LSATS_Compliance_Dashboard.xlsx
```

Users open from network share, create local copy if they want to customize.

##### Step 6: Train Users

**15-minute training session covering:**

1. **Opening the workbook**
   - SharePoint link or network path
   - Excel prompts to enable content (macros/connections)

2. **Refreshing data**
   - Data tab → Refresh All (or F5)
   - Shows progress in status bar
   - Takes 5-30 seconds depending on data volume

3. **Using slicers to filter**
   - Click Department slicer → select their department
   - All pivot tables update instantly
   - Clear filter button to reset

4. **Creating personal analysis**
   - Go to "My Analysis" sheet
   - Insert → PivotTable → Use "Compliance_Data" table
   - Build custom views without affecting shared dashboard

5. **Troubleshooting**
   - "Cannot connect" error → check VPN connection
   - "Access denied" error → contact LSATS admin
   - Data looks old → click Refresh All

**Create quick reference card (PDF):**

```markdown
# LSATS Analytics Quick Reference

## Open Dashboard
1. Go to SharePoint: [link]
2. Open "LSATS_Compliance_Dashboard.xlsx"
3. Click "Enable Content" if prompted

## Refresh Data
- **Method 1:** Data tab → Refresh All
- **Method 2:** Press F5
- **Method 3:** Right-click pivot table → Refresh

## Filter Data
- Click buttons in slicers (Department, Status, etc.)
- Click multiple items to select more than one
- Use filter icon (⨯) to clear

## Create Custom Analysis
1. Go to "My Analysis" sheet
2. Insert → PivotTable
3. Choose "Use this workbook's Data Model"
4. Select "Compliance_Data"
5. Drag fields to build your analysis

## Get Help
- Email: lsats-data@umich.edu
- Teams: LSATS Analytics channel
```

#### Advanced Features

##### Parameter Queries (User-Driven Filters)

Allow users to enter parameters (e.g., their department) before refresh:

```
Power Query Editor:
1. Home → Manage Parameters → New Parameter
   - Name: UserDepartment
   - Type: Text
   - Suggested Values: List of values
   - Values: LSATS, Statistics, Mathematics, etc.

2. In query steps, add filter:
   = Table.SelectRows(Source, each [Department] = UserDepartment)

3. Close & Load

When user clicks Refresh:
- Excel prompts: "Enter your department:"
- User types "LSATS"
- Only LSATS data loads
```

##### Automatic Refresh with Power Automate

```
Power Automate Flow:
Trigger: Recurrence (daily at 6 AM)
Action: Refresh Excel file in SharePoint
  - File: LSATS_Compliance_Dashboard.xlsx
  - Dataset: Compliance_Data

Result: Data always fresh when users open in morning
```

##### Row-Level Security (Show Only User's Department)

**Option 1: Database View Filter (PostgreSQL)**

```sql
-- Create view that filters by current database user
CREATE OR REPLACE VIEW gold.my_department_data AS
SELECT *
FROM gold.compliance_dashboard
WHERE "Department" = (
    SELECT dept_name 
    FROM silver.departments d
    JOIN silver.users u ON d.dept_id = u.department_id
    WHERE u.uniqname = current_user  -- current_user is their Kerberos principal
);

GRANT SELECT ON gold.my_department_data TO "LSATS-DB-ReadOnly";
```

Users connect with their own credentials (Kerberos), automatically see only their data.

**Option 2: Excel Parameter (User-Managed)**

Users select their department from dropdown in Excel, query filters accordingly.

#### Pros and Cons

**Advantages:**
- ✅ Familiar interface (Excel)
- ✅ No training needed for pivot tables/charts
- ✅ Offline analysis (data cached locally)
- ✅ Full Excel functionality (formulas, VBA, etc.)
- ✅ Easy distribution via SharePoint
- ✅ Works with Kerberos (password-less)
- ✅ Can embed in PowerPoint presentations

**Disadvantages:**
- ❌ Requires ODBC driver installation
- ❌ Windows-only (Mac Excel has limited Power Query)
- ❌ Data gets stale (manual refresh required)
- ❌ No collaboration (users work on separate copies)
- ❌ No audit trail (can't see who viewed what)
- ❌ Users can modify data locally (not connected to source)

**Best for:**
- Windows-heavy environment
- Users who live in Excel
- Ad-hoc analysis workflows
- Offline/disconnected scenarios
- Exporting to PowerPoint/Word

---

### Option 2: Google Sheets with Connected Sheets + BigQuery

**Best for:** Google Workspace users, cloud-based workflows, collaborative analysis

#### Overview

Google Sheets "Connected Sheets" feature allows spreadsheet interface to BigQuery data warehouse. Requires intermediate step of syncing PostgreSQL → BigQuery.

**Architecture:**

```
PostgreSQL (LSATS Data Hub)
    ↓ Daily sync
BigQuery (Google Cloud)
    ↓ Connected Sheets
Google Sheets (Users)
```

#### Prerequisites

**Required:**
- Google Workspace account (UMich Google)
- Google Cloud Platform project
- BigQuery API enabled
- Service account with BigQuery permissions

**Database preparation:**
- Create views in `gold` schema
- Set up sync mechanism (Python script or Airbyte)

#### Implementation Steps

##### Step 1: Set Up Google Cloud Project

```
1. Go to console.cloud.google.com
2. Create new project: "lsats-analytics"
3. Enable APIs:
   - BigQuery API
   - BigQuery Data Transfer Service API
4. Create service account:
   - Name: lsats-bq-sync
   - Role: BigQuery Data Editor
   - Download JSON key file
```

##### Step 2: Create BigQuery Dataset

```sql
-- In BigQuery console
CREATE SCHEMA lsats_analytics
  OPTIONS(
    location="us-central1",
    description="LSATS Data Hub Analytics"
  );

-- Create tables (will be populated by sync)
-- Schema matches PostgreSQL gold layer views
```

##### Step 3: Sync PostgreSQL to BigQuery

**Option A: Python Script (Custom)**

```python
# scripts/analytics/sync_postgres_to_bigquery.py

import os
from google.cloud import bigquery
from database.adapters.postgres_adapter import create_postgres_adapter

# Initialize clients
postgres_adapter = create_postgres_adapter()
bq_client = bigquery.Client()

def sync_table(view_name: str, bq_table_id: str):
    """Sync PostgreSQL view to BigQuery table"""
    
    # Read from PostgreSQL
    df = postgres_adapter.query_to_dataframe(
        f"SELECT * FROM gold.{view_name}"
    )
    
    # Write to BigQuery (replaces existing data)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace table
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )
    
    job = bq_client.load_table_from_dataframe(
        df, 
        bq_table_id,
        job_config=job_config
    )
    
    job.result()  # Wait for completion
    print(f"✓ Synced {len(df)} rows to {bq_table_id}")

if __name__ == "__main__":
    # Sync all analytics views
    sync_table("compliance_dashboard", "lsats-analytics.lsats_analytics.compliance_dashboard")
    sync_table("user_activity_summary", "lsats-analytics.lsats_analytics.user_activity_summary")
    sync_table("tickets_by_department", "lsats-analytics.lsats_analytics.tickets_by_department")
```

**Schedule with cron (Linux VM):**

```bash
# crontab -e
0 6 * * * /usr/bin/python3 /path/to/sync_postgres_to_bigquery.py >> /var/log/lsats/bq_sync.log 2>&1
```

**Schedule with Windows Task Scheduler:**

```
Task Scheduler → Create Basic Task
  - Name: LSATS BigQuery Sync
  - Trigger: Daily at 6:00 AM
  - Action: Start program
    - Program: C:\Python39\python.exe
    - Arguments: scripts\analytics\sync_postgres_to_bigquery.py
  - Run as: lsats-ingest-svc (service account)
```

**Option B: Airbyte (Open Source ELT Tool)**

```yaml
# docker-compose.yml addition
airbyte:
  image: airbyte/airbyte-server:latest
  ports:
    - "8000:8000"
  environment:
    DATABASE_URL: ${DATABASE_URL}
    GOOGLE_APPLICATION_CREDENTIALS: /secrets/bq-service-account.json
  volumes:
    - ./secrets/bq-service-account.json:/secrets/bq-service-account.json:ro
```

**Configure Airbyte connection:**
```
1. Open http://localhost:8000
2. Create Source: PostgreSQL
   - Host: lsats-postgres
   - Database: lsats_db
   - Schemas: gold
3. Create Destination: BigQuery
   - Project ID: lsats-analytics
   - Dataset: lsats_analytics
   - Service Account JSON: [upload file]
4. Create Connection: PostgreSQL → BigQuery
   - Sync frequency: Every 24 hours at 6 AM
   - Sync mode: Full refresh (replace tables)
```

##### Step 4: Share BigQuery Access with Users

**Option A: Individual User Access**

```sql
-- In BigQuery, grant viewer access
GRANT `roles/bigquery.dataViewer` 
ON SCHEMA lsats_analytics 
TO 'user:analyst@umich.edu';
```

**Option B: Google Group Access (Better)**

```sql
-- Create Google Group: lsats-analytics@umich.edu
-- Add analysts to group
-- Grant access to group

GRANT `roles/bigquery.dataViewer` 
ON SCHEMA lsats_analytics 
TO 'group:lsats-analytics@umich.edu';
```

##### Step 5: Create Connected Sheet

**User workflow:**

```
1. Open Google Sheets: sheets.google.com
2. Create new blank sheet
3. Data → Data connectors → Connect to BigQuery
4. Select project: lsats-analytics
5. Select dataset: lsats_analytics
6. Select table: compliance_dashboard
7. Click "Connect"

Sheet now shows:
- Data preview (first 500 rows)
- "Extract" button (loads all data into sheet)
- "Refresh" button (reloads from BigQuery)
- Pivot table and chart options
```

**Create pivot table from Connected Sheet:**

```
1. In connected sheet, click "Create pivot table"
2. Choose "New sheet"
3. Pivot table editor appears:
   - Rows: Department
   - Columns: Status
   - Values: COUNTA of Computer
4. Pivot table appears, connected live to BigQuery
5. Click "Refresh" to update with latest data
```

**Scheduled refresh:**

```
Connected sheet options → Scheduled refresh
- Frequency: Daily
- Time: 7:00 AM
- Email notification: On failure

Sheet automatically refreshes data from BigQuery each morning
```

##### Step 6: Create Shared Dashboard Template

**Template structure:**

```
Google Sheet: "LSATS Compliance Dashboard"

Sheet 1: "Dashboard"
  - Summary metrics (cards with key numbers)
  - Charts showing compliance trends
  - Slicers (filter controls) for Department, Status
  - Instructions for refreshing

Sheet 2: "Data" (hidden)
  - Connected sheet to BigQuery compliance_dashboard table
  - Auto-refresh daily at 7 AM
  - Source for all charts/pivots

Sheet 3: "Department View"
  - Pivot table grouped by Department
  - Chart showing department comparison
  - Filter for specific department

Sheet 4: "User Detail"
  - Pivot table showing individual users
  - Filters for Employee, Computer, Status
```

**Share with team:**

```
1. Share button → Add people
2. Enter: lsats-analytics@umich.edu (Google Group)
3. Permission: Viewer
4. ☑ Notify people via email

Users get email with link
They can:
- View data
- Refresh data
- Create their own copies (File → Make a copy)
- Cannot edit shared version
```

**Allow users to make personal copies:**

```
File → Publish to web → Embed tab
☑ Enable "Make a copy" option

Share link with users:
"Click here to get your own copy: [link]"

Each user gets editable copy that refreshes from same BigQuery source
```

#### Advanced Features

##### Calculated Fields in Connected Sheet

```
Users can add calculated columns without modifying BigQuery:

1. In connected sheet, add column to the right
2. Column "Days Since Last Check"
   Formula: =TODAY() - J2
   (where J2 is "Last Compliance Check" date)

3. Column "Priority"
   Formula: =IF(K2 > 30, "High", IF(K2 > 14, "Medium", "Low"))

4. Use calculated columns in pivot tables
```

##### Row-Level Security (BigQuery)

```sql
-- Create authorized view that filters by user
CREATE OR REPLACE VIEW lsats_analytics.my_department_view AS
SELECT *
FROM lsats_analytics.compliance_dashboard
WHERE Department = (
  -- This function returns user's email (analyst@umich.edu)
  SELECT dept_name
  FROM lsats_analytics.user_department_mapping
  WHERE user_email = SESSION_USER()
);

-- Grant access to view only (not underlying table)
GRANT `roles/bigquery.dataViewer` 
ON TABLE lsats_analytics.my_department_view 
TO 'group:lsats-analytics@umich.edu';

-- Users automatically see only their department data
```

##### BigQuery BI Engine (Faster Queries)

```sql
-- Enable BI Engine for faster Connected Sheets performance
-- Reserve memory for caching frequently accessed data

ALTER BI_CAPACITY lsats_analytics.my_capacity
SET size_gb = 1;  -- 1 GB reservation (free tier)

-- Connected Sheets queries now use in-memory cache
-- Pivot tables and charts load instantly
```

##### Data Studio Dashboards (Alternative to Sheets)

```
For more advanced dashboards, use Google Data Studio:

1. Go to datastudio.google.com
2. Create → Data Source → BigQuery
3. Select lsats_analytics.compliance_dashboard
4. Create → Report
5. Drag-and-drop dashboard builder
6. Publish and share link

Benefits over Sheets:
- More chart types
- Better interactivity (drill-downs)
- Mobile-friendly
- No row limits (Sheets caps at 10M cells)
```

#### Pros and Cons

**Advantages:**
- ✅ Cloud-based (access anywhere, any device)
- ✅ Real-time collaboration (multiple users editing)
- ✅ No software installation needed
- ✅ Automatic refresh scheduling
- ✅ Integrates with Google Workspace (Drive, Docs, Slides)
- ✅ Version history (restore previous versions)
- ✅ Easy sharing (just send link)
- ✅ Works on Mac, Windows, Linux, mobile

**Disadvantages:**
- ❌ Requires BigQuery (additional cost and complexity)
- ❌ Data replication (PostgreSQL → BigQuery)
- ❌ Sync latency (data delayed by sync schedule)
- ❌ BigQuery costs (storage + query costs)
- ❌ 10M cell limit in Sheets
- ❌ Slower than native Excel for large datasets
- ❌ Limited offline functionality
- ❌ Less powerful than Excel for complex analysis

**Cost considerations:**
```
BigQuery Pricing (as of 2024):
- Storage: $0.02 per GB per month
  - Estimated: 5 GB = $0.10/month
- Queries: $5 per TB scanned
  - Estimated: 100 queries/day × 5 MB = 15 GB/month = $0.08/month
- BI Engine: Free for first 1 GB

Total estimated cost: ~$2-5/month for typical LSATS usage
```

**Best for:**
- Google Workspace environments
- Cloud-first organizations
- Collaborative analysis workflows
- Mobile/remote access needs
- Users on Mac or Linux machines

---

### Option 3: Tableau (If UMich Has License)

**Best for:** Executive dashboards, complex visualizations, governed analytics

#### Overview

Tableau provides enterprise-grade business intelligence with direct PostgreSQL connectivity. Users create interactive dashboards with drag-and-drop interface.

**Architecture:**

```
Tableau Desktop (Windows/Mac)
    ↓ Direct connection (Kerberos or SSL)
PostgreSQL (LSATS Data Hub)

OR

Tableau Server/Cloud
    ↓ Live connection or extract
PostgreSQL (LSATS Data Hub)
```

#### Prerequisites

**Check UMich licensing:**
- Contact UMich IT or LSA IT
- Tableau Desktop license (Creator role)
- Tableau Server/Cloud access (Explorer/Viewer roles)

**Required:**
- Tableau Desktop installed (Windows or Mac)
- VPN access to database server (if remote)
- Database credentials (Kerberos or password)

#### Implementation Steps

##### Step 1: Verify Tableau License and Install

**Check if UMich has Tableau:**

```
Email UMich IT or LSA IT:
  Subject: Tableau License Availability for LSATS

  We're building analytics for the LSATS Data Hub and would like 
  to leverage Tableau if licenses are available.

  Questions:
  - Does UMich have Tableau Server or Tableau Cloud?
  - How many Creator licenses are available for LSA?
  - Can we get Explorer/Viewer licenses for ~20 analysts?
  - Is Kerberos authentication configured?
```

**Install Tableau Desktop:**

```
1. Download from UMich software portal or tableau.com
2. Install (requires admin rights)
3. Activate license:
   - Enter license key from IT
   - OR sign in with Tableau Server credentials
```

##### Step 2: Connect Tableau to PostgreSQL

**Create new data source:**

```
1. Open Tableau Desktop
2. Connect → To a Server → PostgreSQL
3. Server: vm-hostname.umich.edu
4. Port: 5432
5. Database: lsats_db

Authentication options:

Option A: Username and Password
  - Username: your_uniqname
  - Password: your_password
  - ☑ Require SSL

Option B: Kerberos (if configured)
  - ☑ Use Integrated Security
  - Leave username/password blank
  - Uses Windows Kerberos ticket

6. Click "Sign In"
```

**Select tables/views:**

```
Database: lsats_db
Schema: gold

Drag views to canvas:
- compliance_dashboard
- user_activity_summary
- tickets_by_department

Tableau shows data preview
```

**Live Connection vs Extract:**

```
Live Connection:
- Queries PostgreSQL in real-time
- Always shows latest data
- Requires database connection
- Slower for large datasets
- Use for: Small datasets (<1M rows), frequently changing data

Extract:
- Copies data to .hyper file (Tableau's columnar format)
- Fast performance (in-memory)
- Works offline
- Refresh on schedule
- Use for: Large datasets, infrequent updates, mobile users

For LSATS: Start with Extract, refresh daily
```

**Create extract:**

```
Data source → Extract Data
- All rows or filter (e.g., last 90 days)
- Aggregation: None (keep detail)
- Number of rows: All rows
- ☑ Incremental refresh based on: created_date

Click "Extract"
Save to: LSATS_Compliance.hyper
```

##### Step 3: Build Dashboard

**Create first worksheet:**

```
1. Sheet 1: Rename to "Compliance Overview"
2. Drag fields to shelves:
   - Columns: Status
   - Rows: COUNT(Computer)
   - Color: Status (red for Non-Compliant, green for Compliant)
3. Show Me panel → Select bar chart
4. Add labels: Right-click bars → Mark Labels → Show mark labels
```

**Create second worksheet:**

```
1. New worksheet: "Department Breakdown"
2. Drag fields:
   - Rows: Department
   - Columns: COUNT(Computer)
   - Color: Status
3. Show Me → Stacked bar chart
4. Sort by total: Click toolbar icon → Sort descending
```

**Create third worksheet:**

```
1. New worksheet: "Trend Over Time"
2. Drag fields:
   - Columns: Last Compliance Check (continuous, day)
   - Rows: COUNT(Computer)
   - Color: Status
   - Filters: Last Compliance Check (relative date: Last 90 days)
3. Show Me → Line chart
```

**Create map visualization (if location data available):**

```
1. New worksheet: "Geographic Distribution"
2. Drag fields:
   - Double-click "Building" field (Tableau auto-geocodes)
   - Color: AVG(Days Non-Compliant)
   - Size: COUNT(Computer)
3. Show Me → Map
4. Map → Map Layers → Streets
```

**Combine into dashboard:**

```
1. New Dashboard → Rename "LSATS Compliance Dashboard"
2. Size: Automatic (responsive)
3. Drag worksheets to dashboard:
   - Top: Compliance Overview (full width)
   - Middle: Department Breakdown (left) + Trend Over Time (right)
   - Bottom: Geographic Distribution (full width)
4. Add dashboard objects:
   - Text: Title "LSATS Compliance Dashboard"
   - Text: Last updated timestamp
   - Image: LSA or LSATS logo
```

**Add interactivity (filters and actions):**

```
1. Add filter controls:
   - Drag "Department" to Filters shelf
   - Right-click → Show Filter
   - Filter type: Multiple Values Dropdown
   - Apply to all worksheets

2. Add dashboard actions:
   - Dashboard → Actions → Add Action → Filter
   - Name: "Click to filter"
   - Source: Department Breakdown
   - Target: All sheets
   - User clicks department → all charts update

3. Add parameter for date range:
   - Create Parameter: "Date Range"
   - Data type: String
   - List: Last 30 days, Last 90 days, Last year, All time
   - Show parameter control
   - Create calculated field using parameter
```

##### Step 4: Publish to Tableau Server/Cloud

**If UMich has Tableau Server:**

```
1. Server → Sign In
   - Server: tableau.umich.edu (or similar)
   - Username: your_uniqname
   - Password: (may use Shibboleth SSO)

2. Server → Publish Workbook
   - Project: LSA / LSATS Analytics
   - Name: LSATS Compliance Dashboard
   - Permissions:
     - LSATS-Admins: Full control
     - LSATS-Analysts: Interactor (can filter)
     - LSATS-Viewers: Viewer (read-only)

3. Data source settings:
   - ☑ Embed password (for extracts)
   - OR ☑ Prompt user for credentials (for live connections)
   
4. Click "Publish"
```

**Schedule extract refresh:**

```
Tableau Server web interface:
1. Navigate to published data source
2. Extract Refreshes tab
3. New Extract Refresh
   - Frequency: Daily
   - Time: 6:00 AM
   - Incremental refresh: Yes

Extract automatically refreshes from PostgreSQL each morning
```

**Share with users:**

```
1. Users navigate to: https://tableau.umich.edu
2. Sign in with UMich credentials (Shibboleth)
3. Browse to: LSA / LSATS Analytics / LSATS Compliance Dashboard
4. View interactive dashboard in browser
5. Can:
   - Filter data with controls
   - Click charts to drill down
   - Export to PDF/PowerPoint
   - Subscribe to email snapshots
```

##### Step 5: Enable Self-Service Analytics

**Web Edit (if licenses allow):**

```
Users with Explorer license can:
1. Open published dashboard
2. Click "Edit" button
3. Modify filters, change chart types
4. Save as personal copy
5. Share with team

This allows self-service without Tableau Desktop
```

**Ask Data (Natural Language Interface):**

```
Tableau Server → Ask Data
User types questions in plain English:
- "Show me non-compliant computers by department"
- "What is the average days to close tickets?"
- "Which departments have the most open tickets?"

Tableau auto-generates visualizations
Users can refine and save to dashboard
```

**Certified Data Sources:**

```
As admin, certify your data sources:
1. Server → Data Sources → LSATS Compliance Data
2. More Actions → Certify
   - Certification note: "Official LSATS data, refreshed daily at 6 AM"
   
Users see checkmark badge → trust this is authoritative data
```

#### Advanced Features

##### Row-Level Security

**Scenario:** Managers see only their department data

```sql
-- PostgreSQL: Create user-to-department mapping table
CREATE TABLE gold.tableau_user_permissions (
    tableau_username VARCHAR(255),
    department_id VARCHAR(50)
);

INSERT INTO gold.tableau_user_permissions VALUES
    ('myodhes', 'LSATS'),
    ('manager1', 'Statistics'),
    ('manager2', 'Mathematics');

-- Create view with RLS
CREATE OR REPLACE VIEW gold.compliance_rls AS
SELECT c.*
FROM gold.compliance_dashboard c
WHERE c."Department" IN (
    SELECT department_id 
    FROM gold.tableau_user_permissions
    WHERE tableau_username = current_user
);
```

**In Tableau:**

```
1. Connect to gold.compliance_rls view
2. Publish to server with "User filter" enabled
3. Each user sees only their authorized departments
4. No manual filtering needed
```

**Alternative: Tableau Server RLS (no database changes):**

```
1. Create calculated field: "RLS Filter"
   Formula: [Department] = USERNAME()

2. Add to Filters shelf:
   - Field: RLS Filter
   - Value: True

3. Publish with "Run as viewer" option

Each user sees data filtered by their username matching department
```

##### Tableau Parameters for Dynamic Analysis

```
Create parameter: "Metric Selector"
- Data type: String
- List: Computer Count, Ticket Count, Avg Days Non-Compliant

Create calculated field: "Selected Metric"
CASE [Metric Selector]
    WHEN "Computer Count" THEN COUNT([Computer])
    WHEN "Ticket Count" THEN COUNT([Ticket ID])
    WHEN "Avg Days Non-Compliant" THEN AVG([Days Non-Compliant])
END

Use in chart:
- Columns: Department
- Rows: Selected Metric
- User changes parameter → chart updates dynamically
```

##### Embedded Analytics (Tableau in Web App)

```html
<!-- Embed Tableau dashboard in your web application -->
<iframe 
  src="https://tableau.umich.edu/views/LSATSCompliance/Dashboard?:embed=yes"
  width="100%" 
  height="800">
</iframe>

<!-- Or use Tableau JavaScript API for full control -->
<script src="https://tableau.umich.edu/javascripts/api/tableau-2.min.js"></script>
<script>
  var viz = new tableau.Viz(
    document.getElementById('tableauViz'),
    'https://tableau.umich.edu/views/LSATSCompliance/Dashboard',
    {
      width: '100%',
      height: '800px',
      hideTabs: true,
      onFirstInteractive: function() {
        // Programmatic filtering, parameter changes, etc.
      }
    }
  );
</script>
```

##### Mobile App

```
Users install Tableau Mobile (iOS/Android)
- Sign in with UMich credentials
- See published dashboards
- Offline mode (cached extracts)
- Touch-optimized interface
```

#### User Training and Adoption

**Training modules:**

1. **Viewers (30 min):**
   - Accessing Tableau Server
   - Using filters and date ranges
   - Exporting to PDF/PowerPoint
   - Subscribing to email snapshots

2. **Explorers (2 hours):**
   - Web editing basics
   - Modifying existing dashboards
   - Creating simple visualizations
   - Saving personal copies

3. **Creators (2 days):**
   - Tableau Desktop fundamentals
   - Connecting to data sources
   - Building dashboards from scratch
   - Publishing and permissions

**Self-service resources:**

```
Create Tableau workbook: "LSATS Analytics Training"
- Sample data (anonymized)
- Step-by-step tutorial dashboards
- Common questions and answers
- Link to Tableau public gallery for inspiration
```

#### Pros and Cons

**Advantages:**
- ✅ Professional dashboards (enterprise-grade)
- ✅ Powerful visualizations (maps, forecasts, clustering)
- ✅ Kerberos authentication support
- ✅ Row-level security built-in
- ✅ Mobile apps available
- ✅ Governed analytics (certified data sources)
- ✅ Embedded analytics (integrate into web apps)
- ✅ Natural language queries (Ask Data)
- ✅ Scalable (handles millions of rows efficiently)

**Disadvantages:**
- ❌ Expensive ($$$ per user per year)
- ❌ Steeper learning curve than Excel
- ❌ Requires Tableau Desktop for authoring (unless Web Edit)
- ❌ Less familiar than spreadsheets for many users
- ❌ Licensing complexity (Creator vs Explorer vs Viewer)
- ❌ Requires IT involvement for server management

**Licensing costs (approximate):**
```
Tableau Desktop (Creator): $70/user/month
Tableau Server (Explorer): $35/user/month
Tableau Server (Viewer): $12/user/month

For 2 Creators + 5 Explorers + 20 Viewers:
  2 × $70 + 5 × $35 + 20 × $12 = $555/month = $6,660/year

Check if UMich site license covers this cost
```

**Best for:**
- Organizations with existing Tableau investment
- Executive-level dashboards
- Complex analytical requirements
- Governed, certified analytics
- Mobile workforce
- Embedded analytics in custom applications

---

## Comparison Matrix

| Feature | Excel Power Query | Google Sheets + BigQuery | Tableau |
|---------|-------------------|--------------------------|---------|
| **Cost** | Free (Excel included) | ~$5/month (BigQuery) | $$$$ (per user) |
| **Setup Time** | 2-4 hours | 1-2 days | 3-5 days |
| **Learning Curve** | Minimal (Excel familiar) | Low (Sheets familiar) | Medium (training needed) |
| **User Capacity** | Unlimited | Unlimited | Limited by licenses |
| **Kerberos Support** | ✅ Yes (ODBC) | ❌ No | ✅ Yes (native) |
| **Real-time Data** | Manual refresh | Scheduled refresh | Live or extract |
| **Collaboration** | Poor (file-based) | Excellent (cloud) | Excellent (server) |
| **Offline Use** | ✅ Yes | Limited | ✅ Yes (extracts) |
| **Mobile Access** | Limited (Excel mobile) | ✅ Yes (responsive) | ✅ Yes (native app) |
| **Self-Service** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Data Volume** | <1M rows | <10M cells | Billions of rows |
| **Visualization** | Basic charts | Basic charts | Advanced (maps, etc.) |
| **Row-Level Security** | Database-side | ✅ BigQuery RLS | ✅ Built-in |
| **Platform** | Windows only | Any (web) | Windows/Mac desktop |

## Recommendations by Use Case

### Scenario 1: Compliance Dashboard for Managers
**Users:** 10-15 managers, Windows machines, Excel-heavy workflow

**Recommendation:** Excel Power Query ⭐
- Familiar interface (zero training)
- Fast setup (afternoon project)
- SharePoint distribution
- Kerberos authentication

### Scenario 2: Lab Analytics for Researchers
**Users:** 20-30 researchers, mix of Windows/Mac/Linux, Google Workspace

**Recommendation:** Google Sheets + BigQuery ⭐
- Cross-platform
- Collaborative analysis
- Cloud-based (work from anywhere)
- Low cost

### Scenario 3: Executive Dashboards for Dean's Office
**Users:** 5-10 executives, need mobile access, presentation-ready

**Recommendation:** Tableau (if licensed) ⭐
- Professional appearance
- Mobile apps
- Interactive drill-downs
- Embedded in existing portals

### Scenario 4: Mixed Environment (Recommended)
**Deploy multiple solutions for different audiences:**

```
Tier 1: Power Users (2-3 people)
  → Azure Data Studio with Kerberos
  → Direct SQL access for complex queries

Tier 2: Analysts (10-15 people)
  → Excel Power Query templates
  → Scheduled refreshes, pivot tables

Tier 3: Managers (20-30 people)
  → Tableau Server dashboards (if available)
  → OR Google Sheets dashboards (if not)
  → Read-only, filtered to their department

Tier 4: Executives (5-10 people)
  → Tableau dashboards embedded in portal
  → Mobile app access
  → Email subscriptions for weekly snapshots
```

---

## Getting Started Checklist

### Phase 1: Infrastructure (Week 1)
- [ ] Decide on SSL vs SSH tunnel for remote access
- [ ] Request certificates from UMich PKI (if SSL)
- [ ] Configure PostgreSQL with SSL (if remote access needed)
- [ ] Test Kerberos authentication for PostgreSQL
- [ ] Create AD security groups (LSATS-DB-Admins, LSATS-DB-ReadOnly)
- [ ] Create gold schema views with business-friendly names

### Phase 2: Pilot (Week 2-3)
- [ ] Choose primary analytics tool based on user base
- [ ] Set up ODBC drivers (Excel) or BigQuery sync (Sheets) or Tableau Server
- [ ] Build initial dashboard with 2-3 key metrics
- [ ] Test with 2-3 pilot users
- [ ] Gather feedback and iterate

### Phase 3: Rollout (Week 4-5)
- [ ] Create user documentation and quick reference
- [ ] Conduct 30-minute training sessions
- [ ] Distribute templates/links to all users
- [ ] Set up automated refresh schedules
- [ ] Monitor usage and troubleshoot issues

### Phase 4: Maintenance (Ongoing)
- [ ] Weekly: Check data refresh logs
- [ ] Monthly: Review user feedback and requests
- [ ] Quarterly: Assess if additional views/dashboards needed
- [ ] Annually: Review security (rotate certificates, audit permissions)

---

## Support and Resources

### Internal Documentation
- `.claude/medallion_standards.md` - Overall data architecture
- `.claude/silver_layer_standards.md` - Silver layer view definitions
- `docker/postgres/views/README.md` - View documentation

### External Resources
- **Excel Power Query:** https://support.microsoft.com/en-us/office/power-query
- **Google BigQuery:** https://cloud.google.com/bigquery/docs
- **Tableau:** https://help.tableau.com/current/pro/desktop/en-us/
- **PostgreSQL Kerberos:** https://www.postgresql.org/docs/current/gssapi-auth.html

### Getting Help
- **Database issues:** lsats-data@umich.edu
- **UMich PKI requests:** Contact ITS or LSA IT
- **Tableau licensing:** Contact UMich IT
- **Training resources:** Schedule session with LSATS team
