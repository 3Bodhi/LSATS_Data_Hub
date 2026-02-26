-- ============================================================================
-- LSATS Data Hub — Production Schema
-- ============================================================================
--
-- Generated from Docker development database via pg_dump --schema-only.
-- Cleaned for production: legacy tables removed, FK retargeted to silver.users.
--
-- Prerequisites: production_init.sql must be run first (creates extensions,
-- schemas, helper functions, and meta tables).
--
-- Run after this: docker/postgres/views/silver_views.sql
-- ============================================================================

CREATE FUNCTION silver.update_lab_member_counts(p_lab_id character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE silver.labs
    SET
        member_count = (
            SELECT COUNT(*)
            FROM silver.lab_members
            WHERE lab_id = p_lab_id
        ),
        pi_count = (
            SELECT COUNT(*)
            FROM silver.lab_members
            WHERE lab_id = p_lab_id AND is_pi = true
        ),
        investigator_count = (
            SELECT COUNT(*)
            FROM silver.lab_members
            WHERE lab_id = p_lab_id
              AND member_role LIKE '%Investigator%'
        )
    WHERE lab_id = p_lab_id;
END;
$$;


--
-- Name: FUNCTION update_lab_member_counts(p_lab_id character varying); Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON FUNCTION silver.update_lab_member_counts(p_lab_id character varying) IS 'Recalculates member counts for a specific lab';


--
-- Name: raw_entities; Type: TABLE; Schema: bronze; Owner: -
--

CREATE TABLE bronze.raw_entities (
    raw_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_type character varying(50) NOT NULL,
    source_system character varying(50) NOT NULL,
    external_id character varying(255) NOT NULL,
    raw_data jsonb NOT NULL,
    ingested_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    entity_hash character varying(64) GENERATED ALWAYS AS (encode(sha256(((((((entity_type)::text || '|'::text) || (source_system)::text) || '|'::text) || (external_id)::text))::bytea), 'hex'::text)) STORED,
    ingestion_metadata jsonb DEFAULT '{}'::jsonb
);


--
-- Name: TABLE raw_entities; Type: COMMENT; Schema: bronze; Owner: -
--

COMMENT ON TABLE bronze.raw_entities IS 'Stores raw data exactly as received from any source system';


--
-- Name: department_masters; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.department_masters (
    master_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    canonical_name character varying(255) NOT NULL,
    canonical_code character varying(50),
    canonical_description text,
    is_active boolean DEFAULT true NOT NULL,
    region character varying(100),
    confidence_score numeric(3,2),
    source_count integer DEFAULT 1 NOT NULL,
    primary_source character varying(50) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    last_reconciled_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT department_masters_confidence_score_check CHECK (((confidence_score >= 0.00) AND (confidence_score <= 1.00)))
);


--
-- Name: TABLE department_masters; Type: COMMENT; Schema: gold; Owner: -
--

COMMENT ON TABLE gold.department_masters IS 'Authoritative master department records combining all sources';


--
-- Name: department_source_mapping; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.department_source_mapping (
    mapping_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    master_id uuid NOT NULL,
    silver_id uuid NOT NULL,
    contribution_weight numeric(3,2) DEFAULT 1.00,
    is_primary_source boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: lab_memberships; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.lab_memberships (
    membership_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    user_master_id uuid NOT NULL,
    lab_name character varying(255) NOT NULL,
    lab_pi_uniqname character varying(50),
    user_role character varying(50),
    department_master_id uuid,
    is_active boolean DEFAULT true NOT NULL,
    source_system character varying(50) NOT NULL,
    confidence_score numeric(3,2) DEFAULT 1.00,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: user_masters; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.user_masters (
    master_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    canonical_uniqname character varying(50) NOT NULL,
    canonical_name character varying(255),
    canonical_email character varying(255),
    primary_department_id uuid,
    user_type character varying(50),
    is_active boolean DEFAULT true NOT NULL,
    confidence_score numeric(3,2),
    source_count integer DEFAULT 1 NOT NULL,
    primary_source character varying(50) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    last_reconciled_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT user_masters_confidence_score_check CHECK (((confidence_score >= 0.00) AND (confidence_score <= 1.00)))
);


--
-- Name: department_summary; Type: VIEW; Schema: gold; Owner: -
--

CREATE VIEW gold.department_summary AS
 SELECT dept.canonical_name,
    dept.canonical_code,
    dept.is_active,
    count(DISTINCT user_master.master_id) AS total_users,
    count(DISTINCT
        CASE
            WHEN ((lab.user_role)::text = 'PI'::text) THEN lab.user_master_id
            ELSE NULL::uuid
        END) AS pi_count,
    count(DISTINCT lab.lab_name) AS lab_count,
    dept.confidence_score,
    dept.source_count
   FROM ((gold.department_masters dept
     LEFT JOIN gold.user_masters user_master ON ((user_master.primary_department_id = dept.master_id)))
     LEFT JOIN gold.lab_memberships lab ON (((lab.department_master_id = dept.master_id) AND (lab.is_active = true))))
  GROUP BY dept.master_id, dept.canonical_name, dept.canonical_code, dept.is_active, dept.confidence_score, dept.source_count
  ORDER BY dept.canonical_name;


--
-- Name: VIEW department_summary; Type: COMMENT; Schema: gold; Owner: -
--

COMMENT ON VIEW gold.department_summary IS 'Summary statistics for each department including user and lab counts';


--
-- Name: lab_rosters; Type: VIEW; Schema: gold; Owner: -
--

CREATE VIEW gold.lab_rosters AS
 SELECT membership.lab_name,
    membership.lab_pi_uniqname,
    dept.canonical_name AS department_name,
    membership.user_role,
    user_master.canonical_uniqname,
    user_master.canonical_name AS user_name,
    user_master.canonical_email,
    membership.is_active AS membership_active,
    membership.confidence_score
   FROM ((gold.lab_memberships membership
     JOIN gold.user_masters user_master ON ((membership.user_master_id = user_master.master_id)))
     LEFT JOIN gold.department_masters dept ON ((membership.department_master_id = dept.master_id)))
  WHERE (membership.is_active = true)
  ORDER BY membership.lab_name, membership.user_role, user_master.canonical_name;


--
-- Name: VIEW lab_rosters; Type: COMMENT; Schema: gold; Owner: -
--

COMMENT ON VIEW gold.lab_rosters IS 'Complete view of lab memberships with user details and roles';


--
-- Name: user_source_mapping; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.user_source_mapping (
    mapping_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    master_id uuid NOT NULL,
    silver_id uuid NOT NULL,
    contribution_weight numeric(3,2) DEFAULT 1.00,
    is_primary_source boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: ad_computers; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.ad_computers (
    sam_account_name character varying(20) NOT NULL,
    computer_name character varying(60) NOT NULL,
    dns_hostname character varying(60),
    distinguished_name character varying(300) NOT NULL,
    object_guid uuid NOT NULL,
    object_sid character varying(100) NOT NULL,
    ou_root character varying(50),
    ou_organization_type character varying(50),
    ou_organization character varying(50),
    ou_category character varying(60),
    ou_division character varying(100),
    ou_department character varying(150),
    ou_subdepartment character varying(150),
    ou_immediate_parent character varying(150),
    ou_full_path jsonb DEFAULT '[]'::jsonb,
    operating_system character varying(50),
    operating_system_version character varying(20),
    operating_system_service_pack character varying(50),
    is_enabled boolean NOT NULL,
    user_account_control integer NOT NULL,
    is_critical_system_object boolean,
    service_principal_names jsonb DEFAULT '[]'::jsonb,
    network_addresses jsonb DEFAULT '[]'::jsonb,
    member_of_groups jsonb DEFAULT '[]'::jsonb,
    managed_by character varying(250),
    description character varying(300),
    display_name character varying(100),
    pwd_last_set timestamp with time zone NOT NULL,
    account_expires timestamp with time zone,
    bad_password_time timestamp with time zone,
    bad_pwd_count integer,
    ms_laps_password_expiration_time bigint,
    ms_mcs_adm_pwd_expiration_time bigint,
    msds_supported_encryption_types integer,
    msds_key_credential_link text,
    user_certificate text,
    last_logon timestamp with time zone,
    last_logon_timestamp timestamp with time zone,
    last_logoff timestamp with time zone,
    logon_count integer,
    when_created timestamp with time zone NOT NULL,
    when_changed timestamp with time zone NOT NULL,
    usn_created bigint NOT NULL,
    usn_changed bigint NOT NULL,
    ds_core_propagation_data jsonb DEFAULT '[]'::jsonb,
    additional_attributes jsonb,
    raw_id uuid NOT NULL,
    source_system character varying(50) DEFAULT 'active_directory'::character varying NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE ad_computers; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.ad_computers IS 'Source-specific silver table for Active Directory computers. Part of two-tier silver architecture - feeds into consolidated silver.computers table. Contains typed columns extracted from bronze.raw_entities JSONB data with universal OU extraction for cross-entity matching with AD groups, users, and OUs.';


--
-- Name: COLUMN ad_computers.sam_account_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.sam_account_name IS 'Primary key - SAM account name with $ suffix (e.g., MCDB-5CG0183FB7$). Unique AD identifier. 100% populated.';


--
-- Name: COLUMN ad_computers.computer_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.computer_name IS 'Computer hostname (CN field). CRITICAL for joining with keyconfigure_computers.computer_name and TDX asset names. 100% populated.';


--
-- Name: COLUMN ad_computers.dns_hostname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.dns_hostname IS 'Fully qualified domain name (e.g., mcdb-5cg0183fb7.adsroot.itcs.umich.edu). Used for network identity and IP resolution. 99.7% populated.';


--
-- Name: COLUMN ad_computers.object_guid; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.object_guid IS 'Immutable AD GUID. Survives renames and moves. 100% populated.';


--
-- Name: COLUMN ad_computers.ou_division; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.ou_division IS 'Division OU extracted from DN (e.g., RSN, EHTS, CSG). CRITICAL for cross-entity queries: find all computers, groups, users in same division. Enables departmental analysis.';


--
-- Name: COLUMN ad_computers.ou_department; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.ou_department IS 'Department OU extracted from DN (e.g., Biology, Psychology, Chemistry). CRITICAL for matching computers to departmental groups and users.';


--
-- Name: COLUMN ad_computers.ou_full_path; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.ou_full_path IS 'Complete OU hierarchy as JSONB array (leaf→root order). Supports deep hierarchy queries and edge cases beyond standard extraction.';


--
-- Name: COLUMN ad_computers.operating_system; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.operating_system IS 'OS name (e.g., Windows 11 Enterprise, Mac OS X). CRITICAL for joining with tdx_assets.attr_operating_system_name and keyconfigure_computers.os. 99.6% populated.';


--
-- Name: COLUMN ad_computers.is_enabled; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.is_enabled IS 'Computed from userAccountControl bit 2 (ACCOUNTDISABLE). TRUE = account enabled (94.2%), FALSE = disabled (5.8%). Use for filtering active computers.';


--
-- Name: COLUMN ad_computers.service_principal_names; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.service_principal_names IS 'Kerberos service principal names (JSONB array). Common: HOST, TERMSRV, WSMAN, RestrictedKrbHost. Used for authentication services. 99.7% populated, avg 9 entries.';


--
-- Name: COLUMN ad_computers.member_of_groups; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.member_of_groups IS 'AD group DNs (JSONB array). Normalized from string/array inconsistency on ingestion. Used for access control policies and group membership analysis. 48.7% populated, max 5 groups.';


--
-- Name: COLUMN ad_computers.managed_by; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.managed_by IS 'DN of managing user or group. Links to silver.ad_users or silver.ad_groups for ownership tracking. 39.4% populated.';


--
-- Name: COLUMN ad_computers.pwd_last_set; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.pwd_last_set IS 'Computer account password last changed. Indicates last successful AD authentication. Range: 2013-2025. 100% populated.';


--
-- Name: COLUMN ad_computers.ms_laps_password_expiration_time; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.ms_laps_password_expiration_time IS 'Windows LAPS (Local Admin Password Solution) password expiry timestamp. 81.3% populated. Used for local admin password rotation compliance.';


--
-- Name: COLUMN ad_computers.last_logon_timestamp; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.last_logon_timestamp IS 'Replicated last logon timestamp across all DCs. CRITICAL for identifying active vs stale computers. Prefer this over last_logon (DC-specific). Range: 2020-11-13 to 2025-11-21. 99.8% populated.';


--
-- Name: COLUMN ad_computers.additional_attributes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.additional_attributes IS 'Consolidated JSONB for rare attributes (<10% population): pager, networkAddress, msDFSR, mSMQ, delegation, gMSA fields.';


--
-- Name: COLUMN ad_computers.raw_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.raw_id IS 'Link to most recent bronze.raw_entities record for this computer - for audit trail and bronze data access.';


--
-- Name: COLUMN ad_computers.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_computers.entity_hash IS 'SHA-256 hash of significant fields for change detection - only transform if hash changed (incremental processing).';


--
-- Name: ad_groups; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.ad_groups (
    ad_group_guid uuid NOT NULL,
    cn character varying(255),
    sam_account_name character varying(255),
    distinguished_name text,
    description text,
    group_type integer,
    sam_account_type integer,
    object_category character varying(255),
    object_class text[],
    members jsonb DEFAULT '[]'::jsonb,
    member_of jsonb DEFAULT '[]'::jsonb,
    when_created timestamp with time zone,
    when_changed timestamp with time zone,
    display_name character varying(255),
    info text,
    managed_by text,
    proxy_addresses text[],
    usn_created bigint,
    usn_changed bigint,
    object_sid character varying(255),
    sid_history character varying(255),
    raw_id uuid,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    ou_root character varying(100),
    ou_organization_type character varying(100),
    ou_organization character varying(100),
    ou_category character varying(100),
    ou_status character varying(100),
    ou_division character varying(100),
    ou_department character varying(100),
    ou_subdepartment character varying(100),
    ou_immediate_parent character varying(100),
    ou_full_path jsonb,
    ou_depth integer,
    parent_ou_dn character varying(500),
    group_name character varying(255),
    group_email character varying(255)
);


--
-- Name: TABLE ad_groups; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.ad_groups IS 'Source-specific silver table for Active Directory groups, preserving raw structure and relationships in JSONB format.';


--
-- Name: ad_labs; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.ad_labs (
    ad_lab_id character varying(100) NOT NULL,
    pi_uniqname character varying(50) NOT NULL,
    lab_name character varying(255),
    has_ad_ou boolean DEFAULT true,
    ad_ou_dn text,
    ad_ou_hierarchy jsonb DEFAULT '[]'::jsonb,
    ad_parent_ou text,
    ad_ou_depth integer,
    ad_ou_created timestamp with time zone,
    ad_ou_modified timestamp with time zone,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(50) DEFAULT 'active_directory'::character varying,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    description character varying(1024),
    managed_by character varying(255),
    department_name character varying(255),
    department_id character varying(50),
    department_match_method character varying(50),
    department_match_confidence numeric(3,2)
);


--
-- Name: TABLE ad_labs; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.ad_labs IS 'Pipeline Helper: Aggregates AD OU data per PI. Intermediate step for composite labs.';


--
-- Name: COLUMN ad_labs.department_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_labs.department_name IS 'Department name extracted from AD OU hierarchy (typically 2nd level OU)';


--
-- Name: COLUMN ad_labs.department_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_labs.department_id IS 'FK to silver.departments(dept_id), matched from extracted department_name using fuzzy matching';


--
-- Name: COLUMN ad_labs.department_match_method; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_labs.department_match_method IS 'How the department was matched: fuzzy_match (similarity matching), exact_code (if dept code found in OU)';


--
-- Name: COLUMN ad_labs.department_match_confidence; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_labs.department_match_confidence IS 'Confidence score 0.00-1.00 for the department match';


--
-- Name: ad_organizational_units; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.ad_organizational_units (
    object_guid uuid NOT NULL,
    distinguished_name character varying(250) NOT NULL,
    ou_name character varying(60) NOT NULL,
    name character varying(60) NOT NULL,
    description character varying(100),
    managed_by character varying(200),
    gp_link text,
    gp_options character varying(10),
    object_category character varying(150),
    object_class jsonb DEFAULT '[]'::jsonb,
    instance_type integer,
    system_flags integer,
    ou_root character varying(50),
    ou_organization_type character varying(50),
    ou_organization character varying(50),
    ou_category character varying(100),
    ou_division character varying(100),
    ou_department character varying(150),
    ou_subdepartment character varying(150),
    ou_immediate_parent character varying(150),
    ou_full_path jsonb DEFAULT '[]'::jsonb,
    ou_depth integer NOT NULL,
    parent_ou_dn character varying(250),
    direct_computer_count integer DEFAULT 0,
    has_computer_children boolean DEFAULT false,
    child_ou_count integer DEFAULT 0,
    has_child_ous boolean DEFAULT false,
    depth_category character varying(20),
    extracted_uniqname character varying(20),
    name_patterns jsonb,
    when_created timestamp with time zone NOT NULL,
    when_changed timestamp with time zone NOT NULL,
    usn_created bigint NOT NULL,
    usn_changed bigint NOT NULL,
    ds_core_propagation_data jsonb DEFAULT '[]'::jsonb,
    raw_id uuid NOT NULL,
    source_system character varying(50) DEFAULT 'active_directory'::character varying NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ou_status character varying(20)
);


--
-- Name: TABLE ad_organizational_units; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.ad_organizational_units IS 'Source-specific silver table for Active Directory organizational units. Contains typed columns extracted from bronze.raw_entities JSONB data with universal OU hierarchy extraction for cross-entity matching. Critical for lab identification and organizational structure analysis. Preserves enrichment metadata (computer counts, uniqname extraction) computed during bronze ingestion.';


--
-- Name: COLUMN ad_organizational_units.object_guid; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.object_guid IS 'Primary key - Immutable AD GUID. Survives OU renames and moves. 100% populated.';


--
-- Name: COLUMN ad_organizational_units.distinguished_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.distinguished_name IS 'Full distinguished name (DN). Unique but can change if OU is renamed/moved. Example: OU=psyc-danweiss,OU=Psychology,OU=EHTS,... Max length: 228 chars. 100% populated.';


--
-- Name: COLUMN ad_organizational_units.ou_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.ou_name IS 'OU name from ''ou'' LDAP field. Examples: "psyc-danweiss", "Biology", "Current", "Research and Instrumentation". Max length: 48 chars. 100% populated.';


--
-- Name: COLUMN ad_organizational_units.ou_category; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.ou_category IS 'Category OU extracted from DN (e.g., "Research and Instrumentation", "Current", "Workstations", "Users and Groups"). Position from end: array[length-3].';


--
-- Name: COLUMN ad_organizational_units.ou_division; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.ou_division IS 'Division OU extracted from DN (e.g., RSN, EHTS, CSG, Randall). CRITICAL for cross-entity queries: find all OUs, computers, groups, users in same division. Position from end: array[length-4].';


--
-- Name: COLUMN ad_organizational_units.ou_department; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.ou_department IS 'Department OU extracted from DN (e.g., Biology, Psychology, Chemistry). CRITICAL for matching OUs to departmental groups, users, computers. Position from end: array[length-5].';


--
-- Name: COLUMN ad_organizational_units.ou_subdepartment; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.ou_subdepartment IS 'Sub-department or lab-specific OU. Position from end: array[length-6]. Often NULL for higher-level OUs.';


--
-- Name: COLUMN ad_organizational_units.ou_full_path; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.ou_full_path IS 'Complete OU hierarchy as JSONB array (leaf→root order). Example: ["psyc-danweiss", "Psychology", "EHTS", "Current", "Research and Instrumentation", "LSA", "Organizations", "UMICH"]. Supports deep hierarchy queries and edge cases.';


--
-- Name: COLUMN ad_organizational_units.direct_computer_count; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.direct_computer_count IS 'Number of computers directly in this OU (not recursive). Range: 0-27, avg 2. Computed during bronze ingestion via LDAP query. CRITICAL for identifying lab OUs with managed computers.';


--
-- Name: COLUMN ad_organizational_units.has_computer_children; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.has_computer_children IS 'True if direct_computer_count > 0. Quick filter for OUs with managed computers.';


--
-- Name: COLUMN ad_organizational_units.child_ou_count; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.child_ou_count IS 'Number of immediate sub-OUs (not recursive). Range: 0-41, avg 1. Computed during bronze ingestion via LDAP query.';


--
-- Name: COLUMN ad_organizational_units.depth_category; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.depth_category IS 'Depth-based classification: "potential_lab" (962, depth 8+), "department" (184, depth 7), "region" (37, depth 6), "high_level" (20, depth <6). Used for filtering lab OUs.';


--
-- Name: COLUMN ad_organizational_units.extracted_uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.extracted_uniqname IS 'Uniqname extracted from OU name using pattern matching. CRITICAL for lab matching with silver.lab_awards. Examples: "psyc-danweiss" → "danweiss", "Vsih-Lab" → "vsih". 736 populated (61%), 85 matches with lab awards (49% match rate).';


--
-- Name: COLUMN ad_organizational_units.name_patterns; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.name_patterns IS 'JSONB object with boolean flags for name pattern classification. Structure: {"dept_uniqname": bool, "uniqname_only": bool, "lab_suffix": bool, "has_hyphen": bool}. Used for pattern-based lab identification.';


--
-- Name: COLUMN ad_organizational_units.raw_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.raw_id IS 'Link to most recent bronze.raw_entities record for this OU - for audit trail and complete bronze data access.';


--
-- Name: COLUMN ad_organizational_units.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.entity_hash IS 'SHA-256 hash of significant fields for change detection - only transform if hash changed (incremental processing).';


--
-- Name: COLUMN ad_organizational_units.ou_status; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.ad_organizational_units.ou_status IS 'OU status/lifecycle stage (e.g., "Current", "Legacy", "Staging"). Position from end: array[length-5]. CRITICAL for filtering active vs legacy organizational structures.';


--
-- Name: ad_users; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.ad_users (
    ad_user_guid uuid NOT NULL,
    name text,
    cn text,
    sam_account_name text,
    distinguished_name text,
    user_principal_name text,
    display_name text,
    initials text,
    title text,
    description text,
    mobile text,
    other_mobile text,
    facsimile_telephone_number text,
    street_address text,
    proxy_addresses jsonb DEFAULT '[]'::jsonb,
    department text,
    umichad_ou jsonb DEFAULT '[]'::jsonb,
    umichad_role jsonb DEFAULT '[]'::jsonb,
    user_account_control integer,
    account_expires timestamp with time zone,
    pwd_last_set timestamp with time zone,
    last_logon timestamp with time zone,
    last_logon_timestamp timestamp with time zone,
    last_logoff timestamp with time zone,
    bad_pwd_count integer,
    bad_password_time timestamp with time zone,
    logon_count integer,
    lockout_time timestamp with time zone,
    object_sid text,
    sid_history jsonb DEFAULT '[]'::jsonb,
    when_created timestamp with time zone,
    when_changed timestamp with time zone,
    usn_created bigint,
    usn_changed bigint,
    object_class jsonb DEFAULT '[]'::jsonb,
    object_category text,
    instance_type integer,
    member_of jsonb DEFAULT '[]'::jsonb,
    primary_group_id integer,
    uid_number bigint,
    gid_number bigint,
    home_directory text,
    home_drive text,
    login_shell text,
    employee_type text,
    raw_id uuid NOT NULL,
    entity_hash text NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    ou_root character varying(100),
    ou_organization_type character varying(100),
    ou_organization character varying(100),
    ou_category character varying(100),
    ou_status character varying(100),
    ou_division character varying(100),
    ou_department character varying(100),
    ou_subdepartment character varying(100),
    ou_immediate_parent character varying(100),
    ou_full_path jsonb,
    ou_depth integer,
    parent_ou_dn character varying(500),
    uniqname text,
    first_name text,
    last_name text,
    full_name text,
    primary_email text,
    work_phone text,
    mobile_phone text,
    job_title text,
    department_name text,
    ad_cn text,
    ad_name text,
    ad_object_sid text,
    ldap_uid_number bigint,
    ldap_gid_number bigint,
    preferred_name text
);


--
-- Name: award_labs; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.award_labs (
    award_lab_id character varying(100) NOT NULL,
    pi_uniqname character varying(50) NOT NULL,
    lab_name character varying(255),
    total_award_dollars numeric(15,2) DEFAULT 0.00,
    total_direct_dollars numeric(15,2) DEFAULT 0.00,
    total_indirect_dollars numeric(15,2) DEFAULT 0.00,
    award_count integer DEFAULT 0,
    active_award_count integer DEFAULT 0,
    earliest_award_start date,
    latest_award_end date,
    primary_department_id character varying(50),
    department_ids jsonb DEFAULT '[]'::jsonb,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(50) DEFAULT 'lab_award'::character varying,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    sponsors jsonb DEFAULT '[]'::jsonb,
    award_titles jsonb DEFAULT '[]'::jsonb,
    primary_department_name character varying(255)
);


--
-- Name: TABLE award_labs; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.award_labs IS 'Pipeline Helper: Aggregates lab award data per PI. Intermediate step for composite labs.';


--
-- Name: computer_attributes; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.computer_attributes (
    attribute_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    computer_id character varying(100) NOT NULL,
    attribute_name character varying(255) NOT NULL,
    attribute_value text,
    attribute_value_uid integer,
    source_system character varying(50) DEFAULT 'tdx'::character varying NOT NULL,
    tdx_form_id integer,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE computer_attributes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.computer_attributes IS 'TDX custom attributes that vary by form type';


--
-- Name: computer_groups; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.computer_groups (
    membership_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    computer_id character varying(100) NOT NULL,
    group_id character varying(50),
    group_dn text NOT NULL,
    group_cn character varying(255),
    source_system character varying(50) DEFAULT 'active_directory'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE computer_groups; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.computer_groups IS 'Computer AD group memberships extracted from AD member_of_groups array';


--
-- Name: COLUMN computer_groups.group_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computer_groups.group_id IS 'FK to silver.groups.group_id (if group exists in silver layer)';


--
-- Name: COLUMN computer_groups.group_dn; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computer_groups.group_dn IS 'Full AD distinguished name from memberOf attribute';


--
-- Name: COLUMN computer_groups.group_cn; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computer_groups.group_cn IS 'Extracted CN (common name) from group DN';


--
-- Name: computers; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.computers (
    silver_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    computer_id character varying(100) NOT NULL,
    computer_name character varying(255) NOT NULL,
    computer_name_aliases jsonb DEFAULT '[]'::jsonb,
    serial_number character varying(100),
    serial_numbers jsonb DEFAULT '[]'::jsonb,
    mac_address character varying(17),
    mac_addresses jsonb DEFAULT '[]'::jsonb,
    tdx_asset_id integer,
    tdx_tag character varying(50),
    tdx_status_id integer,
    tdx_status_name character varying(100),
    tdx_form_id integer,
    tdx_form_name character varying(255),
    tdx_configuration_item_id integer,
    tdx_external_id character varying(100),
    tdx_uri character varying(255),
    ad_object_guid uuid,
    ad_object_sid character varying(100),
    ad_sam_account_name character varying(20),
    ad_dns_hostname character varying(60),
    ad_distinguished_name text,
    kc_mac_address character varying(20),
    owner_uniqname character varying(50),
    owner_department_id character varying(50),
    manufacturer character varying(255),
    product_model character varying(255),
    os_family character varying(50),
    os_name character varying(255),
    os_version character varying(100),
    os_install_date timestamp with time zone,
    cpu character varying(255),
    cpu_cores smallint,
    cpu_sockets smallint,
    cpu_speed_mhz integer,
    ram_mb integer,
    disk_gb numeric(10,2),
    disk_free_gb numeric(10,2),
    is_active boolean DEFAULT true NOT NULL,
    is_ad_enabled boolean,
    has_recent_activity boolean,
    last_seen timestamp with time zone,
    last_user character varying(50),
    location_info jsonb DEFAULT '{}'::jsonb,
    ownership_info jsonb DEFAULT '{}'::jsonb,
    hardware_specs jsonb DEFAULT '{}'::jsonb,
    os_details jsonb DEFAULT '{}'::jsonb,
    network_info jsonb DEFAULT '{}'::jsonb,
    ad_security_info jsonb DEFAULT '{}'::jsonb,
    ad_ou_info jsonb DEFAULT '{}'::jsonb,
    financial_info jsonb DEFAULT '{}'::jsonb,
    activity_timestamps jsonb DEFAULT '{}'::jsonb,
    tdx_attributes jsonb DEFAULT '{}'::jsonb,
    tdx_attachments jsonb DEFAULT '[]'::jsonb,
    source_raw_ids jsonb DEFAULT '{}'::jsonb,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(100) NOT NULL,
    source_entity_id character varying(255),
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    financial_owner_uniqname character varying(8),
    primary_lab_id character varying(100),
    primary_lab_method character varying(50),
    lab_association_count integer DEFAULT 0,
    kc_computer_id character varying(100),
    kc_primary_mac character varying(20),
    kc_nic_count smallint,
    CONSTRAINT computers_data_quality_score_check CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE computers; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.computers IS 'Consolidated computer records from TDX, KeyConfigure, and Active Directory';


--
-- Name: COLUMN computers.silver_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.silver_id IS 'Primary key - UUID for internal referencing (standard across all silver tables)';


--
-- Name: COLUMN computers.computer_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.computer_id IS 'Computed stable identifier - LOWER(computer_name) for lookups';


--
-- Name: COLUMN computers.serial_number; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.serial_number IS 'Primary serial number (best from TDX > KC)';


--
-- Name: COLUMN computers.mac_address; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.mac_address IS 'Primary MAC address (best from TDX > KC)';


--
-- Name: COLUMN computers.owner_uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.owner_uniqname IS 'Operational owner - who uses/manages the computer (TDX Owning Customer → KC Owner → AD Managed By)';


--
-- Name: COLUMN computers.owner_department_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.owner_department_id IS 'Primary department - resolved FK to silver.departments.dept_id';


--
-- Name: COLUMN computers.manufacturer; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.manufacturer IS 'Manufacturer name from TDX (Apple, Dell, HP, Lenovo, etc.)';


--
-- Name: COLUMN computers.product_model; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.product_model IS 'Product model from TDX (ThinkPad X1, MacBook Pro, etc.)';


--
-- Name: COLUMN computers.os_family; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.os_family IS 'Derived OS family: Windows, macOS, Linux, etc.';


--
-- Name: COLUMN computers.os_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.os_name IS 'Best OS name from KC > AD > TDX';


--
-- Name: COLUMN computers.is_active; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.is_active IS 'Active status from TDX status_name';


--
-- Name: COLUMN computers.is_ad_enabled; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.is_ad_enabled IS 'AD account enabled status from userAccountControl';


--
-- Name: COLUMN computers.last_seen; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.last_seen IS 'Most recent activity from any source (max of all timestamps)';


--
-- Name: COLUMN computers.location_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.location_info IS 'JSONB: Consolidated location data from TDX';


--
-- Name: COLUMN computers.ownership_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.ownership_info IS 'JSONB: Complete ownership details from all sources';


--
-- Name: COLUMN computers.hardware_specs; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.hardware_specs IS 'JSONB: Detailed hardware specifications from all sources';


--
-- Name: COLUMN computers.os_details; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.os_details IS 'JSONB: Complete OS information from all sources';


--
-- Name: COLUMN computers.network_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.network_info IS 'JSONB: Network configuration (IPs, MACs, DNS)';


--
-- Name: COLUMN computers.ad_security_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.ad_security_info IS 'JSONB: AD security attributes (SPNs, LAPS, etc.)';


--
-- Name: COLUMN computers.ad_ou_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.ad_ou_info IS 'JSONB: Complete AD OU hierarchy';


--
-- Name: COLUMN computers.financial_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.financial_info IS 'JSONB: Financial data from TDX (cost, dates)';


--
-- Name: COLUMN computers.activity_timestamps; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.activity_timestamps IS 'JSONB: All activity timestamps from all sources';


--
-- Name: COLUMN computers.tdx_attributes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.tdx_attributes IS 'JSONB: TDX custom attributes array';


--
-- Name: COLUMN computers.source_raw_ids; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.source_raw_ids IS 'JSONB: Raw IDs linking to bronze.raw_entities for audit trail';


--
-- Name: COLUMN computers.financial_owner_uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.financial_owner_uniqname IS 'Financial owner - who is financially responsible for the computer (TDX Financial Owner only, no fallback)';


--
-- Name: COLUMN computers.kc_computer_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.kc_computer_id IS 'Primary key from silver.keyconfigure_computers (consolidated computer record)';


--
-- Name: COLUMN computers.kc_primary_mac; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.kc_primary_mac IS 'Primary MAC address from KeyConfigure consolidation';


--
-- Name: COLUMN computers.kc_nic_count; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.computers.kc_nic_count IS 'Number of NICs found in KeyConfigure for this computer';


--
-- Name: departments; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.departments (
    silver_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    department_name character varying(255) NOT NULL,
    department_code character varying(50),
    description text,
    is_active boolean DEFAULT true NOT NULL,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(50) NOT NULL,
    source_entity_id character varying(255) NOT NULL,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    dept_id character varying(50) NOT NULL,
    tdx_id integer,
    campus_name character varying(255),
    college_group character varying(255),
    vp_area character varying(255),
    hierarchical_path text,
    tdx_created_date timestamp with time zone,
    tdx_modified_date timestamp with time zone,
    location_info jsonb DEFAULT '{}'::jsonb,
    parent_dept_id text,
    vp_area_name text,
    college_name text,
    dept_group text,
    dept_group_campus text,
    dept_group_vp_area text,
    tdx_manager_uid uuid,
    sources jsonb DEFAULT '[]'::jsonb,
    CONSTRAINT departments_data_quality_score_check CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE departments; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.departments IS 'Consolidated department records from UMAPI and TeamDynamix';


--
-- Name: COLUMN departments.silver_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.silver_id IS 'Internal UUID for referencing from gold layer (maintained for backward compatibility)';


--
-- Name: COLUMN departments.dept_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.dept_id IS '6-digit department code or TDX fallback code';


--
-- Name: COLUMN departments.tdx_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.tdx_id IS 'TeamDynamix internal ID for API write-back operations';


--
-- Name: COLUMN departments.hierarchical_path; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.hierarchical_path IS 'Full organizational path from root (Campus) to department';


--
-- Name: COLUMN departments.tdx_created_date; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.tdx_created_date IS 'Record creation timestamp from TeamDynamix (source-specific)';


--
-- Name: COLUMN departments.tdx_modified_date; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.tdx_modified_date IS 'Record modification timestamp from TeamDynamix (source-specific)';


--
-- Name: COLUMN departments.location_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.location_info IS 'JSONB structure containing city, address, phone, fax, url, postal_code from TDX';


--
-- Name: COLUMN departments.sources; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.departments.sources IS 'List of source systems contributing to this record';


--
-- Name: group_members; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.group_members (
    membership_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    group_id character varying(100) NOT NULL,
    member_type character varying(20) NOT NULL,
    member_uniqname character varying(50),
    member_group_id character varying(100),
    is_direct_member boolean DEFAULT true,
    source_system character varying(50) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_member_reference CHECK (((((member_type)::text = 'user'::text) AND (member_uniqname IS NOT NULL) AND (member_group_id IS NULL)) OR (((member_type)::text = 'group'::text) AND (member_group_id IS NOT NULL) AND (member_uniqname IS NULL)))),
    CONSTRAINT group_members_member_type_check CHECK (((member_type)::text = ANY ((ARRAY['user'::character varying, 'group'::character varying])::text[])))
);


--
-- Name: TABLE group_members; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.group_members IS 'Group membership relationships supporting both user and nested group members';


--
-- Name: COLUMN group_members.group_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.group_members.group_id IS 'Foreign key to silver.groups(group_id). Increased to VARCHAR(100).';


--
-- Name: COLUMN group_members.member_group_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.group_members.member_group_id IS 'Group identifier (cn) when member is a group. Increased to VARCHAR(100) to accommodate long group names.';


--
-- Name: COLUMN group_members.is_direct_member; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.group_members.is_direct_member IS 'True for umichDirectMember, false for inherited/nested memberships';


--
-- Name: group_owners; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.group_owners (
    ownership_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    group_id character varying(100) NOT NULL,
    owner_type character varying(20) NOT NULL,
    owner_uniqname character varying(50),
    owner_group_id character varying(100),
    source_system character varying(50) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_owner_reference CHECK (((((owner_type)::text = 'user'::text) AND (owner_uniqname IS NOT NULL) AND (owner_group_id IS NULL)) OR (((owner_type)::text = 'group'::text) AND (owner_group_id IS NOT NULL) AND (owner_uniqname IS NULL)))),
    CONSTRAINT group_owners_owner_type_check CHECK (((owner_type)::text = ANY ((ARRAY['user'::character varying, 'group'::character varying])::text[])))
);


--
-- Name: TABLE group_owners; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.group_owners IS 'Group ownership relationships from MCommunity (AD does not have owner field)';


--
-- Name: COLUMN group_owners.group_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.group_owners.group_id IS 'Foreign key to silver.groups(group_id). Increased to VARCHAR(100).';


--
-- Name: COLUMN group_owners.owner_group_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.group_owners.owner_group_id IS 'Group identifier (cn) when owner is a group. Increased to VARCHAR(100) to accommodate long group names.';
-- Name: TABLE groups_legacy; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: COLUMN groups_legacy.group_id; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: COLUMN groups_legacy.sync_source; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: COLUMN groups_legacy.data_quality_score; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: groups; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.groups (
    group_id character varying(100) NOT NULL,
    ad_group_guid uuid,
    mcommunity_group_uid uuid,
    group_name character varying(100),
    group_email character varying(100),
    sam_account_name character varying(100),
    cn character varying(100),
    distinguished_name text,
    description text,
    display_name character varying(255),
    group_type integer,
    is_security_group boolean,
    is_distribution_group boolean,
    sam_account_type integer,
    object_category character varying(255),
    is_private boolean,
    is_members_only boolean,
    is_joinable boolean,
    expiry_timestamp timestamp with time zone,
    is_mcomm_adsync boolean DEFAULT false,
    ou_root character varying(100),
    ou_organization character varying(100),
    ou_department character varying(100),
    ou_category character varying(100),
    ou_immediate_parent character varying(100),
    ou_full_path jsonb DEFAULT '[]'::jsonb,
    ou_depth integer,
    parent_ou_dn text,
    members jsonb DEFAULT '[]'::jsonb,
    owners jsonb DEFAULT '[]'::jsonb,
    member_of jsonb DEFAULT '[]'::jsonb,
    direct_members jsonb DEFAULT '[]'::jsonb,
    nested_members jsonb DEFAULT '[]'::jsonb,
    managed_by text,
    contact_info jsonb DEFAULT '{}'::jsonb,
    proxy_addresses text[],
    when_created timestamp with time zone,
    when_changed timestamp with time zone,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(100) NOT NULL,
    source_entity_id character varying(255) NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ad_raw_id uuid,
    mcommunity_raw_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    CONSTRAINT groups_data_quality_score_check1 CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE groups; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.groups IS 'Consolidated groups table merging AD and MCommunity LDAP groups.
- AD-only groups: 7,597
- MCommunity-only groups: 32,671
- Merged groups (CN overlap): 1,129
- Total: 41,397 groups
Business key (group_id) uses natural CN/group_name with no prefixes.';


--
-- Name: COLUMN groups.group_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.groups.group_id IS 'Natural group identifier (CN or group_name). Clean, human-readable, no prefixes.
Examples: lsa-staff, LSA-Chemistry-Faculty, research-coordinators';


--
-- Name: COLUMN groups.is_mcomm_adsync; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.groups.is_mcomm_adsync IS 'TRUE for groups synchronized between AD and MCommunity via MCommADSync.
Detected by OU containing "MCommADSync" in AD distinguished_name.
1,071 groups have this flag set.';


--
-- Name: COLUMN groups.members; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.groups.members IS 'JSONB array of member DNs. Merged and deduplicated from:
- AD: member attribute
- MCommunity: member attribute
Format: ["CN=user1,OU=...", "CN=user2,OU=...", ...]';


--
-- Name: COLUMN groups.owners; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.groups.owners IS 'JSONB array of owner/manager DNs. Merged from:
- MCommunity: owner attribute  
- AD: managedBy attribute (converted to array)
Format: ["uid=user1,ou=...", "uid=user2,ou=...", ...]';


--
-- Name: COLUMN groups.data_quality_score; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.groups.data_quality_score IS 'Quality score from 0.00 to 1.00 based on field completeness and source richness.
Higher scores for merged groups with complete metadata.';


--
-- Name: keyconfigure_computers; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.keyconfigure_computers (
    computer_id character varying(100) NOT NULL,
    computer_name character varying(50) NOT NULL,
    oem_serial_number character varying(30),
    primary_mac_address character varying(20),
    mac_addresses jsonb,
    ip_addresses jsonb,
    nic_count smallint DEFAULT 1,
    cpu character varying(30),
    cpu_cores smallint,
    cpu_sockets smallint,
    clock_speed_mhz integer,
    ram_mb integer,
    disk_gb numeric(10,2),
    disk_free_gb numeric(10,2),
    os character varying(30),
    os_family character varying(30),
    os_version character varying(40),
    os_serial_number character varying(30),
    os_install_date timestamp with time zone,
    last_user character varying(40),
    owner character varying(100),
    login_type character varying(15),
    last_session timestamp with time zone,
    last_startup timestamp with time zone,
    last_audit timestamp with time zone,
    base_audit timestamp with time zone,
    keyconfigure_client_version character varying(15),
    consolidated_raw_ids jsonb,
    raw_id uuid NOT NULL,
    source_system character varying(50) DEFAULT 'key_client'::character varying NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);
-- Name: lab_awards; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.lab_awards (
    award_record_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    award_id character varying(15) NOT NULL,
    person_uniqname character varying(50) NOT NULL,
    person_role character varying(60) NOT NULL,
    project_grant_id character varying(15),
    award_title character varying(250) NOT NULL,
    award_class character varying(30),
    award_total_dollars numeric(15,2),
    award_direct_dollars numeric(15,2),
    award_indirect_dollars numeric(15,2),
    facilities_admin_rate numeric(5,2),
    award_start_date date,
    award_end_date date,
    pre_nce_end_date date,
    award_publish_date date,
    direct_sponsor_name character varying(120),
    direct_sponsor_category character varying(100),
    direct_sponsor_subcategory character varying(100),
    direct_sponsor_reference character varying(100),
    prime_sponsor_name character varying(120),
    prime_sponsor_category character varying(100),
    prime_sponsor_subcategory character varying(100),
    prime_sponsor_reference character varying(100),
    award_admin_department character varying(100),
    award_admin_school_college character varying(100),
    person_first_name character varying(100),
    person_last_name character varying(100),
    person_appt_department character varying(100),
    person_appt_department_id character varying(50),
    person_appt_school_college character varying(100),
    raw_id uuid NOT NULL,
    raw_data_snapshot jsonb,
    source_system character varying(50) DEFAULT 'lab_awards'::character varying NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE lab_awards; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.lab_awards IS 'Source-specific silver table for lab awards (TIER 1). Extracts all fields from bronze.raw_entities where entity_type=lab_award. Part of medallion two-tier architecture. One record per (Award Id, Person Uniqname, Person Role) combination. Total records: 1,043 from bronze.';


--
-- Name: COLUMN lab_awards.award_record_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.award_record_id IS 'Primary key UUID for each record.';


--
-- Name: COLUMN lab_awards.award_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.award_id IS 'Award identifier from source system (e.g., AWD029634). Max length observed: 9 chars.';


--
-- Name: COLUMN lab_awards.person_uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.person_uniqname IS 'Person uniqname (lowercase). Max length observed: 8 chars.';


--
-- Name: COLUMN lab_awards.person_role; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.person_role IS 'Person role on award (e.g., "UM Principal Investigator"). Max length observed: 48 chars.';


--
-- Name: COLUMN lab_awards.award_total_dollars; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.award_total_dollars IS 'Total award dollars parsed from "$X,XXX" format. Range: $0 to $10M+.';


--
-- Name: COLUMN lab_awards.award_start_date; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.award_start_date IS 'Award project start date parsed from "M/D/YYYY" format. 100% populated.';


--
-- Name: COLUMN lab_awards.raw_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.raw_id IS 'Link to bronze.raw_entities record for audit trail and lineage tracking.';


--
-- Name: COLUMN lab_awards.raw_data_snapshot; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.raw_data_snapshot IS 'Optional JSONB snapshot of complete bronze record for audit purposes.';


--
-- Name: COLUMN lab_awards.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_awards.entity_hash IS 'SHA-256 hash of significant fields for change detection. Excludes metadata (_content_hash, _source_file, _ingestion_timestamp).';
-- Name: TABLE lab_awards_legacy; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: COLUMN lab_awards_legacy.is_active; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: COLUMN lab_awards_legacy.bronze_raw_id; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: lab_computers; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.lab_computers (
    association_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    computer_id character varying(100) NOT NULL,
    lab_id character varying(100) NOT NULL,
    association_method character varying(50) NOT NULL,
    confidence_score numeric(3,2) DEFAULT 0.50,
    matched_ou text,
    matched_group_id character varying(50),
    matched_user character varying(50),
    is_primary boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    owner_is_pi boolean DEFAULT false,
    fin_owner_is_pi boolean DEFAULT false,
    owner_is_member boolean DEFAULT false,
    fin_owner_is_member boolean DEFAULT false,
    function_is_research boolean DEFAULT false,
    function_is_classroom boolean DEFAULT false,
    quality_flags jsonb DEFAULT '[]'::jsonb,
    CONSTRAINT check_association_method CHECK (((association_method)::text = ANY ((ARRAY['ad_ou_nested'::character varying, 'owner_is_pi'::character varying, 'fin_owner_is_pi'::character varying, 'name_pattern_pi'::character varying, 'group_membership'::character varying, 'owner_member'::character varying, 'last_user_member'::character varying])::text[]))),
    CONSTRAINT computer_labs_confidence_score_check CHECK (((confidence_score >= 0.00) AND (confidence_score <= 1.00)))
);


--
-- Name: TABLE lab_computers; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.lab_computers IS 'Lab-Computer associations with hierarchical confidence scoring.

Discovery Strategy (2-Tier System):
- Tier 1 (Strong): PI ownership, financial ownership, AD OU, name pattern → confidence 0.70-1.00
- Tier 2 (Weak): Member-only relationships → confidence 0.20-0.50

Transformation Strategy:
- Full refresh (TRUNCATE + INSERT) for data consistency
- Multi-criteria discovery with additive confidence scoring
- Primary lab selection based on highest confidence

Updated: 2025-11-27 (Migration 036)';


--
-- Name: COLUMN lab_computers.association_method; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.association_method IS 'Primary discovery method that initially identified this association: ad_ou_nested (computer in lab OU), owner_is_pi (owner = PI), group_membership (computer in lab groups), owner_member (owner in lab), last_user_member (last user in lab). Kept for backward compatibility and debugging.';


--
-- Name: COLUMN lab_computers.confidence_score; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.confidence_score IS 'Confidence score with hierarchical tier enforcement:
- Tier 1 methods (ad_ou_nested, owner_is_pi, fin_owner_is_pi, name_pattern_pi): 0.70 - 1.00
- Tier 2 methods (group_membership, owner_member, last_user_member): 0.20 - 0.50
- Strong discovery methods (Tier 1) always yield high confidence (floor: 0.70)
- Weak discovery methods (Tier 2) always yield low-medium confidence (ceiling: 0.50)';


--
-- Name: COLUMN lab_computers.is_primary; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.is_primary IS 'TRUE for the single highest-confidence lab association per computer. Used for quick lookup of primary lab assignment. Null/FALSE for secondary associations.';


--
-- Name: COLUMN lab_computers.owner_is_pi; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.owner_is_pi IS 'TRUE if computer.owner_uniqname = lab.pi_uniqname. Strong positive indicator (prevents -0.15 penalty).';


--
-- Name: COLUMN lab_computers.fin_owner_is_pi; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.fin_owner_is_pi IS 'TRUE if computer.financial_owner_uniqname = lab.pi_uniqname. Good positive indicator (prevents -0.10 penalty).';


--
-- Name: COLUMN lab_computers.owner_is_member; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.owner_is_member IS 'TRUE if computer.owner_uniqname is in lab_members but not the PI. Moderate positive indicator (prevents -0.20 penalty).';


--
-- Name: COLUMN lab_computers.fin_owner_is_member; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.fin_owner_is_member IS 'TRUE if computer.financial_owner_uniqname is in lab_members but not the PI. Moderate positive indicator (prevents -0.15 penalty).';


--
-- Name: COLUMN lab_computers.function_is_research; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.function_is_research IS 'TRUE if TDX Function attribute = "Research" (ID 27316). Strong positive indicator (prevents -0.10 penalty). Admin/Staff and Dev/Testing functions apply -0.20 penalty.';


--
-- Name: COLUMN lab_computers.function_is_classroom; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.function_is_classroom IS 'TRUE if TDX Function attribute = "Classroom/Computer Lab" (ID 27312). Minor positive indicator (+0.05 bonus, sometimes used for research labs).';


--
-- Name: COLUMN lab_computers.quality_flags; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_computers.quality_flags IS 'Quality flags array for monitoring association quality.
Common flags:
- low_confidence: confidence < 0.40
- high_confidence: confidence >= 0.90
- fully_pi_owned: both owner and financial owner are PI
- owner_not_affiliated: owner not PI or member
- fin_owner_not_affiliated: financial owner not PI or member
- admin_function: function is Admin/Staff
- dev_function: function is Dev/Testing
- no_function: function is NULL';


--
-- Name: CONSTRAINT check_association_method ON lab_computers; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON CONSTRAINT check_association_method ON silver.lab_computers IS 'Valid association methods:
- ad_ou_nested: Computer DN contains lab AD OU (Tier 1, conf 0.70-1.00)
- owner_is_pi: Owner is the PI (Tier 1, conf 0.70-1.00)
- fin_owner_is_pi: Financial owner is the PI (Tier 1, conf 0.70-1.00)
- name_pattern_pi: Computer name contains PI uniqname (Tier 1, conf 0.70-1.00)
- group_membership: Computer in groups matching lab (Tier 2, conf 0.20-0.50)
- owner_member: Owner is lab member, not PI (Tier 2, conf 0.20-0.50)
- last_user_member: Last user is lab member (Tier 2, conf 0.20-0.50)';


--
-- Name: lab_managers; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.lab_managers (
    lab_manager_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    lab_id character varying(100) NOT NULL,
    manager_uniqname character varying(50) NOT NULL,
    manager_tdx_uid uuid,
    manager_role text,
    manager_job_codes jsonb,
    manager_confidence_score integer NOT NULL,
    manager_rank integer NOT NULL,
    detection_reason text NOT NULL,
    is_verified boolean DEFAULT false,
    verification_notes text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    lab_department_id character varying(50),
    lab_department_name character varying(255),
    manager_department_id character varying(50),
    manager_department_name character varying(255),
    CONSTRAINT lab_managers_manager_confidence_score_check CHECK (((manager_confidence_score >= 1) AND (manager_confidence_score <= 10))),
    CONSTRAINT lab_managers_manager_rank_check CHECK (((manager_rank >= 1) AND (manager_rank <= 3)))
);


--
-- Name: TABLE lab_managers; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.lab_managers IS 'Identified lab managers (up to 3 per lab) with confidence scoring and verification tracking. Used for TDX ticket assignment and lab communication.';


--
-- Name: COLUMN lab_managers.manager_confidence_score; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_managers.manager_confidence_score IS 'Confidence score 1-10 (lower is higher confidence). 1=explicit manager role/code, 5=Research Fellow, 8=Grad students, 9=small lab default.';


--
-- Name: COLUMN lab_managers.is_verified; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_managers.is_verified IS 'Manual verification flag. Set to true after human review confirms manager assignment is correct.';


--
-- Name: COLUMN lab_managers.verification_notes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_managers.verification_notes IS 'Human-entered notes for manual overrides or verification context.';


--
-- Name: COLUMN lab_managers.lab_department_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_managers.lab_department_id IS 'Department ID for the lab (from silver.labs.primary_department_id)';


--
-- Name: COLUMN lab_managers.lab_department_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_managers.lab_department_name IS 'Department name for the lab (from silver.departments.dept_name)';


--
-- Name: COLUMN lab_managers.manager_department_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_managers.manager_department_id IS 'Department ID for the manager (from silver.lab_members.member_department_id)';


--
-- Name: COLUMN lab_managers.manager_department_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_managers.manager_department_name IS 'Department name for the manager (from silver.departments.dept_name)';


--
-- Name: lab_members; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.lab_members (
    membership_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    lab_id character varying(100) NOT NULL,
    member_uniqname character varying(50) NOT NULL,
    member_role text,
    member_first_name character varying(255),
    member_last_name character varying(255),
    member_full_name character varying(255),
    member_department_id character varying(50),
    member_department_name character varying(255),
    silver_user_exists boolean DEFAULT false,
    member_job_title text,
    source_system character varying(50) NOT NULL,
    source_award_ids jsonb DEFAULT '[]'::jsonb,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    is_pi boolean DEFAULT false NOT NULL,
    is_investigator boolean DEFAULT false NOT NULL,
    award_role character varying(100),
    source_group_ids jsonb DEFAULT '[]'::jsonb
);


--
-- Name: TABLE lab_members; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.lab_members IS 'Lab membership records from group membership (primary source) enriched with award data. One record per unique person per lab. Member role comes from job_title, award_role is separate.';


--
-- Name: COLUMN lab_members.member_role; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_members.member_role IS 'Primary role derived from silver.users.job_title. Examples: Graduate Student, Research Fellow, Professor';


--
-- Name: COLUMN lab_members.source_award_ids; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_members.source_award_ids IS 'Array of Award IDs where this person appears (from bronze lab_award data)';


--
-- Name: COLUMN lab_members.is_pi; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_members.is_pi IS 'PI status: true if uniqname=lab_id OR award_role contains Principal Investigator';


--
-- Name: COLUMN lab_members.is_investigator; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_members.is_investigator IS 'Investigator status: true if award_role contains Investigator';


--
-- Name: COLUMN lab_members.award_role; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_members.award_role IS 'Role from award data (if person appears in lab_award records). Examples: UM Principal Investigator, Participating Investigator';


--
-- Name: COLUMN lab_members.source_group_ids; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.lab_members.source_group_ids IS 'Array of group_ids where this person is a member (from silver.groups)';


--
-- Name: labs; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.labs (
    lab_id character varying(100) NOT NULL,
    silver_id uuid DEFAULT public.uuid_generate_v4(),
    pi_uniqname character varying(50) NOT NULL,
    lab_name character varying(255),
    lab_display_name character varying(255),
    primary_department_id character varying(50),
    department_ids jsonb DEFAULT '[]'::jsonb,
    department_names jsonb DEFAULT '[]'::jsonb,
    total_award_dollars numeric(15,2) DEFAULT 0.00,
    total_direct_dollars numeric(15,2) DEFAULT 0.00,
    total_indirect_dollars numeric(15,2) DEFAULT 0.00,
    award_count integer DEFAULT 0,
    active_award_count integer DEFAULT 0,
    earliest_award_start date,
    latest_award_end date,
    has_ad_ou boolean DEFAULT false,
    ad_ou_dn text,
    ad_ou_hierarchy jsonb DEFAULT '[]'::jsonb,
    ad_parent_ou text,
    ad_ou_depth integer,
    computer_count integer DEFAULT 0,
    has_computer_children boolean DEFAULT false,
    has_child_ous boolean DEFAULT false,
    ad_ou_created timestamp with time zone,
    ad_ou_modified timestamp with time zone,
    pi_count integer DEFAULT 0,
    investigator_count integer DEFAULT 0,
    member_count integer DEFAULT 0,
    is_active boolean DEFAULT true NOT NULL,
    has_active_awards boolean DEFAULT false,
    has_active_ou boolean DEFAULT false,
    has_award_data boolean DEFAULT false,
    has_ou_data boolean DEFAULT false,
    data_source character varying(50) NOT NULL,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(100) NOT NULL,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    ou_depth_category character varying(50),
    has_tdx_data boolean DEFAULT false,
    has_tdx_presence boolean DEFAULT false,
    tdx_ci_id integer,
    CONSTRAINT labs_data_quality_score_check CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE labs; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.labs IS 'Tier 3 Composite Entity: Consolidates lab data from TDX, Awards, AD, and Groups. Source of truth for Labs.';


--
-- Name: COLUMN labs.lab_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.lab_id IS 'Primary key: lowercase PI uniqname';


--
-- Name: COLUMN labs.pi_uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.pi_uniqname IS 'Principal Investigator uniqname - must exist in silver.users';


--
-- Name: COLUMN labs.total_award_dollars; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.total_award_dollars IS 'Sum of all award total dollars for this lab';


--
-- Name: COLUMN labs.ad_ou_dn; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.ad_ou_dn IS 'Full AD distinguished name if lab has an OU';


--
-- Name: COLUMN labs.has_award_data; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.has_award_data IS 'Indicates if Award data was successfully merged for this lab';


--
-- Name: COLUMN labs.has_ou_data; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.has_ou_data IS 'Indicates if AD OU data was successfully merged for this lab';


--
-- Name: COLUMN labs.data_source; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.data_source IS 'Concatenated string of data sources (e.g. tdx+award+ad)';


--
-- Name: COLUMN labs.ou_depth_category; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.ou_depth_category IS 'Categorizes labs by OU hierarchy depth from bronze layer:
  - potential_lab: Deep OU likely representing actual lab (ou=labname,ou=pi,...)
  - shallow_ou: Shallow OU likely representing department/division (ou=dept,...)
  - no_ou: Lab without AD OU data (award-only)
Used to filter v_labs_refined view for higher-quality lab identification.';


--
-- Name: COLUMN labs.has_tdx_data; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.has_tdx_data IS 'Indicates if TDX data was successfully merged for this lab';


--
-- Name: COLUMN labs.has_tdx_presence; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.has_tdx_presence IS 'Indicates if the lab has a presence in TDX (e.g. owns computers)';


--
-- Name: COLUMN labs.tdx_ci_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.labs.tdx_ci_id IS 'TeamDynamix Configuration Item ID (Type 10132 - Labs). Populated by sync_tdx_lab_ci_ids.py script. Enables bidirectional sync with TDX for lab CI updates.';


--
-- Name: mcommunity_groups; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.mcommunity_groups (
    mcommunity_group_uid uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    group_email character varying(255) NOT NULL,
    group_name character varying(500),
    distinguished_name text,
    description text,
    gid_number bigint,
    is_private boolean,
    is_members_only boolean,
    is_joinable boolean,
    expiry_timestamp timestamp with time zone,
    owners jsonb DEFAULT '[]'::jsonb,
    members jsonb DEFAULT '[]'::jsonb,
    direct_members jsonb DEFAULT '[]'::jsonb,
    nested_members jsonb DEFAULT '[]'::jsonb,
    requests_to jsonb DEFAULT '[]'::jsonb,
    aliases jsonb DEFAULT '[]'::jsonb,
    contact_info jsonb DEFAULT '{}'::jsonb,
    raw_id uuid,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid
);


--
-- Name: TABLE mcommunity_groups; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.mcommunity_groups IS 'Source-specific silver table for MCommunity groups, preserving raw structure and relationships.';


--
-- Name: mcommunity_users; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.mcommunity_users (
    cn jsonb DEFAULT '[]'::jsonb,
    ou jsonb DEFAULT '[]'::jsonb,
    umich_title text,
    umich_postal_address text,
    umich_postal_address_data text,
    uid_number bigint NOT NULL,
    gid_number bigint NOT NULL,
    home_directory character varying(50),
    login_shell character varying(50),
    object_class jsonb DEFAULT '[]'::jsonb,
    ldap_server character varying(255),
    search_base character varying(255),
    raw_id uuid NOT NULL,
    source_system character varying(50) DEFAULT 'mcommunity_ldap'::character varying NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    uniqname character varying(50),
    first_name character varying(100),
    last_name character varying(100),
    preferred_name character varying(100),
    primary_email character varying(255),
    work_phone character varying(50),
    job_title text,
    cn_aliases jsonb,
    ldap_uid_number bigint,
    ldap_gid_number bigint,
    full_name character varying(255)
);


--
-- Name: TABLE mcommunity_users; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.mcommunity_users IS 'Source-specific silver table for MCommunity LDAP users. Part of two-tier silver architecture - feeds into consolidated silver.users table. Contains typed columns extracted from bronze.raw_entities JSONB data where source_system=mcommunity_ldap.';


--
-- Name: COLUMN mcommunity_users.cn; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.cn IS 'Common names array (JSONB) - can include full name and aliases. First value typically matches displayName.';


--
-- Name: COLUMN mcommunity_users.ou; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.ou IS 'Organizational units array (JSONB) - contains all affiliations (e.g., "LSA MLB Thayer Events & Comm - Faculty and Staff", "College of Lit, Science & Arts - Faculty and Staff"). Critical for determining user roles and access.';


--
-- Name: COLUMN mcommunity_users.umich_title; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.umich_title IS 'Job title from MCommunity - can be very long (up to 611 chars observed). Only 9% of users have this populated.';


--
-- Name: COLUMN mcommunity_users.umich_postal_address; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.umich_postal_address IS 'Postal address in $ delimited format (e.g., "Dept Name $ Building Room $ City State ZIP").';


--
-- Name: COLUMN mcommunity_users.umich_postal_address_data; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.umich_postal_address_data IS 'Structured postal address in key:value format (e.g., "{addr1=...}:{addr2=...}:{city=...}:{state=...}").';


--
-- Name: COLUMN mcommunity_users.uid_number; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.uid_number IS 'POSIX uidNumber - unique numeric identifier for system-level operations. 100% populated.';


--
-- Name: COLUMN mcommunity_users.gid_number; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.gid_number IS 'POSIX gidNumber - primary group ID for system-level operations. 100% populated.';


--
-- Name: COLUMN mcommunity_users.home_directory; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.home_directory IS 'Unix home directory path - typically /users/{uniqname}. 100% populated.';


--
-- Name: COLUMN mcommunity_users.login_shell; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.login_shell IS 'Default login shell - typically /bin/csh, /bin/bash, etc. 99.9% populated.';


--
-- Name: COLUMN mcommunity_users.object_class; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.object_class IS 'LDAP objectClass array - defines what type of entry this is (e.g., inetOrgPerson, posixAccount, umichPerson).';


--
-- Name: COLUMN mcommunity_users.raw_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.raw_id IS 'Link to most recent bronze.raw_entities record for this user - for audit trail and full data access.';


--
-- Name: COLUMN mcommunity_users.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.mcommunity_users.entity_hash IS 'SHA-256 hash of significant fields for change detection - only transform if hash changed from previous version.';
-- Name: TABLE mcommunity_users_backup_20250118; Type: COMMENT; Schema: silver; Owner: -
--
-- Name: tdx_assets; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.tdx_assets (
    tdx_asset_id integer NOT NULL,
    tag character varying(50),
    name character varying(255) NOT NULL,
    uri character varying(255),
    external_id character varying(100),
    serial_number character varying(100),
    app_id integer NOT NULL,
    app_name character varying(255),
    form_id integer NOT NULL,
    form_name character varying(255),
    status_id integer NOT NULL,
    status_name character varying(100),
    parent_id integer,
    parent_tag character varying(50),
    parent_name character varying(255),
    parent_serial_number character varying(100),
    configuration_item_id integer NOT NULL,
    location_id integer,
    location_name character varying(255),
    location_room_id integer,
    location_room_name character varying(255),
    owning_customer_id uuid,
    owning_customer_name character varying(255),
    owning_department_id integer,
    owning_department_name character varying(255),
    requesting_customer_id uuid,
    requesting_customer_name character varying(255),
    requesting_department_id integer,
    requesting_department_name character varying(255),
    purchase_cost numeric(10,2),
    acquisition_date timestamp with time zone,
    expected_replacement_date timestamp with time zone,
    manufacturer_id integer,
    manufacturer_name character varying(255),
    product_model_id integer,
    product_model_name character varying(255),
    supplier_id integer,
    supplier_name character varying(255),
    maintenance_schedule_id integer,
    maintenance_schedule_name character varying(255),
    external_source_id integer,
    external_source_name character varying(255),
    created_uid uuid,
    created_full_name character varying(255),
    created_date timestamp with time zone,
    modified_uid uuid,
    modified_full_name character varying(255),
    modified_date timestamp with time zone,
    attr_mac_address character varying(200),
    attr_ip_address character varying(150),
    attr_operating_system_id integer,
    attr_operating_system_name character varying(255),
    attr_last_inventoried_date timestamp with time zone,
    attr_purchase_shortcode character varying(100),
    attr_function_id integer,
    attr_function_name character varying(255),
    attr_financial_owner_uid uuid,
    attr_financial_owner_name character varying(255),
    attr_support_groups_ids jsonb,
    attr_support_groups_text character varying(500),
    attr_memory character varying(100),
    attr_storage character varying(150),
    attr_processor_count character varying(150),
    attributes jsonb DEFAULT '[]'::jsonb,
    attachments jsonb DEFAULT '[]'::jsonb,
    raw_id uuid NOT NULL,
    raw_data_snapshot jsonb,
    source_system character varying(50) DEFAULT 'tdx'::character varying NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE tdx_assets; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.tdx_assets IS 'Source-specific silver table for TeamDynamix assets. Part of two-tier silver architecture - feeds into consolidated silver.computers/assets tables. Contains typed columns extracted from bronze.raw_entities JSONB data with critical attributes promoted to columns for cross-system matching.';


--
-- Name: COLUMN tdx_assets.tdx_asset_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.tdx_asset_id IS 'Primary key from TDX ID field - unique TeamDynamix asset identifier (INTEGER)';


--
-- Name: COLUMN tdx_assets.tag; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.tag IS 'TDX asset tag/barcode identifier - NULL for some asset types like software licenses (0.01% of assets)';


--
-- Name: COLUMN tdx_assets.configuration_item_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.configuration_item_id IS 'Link to TDX Configuration Item (CI) - 100% of assets have CI associations';


--
-- Name: COLUMN tdx_assets.attr_mac_address; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_mac_address IS 'Extracted from Attributes array - can contain multiple comma-separated MAC addresses (max 138 chars observed)';


--
-- Name: COLUMN tdx_assets.attr_ip_address; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_ip_address IS 'Extracted from Attributes array - can contain multiple comma-separated IP addresses (max 75 chars observed)';


--
-- Name: COLUMN tdx_assets.attr_operating_system_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_operating_system_name IS 'Extracted from Attributes array for OS matching with KeyClient.OS, AD.operatingSystem (89% populated)';


--
-- Name: COLUMN tdx_assets.attr_last_inventoried_date; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_last_inventoried_date IS 'Extracted from Attributes array for compliance queries - when asset was last physically verified (97.1% populated)';


--
-- Name: COLUMN tdx_assets.attr_support_groups_ids; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_support_groups_ids IS 'Extracted from Attributes array - multiselect field stored as JSONB array of choice IDs (99.9% populated)';


--
-- Name: COLUMN tdx_assets.attr_memory; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_memory IS 'Extracted from Attributes array - memory description (e.g., "8GB", or detailed specs up to 64 chars)';


--
-- Name: COLUMN tdx_assets.attr_storage; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_storage IS 'Extracted from Attributes array - storage description (e.g., "1TB", or complex RAID configs up to 110 chars)';


--
-- Name: COLUMN tdx_assets.attr_processor_count; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attr_processor_count IS 'Extracted from Attributes array - processor description can be verbose (max 102 chars observed)';


--
-- Name: COLUMN tdx_assets.attributes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attributes IS 'Complete TDX custom attributes array (JSONB) - includes all 30+ attribute types. Extracted attributes also remain here for audit trail.';


--
-- Name: COLUMN tdx_assets.attachments; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.attachments IS 'TDX attachments array (JSONB) - 38.3% of assets have attachments';


--
-- Name: COLUMN tdx_assets.raw_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.raw_id IS 'Link to most recent bronze.raw_entities record for this asset - for audit trail';


--
-- Name: COLUMN tdx_assets.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_assets.entity_hash IS 'SHA-256 hash of significant fields for change detection - only transform if hash changed';


--
-- Name: tdx_departments; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.tdx_departments (
    tdx_id integer NOT NULL,
    silver_id uuid DEFAULT public.uuid_generate_v4(),
    dept_notes text,
    is_active boolean DEFAULT true NOT NULL,
    parent_id integer,
    manager_uid uuid,
    tdx_created_date timestamp with time zone,
    tdx_modified_date timestamp with time zone,
    location_info jsonb DEFAULT '{}'::jsonb,
    attributes jsonb DEFAULT '[]'::jsonb,
    data_quality_score numeric(3,2) DEFAULT 1.00,
    quality_flags jsonb DEFAULT '[]'::jsonb,
    entity_hash character varying(64) NOT NULL,
    is_enriched boolean DEFAULT false,
    source_bronze_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    dept_id character varying(50),
    department_name character varying(255),
    CONSTRAINT tdx_departments_quality_score_range CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE tdx_departments; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.tdx_departments IS 'Source-specific table for TeamDynamix department data with complete fidelity including custom attributes';


--
-- Name: COLUMN tdx_departments.tdx_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_departments.tdx_id IS 'TeamDynamix Account ID, primary key for write-back operations';


--
-- Name: COLUMN tdx_departments.manager_uid; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_departments.manager_uid IS 'TDX User UID of department manager (00000000-0000-0000-0000-000000000000 treated as NULL)';


--
-- Name: COLUMN tdx_departments.location_info; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_departments.location_info IS 'JSONB object containing address, city, state, postal_code, country, phone, fax, url fields';


--
-- Name: COLUMN tdx_departments.attributes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_departments.attributes IS 'JSONB array of TDX custom attributes with id, name, value, value_text structure';


--
-- Name: COLUMN tdx_departments.data_quality_score; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_departments.data_quality_score IS 'Calculated quality score from 0.00 to 1.00 based on completeness and consistency';


--
-- Name: COLUMN tdx_departments.quality_flags; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_departments.quality_flags IS 'JSONB array of quality issue identifiers';


--
-- Name: COLUMN tdx_departments.is_enriched; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_departments.is_enriched IS 'Boolean flag indicating whether Attributes field has been populated via enrichment';


--
-- Name: tdx_labs; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.tdx_labs (
    tdx_lab_id character varying(100) NOT NULL,
    pi_uniqname character varying(50) NOT NULL,
    lab_name character varying(255),
    computer_count integer DEFAULT 0,
    has_tdx_presence boolean DEFAULT true,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(50) DEFAULT 'tdx'::character varying,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    tdx_user_uid uuid,
    primary_email character varying(255),
    work_phone character varying(50),
    title character varying(255),
    department_name character varying(255),
    company character varying(255),
    is_active boolean,
    department_id character varying(50),
    department_match_method character varying(50),
    department_match_confidence numeric(3,2)
);


--
-- Name: TABLE tdx_labs; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.tdx_labs IS 'Pipeline Helper: Aggregates TDX computer ownership data per PI. Intermediate step for composite labs.';


--
-- Name: COLUMN tdx_labs.department_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_labs.department_name IS 'Derived from default_account_name in TDX';


--
-- Name: COLUMN tdx_labs.department_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_labs.department_id IS 'FK to silver.departments(dept_id), matched from department_name field using fuzzy matching or exact code extraction';


--
-- Name: COLUMN tdx_labs.department_match_method; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_labs.department_match_method IS 'How the department was matched: exact_code (dept code extracted from name), fuzzy_match (similarity matching), pi_inherit (from PI user record)';


--
-- Name: COLUMN tdx_labs.department_match_confidence; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_labs.department_match_confidence IS 'Confidence score 0.00-1.00 for the department match. 1.0 for exact matches, 0.65-0.99 for fuzzy matches';


--
-- Name: tdx_users; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.tdx_users (
    tdx_user_uid uuid NOT NULL,
    uniqname character varying(50),
    external_id character varying(50),
    username character varying(255),
    first_name character varying(255),
    middle_name character varying(255),
    last_name character varying(255),
    full_name character varying(255),
    nickname character varying(255),
    primary_email character varying(255),
    alternate_email character varying(255),
    alert_email character varying(255),
    work_phone character varying(50),
    mobile_phone character varying(50),
    home_phone character varying(50),
    fax character varying(50),
    other_phone character varying(50),
    pager character varying(50),
    im_provider character varying(100),
    im_handle character varying(255),
    work_address character varying(255),
    work_city character varying(100),
    work_state character varying(50),
    work_zip character varying(20),
    work_country character varying(100),
    title character varying(255),
    company character varying(255),
    default_account_id integer,
    default_account_name character varying(255),
    location_id integer,
    location_name character varying(255),
    location_room_id integer,
    location_room_name character varying(255),
    reports_to_uid uuid,
    reports_to_full_name character varying(255),
    is_active boolean DEFAULT true NOT NULL,
    is_employee boolean,
    is_confidential boolean,
    authentication_provider_id integer,
    authentication_user_name character varying(255),
    security_role_id uuid,
    security_role_name character varying(100),
    beid character varying(255),
    beid_int integer,
    default_priority_id integer,
    default_priority_name character varying(100),
    should_report_time boolean,
    is_capacity_managed boolean,
    default_rate numeric(10,2),
    cost_rate numeric(10,2),
    primary_client_portal_application_id integer,
    technician_signature text,
    profile_image_file_name character varying(255),
    apply_technician_signature_to_replies boolean,
    apply_technician_signature_to_updates_and_comments boolean,
    end_date timestamp with time zone,
    report_time_after_date timestamp with time zone,
    attributes jsonb DEFAULT '[]'::jsonb,
    applications jsonb DEFAULT '[]'::jsonb,
    org_applications jsonb DEFAULT '[]'::jsonb,
    group_ids jsonb DEFAULT '[]'::jsonb,
    permissions jsonb DEFAULT '{}'::jsonb,
    raw_id uuid NOT NULL,
    raw_data_snapshot jsonb,
    source_system character varying(50) DEFAULT 'tdx'::character varying NOT NULL,
    entity_hash character varying(64) NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    preferred_name character varying(255),
    job_title character varying(255),
    tdx_account_id integer,
    department_id character varying(10)
);


--
-- Name: TABLE tdx_users; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.tdx_users IS 'Source-specific silver table for TeamDynamix users. Part of two-tier silver architecture - feeds into consolidated silver.users table. Contains typed columns extracted from bronze.raw_entities JSONB data.';


--
-- Name: COLUMN tdx_users.tdx_user_uid; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.tdx_user_uid IS 'Primary key from TDX UID field - unique TeamDynamix user identifier (UUID)';


--
-- Name: COLUMN tdx_users.uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.uniqname IS 'Business key from TDX AlternateID - normalized lowercase uniqname for joining to silver.users';


--
-- Name: COLUMN tdx_users.reports_to_uid; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.reports_to_uid IS 'Logical FK to silver.tdx_users(tdx_user_uid) - supervisor relationship (not enforced per medallion standards)';


--
-- Name: COLUMN tdx_users.attributes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.attributes IS 'Custom TDX attributes array (JSONB) - only 2.8% of users have this populated';


--
-- Name: COLUMN tdx_users.applications; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.applications IS 'TDX application permissions array (JSONB) - only 2.8% of users have this populated';


--
-- Name: COLUMN tdx_users.group_ids; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.group_ids IS 'TDX group membership IDs array (JSONB) - only 2.4% of users have this populated';


--
-- Name: COLUMN tdx_users.raw_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.raw_id IS 'Link to most recent bronze.raw_entities record for this user - for audit trail';


--
-- Name: COLUMN tdx_users.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.tdx_users.entity_hash IS 'SHA-256 hash of significant fields for change detection - only transform if hash changed';


--
-- Name: umapi_departments; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.umapi_departments (
    dept_id character varying(10) NOT NULL,
    silver_id uuid DEFAULT public.uuid_generate_v4(),
    dept_group character varying(50),
    dept_group_campus character varying(20),
    dept_group_vp_area character varying(50),
    college_name character varying(50),
    campus_name character varying(50),
    vp_area_name character varying(50),
    hierarchical_path character varying(255),
    data_quality_score numeric(3,2) DEFAULT 1.00,
    quality_flags jsonb DEFAULT '[]'::jsonb,
    entity_hash character varying(64) NOT NULL,
    source_bronze_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    department_name character varying(255),
    CONSTRAINT umapi_departments_dept_id_check CHECK (((dept_id)::text <> ''::text)),
    CONSTRAINT umapi_departments_quality_score_range CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE umapi_departments; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.umapi_departments IS 'Source-specific table for UMich API department data with organizational hierarchy and campus information';


--
-- Name: COLUMN umapi_departments.dept_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.dept_id IS 'UMich API DeptId, matches dept_code in TDX departments for cross-referencing';


--
-- Name: COLUMN umapi_departments.dept_group; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.dept_group IS 'Machine-readable college/group code (e.g., "COLLEGE_OF_LSA")';


--
-- Name: COLUMN umapi_departments.dept_group_campus; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.dept_group_campus IS 'Machine-readable campus code (e.g., "UM_ANN-ARBOR")';


--
-- Name: COLUMN umapi_departments.dept_group_vp_area; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.dept_group_vp_area IS 'Machine-readable VP area code (e.g., "PRVST_EXC_VP_ACA_AFF")';


--
-- Name: COLUMN umapi_departments.college_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.college_name IS 'Human-readable college/group name (e.g., "College of Lit, Science & Arts")';


--
-- Name: COLUMN umapi_departments.campus_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.campus_name IS 'Human-readable campus name (e.g., "Univ of Mich-Ann-Arbor")';


--
-- Name: COLUMN umapi_departments.vp_area_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.vp_area_name IS 'Human-readable VP area name (e.g., "Provost & Exec VP Academic Aff")';


--
-- Name: COLUMN umapi_departments.hierarchical_path; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.hierarchical_path IS 'Full organizational path from campus to department (e.g., "Univ of Mich-Ann-Arbor -> Provost & Exec VP Academic Aff -> College of Lit, Science & Arts -> LSA Physics")';


--
-- Name: COLUMN umapi_departments.data_quality_score; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.data_quality_score IS 'Calculated quality score from 0.00 to 1.00 based on completeness of hierarchy fields';


--
-- Name: COLUMN umapi_departments.quality_flags; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_departments.quality_flags IS 'JSONB array of quality issue identifiers';


--
-- Name: umapi_employees; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.umapi_employees (
    empl_id character varying(10) NOT NULL,
    empl_rcd smallint NOT NULL,
    silver_id uuid DEFAULT public.uuid_generate_v4(),
    uniqname character varying(10),
    first_name character varying(30) NOT NULL,
    last_name character varying(35),
    full_name character varying(60),
    department_id character varying(10) NOT NULL,
    dept_description character varying(50),
    supervisor_id character varying(10),
    jobcode character varying(10),
    department_job_title character varying(50),
    university_job_title text,
    work_location jsonb DEFAULT '{}'::jsonb,
    data_quality_score numeric(3,2) DEFAULT 1.00,
    quality_flags jsonb DEFAULT '[]'::jsonb,
    raw_id uuid NOT NULL,
    entity_hash character varying(64) NOT NULL,
    source_system character varying(50) DEFAULT 'umich_api'::character varying NOT NULL,
    ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    preferred_name character varying(60),
    job_title text,
    dept_job_title character varying(50),
    work_phone character varying(50),
    work_city character varying(100),
    work_state character varying(50),
    work_postal_code character varying(20),
    work_country character varying(100),
    work_address_line1 character varying(255),
    work_address_line2 character varying(255),
    CONSTRAINT umapi_employees_empl_id_check CHECK (((empl_id)::text <> ''::text)),
    CONSTRAINT umapi_employees_empl_rcd_check CHECK ((empl_rcd >= 0)),
    CONSTRAINT umapi_employees_quality_score_range CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE umapi_employees; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.umapi_employees IS 'Source-specific silver table for UMich API employee records. Part of two-tier silver architecture - feeds into consolidated silver.users table. Stores each employment record separately to handle multi-department employees (dual appointments, multiple job codes).';


--
-- Name: COLUMN umapi_employees.empl_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.empl_id IS 'UMich Employee ID (EmplId) - primary identifier for an employee. Max 8 chars observed, sized to 10 for future growth.';


--
-- Name: COLUMN umapi_employees.empl_rcd; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.empl_rcd IS 'Employment Record number (EmplRcd) - 0 for primary, 1+ for additional appointments. Range 0-511 observed. Employees with multiple records have dual appointments or multiple job codes.';


--
-- Name: COLUMN umapi_employees.uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.uniqname IS 'Business key from UniqName field - normalized lowercase uniqname for joining to silver.users. NULLABLE: 29% of UMAPI employees lack uniqname (temporary workers, students, affiliates).';


--
-- Name: COLUMN umapi_employees.full_name; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.full_name IS 'Full name in "LastName, FirstName" format from UMAPI Name field';


--
-- Name: COLUMN umapi_employees.department_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.department_id IS 'Logical FK to silver.departments(dept_id) - department code (not enforced per medallion standards)';


--
-- Name: COLUMN umapi_employees.supervisor_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.supervisor_id IS 'Logical FK to another empl_id - supervisor relationship. Nullable (6.9% of employees lack supervisor).';


--
-- Name: COLUMN umapi_employees.university_job_title; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.university_job_title IS 'University-wide job title from UniversityJobTitle field. TEXT type to handle 611-character outlier (Provost Emeritus with concatenated titles).';


--
-- Name: COLUMN umapi_employees.work_location; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.work_location IS 'JSONB object containing work address and contact info: {address1, address2, address3, city, state, postal, country, phone, phone_extension}';


--
-- Name: COLUMN umapi_employees.data_quality_score; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.data_quality_score IS 'Calculated quality score 0.00-1.00 based on: has_uniqname (0.30), has_supervisor (0.15), has_phone (0.15), complete_location (0.20), has_jobcode (0.10), has_univ_job_title (0.10)';


--
-- Name: COLUMN umapi_employees.quality_flags; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.quality_flags IS 'JSONB array of quality issue identifiers: missing_uniqname, missing_supervisor, missing_work_phone, incomplete_work_location, missing_jobcode, missing_university_job_title';


--
-- Name: COLUMN umapi_employees.raw_id; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.raw_id IS 'Link to most recent bronze.raw_entities record for this employment record - for audit trail';


--
-- Name: COLUMN umapi_employees.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.umapi_employees.entity_hash IS 'SHA-256 hash of significant fields for change detection - only transform if hash changed';


--
-- Name: users; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.users (
    uniqname character varying(50) NOT NULL,
    silver_id uuid DEFAULT public.uuid_generate_v4(),
    tdx_user_uid uuid,
    umich_empl_id character varying(10),
    umich_empl_ids jsonb DEFAULT '[]'::jsonb,
    ldap_uid_number bigint,
    ldap_gid_number bigint,
    ad_object_guid character varying(255),
    ad_sam_account_name character varying(255),
    ad_object_sid text,
    first_name character varying(255),
    last_name character varying(255),
    full_name character varying(255),
    primary_email character varying(255),
    work_phone character varying(50),
    work_phone_extension character varying(20),
    mobile_phone character varying(50),
    work_address_line1 character varying(255),
    work_address_line2 character varying(255),
    work_address_line3 character varying(255),
    work_city character varying(100),
    work_state character varying(50),
    work_postal_code character varying(20),
    work_country character varying(100),
    department_id character varying(10),
    department_name character varying(255),
    department_ids jsonb DEFAULT '[]'::jsonb,
    job_title text,
    department_job_title character varying(255),
    primary_job_code character varying(10),
    job_codes jsonb DEFAULT '[]'::jsonb,
    primary_supervisor_id character varying(10),
    primary_supervisor_uniqname character varying(50),
    supervisor_ids jsonb DEFAULT '[]'::jsonb,
    reports_to_uid uuid,
    is_pi boolean DEFAULT false,
    is_active boolean DEFAULT true,
    is_employee boolean,
    ad_account_disabled boolean,
    ad_account_locked boolean,
    ad_last_logon timestamp with time zone,
    ad_last_logon_timestamp timestamp with time zone,
    ad_pwd_last_set timestamp with time zone,
    mcommunity_ou_affiliations jsonb DEFAULT '[]'::jsonb,
    ou_department_ids jsonb DEFAULT '[]'::jsonb,
    ad_ou_root character varying(100),
    ad_ou_organization character varying(100),
    ad_ou_department character varying(100),
    ad_ou_full_path jsonb DEFAULT '[]'::jsonb,
    ad_parent_ou_dn character varying(500),
    ad_group_memberships jsonb DEFAULT '[]'::jsonb,
    ad_primary_group_id integer,
    tdx_group_ids jsonb DEFAULT '[]'::jsonb,
    home_directory character varying(255),
    login_shell character varying(50),
    tdx_external_id character varying(255),
    tdx_beid character varying(255),
    tdx_security_role_id uuid,
    tdx_security_role_name character varying(100),
    tdx_is_employee boolean,
    tdx_is_confidential boolean,
    data_quality_score numeric(3,2),
    quality_flags jsonb DEFAULT '[]'::jsonb,
    source_system character varying(200) NOT NULL,
    source_entity_id character varying(255) NOT NULL,
    entity_hash character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id uuid,
    preferred_name character varying(255),
    CONSTRAINT users_data_quality_score_check1 CHECK (((data_quality_score >= 0.00) AND (data_quality_score <= 1.00)))
);


--
-- Name: TABLE users; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON TABLE silver.users IS 'Consolidated users table merging TDX, AD, UMAPI, and MCommunity sources. Comprehensive but not redundant - contains all unique data points from 4 source systems with intelligent merge priority rules. Created 2025-11-23 to replace legacy silver.users table.';


--
-- Name: COLUMN users.uniqname; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.uniqname IS 'Primary business key - normalized lowercase university unique name. Guaranteed unique across all sources.';


--
-- Name: COLUMN users.department_ids; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.department_ids IS 'JSONB array of all departments from UMAPI multiple employment records. Format: [{dept_id, dept_name, empl_rcd}]. Use for users with dual appointments.';


--
-- Name: COLUMN users.job_codes; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.job_codes IS 'JSONB array of all job codes from UMAPI multiple employment records. Format: [{job_code, dept_job_title, empl_rcd}]. Primary job code also available in primary_job_code column.';


--
-- Name: COLUMN users.supervisor_ids; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.supervisor_ids IS 'JSONB array of all supervisors from UMAPI multiple employment records. Format: [{empl_id, uniqname, empl_rcd}]. Primary supervisor also available in primary_supervisor_uniqname column.';


--
-- Name: COLUMN users.is_pi; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.is_pi IS 'Principal Investigator flag derived from silver.lab_awards.person_uniqname UNION silver.ad_organizational_units.extracted_uniqname. Approximately 600 PIs identified.';


--
-- Name: COLUMN users.mcommunity_ou_affiliations; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.mcommunity_ou_affiliations IS 'JSONB array of MCommunity organizational unit strings (e.g., ["LSA - Faculty and Staff", "Alumni"]). 487K users (69% of MCommunity) have only "Alumni" affiliation.';


--
-- Name: COLUMN users.data_quality_score; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.data_quality_score IS 'Calculated quality score 0.00-1.00. Scoring: start 1.00, deduct for missing_email (-0.25), missing_name (-0.20), missing_department (-0.15), missing_job_title (-0.10), not_umapi_employee (-0.10), ad_disabled (-0.10), no_tdx_record (-0.05), mcom_only (-0.15).';


--
-- Name: COLUMN users.source_system; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.source_system IS 'Pipe-delimited list of contributing source systems (e.g., "tdx+umapi+mcom+ad"). Indicates data completeness and provenance.';


--
-- Name: COLUMN users.entity_hash; Type: COMMENT; Schema: silver; Owner: -
--

COMMENT ON COLUMN silver.users.entity_hash IS 'SHA-256 content hash of significant fields for change detection. Only transform record if hash changed from previous version.';
-- Name: TABLE users_legacy; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: COLUMN users_legacy.supervisor_ids; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: v_department_labs; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_department_labs AS
 SELECT d.dept_id,
    d.department_name,
    count(l.lab_id) AS lab_count,
    sum(l.total_award_dollars) AS total_department_funding,
    sum(l.member_count) AS total_lab_members,
    sum(l.computer_count) AS total_lab_computers,
    count(l.lab_id) FILTER (WHERE l.is_active) AS active_lab_count,
    count(l.lab_id) FILTER (WHERE l.has_award_data) AS labs_with_awards,
    count(l.lab_id) FILTER (WHERE l.has_ou_data) AS labs_with_ou
   FROM (silver.departments d
     LEFT JOIN silver.labs l ON (((d.dept_id)::text = (l.primary_department_id)::text)))
  GROUP BY d.dept_id, d.department_name;


--
-- Name: VIEW v_department_labs; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: VIEW v_eligible_lab_members; Type: COMMENT; Schema: silver; Owner: -
--
-- Name: VIEW v_lab_active_awards_legacy; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: v_lab_computers_tdx_reference; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_lab_computers_tdx_reference AS
 SELECT d.tdx_id AS department_tdx_id,
    lc.lab_id,
    lc.computer_id,
    c.tdx_configuration_item_id,
    c.tdx_asset_id AS tdx_computer_id,
    l.tdx_ci_id AS lab_department_tdx_id
   FROM (((silver.lab_computers lc
     JOIN silver.computers c ON (((c.computer_id)::text = (lc.computer_id)::text)))
     JOIN silver.departments d ON ((((((c.ownership_info -> 'tdx_owning'::text) ->> 'department_id'::text))::numeric)::integer = d.tdx_id)))
     JOIN silver.labs l ON (((lc.lab_id)::text = (l.lab_id)::text)))
  WHERE (lc.confidence_score >= 0.65);


--
-- Name: VIEW v_lab_groups; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: v_lab_locations_detailed; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_lab_locations_detailed AS
 SELECT u.department_name,
    lc.lab_id,
    concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), '') AS location_description,
    count(concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), '')) AS computers_with_location_description
   FROM ((silver.computers c
     JOIN silver.lab_computers lc ON (((lc.computer_id)::text = (c.computer_id)::text)))
     JOIN silver.users u ON (((u.uniqname)::text = (lc.lab_id)::text)))
  WHERE (lc.confidence_score >= 0.65)
  GROUP BY u.department_name, lc.lab_id, (concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), ''))
  ORDER BY lc.lab_id, (count(concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), ''))) DESC;


--
-- Name: v_lab_locations_tdx_reference; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_lab_locations_tdx_reference AS
 SELECT u.department_name,
    d.tdx_id AS department_tdx_id,
    lc.lab_id,
    (c.location_info ->> 'room_id'::text) AS room_id,
    (c.location_info ->> 'location_id'::text) AS location_id,
    concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), '') AS location_description,
    count(concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), '')) AS computers_with_location_description
   FROM (((silver.computers c
     JOIN silver.lab_computers lc ON (((lc.computer_id)::text = (c.computer_id)::text)))
     JOIN silver.users u ON (((u.uniqname)::text = (lc.lab_id)::text)))
     JOIN silver.departments d ON (((d.department_code)::text = (u.department_id)::text)))
  WHERE (lc.confidence_score >= 0.65)
  GROUP BY u.department_name, d.tdx_id, lc.lab_id, (c.location_info ->> 'room_id'::text), (c.location_info ->> 'location_id'::text), (concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), ''))
  ORDER BY lc.lab_id, (count(concat((c.location_info ->> 'location_name'::text), ' ', (c.location_info ->> 'room_name'::text), ''))) DESC;


--
-- Name: v_lab_managers_detailed; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_lab_managers_detailed AS
 SELECT d_manager.department_name AS manager_department_full_name,
    lm.lab_id,
    lm.manager_uniqname,
    u.preferred_name,
    u.job_title,
    d_lab.department_name AS lab_department_full_name,
    lm.manager_tdx_uid,
    lm.detection_reason,
    lm.manager_rank,
    lm.manager_confidence_score,
    lm.is_verified,
    lm.verification_notes
   FROM (((silver.lab_managers lm
     JOIN silver.departments d_lab ON (((lm.lab_department_id)::text = (d_lab.department_code)::text)))
     JOIN silver.departments d_manager ON (((lm.manager_department_id)::text = (d_manager.department_code)::text)))
     JOIN silver.users u ON (((lm.manager_uniqname)::text = (u.uniqname)::text)))
  ORDER BY lm.lab_id, lm.manager_rank;


--
-- Name: VIEW v_lab_managers_detailed; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: v_lab_managers_tdx_reference; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_lab_managers_tdx_reference AS
 SELECT lm.lab_id,
    u.tdx_user_uid AS pi_tdx_uid,
    lm.manager_tdx_uid,
    d_lab.tdx_id AS lab_department_tdx_id,
    d_manager.tdx_id AS manager_department_tdx_id
   FROM (((silver.lab_managers lm
     JOIN silver.departments d_lab ON (((lm.lab_department_id)::text = (d_lab.department_code)::text)))
     JOIN silver.departments d_manager ON (((lm.manager_department_id)::text = (d_manager.department_code)::text)))
     JOIN silver.users u ON (((lm.lab_id)::text = (u.uniqname)::text)))
  ORDER BY lm.lab_id;


--
-- Name: v_lab_members_all_tdx_reference; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_lab_members_all_tdx_reference AS
 SELECT lm.membership_id,
    lm.lab_id,
    lm.member_uniqname,
    lm.member_role,
    lm.member_job_title,
    lm.is_pi,
    lm.is_investigator,
    u.job_codes,
    u.tdx_user_uid,
    l.tdx_ci_id
   FROM ((silver.lab_members lm
     JOIN silver.users u ON (((lm.member_uniqname)::text = (u.uniqname)::text)))
     JOIN silver.labs l ON (((lm.lab_id)::text = (l.lab_id)::text)))
  WHERE ((lm.member_role IS NOT NULL) AND ((lm.member_department_id)::text <> ALL (ARRAY[('171240'::character varying)::text, ('481477'::character varying)::text, ('171210'::character varying)::text, ('171220'::character varying)::text, ('171245'::character varying)::text, ('171230'::character varying)::text, ('481207'::character varying)::text, ('309980'::character varying)::text, ('309982'::character varying)::text, ('309981'::character varying)::text, ('315834'::character varying)::text, ('231640'::character varying)::text, ('211600'::character varying)::text, ('481450'::character varying)::text, ('676785'::character varying)::text, ('309919'::character varying)::text, ('309921'::character varying)::text, ('380002'::character varying)::text])) AND (lm.member_role !~~* '%Chief Administrator%'::text) AND ((lm.is_pi = true) OR (u.job_title !~~* '%professor%'::text)));


--
-- Name: VIEW v_lab_members_detailed; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: v_lab_purchase_shortcodes; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_lab_purchase_shortcodes AS
 SELECT u.department_name,
    lc.lab_id,
    (attribute_object.value ->> 'Value'::text) AS shortcode,
    count((attribute_object.value ->> 'Value'::text)) AS num_computers_purchased_on_shortcode
   FROM ((((silver.computers c
     JOIN silver.lab_computers lc ON (((lc.computer_id)::text = (c.computer_id)::text)))
     JOIN silver.users u ON (((u.uniqname)::text = (lc.lab_id)::text)))
     JOIN silver.departments d ON (((d.department_code)::text = (u.department_id)::text)))
     CROSS JOIN LATERAL jsonb_array_elements((c.tdx_attributes -> 'all_attributes'::text)) attribute_object(value))
  WHERE ((lc.confidence_score >= 0.65) AND ((attribute_object.value ->> 'Name'::text) = 'Purchase Shortcode'::text) AND ((c.tdx_status_name)::text = 'Active'::text))
  GROUP BY u.department_name, d.tdx_id, lc.lab_id, (attribute_object.value ->> 'Value'::text)
  ORDER BY lc.lab_id;


--
-- Name: VIEW v_lab_summary; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: v_labs_monitored; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_labs_monitored AS
 SELECT l.lab_id,
    l.lab_name,
    l.lab_display_name,
    l.pi_uniqname,
    l.primary_department_id,
    l.department_ids,
    l.department_names,
    l.member_count,
    l.computer_count,
    l.ad_ou_dn,
    l.ad_ou_hierarchy,
    l.ad_parent_ou,
    l.ad_ou_depth,
    l.has_award_data,
    l.has_ad_ou,
    l.is_active,
    l.source_system,
    l.data_quality_score,
    l.quality_flags,
    l.total_award_dollars,
    l.active_award_count,
    l.tdx_ci_id,
    l.created_at,
    l.updated_at
   FROM silver.labs l
  WHERE ((l.ad_ou_dn IS NOT NULL) AND (l.primary_department_id IS NOT NULL) AND (l.computer_count > 0))
  ORDER BY l.member_count DESC;


--
-- Name: v_labs_refined; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_labs_refined AS
 SELECT labs.lab_id,
    labs.pi_uniqname,
    labs.lab_name,
    labs.lab_display_name,
    labs.primary_department_id,
    labs.department_ids,
    labs.total_award_dollars,
    labs.total_direct_dollars,
    labs.total_indirect_dollars,
    labs.award_count,
    labs.active_award_count,
    labs.earliest_award_start,
    labs.latest_award_end,
    labs.has_ad_ou,
    labs.ad_ou_dn,
    labs.ad_ou_hierarchy,
    labs.ad_ou_depth,
    labs.ou_depth_category,
    labs.computer_count,
    labs.member_count,
    labs.pi_count,
    labs.investigator_count,
    labs.is_active,
    labs.has_active_awards,
    labs.has_active_ou,
    labs.has_award_data,
    labs.has_ou_data,
    labs.data_source,
    labs.data_quality_score,
    labs.quality_flags,
    labs.created_at,
    labs.updated_at
   FROM silver.labs
  WHERE (((labs.ou_depth_category)::text = 'potential_lab'::text) AND (labs.is_active = true))
  ORDER BY labs.lab_name;


--
-- Name: VIEW v_labs_refined; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: v_legitimate_labs; Type: VIEW; Schema: silver; Owner: -
--

CREATE VIEW silver.v_legitimate_labs AS
 SELECT l.lab_id,
    l.lab_name,
    l.pi_uniqname,
    l.member_count,
    l.computer_count,
    l.primary_department_id,
    d.tdx_id AS department_tdx_id
   FROM (silver.labs l
     JOIN silver.departments d ON (((l.primary_department_id)::text = (d.dept_id)::text)))
  WHERE ((l.is_active = true) AND (l.computer_count > 0) AND (d.tdx_id IS NOT NULL));


--
-- Name: VIEW v_legitimate_labs; Type: COMMENT; Schema: silver; Owner: -
--


--
-- Name: raw_entities raw_entities_pkey; Type: CONSTRAINT; Schema: bronze; Owner: -
--

ALTER TABLE ONLY bronze.raw_entities
    ADD CONSTRAINT raw_entities_pkey PRIMARY KEY (raw_id);


--
-- Name: department_masters department_masters_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.department_masters
    ADD CONSTRAINT department_masters_pkey PRIMARY KEY (master_id);


--
-- Name: department_source_mapping department_source_mapping_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.department_source_mapping
    ADD CONSTRAINT department_source_mapping_pkey PRIMARY KEY (mapping_id);


--
-- Name: lab_memberships lab_memberships_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.lab_memberships
    ADD CONSTRAINT lab_memberships_pkey PRIMARY KEY (membership_id);


--
-- Name: user_masters user_masters_canonical_uniqname_key; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.user_masters
    ADD CONSTRAINT user_masters_canonical_uniqname_key UNIQUE (canonical_uniqname);


--
-- Name: user_masters user_masters_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.user_masters
    ADD CONSTRAINT user_masters_pkey PRIMARY KEY (master_id);


--
-- Name: user_source_mapping user_source_mapping_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.user_source_mapping
    ADD CONSTRAINT user_source_mapping_pkey PRIMARY KEY (mapping_id);


--
-- Name: ad_computers ad_computers_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_computers
    ADD CONSTRAINT ad_computers_pkey PRIMARY KEY (sam_account_name);


--
-- Name: ad_groups ad_groups_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_groups
    ADD CONSTRAINT ad_groups_pkey PRIMARY KEY (ad_group_guid);


--
-- Name: ad_labs ad_labs_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_labs
    ADD CONSTRAINT ad_labs_pkey PRIMARY KEY (ad_lab_id);


--
-- Name: ad_organizational_units ad_organizational_units_distinguished_name_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_organizational_units
    ADD CONSTRAINT ad_organizational_units_distinguished_name_key UNIQUE (distinguished_name);


--
-- Name: ad_organizational_units ad_organizational_units_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_organizational_units
    ADD CONSTRAINT ad_organizational_units_pkey PRIMARY KEY (object_guid);


--
-- Name: ad_users ad_users_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_users
    ADD CONSTRAINT ad_users_pkey PRIMARY KEY (ad_user_guid);


--
-- Name: award_labs award_labs_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.award_labs
    ADD CONSTRAINT award_labs_pkey PRIMARY KEY (award_lab_id);


--
-- Name: computer_attributes computer_attributes_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.computer_attributes
    ADD CONSTRAINT computer_attributes_pkey PRIMARY KEY (attribute_id);


--
-- Name: computer_groups computer_groups_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.computer_groups
    ADD CONSTRAINT computer_groups_pkey PRIMARY KEY (membership_id);


--
-- Name: lab_computers computer_labs_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_computers
    ADD CONSTRAINT computer_labs_pkey PRIMARY KEY (association_id);


--
-- Name: computers computers_computer_id_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.computers
    ADD CONSTRAINT computers_computer_id_key UNIQUE (computer_id);


--
-- Name: computers computers_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.computers
    ADD CONSTRAINT computers_pkey PRIMARY KEY (silver_id);


--
-- Name: departments departments_dept_id_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.departments
    ADD CONSTRAINT departments_dept_id_key UNIQUE (dept_id);


--
-- Name: departments departments_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.departments
    ADD CONSTRAINT departments_pkey PRIMARY KEY (dept_id);


--
-- Name: group_members group_members_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.group_members
    ADD CONSTRAINT group_members_pkey PRIMARY KEY (membership_id);


--
-- Name: group_owners group_owners_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.group_owners
    ADD CONSTRAINT group_owners_pkey PRIMARY KEY (ownership_id);


--
-- Name: groups groups_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.groups
    ADD CONSTRAINT groups_pkey PRIMARY KEY (group_id);


--
-- Name: keyconfigure_computers keyconfigure_computers_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.keyconfigure_computers
    ADD CONSTRAINT keyconfigure_computers_pkey PRIMARY KEY (computer_id);


--
-- Name: lab_awards lab_awards_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_awards
    ADD CONSTRAINT lab_awards_pkey PRIMARY KEY (award_record_id);


--
-- Name: lab_managers lab_managers_lab_id_manager_uniqname_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_managers
    ADD CONSTRAINT lab_managers_lab_id_manager_uniqname_key UNIQUE (lab_id, manager_uniqname);


--
-- Name: lab_managers lab_managers_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_managers
    ADD CONSTRAINT lab_managers_pkey PRIMARY KEY (lab_manager_id);


--
-- Name: lab_members lab_members_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_members
    ADD CONSTRAINT lab_members_pkey PRIMARY KEY (membership_id);


--
-- Name: labs labs_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.labs
    ADD CONSTRAINT labs_pkey PRIMARY KEY (lab_id);


--
-- Name: labs labs_silver_id_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.labs
    ADD CONSTRAINT labs_silver_id_key UNIQUE (silver_id);


--
-- Name: mcommunity_groups mcommunity_groups_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.mcommunity_groups
    ADD CONSTRAINT mcommunity_groups_pkey PRIMARY KEY (mcommunity_group_uid);


--
-- Name: departments silver_departments_silver_id_unique; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.departments
    ADD CONSTRAINT silver_departments_silver_id_unique UNIQUE (silver_id);


--
-- Name: tdx_assets tdx_assets_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_assets
    ADD CONSTRAINT tdx_assets_pkey PRIMARY KEY (tdx_asset_id);


--
-- Name: tdx_departments tdx_departments_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_departments
    ADD CONSTRAINT tdx_departments_pkey PRIMARY KEY (tdx_id);


--
-- Name: tdx_departments tdx_departments_silver_id_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_departments
    ADD CONSTRAINT tdx_departments_silver_id_key UNIQUE (silver_id);


--
-- Name: tdx_labs tdx_labs_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_labs
    ADD CONSTRAINT tdx_labs_pkey PRIMARY KEY (tdx_lab_id);


--
-- Name: tdx_users tdx_users_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_users
    ADD CONSTRAINT tdx_users_pkey PRIMARY KEY (tdx_user_uid);


--
-- Name: umapi_departments umapi_departments_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.umapi_departments
    ADD CONSTRAINT umapi_departments_pkey PRIMARY KEY (dept_id);


--
-- Name: umapi_departments umapi_departments_silver_id_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.umapi_departments
    ADD CONSTRAINT umapi_departments_silver_id_key UNIQUE (silver_id);


--
-- Name: umapi_employees umapi_employees_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.umapi_employees
    ADD CONSTRAINT umapi_employees_pkey PRIMARY KEY (empl_id, empl_rcd);


--
-- Name: umapi_employees umapi_employees_silver_id_key; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.umapi_employees
    ADD CONSTRAINT umapi_employees_silver_id_key UNIQUE (silver_id);


--
-- Name: users users_pkey1; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.users
    ADD CONSTRAINT users_pkey1 PRIMARY KEY (uniqname);


--
-- Name: users users_silver_id_key1; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.users
    ADD CONSTRAINT users_silver_id_key1 UNIQUE (silver_id);


--
-- Name: idx_bronze_ad_computer_dn; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ad_computer_dn ON bronze.raw_entities USING btree (((raw_data ->> 'dn'::text))) WHERE (((entity_type)::text = 'computer'::text) AND ((source_system)::text = 'active_directory'::text));


--
-- Name: idx_bronze_ad_computer_guid; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ad_computer_guid ON bronze.raw_entities USING btree (((raw_data ->> 'objectGUID'::text))) WHERE (((entity_type)::text = 'computer'::text) AND ((source_system)::text = 'active_directory'::text));


--
-- Name: idx_bronze_ad_computer_memberof_gin; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ad_computer_memberof_gin ON bronze.raw_entities USING gin (((raw_data -> 'memberOf'::text))) WHERE (((entity_type)::text = 'computer'::text) AND ((source_system)::text = 'active_directory'::text));


--
-- Name: idx_bronze_ad_mcomm_sync; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ad_mcomm_sync ON bronze.raw_entities USING btree (((raw_data ->> 'dn'::text))) WHERE (((entity_type)::text = 'group'::text) AND ((source_system)::text = 'active_directory'::text) AND ((raw_data ->> 'dn'::text) ~ 'MCommADSync'::text));


--
-- Name: INDEX idx_bronze_ad_mcomm_sync; Type: COMMENT; Schema: bronze; Owner: -
--

COMMENT ON INDEX bronze.idx_bronze_ad_mcomm_sync IS 'Identifies AD groups synchronized from MCommunity';


--
-- Name: idx_bronze_ad_uid; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ad_uid ON bronze.raw_entities USING btree (source_system, lower((raw_data ->> 'uid'::text))) WHERE (((entity_type)::text = 'user'::text) AND ((source_system)::text = 'active_directory'::text));


--
-- Name: idx_bronze_computer_mac; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_computer_mac ON bronze.raw_entities USING btree (upper(regexp_replace(COALESCE((raw_data ->> 'MAC'::text), ((raw_data -> 'Attributes'::text) ->> 'MAC Address(es)'::text)), '[^A-F0-9]'::text, ''::text, 'g'::text))) WHERE (((entity_type)::text = ANY ((ARRAY['computer'::character varying, 'asset'::character varying])::text[])) AND (((raw_data ->> 'MAC'::text) IS NOT NULL) OR (((raw_data -> 'Attributes'::text) ->> 'MAC Address(es)'::text) IS NOT NULL)));


--
-- Name: idx_bronze_computer_name; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_computer_name ON bronze.raw_entities USING btree (lower(COALESCE((raw_data ->> 'Name'::text), (raw_data ->> 'cn'::text)))) WHERE ((entity_type)::text = ANY ((ARRAY['computer'::character varying, 'asset'::character varying])::text[]));


--
-- Name: idx_bronze_computer_serial; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_computer_serial ON bronze.raw_entities USING btree (upper(COALESCE((raw_data ->> 'SerialNumber'::text), (raw_data ->> 'OEM SN'::text)))) WHERE (((entity_type)::text = ANY ((ARRAY['computer'::character varying, 'asset'::character varying])::text[])) AND (((raw_data ->> 'SerialNumber'::text) IS NOT NULL) OR ((raw_data ->> 'OEM SN'::text) IS NOT NULL)));


--
-- Name: idx_bronze_computer_source_ingested; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_computer_source_ingested ON bronze.raw_entities USING btree (entity_type, source_system, ingested_at DESC) WHERE ((entity_type)::text = ANY ((ARRAY['computer'::character varying, 'asset'::character varying])::text[]));


--
-- Name: idx_bronze_entity_hash; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_entity_hash ON bronze.raw_entities USING btree (entity_hash);


--
-- Name: idx_bronze_entity_lookup; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_entity_lookup ON bronze.raw_entities USING btree (entity_type, source_system, external_id);


--
-- Name: idx_bronze_group_cn; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_group_cn ON bronze.raw_entities USING btree (source_system, lower(
CASE
    WHEN (jsonb_typeof((raw_data -> 'cn'::text)) = 'array'::text) THEN ((raw_data -> 'cn'::text) ->> 0)
    ELSE (raw_data ->> 'cn'::text)
END)) WHERE ((entity_type)::text = 'group'::text);


--
-- Name: INDEX idx_bronze_group_cn; Type: COMMENT; Schema: bronze; Owner: -
--

COMMENT ON INDEX bronze.idx_bronze_group_cn IS 'Optimizes group lookups by cn (common name), handles MCommunity array format';


--
-- Name: idx_bronze_group_gid; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_group_gid ON bronze.raw_entities USING btree (source_system, ((raw_data ->> 'gidNumber'::text))) WHERE (((entity_type)::text = 'group'::text) AND ((raw_data ->> 'gidNumber'::text) IS NOT NULL));


--
-- Name: INDEX idx_bronze_group_gid; Type: COMMENT; Schema: bronze; Owner: -
--

COMMENT ON INDEX bronze.idx_bronze_group_gid IS 'Optimizes cross-source group matching by POSIX gidNumber';


--
-- Name: idx_bronze_group_members_gin; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_group_members_gin ON bronze.raw_entities USING gin (((raw_data -> 'member'::text))) WHERE ((entity_type)::text = 'group'::text);


--
-- Name: INDEX idx_bronze_group_members_gin; Type: COMMENT; Schema: bronze; Owner: -
--

COMMENT ON INDEX bronze.idx_bronze_group_members_gin IS 'Optimizes member array extraction during transformation';


--
-- Name: idx_bronze_group_owners_gin; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_group_owners_gin ON bronze.raw_entities USING gin (((raw_data -> 'owner'::text))) WHERE (((entity_type)::text = 'group'::text) AND ((source_system)::text = 'mcommunity_ldap'::text));


--
-- Name: INDEX idx_bronze_group_owners_gin; Type: COMMENT; Schema: bronze; Owner: -
--

COMMENT ON INDEX bronze.idx_bronze_group_owners_gin IS 'Optimizes owner array extraction from MCommunity groups';


--
-- Name: idx_bronze_group_source_ingested; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_group_source_ingested ON bronze.raw_entities USING btree (entity_type, source_system, ingested_at DESC) WHERE ((entity_type)::text = 'group'::text);


--
-- Name: INDEX idx_bronze_group_source_ingested; Type: COMMENT; Schema: bronze; Owner: -
--

COMMENT ON INDEX bronze.idx_bronze_group_source_ingested IS 'Supports incremental group transformations by ingestion timestamp';


--
-- Name: idx_bronze_ingestion_time; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ingestion_time ON bronze.raw_entities USING btree (entity_type, ingested_at DESC);


--
-- Name: idx_bronze_kc_last_user; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_kc_last_user ON bronze.raw_entities USING btree (lower((raw_data ->> 'Last User'::text))) WHERE (((entity_type)::text = 'computer'::text) AND ((source_system)::text = 'key_client'::text));


--
-- Name: idx_bronze_kc_owner; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_kc_owner ON bronze.raw_entities USING btree (((raw_data ->> 'Owner'::text))) WHERE (((entity_type)::text = 'computer'::text) AND ((source_system)::text = 'key_client'::text));


--
-- Name: idx_bronze_lab_award_dept; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_lab_award_dept ON bronze.raw_entities USING btree (((raw_data ->> 'Person Appt Department Id'::text))) WHERE ((entity_type)::text = 'lab_award'::text);


--
-- Name: idx_bronze_lab_award_ingested; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_lab_award_ingested ON bronze.raw_entities USING btree (entity_type, source_system, ingested_at DESC) WHERE ((entity_type)::text = 'lab_award'::text);


--
-- Name: idx_bronze_lab_award_role; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_lab_award_role ON bronze.raw_entities USING btree (((raw_data ->> 'Person Role'::text))) WHERE ((entity_type)::text = 'lab_award'::text);


--
-- Name: idx_bronze_lab_award_uniqname; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_lab_award_uniqname ON bronze.raw_entities USING btree (lower((raw_data ->> 'Person Uniqname'::text))) WHERE ((entity_type)::text = 'lab_award'::text);


--
-- Name: idx_bronze_mcom_uid; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_mcom_uid ON bronze.raw_entities USING btree (source_system, lower((raw_data ->> 'uid'::text))) WHERE (((entity_type)::text = 'user'::text) AND ((source_system)::text = 'mcommunity_ldap'::text));


--
-- Name: idx_bronze_ou_extracted_uniqname; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ou_extracted_uniqname ON bronze.raw_entities USING btree (lower((raw_data ->> '_extracted_uniqname'::text))) WHERE (((entity_type)::text = 'organizational_unit'::text) AND ((raw_data ->> '_extracted_uniqname'::text) IS NOT NULL));


--
-- Name: idx_bronze_ou_hierarchy_gin; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ou_hierarchy_gin ON bronze.raw_entities USING gin (((raw_data -> '_ou_hierarchy'::text))) WHERE ((entity_type)::text = 'organizational_unit'::text);


--
-- Name: idx_bronze_ou_ingested; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_ou_ingested ON bronze.raw_entities USING btree (entity_type, source_system, ingested_at DESC) WHERE ((entity_type)::text = 'organizational_unit'::text);


--
-- Name: idx_bronze_raw_data_gin; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_raw_data_gin ON bronze.raw_entities USING gin (raw_data);


--
-- Name: idx_bronze_tdx_alternateid; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_tdx_alternateid ON bronze.raw_entities USING btree (source_system, lower((raw_data ->> 'AlternateID'::text))) WHERE (((entity_type)::text = 'user'::text) AND ((source_system)::text = 'tdx'::text));


--
-- Name: idx_bronze_tdx_asset_id; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_tdx_asset_id ON bronze.raw_entities USING btree ((((raw_data ->> 'ID'::text))::integer)) WHERE (((entity_type)::text = 'asset'::text) AND ((source_system)::text = 'tdx'::text));


--
-- Name: idx_bronze_tdx_attributes_gin; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_tdx_attributes_gin ON bronze.raw_entities USING gin (((raw_data -> 'Attributes'::text))) WHERE (((entity_type)::text = 'asset'::text) AND ((source_system)::text = 'tdx'::text));


--
-- Name: idx_bronze_tdx_dept_id; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_tdx_dept_id ON bronze.raw_entities USING btree ((((raw_data ->> 'OwningDepartmentID'::text))::integer)) WHERE (((entity_type)::text = 'asset'::text) AND ((source_system)::text = 'tdx'::text));


--
-- Name: idx_bronze_tdx_owner_uid; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_tdx_owner_uid ON bronze.raw_entities USING btree (((raw_data ->> 'OwningCustomerID'::text))) WHERE (((entity_type)::text = 'asset'::text) AND ((source_system)::text = 'tdx'::text));


--
-- Name: idx_bronze_umapi_uniqname; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_umapi_uniqname ON bronze.raw_entities USING btree (source_system, lower((raw_data ->> 'UniqName'::text))) WHERE (((entity_type)::text = 'user'::text) AND ((source_system)::text = 'umich_api'::text));


--
-- Name: idx_bronze_user_source_ingested; Type: INDEX; Schema: bronze; Owner: -
--

CREATE INDEX idx_bronze_user_source_ingested ON bronze.raw_entities USING btree (entity_type, source_system, ingested_at DESC) WHERE ((entity_type)::text = 'user'::text);


--
-- Name: idx_ad_computers_additional_attributes_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_additional_attributes_gin ON silver.ad_computers USING gin (additional_attributes) WHERE (additional_attributes IS NOT NULL);


--
-- Name: idx_ad_computers_computer_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_ad_computers_computer_name ON silver.ad_computers USING btree (computer_name);


--
-- Name: idx_ad_computers_dns_hostname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_dns_hostname ON silver.ad_computers USING btree (dns_hostname) WHERE ((dns_hostname IS NOT NULL) AND ((dns_hostname)::text <> ''::text));


--
-- Name: idx_ad_computers_enabled_active; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_enabled_active ON silver.ad_computers USING btree (is_enabled, last_logon_timestamp) WHERE (is_enabled = true);


--
-- Name: idx_ad_computers_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_entity_hash ON silver.ad_computers USING btree (entity_hash);


--
-- Name: idx_ad_computers_ingestion_run_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_ingestion_run_id ON silver.ad_computers USING btree (ingestion_run_id) WHERE (ingestion_run_id IS NOT NULL);


--
-- Name: idx_ad_computers_is_enabled; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_is_enabled ON silver.ad_computers USING btree (is_enabled);


--
-- Name: idx_ad_computers_laps_expiration; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_laps_expiration ON silver.ad_computers USING btree (ms_laps_password_expiration_time) WHERE (ms_laps_password_expiration_time IS NOT NULL);


--
-- Name: idx_ad_computers_last_logon_timestamp; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_last_logon_timestamp ON silver.ad_computers USING btree (last_logon_timestamp);


--
-- Name: idx_ad_computers_managed_by; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_managed_by ON silver.ad_computers USING btree (managed_by) WHERE (managed_by IS NOT NULL);


--
-- Name: idx_ad_computers_member_of_groups_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_member_of_groups_gin ON silver.ad_computers USING gin (member_of_groups);


--
-- Name: idx_ad_computers_object_guid; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_ad_computers_object_guid ON silver.ad_computers USING btree (object_guid);


--
-- Name: idx_ad_computers_operating_system; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_operating_system ON silver.ad_computers USING btree (operating_system) WHERE (operating_system IS NOT NULL);


--
-- Name: idx_ad_computers_ou_category; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_ou_category ON silver.ad_computers USING btree (ou_category) WHERE (ou_category IS NOT NULL);


--
-- Name: idx_ad_computers_ou_department; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_ou_department ON silver.ad_computers USING btree (ou_department) WHERE (ou_department IS NOT NULL);


--
-- Name: idx_ad_computers_ou_division; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_ou_division ON silver.ad_computers USING btree (ou_division) WHERE (ou_division IS NOT NULL);


--
-- Name: idx_ad_computers_ou_full_path_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_ou_full_path_gin ON silver.ad_computers USING gin (ou_full_path);


--
-- Name: idx_ad_computers_ou_immediate_parent; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_ou_immediate_parent ON silver.ad_computers USING btree (ou_immediate_parent) WHERE (ou_immediate_parent IS NOT NULL);


--
-- Name: idx_ad_computers_ou_organization; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_ou_organization ON silver.ad_computers USING btree (ou_organization) WHERE (ou_organization IS NOT NULL);


--
-- Name: idx_ad_computers_pwd_last_set; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_pwd_last_set ON silver.ad_computers USING btree (pwd_last_set);


--
-- Name: idx_ad_computers_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_raw_id ON silver.ad_computers USING btree (raw_id);


--
-- Name: idx_ad_computers_sam_account_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_ad_computers_sam_account_name ON silver.ad_computers USING btree (sam_account_name);


--
-- Name: idx_ad_computers_service_principal_names_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_service_principal_names_gin ON silver.ad_computers USING gin (service_principal_names);


--
-- Name: idx_ad_computers_when_changed; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_when_changed ON silver.ad_computers USING btree (when_changed);


--
-- Name: idx_ad_computers_when_created; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_computers_when_created ON silver.ad_computers USING btree (when_created);


--
-- Name: idx_ad_groups_group_email; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_groups_group_email ON silver.ad_groups USING btree (group_email);


--
-- Name: idx_ad_groups_group_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_groups_group_name ON silver.ad_groups USING btree (group_name);


--
-- Name: idx_ad_groups_ou_department; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_groups_ou_department ON silver.ad_groups USING btree (ou_department);


--
-- Name: idx_ad_groups_ou_organization; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_groups_ou_organization ON silver.ad_groups USING btree (ou_organization);


--
-- Name: idx_ad_labs_department_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_labs_department_id ON silver.ad_labs USING btree (department_id);


--
-- Name: idx_ad_organizational_units_depth_category; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_depth_category ON silver.ad_organizational_units USING btree (depth_category) WHERE (depth_category IS NOT NULL);


--
-- Name: idx_ad_organizational_units_direct_computer_count; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_direct_computer_count ON silver.ad_organizational_units USING btree (direct_computer_count) WHERE (direct_computer_count > 0);


--
-- Name: idx_ad_organizational_units_distinguished_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_ad_organizational_units_distinguished_name ON silver.ad_organizational_units USING btree (distinguished_name);


--
-- Name: idx_ad_organizational_units_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_entity_hash ON silver.ad_organizational_units USING btree (entity_hash);


--
-- Name: idx_ad_organizational_units_extracted_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_extracted_uniqname ON silver.ad_organizational_units USING btree (extracted_uniqname) WHERE (extracted_uniqname IS NOT NULL);


--
-- Name: idx_ad_organizational_units_has_computer_children; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_has_computer_children ON silver.ad_organizational_units USING btree (has_computer_children) WHERE (has_computer_children = true);


--
-- Name: idx_ad_organizational_units_ingestion_run_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ingestion_run_id ON silver.ad_organizational_units USING btree (ingestion_run_id) WHERE (ingestion_run_id IS NOT NULL);


--
-- Name: idx_ad_organizational_units_managed_by; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_managed_by ON silver.ad_organizational_units USING btree (managed_by) WHERE (managed_by IS NOT NULL);


--
-- Name: idx_ad_organizational_units_name_patterns_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_name_patterns_gin ON silver.ad_organizational_units USING gin (name_patterns) WHERE (name_patterns IS NOT NULL);


--
-- Name: idx_ad_organizational_units_object_class_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_object_class_gin ON silver.ad_organizational_units USING gin (object_class);


--
-- Name: idx_ad_organizational_units_ou_category; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ou_category ON silver.ad_organizational_units USING btree (ou_category) WHERE (ou_category IS NOT NULL);


--
-- Name: idx_ad_organizational_units_ou_department; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ou_department ON silver.ad_organizational_units USING btree (ou_department) WHERE (ou_department IS NOT NULL);


--
-- Name: idx_ad_organizational_units_ou_division; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ou_division ON silver.ad_organizational_units USING btree (ou_division) WHERE (ou_division IS NOT NULL);


--
-- Name: idx_ad_organizational_units_ou_full_path_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ou_full_path_gin ON silver.ad_organizational_units USING gin (ou_full_path);


--
-- Name: idx_ad_organizational_units_ou_immediate_parent; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ou_immediate_parent ON silver.ad_organizational_units USING btree (ou_immediate_parent) WHERE (ou_immediate_parent IS NOT NULL);


--
-- Name: idx_ad_organizational_units_ou_organization; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ou_organization ON silver.ad_organizational_units USING btree (ou_organization) WHERE (ou_organization IS NOT NULL);


--
-- Name: idx_ad_organizational_units_ou_status; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_ou_status ON silver.ad_organizational_units USING btree (ou_status) WHERE (ou_status IS NOT NULL);


--
-- Name: idx_ad_organizational_units_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_raw_id ON silver.ad_organizational_units USING btree (raw_id);


--
-- Name: idx_ad_organizational_units_when_changed; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_when_changed ON silver.ad_organizational_units USING btree (when_changed);


--
-- Name: idx_ad_organizational_units_when_created; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_organizational_units_when_created ON silver.ad_organizational_units USING btree (when_created);


--
-- Name: idx_ad_users_first_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_users_first_name ON silver.ad_users USING btree (first_name);


--
-- Name: idx_ad_users_last_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_users_last_name ON silver.ad_users USING btree (last_name);


--
-- Name: idx_ad_users_ldap_uid_number; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_users_ldap_uid_number ON silver.ad_users USING btree (ldap_uid_number);


--
-- Name: idx_ad_users_ou_department; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_users_ou_department ON silver.ad_users USING btree (ou_department);


--
-- Name: idx_ad_users_ou_organization; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_users_ou_organization ON silver.ad_users USING btree (ou_organization);


--
-- Name: idx_ad_users_primary_email; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_users_primary_email ON silver.ad_users USING btree (primary_email);


--
-- Name: idx_ad_users_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_ad_users_uniqname ON silver.ad_users USING btree (uniqname);


--
-- Name: idx_computer_attributes_computer; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_attributes_computer ON silver.computer_attributes USING btree (computer_id);


--
-- Name: idx_computer_attributes_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_attributes_name ON silver.computer_attributes USING btree (lower((attribute_name)::text));


--
-- Name: idx_computer_attributes_unique; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_computer_attributes_unique ON silver.computer_attributes USING btree (computer_id, attribute_name);


--
-- Name: idx_computer_attributes_value_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_attributes_value_hash ON silver.computer_attributes USING hash (attribute_value);


--
-- Name: idx_computer_attributes_value_small; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_attributes_value_small ON silver.computer_attributes USING btree (attribute_value) WHERE (length(attribute_value) < 2000);


--
-- Name: idx_computer_groups_cn; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_groups_cn ON silver.computer_groups USING btree (lower((group_cn)::text));


--
-- Name: idx_computer_groups_computer; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_groups_computer ON silver.computer_groups USING btree (computer_id);


--
-- Name: idx_computer_groups_group; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_groups_group ON silver.computer_groups USING btree (group_id);


--
-- Name: idx_computer_groups_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_groups_source ON silver.computer_groups USING btree (source_system);


--
-- Name: idx_computer_groups_unique; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_computer_groups_unique ON silver.computer_groups USING btree (computer_id, group_dn);


--
-- Name: idx_computer_labs_computer; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_labs_computer ON silver.lab_computers USING btree (computer_id);


--
-- Name: idx_computer_labs_confidence; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_labs_confidence ON silver.lab_computers USING btree (confidence_score DESC);


--
-- Name: idx_computer_labs_lab; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_labs_lab ON silver.lab_computers USING btree (lab_id);


--
-- Name: idx_computer_labs_method; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_labs_method ON silver.lab_computers USING btree (association_method);


--
-- Name: idx_computer_labs_primary; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computer_labs_primary ON silver.lab_computers USING btree (computer_id, is_primary) WHERE (is_primary = true);


--
-- Name: idx_computer_labs_unique; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_computer_labs_unique ON silver.lab_computers USING btree (computer_id, lab_id, association_method);


--
-- Name: idx_computers_financial_owner; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_computers_financial_owner ON silver.computers USING btree (financial_owner_uniqname);


--
-- Name: idx_group_members_direct; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_members_direct ON silver.group_members USING btree (group_id, is_direct_member);


--
-- Name: idx_group_members_group; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_members_group ON silver.group_members USING btree (group_id);


--
-- Name: idx_group_members_nested; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_members_nested ON silver.group_members USING btree (member_group_id) WHERE ((member_type)::text = 'group'::text);


--
-- Name: idx_group_members_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_members_source ON silver.group_members USING btree (source_system);


--
-- Name: idx_group_members_unique_group; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_group_members_unique_group ON silver.group_members USING btree (group_id, member_group_id, source_system) WHERE ((member_type)::text = 'group'::text);


--
-- Name: idx_group_members_unique_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_group_members_unique_user ON silver.group_members USING btree (group_id, member_uniqname, source_system) WHERE ((member_type)::text = 'user'::text);


--
-- Name: idx_group_members_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_members_user ON silver.group_members USING btree (member_uniqname) WHERE ((member_type)::text = 'user'::text);


--
-- Name: idx_group_owners_group; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_owners_group ON silver.group_owners USING btree (group_id);


--
-- Name: idx_group_owners_nested; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_owners_nested ON silver.group_owners USING btree (owner_group_id) WHERE ((owner_type)::text = 'group'::text);


--
-- Name: idx_group_owners_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_owners_source ON silver.group_owners USING btree (source_system);


--
-- Name: idx_group_owners_unique_group; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_group_owners_unique_group ON silver.group_owners USING btree (group_id, owner_group_id) WHERE ((owner_type)::text = 'group'::text);


--
-- Name: idx_group_owners_unique_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_group_owners_unique_user ON silver.group_owners USING btree (group_id, owner_uniqname) WHERE ((owner_type)::text = 'user'::text);


--
-- Name: idx_group_owners_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_group_owners_user ON silver.group_owners USING btree (owner_uniqname) WHERE ((owner_type)::text = 'user'::text);


--
-- Name: idx_groups_email; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_email ON silver.groups USING btree (group_email) WHERE (group_email IS NOT NULL);


--
-- Name: idx_groups_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_entity_hash ON silver.groups USING btree (entity_hash);


--
-- Name: idx_groups_guid; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_guid ON silver.groups USING btree (ad_group_guid) WHERE (ad_group_guid IS NOT NULL);


--
-- Name: idx_groups_is_mcomm_adsync; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_is_mcomm_adsync ON silver.groups USING btree (is_mcomm_adsync) WHERE (is_mcomm_adsync = true);


--
-- Name: idx_groups_mcom_uid; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_mcom_uid ON silver.groups USING btree (mcommunity_group_uid) WHERE (mcommunity_group_uid IS NOT NULL);


--
-- Name: idx_groups_member_of_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_member_of_gin ON silver.groups USING gin (member_of);


--
-- Name: idx_groups_members_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_members_gin ON silver.groups USING gin (members);


--
-- Name: idx_groups_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_name ON silver.groups USING btree (group_name);


--
-- Name: idx_groups_nested_members_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_nested_members_gin ON silver.groups USING gin (nested_members) WHERE (nested_members <> '[]'::jsonb);


--
-- Name: idx_groups_ou_department; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_ou_department ON silver.groups USING btree (ou_department) WHERE (ou_department IS NOT NULL);


--
-- Name: idx_groups_ou_organization; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_ou_organization ON silver.groups USING btree (ou_organization) WHERE (ou_organization IS NOT NULL);


--
-- Name: idx_groups_owners_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_owners_gin ON silver.groups USING gin (owners);


--
-- Name: idx_groups_quality; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_quality ON silver.groups USING btree (data_quality_score DESC);


--
-- Name: idx_groups_sam; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_sam ON silver.groups USING btree (sam_account_name) WHERE (sam_account_name IS NOT NULL);


--
-- Name: idx_groups_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_source ON silver.groups USING btree (source_system);


--
-- Name: idx_groups_updated_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_groups_updated_at ON silver.groups USING btree (updated_at DESC);


--
-- Name: idx_kc_computers_cpu; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_cpu ON silver.keyconfigure_computers USING btree (cpu) WHERE (cpu IS NOT NULL);


--
-- Name: idx_kc_computers_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_entity_hash ON silver.keyconfigure_computers USING btree (entity_hash);


--
-- Name: idx_kc_computers_ingestion_run_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_ingestion_run_id ON silver.keyconfigure_computers USING btree (ingestion_run_id) WHERE (ingestion_run_id IS NOT NULL);


--
-- Name: idx_kc_computers_ip_array; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_ip_array ON silver.keyconfigure_computers USING gin (ip_addresses);


--
-- Name: idx_kc_computers_last_audit; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_last_audit ON silver.keyconfigure_computers USING btree (last_audit) WHERE (last_audit IS NOT NULL);


--
-- Name: idx_kc_computers_last_session; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_last_session ON silver.keyconfigure_computers USING btree (last_session) WHERE (last_session IS NOT NULL);


--
-- Name: idx_kc_computers_mac_array; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_mac_array ON silver.keyconfigure_computers USING gin (mac_addresses);


--
-- Name: idx_kc_computers_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_name ON silver.keyconfigure_computers USING btree (computer_name);


--
-- Name: idx_kc_computers_os; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_os ON silver.keyconfigure_computers USING btree (os) WHERE (os IS NOT NULL);


--
-- Name: idx_kc_computers_os_family; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_os_family ON silver.keyconfigure_computers USING btree (os_family) WHERE (os_family IS NOT NULL);


--
-- Name: idx_kc_computers_owner; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_owner ON silver.keyconfigure_computers USING btree (owner) WHERE (owner IS NOT NULL);


--
-- Name: idx_kc_computers_primary_mac; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_primary_mac ON silver.keyconfigure_computers USING btree (primary_mac_address);


--
-- Name: idx_kc_computers_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_raw_id ON silver.keyconfigure_computers USING btree (raw_id);


--
-- Name: idx_kc_computers_raw_ids_array; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_raw_ids_array ON silver.keyconfigure_computers USING gin (consolidated_raw_ids);


--
-- Name: idx_kc_computers_serial; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_kc_computers_serial ON silver.keyconfigure_computers USING btree (oem_serial_number) WHERE ((oem_serial_number IS NOT NULL) AND ((oem_serial_number)::text <> ''::text));


--
-- Name: idx_lab_awards_award_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_award_id ON silver.lab_awards USING btree (award_id);


--
-- Name: idx_lab_awards_dates; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_dates ON silver.lab_awards USING btree (award_start_date, award_end_date);


--
-- Name: idx_lab_awards_direct_sponsor; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_direct_sponsor ON silver.lab_awards USING btree (direct_sponsor_name) WHERE (direct_sponsor_name IS NOT NULL);


--
-- Name: idx_lab_awards_end_date; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_end_date ON silver.lab_awards USING btree (award_end_date DESC);


--
-- Name: idx_lab_awards_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_entity_hash ON silver.lab_awards USING btree (entity_hash);


--
-- Name: idx_lab_awards_ingestion_run; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_ingestion_run ON silver.lab_awards USING btree (ingestion_run_id) WHERE (ingestion_run_id IS NOT NULL);


--
-- Name: idx_lab_awards_person; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_person ON silver.lab_awards USING btree (person_uniqname);


--
-- Name: idx_lab_awards_person_dept; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_person_dept ON silver.lab_awards USING btree (person_appt_department_id) WHERE (person_appt_department_id IS NOT NULL);


--
-- Name: idx_lab_awards_person_role; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_person_role ON silver.lab_awards USING btree (person_role);


--
-- Name: idx_lab_awards_prime_sponsor; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_prime_sponsor ON silver.lab_awards USING btree (prime_sponsor_name) WHERE (prime_sponsor_name IS NOT NULL);


--
-- Name: idx_lab_awards_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_raw_id ON silver.lab_awards USING btree (raw_id);


--
-- Name: idx_lab_awards_snapshot_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_snapshot_gin ON silver.lab_awards USING gin (raw_data_snapshot) WHERE (raw_data_snapshot IS NOT NULL);


--
-- Name: idx_lab_awards_total_dollars; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_awards_total_dollars ON silver.lab_awards USING btree (award_total_dollars DESC) WHERE (award_total_dollars IS NOT NULL);


--
-- Name: idx_lab_awards_unique; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_lab_awards_unique ON silver.lab_awards USING btree (award_id, person_uniqname, person_role);


--
-- Name: idx_lab_computers_criteria_composite; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_criteria_composite ON silver.lab_computers USING btree (lab_id, owner_is_pi, fin_owner_is_pi, function_is_research) WHERE ((owner_is_pi = true) OR (fin_owner_is_pi = true) OR (function_is_research = true));


--
-- Name: idx_lab_computers_fin_owner_pi; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_fin_owner_pi ON silver.lab_computers USING btree (fin_owner_is_pi) WHERE (fin_owner_is_pi = true);


--
-- Name: idx_lab_computers_fin_owner_pi_high_conf; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_fin_owner_pi_high_conf ON silver.lab_computers USING btree (lab_id, confidence_score DESC) WHERE (fin_owner_is_pi = true);


--
-- Name: idx_lab_computers_function_classroom; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_function_classroom ON silver.lab_computers USING btree (function_is_classroom) WHERE (function_is_classroom = true);


--
-- Name: idx_lab_computers_function_research; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_function_research ON silver.lab_computers USING btree (function_is_research) WHERE (function_is_research = true);


--
-- Name: idx_lab_computers_name_pattern_pi; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_name_pattern_pi ON silver.lab_computers USING btree (lab_id, confidence_score DESC) WHERE ((association_method)::text = 'name_pattern_pi'::text);


--
-- Name: idx_lab_computers_owner_pi; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_owner_pi ON silver.lab_computers USING btree (owner_is_pi) WHERE (owner_is_pi = true);


--
-- Name: idx_lab_computers_quality_flags_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_quality_flags_gin ON silver.lab_computers USING gin (quality_flags);


--
-- Name: idx_lab_computers_tier1_methods; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_tier1_methods ON silver.lab_computers USING btree (association_method, confidence_score DESC) WHERE ((association_method)::text = ANY ((ARRAY['ad_ou_nested'::character varying, 'owner_is_pi'::character varying, 'fin_owner_is_pi'::character varying, 'name_pattern_pi'::character varying])::text[]));


--
-- Name: idx_lab_computers_tier2_methods; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_computers_tier2_methods ON silver.lab_computers USING btree (association_method, confidence_score DESC) WHERE ((association_method)::text = ANY ((ARRAY['group_membership'::character varying, 'owner_member'::character varying, 'last_user_member'::character varying])::text[]));


--
-- Name: idx_lab_managers_confidence; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_managers_confidence ON silver.lab_managers USING btree (manager_confidence_score);


--
-- Name: idx_lab_managers_lab; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_managers_lab ON silver.lab_managers USING btree (lab_id, manager_rank);


--
-- Name: idx_lab_managers_lab_dept; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_managers_lab_dept ON silver.lab_managers USING btree (lab_department_id) WHERE (lab_department_id IS NOT NULL);


--
-- Name: idx_lab_managers_manager_dept; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_managers_manager_dept ON silver.lab_managers USING btree (manager_department_id) WHERE (manager_department_id IS NOT NULL);


--
-- Name: idx_lab_managers_tdx_uid; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_managers_tdx_uid ON silver.lab_managers USING btree (manager_tdx_uid) WHERE (manager_tdx_uid IS NOT NULL);


--
-- Name: idx_lab_managers_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_managers_uniqname ON silver.lab_managers USING btree (manager_uniqname);


--
-- Name: idx_lab_managers_verified; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_managers_verified ON silver.lab_managers USING btree (is_verified) WHERE (is_verified = true);


--
-- Name: idx_lab_members_award_role; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_award_role ON silver.lab_members USING btree (award_role) WHERE (award_role IS NOT NULL);


--
-- Name: idx_lab_members_department; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_department ON silver.lab_members USING btree (member_department_id);


--
-- Name: idx_lab_members_investigator; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_investigator ON silver.lab_members USING btree (lab_id, is_investigator) WHERE (is_investigator = true);


--
-- Name: idx_lab_members_lab; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_lab ON silver.lab_members USING btree (lab_id);


--
-- Name: idx_lab_members_no_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_no_user ON silver.lab_members USING btree (member_uniqname) WHERE (silver_user_exists = false);


--
-- Name: idx_lab_members_role; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_role ON silver.lab_members USING btree (member_role);


--
-- Name: idx_lab_members_silver_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_silver_user ON silver.lab_members USING btree (silver_user_exists, member_uniqname) WHERE (silver_user_exists = true);


--
-- Name: idx_lab_members_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_source ON silver.lab_members USING btree (source_system);


--
-- Name: idx_lab_members_source_awards_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_source_awards_gin ON silver.lab_members USING gin (source_award_ids);


--
-- Name: idx_lab_members_source_groups_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_source_groups_gin ON silver.lab_members USING gin (source_group_ids);


--
-- Name: idx_lab_members_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_lab_members_uniqname ON silver.lab_members USING btree (member_uniqname);


--
-- Name: idx_lab_members_unique_v2; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_lab_members_unique_v2 ON silver.lab_members USING btree (lab_id, member_uniqname);


--
-- Name: idx_labs_depth_category; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_labs_depth_category ON silver.labs USING btree (ou_depth_category) WHERE (ou_depth_category IS NOT NULL);


--
-- Name: idx_labs_tdx_ci_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_labs_tdx_ci_id ON silver.labs USING btree (tdx_ci_id) WHERE (tdx_ci_id IS NOT NULL);


--
-- Name: idx_mcommunity_users_cn_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_cn_gin ON silver.mcommunity_users USING gin (cn);


--
-- Name: idx_mcommunity_users_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_entity_hash ON silver.mcommunity_users USING btree (entity_hash);


--
-- Name: idx_mcommunity_users_first_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_first_name ON silver.mcommunity_users USING btree (first_name);


--
-- Name: idx_mcommunity_users_gid_number; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_gid_number ON silver.mcommunity_users USING btree (gid_number);


--
-- Name: idx_mcommunity_users_ingestion_run; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_ingestion_run ON silver.mcommunity_users USING btree (ingestion_run_id);


--
-- Name: idx_mcommunity_users_last_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_last_name ON silver.mcommunity_users USING btree (last_name);


--
-- Name: idx_mcommunity_users_ldap_gid_number; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_ldap_gid_number ON silver.mcommunity_users USING btree (ldap_gid_number);


--
-- Name: idx_mcommunity_users_ldap_uid_number; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_ldap_uid_number ON silver.mcommunity_users USING btree (ldap_uid_number);


--
-- Name: idx_mcommunity_users_object_class_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_object_class_gin ON silver.mcommunity_users USING gin (object_class);


--
-- Name: idx_mcommunity_users_ou_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_ou_gin ON silver.mcommunity_users USING gin (ou);


--
-- Name: idx_mcommunity_users_primary_email; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_primary_email ON silver.mcommunity_users USING btree (primary_email);


--
-- Name: idx_mcommunity_users_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_raw_id ON silver.mcommunity_users USING btree (raw_id);


--
-- Name: idx_mcommunity_users_uid_number; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_uid_number ON silver.mcommunity_users USING btree (uid_number);


--
-- Name: idx_mcommunity_users_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_mcommunity_users_uniqname ON silver.mcommunity_users USING btree (uniqname);


--
-- Name: idx_silver_ad_groups_cn; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_groups_cn ON silver.ad_groups USING btree (cn);


--
-- Name: idx_silver_ad_groups_dn_pattern; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_groups_dn_pattern ON silver.ad_groups USING btree (distinguished_name text_pattern_ops);


--
-- Name: idx_silver_ad_groups_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_groups_entity_hash ON silver.ad_groups USING btree (entity_hash);


--
-- Name: idx_silver_ad_groups_ingestion_run; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_groups_ingestion_run ON silver.ad_groups USING btree (ingestion_run_id);


--
-- Name: idx_silver_ad_groups_member_of; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_groups_member_of ON silver.ad_groups USING gin (member_of);


--
-- Name: idx_silver_ad_groups_members; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_groups_members ON silver.ad_groups USING gin (members);


--
-- Name: idx_silver_ad_groups_sam_account; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_groups_sam_account ON silver.ad_groups USING btree (sam_account_name);


--
-- Name: idx_silver_ad_users_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_users_entity_hash ON silver.ad_users USING btree (entity_hash);


--
-- Name: idx_silver_ad_users_sam_account_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_ad_users_sam_account_name ON silver.ad_users USING btree (sam_account_name);


--
-- Name: idx_silver_computers_active; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_active ON silver.computers USING btree (is_active, last_seen DESC);


--
-- Name: idx_silver_computers_ad_guid; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_ad_guid ON silver.computers USING btree (ad_object_guid) WHERE (ad_object_guid IS NOT NULL);


--
-- Name: idx_silver_computers_ad_ou_info_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_ad_ou_info_gin ON silver.computers USING gin (ad_ou_info);


--
-- Name: idx_silver_computers_ad_sam; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_ad_sam ON silver.computers USING btree (ad_sam_account_name) WHERE (ad_sam_account_name IS NOT NULL);


--
-- Name: idx_silver_computers_computer_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_silver_computers_computer_id ON silver.computers USING btree (computer_id);


--
-- Name: idx_silver_computers_computer_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_computer_name ON silver.computers USING btree (lower((computer_name)::text));


--
-- Name: idx_silver_computers_created_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_created_at ON silver.computers USING btree (created_at DESC);


--
-- Name: idx_silver_computers_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_entity_hash ON silver.computers USING btree (entity_hash);


--
-- Name: idx_silver_computers_hardware_specs_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_hardware_specs_gin ON silver.computers USING gin (hardware_specs);


--
-- Name: idx_silver_computers_ingestion_run; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_ingestion_run ON silver.computers USING btree (ingestion_run_id) WHERE (ingestion_run_id IS NOT NULL);


--
-- Name: idx_silver_computers_kc_computer_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_kc_computer_id ON silver.computers USING btree (kc_computer_id) WHERE (kc_computer_id IS NOT NULL);


--
-- Name: idx_silver_computers_kc_mac; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_kc_mac ON silver.computers USING btree (kc_mac_address) WHERE (kc_mac_address IS NOT NULL);


--
-- Name: idx_silver_computers_kc_primary_mac; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_kc_primary_mac ON silver.computers USING btree (kc_primary_mac) WHERE (kc_primary_mac IS NOT NULL);


--
-- Name: idx_silver_computers_last_seen; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_last_seen ON silver.computers USING btree (last_seen DESC) WHERE (last_seen IS NOT NULL);


--
-- Name: idx_silver_computers_last_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_last_user ON silver.computers USING btree (lower((last_user)::text)) WHERE (last_user IS NOT NULL);


--
-- Name: idx_silver_computers_location_info_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_location_info_gin ON silver.computers USING gin (location_info);


--
-- Name: idx_silver_computers_mac; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_mac ON silver.computers USING btree (mac_address) WHERE (mac_address IS NOT NULL);


--
-- Name: idx_silver_computers_mac_addresses_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_mac_addresses_gin ON silver.computers USING gin (mac_addresses);


--
-- Name: idx_silver_computers_manufacturer; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_manufacturer ON silver.computers USING btree (manufacturer) WHERE (manufacturer IS NOT NULL);


--
-- Name: idx_silver_computers_name_aliases_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_name_aliases_gin ON silver.computers USING gin (computer_name_aliases);


--
-- Name: idx_silver_computers_os_family; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_os_family ON silver.computers USING btree (os_family) WHERE (os_family IS NOT NULL);


--
-- Name: idx_silver_computers_os_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_os_name ON silver.computers USING btree (os_name) WHERE (os_name IS NOT NULL);


--
-- Name: idx_silver_computers_owner_dept; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_owner_dept ON silver.computers USING btree (owner_department_id) WHERE (owner_department_id IS NOT NULL);


--
-- Name: idx_silver_computers_owner_user; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_owner_user ON silver.computers USING btree (owner_uniqname) WHERE (owner_uniqname IS NOT NULL);


--
-- Name: idx_silver_computers_ownership_info_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_ownership_info_gin ON silver.computers USING gin (ownership_info);


--
-- Name: idx_silver_computers_primary_lab; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_primary_lab ON silver.computers USING btree (primary_lab_id);


--
-- Name: idx_silver_computers_product_model; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_product_model ON silver.computers USING btree (product_model) WHERE (product_model IS NOT NULL);


--
-- Name: idx_silver_computers_quality_flags_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_quality_flags_gin ON silver.computers USING gin (quality_flags);


--
-- Name: idx_silver_computers_quality_score; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_quality_score ON silver.computers USING btree (data_quality_score DESC) WHERE (data_quality_score IS NOT NULL);


--
-- Name: idx_silver_computers_recent_activity; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_recent_activity ON silver.computers USING btree (has_recent_activity, last_seen DESC) WHERE (has_recent_activity = true);


--
-- Name: idx_silver_computers_serial; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_serial ON silver.computers USING btree (upper((serial_number)::text)) WHERE (serial_number IS NOT NULL);


--
-- Name: idx_silver_computers_serial_numbers_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_serial_numbers_gin ON silver.computers USING gin (serial_numbers);


--
-- Name: idx_silver_computers_silver_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_silver_computers_silver_id ON silver.computers USING btree (silver_id);


--
-- Name: idx_silver_computers_tdx_asset_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_tdx_asset_id ON silver.computers USING btree (tdx_asset_id) WHERE (tdx_asset_id IS NOT NULL);


--
-- Name: idx_silver_computers_tdx_attributes_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_tdx_attributes_gin ON silver.computers USING gin (tdx_attributes);


--
-- Name: idx_silver_computers_tdx_status; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_tdx_status ON silver.computers USING btree (tdx_status_id, is_active);


--
-- Name: idx_silver_computers_tdx_tag; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_tdx_tag ON silver.computers USING btree (tdx_tag) WHERE (tdx_tag IS NOT NULL);


--
-- Name: idx_silver_computers_updated_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_computers_updated_at ON silver.computers USING btree (updated_at DESC);


--
-- Name: idx_silver_departments_active; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_active ON silver.departments USING btree (is_active, department_name);


--
-- Name: idx_silver_departments_college; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_college ON silver.departments USING btree (college_name);


--
-- Name: idx_silver_departments_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_entity_hash ON silver.departments USING btree (entity_hash);


--
-- Name: idx_silver_departments_hierarchy; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_hierarchy ON silver.departments USING btree (campus_name, college_group, vp_area);


--
-- Name: idx_silver_departments_location_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_location_gin ON silver.departments USING gin (location_info);


--
-- Name: idx_silver_departments_parent_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_parent_id ON silver.departments USING btree (parent_dept_id);


--
-- Name: idx_silver_departments_quality; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_quality ON silver.departments USING btree (data_quality_score DESC);


--
-- Name: idx_silver_departments_silver_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_silver_id ON silver.departments USING btree (silver_id);


--
-- Name: idx_silver_departments_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_source ON silver.departments USING btree (source_system, source_entity_id);


--
-- Name: idx_silver_departments_tdx_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_tdx_id ON silver.departments USING btree (tdx_id);


--
-- Name: idx_silver_departments_updated_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_departments_updated_at ON silver.departments USING btree (updated_at);


--
-- Name: idx_silver_labs_active; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_active ON silver.labs USING btree (is_active, lab_name);


--
-- Name: idx_silver_labs_active_awards; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_active_awards ON silver.labs USING btree (has_active_awards, latest_award_end) WHERE (has_active_awards = true);


--
-- Name: idx_silver_labs_award_count; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_award_count ON silver.labs USING btree (award_count DESC);


--
-- Name: idx_silver_labs_award_dates; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_award_dates ON silver.labs USING btree (earliest_award_start, latest_award_end);


--
-- Name: idx_silver_labs_award_dollars; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_award_dollars ON silver.labs USING btree (total_award_dollars DESC);


--
-- Name: idx_silver_labs_computer_count; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_computer_count ON silver.labs USING btree (computer_count DESC);


--
-- Name: idx_silver_labs_data_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_data_source ON silver.labs USING btree (data_source, has_award_data, has_ou_data);


--
-- Name: idx_silver_labs_dept_ids_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_dept_ids_gin ON silver.labs USING gin (department_ids);


--
-- Name: idx_silver_labs_dept_names_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_dept_names_gin ON silver.labs USING gin (department_names);


--
-- Name: idx_silver_labs_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_entity_hash ON silver.labs USING btree (entity_hash);


--
-- Name: idx_silver_labs_member_count; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_member_count ON silver.labs USING btree (member_count DESC);


--
-- Name: idx_silver_labs_ou_hierarchy_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_ou_hierarchy_gin ON silver.labs USING gin (ad_ou_hierarchy);


--
-- Name: idx_silver_labs_pi_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_pi_uniqname ON silver.labs USING btree (pi_uniqname);


--
-- Name: idx_silver_labs_primary_dept; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_primary_dept ON silver.labs USING btree (primary_department_id);


--
-- Name: idx_silver_labs_quality; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_quality ON silver.labs USING btree (data_quality_score DESC);


--
-- Name: idx_silver_labs_quality_flags_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_quality_flags_gin ON silver.labs USING gin (quality_flags);


--
-- Name: idx_silver_labs_silver_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_silver_id ON silver.labs USING btree (silver_id);


--
-- Name: idx_silver_labs_with_awards; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_with_awards ON silver.labs USING btree (has_award_data, total_award_dollars DESC) WHERE (has_award_data = true);


--
-- Name: idx_silver_labs_with_ou; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_labs_with_ou ON silver.labs USING btree (has_ad_ou, ad_ou_dn) WHERE (has_ad_ou = true);


--
-- Name: idx_silver_mcommunity_groups_aliases; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_mcommunity_groups_aliases ON silver.mcommunity_groups USING gin (aliases);


--
-- Name: idx_silver_mcommunity_groups_email; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_mcommunity_groups_email ON silver.mcommunity_groups USING btree (group_email);


--
-- Name: idx_silver_mcommunity_groups_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_mcommunity_groups_entity_hash ON silver.mcommunity_groups USING btree (entity_hash);


--
-- Name: idx_silver_mcommunity_groups_ingestion_run; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_mcommunity_groups_ingestion_run ON silver.mcommunity_groups USING btree (ingestion_run_id);


--
-- Name: idx_silver_mcommunity_groups_members; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_mcommunity_groups_members ON silver.mcommunity_groups USING gin (members);


--
-- Name: idx_silver_mcommunity_groups_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_mcommunity_groups_name ON silver.mcommunity_groups USING btree (group_name);


--
-- Name: idx_silver_mcommunity_groups_owners; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_mcommunity_groups_owners ON silver.mcommunity_groups USING gin (owners);


--
-- Name: idx_tdx_assets_attachments_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_attachments_gin ON silver.tdx_assets USING gin (attachments);


--
-- Name: idx_tdx_assets_attributes_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_attributes_gin ON silver.tdx_assets USING gin (attributes);


--
-- Name: idx_tdx_assets_ci_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_ci_id ON silver.tdx_assets USING btree (configuration_item_id);


--
-- Name: idx_tdx_assets_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_entity_hash ON silver.tdx_assets USING btree (entity_hash);


--
-- Name: idx_tdx_assets_financial_owner; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_financial_owner ON silver.tdx_assets USING btree (attr_financial_owner_uid) WHERE (attr_financial_owner_uid IS NOT NULL);


--
-- Name: idx_tdx_assets_form_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_form_id ON silver.tdx_assets USING btree (form_id);


--
-- Name: idx_tdx_assets_form_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_form_name ON silver.tdx_assets USING btree (form_name);


--
-- Name: idx_tdx_assets_function_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_function_id ON silver.tdx_assets USING btree (attr_function_id) WHERE (attr_function_id IS NOT NULL);


--
-- Name: idx_tdx_assets_ingestion_run_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_ingestion_run_id ON silver.tdx_assets USING btree (ingestion_run_id) WHERE (ingestion_run_id IS NOT NULL);


--
-- Name: idx_tdx_assets_ip_address; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_ip_address ON silver.tdx_assets USING btree (attr_ip_address) WHERE ((attr_ip_address IS NOT NULL) AND ((attr_ip_address)::text <> ''::text));


--
-- Name: idx_tdx_assets_last_inventoried; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_last_inventoried ON silver.tdx_assets USING btree (attr_last_inventoried_date) WHERE (attr_last_inventoried_date IS NOT NULL);


--
-- Name: idx_tdx_assets_location_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_location_id ON silver.tdx_assets USING btree (location_id) WHERE (location_id IS NOT NULL);


--
-- Name: idx_tdx_assets_mac_address; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_mac_address ON silver.tdx_assets USING btree (attr_mac_address) WHERE ((attr_mac_address IS NOT NULL) AND ((attr_mac_address)::text <> ''::text));


--
-- Name: idx_tdx_assets_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_name ON silver.tdx_assets USING btree (name);


--
-- Name: idx_tdx_assets_os_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_os_name ON silver.tdx_assets USING btree (attr_operating_system_name) WHERE (attr_operating_system_name IS NOT NULL);


--
-- Name: idx_tdx_assets_owning_customer_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_owning_customer_id ON silver.tdx_assets USING btree (owning_customer_id) WHERE (owning_customer_id IS NOT NULL);


--
-- Name: idx_tdx_assets_owning_department_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_owning_department_id ON silver.tdx_assets USING btree (owning_department_id) WHERE (owning_department_id IS NOT NULL);


--
-- Name: idx_tdx_assets_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_raw_id ON silver.tdx_assets USING btree (raw_id);


--
-- Name: idx_tdx_assets_serial_number; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_serial_number ON silver.tdx_assets USING btree (serial_number) WHERE ((serial_number IS NOT NULL) AND ((serial_number)::text <> ''::text));


--
-- Name: idx_tdx_assets_status_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_status_id ON silver.tdx_assets USING btree (status_id);


--
-- Name: idx_tdx_assets_support_groups_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_assets_support_groups_gin ON silver.tdx_assets USING gin (attr_support_groups_ids);


--
-- Name: idx_tdx_assets_tag; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_tdx_assets_tag ON silver.tdx_assets USING btree (tag);


--
-- Name: idx_tdx_departments_attributes; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_departments_attributes ON silver.tdx_departments USING gin (attributes);


--
-- Name: idx_tdx_departments_department_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_departments_department_name ON silver.tdx_departments USING btree (department_name);


--
-- Name: idx_tdx_departments_dept_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_departments_dept_id ON silver.tdx_departments USING btree (dept_id);


--
-- Name: idx_tdx_departments_is_active; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_departments_is_active ON silver.tdx_departments USING btree (is_active);


--
-- Name: idx_tdx_departments_is_enriched; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_departments_is_enriched ON silver.tdx_departments USING btree (is_enriched);


--
-- Name: idx_tdx_departments_location_info; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_departments_location_info ON silver.tdx_departments USING gin (location_info);


--
-- Name: idx_tdx_departments_updated_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_departments_updated_at ON silver.tdx_departments USING btree (updated_at);


--
-- Name: idx_tdx_labs_department_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_labs_department_id ON silver.tdx_labs USING btree (department_id);


--
-- Name: idx_tdx_users_applications_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_applications_gin ON silver.tdx_users USING gin (applications);


--
-- Name: idx_tdx_users_attributes_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_attributes_gin ON silver.tdx_users USING gin (attributes);


--
-- Name: idx_tdx_users_default_account_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_default_account_id ON silver.tdx_users USING btree (default_account_id);


--
-- Name: idx_tdx_users_department_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_department_id ON silver.tdx_users USING btree (department_id);


--
-- Name: idx_tdx_users_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_entity_hash ON silver.tdx_users USING btree (entity_hash);


--
-- Name: idx_tdx_users_external_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_external_id ON silver.tdx_users USING btree (external_id) WHERE (external_id IS NOT NULL);


--
-- Name: idx_tdx_users_group_ids_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_group_ids_gin ON silver.tdx_users USING gin (group_ids);


--
-- Name: idx_tdx_users_is_active; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_is_active ON silver.tdx_users USING btree (is_active);


--
-- Name: idx_tdx_users_job_title; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_job_title ON silver.tdx_users USING btree (job_title);


--
-- Name: idx_tdx_users_preferred_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_preferred_name ON silver.tdx_users USING btree (preferred_name);


--
-- Name: idx_tdx_users_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_raw_id ON silver.tdx_users USING btree (raw_id);


--
-- Name: idx_tdx_users_reports_to_uid; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_reports_to_uid ON silver.tdx_users USING btree (reports_to_uid) WHERE (reports_to_uid IS NOT NULL);


--
-- Name: idx_tdx_users_tdx_account_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_tdx_users_tdx_account_id ON silver.tdx_users USING btree (tdx_account_id);


--
-- Name: idx_tdx_users_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_tdx_users_uniqname ON silver.tdx_users USING btree (uniqname) WHERE (uniqname IS NOT NULL);


--
-- Name: idx_umapi_departments_campus; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_departments_campus ON silver.umapi_departments USING btree (dept_group_campus);


--
-- Name: idx_umapi_departments_department_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_departments_department_name ON silver.umapi_departments USING btree (department_name);


--
-- Name: idx_umapi_departments_dept_group; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_departments_dept_group ON silver.umapi_departments USING btree (dept_group);


--
-- Name: idx_umapi_departments_hierarchy; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_departments_hierarchy ON silver.umapi_departments USING btree (dept_group_campus, dept_group_vp_area, dept_group);


--
-- Name: idx_umapi_departments_updated_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_departments_updated_at ON silver.umapi_departments USING btree (updated_at);


--
-- Name: idx_umapi_departments_vp_area; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_departments_vp_area ON silver.umapi_departments USING btree (dept_group_vp_area);


--
-- Name: idx_umapi_employees_department_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_department_id ON silver.umapi_employees USING btree (department_id);


--
-- Name: idx_umapi_employees_empl_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_empl_id ON silver.umapi_employees USING btree (empl_id);


--
-- Name: idx_umapi_employees_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_entity_hash ON silver.umapi_employees USING btree (entity_hash);


--
-- Name: idx_umapi_employees_job_title; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_job_title ON silver.umapi_employees USING btree (job_title);


--
-- Name: idx_umapi_employees_quality_score; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_quality_score ON silver.umapi_employees USING btree (data_quality_score DESC);


--
-- Name: idx_umapi_employees_raw_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_raw_id ON silver.umapi_employees USING btree (raw_id);


--
-- Name: idx_umapi_employees_supervisor_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_supervisor_id ON silver.umapi_employees USING btree (supervisor_id) WHERE (supervisor_id IS NOT NULL);


--
-- Name: idx_umapi_employees_uniqname; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_uniqname ON silver.umapi_employees USING btree (uniqname) WHERE (uniqname IS NOT NULL);


--
-- Name: idx_umapi_employees_uniqname_empl_rcd; Type: INDEX; Schema: silver; Owner: -
--

CREATE UNIQUE INDEX idx_umapi_employees_uniqname_empl_rcd ON silver.umapi_employees USING btree (uniqname, empl_rcd) WHERE (uniqname IS NOT NULL);


--
-- Name: idx_umapi_employees_updated_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_updated_at ON silver.umapi_employees USING btree (updated_at);


--
-- Name: idx_umapi_employees_work_city; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_work_city ON silver.umapi_employees USING btree (work_city);


--
-- Name: idx_umapi_employees_work_location_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_work_location_gin ON silver.umapi_employees USING gin (work_location);


--
-- Name: idx_umapi_employees_work_phone; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_umapi_employees_work_phone ON silver.umapi_employees USING btree (work_phone);


--
-- Name: idx_users_ad_groups_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_ad_groups_gin ON silver.users USING gin (ad_group_memberships);


--
-- Name: idx_users_ad_object_guid; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_ad_object_guid ON silver.users USING btree (ad_object_guid) WHERE (ad_object_guid IS NOT NULL);


--
-- Name: idx_users_ad_sam_account_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_ad_sam_account_name ON silver.users USING btree (ad_sam_account_name) WHERE (ad_sam_account_name IS NOT NULL);


--
-- Name: idx_users_department; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_department ON silver.users USING btree (department_id) WHERE (department_id IS NOT NULL);


--
-- Name: idx_users_dept_ids_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_dept_ids_gin ON silver.users USING gin (department_ids);


--
-- Name: idx_users_email; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_email ON silver.users USING btree (primary_email) WHERE (primary_email IS NOT NULL);


--
-- Name: idx_users_entity_hash; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_entity_hash ON silver.users USING btree (entity_hash);


--
-- Name: idx_users_full_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_full_name ON silver.users USING btree (full_name);


--
-- Name: idx_users_ingestion_run; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_ingestion_run ON silver.users USING btree (ingestion_run_id);


--
-- Name: idx_users_is_active; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_is_active ON silver.users USING btree (is_active);


--
-- Name: idx_users_is_employee; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_is_employee ON silver.users USING btree (is_employee) WHERE (is_employee = true);


--
-- Name: idx_users_is_pi; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_is_pi ON silver.users USING btree (is_pi) WHERE (is_pi = true);


--
-- Name: idx_users_job_codes_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_job_codes_gin ON silver.users USING gin (job_codes);


--
-- Name: idx_users_ldap_uid_number; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_ldap_uid_number ON silver.users USING btree (ldap_uid_number) WHERE (ldap_uid_number IS NOT NULL);


--
-- Name: idx_users_mcom_ou_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_mcom_ou_gin ON silver.users USING gin (mcommunity_ou_affiliations);


--
-- Name: idx_users_ou_dept_ids_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_ou_dept_ids_gin ON silver.users USING gin (ou_department_ids);


--
-- Name: idx_users_preferred_name; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_preferred_name ON silver.users USING btree (preferred_name);


--
-- Name: idx_users_primary_job_code; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_primary_job_code ON silver.users USING btree (primary_job_code) WHERE (primary_job_code IS NOT NULL);


--
-- Name: idx_users_primary_supervisor; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_primary_supervisor ON silver.users USING btree (primary_supervisor_uniqname) WHERE (primary_supervisor_uniqname IS NOT NULL);


--
-- Name: idx_users_quality_flags_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_quality_flags_gin ON silver.users USING gin (quality_flags);


--
-- Name: idx_users_quality_score; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_quality_score ON silver.users USING btree (data_quality_score DESC);


--
-- Name: idx_users_silver_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_silver_id ON silver.users USING btree (silver_id);


--
-- Name: idx_users_source; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_source ON silver.users USING btree (source_system, source_entity_id);


--
-- Name: idx_users_supervisor_ids_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_supervisor_ids_gin ON silver.users USING gin (supervisor_ids);


--
-- Name: idx_users_tdx_groups_gin; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_tdx_groups_gin ON silver.users USING gin (tdx_group_ids);


--
-- Name: idx_users_tdx_user_uid; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_tdx_user_uid ON silver.users USING btree (tdx_user_uid) WHERE (tdx_user_uid IS NOT NULL);


--
-- Name: idx_users_umich_empl_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_umich_empl_id ON silver.users USING btree (umich_empl_id) WHERE (umich_empl_id IS NOT NULL);


--
-- Name: idx_users_updated_at; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_users_updated_at ON silver.users USING btree (updated_at);


--
-- Name: department_masters update_gold_department_masters_updated_at; Type: TRIGGER; Schema: gold; Owner: -
--

CREATE TRIGGER update_gold_department_masters_updated_at BEFORE UPDATE ON gold.department_masters FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: lab_memberships update_gold_lab_memberships_updated_at; Type: TRIGGER; Schema: gold; Owner: -
--

CREATE TRIGGER update_gold_lab_memberships_updated_at BEFORE UPDATE ON gold.lab_memberships FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: user_masters update_gold_user_masters_updated_at; Type: TRIGGER; Schema: gold; Owner: -
--

CREATE TRIGGER update_gold_user_masters_updated_at BEFORE UPDATE ON gold.user_masters FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: lab_managers update_lab_managers_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_lab_managers_updated_at BEFORE UPDATE ON silver.lab_managers FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: ad_groups update_silver_ad_groups_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_ad_groups_updated_at BEFORE UPDATE ON silver.ad_groups FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: computers update_silver_computers_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_computers_updated_at BEFORE UPDATE ON silver.computers FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: departments update_silver_departments_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_departments_updated_at BEFORE UPDATE ON silver.departments FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: groups update_silver_groups_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_groups_updated_at BEFORE UPDATE ON silver.groups FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: labs update_silver_labs_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_labs_updated_at BEFORE UPDATE ON silver.labs FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: mcommunity_groups update_silver_mcommunity_groups_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_mcommunity_groups_updated_at BEFORE UPDATE ON silver.mcommunity_groups FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: mcommunity_users update_silver_mcommunity_users_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_mcommunity_users_updated_at BEFORE UPDATE ON silver.mcommunity_users FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: users update_silver_users_updated_at; Type: TRIGGER; Schema: silver; Owner: -
--

CREATE TRIGGER update_silver_users_updated_at BEFORE UPDATE ON silver.users FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: raw_entities raw_entities_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: bronze; Owner: -
--

ALTER TABLE ONLY bronze.raw_entities
    ADD CONSTRAINT raw_entities_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: department_source_mapping department_source_mapping_master_id_fkey; Type: FK CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.department_source_mapping
    ADD CONSTRAINT department_source_mapping_master_id_fkey FOREIGN KEY (master_id) REFERENCES gold.department_masters(master_id) ON DELETE CASCADE;


--
-- Name: department_source_mapping department_source_mapping_silver_id_fkey; Type: FK CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.department_source_mapping
    ADD CONSTRAINT department_source_mapping_silver_id_fkey FOREIGN KEY (silver_id) REFERENCES silver.departments(silver_id) ON DELETE CASCADE;


--
-- Name: lab_memberships lab_memberships_department_master_id_fkey; Type: FK CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.lab_memberships
    ADD CONSTRAINT lab_memberships_department_master_id_fkey FOREIGN KEY (department_master_id) REFERENCES gold.department_masters(master_id);


--
-- Name: lab_memberships lab_memberships_user_master_id_fkey; Type: FK CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.lab_memberships
    ADD CONSTRAINT lab_memberships_user_master_id_fkey FOREIGN KEY (user_master_id) REFERENCES gold.user_masters(master_id) ON DELETE CASCADE;


--
-- Name: user_masters user_masters_primary_department_id_fkey; Type: FK CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.user_masters
    ADD CONSTRAINT user_masters_primary_department_id_fkey FOREIGN KEY (primary_department_id) REFERENCES gold.department_masters(master_id);


--
-- Name: user_source_mapping user_source_mapping_master_id_fkey; Type: FK CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.user_source_mapping
    ADD CONSTRAINT user_source_mapping_master_id_fkey FOREIGN KEY (master_id) REFERENCES gold.user_masters(master_id) ON DELETE CASCADE;


--
-- Name: ad_computers ad_computers_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_computers
    ADD CONSTRAINT ad_computers_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: ad_groups ad_groups_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_groups
    ADD CONSTRAINT ad_groups_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: ad_groups ad_groups_raw_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_groups
    ADD CONSTRAINT ad_groups_raw_id_fkey FOREIGN KEY (raw_id) REFERENCES bronze.raw_entities(raw_id);


--
-- Name: ad_labs ad_labs_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_labs
    ADD CONSTRAINT ad_labs_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: ad_labs ad_labs_pi_uniqname_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_labs
    ADD CONSTRAINT ad_labs_pi_uniqname_fkey FOREIGN KEY (pi_uniqname) REFERENCES silver.users(uniqname);


--
-- Name: ad_organizational_units ad_organizational_units_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.ad_organizational_units
    ADD CONSTRAINT ad_organizational_units_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: award_labs award_labs_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.award_labs
    ADD CONSTRAINT award_labs_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: award_labs award_labs_pi_uniqname_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.award_labs
    ADD CONSTRAINT award_labs_pi_uniqname_fkey FOREIGN KEY (pi_uniqname) REFERENCES silver.users(uniqname);


--
-- Name: award_labs award_labs_primary_department_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.award_labs
    ADD CONSTRAINT award_labs_primary_department_id_fkey FOREIGN KEY (primary_department_id) REFERENCES silver.departments(dept_id);


--
-- Name: lab_computers computer_labs_lab_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_computers
    ADD CONSTRAINT computer_labs_lab_id_fkey FOREIGN KEY (lab_id) REFERENCES silver.labs(lab_id) ON DELETE CASCADE;


--
-- Name: computers computers_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.computers
    ADD CONSTRAINT computers_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: departments departments_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.departments
    ADD CONSTRAINT departments_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: computers fk_computers_financial_owner; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.computers
    ADD CONSTRAINT fk_computers_financial_owner FOREIGN KEY (financial_owner_uniqname) REFERENCES silver.users(uniqname) ON DELETE SET NULL;


--
-- Name: lab_members fk_lab_members_department; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_members
    ADD CONSTRAINT fk_lab_members_department FOREIGN KEY (member_department_id) REFERENCES silver.departments(dept_id) ON DELETE SET NULL;


--
-- Name: labs fk_labs_primary_department; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.labs
    ADD CONSTRAINT fk_labs_primary_department FOREIGN KEY (primary_department_id) REFERENCES silver.departments(dept_id) ON DELETE SET NULL;


--
-- Name: groups groups_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.groups
    ADD CONSTRAINT groups_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: keyconfigure_computers keyconfigure_computers_ingestion_run_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.keyconfigure_computers
    ADD CONSTRAINT keyconfigure_computers_ingestion_run_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: lab_awards lab_awards_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_awards
    ADD CONSTRAINT lab_awards_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: lab_computers lab_computers_lab_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_computers
    ADD CONSTRAINT lab_computers_lab_id_fkey FOREIGN KEY (lab_id) REFERENCES silver.labs(lab_id) ON DELETE CASCADE;


--
-- Name: lab_managers lab_managers_lab_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_managers
    ADD CONSTRAINT lab_managers_lab_id_fkey FOREIGN KEY (lab_id) REFERENCES silver.labs(lab_id) ON DELETE CASCADE;


--
-- Name: lab_members lab_members_lab_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_members
    ADD CONSTRAINT lab_members_lab_id_fkey FOREIGN KEY (lab_id) REFERENCES silver.labs(lab_id) ON DELETE CASCADE;


--
-- Name: labs labs_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.labs
    ADD CONSTRAINT labs_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: mcommunity_groups mcommunity_groups_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.mcommunity_groups
    ADD CONSTRAINT mcommunity_groups_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: mcommunity_groups mcommunity_groups_raw_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.mcommunity_groups
    ADD CONSTRAINT mcommunity_groups_raw_id_fkey FOREIGN KEY (raw_id) REFERENCES bronze.raw_entities(raw_id);


--
-- Name: mcommunity_users mcommunity_users_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.mcommunity_users
    ADD CONSTRAINT mcommunity_users_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: tdx_assets tdx_assets_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_assets
    ADD CONSTRAINT tdx_assets_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: tdx_departments tdx_departments_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_departments
    ADD CONSTRAINT tdx_departments_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: tdx_labs tdx_labs_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_labs
    ADD CONSTRAINT tdx_labs_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: tdx_labs tdx_labs_pi_uniqname_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_labs
    ADD CONSTRAINT tdx_labs_pi_uniqname_fkey FOREIGN KEY (pi_uniqname) REFERENCES silver.users(uniqname);


--
-- Name: tdx_users tdx_users_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.tdx_users
    ADD CONSTRAINT tdx_users_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: umapi_departments umapi_departments_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.umapi_departments
    ADD CONSTRAINT umapi_departments_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: umapi_employees umapi_employees_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.umapi_employees
    ADD CONSTRAINT umapi_employees_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);


--
-- Name: users users_ingestion_run_id_fkey1; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.users
    ADD CONSTRAINT users_ingestion_run_id_fkey1 FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);







--
-- Name: lab_managers lab_managers_manager_uniqname_fkey; Type: FK CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.lab_managers
    ADD CONSTRAINT lab_managers_manager_uniqname_fkey FOREIGN KEY (manager_uniqname) REFERENCES silver.users(uniqname) ON DELETE CASCADE;
