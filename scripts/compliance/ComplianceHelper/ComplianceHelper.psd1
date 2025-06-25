#
# Module manifest for module 'ComplianceHelper'
#

@{
    # Script module or binary module file associated with this manifest.
    RootModule = 'ComplianceHelper.psm1'

    # Version number of this module.
    ModuleVersion = '1.0.0'

    # Supported PSEditions
    CompatiblePSEditions = @('Desktop', 'Core')

    # ID used to uniquely identify this module
    GUID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'

    # Author of this module
    Author = 'LSATS Data Hub'

    # Company or vendor of this module
    CompanyName = 'LSA Technology Services'

    # Copyright statement for this module
    Copyright = '(c) 2025 LSA Technology Services. All rights reserved.'

    # Description of the functionality provided by this module
    Description = 'PowerShell module for LSATS compliance automation tools. Provides native PowerShell access to compliance-automator, compliance-update, and compliance-third-outreach commands with transparent virtual environment management.'

    # Minimum version of the PowerShell engine required by this module
    PowerShellVersion = '5.1'

    # Functions to export from this module, for best performance, do not use wildcards and do not delete the entry
    FunctionsToExport = @(
        'Invoke-ComplianceAutomator',
        'Update-Compliance',
        'Invoke-ComplianceEscalation',
        'Show-ComplianceMenu'
    )

    # Cmdlets to export from this module, for best performance, do not use wildcards and do not delete the entry
    CmdletsToExport = @()

    # Variables to export from this module
    VariablesToExport = @()

    # Aliases to export from this module, for best performance, do not use wildcards and do not delete the entry
    AliasesToExport = @()

    # Private data to pass to the module specified in RootModule/ModuleToProcess
    PrivateData = @{
        PSData = @{
            # Tags applied to this module. These help with module discovery in online galleries.
            Tags = @('Compliance', 'Automation', 'TeamDynamix', 'LSATS', 'LSA')

            # A URL to the license for this module.
            LicenseUri = ''

            # A URL to the main website for this project.
            ProjectUri = 'https://github.com/3Bodhi/LSATS_Data_Hub'

            # A URL to an icon representing this module.
            IconUri = ''

            # Release notes for this module
            ReleaseNotes = @'
v1.0.0
- Initial release
- Native PowerShell access to compliance automation tools
- Transparent virtual environment management
- Interactive compliance menu system
- Support for compliance-automator, compliance-update, and compliance-third-outreach
'@

            # Prerelease string of this module
            Prerelease = ''

            # Flag to indicate whether the module requires explicit user acceptance for install/update/save
            RequireLicenseAcceptance = $false

            # External dependent modules of this module
            ExternalModuleDependencies = @()

        } # End of PSData hashtable

    } # End of PrivateData hashtable

    # HelpInfo URI of this module
    HelpInfoURI = ''

    # Default prefix for commands exported from this module. Override the default prefix using Import-Module -Prefix.
    DefaultCommandPrefix = ''
}
