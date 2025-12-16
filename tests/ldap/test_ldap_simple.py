#!/usr/bin/env python3
"""
Simple LDAP Adapter Test Script

A minimal script to quickly test LDAP adapter methods against both
Active Directory and MCommunity with one-line function calls.

Usage:
    python test_ldap_simple.py

Then use the 'ad' and 'mc' objects to test methods:
    >>> ad.search_users(search_term='john', max_results=5)
    >>> mc.search_groups(search_term='lsa', max_results=10)
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ldap.adapters.ldap_adapter import LDAPAdapter

# ============================================================================
# SETUP - Initialize both adapters
# ============================================================================

print("\n" + "=" * 80)
print("SIMPLE LDAP ADAPTER TEST")
print("=" * 80 + "\n")

# Active Directory configuration
ad_config = {
    "server": "adsroot.itcs.umich.edu",
    "search_base": "OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
    "user": "umroot\\myodhes1",
    "keyring_service": "ldap_umich",
    "port": 636,
    "use_ssl": True,
}

# MCommunity configuration
mc_config = {
    "server": "ldap.umich.edu",
    "search_base": "dc=umich,dc=edu",
    "user": "uid=myodhes,ou=People,dc=umich,dc=edu",
    "keyring_service": "Mcom_umich",
    "port": 636,
    "use_ssl": True,
}

print("🔌 Connecting to Active Directory...")
ad = LDAPAdapter(ad_config)
if ad.test_connection():
    print("✅ Active Directory connected!\n")
else:
    print("❌ Active Directory connection failed!\n")
    ad = None

print("🔌 Connecting to MCommunity...")
mc = LDAPAdapter(mc_config)
if mc.test_connection():
    print("✅ MCommunity connected!\n")
else:
    print("❌ MCommunity connection failed!\n")
    mc = None

# ============================================================================
# HELPER FUNCTIONS - For pretty printing results
# ============================================================================


def print_results(results, title="Results", max_display=5):
    """Pretty print LDAP search results."""
    print(f"\n{'=' * 80}")
    print(f"{title}")
    print(f"{'=' * 80}")
    print(
        f"Found {len(results)} results (showing first {min(len(results), max_display)}):\n"
    )

    for i, entry in enumerate(results[:max_display], 1):
        print(f"{i}. DN: {entry.entry_dn}")
        # Show first few interesting attributes
        attrs_shown = 0
        for attr_name in entry.entry_attributes:
            if attrs_shown >= 3:
                break
            if attr_name.lower() not in ["objectclass", "objectguid", "objectsid"]:
                attr_value = getattr(entry, attr_name)
                if hasattr(attr_value, "value") and attr_value.value:
                    print(f"   {attr_name}: {attr_value.value}")
                    attrs_shown += 1
        print()

    if len(results) > max_display:
        print(f"... and {len(results) - max_display} more results\n")


def print_count(count, description="objects"):
    """Pretty print a count result."""
    print(f"\n📊 Found {count:,} {description}\n")


def print_dict_results(results, title="Results", max_display=5):
    """Pretty print dictionary results."""
    print(f"\n{'=' * 80}")
    print(f"{title}")
    print(f"{'=' * 80}")
    print(
        f"Found {len(results)} results (showing first {min(len(results), max_display)}):\n"
    )

    for i, item in enumerate(results[:max_display], 1):
        print(f"{i}. {item.get('dn', 'No DN')}")
        for key, value in list(item.items())[:5]:
            if key != "dn":
                print(f"   {key}: {value}")
        print()

    if len(results) > max_display:
        print(f"... and {len(results) - max_display} more results\n")


# ============================================================================
# QUICK TEST EXAMPLES
# ============================================================================

print("=" * 80)
print("READY TO USE!")
print("=" * 80)
print("\nYou now have two adapters ready to use:")
print("  • ad  - Active Directory adapter")
print("  • mc  - MCommunity adapter")
print("\nTry these commands:\n")

print("# Count OUs in Active Directory")
print("count = ad.count_search_results('(objectClass=organizationalUnit)')")
print("print_count(count, 'organizational units')")
print()

print("# Search for users in Active Directory")
print("users = ad.search_users(max_results=5)")
print("print_results(users, 'AD Users')")
print()

print("# List top-level OUs in AD")
print("ous = ad.search_organizational_units(max_results=10)")
print("print_results(ous, 'Top-Level OUs')")
print()

print("# Search for people in MCommunity")
print("people = mc.search_users(search_term='lsa', max_results=5)")
print("print_results(people, 'MCommunity People')")
print()

print("# Count groups in MCommunity")
print("count = mc.count_search_results('(objectClass=groupOfNames)')")
print("print_count(count, 'groups')")
print()

print("# Search for LSA groups in MCommunity")
print("groups = mc.search_groups(search_term='lsa', max_results=5)")
print("print_results(groups, 'LSA Groups')")
print()

print("# Get results as dictionaries (easier to work with)")
print("people_dicts = mc.search_as_dicts(")
print("    search_filter='(uid=myodhes)',")
print("    search_base='ou=People,dc=umich,dc=edu',")
print("    scope='subtree',")
print("    max_results=1")
print(")")
print("print_dict_results(people_dicts, 'My MCommunity Record')")
print()

print("# Extract all users from a specific OU")
print("lsa_users = ad.extract_users_from_ou(")
print("    'OU=LSA,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu',")
print("    include_nested=False,")
print("    attributes=['cn', 'mail', 'title']")
print(")")
print("print_dict_results(lsa_users, 'LSA Users')")
print()

print("=" * 80)
print("\nRun any of the above examples, or use the adapters directly!")
print("Type 'exit()' or Ctrl+D to quit\n")

# ============================================================================
# INTERACTIVE MODE - Drop into Python REPL
# ============================================================================

if __name__ == "__main__":
    import code

    # Create a namespace with our adapters and helper functions
    namespace = {
        "ad": ad,
        "mc": mc,
        "print_results": print_results,
        "print_count": print_count,
        "print_dict_results": print_dict_results,
        "LDAPAdapter": LDAPAdapter,
    }

    # Start interactive Python shell
    code.interact(local=namespace, banner="")
