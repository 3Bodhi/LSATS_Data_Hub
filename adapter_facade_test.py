"""
LDAP Facade Usage Example

This example demonstrates how to use the LDAPFacade with the configurations
from your existing ldap_adapter.py main function.
"""
import pandas as pd
import logging
from ldap.facade.ldap_facade import LDAPFacade

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    """Demonstrate LDAP Facade usage with both connection types."""

    # Configuration from your existing main function
    ad_config = {
        'server': 'adsroot.itcs.umich.edu',
        'search_base': 'OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu',
        'user': 'umroot\\myodhes1',
        'keyring_service': 'ldap_umich',
        'port': 636,
        'use_ssl': True
    }

    mcommunity_config = {
        'server': 'ldap.umich.edu',
        'search_base': 'dc=umich,dc=edu',
        'user': 'uid=myodhes,ou=People,dc=umich,dc=edu',
        'keyring_service': 'Mcom_umich',
        'port': 636,
        'use_ssl': True
    }

    print("🔍 LDAP FACADE DEMONSTRATION")
    print("=" * 50)

    try:
        # Initialize facade - connections are activated automatically
        print("\n🔌 Initializing LDAP Facade...")
        with LDAPFacade(ad_config, mcommunity_config) as facade:

            # Show connection info
            print("\n📊 Connection Information:")
            conn_info = facade.get_connection_info()
            for name, info in conn_info.items():
                print(f"   {name}: {info['server']} (base: {info['search_base']})")

            # Demonstrate orchestrated access - search both simultaneously
            print("\n🔍 Orchestrated Search - Users (both connections):")
            user_results = facade.both('search', '(objectClass=person)', max_results=2)

            for source, results in user_results.items():
                if isinstance(results, dict) and 'error' in results:
                    print(f"   ❌ {source}: {results['error']}")
                else:
                    print(f"   ✅ {source}: Found {len(results)} results")
                    if results:
                        print(f"      Sample DN: {results[0].entry_dn}")

            # Demonstrate orchestrated counting
            print("\n📊 Orchestrated Count - Organizational Units:")
            ou_counts = facade.both('count_search_results', '(objectClass=organizationalUnit)')

            for source, count in ou_counts.items():
                if isinstance(count, dict) and 'error' in count:
                    print(f"   ❌ {source}: {count['error']}")
                else:
                    print(f"   ✅ {source}: {count:,} organizational units")

            # Demonstrate direct access to individual adapters
            print("\n🎯 Direct Access - Active Directory Only:")
            try:
                ad_users = facade.active_directory.search('(objectClass=person)', max_results=2)
                print(f"   ✅ Found {len(ad_users)} users via direct AD access")
                if ad_users:
                    print(f"      Sample DN: {ad_users[0].entry_dn}")
                    print(ad_users[0])
            except Exception as e:
                print(f"   ❌ Direct AD access failed: {e}")

            print("\n🎯 Direct Access - MCommunity Only:")
            try:
                mc_people = facade.mcommunity.search('(objectClass=person)', max_results=2)
                print(f"   ✅ Found {len(mc_people)} people via direct MCommunity access")
                if mc_people:
                    print(f"      Sample DN: {mc_people[0].entry_dn}")
                    print(mc_people[0])
            except Exception as e:
                print(f"   ❌ Direct MCommunity access failed: {e}")

            # Demonstrate error handling with invalid method
            print("\n⚠️  Error Handling - Invalid Method:")
            try:
                facade.both('nonexistent_method')
            except AttributeError as e:
                print(f"   ✅ Caught expected error: {e}")

            print("\n🎉 LDAP Facade demonstration completed successfully!")
            print("\n💡 Key capabilities demonstrated:")
            print("   • Automatic connection activation")
            print("   • Orchestrated method calls via both()")
            print("   • Direct adapter access via attributes")
            print("   • Robust error handling")
            print("   • Connection information retrieval")
            print("   • Context manager support")

    except Exception as e:
        print(f"\n❌ Facade initialization failed: {e}")
        print("   This is expected if LDAP credentials are not configured")

if __name__ == "__main__":

    ad_config = {
        'server': 'adsroot.itcs.umich.edu',
        'search_base': 'OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu',
        'user': 'umroot\\myodhes1',
        'keyring_service': 'ldap_umich',
        'port': 636,
        'use_ssl': True
    }

    mcommunity_config = {
        'server': 'ldap.umich.edu',
        'search_base': 'dc=umich,dc=edu',
        'user': 'uid=myodhes,ou=People,dc=umich,dc=edu',
        'keyring_service': 'Mcom_umich',
        'port': 636,
        'use_ssl': True
    }

    """
    LDAP Facade Usage Example

    Integration with your existing ldap_adapter.py configurations.
    """

    from ldap.facade.ldap_facade import LDAPFacade

    # Use your existing configurations from main() function
    facade = LDAPFacade(ad_config, mcommunity_config)

    # Orchestrated Access - Execute same method on both adapters
    results = facade.both('search', '(objectClass=person)', max_results=10)
    # Returns: {'active_directory': [...], 'mcommunity': [...]}

    counts = facade.both('count_search_results', '(objectClass=organizationalUnit)')
    # Returns: {'active_directory': 15, 'mcommunity': 23}
    print(counts)

    # Direct Access - Access individual adapters directly
    ad_users = facade.active_directory.search('(objectClass=person)', max_results=5)
    mc_people = facade.mcommunity.search('(objectClass=person)', max_results=5)

    ad_tree = facade.active_directory.extract_organizational_tree()
    print(ad_tree)
    ad_tree = pd.DataFrame(ad_tree)
    ad_tree.to_csv("ad_tree.csv")
