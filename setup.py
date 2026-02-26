from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="lsats-data-hub",
    version="0.1.0",
    author="LSA Technology Services",
    author_email="lsats@umich.edu",
    description="A set of Python modules to simplify complex queries across LSA Technology Services data sources",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/lsats-data-hub",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "teamdynamix": ["api/*.json"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: System Administrators",
        "Topic :: System :: Systems Administration",
    ],
    python_requires=">=3.11",
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.0.0",
        "python-dotenv>=0.15.0",
        "urllib3>=1.21.1",
        "charset-normalizer<4",
        "chardet<6",
    ],
    extras_require={
        "teamdynamix": [
            "requests>=2.25.0",
        ],
        "database": [
            "sqlalchemy>=1.4.0",
            "psycopg2-binary>=2.9.0",
            "pandas>=1.3.0",
            "python-dotenv>=0.15.0",
            "python-dateutil>=2.8.0",
            "ldap3>=2.9.0",
            "keyring>=23.0.0",
            "requests>=2.25.0",
        ],
        "compliance": [
            "python-dotenv>=0.15.0",
            "pandas>=1.3.0",
            "google-api-python-client>=2.0.0",
            "google-auth>=2.38.0",
            "google-auth-httplib2>=0.1.0",
            "google-auth-oauthlib>=0.4.0",
            "requests>=2.25.0",
        ],
        "google": [
            "google-api-python-client>=2.0.0",
            "google-auth>=2.38.0",
            "google-auth-httplib2>=0.1.0",
            "google-auth-oauthlib>=0.4.0",
        ],
        "ai": [
            "openai>=1.0.0",
            "beautifulsoup4>=4.0.0",
            "html2text>=2020.1.16",
            "readability-lxml>=0.8.1",
            "tldextract>=3.0.0",
        ],
        "lab_notes": [
            "beautifulsoup4>=4.0.0",
            "html2text>=2020.1.16",
            "readability-lxml>=0.8.1",
            "tldextract>=3.0.0",
            "openai>=1.0.0",
        ],
        "all": [
            # database
            "sqlalchemy>=1.4.0",
            "psycopg2-binary>=2.9.0",
            "pandas>=1.3.0",
            "python-dotenv>=0.15.0",
            "python-dateutil>=2.8.0",
            "ldap3>=2.9.0",
            "keyring>=23.0.0",
            "requests>=2.25.0",
            # google / compliance
            "google-api-python-client>=2.0.0",
            "google-auth>=2.38.0",
            "google-auth-httplib2>=0.1.0",
            "google-auth-oauthlib>=0.4.0",
            # ai
            "openai>=1.0.0",
            "beautifulsoup4>=4.0.0",
            "html2text>=2020.1.16",
            "readability-lxml>=0.8.1",
            "tldextract>=3.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            # Compliance scripts
            "compliance-automator=scripts.compliance.compliance_ticket_automator:main",
            "compliance-update=scripts.compliance.compliance_ticket_second_outreach:main",
            "compliance-third-outreach=scripts.compliance.compliance_ticket_third_outreach:main",
            # Lab management scripts
            "create-lab-note=scripts.lab_management.create_lab_note:main",
            # Queue daemon scripts
            "ticket-queue-daemon=scripts.queue.ticket_queue_daemon:main",
            # Future categories can be added here
            # "inventory-scan=scripts.inventory.scanner:main",
        ],
    },
)
