from setuptools import setup, find_packages

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
    python_requires=">=3.6",
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.0.0",
        "python-dotenv>=0.15.0",
    ],
    extras_require={
        "teamdynamix": [
            "requests>=2.25.0",
        ],
        "google": [
            "google>=3.0.0",
            "googleapis-common-protos>=1.66.0",
            "google-api-core>=2.24.1",
            "google-api-python-client>=2.0.0",
            "google-auth>=2.38.0",
            "google-auth-httplib2>=0.1.0",
            "google-auth-oauthlib>=0.4.0",
            "httplib2>=0.22.0",
            "oauthlib>=3.2.2",
            "proto-plus>=1.26.0",
            "uritemplate>=4.1.1",
            "cachetools>=5.5.1"
        ],
        "ai": [
            "openai>=1.0.0",
        ],
        "all": [
                    "requests>=2.25.0",
                    "pandas>=1.3.0",
                    "python-dotenv>=0.19.0",
                    "google-api-python-client>=2.50.0",
                    "google-auth>=2.0.0",
                    "google-auth-httplib2>=0.1.0",
                    "google-auth-oauthlib>=0.5.0",
                    "openai>=1.0.0"
                ],
    },
    entry_points={
        "console_scripts": [
            # Compliance scripts
            "compliance-automator=scripts.compliance.ticket_automator:main",
            "compliance-update=scripts.compliance.second_outreach:main",
            "compliance-third-outreach=scripts.compliance.third_outreach:main",

            # Lab management scripts
            "create-lab-note=scripts.lab_management.create_lab_note:main",

            # Future categories can be added here
            # "inventory-scan=scripts.inventory.scanner:main",
        ],
    },
)
