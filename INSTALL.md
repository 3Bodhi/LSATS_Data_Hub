# LSATS Data Hub Installation Guide

## Prerequisites

- Python 3.6 or higher
- pip (Python package installer)
- git

## Basic Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/lsats-data-hub.git
cd lsats-data-hub
```

### 2. Install the package

Note: Before installing packages, you may want to create a virtual environment. do so by running:

```bash
python -m venv .venv
```
This may require other dependencies for virtual environments.

#### Development mode (for contributors)

This installs the package in development mode, which means changes to the source code will be immediately available without reinstalling:

```bash
pip install -e .
```

#### Regular installation

For users who just want to use the package:

```bash
pip install .
```

### 3. Set up environment variables

Copy the example environment file and edit it with your credentials:

```bash
cp .env.example .env
# Edit .env with your favorite text editor
```

You'll need to set up appropriate API keys and credentials:

- For TeamDynamix: obtain API token from the corresponding TeamDynamix endpoints
- For Google Sheets: follow the [Google Sheets API quickstart guide](https://developers.google.com/sheets/api/quickstart/python) to obtain credentials

### 4. Using command-line utilities

After installation, the following commands will be available:

- `create-lab-note <uniqname>` - Create a lab note for a PI in TeamDynamix
- `compliance-update` - Update compliance tickets with second outreach

## Advanced Installation

### Installing in a virtual environment

It's recommended to use a virtual environment to avoid conflicts with other packages:

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install the package
pip install -e .
```

### Installing specific dependencies

If you only need specific functionality, you can install just what you need:

```bash
# For TeamDynamix only
pip install -e .[teamdynamix]

# For Google integration only
pip install -e .[google]

# For all features
pip install -e .[all]
```

## Troubleshooting

### Authentication issues

- For TeamDynamix authentication issues, verify your API token is correctly set in the `.env` file
- For Google Sheets authentication, ensure your `credentials.json` file is in the correct location and has the proper permissions

### Missing dependencies

If you encounter errors about missing packages, try:

```bash
pip install -e .[all]
```

### Reporting bugs

Please report any issues on the GitHub repository's issue tracker.
