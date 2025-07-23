# LSATS Data Hub - Application Container
# This Dockerfile creates a container image for running LSATS ingestion services

# Use Python 3.11 slim image as our foundation
# Slim images are smaller and more secure than full Python images
FROM python:3.11-slim

# Set environment variables for better Python behavior in containers
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies required for PostgreSQL and other packages
# gcc and build-essential are needed for compiling psycopg2
# postgresql-client provides useful database tools
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create a non-root user for better security
# Running containers as root is a security risk
RUN useradd --create-home --shell /bin/bash lsats

# Set up the application directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
# This allows Docker to reuse the package installation layer when only code changes
COPY requirements.txt setup.py ./

# Install Python dependencies
# We use --no-deps with pip to ensure clean dependency resolution
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install -e .

# Copy the entire LSATS Data Hub codebase
# This comes after requirements installation for optimal caching
COPY . /app/

# Create necessary directories for logs and data
# These directories will be mounted as volumes in docker-compose
RUN mkdir -p /app/logs /app/data/exports && \
    chown -R lsats:lsats /app

# Switch to non-root user for security
USER lsats

# Set the default working directory for the lsats user
WORKDIR /app

# Expose port 8000 in case we add a web interface later
EXPOSE 8000

# Health check to ensure the container can connect to the database
# This provides visibility into container health in Docker Compose
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from database.adapters.postgres_adapter import create_postgres_adapter; create_postgres_adapter()._test_connection()" || exit 1

# Default command runs a shell, but this can be overridden in docker-compose
# In production, this might run your ingestion services
CMD ["/bin/bash"]

# Labels for better container organization and documentation
LABEL maintainer="LSATS Data Hub Team" \
    version="1.0" \
    description="LSATS Data Hub application container for data ingestion and processing" \
    org.label-schema.vcs-url="https://github.com/your-org/LSATS_Data_Hub"
