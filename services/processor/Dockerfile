FROM python:3.11-slim

LABEL maintainer="your-email@example.com"
LABEL description="SEVIRI Pipeline Processor Service"

# Set working directory
WORKDIR /app

# Install system dependencies for satpy and geospatial libraries
RUN apt-get update && apt-get install -y \
    curl \
    libhdf5-dev \
    libnetcdf-dev \
    libgeos-dev \
    libproj-dev \
    libudunits2-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories
RUN mkdir -p /raw /temp /processed /logs

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD celery -A tasks inspect ping || exit 1

# Expose port for monitoring
EXPOSE 8002

# Run Celery worker
CMD celery -A tasks worker \
    --loglevel=${LOG_LEVEL:-INFO} \
    --concurrency=${CELERY_WORKERS:-3} \
    --max-tasks-per-child=5 \
    --time-limit=1800 \
    --soft-time-limit=1500