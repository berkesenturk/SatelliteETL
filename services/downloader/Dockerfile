FROM python:3.11-slim

LABEL maintainer="your-email@example.com"
LABEL description="SEVIRI Pipeline Downloader Service"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories
RUN mkdir -p /raw /logs

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD celery -A tasks inspect ping || exit 1

# Expose port for monitoring
EXPOSE 8001

# Run Celery worker
CMD celery -A tasks worker \
    --loglevel=${LOG_LEVEL:-INFO} \
    --concurrency=${CELERY_WORKERS:-2} \
    --max-tasks-per-child=10 \
    --time-limit=1200 \
    --soft-time-limit=1000