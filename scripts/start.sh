#!/bin/bash

# Start script for SEVIRI Pipeline

echo "========================================="
echo "Starting SEVIRI Data Pipeline"
echo "========================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and configure it"
    exit 1
fi

# Load environment variables
source .env

# Validate required variables
if [ -z "$EUMETSAT_KEY" ] || [ -z "$EUMETSAT_SECRET" ]; then
    echo "Error: EUMETSAT credentials not configured in .env"
    exit 1
fi

if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "Error: POSTGRES_PASSWORD not configured in .env"
    exit 1
fi

echo "Environment variables loaded successfully"
echo ""

# Create data directories if they don't exist
echo "Creating data directories..."
mkdir -p data/raw data/processed data/temp

# Build containers
echo ""
echo "Building Docker containers..."
docker-compose build

# Start services
echo ""
echo "Starting services..."
docker-compose up -d

# Wait for services to be healthy
echo ""
echo "Waiting for services to be healthy..."
sleep 20

# Check health
echo ""
echo "Checking service health..."
docker-compose ps

echo ""
echo "API Poller Health:"
curl -s http://localhost:8000/health | python3 -m json.tool || echo "Failed to connect"

echo ""
echo "Downloader Health:"
curl -s http://localhost:8001/health | python3 -m json.tool || echo "Failed to connect"

echo ""
echo "Processor Health:"
curl -s http://localhost:8002/health | python3 -m json.tool || echo "Failed to connect"

echo ""
echo "========================================="
echo "Services started successfully!"
echo "========================================="
echo ""
echo "Services:"
echo "  - API Poller:   http://localhost:8000"
echo "  - Downloader:   http://localhost:8001"
echo "  - Processor:    http://localhost:8002"
echo "  - PostgreSQL:   localhost:5432"
echo "  - Redis:        localhost:6379"
echo ""
echo "Useful commands:"
echo "  - View poller logs:      docker-compose logs -f api-poller"
echo "  - View downloader logs:  docker-compose logs -f downloader"
echo "  - View processor logs:   docker-compose logs -f processor"
echo "  - Check queues:          docker exec seviri-redis redis-cli LLEN download_queue"
echo "  - Check processing:      docker exec seviri-redis redis-cli LLEN processing_queue"
echo "  - View processed files:  ls -lh data/processed/"
echo "  - Test pipeline:         chmod +x scripts/test-pipeline.sh && ./scripts/test-pipeline.sh"
echo "  - Stop services:         docker-compose down"
echo ""