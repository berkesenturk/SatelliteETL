#!/bin/bash

# End-to-end pipeline test script

echo "========================================="
echo "Testing Complete SEVIRI Pipeline"
echo "========================================="

# 1. Check all services
echo ""
echo "1. Checking service health..."
echo "   API Poller:"
curl -s http://localhost:8000/health | jq -r '.status' || echo "FAILED"
echo "   Downloader:"
curl -s http://localhost:8001/health | jq -r '.status' || echo "FAILED"
echo "   Processor:"
curl -s http://localhost:8002/health | jq -r '.status' || echo "FAILED"

# 2. Trigger manual poll
echo ""
echo "2. Triggering manual poll..."
curl -s -X POST http://localhost:8000/trigger-poll | jq

# 3. Wait a bit for downloads to start
echo ""
echo "3. Waiting 10 seconds for downloads to start..."
sleep 10

# 4. Check queue lengths
echo ""
echo "4. Checking queue lengths..."
echo "   Download queue:"
docker exec seviri-redis redis-cli LLEN download_queue
echo "   Processing queue:"
docker exec seviri-redis redis-cli LLEN processing_queue

# 5. Check database status
echo ""
echo "5. Checking database for file status..."
docker exec -it seviri-postgres psql -U seviri -d seviri_pipeline -c \
  "SELECT file_id, status, quality_score, processed_at 
   FROM file_manifest 
   ORDER BY timestamp DESC 
   LIMIT 5;"

# 6. Check processed files
echo ""
echo "6. Checking processed files..."
find data/processed -name "*.nc" -type f -exec ls -lh {} \; | head -5

# 7. Check logs for errors
echo ""
echo "7. Checking for errors in logs..."
echo "   Recent errors:"
docker-compose logs --tail=50 | grep -i error | tail -5

echo ""
echo "========================================="
echo "Pipeline test complete!"
echo "========================================="