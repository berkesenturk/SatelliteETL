#!/bin/bash

echo "========================================="
echo "Testing Downloader Service"
echo "========================================="

echo ""
echo "1. Checking downloader health..."
curl -s http://localhost:8001/health | python3 -m json.tool

echo ""
echo "2. Checking Celery workers..."
docker exec seviri-downloader celery -A tasks inspect active

echo ""
echo "3. Checking queue length..."
docker exec seviri-redis redis-cli LLEN download_queue

echo ""
echo "4. Checking recent downloads..."
docker exec -it seviri-postgres psql -U seviri -d seviri_pipeline -c \
  "SELECT file_id, status, download_attempt, downloaded_at 
   FROM file_manifest 
   WHERE status IN ('DOWNLOADING', 'DOWNLOADED', 'RETRY', 'FAILED')
   ORDER BY updated_at DESC 
   LIMIT 10;"

echo ""
echo "========================================="