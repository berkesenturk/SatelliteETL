# SEVIRI Data Pipeline

Automated pipeline for downloading and processing MSG SEVIRI HRV satellite data for Paris region.

## Complete Pipeline
```
EUMETSAT API → Poller → Download Queue → Downloader → /raw → 
Processing Queue → Processor → /processed
```

### Services

1. **API Poller** - Queries EUMETSAT every 15 minutes
2. **Downloader** - Downloads .nat files with 3-attempt retry
3. **Processor** - Converts, calibrates, reprojects, subsets
4. **Redis** - Message queue
5. **PostgreSQL** - Metadata tracking

## Quick Start
```bash
# 1. Configure
cp .env.example .env
nano .env  # Add EUMETSAT credentials

# 2. Start
chmod +x scripts/*.sh
./scripts/start.sh

# 3. Test
./scripts/test-pipeline.sh

# 4. Monitor
docker-compose logs -f processor
```

## Output Files

Processed NetCDF files are organized as:
```
/processed/YYYY/MM/DD/YYYYMMDD_HHMMSS_HRV_Paris.nc
```

Each file contains:
- `HRV_reflectance` - TOA reflectance (0-1)
- `solar_zenith_angle` - Solar geometry
- `satellite_zenith_angle` - Viewing geometry
- `quality_flag` - Quality indicator
- Global attributes with processing metadata

## Monitoring

### Check Pipeline Status
```bash
# Queue lengths
docker exec seviri-redis redis-cli LLEN download_queue
docker exec seviri-redis redis-cli LLEN processing_queue

# Recent files
docker exec -it seviri-postgres psql -U seviri -d seviri_pipeline -c \
  "SELECT file_id, status, quality_score FROM file_manifest ORDER BY timestamp DESC LIMIT 10;"

# Processed files
ls -lh data/processed/$(date +%Y/%m/%d)/
```

### View Logs
```bash
docker-compose logs -f processor
docker-compose logs --tail=100 processor | grep ERROR
```

## Troubleshooting

### Processor not working
```bash
# Check workers
docker exec seviri-processor celery -A tasks inspect active

# Check for errors
docker-compose logs processor | grep ERROR
```

### No processed files
```bash
# Check processing queue
docker exec seviri-redis redis-cli LLEN processing_queue

# Check for skipped files
docker exec -it seviri-postgres psql -U seviri -d seviri_pipeline -c \
  "SELECT file_id, skip_reason FROM file_manifest WHERE status='SKIPPED';"
```

## Next Steps

- Add monitoring dashboard (Prometheus + Grafana)
- Implement quicklook generation
- Add email alerts for failures
- Set up automated backups