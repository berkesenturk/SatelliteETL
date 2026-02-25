"""
Monitoring endpoint for Downloader service
"""
import logging
from typing import Dict, Any
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from starlette.responses import Response

from celery_app import celery_app
from config import config
from database import db

logger = logging.getLogger(__name__)

# Prometheus metrics
download_counter = Counter('downloads_total', 'Total downloads', ['status'])
download_duration = Histogram('download_duration_seconds', 'Download duration')
active_downloads = Gauge('downloads_active', 'Active downloads')

# FastAPI app for monitoring
app = FastAPI(title="Downloader Monitor", version="1.0.0")


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "SEVIRI Downloader",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint"""
    try:
        # Check Celery workers
        inspect = celery_app.control.inspect()
        stats = inspect.stats()
        active = inspect.active()
        
        worker_count = len(stats) if stats else 0
        active_tasks = sum(len(tasks) for tasks in active.values()) if active else 0
        
        is_healthy = worker_count > 0
        
        return JSONResponse(
            content={
                "status": "healthy" if is_healthy else "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "workers": worker_count,
                "active_tasks": active_tasks,
                "celery_stats": stats
            },
            status_code=200 if is_healthy else 503
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=503
        )


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get download statistics"""
    try:
        inspect = celery_app.control.inspect()
        
        # Get active tasks
        active = inspect.active()
        active_count = sum(len(tasks) for tasks in active.values()) if active else 0
        
        # Get reserved tasks
        reserved = inspect.reserved()
        reserved_count = sum(len(tasks) for tasks in reserved.values()) if reserved else 0
        
        # Get stats from database
        queue_stats = db.get_queue_stats()
        
        return {
            "celery": {
                "active_tasks": active_count,
                "reserved_tasks": reserved_count,
                "workers": len(active) if active else 0
            },
            "database": queue_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Stats check failed: {e}")
        return {"error": str(e)}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type="text/plain")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)