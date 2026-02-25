"""
API Poller Service - Main application
Polls EUMETSAT API every 15 minutes and enqueues download tasks
"""
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Any
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from starlette.responses import Response

from config import config
from database import db
from poller import EUMETSATPoller

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.log_level),
    format=config.get('logging.format'),
    handlers=[
        logging.FileHandler(config.get('logging.file', '/logs/api-poller.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Prometheus metrics
poll_counter = Counter('poller_cycles_total', 'Total number of polling cycles')
files_found_gauge = Gauge('poller_files_found', 'Number of files found in last poll')
files_enqueued_counter = Counter('poller_files_enqueued_total', 'Total files enqueued')
poll_duration = Histogram('poller_cycle_duration_seconds', 'Polling cycle duration')
queue_length_gauge = Gauge('poller_queue_length', 'Current download queue length')

# FastAPI app
app = FastAPI(title="SEVIRI API Poller", version="1.0.0")

# Global poller instance
poller: EUMETSATPoller = None
scheduler: BackgroundScheduler = None
last_poll_time: datetime = None
last_poll_status: str = "NOT_STARTED"


def scheduled_poll():
    """Scheduled polling function"""
    global last_poll_time, last_poll_status
    
    poll_counter.inc()
    logger.info("Starting scheduled poll")
    
    start_time = time.time()
    
    try:
        # Execute poll
        poller.poll()
        
        # Update metrics
        queue_length = poller.get_queue_length()
        queue_length_gauge.set(queue_length)
        
        duration = time.time() - start_time
        poll_duration.observe(duration)
        
        last_poll_time = datetime.utcnow()
        last_poll_status = "SUCCESS"
        
        logger.info(f"Polling completed in {duration:.2f}s, queue length: {queue_length}")
        
    except Exception as e:
        last_poll_status = "FAILED"
        logger.error(f"Polling failed: {e}", exc_info=True)


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global poller, scheduler
    
    logger.info("Starting API Poller service...")
    
    try:
        # Initialize poller
        poller = EUMETSATPoller()
        
        # Initialize scheduler
        scheduler = BackgroundScheduler(timezone='UTC')
        
        # Schedule polling job
        interval_minutes = config.get('polling.interval_minutes', 15)
        initial_delay = config.get('polling.initial_delay_seconds', 10)
        
        scheduler.add_job(
            scheduled_poll,
            'interval',
            minutes=interval_minutes,
            id='eumetsat_poll',
            next_run_time=datetime.utcnow() + timedelta(seconds=initial_delay)
        )
        
        scheduler.start()
        
        logger.info(f"Scheduler started - polling every {interval_minutes} minutes")
        
        # Log health check
        db.log_health_check('api-poller', 'healthy', 'Service started successfully')
        
    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        sys.exit(1)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global scheduler, poller
    
    logger.info("Shutting down API Poller service...")
    
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
    
    if poller:
        poller.close()
    
    db.close()
    logger.info("Service shutdown complete")


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "SEVIRI API Poller",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint"""
    try:
        # Check Redis connection
        redis_status = "healthy" if poller.redis_client.ping() else "unhealthy"
        
        # Check database
        try:
            db.get_queue_stats()
            db_status = "healthy"
        except:
            db_status = "unhealthy"
        
        # Overall status
        is_healthy = redis_status == "healthy" and db_status == "healthy"
        
        health_data = {
            "status": "healthy" if is_healthy else "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "last_poll": last_poll_time.isoformat() if last_poll_time else None,
            "last_poll_status": last_poll_status,
            "components": {
                "redis": redis_status,
                "database": db_status,
                "scheduler": "running" if scheduler and scheduler.running else "stopped"
            },
            "queue_length": poller.get_queue_length()
        }
        
        return JSONResponse(
            content=health_data,
            status_code=200 if is_healthy else 503
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=503
        )


@app.get("/status")
async def get_status() -> Dict[str, Any]:
    """Get detailed status information"""
    try:
        queue_stats = db.get_queue_stats()
        
        return {
            "service": "api-poller",
            "uptime_seconds": time.time() - start_time if 'start_time' in globals() else 0,
            "last_poll": last_poll_time.isoformat() if last_poll_time else None,
            "last_poll_status": last_poll_status,
            "queue_length": poller.get_queue_length(),
            "files_by_status": queue_stats,
            "scheduler_running": scheduler.running if scheduler else False,
            "next_run": scheduler.get_job('eumetsat_poll').next_run_time.isoformat() 
                       if scheduler and scheduler.get_job('eumetsat_poll') else None
        }
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return {"error": str(e)}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type="text/plain")


@app.post("/trigger-poll")
async def trigger_manual_poll():
    """Manually trigger a polling cycle"""
    try:
        logger.info("Manual poll triggered via API")
        scheduled_poll()
        return {"status": "success", "message": "Polling cycle triggered"}
    except Exception as e:
        logger.error(f"Manual poll failed: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {sig}, shutting down...")
    sys.exit(0)


if __name__ == "__main__":
    # Record start time
    start_time = time.time()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start the service
    logger.info("="*60)
    logger.info("SEVIRI API Poller Service Starting")
    logger.info(f"ROI: {config.get('roi.name')}")
    logger.info(f"Polling interval: {config.get('polling.interval_minutes')} minutes")
    logger.info("="*60)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level=config.log_level.lower()
    )
