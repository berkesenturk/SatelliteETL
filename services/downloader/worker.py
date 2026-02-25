"""
Celery worker entry point
"""
import logging
import signal
import sys
from celery_app import celery_app
from config import config

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/downloader.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig}, shutting down...")
    sys.exit(0)


if __name__ == '__main__':
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("="*60)
    logger.info("SEVIRI Downloader Worker Starting")
    logger.info(f"Workers: {config.celery_workers}")
    logger.info("="*60)
    
    # Start worker
    celery_app.worker_main([
        'worker',
        f'--loglevel={config.log_level}',
        f'--concurrency={config.celery_workers}',
        '--max-tasks-per-child=10',
        '--time-limit=1200',
        '--soft-time-limit=1000'
    ])