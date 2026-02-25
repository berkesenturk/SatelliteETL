"""
Celery application configuration for Processor
"""
import logging
from celery import Celery
from config import config

logger = logging.getLogger(__name__)

# Create Celery app
celery_app = Celery(
    'processor',
    broker=config.celery_broker_url,
    backend=config.celery_result_backend,
    include=['tasks']
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task execution settings
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    
    # Time limits
    task_time_limit=1800,  # 30 minutes hard limit
    task_soft_time_limit=1500,  # 25 minutes soft limit
    
    # Worker settings
    worker_prefetch_multiplier=1,  # One task at a time per worker
    worker_max_tasks_per_child=5,  # Restart worker after 5 tasks (memory cleanup)
    
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    result_persistent=False,
    
    # Queue settings
    task_default_queue='processing_queue',
    task_routes={
        'tasks.process_file': {'queue': 'processing_queue'},
    },
)

logger.info("Celery app configured successfully")   