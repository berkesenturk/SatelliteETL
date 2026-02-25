import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any
import requests
import eumdac
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded
import redis

from celery_app import celery_app
from config import config
from database import db

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis client for enqueueing processing tasks
redis_client = redis.Redis(
    host=config.redis_host,
    port=config.redis_port,
    decode_responses=True
)

# EUMETSAT credentials from environment
EUMETSAT_KEY = os.getenv('EUMETSAT_KEY')
EUMETSAT_SECRET = os.getenv('EUMETSAT_SECRET')


class DownloadTask(Task):
    """Base task with custom retry logic"""
    
    autoretry_for = (requests.RequestException, IOError)
    retry_backoff = True
    retry_backoff_max = 900  # 15 minutes max
    retry_jitter = True
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure"""
        task_data = args[0] if args else {}
        file_id = task_data.get('file_id', 'unknown')
        
        logger.error(
            f"Download task failed permanently for {file_id}: {exc}",
            exc_info=True
        )
        
        # Update database with failure
        db.update_download_status(
            file_id=file_id,
            status='FAILED',
            error_message=str(exc)
        )

@celery_app.task(
    base=DownloadTask,
    bind=True,
    max_retries=3,
    name='tasks.download_file'
)
def download_file(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Download SEVIRI product (all files) using EUMDAC
    
    Args:
        task_data: Dictionary containing file_id, collection_id, product_id, etc.
    
    Returns:
        Dictionary with download result
    """
    file_id = task_data['file_id']
    collection_id = task_data['collection_id']
    product_id = task_data['product_id']
    timestamp = task_data['timestamp']
    
    attempt = self.request.retries + 1
    
    logger.info(f"Starting download for {file_id} (attempt {attempt}/3)")
    
    db.update_download_status(
        file_id=file_id,
        status='DOWNLOADING',
        attempt=attempt
    )
    
    # Construct output directory (not single file)
    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    year_month_day = dt.strftime('%Y/%m/%d')
    product_dir_name = dt.strftime('%Y%m%d_%H%M%S') + '_MSG_HRSEVIRI'
    
    output_base_dir = f"/raw/{year_month_day}"
    output_dir = f"{output_base_dir}/{product_dir_name}"
    
    # Create directory
    os.makedirs(output_dir, exist_ok=True)
    
    start_time = time.time()
    
    try:
        # Connect to EUMETSAT
        logger.info(f"Connecting to EUMETSAT Data Store...")
        credentials = (EUMETSAT_KEY, EUMETSAT_SECRET)
        token = eumdac.AccessToken(credentials)
        datastore = eumdac.DataStore(token)
        
        collection = datastore.get_collection(collection_id)
        logger.info(f"Retrieved collection: {collection.title}")
        
        # Find product
        logger.info(f"Searching for product: {product_id}")
        search_results = collection.search(
            dtstart=dt - timedelta(hours=1),
            dtend=dt + timedelta(hours=1)
        )
        
        product = None
        for p in search_results:
            p_id = p.id if hasattr(p, 'id') else str(p)
            if p_id == product_id:
                product = p
                logger.info(f"Found product: {product_id}")
                break
        
        if product is None:
            raise ValueError(f"Product not found: {product_id}")
        
        # Check product entries (multiple files)
        if hasattr(product, 'entries'):
            logger.info(f"Product contains {len(product.entries)} files: {product.entries}")
        
        # Download the product (this gets all files)
        logger.info(f"Downloading product to: {output_dir}")
        
        # Use product.download() to get all files, or manually download each entry
        downloaded_files = []
        
        # Method 1: Try product.download() if available
        try:
            # Some EUMDAC versions support direct download
            with product.open() as fsrc:
                # This might be a tar/zip, we need to extract
                main_file = os.path.join(output_dir, 'product_data.nat')
                with open(main_file, 'wb') as fdst:
                    downloaded = 0
                    chunk_size = 10 * 1024 * 1024
                    
                    while True:
                        chunk = fsrc.read(chunk_size)
                        if not chunk:
                            break
                        fdst.write(chunk)
                        downloaded += len(chunk)
                        
                        if downloaded % (50 * 1024 * 1024) < chunk_size:
                            logger.info(f"Download progress: {downloaded / (1024*1024):.1f} MB")
                
                downloaded_files.append(main_file)
                
                # Check if it's a tar or zip file
                if main_file.endswith('.tar') or _is_tar_file(main_file):
                    logger.info("Extracting tar archive...")
                    import tarfile
                    with tarfile.open(main_file, 'r') as tar:
                        tar.extractall(output_dir)
                    # Remove tar file
                    os.remove(main_file)
                    # List extracted files
                    downloaded_files = [
                        os.path.join(output_dir, f) 
                        for f in os.listdir(output_dir)
                    ]
                    
                elif main_file.endswith('.zip') or _is_zip_file(main_file):
                    logger.info("Extracting zip archive...")
                    import zipfile
                    with zipfile.ZipFile(main_file, 'r') as zip_ref:
                        zip_ref.extractall(output_dir)
                    os.remove(main_file)
                    downloaded_files = [
                        os.path.join(output_dir, f) 
                        for f in os.listdir(output_dir)
                    ]
                
        except Exception as e:
            logger.warning(f"Direct download failed: {e}, trying alternative method")
            
            # Method 2: Download using entries attribute (if available)
            if hasattr(product, 'entries'):
                for entry_name in product.entries:
                    entry_path = os.path.join(output_dir, entry_name)
                    logger.info(f"Downloading entry: {entry_name}")
                    
                    # This is a workaround - EUMDAC API varies by version
                    # You might need to adjust this based on your EUMDAC version
                    with product.open() as fsrc:
                        with open(entry_path, 'wb') as fdst:
                            shutil.copyfileobj(fsrc, fdst)
                    
                    downloaded_files.append(entry_path)
        
        download_duration = time.time() - start_time
        
        # Find the main .nat file
        nat_files = [f for f in downloaded_files if f.endswith('.nat')]
        
        if not nat_files:
            raise ValueError(f"No .nat file found in downloaded product")
        
        main_file = nat_files[0]
        
        logger.info(f"Downloaded {len(downloaded_files)} files in {download_duration:.2f}s")
        logger.info(f"Main file: {main_file}")
        
        # Update database
        db.update_download_status(
            file_id=file_id,
            status='DOWNLOADED',
            file_path=output_dir,  # Store directory path, not file
            download_duration=download_duration
        )
        
        db.log_download_metrics(
            file_id=file_id,
            stage='download',
            duration_seconds=download_duration,
            status='SUCCESS'
        )
        
        # Enqueue processing task
        celery_app.send_task(
            'tasks.process_file',
            args=[{
                'file_id': file_id,
                'file_path': output_dir,  # Pass directory, not file
                'main_file': main_file,   # Also pass main .nat file
                'timestamp': timestamp,
                'satellite': task_data.get('satellite', 'MSG')
            }],
            queue='processing_queue'
        )
        
        logger.info(f"Enqueued processing task for {file_id}")
        
        return {
            'status': 'success',
            'file_id': file_id,
            'file_path': output_dir,
            'files_downloaded': len(downloaded_files),
            'duration_sec': download_duration
        }
        
    except SoftTimeLimitExceeded:
        logger.error(f"Download timed out for {file_id}")
        
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        
        db.update_download_status(
            file_id=file_id,
            status='RETRY',
            error_message='Download timed out'
        )
        
        retry_delays = config.get('download.retry_delay_seconds', [60, 300, 900])
        countdown = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        
        raise self.retry(exc=SoftTimeLimitExceeded(), countdown=countdown)
        
    except Exception as e:
        logger.error(f"Download failed for {file_id}: {e}", exc_info=True)
        
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        
        db.log_download_metrics(
            file_id=file_id,
            stage='download',
            duration_seconds=time.time() - start_time,
            status='FAILED',
            error_message=str(e)
        )
        
        if attempt < 3:
            db.update_download_status(
                file_id=file_id,
                status='RETRY',
                error_message=str(e)
            )
            
            retry_delays = config.get('download.retry_delay_seconds', [60, 300, 900])
            countdown = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
            
            logger.info(f"Retrying {file_id} in {countdown} seconds")
            raise self.retry(exc=e, countdown=countdown)
        else:
            db.update_download_status(
                file_id=file_id,
                status='FAILED',
                error_message=f"Max retries reached: {str(e)}"
            )
            
            logger.error(f"Max retries reached for {file_id}, flagged as FAILED")
            
            return {
                'status': 'failed',
                'file_id': file_id,
                'error': str(e),
                'attempts': attempt
            }


def _is_tar_file(filepath):
    """Check if file is a tar archive"""
    try:
        import tarfile
        return tarfile.is_tarfile(filepath)
    except:
        return False


def _is_zip_file(filepath):
    """Check if file is a zip archive"""
    try:
        import zipfile
        return zipfile.is_zipfile(filepath)
    except:
        return False