"""
EUMETSAT API Poller - Queries for new SEVIRI HRV files covering Paris
"""
import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import redis
import eumdac
from celery import Celery
from config import config
from database import db

logger = logging.getLogger(__name__)


class EUMETSATPoller:
    """Polls EUMETSAT API for new SEVIRI HRV files"""
    
    def __init__(self):
        self.redis_client = self._connect_redis()
        self.celery_app = self._setup_celery()
        self.datastore = self._connect_eumetsat()
        self.collection_id = config.get('eumetsat.collection')
        
        logger.info("Connecting to EUMETSAT Data Store...")
        logger.debug(f"Using credentials: {os.getenv('EUMETSAT_KEY')[:4]}...")
        logger.debug(f"Using secret: {os.getenv('EUMETSAT_SECRET')[:4]}")       

        # Get collection
        self.collection = None
        self._initialize_collection()
        
        # ROI configuration
        self.roi = config.get('roi')
        self.roi_polygon = self._build_roi_polygon()
        
        logger.info(f"Poller initialized for ROI: {self.roi['name']}")
    
    def _connect_redis(self) -> redis.Redis:
        """Connect to Redis"""
        try:
            client = redis.Redis(
                host=config.redis_host,
                port=config.redis_port,
                decode_responses=True,
                socket_connect_timeout=5
            )
            client.ping()
            logger.info("Connected to Redis successfully")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def _setup_celery(self) -> Celery:
        """Setup Celery client for sending tasks"""
        celery_app = Celery(
            'poller',
            broker=config.redis_url,
            backend=config.redis_url
        )
        
        celery_app.conf.update(
            task_serializer='json',
            accept_content=['json'],
            result_serializer='json',
            timezone='UTC',
            enable_utc=True,
        )
        
        logger.info("Celery client configured")
        return celery_app
    
    def _connect_eumetsat(self) -> eumdac.DataStore:
        """Connect to EUMETSAT Data Store"""
        try:
            credentials = (config.eumetsat_key, config.eumetsat_secret)

            token = eumdac.AccessToken(credentials)
            datastore = eumdac.DataStore(token)
            logger.info("Connected to EUMETSAT Data Store successfully")
            return datastore
        except Exception as e:
            logger.error(f"Failed to connect to EUMETSAT: {e}")
            raise
    
    def _initialize_collection(self):
        """Initialize and validate collection"""
        try:
            self.collection = self.datastore.get_collection(self.collection_id)
            logger.info(f"Collection loaded: {self.collection.title}")
            
            # Log available search options
            search_opts = self.collection.search_options
            logger.info(f"Available search options: {list(search_opts.keys())}")
            
        except Exception as e:
            logger.error(f"Failed to initialize collection: {e}")
            raise
    
    def _build_roi_polygon(self) -> str:
        """Build WKT polygon for ROI"""
        lon_min = self.roi['lon_min']
        lon_max = self.roi['lon_max']
        lat_min = self.roi['lat_min']
        lat_max = self.roi['lat_max']
        
        polygon = (
            f"POLYGON(("
            f"{lon_min} {lat_min}, "
            f"{lon_max} {lat_min}, "
            f"{lon_max} {lat_max}, "
            f"{lon_min} {lat_max}, "
            f"{lon_min} {lat_min}"
            f"))"
        )
        logger.info(f"ROI Polygon: {polygon}")
        return polygon
    
    def query_new_files(self) -> List[Dict[str, Any]]:
        """
        Query EUMETSAT API for new files covering Paris ROI
        
        IMPORTANT: Searches for data at least min_age_hours old to avoid
        near-real-time data which requires special EUMETSAT licensing.
        
        Example: If min_age_hours=1 and lookback_minutes=30:
        - Current time: 10:00 UTC
        - Search window: 08:30-09:00 UTC (1 hour ago, for 30 minutes)
        
        Returns list of file metadata
        """
        lookback_minutes = config.get('polling.lookback_minutes', 30)
        min_age_hours = config.get('polling.min_age_hours', 1)
        
        # Calculate time window that avoids near-real-time data
        now = datetime.utcnow()
        
        # End time: min_age_hours ago (e.g., 1 hour ago)
        dtend = now - timedelta(hours=min_age_hours)
        
        # Start time: lookback_minutes before dtend
        dtstart = dtend - timedelta(minutes=lookback_minutes)
        
        logger.info(
            f"Querying EUMETSAT for files from {dtstart.strftime('%Y-%m-%d %H:%M:%S')} "
            f"to {dtend.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
        logger.info(
            f"(Avoiding near-real-time data: searching {min_age_hours} hour(s) back)"
        )
        
        start_time = time.time()
        
        try:
            # Perform search with spatial and temporal filters
            search_results = self.collection.search(
                dtstart=dtstart,
                dtend=dtend,
                geo=self.roi_polygon
            )
            
            # Get total results count
            total_results = search_results.total_results
            logger.info(f"Search returned {total_results} total results")
            
            if total_results == 0:
                logger.info("No files found in this time window")
                return []
            
            # Extract metadata from products
            file_list = []
            product_count = 0
            
            for product in search_results:
                product_count += 1
                try:
                    # On first product, inspect its structure
                    if product_count == 1:
                        self._inspect_product_structure(product)
                    
                    file_metadata = self._extract_product_metadata(product)
                    if file_metadata:
                        file_list.append(file_metadata)
                except Exception as e:
                    logger.error(f"Failed to extract metadata from product {product_count}: {e}")
                    continue
            
            query_duration = time.time() - start_time
            
            logger.info(
                f"Extracted metadata for {len(file_list)}/{product_count} files "
                f"in {query_duration:.2f}s"
            )
            
            # Log query to database
            db.log_api_query(
                query_timestamp=datetime.utcnow(),
                files_found=total_results,
                files_new=0,  # Will be updated after filtering
                query_duration=query_duration,
                status="SUCCESS"
            )
            
            return file_list
            
        except Exception as e:
            query_duration = time.time() - start_time
            logger.error(f"API query failed: {e}", exc_info=True)
            
            # Log failed query
            db.log_api_query(
                query_timestamp=datetime.utcnow(),
                files_found=0,
                files_new=0,
                query_duration=query_duration,
                status="FAILED",
                error_message=str(e)
            )
            
            return []
    
    def _inspect_product_structure(self, product):
        """
        Inspect Product object structure for debugging
        Logs all available attributes and methods
        """
        logger.info("=== Inspecting Product Structure ===")
        
        # Log object type
        logger.info(f"Product type: {type(product)}")
        
        # Log all attributes
        attrs = [attr for attr in dir(product) if not attr.startswith('_')]
        logger.info(f"Available attributes: {attrs}")
        
        # Check direct attributes
        for attr in ['sensing_start', 'sensing_end', 'satellite', 'size', 'product_type']:
            if hasattr(product, attr):
                try:
                    value = getattr(product, attr)
                    logger.info(f"product.{attr}: {value} (type: {type(value).__name__})")
                except Exception as e:
                    logger.warning(f"Could not access product.{attr}: {e}")
        
        # Try to access entries
        if hasattr(product, 'entries'):
            try:
                value = getattr(product, 'entries')
                logger.info(f"product.entries: {value}")
            except Exception as e:
                logger.warning(f"Could not access product.entries: {e}")
        
        # Inspect metadata.properties if available
        if hasattr(product, 'metadata') and isinstance(product.metadata, dict):
            logger.info(f"product.metadata keys: {list(product.metadata.keys())}")
            
            if 'properties' in product.metadata:
                try:
                    props = product.metadata['properties']
                    logger.info(f"product.metadata['properties'] keys: {list(props.keys())}")
                    # Log ALL properties for debugging
                    for key, value in props.items():
                        logger.info(f"  properties['{key}']: {value}")
                except Exception as e:
                    logger.warning(f"Could not access metadata properties: {e}")
        
        # Check if product has __str__ method
        try:
            product_str = str(product)
            logger.info(f"Product as string: {product_str}")
        except:
            pass
        
        logger.info("=== End Product Inspection ===")
    
    def _extract_product_metadata(self, product) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from EUMDAC Product object (v3.1.0+)
        
        Args:
            product: eumdac.product.Product object
            
        Returns:
            Dictionary with file metadata or None if extraction fails
        """
        try:
            metadata = {}
            
            # 1. Extract Product ID from string representation
            file_id = str(product)
            metadata['file_id'] = file_id
            logger.debug(f"Extracted file_id: {file_id}")
            
            # 2. Extract timestamp - try multiple sources
            timestamp = None
            
            # First try: direct attributes
            if hasattr(product, 'sensing_start') and product.sensing_start:
                timestamp = product.sensing_start
                logger.debug(f"Using sensing_start: {timestamp}")
            elif hasattr(product, 'sensing_end') and product.sensing_end:
                timestamp = product.sensing_end
                logger.debug(f"Using sensing_end: {timestamp}")
            
            # Second try: metadata properties
            if timestamp is None and hasattr(product, 'metadata'):
                if isinstance(product.metadata, dict) and 'properties' in product.metadata:
                    props = product.metadata['properties']
                    # Try common timestamp fields
                    for field in ['datetime', 'start_datetime', 'date', 'created']:
                        if field in props:
                            timestamp = props[field]
                            logger.debug(f"Using properties['{field}']: {timestamp}")
                            break
            
            # Fallback: use current time
            if timestamp is None:
                logger.warning(f"No timestamp found for {file_id}, using current time")
                timestamp = datetime.utcnow()
            
            # Convert timestamp to datetime if needed
            if isinstance(timestamp, str):
                try:
                    # Handle ISO format with Z
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    try:
                        # Try dateutil parser
                        from dateutil import parser
                        timestamp = parser.parse(timestamp)
                    except:
                        logger.warning(f"Could not parse timestamp: {timestamp}")
                        timestamp = datetime.utcnow()
            elif not isinstance(timestamp, datetime):
                logger.warning(f"Unexpected timestamp type: {type(timestamp)}")
                timestamp = datetime.utcnow()
            
            metadata['timestamp'] = timestamp
            
            # 3. Extract satellite from direct attribute
            satellite = "MSG"
            if hasattr(product, 'satellite') and product.satellite:
                satellite = str(product.satellite)
            metadata['satellite'] = satellite
            
            # 4. Extract product type from direct attribute
            product_type = "HRSEVIRI"
            if hasattr(product, 'product_type') and product.product_type:
                product_type = str(product.product_type)
            metadata['product_type'] = product_type
            
            # 5. Extract file size from direct attribute
            size_mb = 0.0
            if hasattr(product, 'size') and product.size:
                try:
                    size_mb = float(product.size) / (1024 * 1024)
                except:
                    pass
            metadata['size_mb'] = size_mb
            
            # 6. Store download information
            metadata['download_info'] = {
                'product_id': file_id,
                'collection_id': self.collection_id
            }
            
            logger.info(
                f"✓ Extracted: id={file_id[:50]}..., "
                f"time={timestamp.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"sat={satellite}, type={product_type}, size={size_mb:.2f}MB"
            )
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to extract product metadata: {e}", exc_info=True)
            return None
    
    def filter_new_files(self, file_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter out files that have already been processed
        Only returns NEW files that haven't been downloaded yet
        """
        new_files = []
        
        for file_meta in file_list:
            file_id = file_meta['file_id']
            
            # Check if file already exists in database
            if not db.is_file_processed(file_id):
                new_files.append(file_meta)
                logger.info(f"✓ NEW file: {file_id[:60]}...")
            else:
                logger.debug(f"✗ Already processed: {file_id[:60]}...")
        
        if new_files:
            logger.info(f"Found {len(new_files)} NEW files to download")
        else:
            logger.info("No new files - all files already in database")
        
        return new_files
    
    def enqueue_downloads(self, file_list: List[Dict[str, Any]]) -> int:
        """
        Enqueue download tasks using Celery
        Returns number of files successfully enqueued
        """
        enqueued_count = 0
        
        for file_meta in file_list:
            try:
                # Log to database first
                success = db.log_new_file(
                    file_id=file_meta['file_id'],
                    timestamp=file_meta['timestamp'],
                    satellite=file_meta['satellite'],
                    product_type=file_meta['product_type'],
                    file_size_mb=file_meta['size_mb']
                )
                
                if success:
                    # Create download task data
                    task_data = {
                        'file_id': file_meta['file_id'],
                        'timestamp': file_meta['timestamp'].isoformat(),
                        'satellite': file_meta['satellite'],
                        'size_mb': file_meta['size_mb'],
                        'collection_id': file_meta['download_info']['collection_id'],
                        'product_id': file_meta['download_info']['product_id']
                    }
                    
                    # Send task to Celery
                    self.celery_app.send_task(
                        'tasks.download_file',
                        args=[task_data],
                        queue='download_queue'
                    )
                    
                    enqueued_count += 1
                    logger.info(f"✓ Enqueued: {file_meta['file_id'][:60]}...")
                
            except Exception as e:
                logger.error(f"Failed to enqueue {file_meta['file_id']}: {e}")
        
        return enqueued_count
    
    def poll(self):
        """Execute one polling cycle"""
        logger.info("="*70)
        logger.info("POLLING CYCLE START")
        logger.info("="*70)
        
        try:
            # Step 1: Query EUMETSAT API
            all_files = self.query_new_files()
            
            if not all_files:
                logger.info("No files found in search window")
                logger.info("="*70)
                return
            
            # Step 2: Filter for NEW files
            new_files = self.filter_new_files(all_files)
            
            if not new_files:
                logger.info("All files already processed - nothing to download")
                logger.info("="*70)
                return
            
            # Step 3: Enqueue downloads
            enqueued = self.enqueue_downloads(new_files)
            
            logger.info("="*70)
            logger.info(f"RESULT: {enqueued} files enqueued for download")
            logger.info("="*70)
            
        except Exception as e:
            logger.error(f"Polling cycle failed: {e}", exc_info=True)
            logger.info("="*70)
    
    def get_queue_length(self) -> int:
        """Get current download queue length"""
        try:
            inspect = self.celery_app.control.inspect()
            reserved = inspect.reserved()
            if reserved:
                return sum(len(tasks) for tasks in reserved.values())
            return 0
        except Exception as e:
            logger.error(f"Failed to get queue length: {e}")
            return -1
    
    def close(self):
        """Cleanup connections"""
        try:
            self.redis_client.close()
            logger.info("Poller connections closed")
        except Exception as e:
            logger.error(f"Error closing connections: {e}")