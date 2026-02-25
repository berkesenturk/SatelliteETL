"""
Database operations for Processor service
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from config import config

logger = logging.getLogger(__name__)


class Database:
    """Database connection and operations manager"""
    
    def __init__(self):
        self.pool = self._create_pool()
    
    def _create_pool(self) -> ThreadedConnectionPool:
        """Create connection pool"""
        try:
            pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=config.postgres_host,
                port=config.postgres_port,
                database=config.postgres_db,
                user=config.postgres_user,
                password=config.postgres_password
            )
            logger.info("Database connection pool created successfully")
            return pool
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    def get_connection(self):
        """Get connection from pool"""
        return self.pool.getconn()
    
    def return_connection(self, conn):
        """Return connection to pool"""
        self.pool.putconn(conn)
    
    def update_processing_status(
        self,
        file_id: str,
        status: str,
        attempt: Optional[int] = None,
        error_message: Optional[str] = None,
        skip_reason: Optional[str] = None,
        output_path: Optional[str] = None,
        quality_score: Optional[float] = None,
        missing_data_pct: Optional[float] = None,
        saturation_pct: Optional[float] = None,
        mean_solar_zenith: Optional[float] = None,
        processing_duration: Optional[float] = None
    ) -> bool:
        """Update processing status in database"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Build update query dynamically
                updates = ["status = %s", "updated_at = NOW()"]
                params = [status]
                
                if attempt is not None:
                    updates.append("processing_attempt = %s")
                    params.append(attempt)
                
                if error_message is not None:
                    updates.append("error_message = %s")
                    params.append(error_message)
                
                if skip_reason is not None:
                    updates.append("skip_reason = %s")
                    params.append(skip_reason)
                
                if output_path is not None:
                    updates.append("output_file_path = %s")
                    params.append(output_path)
                
                if quality_score is not None:
                    updates.append("quality_score = %s")
                    params.append(quality_score)
                
                if missing_data_pct is not None:
                    updates.append("missing_data_pct = %s")
                    params.append(missing_data_pct)
                
                if saturation_pct is not None:
                    updates.append("saturation_pct = %s")
                    params.append(saturation_pct)
                
                if mean_solar_zenith is not None:
                    updates.append("mean_solar_zenith = %s")
                    params.append(mean_solar_zenith)
                
                if processing_duration is not None:
                    updates.append("processing_duration_sec = %s")
                    params.append(processing_duration)
                
                # Set timestamps based on status
                if status == 'PROCESSING':
                    updates.append("processing_started_at = NOW()")
                elif status in ('COMPLETE', 'SKIPPED'):
                    updates.append("processed_at = NOW()")
                
                params.append(file_id)
                
                query = f"""
                    UPDATE file_manifest 
                    SET {', '.join(updates)}
                    WHERE file_id = %s
                """
                
                cursor.execute(query, params)
                conn.commit()
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update status for {file_id}: {e}")
            return False
        finally:
            self.return_connection(conn)
    
    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Get file information from database"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM file_manifest WHERE file_id = %s",
                    (file_id,)
                )
                result = cursor.fetchone()
                return dict(result) if result else None
        finally:
            self.return_connection(conn)
    
    def log_processing_metrics(
        self,
        file_id: str,
        stage: str,
        duration_seconds: float,
        memory_mb: Optional[float] = None,
        cpu_percent: Optional[float] = None,
        status: str = "SUCCESS",
        error_message: Optional[str] = None
    ):
        """Log processing metrics"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO processing_metrics 
                    (file_id, stage, duration_seconds, memory_mb, 
                     cpu_percent, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (file_id, stage, duration_seconds, memory_mb, 
                     cpu_percent, status, error_message)
                )
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to log metrics: {e}")
        finally:
            self.return_connection(conn)
    
    def close(self):
        """Close all connections"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connections closed")


# Global database instance
db = Database()