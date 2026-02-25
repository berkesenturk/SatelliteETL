"""
Database operations for API Poller service
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
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
                maxconn=config.get('database.pool_size', 5),
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
    
    def is_file_processed(self, file_id: str) -> bool:
        """Check if file has already been processed"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM file_manifest WHERE file_id = %s",
                    (file_id,)
                )
                return cursor.fetchone() is not None
        finally:
            self.return_connection(conn)
    
    def log_new_file(
        self,
        file_id: str,
        timestamp: datetime,
        satellite: str,
        product_type: str,
        file_size_mb: float
    ) -> bool:
        """Log new file to manifest"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO file_manifest 
                    (file_id, timestamp, satellite, product_type, status, 
                     queued_at, file_size_mb)
                    VALUES (%s, %s, %s, %s, 'QUEUED', NOW(), %s)
                    ON CONFLICT (file_id) DO NOTHING
                    RETURNING id
                    """,
                    (file_id, timestamp, satellite, product_type, file_size_mb)
                )
                result = cursor.fetchone()
                conn.commit()
                return result is not None
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to log file {file_id}: {e}")
            return False
        finally:
            self.return_connection(conn)
    
    def log_api_query(
        self,
        query_timestamp: datetime,
        files_found: int,
        files_new: int,
        query_duration: float,
        status: str = "SUCCESS",
        error_message: Optional[str] = None
    ):
        """Log API query to database"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO api_query_log 
                    (query_timestamp, files_found, files_new, 
                     query_duration_sec, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (query_timestamp, files_found, files_new, 
                     query_duration, status, error_message)
                )
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to log API query: {e}")
        finally:
            self.return_connection(conn)
    
    def log_health_check(
        self,
        service: str,
        status: str,
        message: Optional[str] = None,
        response_time_ms: Optional[float] = None
    ):
        """Log health check result"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO health_checks 
                    (service, status, message, response_time_ms)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (service, status, message, response_time_ms)
                )
                conn.commit()
        finally:
            self.return_connection(conn)
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get statistics about queued files"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT 
                        status,
                        COUNT(*) as count
                    FROM file_manifest
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                    GROUP BY status
                    """
                )
                results = cursor.fetchall()
                return {row['status']: row['count'] for row in results}
        finally:
            self.return_connection(conn)
    
    def close(self):
        """Close all connections"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connections closed")


# Global database instance
db = Database()
