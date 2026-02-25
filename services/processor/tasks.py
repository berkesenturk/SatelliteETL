"""
Celery tasks for processing SEVIRI files
"""
import logging
import os
import time
import shutil
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

from celery_app import celery_app
from config import config
from database import db
from converter import SEVIRIConverter
from quality import QualityAssessment
from calibrator import Calibrator
from reprojector import Reprojector
from subsetter import Subsetter

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProcessingTask(Task):
    """Base task for processing"""
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure"""
        task_data = args[0] if args else {}
        file_id = task_data.get('file_id', 'unknown')
        
        logger.error(
            f"Processing task failed for {file_id}: {exc}",
            exc_info=True
        )
        
        # Update database with failure
        db.update_processing_status(
            file_id=file_id,
            status='PROCESSING_FAILED',
            error_message=str(exc)
        )


@celery_app.task(
    base=ProcessingTask,
    bind=True,
    name='tasks.process_file'
)
def process_file(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process SEVIRI file: convert, calibrate, reproject, subset
    
    Args:
        task_data: Dictionary containing:
            - file_id: Unique file identifier
            - file_path: Path to raw .nat file
            - timestamp: File timestamp
            - satellite: Satellite name
    
    Returns:
        Dictionary with processing result
    """
    file_id = task_data['file_id']
    input_path = task_data['file_path']
    timestamp = task_data['timestamp']
    
    logger.info(f"Starting processing for {file_id}")
    
    # Update status: PROCESSING
    db.update_processing_status(
        file_id=file_id,
        status='PROCESSING',
        attempt=1
    )
    
    total_start_time = time.time()
    
    try:
        # Step 1: Load and convert .nat file
        logger.info(f"[{file_id}] Step 1: Loading .nat file")
        stage_start = time.time()
        
        converter = SEVIRIConverter()
        scene = converter.load_nat_file(input_path)
        
        if scene is None:
            raise ValueError("Failed to load .nat file")
        
        db.log_processing_metrics(
            file_id=file_id,
            stage='load_nat',
            duration_seconds=time.time() - stage_start,
            status='SUCCESS'
        )
        
        # Step 2: Quality assessment
        logger.info(f"[{file_id}] Step 2: Quality assessment")
        stage_start = time.time()
        
        qa = QualityAssessment(config)
        quality_result = qa.assess(scene)
        
        db.log_processing_metrics(
            file_id=file_id,
            stage='quality_check',
            duration_seconds=time.time() - stage_start,
            status='SUCCESS'
        )
        
        # Check if file should be skipped
        if quality_result['skip']:
            logger.warning(
                f"[{file_id}] Skipping file: {quality_result['skip_reason']}"
            )
            
            db.update_processing_status(
                file_id=file_id,
                status='SKIPPED',
                skip_reason=quality_result['skip_reason'],
                quality_score=quality_result['quality_score'],
                missing_data_pct=quality_result['missing_data_pct'],
                mean_solar_zenith=quality_result.get('mean_solar_zenith'),
                processing_duration=time.time() - total_start_time
            )
            
            # Cleanup raw file
            cleanup_file(input_path)
            
            return {
                'status': 'skipped',
                'file_id': file_id,
                'reason': quality_result['skip_reason']
            }
        
        # Step 3: Calibrate to reflectance
        logger.info(f"[{file_id}] Step 3: Calibrating to reflectance")
        stage_start = time.time()
        
        calibrator = Calibrator()
        calibrated_scene = calibrator.calibrate(scene)
        
        db.log_processing_metrics(
            file_id=file_id,
            stage='calibration',
            duration_seconds=time.time() - stage_start,
            status='SUCCESS'
        )
        
        # Step 4: Reproject to WGS84
        logger.info(f"[{file_id}] Step 4: Reprojecting to WGS84")
        stage_start = time.time()
        
        reprojector = Reprojector(config)
        reprojected_scene = reprojector.reproject(calibrated_scene)
        
        db.log_processing_metrics(
            file_id=file_id,
            stage='reprojection',
            duration_seconds=time.time() - stage_start,
            status='SUCCESS'
        )
        
        # Step 5: Subset to Paris AOI
        logger.info(f"[{file_id}] Step 5: Subsetting to Paris AOI")
        stage_start = time.time()
        
        subsetter = Subsetter(config)
        subset_data = subsetter.subset(reprojected_scene)
        
        db.log_processing_metrics(
            file_id=file_id,
            stage='subsetting',
            duration_seconds=time.time() - stage_start,
            status='SUCCESS'
        )
        
        # Step 6: Write output NetCDF
        logger.info(f"[{file_id}] Step 6: Writing NetCDF output")
        stage_start = time.time()
        
        output_path = generate_output_path(timestamp)
        converter.write_netcdf(subset_data, output_path, quality_result)
        
        db.log_processing_metrics(
            file_id=file_id,
            stage='write_netcdf',
            duration_seconds=time.time() - stage_start,
            status='SUCCESS'
        )
        
        # Calculate total processing time
        total_duration = time.time() - total_start_time
        
        logger.info(
            f"[{file_id}] Processing complete in {total_duration:.2f}s: {output_path}"
        )
        
        # Update database: COMPLETE
        db.update_processing_status(
            file_id=file_id,
            status='COMPLETE',
            output_path=output_path,
            quality_score=quality_result['quality_score'],
            missing_data_pct=quality_result['missing_data_pct'],
            saturation_pct=quality_result.get('saturation_pct', 0),
            mean_solar_zenith=quality_result.get('mean_solar_zenith'),
            processing_duration=total_duration
        )
        
        # Step 7: Cleanup raw file
        logger.info(f"[{file_id}] Step 7: Cleaning up raw file")
        cleanup_file(input_path)
        
        return {
            'status': 'success',
            'file_id': file_id,
            'output_path': output_path,
            'duration_sec': total_duration,
            'quality_score': quality_result['quality_score']
        }
        
    except SoftTimeLimitExceeded:
        logger.error(f"[{file_id}] Processing timed out")
        
        db.update_processing_status(
            file_id=file_id,
            status='PROCESSING_FAILED',
            error_message='Processing timeout'
        )
        
        cleanup_file(input_path)
        
        return {
            'status': 'timeout',
            'file_id': file_id,
            'error': 'Processing timeout'
        }
        
    except Exception as e:
        logger.error(f"[{file_id}] Processing failed: {e}", exc_info=True)
        
        # Log failed metrics
        db.log_processing_metrics(
            file_id=file_id,
            stage='processing',
            duration_seconds=time.time() - total_start_time,
            status='FAILED',
            error_message=str(e)
        )
        
        db.update_processing_status(
            file_id=file_id,
            status='PROCESSING_FAILED',
            error_message=str(e),
            processing_duration=time.time() - total_start_time
        )
        
        cleanup_file(input_path)
        
        return {
            'status': 'error',
            'file_id': file_id,
            'error': str(e)
        }


def generate_output_path(timestamp: str) -> str:
    """Generate output NetCDF file path"""
    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    year_month_day = dt.strftime('%Y/%m/%d')
    filename = dt.strftime('%Y%m%d_%H%M%S') + '_HRV_Paris.nc'
    
    output_dir = f"/processed/{year_month_day}"
    os.makedirs(output_dir, exist_ok=True)
    
    return f"{output_dir}/{filename}"


def cleanup_file(filepath: str):
    """Delete file or directory after processing"""
    try:
        if os.path.isdir(filepath):
            shutil.rmtree(filepath)
            logger.info(f"Deleted directory: {filepath}")
        elif os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Deleted file: {filepath}")
    except Exception as e:
        logger.error(f"Failed to delete {filepath}: {e}")