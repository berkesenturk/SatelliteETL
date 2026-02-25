"""
Quality assessment for SEVIRI data
"""
import logging
from typing import Dict, Any
import numpy as np
from satpy import Scene

logger = logging.getLogger(__name__)


class QualityAssessment:
    """Performs quality checks on SEVIRI data"""
    
    def __init__(self, config):
        self.config = config
        self.max_missing_pct = config.get('quality.max_missing_percentage', 50)
        self.max_solar_zenith = config.get('quality.min_solar_zenith', 85)
    
    def assess(self, scene: Scene) -> Dict[str, Any]:
        """
        Perform comprehensive quality assessment
        
        Args:
            scene: Satpy Scene with loaded HRV data
            
        Returns:
            Dictionary with quality metrics and skip decision
        """
        logger.info("Performing quality assessment")
        
        result = {
            'skip': False,
            'skip_reason': None,
            'quality_score': 100.0,
            'missing_data_pct': 0.0,
            'saturation_pct': 0.0,
            'mean_solar_zenith': None
        }
        
        try:
            # Get HRV data
            hrv = scene['HRV'].values
            
            # Check 1: Missing data
            nan_count = np.isnan(hrv).sum()
            total_pixels = hrv.size
            missing_pct = (nan_count / total_pixels) * 100
            
            result['missing_data_pct'] = missing_pct
            
            if missing_pct > self.max_missing_pct:
                result['skip'] = True
                result['skip_reason'] = f'EXCESSIVE_MISSING_DATA ({missing_pct:.1f}%)'
                result['quality_score'] = 0
                return result
            
            # Check 2: Solar zenith angle (HRV is solar channel)
            if 'solar_zenith_angle' in scene:
                sza = scene['solar_zenith_angle'].values
                mean_sza = np.nanmean(sza)
                result['mean_solar_zenith'] = float(mean_sza)
                
                if mean_sza > self.max_solar_zenith:
                    result['skip'] = True
                    result['skip_reason'] = f'NIGHTTIME_IMAGE (SZA={mean_sza:.1f}°)'
                    result['quality_score'] = 0
                    return result
            
            # Check 3: Saturation
            # HRV saturates at very bright clouds (reflectance ~1.0)
            saturation_threshold = 0.95
            saturated_pixels = (hrv > saturation_threshold).sum()
            saturation_pct = (saturated_pixels / total_pixels) * 100
            
            result['saturation_pct'] = float(saturation_pct)
            
            # Note: Saturation is OK for cloud hole work, just log it
            if saturation_pct > 10:
                logger.warning(f"High saturation: {saturation_pct:.1f}%")
            
            # Calculate quality score (0-100)
            # Penalize for missing data, but not for saturation
            quality_score = 100 - missing_pct
            result['quality_score'] = max(0, quality_score)
            
            logger.info(
                f"Quality assessment complete: "
                f"score={quality_score:.1f}, missing={missing_pct:.1f}%, "
                f"saturation={saturation_pct:.1f}%"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Quality assessment failed: {e}", exc_info=True)
            result['skip'] = True
            result['skip_reason'] = f'QUALITY_CHECK_ERROR: {str(e)}'
            return result