"""
Radiometric calibration for SEVIRI HRV
"""
import logging
from satpy import Scene

logger = logging.getLogger(__name__)


class Calibrator:
    """Calibrates SEVIRI HRV to TOA reflectance"""
    
    def __init__(self):
        pass
    
    def calibrate(self, scene: Scene) -> Scene:
        """
        Calibrate HRV to top-of-atmosphere reflectance
        
        Note: Satpy handles calibration automatically when loading
        with calibration='reflectance', so this is mostly a pass-through
        with validation.
        
        Args:
            scene: Satpy Scene with HRV data
            
        Returns:
            Calibrated Scene
        """
        logger.info("Calibrating HRV to reflectance")
        
        try:
            # Verify HRV is in reflectance units
            hrv = scene['HRV']
            
            if hasattr(hrv, 'attrs'):
                calibration = hrv.attrs.get('calibration', 'unknown')
                units = hrv.attrs.get('units', 'unknown')
                
                logger.info(f"HRV calibration: {calibration}, units: {units}")
                
                if calibration != 'reflectance':
                    logger.warning(
                        f"Expected reflectance calibration, got: {calibration}"
                    )
            
            # Satpy has already applied:
            # - Radiance calibration (counts -> radiance)
            # - Reflectance conversion (radiance -> reflectance)
            # - Solar zenith angle correction
            # - Earth-Sun distance correction
            
            logger.info("Calibration complete")
            return scene
            
        except Exception as e:
            logger.error(f"Calibration failed: {e}", exc_info=True)
            raise