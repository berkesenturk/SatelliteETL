"""
Reprojection from geostationary to WGS84
"""
import logging
from satpy import Scene
from pyresample import create_area_def

logger = logging.getLogger(__name__)


class Reprojector:
    """Reprojects SEVIRI data from geostationary to WGS84"""
    
    def __init__(self, config):
        self.config = config
        self.target_resolution = config.get('processing.output_resolution', 0.01)
        
        # Paris ROI
        roi = config.get('roi')
        self.lon_min = roi['lon_min']
        self.lon_max = roi['lon_max']
        self.lat_min = roi['lat_min']
        self.lat_max = roi['lat_max']
    
    def reproject(self, scene: Scene) -> Scene:
        """
        Reproject from geostationary to WGS84
        
        Args:
            scene: Satpy Scene in geostationary projection
            
        Returns:
            Reprojected Scene in WGS84
        """
        logger.info("Reprojecting to WGS84")
        
        try:
            # Define target area (WGS84 for Paris region)
            target_area = create_area_def(
                'paris_wgs84',
                'EPSG:4326',
                area_extent=[
                    self.lon_min,
                    self.lat_min,
                    self.lon_max,
                    self.lat_max
                ],
                resolution=self.target_resolution,
                units='degrees',
                description='Paris region in WGS84'
            )
            
            logger.info(
                f"Target area: {target_area.shape[0]}x{target_area.shape[1]} pixels "
                f"at {self.target_resolution}° resolution"
            )
            
            # Resample using nearest neighbor (preserves sharp cloud edges)
            # This is important for cloud hole detection
            reprojected_scene = scene.resample(
                target_area,
                resampler='nearest',
                radius_of_influence=2000  # 2km search radius
            )
            
            logger.info("Reprojection complete")
            return reprojected_scene
            
        except Exception as e:
            logger.error(f"Reprojection failed: {e}", exc_info=True)
            raise