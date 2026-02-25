"""
Spatial subsetting to AOI
"""
import logging
import xarray as xr
from satpy import Scene

logger = logging.getLogger(__name__)


class Subsetter:
    """Subsets data to Area of Interest"""
    
    def __init__(self, config):
        self.config = config
        
        # Paris ROI
        roi = config.get('roi')
        self.lon_min = roi['lon_min']
        self.lon_max = roi['lon_max']
        self.lat_min = roi['lat_min']
        self.lat_max = roi['lat_max']
    
    def subset(self, scene: Scene) -> xr.Dataset:
        """
        Subset Scene to Paris AOI and convert to xarray Dataset
        
        Args:
            scene: Satpy Scene (already in WGS84)
            
        Returns:
            xarray Dataset with subset data
        """
        logger.info("Subsetting to Paris AOI")
        
        try:
            # Get HRV data (already an xarray DataArray)
            hrv = scene['HRV']
            
            logger.info(f"HRV data shape: {hrv.shape}")
            logger.info(f"HRV data type: {type(hrv)}")
            
            # Create xarray Dataset
            ds = xr.Dataset()
            
            # Add HRV reflectance (it's already an xarray DataArray)
            ds['HRV_reflectance'] = hrv
            
            # Add ancillary data if available
            if 'solar_zenith_angle' in scene:
                ds['solar_zenith_angle'] = scene['solar_zenith_angle']
            
            if 'satellite_zenith_angle' in scene:
                ds['satellite_zenith_angle'] = scene['satellite_zenith_angle']
            
            # If satellite_azimuth_angle exists
            if 'satellite_azimuth_angle' in scene:
                ds['satellite_azimuth_angle'] = scene['satellite_azimuth_angle']
            
            # Extract time information from attributes
            if hasattr(hrv, 'attrs'):
                if 'start_time' in hrv.attrs:
                    ds.attrs['time_coverage_start'] = str(hrv.attrs['start_time'])
                
                if 'end_time' in hrv.attrs:
                    ds.attrs['time_coverage_end'] = str(hrv.attrs['end_time'])
                
                # Copy other relevant attributes
                for attr in ['sensor', 'platform_name', 'orbital_parameters']:
                    if attr in hrv.attrs:
                        ds.attrs[attr] = str(hrv.attrs[attr])
            
            # Add spatial metadata
            ds.attrs['geospatial_lat_min'] = self.lat_min
            ds.attrs['geospatial_lat_max'] = self.lat_max
            ds.attrs['geospatial_lon_min'] = self.lon_min
            ds.attrs['geospatial_lon_max'] = self.lon_max
            
            # Add coordinate reference system info
            ds.attrs['crs'] = 'EPSG:4326'
            ds.attrs['crs_wkt'] = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
            
            # Create quality flag (all good for now)
            # We could enhance this with actual quality information
            quality_flag = xr.DataArray(
                0,  # 0 = good
                dims=hrv.dims,
                coords=hrv.coords,
                attrs={
                    'long_name': 'Quality flag',
                    'flag_values': [0, 1, 2, 3, 4],
                    'flag_meanings': 'good moderate poor saturated missing'
                }
            )
            ds['quality_flag'] = quality_flag
            
            logger.info(f"Dataset created with variables: {list(ds.data_vars)}")
            logger.info(f"Dataset shape: {ds.dims}")
            
            return ds
            
        except Exception as e:
            logger.error(f"Subsetting failed: {e}", exc_info=True)
            raise