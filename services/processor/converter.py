"""
SEVIRI .nat file converter
"""
import logging
from typing import Optional, Dict, Any
import xarray as xr
from satpy import Scene
import os 
logger = logging.getLogger(__name__)


class SEVIRIConverter:
    """Converts SEVIRI .nat files to NetCDF"""
    
    def __init__(self):
        pass
    
    def load_nat_file(self, filepath: str) -> Optional[Scene]:
        """
        Load SEVIRI .nat file using satpy
        
        Args:
            filepath: Path to .nat file OR directory containing product files
            
        Returns:
            Satpy Scene object or None if failed
        """
        try:
            # Check if filepath is a directory
            if os.path.isdir(filepath):
                logger.info(f"Loading product directory: {filepath}")
                
                # Find all files in directory
                files_in_dir = os.listdir(filepath)
                logger.info(f"Files in directory: {files_in_dir}")
                
                # Find the main .nat file
                nat_files = [f for f in files_in_dir if f.endswith('.nat')]
                
                if not nat_files:
                    raise ValueError("No .nat file found in directory")
                
                # Use all files in the directory (Satpy will figure out what it needs)
                file_paths = [os.path.join(filepath, f) for f in files_in_dir]
                
            else:
                logger.info(f"Loading single file: {filepath}")
                file_paths = [filepath]
                
                # Check file exists and size
                if not os.path.exists(filepath):
                    raise FileNotFoundError(f"File not found: {filepath}")
                
                file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                logger.info(f"File size: {file_size_mb:.2f} MB")
            
            # Try to load with Satpy - pass all files
            logger.info(f"Loading {len(file_paths)} file(s) with Satpy")
            
            # Try native reader
            try:
                logger.info("Attempting seviri_l1b_native reader")
                scene = Scene(
                    filenames=file_paths,
                    reader='seviri_l1b_native'
                )
                
                # Load HRV channel
                scene.load(['HRV'], calibration='reflectance')
                
                logger.info("✓ Successfully loaded with seviri_l1b_native reader")
                return scene
                
            except Exception as e:
                logger.error(f"seviri_l1b_native reader failed: {e}")
                raise
            
        except Exception as e:
            logger.error(f"Failed to load .nat file: {e}", exc_info=True)
            return None
    
    def write_netcdf(
        self,
        dataset: xr.Dataset,
        output_path: str,
        quality_metrics: Dict[str, Any]
    ):
        """
        Write processed data to NetCDF file
        
        Args:
            dataset: xarray Dataset with processed data
            output_path: Output file path
            quality_metrics: Quality assessment results
        """
        try:
            logger.info(f"Writing NetCDF to: {output_path}")
            
            # Add global attributes
            dataset.attrs.update({
                'title': 'MSG SEVIRI HRV Reflectance - Paris Region',
                'institution': 'SEVIRI Pipeline',
                'source': 'EUMETSAT MSG SEVIRI Level 1.5',
                'processing_level': 'L2-subset',
                'spatial_resolution': '0.01 degrees (~1km)',
                'projection': 'WGS84 (EPSG:4326)',
                'original_projection': 'Geostationary 0E',
                'quality_score': quality_metrics.get('quality_score', 0),
                'missing_data_percentage': quality_metrics.get('missing_data_pct', 0),
                'saturation_percentage': quality_metrics.get('saturation_pct', 0),
                'processing_timestamp': str(xr.coding.times.CFDatetimeCoder().encode(
                    xr.DataArray([xr.CFTimeIndex([xr.CFDatetimeCoder().decode(
                        xr.DataArray([0]), 'seconds since 1970-01-01')])[0]])
                ))
            })
            
            # Encoding for compression
            encoding = {}
            for var in dataset.data_vars:
                encoding[var] = {
                    'zlib': True,
                    'complevel': 4,
                    'dtype': 'float32',
                    '_FillValue': -999.0
                }
            
            # Write to NetCDF
            dataset.to_netcdf(output_path, encoding=encoding, format='NETCDF4')
            
            logger.info(f"Successfully wrote NetCDF: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to write NetCDF: {e}", exc_info=True)
            raise