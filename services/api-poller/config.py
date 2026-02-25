"""
Configuration loader for API Poller service
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any


class Config:
    """Configuration management class"""
    
    def __init__(self, config_file: str = "/app/config/poller.yaml"):
        self.config_file = config_file
        self.config = self._load_config()
        self._load_env_vars()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not Path(self.config_file).exists():
            raise FileNotFoundError(f"Config file not found: {self.config_file}")
        
        with open(self.config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def _load_env_vars(self):
        """Load sensitive data from environment variables"""
        # EUMETSAT credentials
        self.eumetsat_key = os.getenv('EUMETSAT_KEY')
        self.eumetsat_secret = os.getenv('EUMETSAT_SECRET')
        
        if not self.eumetsat_key or not self.eumetsat_secret:
            raise ValueError("EUMETSAT_KEY and EUMETSAT_SECRET must be set")
        
        # Redis connection
        self.redis_host = os.getenv('REDIS_HOST', 'redis')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        
        # PostgreSQL connection
        self.postgres_host = os.getenv('POSTGRES_HOST', 'postgres')
        self.postgres_port = int(os.getenv('POSTGRES_PORT', 5432))
        self.postgres_db = os.getenv('POSTGRES_DB', 'seviri_pipeline')
        self.postgres_user = os.getenv('POSTGRES_USER', 'seviri')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD')
        
        if not self.postgres_password:
            raise ValueError("POSTGRES_PASSWORD must be set")
        
        # Logging
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    def get(self, key: str, default=None):
        """Get configuration value by key"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            value = value.get(k, default)
            if value is None:
                return default
        return value
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    @property
    def redis_url(self) -> str:
        """Get Redis connection URL"""
        return f"redis://{self.redis_host}:{self.redis_port}/0"


# Global config instance
config = Config()
