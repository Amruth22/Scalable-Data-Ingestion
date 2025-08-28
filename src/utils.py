"""
Core utilities and configuration management for scalable data ingestion pipeline
"""

import yaml
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import time
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataSourceType(Enum):
    """Data source types"""
    API_REST = "api_rest"
    FILE_CSV = "file_csv"
    FILE_JSON = "file_json"
    DATABASE = "database"

class ProcessingStatus(Enum):
    """Processing status types"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class ProcessingResult:
    """Standard processing result container"""
    success: bool
    data: Any = None
    records_processed: int = 0
    error_message: Optional[str] = None
    execution_time: float = 0.0
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

class Config:
    """Configuration management class"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                logger.warning(f"Config file not found: {self.config_path}, using defaults")
                return self._get_default_config()
            
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            'database': {
                'path': 'data/orders.db',
                'connection_timeout': 30,
                'batch_size': 1000
            },
            'api': {
                'base_url': 'https://jsonplaceholder.typicode.com',
                'timeout': 30,
                'retry_attempts': 3,
                'retry_delay': 5
            },
            'pipeline': {
                'batch_size': 1000,
                'max_workers': 4,
                'log_level': 'INFO'
            },
            'data_quality': {
                'quality_threshold': 80,
                'required_fields': ['order_id', 'customer_name', 'product', 'quantity', 'price', 'order_date']
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key (supports dot notation)"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value

# Global configuration instance
config = Config()

def ensure_directory_exists(directory: str) -> None:
    """Ensure directory exists, create if not"""
    Path(directory).mkdir(parents=True, exist_ok=True)

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division with default value"""
    try:
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ZeroDivisionError):
        return default

def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def retry_operation(func, max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Retry operation with exponential backoff"""
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e
            
            wait_time = delay * (backoff ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)

def timing_decorator(func):
    """Decorator to measure function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Add timing info to result if it's a ProcessingResult
            if isinstance(result, ProcessingResult):
                result.execution_time = execution_time
            
            logger.debug(f"{func.__name__} executed in {execution_time:.2f}s")
            return result
        
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.2f}s: {e}")
            raise
    
    return wrapper

def validate_required_fields(data, required_fields: list) -> tuple[bool, list]:
    """Validate that required fields are present in data"""
    if hasattr(data, 'columns'):  # DataFrame
        missing_fields = [field for field in required_fields if field not in data.columns]
    elif isinstance(data, dict):  # Dictionary
        missing_fields = [field for field in required_fields if field not in data]
    else:
        return False, ["Invalid data type for validation"]
    
    return len(missing_fields) == 0, missing_fields

def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration"""
    ensure_directory_exists("logs")
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/pipeline.log'),
            logging.StreamHandler()
        ]
    )

if __name__ == "__main__":
    # Test configuration loading
    print("Testing configuration...")
    print(f"Database path: {config.get('database.path')}")
    print(f"API base URL: {config.get('api.base_url')}")
    print(f"Quality threshold: {config.get('data_quality.quality_threshold')}")
    
    # Test utilities
    print(f"Duration formatting: {format_duration(3661.5)}")
    print(f"Safe division: {safe_divide(10, 3)}")
    
    print("✅ Utils module working correctly!")