"""
Scalable Data Ingestion Pipeline
A comprehensive data processing pipeline for ingesting, validating, transforming, and storing data
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"
__description__ = "Scalable data ingestion pipeline with comprehensive data processing capabilities"

# Import main components for easy access
from .pipeline import PipelineManager, PipelineResult
from .utils import Config, ProcessingResult
from .ingestion import DataIngestion
from .validation import ValidationOrchestrator
from .transformation import TransformationOrchestrator
from .storage import StorageOrchestrator

__all__ = [
    'PipelineManager',
    'PipelineResult', 
    'Config',
    'ProcessingResult',
    'DataIngestion',
    'ValidationOrchestrator',
    'TransformationOrchestrator',
    'StorageOrchestrator'
]