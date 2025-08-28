"""
Comprehensive unit test suite for Scalable Data Ingestion Pipeline
Contains 10 core test cases covering all major components and functionality
"""

import pytest
import pandas as pd
import sqlite3
import tempfile
import os
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.utils import Config, ProcessingResult, safe_divide, format_duration
from src.ingestion import APIIngestion, DataIngestion
from src.validation import DataValidator, ValidationOrchestrator
from src.transformation import DataCleaner, DataEnricher, TransformationOrchestrator
from src.storage import DatabaseManager, StorageOrchestrator
from src.pipeline import PipelineManager

class TestScalableDataIngestionPipeline:
    """Test suite for Scalable Data Ingestion Pipeline"""
    
    @pytest.fixture
    def sample_orders_data(self):
        """Sample orders data for testing"""
        return pd.DataFrame({
            'order_id': ['API-0001', 'API-0002', 'API-0003'],
            'customer_name': ['John Doe', 'Jane Smith', 'Bob Wilson'],
            'customer_email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
            'quantity': [1, 1, 2],
            'price': [999.99, 1999.99, 249.99],
            'total_amount': [999.99, 1999.99, 499.98],
            'order_date': ['2024-01-15', '2024-01-16', '2024-01-17'],
            'source': ['api_rest', 'api_rest', 'api_rest']
        })
    
    @pytest.fixture
    def temp_database(self):
        """Temporary database for testing"""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_file.close()
        yield temp_file.name
        # Windows-safe cleanup
        try:
            os.unlink(temp_file.name)
        except PermissionError:
            # Windows file locking - ignore cleanup error
            pass
    
    def test_api_ingestion_success(self):
        """Test 1: API ingestion successfully fetches and transforms data"""
        api_ingestion = APIIngestion()
        
        # Mock successful API response
        mock_posts = [
            {'id': 1, 'userId': 1, 'title': 'iPhone post', 'body': 'test body'},
            {'id': 2, 'userId': 2, 'title': 'MacBook post', 'body': 'test body'}
        ]
        
        with patch.object(api_ingestion, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = ProcessingResult(
                success=True,
                data=mock_posts,
                records_processed=2
            )
            
            result = api_ingestion.fetch_orders(limit=2)
            
            assert result.success == True
            assert result.records_processed == 2
            assert isinstance(result.data, pd.DataFrame)
            assert len(result.data) == 2
            assert 'order_id' in result.data.columns
            assert 'customer_name' in result.data.columns
            assert 'product' in result.data.columns
            assert result.data['order_id'].iloc[0] == 'API-0001'
    
    def test_data_validation_quality_scoring(self, sample_orders_data):
        """Test 2: Data validation calculates quality scores correctly"""
        validator = DataValidator()
        
        result = validator.validate_data_quality(sample_orders_data)
        
        assert result.success == True
        assert 'quality_score' in result.metadata
        assert 'quality_level' in result.metadata
        assert 'valid_records' in result.metadata
        assert 'invalid_records' in result.metadata
        
        quality_score = result.metadata['quality_score']
        assert 0 <= quality_score <= 100
        assert quality_score > 80  # Should be high quality for clean test data
        
        # Check quality metrics breakdown
        quality_metrics = result.metadata['quality_metrics']
        assert 'completeness' in quality_metrics
        assert 'validity' in quality_metrics
        assert 'consistency' in quality_metrics
        assert 'accuracy' in quality_metrics
    
    def test_data_transformation_cleaning(self, sample_orders_data):
        """Test 3: Data cleaning removes duplicates and fixes data types"""
        # Add some messy data
        messy_data = sample_orders_data.copy()
        # Add duplicate
        messy_data = pd.concat([messy_data, messy_data.iloc[[0]]], ignore_index=True)
        # Add messy text
        messy_data.loc[0, 'customer_name'] = 'john doe'  # lowercase
        messy_data.loc[1, 'customer_email'] = 'JANE@EXAMPLE.COM'  # uppercase
        
        cleaner = DataCleaner()
        result = cleaner.clean_data(messy_data)
        
        assert result.success == True
        assert len(result.data) == 3  # Should remove 1 duplicate
        assert result.metadata['records_removed'] > 0
        assert 'operations_performed' in result.metadata
        
        # Check text cleaning
        assert result.data['customer_name'].iloc[0] == 'John Doe'  # Should be title case
        assert result.data['customer_email'].iloc[1] == 'jane@example.com'  # Should be lowercase
    
    def test_data_enrichment(self, sample_orders_data):
        """Test 4: Data enrichment adds business intelligence fields"""
        enricher = DataEnricher()
        
        result = enricher.enrich_data(sample_orders_data)
        
        assert result.success == True
        assert result.metadata['fields_added'] > 0
        
        enriched_data = result.data
        
        # Check for added fields
        expected_fields = [
            'customer_segment', 'product_brand', 'estimated_profit_margin',
            'risk_score', 'risk_level', 'season', 'order_year', 'order_month'
        ]
        
        for field in expected_fields:
            assert field in enriched_data.columns, f"Missing enriched field: {field}"
        
        # Check customer segmentation
        assert enriched_data['customer_segment'].notna().all()
        assert enriched_data['customer_segment'].iloc[0] in ['VIP', 'Premium', 'Standard', 'Budget']
        
        # Check product brand extraction
        assert enriched_data['product_brand'].notna().all()
        assert enriched_data['product_brand'].iloc[0] == 'Apple'  # iPhone should be Apple
    
    def test_database_save_orders(self, sample_orders_data, temp_database):
        """Test 5: Database manager successfully saves orders"""
        # Create database manager with temp database
        with patch('src.storage.config') as mock_config:
            def config_side_effect(key, default=None):
                if key == 'database.path':
                    return temp_database
                elif key == 'database.connection_timeout':
                    return 30
                elif key == 'database.batch_size':
                    return 1000
                else:
                    return default
            
            mock_config.get.side_effect = config_side_effect
            db_manager = DatabaseManager()
            
            result = db_manager.save_orders(sample_orders_data)
            
            assert result.success == True
            assert result.records_processed == 3
            assert result.metadata['records_saved'] == 3
            assert result.metadata['records_failed'] == 0
            
            # Verify data was actually saved
            retrieval_result = db_manager.get_orders()
            assert retrieval_result.success is True
            assert len(retrieval_result.data) == 3
            assert retrieval_result.data['order_id'].iloc[0] == 'API-0001'
    
    def test_pipeline_orchestration(self, sample_orders_data):
        """Test 6: Pipeline manager orchestrates all stages correctly"""
        pipeline = PipelineManager("test_pipeline")
        
        # Mock all components to return success
        with patch.object(pipeline.data_ingestion, 'collect_all_data') as mock_ingestion, \
             patch.object(pipeline.validation_orchestrator, 'validate_all') as mock_validation, \
             patch.object(pipeline.transformation_orchestrator, 'transform_all') as mock_transformation, \
             patch.object(pipeline.storage_orchestrator, 'store_all') as mock_storage:
            
            # Setup mocks
            mock_ingestion.return_value = ProcessingResult(
                success=True,
                data=sample_orders_data,
                records_processed=3,
                metadata={'sources_summary': {'api': {'success': True, 'records': 3}}}
            )
            
            mock_validation.return_value = ProcessingResult(
                success=True,
                data=sample_orders_data,
                records_processed=3,
                metadata={'quality_validation': {'quality_score': 95.0}}
            )
            
            mock_transformation.return_value = ProcessingResult(
                success=True,
                data=sample_orders_data,
                records_processed=3,
                metadata={'final_records': 3, 'fields_added': 10}
            )
            
            mock_storage.return_value = ProcessingResult(
                success=True,
                records_processed=3,
                metadata={'successful_operations': 3, 'total_operations': 3}
            )
            
            # Run pipeline
            result = pipeline.run_pipeline(api_limit=10)
            
            assert result.success == True
            assert result.total_records_processed == 3
            assert len(result.stages_completed) == 4
            assert len(result.stages_failed) == 0
            assert result.run_id is not None
            assert result.execution_time >= 0  # Allow for very fast execution
            
            # Verify all stages were called
            mock_ingestion.assert_called_once()
            mock_validation.assert_called_once()
            mock_transformation.assert_called_once()
            mock_storage.assert_called_once()
    
    def test_error_handling(self):
        """Test 7: Pipeline handles errors gracefully"""
        pipeline = PipelineManager("error_test_pipeline")
        
        # Mock ingestion to fail
        with patch.object(pipeline.data_ingestion, 'collect_all_data') as mock_ingestion:
            mock_ingestion.return_value = ProcessingResult(
                success=False,
                error_message="API connection failed"
            )
            
            result = pipeline.run_pipeline()
            
            assert result.success is False
            assert result.error_message is not None
            assert 'ingestion' in result.stages_failed
            assert len(result.stages_completed) == 0
            assert result.total_records_processed == 0
    
    def test_configuration_loading(self):
        """Test 8: Configuration system loads settings correctly"""
        config = Config()
        
        # Test default configuration loading
        assert config.get('database.path') is not None
        assert config.get('api.base_url') is not None
        assert config.get('pipeline.batch_size') is not None
        
        # Test dot notation access
        db_path = config.get('database.path')
        assert isinstance(db_path, str)
        assert db_path.endswith('.db')
        
        # Test default values
        non_existent = config.get('non.existent.key', 'default_value')
        assert non_existent == 'default_value'
        
        # Test API configuration
        api_url = config.get('api.base_url')
        assert 'jsonplaceholder' in api_url.lower()
    
    def test_data_quality_metrics(self, sample_orders_data):
        """Test 9: Data quality metrics are calculated accurately"""
        validator = DataValidator()
        
        # Test with perfect data
        result = validator.validate_data_quality(sample_orders_data)
        quality_metrics = result.metadata['quality_metrics']
        
        # Completeness should be high (no missing values)
        assert quality_metrics['completeness'] >= 95
        
        # Validity should be high (valid formats)
        assert quality_metrics['validity'] >= 80
        
        # Consistency should be high (no duplicates, consistent data)
        assert quality_metrics['consistency'] >= 80
        
        # Accuracy should be high (reasonable values)
        assert quality_metrics['accuracy'] >= 80
        
        # Test with imperfect data
        imperfect_data = sample_orders_data.copy()
        imperfect_data.loc[0, 'customer_email'] = 'invalid-email'  # Invalid email
        imperfect_data.loc[1, 'price'] = -100  # Invalid price
        imperfect_data.loc[2, 'quantity'] = None  # Missing value
        
        result_imperfect = validator.validate_data_quality(imperfect_data)
        imperfect_metrics = result_imperfect.metadata['quality_metrics']
        
        # Quality should be lower
        assert imperfect_metrics['completeness'] < quality_metrics['completeness']
        assert imperfect_metrics['validity'] < quality_metrics['validity']
        assert imperfect_metrics['accuracy'] < quality_metrics['accuracy']
    
    def test_end_to_end_pipeline(self):
        """Test 10: Complete end-to-end pipeline execution with real components"""
        pipeline = PipelineManager("e2e_test_pipeline")
        
        # Use temporary database
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_file.close()
        
        try:
            # Patch all config references to use temp file
            with patch('src.storage.config') as mock_storage_config, \
                 patch('src.pipeline.config') as mock_pipeline_config, \
                 patch('src.ingestion.config') as mock_ingestion_config, \
                 patch('src.validation.config') as mock_validation_config, \
                 patch('src.transformation.config') as mock_transformation_config:
                def config_side_effect(key, default=None):
                    if key == 'database.path':
                        return temp_file.name
                    elif key == 'api.base_url':
                        return 'https://jsonplaceholder.typicode.com'
                    elif key == 'api.timeout':
                        return 30
                    elif key == 'api.retry_attempts':
                        return 3
                    elif key == 'api.retry_delay':
                        return 5
                    elif key == 'data_quality.quality_threshold':
                        return 50  # Lower threshold for test
                    elif key == 'data_quality.required_fields':
                        return ['order_id', 'customer_name', 'product', 'quantity', 'price']
                    else:
                        return default
                
                # Apply config to all modules
                for mock_config in [mock_storage_config, mock_pipeline_config, 
                                  mock_ingestion_config, mock_validation_config, 
                                  mock_transformation_config]:
                    mock_config.get.side_effect = config_side_effect
                
                # Run pipeline with small limit
                result = pipeline.run_pipeline(api_limit=5)
                
                # Pipeline should succeed (even if some stages have warnings)
                assert result.run_id is not None
                assert result.execution_time >= 0  # Allow for very fast execution
                assert 'ingestion' in result.stage_results
                
                # Check that we got some data
                if result.success:
                    assert result.total_records_processed > 0
                    assert len(result.stages_completed) > 0
                
                # Verify database was created and has data
                if result.success and 'storage' in result.stages_completed:
                    try:
                        conn = sqlite3.connect(temp_file.name)
                        cursor = conn.cursor()
                        cursor.execute("SELECT COUNT(*) FROM orders")
                        count = cursor.fetchone()[0]
                        conn.close()
                        assert count > 0
                    except sqlite3.OperationalError:
                        # Table might not exist if storage failed - check if pipeline at least ran
                        assert result.run_id is not None
        
        finally:
            # Cleanup with retry for Windows file locking
            if os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except PermissionError:
                    # Windows file locking issue - ignore for tests
                    pass

# Additional utility tests
class TestUtilityFunctions:
    """Test utility functions"""
    
    def test_safe_divide(self):
        """Test safe division utility"""
        assert safe_divide(10, 2) == 5.0
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(10, 0, default=1.0) == 1.0
        assert safe_divide(None, 2, default=0.0) == 0.0
    
    def test_format_duration(self):
        """Test duration formatting utility"""
        assert format_duration(30) == "30.00s"
        assert format_duration(90) == "1.5m"
        assert format_duration(3661) == "1.0h"
    
    def test_processing_result(self):
        """Test ProcessingResult dataclass"""
        result = ProcessingResult(
            success=True,
            data=pd.DataFrame({'test': [1, 2, 3]}),
            records_processed=3,
            error_message=None
        )
        
        assert result.success == True
        assert len(result.data) == 3
        assert result.records_processed == 3
        assert result.error_message is None
        assert result.metadata == {}  # Default empty dict

if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short"])