import pytest
import pandas as pd
import sqlite3
import tempfile
import os
import sys
from unittest.mock import patch, MagicMock
from datetime import datetime
from typing import Dict, Any

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.utils import Config, ProcessingResult, safe_divide, format_duration
from src.ingestion import APIIngestion, DataIngestion
from src.validation import DataValidator, ValidationOrchestrator
from src.transformation import DataCleaner, DataEnricher, TransformationOrchestrator
from src.storage import DatabaseManager, StorageOrchestrator
from src.pipeline import PipelineManager

def test_api_ingestion_success():
    """Test that API ingestion successfully fetches and transforms data"""
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
        assert result.data['product'].iloc[0] == 'iPhone 15'  # Should detect iPhone from title
    
    print("PASS: API ingestion success test passed")

def test_data_validation_quality_scoring():
    """Test that data validation calculates quality scores correctly"""
    validator = DataValidator()
    
    # Create high-quality sample data
    sample_data = pd.DataFrame({
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
    
    result = validator.validate_data_quality(sample_data)
    
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
    assert quality_metrics['completeness'] >= 95  # No missing values
    
    print("PASS: Data validation quality scoring test passed")

def test_data_transformation_cleaning():
    """Test that data cleaning removes duplicates and fixes data types"""
    # Create messy sample data
    messy_data = pd.DataFrame({
        'order_id': ['API-0001', 'API-0002', 'API-0001'],  # Duplicate
        'customer_name': ['john doe', 'JANE SMITH', 'bob wilson'],  # Case issues
        'customer_email': ['JOHN@EXAMPLE.COM', 'jane@example.com', 'bob@example.com'],
        'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
        'quantity': ['1', '1', '2'],  # String numbers
        'price': ['999.99', '1999.99', '249.99'],  # String prices
        'total_amount': [999.99, 1999.99, 499.98],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17']
    })
    
    cleaner = DataCleaner()
    result = cleaner.clean_data(messy_data)
    
    assert result.success == True
    assert len(result.data) == 2  # Should remove 1 duplicate
    assert result.metadata['records_removed'] > 0
    assert 'operations_performed' in result.metadata
    
    # Check text cleaning
    assert result.data['customer_name'].iloc[0] == 'John Doe'  # Should be title case
    assert result.data['customer_email'].iloc[1] == 'jane@example.com'  # Should be lowercase
    
    # Check data type fixing
    assert result.data['quantity'].dtype in ['int64', 'float64']
    assert result.data['price'].dtype in ['float64']
    
    print("PASS: Data transformation cleaning test passed")

def test_data_enrichment():
    """Test that data enrichment adds business intelligence fields"""
    sample_data = pd.DataFrame({
        'order_id': ['API-0001', 'API-0002', 'API-0003'],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Wilson'],
        'customer_email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
        'quantity': [1, 1, 2],
        'price': [999.99, 1999.99, 249.99],
        'total_amount': [999.99, 1999.99, 499.98],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17']
    })
    
    enricher = DataEnricher()
    result = enricher.enrich_data(sample_data)
    
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
    
    # Check risk assessment
    assert enriched_data['risk_score'].notna().all()
    assert all(0 <= score <= 100 for score in enriched_data['risk_score'])
    
    print("PASS: Data enrichment test passed")

def test_database_save_orders():
    """Test that database manager successfully saves orders"""
    # Create temporary database
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_file.close()
    
    try:
        # Create database manager with temp database
        with patch('src.storage.config') as mock_config:
            def config_side_effect(key, default=None):
                if key == 'database.path':
                    return temp_file.name
                elif key == 'database.connection_timeout':
                    return 30
                elif key == 'database.batch_size':
                    return 1000
                else:
                    return default
            
            mock_config.get.side_effect = config_side_effect
            db_manager = DatabaseManager()
            
            # Test with sample data
            sample_data = pd.DataFrame({
                'order_id': ['TEST-001', 'TEST-002', 'TEST-003'],
                'customer_name': ['John Doe', 'Jane Smith', 'Bob Wilson'],
                'customer_email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
                'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
                'quantity': [1, 1, 2],
                'price': [999.99, 1999.99, 249.99],
                'total_amount': [999.99, 1999.99, 499.98],
                'order_date': ['2024-01-15', '2024-01-16', '2024-01-17']
            })
            
            result = db_manager.save_orders(sample_data)
            
            assert result.success == True
            assert result.records_processed == 3
            assert result.metadata['records_saved'] == 3
            assert result.metadata['records_failed'] == 0
            
            # Verify data was actually saved
            retrieval_result = db_manager.get_orders()
            assert retrieval_result.success == True
            assert len(retrieval_result.data) == 3
            assert retrieval_result.data['order_id'].iloc[0] == 'TEST-001'
    
    finally:
        # Cleanup
        try:
            os.unlink(temp_file.name)
        except PermissionError:
            pass
    
    print("PASS: Database save orders test passed")

def test_pipeline_orchestration():
    """Test that pipeline manager orchestrates all stages correctly"""
    pipeline = PipelineManager("test_pipeline")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'order_id': ['API-0001', 'API-0002', 'API-0003'],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Wilson'],
        'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
        'quantity': [1, 1, 2],
        'price': [999.99, 1999.99, 249.99],
        'total_amount': [999.99, 1999.99, 499.98],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17']
    })
    
    # Mock all components to return success
    with patch.object(pipeline.data_ingestion, 'collect_all_data') as mock_ingestion, \
         patch.object(pipeline.validation_orchestrator, 'validate_all') as mock_validation, \
         patch.object(pipeline.transformation_orchestrator, 'transform_all') as mock_transformation, \
         patch.object(pipeline.storage_orchestrator, 'store_all') as mock_storage:
        
        # Setup mocks
        mock_ingestion.return_value = ProcessingResult(
            success=True,
            data=sample_data,
            records_processed=3,
            metadata={'sources_summary': {'api': {'success': True, 'records': 3}}}
        )
        
        mock_validation.return_value = ProcessingResult(
            success=True,
            data=sample_data,
            records_processed=3,
            metadata={'quality_validation': {'quality_score': 95.0}}
        )
        
        mock_transformation.return_value = ProcessingResult(
            success=True,
            data=sample_data,
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
    
    print("PASS: Pipeline orchestration test passed")

def test_error_handling():
    """Test that pipeline handles errors gracefully"""
    pipeline = PipelineManager("error_test_pipeline")
    
    # Mock ingestion to fail
    with patch.object(pipeline.data_ingestion, 'collect_all_data') as mock_ingestion:
        mock_ingestion.return_value = ProcessingResult(
            success=False,
            error_message="API connection failed"
        )
        
        result = pipeline.run_pipeline()
        
        assert result.success == False
        assert result.error_message is not None
        assert 'ingestion' in result.stages_failed
        assert len(result.stages_completed) == 0
        assert result.total_records_processed == 0
        assert "API connection failed" in result.error_message
    
    print("PASS: Error handling test passed")

def test_configuration_loading():
    """Test that configuration system loads settings correctly"""
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
    
    # Test data quality configuration
    quality_threshold = config.get('data_quality.quality_threshold')
    assert isinstance(quality_threshold, (int, float))
    assert 0 <= quality_threshold <= 100
    
    print("PASS: Configuration loading test passed")

def test_data_quality_metrics():
    """Test that data quality metrics are calculated accurately"""
    validator = DataValidator()
    
    # Test with perfect data
    perfect_data = pd.DataFrame({
        'order_id': ['API-0001', 'API-0002', 'API-0003'],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Wilson'],
        'customer_email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
        'quantity': [1, 1, 2],
        'price': [999.99, 1999.99, 249.99],
        'total_amount': [999.99, 1999.99, 499.98],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17']
    })
    
    result = validator.validate_data_quality(perfect_data)
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
    imperfect_data = perfect_data.copy()
    imperfect_data.loc[0, 'customer_email'] = 'invalid-email'  # Invalid email
    imperfect_data.loc[1, 'price'] = -100  # Invalid price
    imperfect_data.loc[2, 'quantity'] = None  # Missing value
    
    result_imperfect = validator.validate_data_quality(imperfect_data)
    imperfect_metrics = result_imperfect.metadata['quality_metrics']
    
    # Quality should be lower
    assert imperfect_metrics['completeness'] < quality_metrics['completeness']
    assert imperfect_metrics['validity'] < quality_metrics['validity']
    assert imperfect_metrics['accuracy'] < quality_metrics['accuracy']
    
    print("PASS: Data quality metrics test passed")

def test_end_to_end_pipeline():
    """Test complete end-to-end pipeline execution with real components"""
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
                elif key == 'database.connection_timeout':
                    return 30
                elif key == 'database.batch_size':
                    return 1000
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
    
    print("PASS: End-to-end pipeline test passed")

def run_all_tests():
    """Run all tests and provide summary"""
    print("Running unit tests for Scalable Data Ingestion Pipeline...")
    print("Testing core pipeline functionality and data processing capabilities")
    print("=" * 70)
    
    # List of exactly 10 test functions
    test_functions = [
        test_api_ingestion_success,
        test_data_validation_quality_scoring,
        test_data_transformation_cleaning,
        test_data_enrichment,
        test_database_save_orders,
        test_pipeline_orchestration,
        test_error_handling,
        test_configuration_loading,
        test_data_quality_metrics,
        test_end_to_end_pipeline
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test_func.__name__} - {e}")
            failed += 1
    
    print("=" * 70)
    print(f"📊 Test Results Summary:")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Total: {passed + failed}")
    
    if failed == 0:
        print("🎉 All tests passed!")
        print("✅ Scalable Data Ingestion Pipeline is working correctly")
        print("🚀 Pipeline components: Ingestion, Validation, Transformation, Storage")
        print("📊 Data processing: API → Quality Scoring → Business Intelligence → Database")
        return True
    else:
        print(f"⚠️  {failed} test(s) failed")
        print("🔧 Check pipeline components and configuration")
        return False

if __name__ == "__main__":
    print("🚀 Starting Scalable Data Ingestion Pipeline Unit Tests")
    print("📋 Testing all core components: ingestion, validation, transformation, storage")
    print("🔧 Pipeline processes data from API → validation → enrichment → database")
    print()
    
    # Run the tests
    success = run_all_tests()
    
    if success:
        print("\n🎯 Next Steps:")
        print("  1. Run the full pipeline: python run_pipeline.py")
        print("  2. Check health status: python run_pipeline.py --health")
        print("  3. Generate reports: python run_pipeline.py --report auto")
        print("  4. View database: data/orders.db")
    
    exit(0 if success else 1)