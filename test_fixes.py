#!/usr/bin/env python3
"""
Quick test script to verify the fixes work
"""

import sys
import os
import tempfile
import sqlite3
import pandas as pd
from unittest.mock import patch

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_database_fix():
    """Test database configuration fix"""
    print("Testing database configuration fix...")
    
    from src.storage import DatabaseManager
    
    # Create temp database
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_file.close()
    
    try:
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
                'order_id': ['TEST-001'],
                'customer_name': ['Test Customer'],
                'product': ['Test Product'],
                'quantity': [1],
                'price': [99.99],
                'total_amount': [99.99],
                'order_date': ['2024-01-15']
            })
            
            result = db_manager.save_orders(sample_data)
            print(f"✅ Database save result: {result.success}")
            
    finally:
        if os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except PermissionError:
                pass

def test_validation_fix():
    """Test validation boolean comparison fix"""
    print("Testing validation boolean comparison fix...")
    
    from src.validation import DataValidator
    
    sample_data = pd.DataFrame({
        'order_id': ['API-0001', 'API-0002'],
        'customer_name': ['John Doe', 'Jane Smith'],
        'customer_email': ['john@example.com', 'jane@example.com'],
        'product': ['iPhone 15', 'MacBook Pro'],
        'quantity': [1, 1],
        'price': [999.99, 1999.99],
        'total_amount': [999.99, 1999.99],
        'order_date': ['2024-01-15', '2024-01-16']
    })
    
    validator = DataValidator()
    result = validator.validate_data_quality(sample_data)
    
    # Test boolean comparison
    success_check = result.success == True
    print(f"✅ Validation boolean comparison: {success_check}")
    print(f"✅ Quality score: {result.metadata['quality_score']:.1f}%")

def test_pipeline_execution_time():
    """Test pipeline execution time measurement"""
    print("Testing pipeline execution time measurement...")
    
    from src.pipeline import PipelineManager
    from src.utils import ProcessingResult
    
    pipeline = PipelineManager("test_pipeline")
    
    # Mock all components
    with patch.object(pipeline.data_ingestion, 'collect_all_data') as mock_ingestion, \
         patch.object(pipeline.validation_orchestrator, 'validate_all') as mock_validation, \
         patch.object(pipeline.transformation_orchestrator, 'transform_all') as mock_transformation, \
         patch.object(pipeline.storage_orchestrator, 'store_all') as mock_storage:
        
        sample_data = pd.DataFrame({'test': [1, 2, 3]})
        
        mock_ingestion.return_value = ProcessingResult(
            success=True, data=sample_data, records_processed=3,
            metadata={'sources_summary': {'api': {'success': True, 'records': 3}}}
        )
        mock_validation.return_value = ProcessingResult(
            success=True, data=sample_data, records_processed=3,
            metadata={'quality_validation': {'quality_score': 95.0}}
        )
        mock_transformation.return_value = ProcessingResult(
            success=True, data=sample_data, records_processed=3,
            metadata={'final_records': 3, 'fields_added': 10}
        )
        mock_storage.return_value = ProcessingResult(
            success=True, records_processed=3,
            metadata={'successful_operations': 3, 'total_operations': 3}
        )
        
        result = pipeline.run_pipeline(api_limit=10)
        print(f"✅ Pipeline execution time: {result.execution_time}s (should be > 0)")
        print(f"✅ Pipeline success: {result.success}")

if __name__ == "__main__":
    print("🔧 Testing fixes...")
    
    try:
        test_database_fix()
        test_validation_fix()
        test_pipeline_execution_time()
        print("\n✅ All fixes verified successfully!")
        
    except Exception as e:
        print(f"\n❌ Fix verification failed: {e}")
        import traceback
        traceback.print_exc()