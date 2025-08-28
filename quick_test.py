#!/usr/bin/env python3
"""
Quick test of the complete pipeline
"""

import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_pipeline():
    """Test the complete pipeline"""
    print("🚀 Testing Scalable Data Ingestion Pipeline...")
    
    try:
        from src.pipeline import PipelineManager
        
        # Initialize pipeline
        pipeline = PipelineManager("quick_test_pipeline")
        
        # Run health check first
        print("\n🔍 Running health check...")
        health = pipeline.health_check()
        print(f"Health status: {health['overall_status']}")
        
        if health['issues']:
            print("Issues found:")
            for issue in health['issues']:
                print(f"  - {issue}")
        
        # Run pipeline with small limit
        print("\n🔄 Running pipeline...")
        result = pipeline.run_pipeline(api_limit=5)
        
        # Print results
        print(f"\n📊 RESULTS:")
        print(f"✅ Success: {result.success}")
        print(f"🆔 Run ID: {result.run_id}")
        print(f"⏱️ Execution Time: {result.execution_time:.3f}s")
        print(f"📈 Records Processed: {result.total_records_processed}")
        print(f"🔄 Stages Completed: {', '.join(result.stages_completed)}")
        
        if result.stages_failed:
            print(f"❌ Stages Failed: {', '.join(result.stages_failed)}")
        
        if result.error_message:
            print(f"⚠️ Error: {result.error_message}")
        
        # Show stage details
        print(f"\n📋 STAGE DETAILS:")
        for stage_name, stage_result in result.stage_results.items():
            print(f"  📌 {stage_name.title()}:")
            
            if stage_name == 'ingestion':
                sources = stage_result.get('sources_summary', {})
                for source, details in sources.items():
                    status = "✅" if details.get('success', False) else "❌"
                    print(f"    {status} {source.upper()}: {details.get('records', 0)} records")
            
            elif stage_name == 'validation':
                quality_validation = stage_result.get('quality_validation', {})
                if quality_validation:
                    print(f"    🎯 Quality Score: {quality_validation.get('quality_score', 0):.1f}%")
            
            elif stage_name == 'transformation':
                print(f"    📊 Final Records: {stage_result.get('final_records', 0)}")
                print(f"    📈 Fields Added: {stage_result.get('fields_added', 0)}")
            
            elif stage_name == 'storage':
                db_results = stage_result.get('database_results', {})
                print(f"    🗄️ Database Records: {db_results.get('records_saved', 0)}")
        
        return result.success
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_pipeline()
    
    if success:
        print("\n🎉 Pipeline test completed successfully!")
        print("\n📋 Next steps:")
        print("  1. Run full tests: python -m pytest tests.py -v")
        print("  2. Run full pipeline: python run_pipeline.py")
        print("  3. Check database: data/orders.db")
    else:
        print("\n💥 Pipeline test failed!")
    
    sys.exit(0 if success else 1)