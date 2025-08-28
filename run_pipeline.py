#!/usr/bin/env python3
"""
Main execution script for Scalable Data Ingestion Pipeline
Provides CLI interface for running the complete data ingestion workflow
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.pipeline import PipelineManager
from src.utils import setup_logging, ensure_directory_exists, format_duration

def print_banner():
    """Print pipeline banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║           📥 SCALABLE DATA INGESTION PIPELINE 📥             ║
    ║                                                              ║
    ║              Fast • Reliable • Comprehensive                 ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def print_configuration():
    """Print current configuration"""
    print("🔧 PIPELINE CONFIGURATION")
    print("=" * 50)
    print("📊 Database: SQLite (data/orders.db)")
    print("🌐 API Source: JSONPlaceholder (demo data)")
    print("📁 File Sources: CSV, JSON files")
    print("📤 Output: Database + CSV/JSON exports")
    print("🔍 Validation: Quality scoring + schema validation")
    print("🧹 Transformation: Cleaning + enrichment + standardization")
    print("=" * 50)
    print()

def run_pipeline_with_options(args):
    """Run pipeline with command line options"""
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize pipeline manager
        pipeline_name = args.name or "scalable_data_ingestion"
        pipeline = PipelineManager(pipeline_name)
        
        # Configure pipeline based on arguments
        if args.no_validation:
            pipeline.enable_validation = False
            logger.info("🚫 Data validation disabled")
        
        if args.no_transformation:
            pipeline.enable_transformation = False
            logger.info("🚫 Data transformation disabled")
        
        if args.no_storage:
            pipeline.enable_storage = False
            logger.info("🚫 Data storage disabled")
        
        # Print pipeline configuration
        print("🔄 PIPELINE STAGES")
        print("=" * 50)
        print(f"📥 Data Ingestion: {'✅ Enabled' if pipeline.enable_ingestion else '❌ Disabled'}")
        print(f"🔍 Data Validation: {'✅ Enabled' if pipeline.enable_validation else '❌ Disabled'}")
        print(f"🧹 Data Transformation: {'✅ Enabled' if pipeline.enable_transformation else '❌ Disabled'}")
        print(f"💾 Data Storage: {'✅ Enabled' if pipeline.enable_storage else '❌ Disabled'}")
        print("=" * 50)
        print()
        
        # Run pipeline
        logger.info(f"🚀 Starting pipeline execution: {pipeline_name}")
        start_time = datetime.now()
        
        result = pipeline.run_pipeline(api_limit=args.limit)
        
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        # Print results
        print_results(result, total_time)
        
        # Generate and save report if requested
        if args.report:
            save_pipeline_report(pipeline, result, args.report)
        
        # Print pipeline status if requested
        if args.status:
            print_pipeline_status(pipeline)
        
        return 0 if result.success else 1
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline execution interrupted by user")
        print("\n⚠️ Pipeline execution was interrupted by user")
        return 130
    
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        print(f"\n❌ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

def print_results(result, total_time):
    """Print pipeline execution results"""
    print("\n" + "=" * 60)
    print("📊 PIPELINE EXECUTION RESULTS")
    print("=" * 60)
    
    # Status
    status_icon = "🎉" if result.success else "💥"
    status_text = "SUCCESS" if result.success else "FAILED"
    print(f"{status_icon} Status: {status_text}")
    print(f"🆔 Run ID: {result.run_id}")
    print(f"⏱️ Execution Time: {format_duration(result.execution_time)}")
    print(f"📈 Records Processed: {result.total_records_processed:,}")
    print(f"📉 Records Failed: {result.total_records_failed:,}")
    
    if result.total_records_processed > 0:
        success_rate = ((result.total_records_processed - result.total_records_failed) / result.total_records_processed) * 100
        print(f"✅ Success Rate: {success_rate:.1f}%")
    
    # Stages
    print(f"\n🔄 Stages Completed: {len(result.stages_completed)}")
    for stage in result.stages_completed:
        print(f"  ✅ {stage.replace('_', ' ').title()}")
    
    if result.stages_failed:
        print(f"\n❌ Stages Failed: {len(result.stages_failed)}")
        for stage in result.stages_failed:
            print(f"  ❌ {stage.replace('_', ' ').title()}")
    
    # Stage Details
    print(f"\n📋 Stage Details:")
    for stage_name, stage_result in result.stage_results.items():
        print(f"  📌 {stage_name.title()}:")
        
        if stage_name == 'ingestion':
            sources = stage_result.get('sources_summary', {})
            for source, details in sources.items():
                status = "✅" if details.get('success', False) else "❌"
                print(f"    {status} {source.upper()}: {details.get('records', 0):,} records")
        
        elif stage_name == 'validation':
            quality_validation = stage_result.get('quality_validation', {})
            if quality_validation:
                print(f"    🎯 Quality Score: {quality_validation.get('quality_score', 0):.1f}%")
                print(f"    ✅ Valid Records: {quality_validation.get('valid_records', 0):,}")
        
        elif stage_name == 'transformation':
            print(f"    📊 Final Records: {stage_result.get('final_records', 0):,}")
            print(f"    📈 Fields Added: {stage_result.get('fields_added', 0)}")
            print(f"    📉 Retention Rate: {stage_result.get('records_retained', 0):.1f}%")
        
        elif stage_name == 'storage':
            db_results = stage_result.get('database_results', {})
            file_results = stage_result.get('file_export_results', {})
            print(f"    🗄️ Database Records: {db_results.get('records_saved', 0):,}")
            print(f"    📁 Files Exported: {file_results.get('total_files', 0)}")
    
    # Error details
    if result.error_message:
        print(f"\n❌ Error Details:")
        print(f"   {result.error_message}")
    
    print("=" * 60)

def save_pipeline_report(pipeline, result, report_path):
    """Save detailed pipeline report"""
    try:
        report_content = pipeline.generate_pipeline_report(result)
        
        # Ensure report directory exists
        if report_path == 'auto':
            ensure_directory_exists("reports")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = f"reports/pipeline_report_{result.run_id}_{timestamp}.md"
        
        # Save report
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print(f"\n📄 Pipeline report saved: {report_path}")
        
    except Exception as e:
        print(f"\n⚠️ Failed to save pipeline report: {e}")

def print_pipeline_status(pipeline):
    """Print current pipeline status"""
    try:
        status = pipeline.get_pipeline_status()
        
        print(f"\n📊 PIPELINE STATUS")
        print("=" * 50)
        print(f"Pipeline Name: {status.get('pipeline_name', 'Unknown')}")
        print(f"Current Run ID: {status.get('current_run_id', 'None')}")
        print(f"Timestamp: {status.get('timestamp', 'Unknown')}")
        
        # Database stats
        db_stats = status.get('database_stats', {})
        if db_stats:
            print(f"\n🗄️ Database Statistics:")
            print(f"  Orders: {db_stats.get('orders_count', 0):,}")
            print(f"  Pipeline Runs: {db_stats.get('pipeline_runs_count', 0):,}")
            print(f"  Database Size: {db_stats.get('database_size_mb', 0):.2f} MB")
            print(f"  Avg Quality Score: {db_stats.get('avg_quality_score', 0):.1f}%")
        
        # Storage stats
        storage_stats = status.get('storage_summary', {})
        if storage_stats:
            print(f"\n📁 Storage Statistics:")
            print(f"  Total Files: {storage_stats.get('total_files', 0)}")
            print(f"  Total Size: {storage_stats.get('total_size_mb', 0):.2f} MB")
        
        print("=" * 50)
        
    except Exception as e:
        print(f"\n⚠️ Failed to get pipeline status: {e}")

def run_health_check():
    """Run pipeline health check"""
    try:
        print("🔍 Running pipeline health check...")
        pipeline = PipelineManager("health_check")
        health = pipeline.health_check()
        
        print(f"\n🏥 HEALTH CHECK RESULTS")
        print("=" * 50)
        print(f"Overall Status: {health['overall_status'].upper()}")
        print(f"Timestamp: {health['timestamp']}")
        
        print(f"\n📋 Component Status:")
        for component, details in health['components'].items():
            status_icon = "✅" if details['status'] == 'healthy' else "❌"
            print(f"  {status_icon} {component.replace('_', ' ').title()}: {details['status']}")
            
            if details.get('error'):
                print(f"    Error: {details['error']}")
        
        if health['issues']:
            print(f"\n⚠️ Issues Found:")
            for issue in health['issues']:
                print(f"  - {issue}")
        else:
            print(f"\n✅ No issues found")
        
        print("=" * 50)
        
        return 0 if health['overall_status'] in ['healthy', 'degraded'] else 1
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return 1

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Execute the Scalable Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                    # Run full pipeline
  python run_pipeline.py --limit 50         # Limit API records to 50
  python run_pipeline.py --no-validation    # Skip validation stage
  python run_pipeline.py --report auto      # Generate automatic report
  python run_pipeline.py --status           # Show pipeline status
  python run_pipeline.py --health           # Run health check only
        """
    )
    
    # Pipeline options
    parser.add_argument('--name', type=str, help='Pipeline name')
    parser.add_argument('--limit', type=int, default=100, help='API data limit (default: 100)')
    parser.add_argument('--no-validation', action='store_true', help='Disable data validation')
    parser.add_argument('--no-transformation', action='store_true', help='Disable data transformation')
    parser.add_argument('--no-storage', action='store_true', help='Disable data storage')
    
    # Output options
    parser.add_argument('--report', type=str, nargs='?', const='auto', help='Generate detailed report')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    parser.add_argument('--health', action='store_true', help='Run health check only')
    parser.add_argument('--log-level', type=str, default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Print banner
    print_banner()
    
    # Handle health check only
    if args.health:
        return run_health_check()
    
    # Print configuration
    print_configuration()
    
    logger.info(f"Pipeline execution started")
    
    try:
        # Run pipeline
        exit_code = run_pipeline_with_options(args)
        
        logger.info(f"Pipeline execution completed with exit code: {exit_code}")
        
        return exit_code
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)