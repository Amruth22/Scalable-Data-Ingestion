"""
Main pipeline orchestration module for scalable data ingestion
Coordinates all pipeline stages: ingestion, validation, transformation, and storage
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import uuid

from .utils import (
    config, ProcessingResult, timing_decorator, 
    format_duration, safe_divide
)
from .ingestion import DataIngestion
from .validation import ValidationOrchestrator
from .transformation import TransformationOrchestrator
from .storage import StorageOrchestrator

logger = logging.getLogger(__name__)

@dataclass
class PipelineResult:
    """Pipeline execution result container"""
    success: bool
    run_id: str
    pipeline_name: str
    start_time: str
    end_time: str
    execution_time: float
    total_records_processed: int
    total_records_failed: int
    stages_completed: List[str]
    stages_failed: List[str]
    stage_results: Dict[str, Any]
    error_message: Optional[str] = None

class PipelineManager:
    """Main pipeline manager for orchestrating data ingestion workflow"""
    
    def __init__(self, pipeline_name: str = "scalable_data_ingestion"):
        """
        Initialize pipeline manager
        
        Args:
            pipeline_name (str): Name of the pipeline
        """
        self.pipeline_name = pipeline_name
        self.run_id = None
        
        # Initialize pipeline components
        self.data_ingestion = DataIngestion()
        self.validation_orchestrator = ValidationOrchestrator()
        self.transformation_orchestrator = TransformationOrchestrator()
        self.storage_orchestrator = StorageOrchestrator()
        
        # Pipeline configuration
        self.enable_ingestion = True
        self.enable_validation = True
        self.enable_transformation = True
        self.enable_storage = True
        
        logger.info(f"Pipeline manager initialized: {pipeline_name}")
    
    @timing_decorator
    def run_pipeline(self, api_limit: int = 100) -> PipelineResult:
        """
        Execute the complete data ingestion pipeline
        
        Args:
            api_limit (int): Limit for API data collection
            
        Returns:
            PipelineResult: Complete pipeline execution result
        """
        # Generate unique run ID
        self.run_id = f"RUN-{datetime.now().strftime('%Y%m%d_%H%M%S')}-{str(uuid.uuid4())[:8]}"
        start_time = datetime.now()
        
        logger.info(f"🚀 Starting pipeline execution: {self.run_id}")
        
        # Initialize result tracking
        result = PipelineResult(
            success=False,
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            start_time=start_time.isoformat(),
            end_time="",
            execution_time=0.0,
            total_records_processed=0,
            total_records_failed=0,
            stages_completed=[],
            stages_failed=[],
            stage_results={}
        )
        
        try:
            current_data = None
            
            # Stage 1: Data Ingestion
            if self.enable_ingestion:
                logger.info("📥 Executing data ingestion stage...")
                ingestion_result = self.data_ingestion.collect_all_data(api_limit)
                result.stage_results['ingestion'] = ingestion_result.metadata
                
                if not ingestion_result.success:
                    result.stages_failed.append('ingestion')
                    raise Exception(f"Ingestion stage failed: {ingestion_result.error_message}")
                
                result.stages_completed.append('ingestion')
                current_data = ingestion_result.data
                
                if current_data is None or current_data.empty:
                    raise Exception("No data collected from ingestion stage")
                
                logger.info(f"✅ Ingestion completed: {len(current_data)} records collected")
            
            # Stage 2: Data Validation
            if self.enable_validation and current_data is not None:
                logger.info("🔍 Executing data validation stage...")
                validation_result = self.validation_orchestrator.validate_all(current_data)
                result.stage_results['validation'] = validation_result.metadata
                
                if not validation_result.success:
                    # Log warning but continue pipeline (validation issues are not fatal)
                    logger.warning(f"Validation stage completed with issues: {validation_result.error_message}")
                    result.stages_failed.append('validation')
                else:
                    result.stages_completed.append('validation')
                    logger.info(f"✅ Validation completed: {validation_result.metadata.get('quality_validation', {}).get('quality_score', 0):.1f}% quality score")
            
            # Stage 3: Data Transformation
            if self.enable_transformation and current_data is not None:
                logger.info("🧹 Executing data transformation stage...")
                transformation_result = self.transformation_orchestrator.transform_all(current_data)
                result.stage_results['transformation'] = transformation_result.metadata
                
                if not transformation_result.success:
                    result.stages_failed.append('transformation')
                    raise Exception(f"Transformation stage failed: {transformation_result.error_message}")
                
                result.stages_completed.append('transformation')
                current_data = transformation_result.data
                
                logger.info(f"✅ Transformation completed: {len(current_data)} records processed")
            
            # Stage 4: Data Storage
            if self.enable_storage and current_data is not None:
                logger.info("💾 Executing data storage stage...")
                storage_result = self.storage_orchestrator.store_all(current_data, f"pipeline_{self.run_id}")
                result.stage_results['storage'] = storage_result.metadata
                
                if not storage_result.success:
                    result.stages_failed.append('storage')
                    raise Exception(f"Storage stage failed: {storage_result.error_message}")
                
                result.stages_completed.append('storage')
                
                logger.info(f"✅ Storage completed: data saved to database and files")
            
            # Calculate final metrics
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            result.success = True
            result.end_time = end_time.isoformat()
            result.execution_time = execution_time
            result.total_records_processed = len(current_data) if current_data is not None else 0
            result.total_records_failed = 0  # Could be calculated from stage results
            
            # Save pipeline run information
            self._save_pipeline_run_info(result)
            
            logger.info(f"🎉 Pipeline execution completed successfully: {self.run_id}")
            logger.info(f"📊 Summary: {result.total_records_processed} records processed in {format_duration(execution_time)}")
            
            return result
            
        except Exception as e:
            # Handle pipeline failure
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            result.success = False
            result.end_time = end_time.isoformat()
            result.execution_time = execution_time
            result.error_message = str(e)
            
            # Save failed pipeline run
            self._save_pipeline_run_info(result)
            
            logger.error(f"❌ Pipeline execution failed: {self.run_id} - {str(e)}")
            
            return result
        
        finally:
            # Cleanup resources
            self.data_ingestion.close()
    
    def _save_pipeline_run_info(self, result: PipelineResult):
        """Save pipeline run information to database"""
        try:
            run_data = {
                'run_id': result.run_id,
                'pipeline_name': result.pipeline_name,
                'start_time': result.start_time,
                'end_time': result.end_time,
                'status': 'completed' if result.success else 'failed',
                'records_processed': result.total_records_processed,
                'records_failed': result.total_records_failed,
                'error_message': result.error_message
            }
            
            # Save to database
            db_result = self.storage_orchestrator.database_manager.save_pipeline_run(run_data)
            
            # Save quality metrics if available
            if result.success and 'validation' in result.stage_results:
                validation_metadata = result.stage_results['validation']
                quality_validation = validation_metadata.get('quality_validation', {})
                
                if quality_validation:
                    metrics_data = [
                        {
                            'run_id': result.run_id,
                            'metric_name': 'data_quality_score',
                            'metric_value': quality_validation.get('quality_score', 0),
                            'metric_type': 'percentage',
                            'source_table': 'orders',
                            'measured_at': result.end_time
                        },
                        {
                            'run_id': result.run_id,
                            'metric_name': 'records_processed',
                            'metric_value': result.total_records_processed,
                            'metric_type': 'count',
                            'source_table': 'orders',
                            'measured_at': result.end_time
                        },
                        {
                            'run_id': result.run_id,
                            'metric_name': 'execution_time_seconds',
                            'metric_value': result.execution_time,
                            'metric_type': 'duration',
                            'source_table': 'pipeline_runs',
                            'measured_at': result.end_time
                        }
                    ]
                    
                    self.storage_orchestrator.database_manager.save_quality_metrics(metrics_data)
            
        except Exception as e:
            logger.warning(f"Failed to save pipeline run information: {e}")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and recent runs"""
        try:
            # Get database stats
            stats_result = self.storage_orchestrator.database_manager.get_database_stats()
            
            # Get storage summary
            storage_summary = self.storage_orchestrator.file_manager.get_storage_summary()
            
            status = {
                'pipeline_name': self.pipeline_name,
                'current_run_id': self.run_id,
                'timestamp': datetime.now().isoformat(),
                'database_stats': stats_result.metadata if stats_result.success else {},
                'storage_summary': storage_summary,
                'configuration': {
                    'ingestion_enabled': self.enable_ingestion,
                    'validation_enabled': self.enable_validation,
                    'transformation_enabled': self.enable_transformation,
                    'storage_enabled': self.enable_storage
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting pipeline status: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def generate_pipeline_report(self, result: PipelineResult) -> str:
        """Generate comprehensive pipeline execution report"""
        report = []
        report.append("# Scalable Data Ingestion Pipeline Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Executive Summary
        report.append("## Executive Summary")
        report.append(f"- **Pipeline**: {result.pipeline_name}")
        report.append(f"- **Run ID**: {result.run_id}")
        report.append(f"- **Status**: {'✅ SUCCESS' if result.success else '❌ FAILED'}")
        report.append(f"- **Execution Time**: {format_duration(result.execution_time)}")
        report.append(f"- **Records Processed**: {result.total_records_processed:,}")
        report.append(f"- **Records Failed**: {result.total_records_failed:,}")
        
        if result.total_records_processed > 0:
            success_rate = safe_divide(result.total_records_processed - result.total_records_failed, result.total_records_processed, 0) * 100
            report.append(f"- **Success Rate**: {success_rate:.1f}%")
        
        report.append("")
        
        # Stage Results
        report.append("## Stage Results")
        
        for stage_name, stage_result in result.stage_results.items():
            status_icon = "✅" if stage_name in result.stages_completed else "❌"
            report.append(f"### {status_icon} {stage_name.title()} Stage")
            
            if stage_name == 'ingestion':
                sources_summary = stage_result.get('sources_summary', {})
                report.append(f"- **Total Records**: {stage_result.get('original_records', 0):,}")
                for source, details in sources_summary.items():
                    status = "✅" if details.get('success', False) else "❌"
                    report.append(f"- **{source.upper()}**: {status} ({details.get('records', 0):,} records)")
            
            elif stage_name == 'validation':
                quality_validation = stage_result.get('quality_validation', {})
                if quality_validation:
                    report.append(f"- **Quality Score**: {quality_validation.get('quality_score', 0):.1f}%")
                    report.append(f"- **Valid Records**: {quality_validation.get('valid_records', 0):,}")
                    report.append(f"- **Invalid Records**: {quality_validation.get('invalid_records', 0):,}")
            
            elif stage_name == 'transformation':
                report.append(f"- **Final Records**: {stage_result.get('final_records', 0):,}")
                report.append(f"- **Final Fields**: {stage_result.get('final_fields', 0)}")
                report.append(f"- **Fields Added**: {stage_result.get('fields_added', 0)}")
                report.append(f"- **Retention Rate**: {stage_result.get('records_retained', 0):.1f}%")
            
            elif stage_name == 'storage':
                db_results = stage_result.get('database_results', {})
                file_results = stage_result.get('file_export_results', {})
                report.append(f"- **Database Records Saved**: {db_results.get('records_saved', 0):,}")
                report.append(f"- **Files Exported**: {file_results.get('total_files', 0)}")
                report.append(f"- **Operations Successful**: {stage_result.get('successful_operations', 0)}/{stage_result.get('total_operations', 0)}")
            
            report.append("")
        
        # Stages Summary
        report.append("## Pipeline Stages Summary")
        report.append(f"- **Completed Stages**: {', '.join(result.stages_completed) if result.stages_completed else 'None'}")
        report.append(f"- **Failed Stages**: {', '.join(result.stages_failed) if result.stages_failed else 'None'}")
        report.append("")
        
        # Performance Metrics
        report.append("## Performance Metrics")
        if result.total_records_processed > 0 and result.execution_time > 0:
            throughput = result.total_records_processed / result.execution_time
            report.append(f"- **Throughput**: {throughput:.1f} records/second")
        
        report.append(f"- **Total Execution Time**: {format_duration(result.execution_time)}")
        report.append("")
        
        # Error Details
        if result.error_message:
            report.append("## Error Details")
            report.append("```")
            report.append(result.error_message)
            report.append("```")
            report.append("")
        
        # Recommendations
        report.append("## Recommendations")
        if result.success:
            report.append("- ✅ Pipeline executed successfully")
            report.append("- Consider scheduling regular pipeline runs")
            report.append("- Monitor data quality trends over time")
            report.append("- Review performance metrics for optimization opportunities")
        else:
            report.append("- ❌ Pipeline execution failed")
            report.append("- Review error details and fix issues")
            report.append("- Check data source availability")
            report.append("- Verify system resources and permissions")
            report.append("- Consider implementing retry mechanisms")
        
        report.append("")
        report.append("---")
        report.append("*Report generated by Scalable Data Ingestion Pipeline*")
        
        return "\n".join(report)
    
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check of pipeline components"""
        health_status = {
            'overall_status': 'unknown',
            'timestamp': datetime.now().isoformat(),
            'components': {},
            'issues': []
        }
        
        try:
            # Check API ingestion
            api_health = self.data_ingestion.api_ingestion.test_api_connection()
            health_status['components']['api_ingestion'] = {
                'status': 'healthy' if api_health['success'] else 'unhealthy',
                'response_time': api_health.get('response_time', 0),
                'error': api_health.get('error_message')
            }
            
            if not api_health['success']:
                health_status['issues'].append(f"API ingestion: {api_health.get('error_message', 'Unknown error')}")
            
            # Check database
            db_stats = self.storage_orchestrator.database_manager.get_database_stats()
            health_status['components']['database'] = {
                'status': 'healthy' if db_stats.success else 'unhealthy',
                'error': db_stats.error_message if not db_stats.success else None
            }
            
            if not db_stats.success:
                health_status['issues'].append(f"Database: {db_stats.error_message}")
            
            # Check file system
            storage_summary = self.storage_orchestrator.file_manager.get_storage_summary()
            health_status['components']['file_system'] = {
                'status': 'healthy' if 'error' not in storage_summary else 'unhealthy',
                'total_files': storage_summary.get('total_files', 0),
                'total_size_mb': storage_summary.get('total_size_mb', 0),
                'error': storage_summary.get('error')
            }
            
            if 'error' in storage_summary:
                health_status['issues'].append(f"File system: {storage_summary['error']}")
            
            # Determine overall status
            healthy_components = sum(1 for comp in health_status['components'].values() if comp['status'] == 'healthy')
            total_components = len(health_status['components'])
            
            if healthy_components == total_components:
                health_status['overall_status'] = 'healthy'
            elif healthy_components >= total_components * 0.5:
                health_status['overall_status'] = 'degraded'
            else:
                health_status['overall_status'] = 'unhealthy'
            
            logger.info(f"Health check completed: {health_status['overall_status']} ({healthy_components}/{total_components} components healthy)")
            
        except Exception as e:
            health_status['overall_status'] = 'error'
            health_status['issues'].append(f"Health check error: {str(e)}")
            logger.error(f"Health check error: {e}")
        
        return health_status

if __name__ == "__main__":
    # Test pipeline execution
    print("Testing Scalable Data Ingestion Pipeline...")
    
    # Initialize pipeline
    pipeline = PipelineManager("test_pipeline")
    
    try:
        # Run health check
        print("🔍 Running health check...")
        health = pipeline.health_check()
        print(f"Health status: {health['overall_status']}")
        
        if health['issues']:
            print("Issues found:")
            for issue in health['issues']:
                print(f"  - {issue}")
        
        # Run pipeline
        print("\n🚀 Running pipeline...")
        result = pipeline.run_pipeline(api_limit=10)
        
        # Print results
        print(f"\nPipeline Result:")
        print(f"Success: {result.success}")
        print(f"Run ID: {result.run_id}")
        print(f"Execution Time: {format_duration(result.execution_time)}")
        print(f"Records Processed: {result.total_records_processed}")
        print(f"Stages Completed: {', '.join(result.stages_completed)}")
        
        if result.stages_failed:
            print(f"Stages Failed: {', '.join(result.stages_failed)}")
        
        if result.error_message:
            print(f"Error: {result.error_message}")
        
        # Generate report
        print(f"\n📄 Generating pipeline report...")
        report = pipeline.generate_pipeline_report(result)
        print(f"Report generated ({len(report)} characters)")
        
        # Get pipeline status
        print(f"\n📊 Getting pipeline status...")
        status = pipeline.get_pipeline_status()
        print(f"Pipeline status retrieved: {'error' not in status}")
        
        print("\n✅ Pipeline test completed!")
        
    except Exception as e:
        print(f"\n❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()