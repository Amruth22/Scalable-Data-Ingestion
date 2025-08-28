"""
Data validation module for quality assessment and schema validation
Provides comprehensive data quality scoring and validation rules
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import re

from .utils import (
    config, ProcessingResult, timing_decorator, 
    validate_required_fields, safe_divide
)

logger = logging.getLogger(__name__)

class DataValidator:
    """Comprehensive data validation and quality assessment"""
    
    def __init__(self):
        self.quality_threshold = config.get('data_quality.quality_threshold', 80)
        self.required_fields = config.get('data_quality.required_fields', [])
        logger.info(f"Data validator initialized with {self.quality_threshold}% quality threshold")
    
    @timing_decorator
    def validate_data_quality(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Comprehensive data quality validation
        
        Args:
            data (pd.DataFrame): Data to validate
            
        Returns:
            ProcessingResult: Validation results with quality score
        """
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for validation"
                )
            
            quality_metrics = {}
            
            # 1. Completeness Score (30% weight)
            completeness_score = self._calculate_completeness_score(data)
            quality_metrics['completeness'] = completeness_score
            
            # 2. Validity Score (30% weight)
            validity_score = self._calculate_validity_score(data)
            quality_metrics['validity'] = validity_score
            
            # 3. Consistency Score (20% weight)
            consistency_score = self._calculate_consistency_score(data)
            quality_metrics['consistency'] = consistency_score
            
            # 4. Accuracy Score (20% weight)
            accuracy_score = self._calculate_accuracy_score(data)
            quality_metrics['accuracy'] = accuracy_score
            
            # Calculate overall quality score (weighted average)
            weights = {'completeness': 0.3, 'validity': 0.3, 'consistency': 0.2, 'accuracy': 0.2}
            overall_score = sum(quality_metrics[metric] * weights[metric] for metric in weights)
            
            # Determine quality level
            quality_level = self._get_quality_level(overall_score)
            
            # Count valid/invalid records
            valid_records, invalid_records = self._count_record_quality(data)
            
            # Generate quality report
            quality_report = self._generate_quality_report(data, quality_metrics, overall_score)
            
            success = overall_score >= self.quality_threshold
            
            logger.info(f"Data quality assessment: {overall_score:.1f}% ({quality_level})")
            
            return ProcessingResult(
                success=success,
                data=data,
                records_processed=len(data),
                metadata={
                    'quality_score': overall_score,
                    'quality_level': quality_level,
                    'quality_metrics': quality_metrics,
                    'valid_records': valid_records,
                    'invalid_records': invalid_records,
                    'quality_report': quality_report,
                    'threshold_met': success
                }
            )
            
        except Exception as e:
            logger.error(f"Error in data quality validation: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Validation error: {str(e)}"
            )
    
    def _calculate_completeness_score(self, data: pd.DataFrame) -> float:
        """Calculate data completeness score (0-100)"""
        if data.empty:
            return 0.0
        
        total_cells = data.size
        non_null_cells = data.count().sum()
        
        completeness = safe_divide(non_null_cells, total_cells, 0) * 100
        return min(100.0, max(0.0, completeness))
    
    def _calculate_validity_score(self, data: pd.DataFrame) -> float:
        """Calculate data validity score based on field-specific rules"""
        if data.empty:
            return 0.0
        
        validity_checks = []
        
        # Order ID validation
        if 'order_id' in data.columns:
            valid_order_ids = data['order_id'].str.match(r'^[A-Z]{3}-\d{4}$|^API-\d{4}$', na=False)
            validity_checks.append(valid_order_ids.mean())
        
        # Email validation
        if 'customer_email' in data.columns:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            valid_emails = data['customer_email'].str.match(email_pattern, na=False)
            validity_checks.append(valid_emails.mean())
        
        # Numeric field validation
        numeric_fields = ['quantity', 'price', 'total_amount']
        for field in numeric_fields:
            if field in data.columns:
                valid_numeric = pd.to_numeric(data[field], errors='coerce').notna()
                positive_values = data[field] > 0
                validity_checks.append((valid_numeric & positive_values).mean())
        
        # Date validation
        if 'order_date' in data.columns:
            try:
                valid_dates = pd.to_datetime(data['order_date'], errors='coerce').notna()
                validity_checks.append(valid_dates.mean())
            except:
                validity_checks.append(0.0)
        
        # Calculate average validity
        if validity_checks:
            validity_score = np.mean(validity_checks) * 100
        else:
            validity_score = 100.0  # No specific validation rules applied
        
        return min(100.0, max(0.0, validity_score))
    
    def _calculate_consistency_score(self, data: pd.DataFrame) -> float:
        """Calculate data consistency score"""
        if data.empty:
            return 0.0
        
        consistency_checks = []
        
        # Check for duplicate order IDs
        if 'order_id' in data.columns:
            unique_ratio = data['order_id'].nunique() / len(data)
            consistency_checks.append(unique_ratio)
        
        # Check price vs total_amount consistency
        if all(col in data.columns for col in ['price', 'quantity', 'total_amount']):
            calculated_total = data['price'] * data['quantity']
            actual_total = data['total_amount']
            consistent_totals = np.isclose(calculated_total, actual_total, rtol=0.01)
            consistency_checks.append(consistent_totals.mean())
        
        # Check data type consistency
        for column in data.columns:
            if data[column].dtype == 'object':
                # Check for consistent string formatting
                non_null_values = data[column].dropna()
                if len(non_null_values) > 0:
                    # Simple consistency check: similar string lengths
                    lengths = non_null_values.str.len()
                    length_consistency = 1 - (lengths.std() / lengths.mean()) if lengths.mean() > 0 else 1
                    consistency_checks.append(min(1.0, max(0.0, length_consistency)))
        
        if consistency_checks:
            consistency_score = np.mean(consistency_checks) * 100
        else:
            consistency_score = 100.0
        
        return min(100.0, max(0.0, consistency_score))
    
    def _calculate_accuracy_score(self, data: pd.DataFrame) -> float:
        """Calculate data accuracy score based on business rules"""
        if data.empty:
            return 0.0
        
        accuracy_checks = []
        
        # Reasonable price ranges
        if 'price' in data.columns:
            reasonable_prices = (data['price'] >= 1) & (data['price'] <= 10000)
            accuracy_checks.append(reasonable_prices.mean())
        
        # Reasonable quantities
        if 'quantity' in data.columns:
            reasonable_quantities = (data['quantity'] >= 1) & (data['quantity'] <= 100)
            accuracy_checks.append(reasonable_quantities.mean())
        
        # Valid customer names (not empty, reasonable length)
        if 'customer_name' in data.columns:
            valid_names = (data['customer_name'].str.len() >= 2) & (data['customer_name'].str.len() <= 100)
            accuracy_checks.append(valid_names.mean())
        
        # Valid product names
        if 'product' in data.columns:
            valid_products = (data['product'].str.len() >= 2) & (data['product'].str.len() <= 200)
            accuracy_checks.append(valid_products.mean())
        
        # Date accuracy (not in future, not too old)
        if 'order_date' in data.columns:
            try:
                dates = pd.to_datetime(data['order_date'], errors='coerce')
                today = datetime.now()
                five_years_ago = today - timedelta(days=1825)
                
                valid_date_range = (dates >= five_years_ago) & (dates <= today)
                accuracy_checks.append(valid_date_range.mean())
            except:
                accuracy_checks.append(0.0)
        
        if accuracy_checks:
            accuracy_score = np.mean(accuracy_checks) * 100
        else:
            accuracy_score = 100.0
        
        return min(100.0, max(0.0, accuracy_score))
    
    def _get_quality_level(self, score: float) -> str:
        """Get quality level based on score"""
        if score >= 95:
            return "excellent"
        elif score >= 85:
            return "good"
        elif score >= 70:
            return "fair"
        elif score >= 50:
            return "poor"
        else:
            return "critical"
    
    def _count_record_quality(self, data: pd.DataFrame) -> Tuple[int, int]:
        """Count valid and invalid records"""
        if data.empty:
            return 0, 0
        
        # Simple record quality check based on required fields
        valid_records = 0
        
        for _, row in data.iterrows():
            is_valid = True
            
            # Check required fields are not null
            for field in self.required_fields:
                if field in data.columns and pd.isna(row[field]):
                    is_valid = False
                    break
            
            # Additional basic checks
            if is_valid and 'price' in data.columns:
                if pd.isna(row['price']) or row['price'] <= 0:
                    is_valid = False
            
            if is_valid and 'quantity' in data.columns:
                if pd.isna(row['quantity']) or row['quantity'] <= 0:
                    is_valid = False
            
            if is_valid:
                valid_records += 1
        
        invalid_records = len(data) - valid_records
        return valid_records, invalid_records
    
    def _generate_quality_report(self, data: pd.DataFrame, metrics: Dict[str, float], overall_score: float) -> Dict[str, Any]:
        """Generate detailed quality report"""
        report = {
            'total_records': len(data),
            'total_fields': len(data.columns),
            'overall_score': overall_score,
            'quality_level': self._get_quality_level(overall_score),
            'metrics_breakdown': metrics,
            'field_analysis': {},
            'issues_found': []
        }
        
        # Field-level analysis
        for column in data.columns:
            field_info = {
                'data_type': str(data[column].dtype),
                'null_count': data[column].isnull().sum(),
                'null_percentage': (data[column].isnull().sum() / len(data)) * 100,
                'unique_values': data[column].nunique(),
                'unique_percentage': (data[column].nunique() / len(data)) * 100
            }
            
            # Add specific analysis for different field types
            if data[column].dtype in ['int64', 'float64']:
                field_info.update({
                    'min_value': data[column].min(),
                    'max_value': data[column].max(),
                    'mean_value': data[column].mean(),
                    'std_value': data[column].std()
                })
            elif data[column].dtype == 'object':
                field_info.update({
                    'avg_length': data[column].str.len().mean() if not data[column].empty else 0,
                    'max_length': data[column].str.len().max() if not data[column].empty else 0
                })
            
            report['field_analysis'][column] = field_info
            
            # Identify issues
            if field_info['null_percentage'] > 10:
                report['issues_found'].append(f"High null percentage in {column}: {field_info['null_percentage']:.1f}%")
        
        return report

class SchemaValidator:
    """Schema validation for data structure"""
    
    def __init__(self):
        self.required_fields = config.get('data_quality.required_fields', [])
        logger.info(f"Schema validator initialized with {len(self.required_fields)} required fields")
    
    @timing_decorator
    def validate_schema(self, data: pd.DataFrame, schema_name: str = "orders") -> ProcessingResult:
        """
        Validate data schema against expected structure
        
        Args:
            data (pd.DataFrame): Data to validate
            schema_name (str): Schema name for validation
            
        Returns:
            ProcessingResult: Schema validation results
        """
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for schema validation"
                )
            
            validation_results = {
                'schema_name': schema_name,
                'total_fields': len(data.columns),
                'required_fields': len(self.required_fields),
                'missing_fields': [],
                'extra_fields': [],
                'field_types': {},
                'validation_errors': [],
                'validation_warnings': []
            }
            
            # Check required fields
            is_valid, missing_fields = validate_required_fields(data, self.required_fields)
            validation_results['missing_fields'] = missing_fields
            
            if missing_fields:
                validation_results['validation_errors'].append(f"Missing required fields: {', '.join(missing_fields)}")
            
            # Check field types
            expected_types = {
                'order_id': 'object',
                'customer_name': 'object',
                'product': 'object',
                'quantity': ['int64', 'float64'],
                'price': ['float64', 'int64'],
                'order_date': 'object'
            }
            
            for field, expected_type in expected_types.items():
                if field in data.columns:
                    actual_type = str(data[field].dtype)
                    validation_results['field_types'][field] = actual_type
                    
                    if isinstance(expected_type, list):
                        if actual_type not in expected_type:
                            validation_results['validation_warnings'].append(
                                f"Field {field} has type {actual_type}, expected one of {expected_type}"
                            )
                    else:
                        if actual_type != expected_type:
                            validation_results['validation_warnings'].append(
                                f"Field {field} has type {actual_type}, expected {expected_type}"
                            )
            
            # Check for extra fields (informational)
            all_expected_fields = set(self.required_fields + list(expected_types.keys()))
            actual_fields = set(data.columns)
            extra_fields = actual_fields - all_expected_fields
            validation_results['extra_fields'] = list(extra_fields)
            
            if extra_fields:
                validation_results['validation_warnings'].append(f"Extra fields found: {', '.join(extra_fields)}")
            
            # Overall validation success
            success = is_valid and len(validation_results['validation_errors']) == 0
            
            logger.info(f"Schema validation for {schema_name}: {'✅ PASSED' if success else '❌ FAILED'}")
            
            return ProcessingResult(
                success=success,
                data=data,
                records_processed=len(data),
                metadata=validation_results
            )
            
        except Exception as e:
            logger.error(f"Error in schema validation: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Schema validation error: {str(e)}"
            )

class ValidationOrchestrator:
    """Main validation orchestrator"""
    
    def __init__(self):
        self.data_validator = DataValidator()
        self.schema_validator = SchemaValidator()
        logger.info("Validation orchestrator initialized")
    
    @timing_decorator
    def validate_all(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Run all validation checks
        
        Args:
            data (pd.DataFrame): Data to validate
            
        Returns:
            ProcessingResult: Combined validation results
        """
        try:
            validation_summary = {
                'schema_validation': {},
                'quality_validation': {},
                'overall_success': False,
                'recommendations': []
            }
            
            # Schema validation
            logger.info("Running schema validation...")
            schema_result = self.schema_validator.validate_schema(data)
            validation_summary['schema_validation'] = schema_result.metadata
            
            # Data quality validation
            logger.info("Running data quality validation...")
            quality_result = self.data_validator.validate_data_quality(data)
            validation_summary['quality_validation'] = quality_result.metadata
            
            # Determine overall success
            schema_success = schema_result.success
            quality_success = quality_result.success
            overall_success = schema_success and quality_success
            
            validation_summary['overall_success'] = overall_success
            
            # Generate recommendations
            recommendations = []
            
            if not schema_success:
                recommendations.append("Fix schema issues: ensure all required fields are present")
            
            if not quality_success:
                quality_score = quality_result.metadata.get('quality_score', 0)
                recommendations.append(f"Improve data quality: current score {quality_score:.1f}% is below threshold")
            
            if quality_result.metadata.get('invalid_records', 0) > 0:
                invalid_count = quality_result.metadata['invalid_records']
                recommendations.append(f"Review and fix {invalid_count} invalid records")
            
            validation_summary['recommendations'] = recommendations
            
            logger.info(f"Validation completed: Schema={'✅' if schema_success else '❌'}, Quality={'✅' if quality_success else '❌'}")
            
            return ProcessingResult(
                success=overall_success,
                data=data,
                records_processed=len(data),
                metadata=validation_summary
            )
            
        except Exception as e:
            logger.error(f"Error in validation orchestration: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Validation orchestration error: {str(e)}"
            )

if __name__ == "__main__":
    # Test validation
    import pandas as pd
    
    # Create sample data
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
    
    print("Testing validation...")
    validator = ValidationOrchestrator()
    result = validator.validate_all(sample_data)
    
    if result.success:
        print("✅ Validation passed")
        quality_score = result.metadata['quality_validation'].get('quality_score', 0)
        print(f"Quality score: {quality_score:.1f}%")
    else:
        print("❌ Validation failed")
        print(f"Error: {result.error_message}")
        if result.metadata.get('recommendations'):
            print("Recommendations:")
            for rec in result.metadata['recommendations']:
                print(f"  - {rec}")