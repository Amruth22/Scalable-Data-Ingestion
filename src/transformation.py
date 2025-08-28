"""
Data transformation module for cleaning, enrichment, and standardization
Provides comprehensive data processing and business intelligence enhancement
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import re

from .utils import (
    config, ProcessingResult, timing_decorator, safe_divide
)

logger = logging.getLogger(__name__)

class DataCleaner:
    """Data cleaning and preprocessing"""
    
    def __init__(self):
        self.enable_cleaning = config.get('transformation.enable_cleaning', True)
        logger.info("Data cleaner initialized")
    
    @timing_decorator
    def clean_data(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Comprehensive data cleaning
        
        Args:
            data (pd.DataFrame): Raw data to clean
            
        Returns:
            ProcessingResult: Cleaned data
        """
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for cleaning"
                )
            
            original_records = len(data)
            cleaned_data = data.copy()
            
            cleaning_summary = {
                'original_records': original_records,
                'operations_performed': [],
                'records_removed': 0,
                'fields_cleaned': 0
            }
            
            # 1. Remove exact duplicates
            before_dedup = len(cleaned_data)
            cleaned_data = cleaned_data.drop_duplicates()
            after_dedup = len(cleaned_data)
            
            if before_dedup != after_dedup:
                removed = before_dedup - after_dedup
                cleaning_summary['operations_performed'].append(f"Removed {removed} duplicate records")
                cleaning_summary['records_removed'] += removed
            
            # 2. Remove duplicates based on order_id
            if 'order_id' in cleaned_data.columns:
                before_order_dedup = len(cleaned_data)
                cleaned_data = cleaned_data.drop_duplicates(subset=['order_id'], keep='first')
                after_order_dedup = len(cleaned_data)
                
                if before_order_dedup != after_order_dedup:
                    removed = before_order_dedup - after_order_dedup
                    cleaning_summary['operations_performed'].append(f"Removed {removed} duplicate order IDs")
                    cleaning_summary['records_removed'] += removed
            
            # 3. Fix data types
            cleaned_data = self._fix_data_types(cleaned_data, cleaning_summary)
            
            # 4. Clean text fields
            cleaned_data = self._clean_text_fields(cleaned_data, cleaning_summary)
            
            # 5. Handle missing values
            cleaned_data = self._handle_missing_values(cleaned_data, cleaning_summary)
            
            # 6. Remove invalid records
            cleaned_data = self._remove_invalid_records(cleaned_data, cleaning_summary)
            
            final_records = len(cleaned_data)
            retention_rate = safe_divide(final_records, original_records, 0) * 100
            
            cleaning_summary.update({
                'final_records': final_records,
                'retention_rate': retention_rate,
                'total_removed': original_records - final_records
            })
            
            logger.info(f"Data cleaning completed: {final_records}/{original_records} records retained ({retention_rate:.1f}%)")
            
            return ProcessingResult(
                success=True,
                data=cleaned_data,
                records_processed=final_records,
                metadata=cleaning_summary
            )
            
        except Exception as e:
            logger.error(f"Error in data cleaning: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Data cleaning error: {str(e)}"
            )
    
    def _fix_data_types(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Fix data types for proper processing"""
        fields_fixed = 0
        
        # Convert numeric fields
        numeric_fields = ['quantity', 'price', 'total_amount', 'discount']
        for field in numeric_fields:
            if field in data.columns:
                try:
                    original_type = data[field].dtype
                    data[field] = pd.to_numeric(data[field], errors='coerce')
                    if str(original_type) != str(data[field].dtype):
                        fields_fixed += 1
                except:
                    pass
        
        # Convert date fields
        date_fields = ['order_date', 'ingested_at']
        for field in date_fields:
            if field in data.columns:
                try:
                    original_type = data[field].dtype
                    data[field] = pd.to_datetime(data[field], errors='coerce')
                    if str(original_type) != str(data[field].dtype):
                        fields_fixed += 1
                except:
                    pass
        
        if fields_fixed > 0:
            summary['operations_performed'].append(f"Fixed data types for {fields_fixed} fields")
            summary['fields_cleaned'] += fields_fixed
        
        return data
    
    def _clean_text_fields(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Clean and standardize text fields"""
        text_fields_cleaned = 0
        
        # Clean customer names
        if 'customer_name' in data.columns:
            data['customer_name'] = data['customer_name'].str.strip().str.title()
            text_fields_cleaned += 1
        
        # Clean product names
        if 'product' in data.columns:
            data['product'] = data['product'].str.strip()
            text_fields_cleaned += 1
        
        # Clean email addresses
        if 'customer_email' in data.columns:
            data['customer_email'] = data['customer_email'].str.strip().str.lower()
            text_fields_cleaned += 1
        
        # Clean source field
        if 'source' in data.columns:
            data['source'] = data['source'].str.strip().str.lower()
            text_fields_cleaned += 1
        
        if text_fields_cleaned > 0:
            summary['operations_performed'].append(f"Cleaned {text_fields_cleaned} text fields")
            summary['fields_cleaned'] += text_fields_cleaned
        
        return data
    
    def _handle_missing_values(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Handle missing values based on field-specific strategies"""
        missing_handled = 0
        
        # Fill missing quantities with 1
        if 'quantity' in data.columns:
            missing_qty = data['quantity'].isnull().sum()
            if missing_qty > 0:
                data['quantity'].fillna(1, inplace=True)
                missing_handled += missing_qty
        
        # Fill missing discounts with 0
        if 'discount' in data.columns:
            missing_discount = data['discount'].isnull().sum()
            if missing_discount > 0:
                data['discount'].fillna(0.0, inplace=True)
                missing_handled += missing_discount
        
        # Fill missing customer names
        if 'customer_name' in data.columns:
            missing_names = data['customer_name'].isnull().sum()
            if missing_names > 0:
                data['customer_name'].fillna('Unknown Customer', inplace=True)
                missing_handled += missing_names
        
        if missing_handled > 0:
            summary['operations_performed'].append(f"Handled {missing_handled} missing values")
        
        return data
    
    def _remove_invalid_records(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Remove records that are fundamentally invalid"""
        before_removal = len(data)
        
        # Remove records with missing critical fields
        critical_fields = ['order_id', 'product', 'price']
        for field in critical_fields:
            if field in data.columns:
                data = data.dropna(subset=[field])
        
        # Remove records with invalid prices
        if 'price' in data.columns:
            data = data[data['price'] > 0]
        
        # Remove records with invalid quantities
        if 'quantity' in data.columns:
            data = data[data['quantity'] > 0]
        
        after_removal = len(data)
        removed = before_removal - after_removal
        
        if removed > 0:
            summary['operations_performed'].append(f"Removed {removed} invalid records")
            summary['records_removed'] += removed
        
        return data

class DataEnricher:
    """Data enrichment with business intelligence"""
    
    def __init__(self):
        self.enable_enrichment = config.get('transformation.enable_enrichment', True)
        logger.info("Data enricher initialized")
    
    @timing_decorator
    def enrich_data(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Comprehensive data enrichment
        
        Args:
            data (pd.DataFrame): Clean data to enrich
            
        Returns:
            ProcessingResult: Enriched data with business intelligence
        """
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for enrichment"
                )
            
            original_fields = len(data.columns)
            enriched_data = data.copy()
            
            enrichment_summary = {
                'original_fields': original_fields,
                'enrichment_operations': [],
                'fields_added': 0
            }
            
            # 1. Add calculated fields
            enriched_data = self._add_calculated_fields(enriched_data, enrichment_summary)
            
            # 2. Add customer intelligence
            enriched_data = self._add_customer_intelligence(enriched_data, enrichment_summary)
            
            # 3. Add product intelligence
            enriched_data = self._add_product_intelligence(enriched_data, enrichment_summary)
            
            # 4. Add financial analytics
            enriched_data = self._add_financial_analytics(enriched_data, enrichment_summary)
            
            # 5. Add temporal analytics
            enriched_data = self._add_temporal_analytics(enriched_data, enrichment_summary)
            
            # 6. Add risk assessment
            enriched_data = self._add_risk_assessment(enriched_data, enrichment_summary)
            
            final_fields = len(enriched_data.columns)
            fields_added = final_fields - original_fields
            
            enrichment_summary.update({
                'final_fields': final_fields,
                'total_fields_added': fields_added
            })
            
            logger.info(f"Data enrichment completed: {fields_added} fields added ({original_fields} → {final_fields})")
            
            return ProcessingResult(
                success=True,
                data=enriched_data,
                records_processed=len(enriched_data),
                metadata=enrichment_summary
            )
            
        except Exception as e:
            logger.error(f"Error in data enrichment: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Data enrichment error: {str(e)}"
            )
    
    def _add_calculated_fields(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Add basic calculated fields"""
        fields_added = 0
        
        # Calculate total amount if not present
        if 'total_amount' not in data.columns and all(col in data.columns for col in ['price', 'quantity']):
            discount = data.get('discount', 0)
            data['total_amount'] = (data['price'] * data['quantity']) - discount
            fields_added += 1
        
        # Add processing timestamp
        data['processed_at'] = datetime.now().isoformat()
        fields_added += 1
        
        # Add record ID for tracking
        data['record_id'] = range(1, len(data) + 1)
        fields_added += 1
        
        if fields_added > 0:
            summary['enrichment_operations'].append(f"Added {fields_added} calculated fields")
            summary['fields_added'] += fields_added
        
        return data
    
    def _add_customer_intelligence(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Add customer segmentation and intelligence"""
        fields_added = 0
        
        # Customer segmentation based on order value
        if 'total_amount' in data.columns:
            def segment_customer(amount):
                if amount >= 1000:
                    return 'VIP'
                elif amount >= 500:
                    return 'Premium'
                elif amount >= 100:
                    return 'Standard'
                else:
                    return 'Budget'
            
            data['customer_segment'] = data['total_amount'].apply(segment_customer)
            fields_added += 1
        
        # Customer value tier
        if 'total_amount' in data.columns:
            data['customer_value_tier'] = pd.cut(
                data['total_amount'],
                bins=[0, 50, 200, 500, 1000, float('inf')],
                labels=['Low', 'Medium', 'High', 'Premium', 'VIP']
            )
            fields_added += 1
        
        # Customer type based on email domain
        if 'customer_email' in data.columns:
            def get_customer_type(email):
                if pd.isna(email):
                    return 'Unknown'
                domain = email.split('@')[-1].lower()
                business_domains = ['company.com', 'corp.com', 'business.com']
                if any(bd in domain for bd in business_domains):
                    return 'Business'
                else:
                    return 'Individual'
            
            data['customer_type'] = data['customer_email'].apply(get_customer_type)
            fields_added += 1
        
        if fields_added > 0:
            summary['enrichment_operations'].append(f"Added {fields_added} customer intelligence fields")
            summary['fields_added'] += fields_added
        
        return data
    
    def _add_product_intelligence(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Add product categorization and intelligence"""
        fields_added = 0
        
        # Extract product brand
        if 'product' in data.columns:
            def extract_brand(product):
                if pd.isna(product):
                    return 'Unknown'
                product_lower = product.lower()
                brands = {
                    'apple': ['iphone', 'ipad', 'macbook', 'airpods', 'apple watch'],
                    'samsung': ['galaxy', 'samsung'],
                    'nintendo': ['nintendo', 'switch'],
                    'amazon': ['kindle', 'echo', 'alexa'],
                    'sony': ['playstation', 'sony'],
                    'microsoft': ['xbox', 'surface']
                }
                
                for brand, keywords in brands.items():
                    if any(keyword in product_lower for keyword in keywords):
                        return brand.title()
                
                return 'Other'
            
            data['product_brand'] = data['product'].apply(extract_brand)
            fields_added += 1
        
        # Product category
        if 'product' in data.columns:
            def categorize_product(product):
                if pd.isna(product):
                    return 'Unknown'
                product_lower = product.lower()
                
                categories = {
                    'Mobile': ['iphone', 'phone', 'mobile'],
                    'Computer': ['macbook', 'laptop', 'computer', 'surface'],
                    'Audio': ['airpods', 'headphones', 'speaker', 'echo'],
                    'Gaming': ['nintendo', 'xbox', 'playstation', 'switch'],
                    'Tablet': ['ipad', 'tablet'],
                    'Wearable': ['watch', 'fitness'],
                    'E-Reader': ['kindle', 'reader']
                }
                
                for category, keywords in categories.items():
                    if any(keyword in product_lower for keyword in keywords):
                        return category
                
                return 'Electronics'
            
            data['product_category_detailed'] = data['product'].apply(categorize_product)
            fields_added += 1
        
        # Product price tier
        if 'price' in data.columns:
            data['product_price_tier'] = pd.cut(
                data['price'],
                bins=[0, 100, 500, 1000, 2000, float('inf')],
                labels=['Budget', 'Mid-Range', 'Premium', 'Luxury', 'Ultra-Premium']
            )
            fields_added += 1
        
        if fields_added > 0:
            summary['enrichment_operations'].append(f"Added {fields_added} product intelligence fields")
            summary['fields_added'] += fields_added
        
        return data
    
    def _add_financial_analytics(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Add financial analytics and metrics"""
        fields_added = 0
        
        # Estimated profit margin (simplified)
        if 'price' in data.columns:
            def estimate_profit_margin(price):
                if price < 100:
                    return 0.15  # 15%
                elif price < 500:
                    return 0.20  # 20%
                elif price < 1000:
                    return 0.25  # 25%
                else:
                    return 0.30  # 30%
            
            data['estimated_profit_margin'] = data['price'].apply(estimate_profit_margin)
            fields_added += 1
        
        # Estimated profit amount
        if all(col in data.columns for col in ['total_amount', 'estimated_profit_margin']):
            data['estimated_profit'] = data['total_amount'] * data['estimated_profit_margin']
            fields_added += 1
        
        # Revenue tier
        if 'total_amount' in data.columns:
            data['revenue_tier'] = pd.cut(
                data['total_amount'],
                bins=[0, 100, 300, 600, 1000, float('inf')],
                labels=['Low', 'Medium', 'High', 'Premium', 'Enterprise']
            )
            fields_added += 1
        
        if fields_added > 0:
            summary['enrichment_operations'].append(f"Added {fields_added} financial analytics fields")
            summary['fields_added'] += fields_added
        
        return data
    
    def _add_temporal_analytics(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Add temporal analytics"""
        fields_added = 0
        
        if 'order_date' in data.columns:
            try:
                # Ensure order_date is datetime
                data['order_date'] = pd.to_datetime(data['order_date'])
                
                # Extract date components
                data['order_year'] = data['order_date'].dt.year
                data['order_month'] = data['order_date'].dt.month
                data['order_day'] = data['order_date'].dt.day
                data['order_weekday'] = data['order_date'].dt.day_name()
                data['order_quarter'] = data['order_date'].dt.quarter
                fields_added += 5
                
                # Season
                def get_season(month):
                    if month in [12, 1, 2]:
                        return 'Winter'
                    elif month in [3, 4, 5]:
                        return 'Spring'
                    elif month in [6, 7, 8]:
                        return 'Summer'
                    else:
                        return 'Fall'
                
                data['season'] = data['order_month'].apply(get_season)
                fields_added += 1
                
                # Days since order
                data['days_since_order'] = (datetime.now() - data['order_date']).dt.days
                fields_added += 1
                
            except Exception as e:
                logger.warning(f"Error in temporal analytics: {e}")
        
        if fields_added > 0:
            summary['enrichment_operations'].append(f"Added {fields_added} temporal analytics fields")
            summary['fields_added'] += fields_added
        
        return data
    
    def _add_risk_assessment(self, data: pd.DataFrame, summary: Dict) -> pd.DataFrame:
        """Add risk assessment fields"""
        fields_added = 0
        
        # Order risk score (simplified)
        def calculate_risk_score(row):
            risk_score = 0
            
            # High value orders are riskier
            if 'total_amount' in row and row['total_amount'] > 1000:
                risk_score += 30
            
            # New customers are riskier
            if 'customer_name' in row and 'Unknown' in str(row['customer_name']):
                risk_score += 20
            
            # Weekend orders might be riskier
            if 'order_weekday' in row and row['order_weekday'] in ['Saturday', 'Sunday']:
                risk_score += 10
            
            return min(100, risk_score)
        
        data['risk_score'] = data.apply(calculate_risk_score, axis=1)
        fields_added += 1
        
        # Risk level
        def get_risk_level(score):
            if score >= 50:
                return 'High'
            elif score >= 25:
                return 'Medium'
            else:
                return 'Low'
        
        data['risk_level'] = data['risk_score'].apply(get_risk_level)
        fields_added += 1
        
        if fields_added > 0:
            summary['enrichment_operations'].append(f"Added {fields_added} risk assessment fields")
            summary['fields_added'] += fields_added
        
        return data

class DataStandardizer:
    """Data standardization and formatting"""
    
    def __init__(self):
        self.enable_standardization = config.get('transformation.enable_standardization', True)
        logger.info("Data standardizer initialized")
    
    @timing_decorator
    def standardize_data(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Standardize data formats and values
        
        Args:
            data (pd.DataFrame): Data to standardize
            
        Returns:
            ProcessingResult: Standardized data
        """
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for standardization"
                )
            
            standardized_data = data.copy()
            
            standardization_summary = {
                'operations_performed': [],
                'fields_standardized': 0
            }
            
            # Standardize order IDs
            if 'order_id' in standardized_data.columns:
                standardized_data['order_id'] = standardized_data['order_id'].str.upper()
                standardization_summary['operations_performed'].append("Standardized order IDs to uppercase")
                standardization_summary['fields_standardized'] += 1
            
            # Standardize customer names
            if 'customer_name' in standardized_data.columns:
                standardized_data['customer_name'] = standardized_data['customer_name'].str.title()
                standardization_summary['operations_performed'].append("Standardized customer names to title case")
                standardization_summary['fields_standardized'] += 1
            
            # Standardize email addresses
            if 'customer_email' in standardized_data.columns:
                standardized_data['customer_email'] = standardized_data['customer_email'].str.lower().str.strip()
                standardization_summary['operations_performed'].append("Standardized email addresses to lowercase")
                standardization_summary['fields_standardized'] += 1
            
            # Standardize source field
            if 'source' in standardized_data.columns:
                standardized_data['source'] = standardized_data['source'].str.lower()
                standardization_summary['operations_performed'].append("Standardized source field to lowercase")
                standardization_summary['fields_standardized'] += 1
            
            # Round numeric fields
            numeric_fields = ['price', 'total_amount', 'estimated_profit']
            for field in numeric_fields:
                if field in standardized_data.columns:
                    standardized_data[field] = standardized_data[field].round(2)
                    standardization_summary['fields_standardized'] += 1
            
            if numeric_fields:
                standardization_summary['operations_performed'].append(f"Rounded {len([f for f in numeric_fields if f in standardized_data.columns])} numeric fields to 2 decimal places")
            
            logger.info(f"Data standardization completed: {standardization_summary['fields_standardized']} fields standardized")
            
            return ProcessingResult(
                success=True,
                data=standardized_data,
                records_processed=len(standardized_data),
                metadata=standardization_summary
            )
            
        except Exception as e:
            logger.error(f"Error in data standardization: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Data standardization error: {str(e)}"
            )

class TransformationOrchestrator:
    """Main transformation orchestrator"""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.enricher = DataEnricher()
        self.standardizer = DataStandardizer()
        logger.info("Transformation orchestrator initialized")
    
    @timing_decorator
    def transform_all(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Run complete transformation pipeline
        
        Args:
            data (pd.DataFrame): Raw data to transform
            
        Returns:
            ProcessingResult: Fully transformed data
        """
        try:
            transformation_summary = {
                'original_records': len(data),
                'original_fields': len(data.columns),
                'cleaning_results': {},
                'enrichment_results': {},
                'standardization_results': {},
                'final_records': 0,
                'final_fields': 0
            }
            
            current_data = data.copy()
            
            # Step 1: Data Cleaning
            logger.info("Starting data cleaning...")
            cleaning_result = self.cleaner.clean_data(current_data)
            transformation_summary['cleaning_results'] = cleaning_result.metadata
            
            if not cleaning_result.success:
                return ProcessingResult(
                    success=False,
                    error_message=f"Data cleaning failed: {cleaning_result.error_message}"
                )
            
            current_data = cleaning_result.data
            
            # Step 2: Data Enrichment
            logger.info("Starting data enrichment...")
            enrichment_result = self.enricher.enrich_data(current_data)
            transformation_summary['enrichment_results'] = enrichment_result.metadata
            
            if not enrichment_result.success:
                logger.warning(f"Data enrichment failed: {enrichment_result.error_message}")
                # Continue with cleaning results
            else:
                current_data = enrichment_result.data
            
            # Step 3: Data Standardization
            logger.info("Starting data standardization...")
            standardization_result = self.standardizer.standardize_data(current_data)
            transformation_summary['standardization_results'] = standardization_result.metadata
            
            if not standardization_result.success:
                logger.warning(f"Data standardization failed: {standardization_result.error_message}")
                # Continue with previous results
            else:
                current_data = standardization_result.data
            
            # Final summary
            transformation_summary.update({
                'final_records': len(current_data),
                'final_fields': len(current_data.columns),
                'records_retained': len(current_data) / len(data) * 100,
                'fields_added': len(current_data.columns) - len(data.columns)
            })
            
            logger.info(f"Transformation completed: {len(data)} → {len(current_data)} records, {len(data.columns)} → {len(current_data.columns)} fields")
            
            return ProcessingResult(
                success=True,
                data=current_data,
                records_processed=len(current_data),
                metadata=transformation_summary
            )
            
        except Exception as e:
            logger.error(f"Error in transformation orchestration: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Transformation error: {str(e)}"
            )

if __name__ == "__main__":
    # Test transformation
    sample_data = pd.DataFrame({
        'order_id': ['api-0001', 'API-0002', 'api-0001'],  # Duplicate and case issues
        'customer_name': ['john doe', 'JANE SMITH', 'bob wilson'],
        'customer_email': ['JOHN@EXAMPLE.COM', 'jane@example.com', 'bob@example.com'],
        'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
        'quantity': [1, 1, 2],
        'price': [999.999, 1999.99, 249.99],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17']
    })
    
    print("Testing transformation...")
    transformer = TransformationOrchestrator()
    result = transformer.transform_all(sample_data)
    
    if result.success:
        print(f"✅ Transformation successful")
        print(f"Records: {result.metadata['original_records']} → {result.metadata['final_records']}")
        print(f"Fields: {result.metadata['original_fields']} → {result.metadata['final_fields']}")
        print(f"Sample transformed data:\n{result.data.head()}")
    else:
        print(f"❌ Transformation failed: {result.error_message}")