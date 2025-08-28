"""
Data ingestion module for collecting data from APIs and files
Handles HTTP requests, file processing, and data collection
"""

import pandas as pd
import requests
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import time

from .utils import (
    config, ProcessingResult, DataSourceType, 
    retry_operation, timing_decorator, ensure_directory_exists
)

logger = logging.getLogger(__name__)

class APIIngestion:
    """API data ingestion class"""
    
    def __init__(self):
        self.base_url = config.get('api.base_url')
        self.timeout = config.get('api.timeout', 30)
        self.retry_attempts = config.get('api.retry_attempts', 3)
        self.retry_delay = config.get('api.retry_delay', 5)
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ScalableDataIngestion/1.0',
            'Accept': 'application/json'
        })
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.0
        
        logger.info(f"API ingestion initialized: {self.base_url}")
    
    def _rate_limit(self):
        """Apply rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @timing_decorator
    def fetch_data(self, endpoint: str, limit: int = 100) -> ProcessingResult:
        """
        Fetch data from API endpoint
        
        Args:
            endpoint (str): API endpoint
            limit (int): Maximum records to fetch
            
        Returns:
            ProcessingResult: API response data
        """
        try:
            self._rate_limit()
            
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
            params = {'_limit': limit} if limit else {}
            
            def make_request():
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            
            # Retry logic
            data = retry_operation(make_request, self.retry_attempts, self.retry_delay)
            
            if not data:
                return ProcessingResult(
                    success=False,
                    error_message="No data received from API"
                )
            
            logger.info(f"Successfully fetched {len(data)} records from {endpoint}")
            
            return ProcessingResult(
                success=True,
                data=data,
                records_processed=len(data),
                metadata={'source': 'api', 'endpoint': endpoint, 'url': url}
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"API request failed: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected error in API ingestion: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Unexpected error: {str(e)}"
            )
    
    @timing_decorator
    def fetch_orders(self, limit: int = 100) -> ProcessingResult:
        """
        Fetch orders from API (converts posts to order-like data)
        
        Args:
            limit (int): Maximum orders to fetch
            
        Returns:
            ProcessingResult: Order data
        """
        try:
            # Fetch posts data from JSONPlaceholder
            api_result = self.fetch_data('posts', limit)
            
            if not api_result.success:
                return api_result
            
            posts_data = api_result.data
            
            # Transform posts to order-like structure
            orders = []
            for i, post in enumerate(posts_data):
                order = {
                    'order_id': f"API-{post.get('id', i+1):04d}",
                    'customer_name': f"Customer {post.get('userId', 1)}",
                    'customer_email': f"customer{post.get('userId', 1)}@example.com",
                    'product': self._generate_product_name(post.get('title', 'Unknown Product')),
                    'product_category': 'Electronics',
                    'quantity': 1,
                    'price': round(50 + (post.get('id', 1) % 20) * 25.99, 2),
                    'discount': 0.0,
                    'order_date': datetime.now().strftime('%Y-%m-%d'),
                    'source': DataSourceType.API_REST.value,
                    'ingested_at': datetime.now().isoformat(),
                    'api_post_id': post.get('id')
                }
                
                # Calculate total amount
                order['total_amount'] = round(order['price'] * order['quantity'] - order['discount'], 2)
                orders.append(order)
            
            # Convert to DataFrame
            df = pd.DataFrame(orders)
            
            logger.info(f"Transformed {len(df)} API records to orders")
            
            return ProcessingResult(
                success=True,
                data=df,
                records_processed=len(df),
                metadata={'source': 'api_orders', 'original_records': len(posts_data)}
            )
            
        except Exception as e:
            logger.error(f"Error fetching orders: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Error fetching orders: {str(e)}"
            )
    
    def _generate_product_name(self, title: str) -> str:
        """Generate realistic product name from post title"""
        title_lower = title.lower()
        
        product_mapping = {
            'phone': 'iPhone 15',
            'computer': 'MacBook Pro',
            'music': 'AirPods Pro',
            'watch': 'Apple Watch',
            'tablet': 'iPad Air',
            'game': 'Nintendo Switch',
            'book': 'Kindle Paperwhite'
        }
        
        for keyword, product in product_mapping.items():
            if keyword in title_lower:
                return product
        
        # Default products
        products = ['iPhone 15', 'MacBook Pro', 'AirPods Pro', 'iPad Air', 'Apple Watch']
        return products[hash(title) % len(products)]
    
    def close(self):
        """Close session and cleanup"""
        self.session.close()
        logger.info("API session closed")

class FileIngestion:
    """File data ingestion class"""
    
    def __init__(self):
        self.input_dir = config.get('files.input_dir', 'data/input')
        self.processed_dir = config.get('files.processed_dir', 'data/processed')
        self.error_dir = config.get('files.error_dir', 'data/errors')
        self.supported_formats = config.get('files.supported_formats', ['csv', 'json'])
        
        # Ensure directories exist
        for directory in [self.input_dir, self.processed_dir, self.error_dir]:
            ensure_directory_exists(directory)
        
        logger.info(f"File ingestion initialized: {self.input_dir}")
    
    @timing_decorator
    def process_csv_files(self) -> ProcessingResult:
        """Process CSV files from input directory"""
        try:
            csv_files = list(Path(self.input_dir).glob('*.csv'))
            
            if not csv_files:
                return ProcessingResult(
                    success=True,
                    data=pd.DataFrame(),
                    records_processed=0,
                    metadata={'message': 'No CSV files found'}
                )
            
            all_data = []
            processed_files = 0
            
            for csv_file in csv_files:
                try:
                    # Read CSV with error handling
                    df = pd.read_csv(csv_file, encoding='utf-8')
                    
                    # Add metadata
                    df['source'] = DataSourceType.FILE_CSV.value
                    df['source_file'] = csv_file.name
                    df['ingested_at'] = datetime.now().isoformat()
                    
                    all_data.append(df)
                    processed_files += 1
                    
                    # Move to processed directory
                    processed_path = Path(self.processed_dir) / csv_file.name
                    csv_file.rename(processed_path)
                    
                    logger.info(f"Processed CSV file: {csv_file.name} ({len(df)} records)")
                    
                except Exception as e:
                    logger.error(f"Error processing {csv_file.name}: {e}")
                    # Move to error directory
                    error_path = Path(self.error_dir) / csv_file.name
                    csv_file.rename(error_path)
            
            # Combine all data
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
            else:
                combined_df = pd.DataFrame()
            
            return ProcessingResult(
                success=True,
                data=combined_df,
                records_processed=len(combined_df),
                metadata={'files_processed': processed_files, 'total_files': len(csv_files)}
            )
            
        except Exception as e:
            logger.error(f"Error in CSV processing: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"CSV processing error: {str(e)}"
            )
    
    @timing_decorator
    def process_json_files(self) -> ProcessingResult:
        """Process JSON files from input directory"""
        try:
            json_files = list(Path(self.input_dir).glob('*.json'))
            
            if not json_files:
                return ProcessingResult(
                    success=True,
                    data=pd.DataFrame(),
                    records_processed=0,
                    metadata={'message': 'No JSON files found'}
                )
            
            all_data = []
            processed_files = 0
            
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    # Handle different JSON structures
                    if isinstance(json_data, list):
                        df = pd.DataFrame(json_data)
                    elif isinstance(json_data, dict):
                        if 'orders' in json_data:
                            df = pd.DataFrame(json_data['orders'])
                        else:
                            df = pd.DataFrame([json_data])
                    else:
                        raise ValueError("Unsupported JSON structure")
                    
                    # Add metadata
                    df['source'] = DataSourceType.FILE_JSON.value
                    df['source_file'] = json_file.name
                    df['ingested_at'] = datetime.now().isoformat()
                    
                    all_data.append(df)
                    processed_files += 1
                    
                    # Move to processed directory
                    processed_path = Path(self.processed_dir) / json_file.name
                    json_file.rename(processed_path)
                    
                    logger.info(f"Processed JSON file: {json_file.name} ({len(df)} records)")
                    
                except Exception as e:
                    logger.error(f"Error processing {json_file.name}: {e}")
                    # Move to error directory
                    error_path = Path(self.error_dir) / json_file.name
                    json_file.rename(error_path)
            
            # Combine all data
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
            else:
                combined_df = pd.DataFrame()
            
            return ProcessingResult(
                success=True,
                data=combined_df,
                records_processed=len(combined_df),
                metadata={'files_processed': processed_files, 'total_files': len(json_files)}
            )
            
        except Exception as e:
            logger.error(f"Error in JSON processing: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"JSON processing error: {str(e)}"
            )

class DataIngestion:
    """Main data ingestion orchestrator"""
    
    def __init__(self):
        self.api_ingestion = APIIngestion()
        self.file_ingestion = FileIngestion()
        logger.info("Data ingestion orchestrator initialized")
    
    @timing_decorator
    def collect_all_data(self, api_limit: int = 100) -> ProcessingResult:
        """
        Collect data from all sources
        
        Args:
            api_limit (int): Limit for API data collection
            
        Returns:
            ProcessingResult: Combined data from all sources
        """
        try:
            all_data = []
            total_records = 0
            sources_summary = {}
            
            # Collect from API
            logger.info("Collecting data from API...")
            api_result = self.api_ingestion.fetch_orders(api_limit)
            sources_summary['api'] = {
                'success': api_result.success,
                'records': api_result.records_processed,
                'error': api_result.error_message
            }
            
            if api_result.success and api_result.data is not None:
                all_data.append(api_result.data)
                total_records += api_result.records_processed
            
            # Collect from CSV files
            logger.info("Collecting data from CSV files...")
            csv_result = self.file_ingestion.process_csv_files()
            sources_summary['csv'] = {
                'success': csv_result.success,
                'records': csv_result.records_processed,
                'error': csv_result.error_message
            }
            
            if csv_result.success and csv_result.data is not None and not csv_result.data.empty:
                all_data.append(csv_result.data)
                total_records += csv_result.records_processed
            
            # Collect from JSON files
            logger.info("Collecting data from JSON files...")
            json_result = self.file_ingestion.process_json_files()
            sources_summary['json'] = {
                'success': json_result.success,
                'records': json_result.records_processed,
                'error': json_result.error_message
            }
            
            if json_result.success and json_result.data is not None and not json_result.data.empty:
                all_data.append(json_result.data)
                total_records += json_result.records_processed
            
            # Combine all data
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Remove duplicates based on order_id if present
                if 'order_id' in combined_df.columns:
                    before_dedup = len(combined_df)
                    combined_df = combined_df.drop_duplicates(subset=['order_id'], keep='first')
                    after_dedup = len(combined_df)
                    if before_dedup != after_dedup:
                        logger.info(f"Removed {before_dedup - after_dedup} duplicate records")
            else:
                combined_df = pd.DataFrame()
            
            success = len(combined_df) > 0
            
            logger.info(f"Data collection completed: {len(combined_df)} total records from {len(all_data)} sources")
            
            return ProcessingResult(
                success=success,
                data=combined_df,
                records_processed=len(combined_df),
                metadata={
                    'sources_summary': sources_summary,
                    'total_sources': len(all_data),
                    'original_records': total_records
                }
            )
            
        except Exception as e:
            logger.error(f"Error in data collection: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Data collection error: {str(e)}"
            )
    
    def close(self):
        """Cleanup resources"""
        self.api_ingestion.close()
        logger.info("Data ingestion resources closed")

if __name__ == "__main__":
    # Test data ingestion
    ingestion = DataIngestion()
    
    try:
        print("Testing data ingestion...")
        result = ingestion.collect_all_data(limit=10)
        
        if result.success:
            print(f"✅ Successfully collected {result.records_processed} records")
            if result.data is not None and not result.data.empty:
                print(f"Sample data:\n{result.data.head()}")
        else:
            print(f"❌ Data collection failed: {result.error_message}")
    
    finally:
        ingestion.close()