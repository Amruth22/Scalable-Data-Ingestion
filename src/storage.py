"""
Data storage module for database operations and file management
Handles SQLite database operations and file export functionality
"""

import pandas as pd
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import json

from .utils import (
    config, ProcessingResult, timing_decorator, 
    ensure_directory_exists, safe_divide
)

logger = logging.getLogger(__name__)

class DatabaseManager:
    """SQLite database management for data storage"""
    
    def __init__(self):
        self.db_path = config.get('database.path', 'data/orders.db')
        self.connection_timeout = config.get('database.connection_timeout', 30)
        self.batch_size = config.get('database.batch_size', 1000)
        
        # Ensure database directory exists
        ensure_directory_exists(Path(self.db_path).parent)
        
        # Initialize database
        self._initialize_database()
        
        logger.info(f"Database manager initialized: {self.db_path}")
    
    def _initialize_database(self):
        """Initialize database with required tables"""
        try:
            with sqlite3.connect(self.db_path, timeout=self.connection_timeout) as conn:
                cursor = conn.cursor()
                
                # Create orders table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id TEXT UNIQUE NOT NULL,
                        customer_name TEXT,
                        customer_email TEXT,
                        product TEXT,
                        product_category TEXT,
                        product_brand TEXT,
                        quantity INTEGER,
                        price REAL,
                        discount REAL DEFAULT 0,
                        total_amount REAL,
                        order_date TEXT,
                        source TEXT,
                        customer_segment TEXT,
                        risk_level TEXT,
                        estimated_profit REAL,
                        processed_at TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create pipeline_runs table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT UNIQUE NOT NULL,
                        pipeline_name TEXT,
                        start_time TEXT,
                        end_time TEXT,
                        status TEXT,
                        records_processed INTEGER DEFAULT 0,
                        records_failed INTEGER DEFAULT 0,
                        error_message TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create data_quality_metrics table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS data_quality_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT,
                        metric_name TEXT,
                        metric_value REAL,
                        metric_type TEXT,
                        source_table TEXT,
                        measured_at TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (run_id) REFERENCES pipeline_runs (run_id)
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders (order_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_customer_email ON orders (customer_email)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders (order_date)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_id ON pipeline_runs (run_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status)')
                
                conn.commit()
                logger.info("Database tables initialized successfully")
                
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    @timing_decorator
    def save_orders(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Save orders data to database
        
        Args:
            data (pd.DataFrame): Orders data to save
            
        Returns:
            ProcessingResult: Save operation results
        """
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for saving"
                )
            
            records_saved = 0
            records_updated = 0
            records_failed = 0
            
            with sqlite3.connect(self.db_path, timeout=self.connection_timeout) as conn:
                cursor = conn.cursor()
                
                # Prepare data for insertion
                data_to_save = data.copy()
                
                # Ensure required columns exist
                required_columns = ['order_id', 'customer_name', 'product', 'quantity', 'price', 'total_amount']
                for col in required_columns:
                    if col not in data_to_save.columns:
                        if col in ['quantity']:
                            data_to_save[col] = 1
                        elif col in ['price', 'total_amount']:
                            data_to_save[col] = 0.0
                        else:
                            data_to_save[col] = 'Unknown'
                
                # Add timestamps
                current_time = datetime.now().isoformat()
                data_to_save['updated_at'] = current_time
                
                # Process in batches
                for i in range(0, len(data_to_save), self.batch_size):
                    batch = data_to_save.iloc[i:i + self.batch_size]
                    
                    for _, row in batch.iterrows():
                        try:
                            # Try to insert, update if exists
                            cursor.execute('''
                                INSERT OR REPLACE INTO orders (
                                    order_id, customer_name, customer_email, product, 
                                    product_category, product_brand, quantity, price, 
                                    discount, total_amount, order_date, source,
                                    customer_segment, risk_level, estimated_profit,
                                    processed_at, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                row.get('order_id'),
                                row.get('customer_name'),
                                row.get('customer_email'),
                                row.get('product'),
                                row.get('product_category'),
                                row.get('product_brand'),
                                row.get('quantity', 1),
                                row.get('price', 0.0),
                                row.get('discount', 0.0),
                                row.get('total_amount', 0.0),
                                row.get('order_date'),
                                row.get('source'),
                                row.get('customer_segment'),
                                row.get('risk_level'),
                                row.get('estimated_profit'),
                                row.get('processed_at'),
                                current_time
                            ))
                            
                            if cursor.rowcount > 0:
                                records_saved += 1
                            
                        except Exception as e:
                            logger.warning(f"Failed to save record {row.get('order_id', 'unknown')}: {e}")
                            records_failed += 1
                
                conn.commit()
            
            success = records_saved > 0
            
            logger.info(f"Database save completed: {records_saved} saved, {records_failed} failed")
            
            return ProcessingResult(
                success=success,
                records_processed=records_saved,
                metadata={
                    'records_saved': records_saved,
                    'records_updated': records_updated,
                    'records_failed': records_failed,
                    'total_records': len(data),
                    'success_rate': safe_divide(records_saved, len(data), 0) * 100
                }
            )
            
        except Exception as e:
            logger.error(f"Error saving orders to database: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Database save error: {str(e)}"
            )
    
    @timing_decorator
    def get_orders(self, limit: Optional[int] = None, order_by: str = 'created_at DESC') -> ProcessingResult:
        """
        Retrieve orders from database
        
        Args:
            limit (int, optional): Maximum number of records to retrieve
            order_by (str): Order by clause
            
        Returns:
            ProcessingResult: Retrieved orders data
        """
        try:
            with sqlite3.connect(self.db_path, timeout=self.connection_timeout) as conn:
                query = f"SELECT * FROM orders ORDER BY {order_by}"
                if limit:
                    query += f" LIMIT {limit}"
                
                data = pd.read_sql_query(query, conn)
            
            logger.info(f"Retrieved {len(data)} orders from database")
            
            return ProcessingResult(
                success=True,
                data=data,
                records_processed=len(data),
                metadata={'query': query, 'total_records': len(data)}
            )
            
        except Exception as e:
            logger.error(f"Error retrieving orders from database: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Database retrieval error: {str(e)}"
            )
    
    @timing_decorator
    def save_pipeline_run(self, run_data: Dict[str, Any]) -> ProcessingResult:
        """Save pipeline run information"""
        try:
            with sqlite3.connect(self.db_path, timeout=self.connection_timeout) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO pipeline_runs (
                        run_id, pipeline_name, start_time, end_time, status,
                        records_processed, records_failed, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    run_data.get('run_id'),
                    run_data.get('pipeline_name'),
                    run_data.get('start_time'),
                    run_data.get('end_time'),
                    run_data.get('status'),
                    run_data.get('records_processed', 0),
                    run_data.get('records_failed', 0),
                    run_data.get('error_message')
                ))
                
                conn.commit()
            
            return ProcessingResult(success=True, records_processed=1)
            
        except Exception as e:
            logger.error(f"Error saving pipeline run: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Pipeline run save error: {str(e)}"
            )
    
    @timing_decorator
    def save_quality_metrics(self, metrics_data: List[Dict[str, Any]]) -> ProcessingResult:
        """Save data quality metrics"""
        try:
            with sqlite3.connect(self.db_path, timeout=self.connection_timeout) as conn:
                cursor = conn.cursor()
                
                for metric in metrics_data:
                    cursor.execute('''
                        INSERT INTO data_quality_metrics (
                            run_id, metric_name, metric_value, metric_type,
                            source_table, measured_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        metric.get('run_id'),
                        metric.get('metric_name'),
                        metric.get('metric_value'),
                        metric.get('metric_type'),
                        metric.get('source_table'),
                        metric.get('measured_at')
                    ))
                
                conn.commit()
            
            return ProcessingResult(success=True, records_processed=len(metrics_data))
            
        except Exception as e:
            logger.error(f"Error saving quality metrics: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Quality metrics save error: {str(e)}"
            )
    
    @timing_decorator
    def get_database_stats(self) -> ProcessingResult:
        """Get database statistics"""
        try:
            with sqlite3.connect(self.db_path, timeout=self.connection_timeout) as conn:
                stats = {}
                
                # Orders count
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM orders")
                stats['orders_count'] = cursor.fetchone()[0]
                
                # Pipeline runs count
                cursor.execute("SELECT COUNT(*) FROM pipeline_runs")
                stats['pipeline_runs_count'] = cursor.fetchone()[0]
                
                # Database size
                db_size = Path(self.db_path).stat().st_size / (1024 * 1024)  # MB
                stats['database_size_mb'] = round(db_size, 2)
                
                # Recent activity
                cursor.execute("SELECT COUNT(*) FROM orders WHERE date(created_at) = date('now')")
                stats['orders_today'] = cursor.fetchone()[0]
                
                # Quality metrics
                cursor.execute("SELECT AVG(metric_value) FROM data_quality_metrics WHERE metric_name = 'data_quality_score'")
                avg_quality = cursor.fetchone()[0]
                stats['avg_quality_score'] = round(avg_quality, 2) if avg_quality else 0
            
            stats_df = pd.DataFrame([stats])
            
            return ProcessingResult(
                success=True,
                data=stats_df,
                records_processed=1,
                metadata=stats
            )
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Database stats error: {str(e)}"
            )

class FileManager:
    """File management for data export and archiving"""
    
    def __init__(self):
        self.output_dir = config.get('files.output_dir', 'data/output')
        self.archive_dir = config.get('files.archive_dir', 'data/archive')
        
        # Ensure directories exist
        ensure_directory_exists(self.output_dir)
        ensure_directory_exists(self.archive_dir)
        
        logger.info(f"File manager initialized: {self.output_dir}")
    
    @timing_decorator
    def export_data(self, data: pd.DataFrame, filename: str, formats: List[str] = None) -> ProcessingResult:
        """
        Export data to various file formats
        
        Args:
            data (pd.DataFrame): Data to export
            filename (str): Base filename (without extension)
            formats (List[str]): File formats to export ['csv', 'json', 'parquet']
            
        Returns:
            ProcessingResult: Export operation results
        """
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for export"
                )
            
            if formats is None:
                formats = ['csv', 'json']
            
            exported_files = []
            export_summary = {}
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"{filename}_{timestamp}"
            
            for format_type in formats:
                try:
                    if format_type.lower() == 'csv':
                        file_path = Path(self.output_dir) / f"{base_filename}.csv"
                        data.to_csv(file_path, index=False)
                        exported_files.append(str(file_path))
                        export_summary['csv'] = {'success': True, 'path': str(file_path), 'size_mb': file_path.stat().st_size / (1024*1024)}
                    
                    elif format_type.lower() == 'json':
                        file_path = Path(self.output_dir) / f"{base_filename}.json"
                        data.to_json(file_path, orient='records', indent=2)
                        exported_files.append(str(file_path))
                        export_summary['json'] = {'success': True, 'path': str(file_path), 'size_mb': file_path.stat().st_size / (1024*1024)}
                    
                    elif format_type.lower() == 'parquet':
                        try:
                            file_path = Path(self.output_dir) / f"{base_filename}.parquet"
                            data.to_parquet(file_path, index=False)
                            exported_files.append(str(file_path))
                            export_summary['parquet'] = {'success': True, 'path': str(file_path), 'size_mb': file_path.stat().st_size / (1024*1024)}
                        except ImportError:
                            logger.warning("Parquet export requires pyarrow or fastparquet")
                            export_summary['parquet'] = {'success': False, 'error': 'Missing parquet library'}
                    
                except Exception as e:
                    logger.error(f"Error exporting to {format_type}: {e}")
                    export_summary[format_type] = {'success': False, 'error': str(e)}
            
            success = len(exported_files) > 0
            
            logger.info(f"Data export completed: {len(exported_files)} files created")
            
            return ProcessingResult(
                success=success,
                records_processed=len(data),
                metadata={
                    'exported_files': exported_files,
                    'export_summary': export_summary,
                    'total_files': len(exported_files),
                    'base_filename': base_filename
                }
            )
            
        except Exception as e:
            logger.error(f"Error in data export: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Data export error: {str(e)}"
            )
    
    @timing_decorator
    def create_summary_report(self, data: pd.DataFrame, filename: str) -> ProcessingResult:
        """Create a summary report of the data"""
        try:
            if data is None or data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data provided for summary report"
                )
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_filename = f"{filename}_summary_{timestamp}.json"
            report_path = Path(self.output_dir) / report_filename
            
            # Generate summary statistics
            summary = {
                'report_generated_at': datetime.now().isoformat(),
                'total_records': len(data),
                'total_fields': len(data.columns),
                'data_types': data.dtypes.astype(str).to_dict(),
                'missing_values': data.isnull().sum().to_dict(),
                'summary_statistics': {}
            }
            
            # Numeric field statistics
            numeric_fields = data.select_dtypes(include=['number']).columns
            for field in numeric_fields:
                summary['summary_statistics'][field] = {
                    'count': int(data[field].count()),
                    'mean': float(data[field].mean()) if not data[field].empty else 0,
                    'std': float(data[field].std()) if not data[field].empty else 0,
                    'min': float(data[field].min()) if not data[field].empty else 0,
                    'max': float(data[field].max()) if not data[field].empty else 0,
                    'median': float(data[field].median()) if not data[field].empty else 0
                }
            
            # Categorical field statistics
            categorical_fields = data.select_dtypes(include=['object']).columns
            for field in categorical_fields:
                value_counts = data[field].value_counts().head(10)
                summary['summary_statistics'][field] = {
                    'unique_values': int(data[field].nunique()),
                    'most_common': value_counts.to_dict()
                }
            
            # Business intelligence summary
            if 'customer_segment' in data.columns:
                summary['business_intelligence'] = {
                    'customer_segments': data['customer_segment'].value_counts().to_dict()
                }
            
            if 'product_brand' in data.columns:
                summary['business_intelligence']['product_brands'] = data['product_brand'].value_counts().to_dict()
            
            if 'total_amount' in data.columns:
                summary['business_intelligence']['revenue_summary'] = {
                    'total_revenue': float(data['total_amount'].sum()),
                    'average_order_value': float(data['total_amount'].mean()),
                    'highest_order': float(data['total_amount'].max()),
                    'lowest_order': float(data['total_amount'].min())
                }
            
            # Save summary report
            with open(report_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Summary report created: {report_path}")
            
            return ProcessingResult(
                success=True,
                records_processed=len(data),
                metadata={
                    'report_path': str(report_path),
                    'report_size_mb': report_path.stat().st_size / (1024*1024),
                    'summary': summary
                }
            )
            
        except Exception as e:
            logger.error(f"Error creating summary report: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Summary report error: {str(e)}"
            )
    
    def get_storage_summary(self) -> Dict[str, Any]:
        """Get storage directory summary"""
        try:
            summary = {
                'output_directory': self.output_dir,
                'archive_directory': self.archive_dir,
                'output_files': [],
                'archive_files': [],
                'total_size_mb': 0
            }
            
            # Scan output directory
            if Path(self.output_dir).exists():
                for file_path in Path(self.output_dir).iterdir():
                    if file_path.is_file():
                        file_info = {
                            'name': file_path.name,
                            'size_mb': file_path.stat().st_size / (1024*1024),
                            'modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                        }
                        summary['output_files'].append(file_info)
                        summary['total_size_mb'] += file_info['size_mb']
            
            # Scan archive directory
            if Path(self.archive_dir).exists():
                for file_path in Path(self.archive_dir).iterdir():
                    if file_path.is_file():
                        file_info = {
                            'name': file_path.name,
                            'size_mb': file_path.stat().st_size / (1024*1024),
                            'modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                        }
                        summary['archive_files'].append(file_info)
                        summary['total_size_mb'] += file_info['size_mb']
            
            summary['total_files'] = len(summary['output_files']) + len(summary['archive_files'])
            summary['total_size_mb'] = round(summary['total_size_mb'], 2)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting storage summary: {e}")
            return {'error': str(e)}

class StorageOrchestrator:
    """Main storage orchestrator"""
    
    def __init__(self):
        self.database_manager = DatabaseManager()
        self.file_manager = FileManager()
        logger.info("Storage orchestrator initialized")
    
    @timing_decorator
    def store_all(self, data: pd.DataFrame, export_name: str = "processed_data") -> ProcessingResult:
        """
        Store data in database and export to files
        
        Args:
            data (pd.DataFrame): Data to store
            export_name (str): Base name for exported files
            
        Returns:
            ProcessingResult: Combined storage results
        """
        try:
            storage_summary = {
                'database_results': {},
                'file_export_results': {},
                'summary_report_results': {},
                'total_operations': 0,
                'successful_operations': 0
            }
            
            # Store in database
            logger.info("Storing data in database...")
            db_result = self.database_manager.save_orders(data)
            storage_summary['database_results'] = db_result.metadata
            storage_summary['total_operations'] += 1
            if db_result.success:
                storage_summary['successful_operations'] += 1
            
            # Export to files
            logger.info("Exporting data to files...")
            export_result = self.file_manager.export_data(data, export_name, ['csv', 'json'])
            storage_summary['file_export_results'] = export_result.metadata
            storage_summary['total_operations'] += 1
            if export_result.success:
                storage_summary['successful_operations'] += 1
            
            # Create summary report
            logger.info("Creating summary report...")
            report_result = self.file_manager.create_summary_report(data, export_name)
            storage_summary['summary_report_results'] = report_result.metadata
            storage_summary['total_operations'] += 1
            if report_result.success:
                storage_summary['successful_operations'] += 1
            
            # Overall success
            success = storage_summary['successful_operations'] > 0
            
            logger.info(f"Storage completed: {storage_summary['successful_operations']}/{storage_summary['total_operations']} operations successful")
            
            return ProcessingResult(
                success=success,
                records_processed=len(data),
                metadata=storage_summary
            )
            
        except Exception as e:
            logger.error(f"Error in storage orchestration: {e}")
            return ProcessingResult(
                success=False,
                error_message=f"Storage orchestration error: {str(e)}"
            )

if __name__ == "__main__":
    # Test storage
    sample_data = pd.DataFrame({
        'order_id': ['TEST-001', 'TEST-002', 'TEST-003'],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Wilson'],
        'customer_email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'product': ['iPhone 15', 'MacBook Pro', 'AirPods Pro'],
        'quantity': [1, 1, 2],
        'price': [999.99, 1999.99, 249.99],
        'total_amount': [999.99, 1999.99, 499.98],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17'],
        'customer_segment': ['Premium', 'VIP', 'Standard']
    })
    
    print("Testing storage...")
    storage = StorageOrchestrator()
    result = storage.store_all(sample_data, "test_export")
    
    if result.success:
        print("✅ Storage successful")
        print(f"Operations: {result.metadata['successful_operations']}/{result.metadata['total_operations']}")
    else:
        print(f"❌ Storage failed: {result.error_message}")
    
    # Test database retrieval
    print("\nTesting data retrieval...")
    db_result = storage.database_manager.get_orders(limit=5)
    if db_result.success:
        print(f"✅ Retrieved {len(db_result.data)} records from database")
    else:
        print(f"❌ Retrieval failed: {db_result.error_message}")