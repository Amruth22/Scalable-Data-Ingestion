# Scalable Data Ingestion Pipeline - Data Engineering Project

## Project Overview

Design and implement a production-ready data ingestion pipeline that collects, validates, transforms, and stores data from multiple sources with comprehensive business intelligence enrichment. The system should demonstrate enterprise-grade data processing capabilities with quality scoring, error handling, and monitoring.

## Requirements

### Pipeline Architecture
- Create a modular data ingestion pipeline with 6 core components
- Implement multi-source data collection (API, CSV, JSON files)
- Include comprehensive data validation with quality scoring (0-100%)
- Perform data transformation with cleaning, enrichment, and standardization
- Store processed data in SQLite database with file exports
- Provide CLI interface with multiple execution options

### Core Components Structure

### Component 1: Configuration and Utilities (`src/utils.py`)
- **Responsibility**: Configuration management and utility functions
- **Key Classes**:
  - `Config` - YAML configuration loader with dot notation access
  - `ProcessingResult` - Standard result container for all operations
  - `DataSourceType` - Enum for data source types
  - `ProcessingStatus` - Enum for processing status types
- **Key Functions**:
  - `safe_divide(numerator, denominator, default)` - Safe division with default value
  - `format_duration(seconds)` - Human-readable duration formatting
  - `retry_operation(func, max_attempts, delay)` - Retry logic with exponential backoff
  - `validate_required_fields(data, required_fields)` - Field validation utility
  - `ensure_directory_exists(directory)` - Directory creation utility
- **Configuration Features**:
  - YAML-based configuration with environment-specific overrides
  - Default configuration fallback system
  - Dot notation access for nested configuration values

### Component 2: Data Ingestion (`src/ingestion.py`)
- **Responsibility**: Collect data from multiple sources
- **Key Classes**:
  - `APIIngestion` - REST API data collection with rate limiting
  - `FileIngestion` - CSV and JSON file processing
  - `DataIngestion` - Main ingestion orchestrator
- **Data Sources**:
  - **API Source**: JSONPlaceholder API (transforms posts to order-like data)
  - **CSV Files**: Monitors `data/input/csv/` directory
  - **JSON Files**: Monitors `data/input/json/` directory
- **Key Functions**:
  - `fetch_orders(limit)` - Fetch and transform API data to orders
  - `process_csv_files()` - Process all CSV files in input directory
  - `process_json_files()` - Process all JSON files in input directory
  - `collect_all_data(api_limit)` - Orchestrate collection from all sources
- **Features**:
  - Rate limiting for API requests (1 request/second minimum)
  - Automatic file encoding detection
  - Error handling with file quarantine (move failed files to error directory)
  - Duplicate removal based on order_id
  - Connection pooling for HTTP requests

### Component 3: Data Validation (`src/validation.py`)
- **Responsibility**: Assess data quality and validate schema
- **Key Classes**:
  - `DataValidator` - Comprehensive quality assessment
  - `SchemaValidator` - Schema structure validation
  - `ValidationOrchestrator` - Main validation coordinator
- **Quality Metrics** (Weighted scoring system):
  - **Completeness (30%)**: Percentage of non-null values
  - **Validity (30%)**: Field-specific validation rules
  - **Consistency (20%)**: Data consistency checks
  - **Accuracy (20%)**: Business rule validation
- **Key Functions**:
  - `validate_data_quality(data)` - Calculate overall quality score (0-100%)
  - `validate_schema(data, schema_name)` - Validate required fields and types
  - `validate_all(data)` - Run complete validation suite
- **Validation Rules**:
  - Order ID format: `^[A-Z]{3}-\d{4}$` or `^API-\d{4}$`
  - Email format: Standard email regex validation
  - Price validation: Must be positive numbers
  - Date validation: Valid dates within reasonable range
  - Required fields: order_id, customer_name, product, quantity, price, order_date

### Component 4: Data Transformation (`src/transformation.py`)
- **Responsibility**: Clean, enrich, and standardize data
- **Key Classes**:
  - `DataCleaner` - Data cleaning and preprocessing
  - `DataEnricher` - Business intelligence enhancement
  - `DataStandardizer` - Format standardization
  - `TransformationOrchestrator` - Main transformation coordinator
- **Transformation Stages**:
  1. **Cleaning**: Remove duplicates, fix data types, handle missing values
  2. **Enrichment**: Add 48+ business intelligence fields
  3. **Standardization**: Consistent formatting and normalization
- **Key Functions**:
  - `clean_data(data)` - Comprehensive data cleaning
  - `enrich_data(data)` - Add business intelligence fields
  - `standardize_data(data)` - Format standardization
  - `transform_all(data)` - Complete transformation pipeline
- **Business Intelligence Fields Added**:
  - **Customer Intelligence**: customer_segment, customer_value_tier, customer_type
  - **Product Intelligence**: product_brand, product_category_detailed, product_price_tier
  - **Financial Analytics**: estimated_profit_margin, estimated_profit, revenue_tier
  - **Temporal Analytics**: order_year, order_month, order_weekday, season
  - **Risk Assessment**: risk_score, risk_level

### Component 5: Data Storage (`src/storage.py`)
- **Responsibility**: Database operations and file management
- **Key Classes**:
  - `DatabaseManager` - SQLite database operations
  - `FileManager` - File export and archiving
  - `StorageOrchestrator` - Main storage coordinator
- **Database Schema**:
  - `orders` table - Main order data with 18+ fields
  - `pipeline_runs` table - Execution tracking
  - `data_quality_metrics` table - Quality metrics over time
- **Key Functions**:
  - `save_orders(data)` - Batch insert/update orders with error handling
  - `get_orders(limit, order_by)` - Retrieve orders with pagination
  - `save_pipeline_run(run_data)` - Track pipeline execution
  - `export_data(data, filename, formats)` - Export to CSV, JSON, Parquet
  - `create_summary_report(data, filename)` - Generate data summary
- **Features**:
  - Batch processing for large datasets
  - UPSERT operations (insert or update)
  - Database indexing for performance
  - Multiple export formats
  - Storage usage analytics

### Component 6: Pipeline Orchestration (`src/pipeline.py`)
- **Responsibility**: Coordinate all pipeline stages
- **Key Classes**:
  - `PipelineManager` - Main pipeline orchestrator
  - `PipelineResult` - Comprehensive execution result
- **Pipeline Stages**:
  1. **Ingestion** - Collect data from all sources
  2. **Validation** - Assess quality and validate schema
  3. **Transformation** - Clean, enrich, and standardize
  4. **Storage** - Save to database and export files
- **Key Functions**:
  - `run_pipeline(api_limit)` - Execute complete pipeline
  - `health_check()` - System health assessment
  - `get_pipeline_status()` - Current status and statistics
  - `generate_pipeline_report(result)` - Comprehensive execution report
- **Features**:
  - Stage-level error recovery
  - Execution time tracking
  - Comprehensive logging
  - Performance metrics collection
  - Automatic run ID generation

### Main Execution Script (`run_pipeline.py`)
- **Responsibility**: CLI interface for pipeline execution
- **Key Features**:
  - Command-line argument parsing
  - Multiple execution modes
  - Health check functionality
  - Report generation
  - Status monitoring
- **CLI Options**:
  - `--limit N` - API record limit (default: 100)
  - `--no-validation` - Skip validation stage
  - `--no-transformation` - Skip transformation stage
  - `--no-storage` - Skip storage stage
  - `--report auto` - Generate execution report
  - `--status` - Show pipeline status
  - `--health` - Run health check only
  - `--log-level LEVEL` - Set logging level

### Configuration System (`config/config.yaml`)
- **Database Configuration**: Path, timeout, batch size
- **API Configuration**: Base URL, timeout, retry settings
- **File Processing**: Input/output directories, supported formats
- **Pipeline Settings**: Batch size, workers, logging level
- **Data Quality**: Thresholds, required fields, validation rules
- **Transformation**: Enable/disable stages, strategies

### Unit Testing (`tests.py`)
- **Test Coverage**: Exactly 10 core unit test cases
- **Test Categories**:
  1. `test_api_ingestion_success` - API data fetching and transformation
  2. `test_data_validation_quality_scoring` - Quality assessment accuracy
  3. `test_data_transformation_cleaning` - Data cleaning effectiveness
  4. `test_data_enrichment` - Business intelligence field addition
  5. `test_database_save_orders` - Database storage operations
  6. `test_pipeline_orchestration` - Complete pipeline coordination
  7. `test_error_handling` - Graceful error management
  8. `test_configuration_loading` - Configuration system functionality
  9. `test_data_quality_metrics` - Quality metrics calculation
  10. `test_end_to_end_pipeline` - Complete real pipeline execution

### Data Processing Workflow
1. **Data Collection**: Fetch ~100 records from JSONPlaceholder API, process any CSV/JSON files
2. **Quality Assessment**: Calculate quality scores using weighted metrics
3. **Data Cleaning**: Remove duplicates, fix data types, handle missing values
4. **Business Intelligence**: Add customer segmentation, product intelligence, financial analytics
5. **Standardization**: Consistent formatting and normalization
6. **Storage**: Save to SQLite database with comprehensive schema
7. **Export**: Generate CSV, JSON files and summary reports
8. **Monitoring**: Track execution metrics and data quality trends

### Expected Data Transformation
- **Input**: 11 basic fields from API (id, userId, title, body, etc.)
- **Output**: 59+ enriched fields with business intelligence
- **Field Expansion**: 437% increase in data richness
- **Quality Improvement**: 0% → 95%+ quality scores
- **Processing Speed**: Sub-second execution for 100 records

### Technical Specifications
- **Framework**: Pure Python with pandas, requests, SQLite
- **Database**: SQLite with comprehensive schema and indexing
- **Configuration**: YAML-based with environment overrides
- **Logging**: Structured logging with multiple levels
- **Error Handling**: Comprehensive exception handling with recovery
- **Testing**: Pytest with mocking and fixtures
- **CLI**: Click-based command-line interface
- **Data Formats**: CSV, JSON, Parquet export support

### Performance Requirements
- Process 100 records in under 3 seconds
- Achieve 95%+ data quality scores
- Handle API rate limiting gracefully
- Support batch processing for large datasets
- Maintain sub-second response for health checks

### Error Handling Requirements
- Graceful degradation when data sources are unavailable
- Comprehensive logging of all errors and warnings
- Stage-level error recovery (continue pipeline if non-critical stage fails)
- File quarantine for corrupted input files
- Database transaction rollback on critical errors

### Monitoring and Observability
- Execution time tracking for each stage
- Data quality metrics collection over time
- Pipeline run history with success/failure tracking
- Storage usage monitoring
- Health check endpoints for system status

### Deliverables
1. **Core Pipeline Components** (`src/` directory with 6 modules)
2. **Configuration System** (`config/config.yaml`)
3. **Main Execution Script** (`run_pipeline.py`)
4. **Unit Test Suite** (`tests.py` with exactly 10 test cases)
5. **Dependencies** (`requirements.txt`)
6. **Documentation** (`README.md` with setup and usage)
7. **Package Structure** (`src/__init__.py`)

### Expected Learning Outcomes
- **Data Engineering Principles**: ETL/ELT pipeline design and implementation
- **Data Quality Management**: Validation, scoring, and improvement strategies
- **Business Intelligence**: Data enrichment and customer/product analytics
- **Error Handling**: Robust error management and recovery patterns
- **Testing Strategies**: Unit testing for data pipelines with mocking
- **Configuration Management**: YAML-based configuration with environment support
- **CLI Development**: Command-line interface design and argument parsing
- **Database Operations**: SQLite operations with schema design and indexing
- **Performance Optimization**: Batch processing and connection pooling
- **Monitoring and Logging**: Comprehensive observability for data pipelines

### Evaluation Criteria
- **Functionality** (30%): All pipeline stages work correctly
- **Data Quality** (25%): Achieves high quality scores and proper validation
- **Code Quality** (20%): Clean, modular, well-documented code
- **Error Handling** (15%): Robust error management and recovery
- **Testing** (10%): Comprehensive unit test coverage

### Advanced Extensions (Optional)
- Add support for additional data sources (databases, web scraping)
- Implement data lineage tracking
- Add machine learning-based anomaly detection
- Create web dashboard for pipeline monitoring
- Implement data versioning and rollback capabilities
- Add support for streaming data processing
- Integrate with cloud storage services (AWS S3, Google Cloud)
- Implement data encryption and security features