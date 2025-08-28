# Scalable Data Ingestion Pipeline

A production-ready data ingestion pipeline that collects, validates, transforms, and stores data from multiple sources with comprehensive business intelligence enrichment.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Internet connection (for API data)

### Installation
```bash
# Clone repository
git clone https://github.com/Amruth22/Scalable-Data-Ingestion.git
cd Scalable-Data-Ingestion

# Install dependencies
pip install -r requirements.txt
```

### Run Pipeline
```bash
# Run complete pipeline
python run_pipeline.py

# Run with options
python run_pipeline.py --limit 50 --report auto

# Run health check
python run_pipeline.py --health
```

### Run Tests
```bash
# Run all unit tests
python3 -m pytest tests.py -v

# Run with coverage
python3 -m pytest tests.py -v --cov=src
```

## 📊 What It Does

**Data Flow:**
```
📥 API Data → 🔍 Validation → 🧹 Transformation → 💾 SQLite Database
```

**Key Features:**
- ✅ **Multi-source ingestion** (API, CSV, JSON files)
- ✅ **Data quality scoring** (0-100% with detailed metrics)
- ✅ **Business intelligence enrichment** (11 → 59+ fields)
- ✅ **Customer segmentation** (VIP, Premium, Standard, Budget)
- ✅ **Product intelligence** (brand detection, categorization)
- ✅ **Risk assessment** (order risk scoring)
- ✅ **SQLite database storage** with comprehensive schema
- ✅ **File exports** (CSV, JSON formats)
- ✅ **Comprehensive logging** and error handling

## 🏗️ Architecture

```
📦 src/
├── 📄 ingestion.py      # Data collection (API + files)
├── 📄 validation.py     # Quality scoring + schema validation  
├── 📄 transformation.py # Cleaning + enrichment + standardization
├── 📄 storage.py        # Database + file operations
├── 📄 pipeline.py       # Main orchestration
└── 📄 utils.py          # Configuration + utilities
```

## 📈 Results

**Data Transformation:**
- **437% field increase**: 11 → 59 fields
- **Quality improvement**: 0% → 95%+ scores
- **Business intelligence**: Customer segments, product brands, risk scores
- **Performance**: Sub-second processing for 100 records

**Database Schema:**
- `orders` - Main order data with enriched fields
- `pipeline_runs` - Execution tracking
- `data_quality_metrics` - Quality metrics over time

## 🧪 Testing

**10 Core Unit Tests:**
1. API ingestion success
2. Data validation quality scoring  
3. Data transformation cleaning
4. Data enrichment
5. Database save operations
6. Pipeline orchestration
7. Error handling
8. Configuration loading
9. Data quality metrics
10. End-to-end pipeline execution

## 📋 Configuration

Edit `config/config.yaml` to customize:
- Database settings
- API endpoints  
- Data quality thresholds
- Transformation options

## 🎯 Use Cases

- **E-commerce analytics** - Order processing and customer insights
- **Data quality management** - Comprehensive validation and scoring
- **Business intelligence** - Automated customer and product analysis
- **Learning data engineering** - Production-ready pipeline patterns

## 📊 Sample Output

```bash
🎉 Status: SUCCESS
📈 Records Processed: 100
⏱️ Execution Time: 2.3s
🎯 Quality Score: 96.2%
📊 Fields Added: 48
💾 Database Records: 100
📁 Files Exported: 2
```

## 🔧 CLI Options

```bash
python run_pipeline.py --help

Options:
  --limit N              API record limit (default: 100)
  --no-validation        Skip validation stage
  --no-transformation    Skip transformation stage  
  --no-storage          Skip storage stage
  --report auto         Generate execution report
  --status              Show pipeline status
  --health              Run health check
  --log-level LEVEL     Set logging level
```

## 📄 License

MIT License - see LICENSE file for details.