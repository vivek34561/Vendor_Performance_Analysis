# Vendor Performance Analysis

A comprehensive data analysis project for tracking and analyzing vendor performance metrics in inventory management and sales operations. This project provides insights into vendor sales, purchase patterns, profit margins, and operational efficiency.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [Key Metrics](#key-metrics)
- [Requirements](#requirements)
- [License](#license)

## 🎯 Overview

This project analyzes vendor performance by integrating multiple data sources including:
- Purchase data (vendor invoices, purchase prices)
- Sales data (transactions, quantities, revenue)
- Inventory data (stock levels, turnover rates)

The analysis provides actionable insights for vendor management, procurement optimization, and business intelligence.

## ✨ Features

- **Automated Data Ingestion**: Load CSV files into SQLite database automatically
- **Data Integration**: Merge purchase, sales, and freight data across vendors
- **Performance Metrics**: Calculate profit margins, stock turnover, and sales ratios
- **Exploratory Analysis**: Interactive Jupyter notebooks for data exploration
- **DVC Integration**: Version control for large datasets
- **Logging System**: Comprehensive logging for data pipeline operations

## 📁 Project Structure

```
Vendor_Performance_Analysis/
├── data/                              # Data directory (tracked with DVC)
├── data_ingestion.py                  # Script to load CSV files into SQLite database
├── get_vendor_summary.py              # Script to generate vendor performance summary
├── vendor_performance_analysis.ipynb  # Main analysis notebook
├── Exploratory_data_analysis.ipynb    # EDA notebook
├── new.ipynb                          # Additional analysis notebook
├── requirements.txt                   # Python dependencies
├── inventory.db                       # SQLite database (tracked with DVC)
├── vendor_sales_summary.csv           # Generated summary file (tracked with DVC)
├── data.dvc                           # DVC tracking file for data directory
├── inventory.db.dvc                   # DVC tracking file for database
├── vendor_sales_summary.csv.dvc       # DVC tracking file for summary
└── logs/                              # Log files directory
```

## 🚀 Installation

### Prerequisites

- Python 3.7 or higher
- pip (Python package manager)
- Git
- DVC (Data Version Control)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/vivek34561/Vendor_Performance_Analysis.git
cd Vendor_Performance_Analysis
```

2. Create a virtual environment (recommended):
```bash
python -m venv vendor_env
source vendor_env/bin/activate  # On Windows: vendor_env\Scripts\activate
```

3. Install required dependencies:
```bash
pip install -r requirements.txt
```

4. Pull data from DVC (if configured):
```bash
dvc pull
```

## 💻 Usage

### 1. Data Ingestion

Load CSV files from the `data/` directory into the SQLite database:

```bash
python data_ingestion.py
```

This script will:
- Read all CSV files from the `data/` directory
- Create tables in `inventory.db` SQLite database
- Log the ingestion process to `logs/ingestion_db.log`

### 2. Generate Vendor Summary

Create a comprehensive vendor performance summary:

```bash
python get_vendor_summary.py
```

This script will:
- Query and merge purchase, sales, and freight data
- Calculate key performance metrics
- Generate `vendor_sales_summary.csv`
- Log the process to `logs/get_vendor_summary.log`

### 3. Exploratory Analysis

Launch Jupyter notebooks for interactive analysis:

```bash
jupyter notebook
```

Open any of the following notebooks:
- `vendor_performance_analysis.ipynb` - Main performance analysis
- `Exploratory_data_analysis.ipynb` - Initial data exploration
- `new.ipynb` - Additional analysis

## 🔄 Data Pipeline

The data pipeline consists of three main stages:

1. **Ingestion Stage** (`data_ingestion.py`)
   - Loads raw CSV files
   - Creates normalized database tables
   - Validates data integrity

2. **Transformation Stage** (`get_vendor_summary.py`)
   - Joins multiple data sources
   - Aggregates vendor-level metrics
   - Calculates derived KPIs

3. **Analysis Stage** (Jupyter Notebooks)
   - Visualizes trends and patterns
   - Performs statistical analysis
   - Generates insights and recommendations

## 📊 Key Metrics

The analysis computes the following key performance indicators:

| Metric | Description |
|--------|-------------|
| **Gross Profit** | Total Sales Dollars - Total Purchase Dollars |
| **Profit Margin** | (Gross Profit / Total Sales Dollars) × 100 |
| **Stock Turnover** | Total Sales Quantity / Total Purchase Quantity |
| **Sales to Purchase Ratio** | Total Sales Dollars / Total Purchase Dollars |
| **Freight Cost** | Total freight charges per vendor |
| **Excise Tax** | Total excise tax collected |

## 📦 Requirements

All Python dependencies are listed in `requirements.txt`:

- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **sqlalchemy**: Database ORM and connection management
- **matplotlib**: Data visualization
- **seaborn**: Statistical data visualization
- **scipy**: Scientific computing
- **ipykernel**: Jupyter notebook kernel
- **dvc**: Data version control

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2025 vivek kumar gupta

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the issues page.

## 📧 Contact

For questions or feedback, please open an issue on the GitHub repository.

---

**Note**: This project uses DVC for data versioning. Large data files are tracked separately and need to be pulled using `dvc pull` after cloning the repository.