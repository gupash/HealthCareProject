# Healthcare Data Ingestion Pipeline

## Project Overview
This project implements a streaming data ingestion pipeline for healthcare member enrollment data. 
It processes CSV files containing member information, performs data quality checks, validates and transforms the data, 
and loads it into a Delta Lake table for downstream analytics.

### Key Features

- **Streaming ingestion**: Monitors a directory for new CSV files and processes them incrementally
- **Data validation**: Validates input data against a defined schema using Pandera
- **Data transformation**: Standardizes formats (phone numbers, dates, gender codes, zip code)
- **Deduplication & merge**: Implements SCD Type 1 logic to keep the most recent member information
- **Audit logging**: Tracks ingestion statistics timestamp, (valid/invalid records) for each file

## Data Flow

### Input Source

- **Format**: CSV files
- **Location**: `input/raw/` directory
- **Naming convention**: `{client_id}_{creation_date}[Optional: _1 or _2 suffix].csv`
- **Schema**: Contains member demographics (member_id, name, dob, gender, contact info, plan_id, zip)

### Output Destination

- **Format**: Delta Lake tables
- **Warehouse type**: Delta Lake (ACID-compliant data lake storage)
- **Primary table**: `output/silver/` - Stores cleaned and validated member records
- **Audit table**: `output/checkpoint/audit/` - Tracks ingestion metrics per file

### Delta Lake Features

- **ACID transactions**: Ensures data consistency
- **Change Data Feed**: Enabled for tracking record-level changes
- **Time travel**: Allows querying historical versions of data
- **Schema evolution**: Supports adding new columns without breaking existing queries

## Audit Table

The audit table captures metadata for each ingestion run:

| Column | Description |
|--------|-------------|
| `client_id` | Source system identifier |
| `file_name` | Name of the processed CSV file |
| `total_rows` | Total records in the input file |
| `valid_count` | Number of records that passed validation |
| `invalid_count` | Number of records that failed validation |
| `ingestion_time` | Timestamp when the file was processed |
| `run_id` | Unique identifier for the ingestion job |

# Project Setup

## First Time Setup

1. Clone the repository
2. Run the setup script:
   ```bash
   ./setup.sh
   ```

## Workflow
1. Open Terminal and run: `source venv/bin/activate`
2. Format and fix issues: `./lint_and_test.sh`
4. Run full pipeline: `./run_pipeline.sh`