# ZEFIX Data Platform

A dbt project for processing Swiss commercial register data from ZEFIX using the medallion architecture pattern.

## Overview

This project transforms raw ZEFIX (Swiss Central Business Names Index) data into analytics-ready tables using a three-layer medallion architecture:

- **Bronze Layer**: Raw data extraction from JSON variants
- **Silver Layer**: Cleaned and structured data
- **Gold Layer**: Business-ready analytics and metrics

## Data Source

The project processes data from the ZEFIX Public API, which provides information about Swiss companies registered in the commercial register. The source data is stored in:

- **Database**: `ZEFIX`
- **Schema**: `PUBLIC`
- **Table**: `RAW`
- **Column**: `CONTENT` (Variant type containing JSON data)

## Architecture

### Bronze Layer
- `bronze_zefix_companies`: Raw JSON data extracted into structured columns

### Silver Layer
- `silver_companies`: Cleaned company master data
- `silver_shab_publications`: Normalized SHAB (Swiss Official Gazette) publications
- `silver_mutation_types`: Individual mutation types from publications

### Gold Layer
- `gold_company_overview`: Comprehensive company overview with business metrics
- `gold_company_activity`: Company activity analysis over time
- `gold_canton_statistics`: Canton-level aggregated statistics

### Seeds
- `legal_forms`: Swiss legal form reference data with German/English names and abbreviations

## Setup Instructions

### 1. Environment Setup

Create a `.env` file with your Snowflake credentials:

```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=your_role
export SNOWFLAKE_DATABASE=ZEFIX
export SNOWFLAKE_WAREHOUSE=your_warehouse
```

### 2. Install dbt Dependencies

```bash
dbt deps
```

### 3. Test Connection

```bash
dbt debug
```

### 4. Run the Pipeline

```bash
# Run all models
dbt run

# Run with tests
dbt run && dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Dictionary

### Key Fields

- **UID**: Unique Company Identification (format: CHE-###.###.###)
- **CHID**: Company House Identification
- **EHRAID**: Electronic HR Archive Identification
- **Legal Form ID**: Numeric identifier for company legal structure
- **SHAB**: Swiss Official Gazette of Commerce publications
- **LOADED_AT**: Timestamp when record was loaded into source system (TIMESTAMP_TZ)

### Legal Forms

Legal form mappings are maintained in the `legal_forms` seed with:
- German names (e.g., Aktiengesellschaft, Gesellschaft mit beschränkter Haftung)
- English translations (e.g., Stock Company, Limited Liability Company)
- Standard abbreviations (e.g., AG, GmbH)
- Detailed descriptions

This ensures consistency across all models and enables easy maintenance of legal form information.

## Data Quality

The project includes comprehensive data quality tests:

- **Uniqueness**: Company UIDs must be unique
- **Format validation**: UIDs must follow CHE-###.###.### format
- **Swiss ZIP codes**: Must be 4-digit format
- **Canton codes**: Must be valid Swiss canton abbreviations
- **Referential integrity**: Foreign key relationships maintained

## Usage Examples

### Active Companies by Canton
```sql
SELECT 
  canton,
  active_companies,
  total_companies,
  active_company_percentage
FROM {{ ref('gold_canton_statistics') }}
ORDER BY active_companies DESC;
```

### Recent Company Activity
```sql
SELECT 
  company_name,
  activity_type,
  shab_date,
  publication_message
FROM {{ ref('gold_company_activity') }}
WHERE recency_bucket = 'Last 30 days'
ORDER BY shab_date DESC;
```

### Company Overview
```sql
SELECT 
  company_name,
  legal_form_name,
  full_address,
  address_town,
  is_active,
  total_shab_publications,
  days_since_last_publication
FROM {{ ref('gold_company_overview') }}
WHERE is_active = TRUE
ORDER BY total_shab_publications DESC;
```

## API Reference

This project is based on the [ZEFIX Public API](https://www.zefix.admin.ch/ZefixPublicREST/swagger-ui/index.html#/Company/search).

## Maintenance

### Incremental Updates

All models use the `LOADED_AT` timestamp column for efficient incremental processing. This enables real-time processing of newly loaded data without the need for overlapping windows or complex change detection logic.

**Incremental Strategy:**
- **Silver Layer**: Uses `LOADED_AT > MAX(_loaded_at)` for precise incremental processing
- **Gold Layer**: Processes records based on upstream `LOADED_AT` timestamps
- **No Overlap**: Exact timestamp matching eliminates data duplication
- **Real-time Ready**: Immediate processing of new source data

### Monitoring

Key metrics to monitor:
- Number of records processed per layer
- Data freshness (time since last update)
- Test failure rates
- Processing time per model

## Contributing

1. Follow the established naming conventions
2. Add appropriate tests for new models
3. Update documentation for schema changes
4. Ensure data quality standards are maintained

## Contact

For questions about ZEFIX data, refer to the [official ZEFIX documentation](https://www.zefix.admin.ch/) or the Swiss Federal Statistical Office. 