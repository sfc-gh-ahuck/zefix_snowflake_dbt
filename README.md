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

### Legal Forms

1. Einzelunternehmen (Sole Proprietorship)
2. Kollektivgesellschaft (General Partnership)
3. Aktiengesellschaft (Corporation/Stock Company)
4. Kommanditgesellschaft (Limited Partnership)
5. Gesellschaft mit beschränkter Haftung (Limited Liability Company)
6. Genossenschaft (Cooperative)
7. Verein (Association)
8. Stiftung (Foundation)

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

The bronze layer includes change detection using content hashing. For production deployments, consider implementing incremental models to process only new or changed records.

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