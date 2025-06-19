{% docs zefix_overview %}

# ZEFIX Data Platform

This dbt project transforms and analyzes data from the Swiss Federal Office of Justice (ZEFIX) Commercial Register.

## Data Architecture

The project follows a medallion architecture pattern:

### 🥉 Bronze Layer
- **Purpose**: Raw source data definitions only
- **Models**: None (sources only)
- **Schema**: `bronze`
- **Description**: Contains source definitions for raw ZEFIX data - truly raw layer with no transformations

### 🥈 Silver Layer  
- **Purpose**: First transformation layer, data cleaning, and normalization
- **Models**: 
  - `silver_zefix_companies_raw` - Raw extraction from JSON variants (append-only)
  - `silver_companies` - Cleaned and deduplicated company master data
  - `silver_shab_publications` - Normalized SHAB publication records
  - `silver_mutation_types` - Individual mutation/change types
- **Schema**: `silver`
- **Description**: Applies JSON extraction, business rules, data quality checks, and deduplication

### 🥇 Gold Layer
- **Purpose**: Business intelligence and analytics
- **Models**:
  - `gold_company_overview` - Comprehensive company profiles with metrics
  - `gold_company_activity` - Activity analysis and change tracking
  - `gold_canton_statistics` - Canton-level aggregated statistics
- **Schema**: `gold`
- **Description**: Provides business-ready datasets for reporting and analysis

### 🌱 Seeds
- **Purpose**: Reference data and lookup tables
- **Seeds**:
  - `legal_forms` - Swiss legal form mappings with German/English names and abbreviations
- **Description**: Static reference data used across multiple models

## Key Features

- **Incremental Processing**: All models support incremental updates based on `LOADED_AT` timestamp
- **Data Quality**: Comprehensive testing and validation at each layer
- **Documentation**: Full documentation persisted in database comments
- **Merge Strategy**: Proper handling of updates and inserts for incremental models
- **Reference Data**: Centralized legal form mappings in seeds for consistency

## Data Sources

- **ZEFIX API**: Swiss Commercial Register data from the Federal Office of Justice
- **SHAB**: Swiss Official Gazette of Commerce publications

## Business Context

This platform enables analysis of:
- Company formations and dissolutions
- Business activity trends by canton
- Legal form distributions
- Merger and acquisition activity
- Regulatory compliance patterns

{% enddocs %}

{% docs loaded_at_incremental %}

## Incremental Processing Strategy

All models in this project use `LOADED_AT` (timestamp when record was loaded into source system) as the incremental key for efficient processing.

### Why LOADED_AT?
- **Real-time Processing**: Enables immediate processing of newly loaded data
- **Reliable Ordering**: Timestamp provides clear chronological ordering
- **Efficient Queries**: Simple timestamp comparison for incremental logic
- **Data Freshness**: Ensures latest data is always processed first
- **No Overlap Needed**: Exact timestamp matching eliminates data duplication

### Implementation Pattern
```sql
{% if is_incremental() %}
  AND loaded_at > (
    SELECT MAX(_loaded_at) 
    FROM {{ this }}
  )
{% endif %}
```

{% enddocs %}

{% docs legal_forms_seed %}

## Legal Forms Reference Data

The `legal_forms` seed contains the official Swiss legal form mappings used throughout the project.

### Usage
This seed is joined to company data in the Silver layer to provide:
- German legal form names (`legal_form_name_de`)
- English translations (`legal_form_name_en`) 
- Standard abbreviations (`abbreviation`)
- Detailed descriptions (`description`)

### Benefits
- **Consistency**: Single source of truth for legal form names
- **Maintainability**: Easy to update legal form information
- **Multilingual**: Supports both German and English naming
- **Extensibility**: Can be easily extended with additional metadata

{% enddocs %} 