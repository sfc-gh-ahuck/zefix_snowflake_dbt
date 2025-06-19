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

## Key Features

- **Incremental Processing**: All models support incremental updates based on `shabDate`
- **Data Quality**: Comprehensive testing and validation at each layer
- **Documentation**: Full documentation persisted in database comments
- **Merge Strategy**: Proper handling of updates and inserts for incremental models

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

{% docs shabdate_incremental %}

## Incremental Processing Strategy

All models in this project use `shabDate` (SHAB publication date) as the incremental key with a **one-day overlap** for data resilience.

### Why One-Day Overlap?
- Ensures data consistency during API updates
- Handles late-arriving records gracefully  
- Provides resilience against data quality issues
- Maintains referential integrity across layers

{% enddocs %}

{% docs legal_forms %}

## Swiss Legal Forms

| ID | German Name | English Translation | Abbreviation |
|----|-------------|-------------------|--------------|
| 1  | Einzelunternehmen | Sole Proprietorship | - |
| 2  | Kollektivgesellschaft | General Partnership | - |
| 3  | Aktiengesellschaft | Stock Company/Corporation | AG |
| 4  | Kommanditgesellschaft | Limited Partnership | - |
| 5  | Gesellschaft mit beschränkter Haftung | Limited Liability Company | GmbH |
| 6  | Genossenschaft | Cooperative | - |
| 7  | Verein | Association | - |
| 8  | Stiftung | Foundation | - |

{% enddocs %} 