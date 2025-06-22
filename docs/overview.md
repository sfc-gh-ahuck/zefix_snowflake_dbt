{% docs zefix_overview %}

# ZEFIX Data Platform

Transform Swiss commercial register data into business-ready analytics.

## Architecture

**Medallion Pattern**: Bronze → Silver → Gold

### 🥉 Bronze Layer
- **Purpose**: Raw data sources
- **Content**: JSON data from ZEFIX API
- **Schema**: `bronze`

### 🥈 Silver Layer  
- **Purpose**: Cleaned and normalized data
- **Models**: 
  - `silver_companies` - Company master data
  - `silver_shab_publications` - Publication records
  - `silver_mutation_types` - Change events
- **Schema**: `silver`

### 🥇 Gold Layer
- **Purpose**: Business analytics
- **Models**:
  - `gold_company_overview` - Company profiles
  - `gold_company_activity` - Activity analysis
  - `gold_canton_statistics` - Geographic statistics
- **Schema**: `gold`

### 🌱 Seeds
- **Purpose**: Reference data
- **Content**: `legal_forms` - Swiss legal entity types

## Key Features

- **Incremental Processing**: Efficient updates using timestamps
- **Data Quality Testing**: Comprehensive validation
- **Swiss Business Intelligence**: Canton-level insights
- **Natural Language Queries**: Semantic views for Cortex Analyst

## Business Use Cases

- Company formation trends
- Geographic business distribution
- Legal form analysis
- Market research and due diligence

{% enddocs %}

{% docs incremental_strategy %}

## Incremental Processing

All models use `_loaded_at` timestamps for efficient incremental updates.

**Benefits:**
- Real-time processing
- Efficient resource usage
- No data duplication

{% enddocs %}

{% docs legal_forms_reference %}

## Legal Forms Reference

Swiss legal entity types with German/English names and abbreviations.

**Common Types:**
- AG (Aktiengesellschaft) - Stock Company
- GmbH (Gesellschaft mit beschränkter Haftung) - LLC
- Verein - Association
- Stiftung - Foundation

{% enddocs %} 