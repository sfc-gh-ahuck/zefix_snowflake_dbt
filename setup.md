# ZEFIX dbt Project Setup Guide

## Environment Configuration

Create a `.env` file in your project root with the following variables:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.region.cloud
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_DATABASE=ZEFIX
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

## Quick Start

1. **Install dbt dependencies**:
   ```bash
   dbt deps
   ```

2. **Test your connection**:
   ```bash
   dbt debug
   ```

3. **Run the complete pipeline**:
   ```bash
   dbt run
   ```

4. **Run tests**:
   ```bash
   dbt test
   ```

5. **Generate and serve documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Project Structure

```
zefix_data_platform/
├── models/
│   ├── sources.yml
│   ├── bronze/
│   │   ├── bronze_zefix_companies.sql
│   │   └── schema.yml
│   ├── silver/
│   │   ├── silver_companies.sql
│   │   ├── silver_shab_publications.sql
│   │   ├── silver_mutation_types.sql
│   │   └── schema.yml
│   └── gold/
│       ├── gold_company_overview.sql
│       ├── gold_company_activity.sql
│       ├── gold_canton_statistics.sql
│       └── schema.yml
├── macros/
│   └── test_data_quality.sql
├── tests/
│   └── test_company_uid_uniqueness.sql
├── dbt_project.yml
├── profiles.yml
├── packages.yml
└── README.md
```

## Prerequisites

- dbt-snowflake adapter
- Access to Snowflake with ZEFIX.PUBLIC.RAW table
- Required permissions for creating schemas and tables 