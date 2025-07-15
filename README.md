# ZEFIX Data Platform

A dbt project for analyzing Swiss commercial register data using Snowflake's native dbt projects feature.

## 🏗️ Architecture

**Native Snowflake dbt Project** using workspaces and SQL commands

- **Bronze**: Raw data sources (JSON from ZEFIX API)
- **Silver**: Cleaned and normalized data
- **Gold**: Business-ready analytics tables
- **Semantic**: Natural language views for Cortex Analyst

## 📊 Key Models

### Silver Layer
| Model | Description |
|-------|-------------|
| `silver_companies` | Cleaned company master data |
| `silver_shab_publications` | SHAB publication records |
| `silver_mutation_types` | Company change events |

### Gold Layer
| Model | Description |
|-------|-------------|
| `gold_company_overview` | Comprehensive company profiles |
| `gold_company_activity` | Company activity analysis |
| `gold_canton_statistics` | Canton-level business metrics |

## 🚀 Quick Start

### Prerequisites
- Snowflake account with personal databases enabled
- ACCOUNTADMIN privileges (for setup)
- Git repository access

### 1. Setup Snowflake Environment
```sql
-- Enable personal databases (requires ACCOUNTADMIN)
ALTER ACCOUNT SET ENABLE_PERSONAL_DATABASE = TRUE;

-- Create database and schema
CREATE DATABASE zefix;
CREATE SCHEMA zefix.dev;
CREATE SCHEMA zefix.prod;

-- Create warehouse
CREATE WAREHOUSE zefix_dbt_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
       AUTO_SUSPEND = 300
       AUTO_RESUME = TRUE;
```

### 2. Create API Integration for Git
```sql
CREATE OR REPLACE API INTEGRATION zefix_git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com')
  ENABLED = TRUE;
```

### 3. Create External Access Integration for Dependencies
```sql
-- Network rule for dbt dependencies
CREATE OR REPLACE NETWORK RULE zefix_dbt_deps_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com:443', 'github.com:443', 'raw.githubusercontent.com:443');

-- External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION zefix_dbt_deps_integration
  ALLOWED_NETWORK_RULES = (zefix_dbt_deps_network_rule)
  ENABLED = TRUE;
```

### 4. Create Workspace in Snowsight
1. Open Snowsight
2. Go to Projects → Worksheets
3. Create new workspace integrated with your Git repository
4. Clone this repository into the workspace

### 5. Deploy dbt Project Object
```sql
-- Create dbt project object from workspace
CREATE OR REPLACE DBT PROJECT zefix.dev.zefix_dbt_project 
  FROM snow://workspace/USER$<username>.PUBLIC."zefix_workspace"/versions/live/;

-- Install dependencies
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='deps';

-- Run the project
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='run --target dev';
```

### 6. Schedule with Tasks
```sql
-- Create scheduled task
CREATE OR REPLACE TASK zefix.dev.zefix_dbt_hourly_run
  WAREHOUSE = zefix_dbt_wh
  SCHEDULE = '60 MINUTE'
AS
  EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='run --target dev';

-- Resume task
ALTER TASK zefix.dev.zefix_dbt_hourly_run RESUME;
```

## 🔧 Native Snowflake Commands

### Execute dbt Commands
```sql
-- Run all models
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='run';

-- Run specific model
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='run --select gold_company_overview';

-- Run tests
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='test';

-- Build (run + test)
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='build';
```

### Manage dbt Project Objects
```sql
-- Show dbt projects
SHOW DBT PROJECTS;

-- Describe project
DESCRIBE DBT PROJECT zefix.dev.zefix_dbt_project;

-- List project files
LIST 'snow://dbt/zefix.dev.zefix_dbt_project/versions/last/';
```

## 📈 Sample Queries

### Active Companies by Canton
```sql
SELECT canton, active_companies, total_companies
FROM zefix.dev.gold_canton_statistics
ORDER BY active_companies DESC;
```

### Recent Company Activity
```sql
SELECT company_name, activity_type, shab_date
FROM zefix.dev.gold_company_activity
WHERE recency_bucket = 'Last 30 days'
ORDER BY shab_date DESC;
```

## 🔍 Monitoring & Observability

Enable logging and tracing:
```sql
ALTER SCHEMA zefix.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA zefix.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA zefix.dev SET METRIC_LEVEL = 'ALL';
```

View execution history:
- Query History in Snowsight
- Task History for scheduled runs
- Event tables for detailed logging

## 🧠 Semantic Views

Natural language querying with Cortex Analyst:
- `sem_company_overview` - Basic company information
- `sem_publication_activity` - SHAB publication trends
- `sem_business_changes` - Company mutations
- `sem_geographic_analysis` - Canton-level insights

## 📝 Data Dictionary

**Key Fields:**
- **UID**: Company ID (format: CHE-###.###.###)
- **SHAB**: Swiss Official Gazette publications
- **Legal Form**: Company type (AG, GmbH, etc.)
- **Canton**: Swiss state/region

## 🔗 Data Source

[ZEFIX Public API](https://www.zefix.ch/en/search/shab/welcome) - Swiss Federal Office of Justice

## 📚 Features

- **Native Snowflake Integration**: No external dbt installation required
- **Workspace IDE**: Web-based development environment
- **Git Integration**: Version control built-in
- **SQL Management**: Create and manage projects with SQL
- **Task Scheduling**: Native Snowflake scheduling
- **Observability**: Built-in logging, tracing, and metrics
- **Cortex Integration**: AI-powered semantic views

## 🔄 CI/CD Integration

Use Snowflake CLI for CI/CD workflows:
```bash
# Deploy using Snowflake CLI
snow dbt project create --database zefix --schema dev
snow dbt project execute --args "run --target prod"
```

## ⚠️ Requirements

- dbt Core 1.8.9+ (managed by Snowflake)
- Personal databases enabled
- API integration for Git repository
- External access integration for dependencies
- Workspace limit: 20,000 files per project

Built with [Snowflake's native dbt projects](https://docs.snowflake.com/LIMITEDACCESS/dbt-projects-on-snowflake) feature. 