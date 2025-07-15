# ZEFIX dbt Projects on Snowflake Setup

Complete setup guide for running the ZEFIX dbt project using Snowflake's native dbt projects feature.

## Prerequisites

- Snowflake account with ACCOUNTADMIN role
- Git repository access
- Snowsight access

## Step 1: Account Configuration

### Enable Personal Databases
```sql
-- Required for workspaces (run as ACCOUNTADMIN)
ALTER ACCOUNT SET ENABLE_PERSONAL_DATABASE = TRUE;
```

### Create Infrastructure
```sql
-- Create main database
CREATE DATABASE IF NOT EXISTS zefix
  COMMENT = 'Swiss company registry data platform';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS zefix.dev
  COMMENT = 'Development environment';
CREATE SCHEMA IF NOT EXISTS zefix.prod
  COMMENT = 'Production environment';
CREATE SCHEMA IF NOT EXISTS zefix.raw
  COMMENT = 'Raw data staging';

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS zefix_dbt_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
       AUTO_SUSPEND = 300
       AUTO_RESUME = TRUE
       COMMENT = 'Warehouse for ZEFIX dbt project';
```

## Step 2: API Integration Setup

### Git Repository Integration
```sql
CREATE OR REPLACE API INTEGRATION zefix_git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com')
  ENABLED = TRUE
  COMMENT = 'API integration for ZEFIX dbt Git repository';
```

### External Access for Dependencies
```sql
-- Network rule for dbt package dependencies
CREATE OR REPLACE NETWORK RULE zefix_dbt_deps_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com:443', 'github.com:443', 'raw.githubusercontent.com:443')
  COMMENT = 'Network access for dbt dependencies';

-- External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION zefix_dbt_deps_integration
  ALLOWED_NETWORK_RULES = (zefix_dbt_deps_network_rule)
  ENABLED = TRUE
  COMMENT = 'External access for dbt package installation';
```

## Step 3: Enable Observability

```sql
-- Enable logging and tracing
ALTER SCHEMA zefix.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA zefix.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA zefix.dev SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA zefix.prod SET LOG_LEVEL = 'INFO';
ALTER SCHEMA zefix.prod SET TRACE_LEVEL = 'ON_EVENT';
ALTER SCHEMA zefix.prod SET METRIC_LEVEL = 'ALL';
```

## Step 4: Create Workspace in Snowsight

### In Snowsight:
1. Navigate to **Projects** → **Worksheets**
2. Click **+ Worksheet** → **SQL Worksheet**
3. Create new workspace:
   - Choose **Git Repository**
   - Connect using the API integration created above
   - Clone your ZEFIX dbt repository

### Workspace Configuration:
- Name: `zefix_dbt_workspace`
- Database: `zefix`
- Schema: `dev`
- Warehouse: `zefix_dbt_wh`

## Step 5: Prepare dbt Project

### Update profiles.yml
Ensure your `profiles.yml` exists in project root:
```yaml
zefix:
  outputs:
    dev:
      type: snowflake
      # Credentials managed by Snowflake - no need to specify
      database: zefix
      schema: dev
      warehouse: zefix_dbt_wh
    prod:
      type: snowflake
      database: zefix
      schema: prod
      warehouse: zefix_dbt_wh
  target: dev
```

## Step 6: Deploy dbt Project Object

```sql
-- Create dbt project object from workspace
CREATE OR REPLACE DBT PROJECT zefix.dev.zefix_dbt_project 
  FROM snow://workspace/USER$<username>.PUBLIC."zefix_workspace"/versions/live/;

-- Install dependencies
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='deps';

-- Run the project
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='run --target dev';

-- Verify creation
SHOW DBT PROJECTS LIKE 'zefix%';
DESCRIBE DBT PROJECT zefix.dev.zefix_dbt_project;
```

## Step 7: Execute dbt Commands

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

## Step 8: Schedule Execution

```sql
-- Create scheduled task
CREATE OR REPLACE TASK zefix.dev.zefix_dbt_hourly_run
  WAREHOUSE = zefix_dbt_wh
  SCHEDULE = '60 MINUTE'
  COMMENT = 'Hourly execution of ZEFIX dbt project'
AS
  EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='run --target dev';

-- Create test task (runs after main task)
CREATE OR REPLACE TASK zefix.dev.zefix_dbt_hourly_test
  WAREHOUSE = zefix_dbt_wh
  SCHEDULE = '60 MINUTE'
  AFTER zefix.dev.zefix_dbt_hourly_run
  COMMENT = 'Hourly testing of ZEFIX dbt project'
AS
  EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='test';

-- Resume tasks
ALTER TASK zefix.dev.zefix_dbt_hourly_run RESUME;
ALTER TASK zefix.dev.zefix_dbt_hourly_test RESUME;
```

## Step 9: Create Source Data (Optional for Testing)

```sql
-- Create raw tables for ZEFIX data
CREATE TABLE IF NOT EXISTS zefix.raw.zefix_companies_raw (
  company_uid STRING,
  company_name STRING,
  company_status STRING,
  is_active BOOLEAN,
  legal_form_id INTEGER,
  legal_seat STRING,
  address_zip_code STRING,
  address_town STRING,
  shab_date DATE,
  _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data (replace with actual data loading)
INSERT INTO zefix.raw.zefix_companies_raw VALUES
  ('CHE-123.456.789', 'Swiss Tech Solutions AG', 'ACTIVE', TRUE, 1, 'Zürich', '8001', 'Zürich', '2024-01-15', CURRENT_TIMESTAMP()),
  ('CHE-987.654.321', 'Alpine Consulting GmbH', 'ACTIVE', TRUE, 2, 'Bern', '3003', 'Bern', '2024-01-10', CURRENT_TIMESTAMP());
```

## Step 10: Verification

### Check Models
```sql
-- Verify models exist
SELECT * FROM zefix.dev.silver_companies LIMIT 5;
SELECT * FROM zefix.dev.gold_company_overview LIMIT 5;

-- Check row counts
SELECT 
  'silver_companies' as model, COUNT(*) as rows FROM zefix.dev.silver_companies
UNION ALL
SELECT 
  'gold_company_overview' as model, COUNT(*) as rows FROM zefix.dev.gold_company_overview;
```

### Monitor Execution
```sql
-- View task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'zefix%'
ORDER BY SCHEDULED_TIME DESC;

-- Check dbt project status
LIST 'snow://dbt/zefix.dev.zefix_dbt_project/versions/last/';

-- Manage dbt Project Objects
SHOW DBT PROJECTS;
DESCRIBE DBT PROJECT zefix.dev.zefix_dbt_project;
LIST 'snow://dbt/zefix.dev.zefix_dbt_project/versions/last/';
```

## CI/CD Integration

### Using Snowflake CLI
Use Snowflake CLI for automated deployment workflows:

```bash
# Deploy using Snowflake CLI
snow dbt project create --database zefix --schema dev
snow dbt project execute --args "run --target prod"
```

### GitHub Actions Example
```yaml
name: Deploy ZEFIX dbt Project
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to Snowflake
        run: |
          snow dbt project create --database zefix --schema prod
          snow dbt project execute --args "build --target prod"
```

## Troubleshooting

### Common Issues

**Personal databases not enabled:**
```sql
-- Check account parameter
SHOW PARAMETERS LIKE 'ENABLE_PERSONAL_DATABASE' IN ACCOUNT;
```

**API integration not working:**
```sql
-- Verify integrations
SHOW INTEGRATIONS;
DESCRIBE INTEGRATION zefix_git_integration;
```

**Dependencies failing:**
```sql
-- Check external access
SHOW EXTERNAL ACCESS INTEGRATIONS;
SHOW NETWORK RULES;
```

**Workspace file limits:**
- Maximum 20,000 files per workspace
- Consider .gitignore for large dependency folders

**Task execution issues:**
```sql
-- Check task status
SHOW TASKS LIKE 'zefix%';
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
WHERE NAME LIKE 'zefix%' 
ORDER BY SCHEDULED_TIME DESC LIMIT 10;
``` 