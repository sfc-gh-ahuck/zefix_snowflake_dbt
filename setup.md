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
  WITH WAREHOUSE_SIZE = 'MEDIUM'
       AUTO_SUSPEND = 300
       AUTO_RESUME = TRUE;
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

## Step 4: Create Source Data

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

## Step 5: Create Workspace

### In Snowsight:
1. Navigate to **Projects** → **Worksheets**
2. Click **+ Worksheet** → **SQL Worksheet**
3. Create new workspace:
   - Choose **Git Repository**
   - Connect using the API integration created above
   - Clone your ZELIX dbt repository

### Workspace Configuration:
- Name: `zefix_dbt_workspace`
- Database: `zefix`
- Schema: `dev`
- Warehouse: `zefix_dbt_wh`

## Step 6: Prepare dbt Project

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

### Install Dependencies
In the workspace terminal or via SQL:
```sql
-- Create dbt project object first (temporary for deps)
CREATE OR REPLACE DBT PROJECT zefix.dev.zefix_temp_project 
  FROM snow://workspace/USER$<your_username>.PUBLIC."zefix_dbt_workspace"/versions/live/;

-- Install dependencies
EXECUTE DBT PROJECT zefix.dev.zefix_temp_project args='deps';
```

## Step 7: Deploy dbt Project Object

```sql
-- Create the main dbt project object
CREATE OR REPLACE DBT PROJECT zefix.dev.zefix_dbt_project 
  FROM snow://workspace/USER$<your_username>.PUBLIC."zefix_dbt_workspace"/versions/live/;

-- Verify creation
SHOW DBT PROJECTS LIKE 'zefix%';
DESCRIBE DBT PROJECT zefix.dev.zefix_dbt_project;
```

## Step 8: Execute dbt Commands

```sql
-- Run initial build
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='run --target dev';

-- Run tests
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='test';

-- Build everything (run + test)
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='build';
```

## Step 9: Schedule Execution

```sql
-- Create hourly task
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

**Task not running:**
```sql
-- Check task status
SHOW TASKS LIKE 'zefix%';
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('zefix.dev.zefix_dbt_hourly_run');
```

## Advanced Configuration

### Production Deployment
```sql
-- Create production project object
CREATE OR REPLACE DBT PROJECT zefix.prod.zefix_dbt_prod_project 
  FROM snow://workspace/USER$<your_username>.PUBLIC."zefix_dbt_workspace"/versions/live/;

-- Production task
CREATE OR REPLACE TASK zefix.prod.zefix_dbt_daily_prod
  WAREHOUSE = zefix_dbt_wh
  SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
  EXECUTE DBT PROJECT zefix.prod.zefix_dbt_prod_project args='build --target prod';
```

### CI/CD with Snowflake CLI
```bash
# Install Snowflake CLI
pip install snowflake-cli-labs

# Configure connection
snow connection add --connection-name zefix --account <account> --user <user>

# Deploy via CLI
snow dbt project create zefix_dbt_project --database zefix --schema dev
snow dbt project execute zefix_dbt_project --args "run --target prod"
```

## Next Steps

1. **Set up monitoring**: Configure alerts for task failures
2. **Implement CI/CD**: Use Snowflake CLI for automated deployments
3. **Add semantic views**: Enable natural language querying
4. **Create data shares**: Share insights across your organization

For detailed documentation, see [Snowflake's dbt projects documentation](https://docs.snowflake.com/LIMITEDACCESS/dbt-projects-on-snowflake). 