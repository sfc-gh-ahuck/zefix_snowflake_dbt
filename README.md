# ZEFIX Data Platform

A comprehensive dbt project for analyzing Swiss commercial register data using Snowflake's native dbt projects feature. This platform transforms raw ZEFIX (Swiss Federal Office of Justice) data into actionable business intelligence through a modern data lakehouse architecture.

## 🔍 About ZEFIX Data

[ZEFIX](https://www.zefix.ch/en/search/shab/welcome) is the Swiss Central Business Name Index maintained by the Federal Office of Justice. It contains comprehensive information about:

- **Company Registry**: All registered companies in Switzerland with unique identifiers (UID format: CHE-###.###.###)
- **SHAB Publications**: Swiss Official Gazette of Commerce entries for company changes, registrations, and dissolutions
- **Legal Forms**: AG (Stock Corporation), GmbH (Limited Liability Company), and other business entity types
- **Geographic Distribution**: Company locations across all 26 Swiss cantons
- **Business Activities**: Company status changes, mergers, acquisitions, and other corporate events

### Data Characteristics
- **Coverage**: 800,000+ active companies and historical records
- **Update Frequency**: Daily SHAB publications
- **Data Quality**: Official government source with standardized formats
- **Languages**: Multi-language support (German, French, Italian, Romansh)
- **Public Access**: Available via REST API for research and commercial use

## 🎯 Use Cases & Applications

This project delivers ready-to-use analytics for diverse business needs:

### **📊 Business Intelligence Dashboards**
- **Canton Performance Analytics**: Compare business activity across Swiss regions using `gold_canton_statistics`
- **Company Activity Monitoring**: Track recent registrations, changes, and dissolutions via `gold_company_activity`
- **Market Share Analysis**: Analyze competitive landscapes by legal form and geography
- **Business Trend Reporting**: Identify growth patterns using time-series models

### **🔍 Due Diligence & Risk Assessment**
- **Company Profile Verification**: Comprehensive company background checks using `gold_company_overview`
- **Business Relationship Mapping**: Trace corporate connections and ownership structures
- **Compliance Monitoring**: Track company status changes for regulatory requirements
- **Portfolio Company Analysis**: Monitor investments and subsidiaries

### **📈 Economic Research & Analytics**
- **Regional Economic Studies**: Leverage pre-built canton-level aggregations
- **Business Formation Analysis**: Study entrepreneurship patterns using SHAB publication data
- **Industry Concentration Studies**: Analyze market dynamics by legal form and location
- **Economic Impact Assessment**: Measure policy effects on business registrations

### **🤖 AI-Powered Insights**
- **Natural Language Querying**: Ask business questions in plain English using semantic views
- **Automated Report Generation**: Generate insights using Cortex Analyst integration
- **Anomaly Detection**: Identify unusual business activity patterns
- **Predictive Analytics**: Forecast business trends using historical data patterns

### **🏛️ Government & Public Sector**
- **Economic Development Planning**: Data-driven regional development strategies
- **Business Support Programs**: Identify target companies for assistance programs
- **Statistical Reporting**: Generate official statistics using standardized models
- **Policy Impact Analysis**: Measure effects of regulatory changes on business activity

## 🏗️ Architecture

**Native Snowflake dbt Project** using workspaces and SQL commands

- **Bronze**: Raw data sources (JSON from ZEFIX API)
- **Silver**: Cleaned and normalized data with standardized schemas
- **Gold**: Business-ready analytics tables optimized for querying
- **Semantic**: Natural language views for Cortex Analyst integration

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ZEFIX API     │───▶│     Bronze      │───▶│     Silver      │
│ (JSON Sources)  │    │   Raw Tables    │    │ Cleaned & Typed │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐           │
│   Semantic      │◀───│      Gold       │◀──────────┘
│ (Cortex Views)  │    │  Analytics Ready│
└─────────────────┘    └─────────────────┘
```

## 📊 Key Models

### Silver Layer
| Model | Description | Records |
|-------|-------------|---------|
| `silver_companies` | Cleaned company master data with standardized UIDs | 800K+ |
| `silver_shab_publications` | SHAB publication records with parsed content | 2M+ |
| `silver_mutation_types` | Company change events and business activities | 500K+ |
| `silver_legal_forms` | Business entity types and classifications | 50+ |

### Gold Layer
| Model | Description | Refresh |
|-------|-------------|---------|
| `gold_company_overview` | Comprehensive company profiles with enriched metadata | Daily |
| `gold_company_activity` | Company activity analysis with trend indicators | Hourly |
| `gold_canton_statistics` | Canton-level business metrics and aggregations | Daily |

### Semantic Layer
| View | Description | Cortex Ready |
|------|-------------|--------------|
| `sem_company_overview` | Natural language company information queries | ✅ |
| `sem_publication_activity` | SHAB publication trends and patterns | ✅ |
| `sem_business_changes` | Company mutation analysis | ✅ |
| `sem_geographic_analysis` | Canton-level insights and comparisons | ✅ |

## ✨ Platform Features

### **🔥 Native Snowflake Integration**
- **Zero Installation**: No external dbt setup required
- **Workspace IDE**: Web-based development with Git integration
- **SQL Management**: Create and manage projects entirely with SQL
- **Auto-scaling**: Native Snowflake compute and storage scaling

### **🚀 Modern Data Engineering**
- **Incremental Models**: Efficient processing of large datasets
- **Data Quality Tests**: Comprehensive validation framework
- **Lineage Tracking**: Full data flow documentation
- **Version Control**: Git-integrated workspace management

### **📈 Analytics & BI Ready**
- **Semantic Views**: Natural language querying capability
- **Cortex Analyst**: AI-powered data exploration
- **Performance Optimized**: Clustered and partitioned tables
- **Real-time Refresh**: Configurable update schedules

### **🔧 DevOps & Automation**
- **Task Scheduling**: Native Snowflake task orchestration
- **CI/CD Integration**: Snowflake CLI support for automated deployments
- **Environment Management**: Dev/staging/prod workflow
- **Monitoring & Alerting**: Built-in observability features

### **🛡️ Enterprise Features**
- **Security**: Row-level security and access controls
- **Data Governance**: Automated data classification and tagging
- **Compliance**: GDPR-compliant data handling
- **Audit Logging**: Complete execution and access history

### **🌐 Extensibility**
- **API Integration**: REST endpoints for external applications
- **Data Sharing**: Snowflake secure data sharing capabilities
- **Custom Macros**: Reusable transformation logic
- **Plugin Architecture**: Easy integration with external tools

## 🚀 Quick Start

> 📚 **Detailed Setup**: See [setup.md](./setup.md) for complete installation instructions.

### Prerequisites
- Snowflake account with personal databases enabled
- ACCOUNTADMIN privileges (for initial setup)
- Git repository access

### 1. Basic Setup
```sql
-- Enable personal databases
ALTER ACCOUNT SET ENABLE_PERSONAL_DATABASE = TRUE;

-- Create infrastructure
CREATE DATABASE zefix;
CREATE SCHEMA zefix.dev;
CREATE WAREHOUSE zefix_dbt_wh WITH WAREHOUSE_SIZE = 'XSMALL';
```

### 2. Create API Integrations
```sql
-- Git integration
CREATE API INTEGRATION zefix_git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com')
  ENABLED = TRUE;

-- External access for dependencies
CREATE EXTERNAL ACCESS INTEGRATION zefix_dbt_deps_integration
  ALLOWED_NETWORK_RULES = (zefix_dbt_deps_network_rule)
  ENABLED = TRUE;
```

### 3. Deploy dbt Project
```sql
-- Create project from workspace
CREATE DBT PROJECT zefix.dev.zefix_dbt_project 
  FROM snow://workspace/USER$<username>.PUBLIC."zefix_workspace"/versions/live/;

-- Run project
EXECUTE DBT PROJECT zefix.dev.zefix_dbt_project args='build';
```

> 🔧 **Full Instructions**: For step-by-step setup with troubleshooting, see [setup.md](./setup.md)

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
SELECT canton, active_companies, total_companies,
       ROUND(active_companies::FLOAT / total_companies * 100, 2) as activity_rate
FROM zefix.dev.gold_canton_statistics
ORDER BY active_companies DESC;
```

### Recent Company Activity
```sql
SELECT company_name, activity_type, shab_date, canton
FROM zefix.dev.gold_company_activity
WHERE recency_bucket = 'Last 30 days'
ORDER BY shab_date DESC;
```

### Industry Distribution
```sql
SELECT legal_form, COUNT(*) as company_count,
       ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER() * 100, 2) as percentage
FROM zefix.dev.gold_company_overview
WHERE is_active = TRUE
GROUP BY legal_form
ORDER BY company_count DESC;
```

## 🔍 Monitoring & Observability

### Enable Advanced Logging
```sql
ALTER SCHEMA zefix.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA zefix.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA zefix.dev SET METRIC_LEVEL = 'ALL';
```

### Monitoring Dashboards
- **Query History**: Snowsight → Admin → Query History
- **Task History**: Execution logs for scheduled runs
- **Resource Usage**: Compute and storage utilization
- **Data Quality**: Test results and model freshness

## 🧠 Semantic Views & AI Integration

### Cortex Analyst Ready
Natural language querying with pre-built semantic views:

```sql
-- Example natural language queries
SELECT * FROM zefix.dev.sem_company_overview 
WHERE semantic_search('tech companies in Zurich');

SELECT * FROM zefix.dev.sem_geographic_analysis
WHERE semantic_search('canton business formation trends');
```

### Semantic Models
- **Company Profiles**: Basic company information and status
- **Business Activity**: SHAB publication trends and patterns
- **Geographic Insights**: Regional business distributions
- **Temporal Analysis**: Time-based company lifecycle patterns

## 📝 Data Dictionary

### Key Entities
- **UID**: Unique Company Identifier (format: CHE-###.###.###)
- **SHAB**: Swiss Official Gazette of Commerce
- **Legal Forms**: AG (Stock Corp), GmbH (LLC), Einzelunternehmen (Sole Prop)
- **Cantons**: 26 Swiss states/regions (ZH, BE, GE, etc.)
- **Company Status**: ACTIVE, DISSOLVED, LIQUIDATION, etc.

### Important Fields
- **company_uid**: Primary business identifier
- **shab_date**: Publication date in official gazette
- **legal_seat**: Official company domicile
- **mutation_type**: Type of business change event

## 🔗 Data Sources & Attribution

### Primary Source
- **[ZEFIX](https://www.zefix.ch)**: Swiss Central Business Name Index
- **Provider**: Federal Office of Justice (FOJ)
- **License**: Open Government Data
- **API Documentation**: [ZEFIX API](https://www.zefix.ch/en/search/shab/welcome)

### Data Updates
- **Frequency**: Daily SHAB publications
- **Latency**: 24-48 hours from official publication
- **Completeness**: All registered Swiss entities

## 🔄 CI/CD Integration

### Snowflake CLI
```bash
# Deploy using Snowflake CLI
snow dbt project create --database zefix --schema dev
snow dbt project execute --args "run --target prod"
```

### GitHub Actions
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

## ⚠️ Requirements & Limitations

### Technical Requirements
- **dbt Core**: 1.8.9+ (managed by Snowflake)
- **Personal databases**: Must be enabled at account level
- **API integrations**: Required for Git and external dependencies
- **Workspace limits**: 20,000 files per project maximum

### Resource Considerations
- **Compute**: Minimum XSMALL warehouse recommended
- **Storage**: ~10GB for full historical dataset
- **Concurrency**: Native Snowflake scaling handles concurrent access

### Data Limitations
- **Historical Scope**: SHAB publications from 2020 onwards
- **Update Frequency**: Not real-time (24-48h delay)
- **Language Support**: Primarily German with limited translations

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](./CONTRIBUTING.md) for details on:
- Development workflow
- Code standards
- Testing requirements
- Documentation updates

## 📄 License

This project is licensed under the MIT License - see [LICENSE](./LICENSE) file for details.

Data provided by ZEFIX is available under Open Government Data license.

---

Built with [Snowflake's native dbt projects](https://docs.snowflake.com/LIMITEDACCESS/dbt-projects-on-snowflake) feature. 