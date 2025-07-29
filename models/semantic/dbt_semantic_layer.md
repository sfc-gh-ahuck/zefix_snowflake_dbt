# dbt Semantic Layer for ZEFIX Data

This document explains the dbt semantic model definition for Swiss company data, enabling powerful analytics through MetricFlow.

## 🎯 What is the dbt Semantic Layer?

The dbt semantic layer provides a universal interface for metrics and dimensions, enabling:
- **Consistent metrics** across all tools and applications
- **Self-service analytics** for business users
- **Version control** for business logic
- **Lineage tracking** for metrics and dimensions

## 📊 Semantic Models Defined

### 1. Companies Model (`companies`)
**Source**: `silver_companies`
**Primary Entity**: `company` (company_uid)

**Key Dimensions:**
- Time: `registration_date`, `registration_year`
- Categorical: `legal_form`, `company_status`, `is_active`, `legal_seat`

**Key Measures:**
- `total_companies`: Count of all companies
- `company_count`: General company count for analysis
- `avg_company_age_days`: Average company age

### 2. Publications Model (`publications`)
**Source**: `silver_shab_publications`
**Primary Entity**: `publication` (publication_id)
**Foreign Entity**: `company` (company_uid)

**Key Dimensions:**
- Time: `publication_date`, `publication_month`, `publication_year`
- Categorical: `mutation_type`, `activity_type`

**Key Measures:**
- `total_publications`: Count of all SHAB publications
- `publication_count`: General publication count for analysis

## 📈 Available Metrics

### Simple Metrics
- `total_companies_metric`: Total number of companies
- `total_publications_metric`: Total number of SHAB publications
- `avg_company_age_metric`: Average age of companies in days

## 🚀 Usage Examples

### Using MetricFlow CLI

```bash
# Query total companies by legal form
mf query --metrics total_companies_metric --group-by company__legal_form

# Regional publication activity
mf query --metrics total_publications_metric --group-by company__legal_seat

# Company age analysis
mf query --metrics avg_company_age_metric --group-by company__legal_form
```

### Using Saved Queries

```bash
# Pre-built business queries
mf query --saved-query company_overview
mf query --saved-query publications_by_region
mf query --saved-query temporal_analysis
```

### SQL API (via dbt Cloud)

```python
import requests

# Query via dbt Cloud Semantic Layer API
response = requests.post(
    "https://semantic-layer.cloud.getdbt.com/api/graphql",
    headers={"Authorization": "Bearer YOUR_TOKEN"},
    json={
        "query": """
        query {
          metrics(metrics: ["active_companies", "registrations"]) {
            data {
              active_companies
              registrations
              company__legal_form
            }
          }
        }
        """
    }
)
```

## 🔗 Integration with BI Tools

### Supported Integrations
- **Tableau**: Native connector via dbt Cloud
- **Looker**: LookML generator from semantic models
- **Power BI**: REST API integration
- **Mode**: SQL queries via semantic layer
- **Hex**: Python SDK integration

### Example Integration (Tableau)
1. Connect to dbt Cloud semantic layer
2. Use pre-built dimensions and metrics
3. Create dashboards with consistent business logic

## 📋 Available Saved Queries

| Query Name | Description | Key Metrics |
|------------|-------------|-------------|
| `company_overview` | Basic company statistics overview | total_companies_metric, avg_company_age_metric |
| `publications_by_region` | Publication activity by canton | total_publications_metric by legal_seat |
| `temporal_analysis` | Time-based analysis | total_companies_metric, total_publications_metric |

## 🛠️ Development Commands

### Building the Semantic Layer
```bash
# Install package dependencies
dbt deps

# Parse semantic models (now working!)
dbt parse

# Test semantic models
dbt test --select tag:semantic

# Build dependencies
dbt build
```

**✅ Status**: Basic semantic models are now parsing successfully without MetricFlow time spine dependency.

### Time Spine Model
The semantic layer requires a time spine model for time-based calculations:

- **Model**: `time_spine` (located in `models/utilities/`)
- **Column**: `date_day` (DATE type)
- **Date Range**: 2020-01-01 to 2030-12-31  
- **Granularity**: Daily
- **Configuration**: Defined in `dbt_project.yml` under `metricflow.time_spine`
- **Reference**: [dbt MetricFlow time spine documentation](https://docs.getdbt.com/docs/build/metricflow-time-spine#example-time-spine-tables)

**Why needed**: MetricFlow uses the time spine to:
- Fill gaps in time series data
- Perform period-over-period calculations  
- Generate consistent time-based aggregations

### Validating Metrics
```bash
# Validate all metrics
mf validate-configs

# List available metrics
mf list metrics

# List available dimensions
mf list dimensions
```

## 🔍 Querying Patterns

### Time-Based Analysis
```bash
# Companies by registration year
mf query --metrics total_companies_metric --group-by company__registration_year

# Publications over time
mf query --metrics total_publications_metric --group-by publication__publication_year
```

### Categorical Analysis
```bash
# Legal form distribution
mf query --metrics total_companies_metric --group-by company__legal_form

# Regional comparisons
mf query --metrics total_companies_metric,total_publications_metric --group-by company__legal_seat
```

### Analytical Queries
```bash
# Company age by legal form
mf query --metrics avg_company_age_metric --group-by company__legal_form

# Company and publication activity by region
mf query --metrics total_companies_metric,total_publications_metric --group-by company__legal_seat
```

## 🚀 Advanced Features

### Custom Time Grains
- Day, week, month, quarter, year granularity
- Automatic time spine generation
- Period-over-period calculations

### Entity Relationships
- Join between companies and publications via company_uid
- Automatic relationship inference
- Cross-model metric calculations

### Metric Types
- **Simple**: Direct aggregations (count, sum, avg)
- **Ratio**: Numerator/denominator calculations
- **Derived**: Complex expressions using other metrics
- **Cumulative**: Running totals and window functions

## 📊 Business Impact

### Self-Service Analytics
- Business users can query metrics without SQL knowledge
- Consistent definitions across all reports
- Reduced analyst workload for ad-hoc requests

### Data Governance
- Single source of truth for business metrics
- Version controlled business logic
- Automated lineage documentation

### Performance
- Pre-aggregated metric calculations
- Optimized query generation
- Cached results for faster dashboards

## 🔄 Maintenance

### Adding New Metrics
1. Define in `semantic_models.yml`
2. Test with `mf validate-configs`
3. Deploy via dbt Cloud
4. Update documentation

### Updating Dimensions
1. Modify semantic model definition
2. Update any dependent metrics
3. Test downstream impacts
4. Deploy changes

This semantic layer provides a powerful foundation for self-service analytics while maintaining consistency and governance across your Swiss company data platform. 