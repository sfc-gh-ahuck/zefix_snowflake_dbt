# ZEFIX Semantic Layer

This directory contains two approaches for semantic data modeling:

## 🎯 Dual Semantic Approach

### 1. **dbt Semantic Layer** (`semantic_models.yml`)
Modern semantic modeling using dbt's official semantic layer with MetricFlow:
- ✅ **Universal metrics** across all BI tools
- ✅ **Self-service analytics** for business users  
- ✅ **API-driven** integration
- ✅ **Version controlled** business logic

### 2. **Cortex Analyst Views** (`sem_*.sql`)
Snowflake-native semantic views for natural language querying:
- ✅ **Natural language** queries in Snowsight
- ✅ **Cortex Analyst** AI-powered insights
- ✅ **Zero-setup** business intelligence
- ✅ **Custom materialization** for Snowflake

### 3. **Native Snowflake Semantic Views** (`*_semantic_view.sql`)
Auto-generated Snowflake semantic views using the [dbt_semantic_view_converter](https://github.com/sfc-gh-ahuck/dbt_semantic_view_converter.git) package:
- ✅ **Native Snowflake objects** created from dbt semantic models
- ✅ **BI tool integration** with pre-defined business logic
- ✅ **Version controlled** semantic definitions
- ✅ **Automatic generation** from existing semantic models

## 📊 dbt Semantic Layer (Recommended)

### Key Benefits
- **Cross-platform consistency**: Same metrics in Tableau, Looker, Power BI
- **Self-service enabled**: Business users query without SQL
- **API-first design**: Programmatic access to all metrics
- **Enterprise governance**: Version control + lineage tracking

### Available Models
| Model | Entities | Key Metrics |
|-------|----------|-------------|
| `companies` | company | active_companies, total_companies, avg_company_age |
| `publications` | publication, company | registrations, dissolutions, management_changes |

### Usage Examples
```bash
# Query active companies by legal form
mf query --metrics active_companies --group-by company__legal_form

# Monthly business activity trends
mf query --saved-query monthly_business_activity

# Regional analysis
mf query --metrics active_companies,business_activity_ratio --group-by company__legal_seat
```

**📚 Documentation**: See [dbt_semantic_layer.md](./dbt_semantic_layer.md) for complete usage guide.

## 🧠 Cortex Analyst Views

### Natural Language Querying
Enable business users to ask questions in plain English:
- *"How many active companies are there?"*
- *"Show me company formations this year"*
- *"Which canton has the most AG companies?"*

### Available Views
| View | Purpose | Example Questions |
|------|---------|-------------------|
| `sem_company_overview` | Basic company info | "How many active companies?" |
| `sem_publication_activity` | SHAB publications | "Show formation trends" |
| `sem_business_changes` | Company mutations | "How many management changes?" |
| `sem_geographic_analysis` | Location insights | "Which canton has most companies?" |

### Usage in Snowsight
1. Open Snowflake Cortex Analyst
2. Reference semantic views by name
3. Ask natural language questions
4. Get AI-generated insights

## 🚀 Getting Started

### Option 1: dbt Semantic Layer (Modern Approach)
```bash
# 1. Install dependencies
dbt deps

# 2. Parse semantic models (now working!)
dbt parse

# 3. Build semantic models
dbt build

# 4. (Optional) Validate with MetricFlow if configured
# mf validate-configs

# 5. (Optional) Query via MetricFlow if configured
# mf query --metrics total_companies_metric --group-by company__legal_form
```

**✅ Status**: Basic semantic models are now parsing successfully! MetricFlow features are optional.

### Option 2: Cortex Analyst Views (Snowflake Native)
```bash
# Build semantic views
dbt run --models models/semantic/

# Query in Snowsight Cortex Analyst
# Reference: sem_company_overview, sem_publication_activity, etc.
```

### Option 3: Native Snowflake Semantic Views (Recommended for BI)
```bash
# 1. Install dependencies (includes semantic view converter)
dbt deps

# 2. Build semantic view models
dbt run --models tag:semantic

# 3. Query directly in Snowflake or BI tools
# Reference: semantic_layer.companies_semantic_view, semantic_layer.publications_semantic_view
```

**✅ Status**: Auto-generates native Snowflake semantic views from dbt semantic models! Perfect for BI tool integration.

## 🔧 Development

### Build All Semantic Assets
```bash
# Build time spine first (required for dbt semantic layer)
dbt run --select time_spine

# Build both approaches
dbt run --models models/semantic/
dbt build --select +semantic_models.yml
```

### Time Spine Requirement
The dbt semantic layer requires a **time spine model** (`utilities/time_spine.sql`) that provides:
- Continuous daily dates from 2020-2030 (column: `date_day`)
- Foundation for time-based metric calculations
- Configured in `dbt_project.yml` under `metricflow.time_spine`
- Built following [official dbt documentation](https://docs.getdbt.com/docs/build/metricflow-time-spine#example-time-spine-tables)

### Creating New Metrics (dbt Semantic Layer)
1. Add to `semantic_models.yml`
2. Define measures and dimensions
3. Test with MetricFlow: `mf validate-configs`
4. Deploy and document

### Creating New Views (Cortex Analyst)
1. Copy existing view as template
2. Update table references and relationships
3. Define facts, dimensions, and metrics
4. Add synonyms for natural language
5. Build and test with Cortex Analyst

## 🎯 When to Use Which Approach

### Use **dbt Semantic Layer** for:
- ✅ Multi-tool BI environments (Tableau + Looker + etc.)
- ✅ API-driven applications
- ✅ Strict governance requirements
- ✅ Self-service analytics at scale

### Use **Native Snowflake Semantic Views** for:
- ✅ BI tool integration with embedded business logic
- ✅ Performance-optimized semantic queries
- ✅ Enterprise data governance and lineage
- ✅ Multi-tool consistency with native Snowflake objects

### Use **Cortex Analyst Views** for:
- ✅ Snowflake-only environments
- ✅ Natural language querying needs
- ✅ Rapid prototyping
- ✅ Executive dashboards with AI insights

## 📋 Best Practices

### dbt Semantic Layer
- Define clear entity relationships
- Use consistent naming conventions
- Add comprehensive descriptions
- Test metric calculations
- Version control all changes

### Cortex Analyst Views
- Use clear, descriptive names
- Include comprehensive synonyms
- Add helpful comments
- Test with sample questions
- Keep views focused and purpose-built

## 🔗 Integration Examples

### Tableau (via dbt Semantic Layer)
```python
# Connect to dbt Cloud semantic layer
tableau_connection.connect(
    server="semantic-layer.cloud.getdbt.com",
    token="your_dbt_token"
)
```

### Snowsight (via Cortex Analyst)
```sql
-- Ask natural language questions
SELECT * FROM sem_company_overview 
WHERE cortex_analyst('show me tech companies in Zurich');
```

Both approaches complement each other and can be used simultaneously to serve different analytical needs within your organization. 