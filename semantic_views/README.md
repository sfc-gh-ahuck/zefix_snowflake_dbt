# ZEFIX Semantic Views for Cortex Analyst

Natural language querying for Swiss company data.

## 🎯 What You Can Ask

- *"How many companies are currently active?"*
- *"What's the distribution by legal form?"*
- *"Show me company formations this year"*
- *"Which canton has the most business activity?"*
- *"How many management changes happened recently?"*

## 🚀 Setup

The semantic view deploys automatically with dbt:

```bash
# Run dbt - semantic view creates automatically
dbt run

# View location: {database}.{schema}.zefix_business_intelligence
```

## 📊 Business Metrics

### Company Stats
- Total companies, active companies, deletions
- Legal form distribution (AG, GmbH, Verein, etc.)
- Formation and dissolution trends

### Geographic Analysis
- Canton-level business distribution
- Regional activity patterns

### Activity Tracking
- SHAB publications
- Business changes and mutations
- Management and capital changes

## 🔗 Usage

### Snowsight
1. Open Snowflake Snowsight
2. Use Cortex Analyst
3. Reference: `zefix_business_intelligence`
4. Ask questions in natural language

### Example Questions

**Company Analysis**
- "How many companies are active?"
- "Show me AG vs GmbH distribution"
- "Which canton has most companies?"

**Trends**
- "Company formations this year vs last year"
- "Recent business activity trends"
- "Management changes in last quarter"

## 🏗️ Technical Details

**Tables**: Companies, Publications, Mutations
**Relationships**: Linked by company_uid
**Metrics**: 25+ business metrics
**Updates**: Daily refresh from ZEFIX data

Built automatically from Gold layer models using dbt post-hooks. 