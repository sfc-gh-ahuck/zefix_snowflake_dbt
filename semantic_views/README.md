# 🧠 ZEFIX Snowflake Semantic Views for Cortex Analyst

Transform your ZEFIX Swiss company data into **natural language queryable** business intelligence using Snowflake's Cortex Analyst and Semantic Views.

## 🎯 What This Enables

Ask natural language questions about Swiss company data:

- *"How many companies are currently active?"*
- *"What's the distribution of companies by legal form?"*
- *"Show me company formations this year vs last year"*
- *"Which canton has the most business activity?"*
- *"How many management changes happened recently?"*
- *"What percentage of companies are AGs versus GmbHs?"*

## 🚀 Automated dbt Integration

The semantic view is **automatically deployed** as part of your dbt workflow! No manual setup required.

### How It Works

1. **dbt Model**: `models/gold/zefix_semantic_view_deployer.sql` serves as deployment trigger
2. **Post-Hook Macro**: `macros/create_zefix_semantic_view.sql` contains the semantic view definition
3. **Automatic Deployment**: When you run `dbt run`, the semantic view is created/updated in your target schema

### Simple Setup

```bash
# Just run dbt as usual - the semantic view deploys automatically!
dbt run

# The semantic view will be created at:
# {your_database}.{your_schema}.zefix_business_intelligence
```

You'll see these confirmation messages in your dbt logs:
```
Creating ZEFIX Semantic View: zefix_business_intelligence
✅ ZEFIX Semantic View created successfully!
```

> **✨ Single Source of Truth**: The semantic view definition is maintained in one place: `macros/create_zefix_semantic_view.sql`. No duplication, easy maintenance!

## 🏗️ Business Data Model Architecture

### Logical Tables
- **companies**: Core business entity (from `silver_companies`)
- **publications**: SHAB publication activity (from `silver_shab_publications`)
- **mutations**: Business changes/mutations (from `silver_mutation_types`)

### Key Dimensions
- **Company Information**: UIDs, names, legal forms, status, locations
- **Geographic**: Swiss cantons, towns, addresses
- **Temporal**: Years, quarters, recency buckets, registration dates
- **Activity Types**: Formations, dissolutions, capital changes, management changes
- **Legal Forms**: AG, GmbH, Verein, Stiftung, Genossenschaft, etc.

### Business Metrics (25+ Metrics)

#### 📊 Company Overview
- `total_companies` - Total number of companies
- `active_companies` - Currently active companies
- `deleted_companies` - Deleted/dissolved companies
- `active_company_percentage` - Percentage of active companies

#### 📈 Publication Activity
- `total_publications` - Total SHAB publications
- `recent_publications` - Publications in last 30 days
- `this_year_publications` - Publications in current year
- `average_publications_per_company` - Average publications per company

#### 🏢 Formation & Dissolution
- `company_formations` - Number of new company formations
- `company_dissolutions` - Number of company dissolutions
- `net_company_formation` - Net formation (formations - dissolutions)

#### ⚖️ Legal Form Distribution
- `aktiengesellschaft_count` - Stock companies (AG)
- `gmbh_count` - Limited liability companies (GmbH)
- `verein_count` - Associations (Verein)
- `stiftung_count` - Foundations (Stiftung)
- `genossenschaft_count` - Cooperatives (Genossenschaft)

#### 🔄 Business Changes
- `total_mutations` - Total business changes/mutations
- `management_changes` - Management/administration changes
- `capital_changes` - Capital-related changes
- `address_changes` - Address changes

#### 🗺️ Geographic Analysis
- `unique_cantons` - Number of different Swiss cantons
- `companies_per_canton` - Average companies per canton

#### ⏰ Time-based Analysis
- `oldest_company_age_days` - Age of oldest company in days
- `companies_registered_this_year` - Companies registered this year

## 🎨 Advanced Features

### Intelligent Activity Classification
Automatically categorizes business activities based on publication content:
- **Formation**: Company creation (`gründung`, `constitution`)
- **Dissolution**: Company closure (`auflösung`, `dissolution`)
- **Capital Change**: Capital modifications (`kapital`, `capital`)
- **Address Change**: Location changes (`adresse`, `address`)
- **Management Change**: Leadership changes (`verwaltung`, `administration`)
- **Purpose Change**: Business purpose modifications (`zweck`, `purpose`)
- **Merger**: Company mergers (`fusion`, `merger`)

### Time Intelligence
- **Recency Buckets**: Last 30 days, 90 days, year, older
- **Registration Year**: Extracted from first SHAB publication
- **Days Since**: Calculated days since registration/publication

### Legal Form Translation
Maps numeric legal form IDs to human-readable German names:
- `1` → Einzelunternehmen (Sole Proprietorship)
- `3` → Aktiengesellschaft (Stock Company)
- `5` → Gesellschaft mit beschränkter Haftung (Limited Liability Company)
- `7` → Verein (Association)
- `8` → Stiftung (Foundation)

## 🔗 Usage with Cortex Analyst

### Via Snowsight
1. Navigate to Snowflake Snowsight
2. Use the Cortex Analyst interface
3. Reference the semantic view: `zefix_business_intelligence`
4. Ask natural language questions

### Via REST API
```json
{
  "semantic_view": "your_database.your_schema.zefix_business_intelligence",
  "question": "How many companies were formed this year?"
}
```

### Via SQL (Preview Feature)
```sql
-- Direct SQL queries against semantic view
SELECT * FROM zefix_business_intelligence LIMIT 5;

-- Query specific metrics
SELECT 
  total_companies,
  active_companies,
  active_company_percentage
FROM zefix_business_intelligence;
```

## 📚 Example Natural Language Queries

### Company Analysis
- "How many companies are currently active?"
- "What percentage of companies are stock companies?"
- "Show me the top 5 cantons by number of companies"
- "How many companies have been deleted or dissolved?"

### Activity Trends
- "What's the trend of company formations over the last 5 years?"
- "How many new companies were registered this month?"
- "Show me publication activity in the last quarter"
- "What's the net company formation rate?"

### Legal Form Analysis
- "Compare the number of AGs vs GmbHs"
- "What's the distribution of companies by legal form?"
- "How many associations (Verein) are there?"
- "Show me the percentage breakdown of legal forms"

### Geographic Insights
- "Which canton has the most registered companies?"
- "Show me business activity by canton"
- "How many different cantons have company activity?"
- "What's the average number of companies per canton?"

### Change Analysis
- "How many management changes happened this year?"
- "Show me recent capital changes"
- "What types of business changes are most common?"
- "How many address changes occurred in the last 90 days?"

## 🔧 Technical Implementation

### Relationships
```
publications ←→ companies (via company_uid)
mutations ←→ companies (via company_uid)
mutations ←→ publications (via company_uid + shab_date)
```

### Data Quality Features
- **Null-safe calculations** using `NULLIF()` functions
- **Content-based intelligence** via `ILIKE` pattern matching
- **Multi-language support** (German and English keywords)
- **Robust date handling** with `EXTRACT()` and `DATEDIFF()`

### Performance Optimization
- **Efficient aggregations** using `COUNT(DISTINCT)` patterns
- **Conditional logic** with `CASE WHEN` expressions
- **Window functions** for complex calculations
- **Proper indexing** via semantic view relationships

## 🛠️ Maintenance & Updates

### Automatic Updates
The semantic view automatically updates when you run:
```bash
dbt run
```

### Manual Updates
If you need to modify the semantic view:
1. Edit `macros/create_zefix_semantic_view.sql` (single source of truth)
2. Run `dbt run --select zefix_semantic_view_deployer`

### Adding New Metrics
To add new business metrics:
1. Edit `macros/create_zefix_semantic_view.sql`
2. Add metric definition following the existing pattern:
```sql
- new_metric_name:
    DESCRIPTION: "Description of what this metric measures"
    EXPR: SQL_EXPRESSION_HERE
```
3. Run `dbt run` to deploy the updated semantic view

### Schema Changes
If underlying Silver models change:
1. Update table references in `macros/create_zefix_semantic_view.sql`
2. Adjust dimension/fact definitions as needed
3. Test the semantic view with sample queries

## 📁 File Structure

```
semantic_views/
└── README.md                           # This comprehensive guide

models/gold/
└── zefix_semantic_view_deployer.sql   # dbt model with post-hook trigger

macros/
└── create_zefix_semantic_view.sql     # Semantic view definition (single source of truth)
```

**Key Design Principles:**
- ✅ **No Duplication**: Semantic view logic exists in one place only
- ✅ **Automated Deployment**: Integrates seamlessly with dbt workflow
- ✅ **Single Source of Truth**: All changes made in the macro file
- ✅ **Version Controlled**: Semantic view evolves with your dbt project
- ✅ **dbt Native**: Uses `{{ ref() }}` for proper table references and dependencies

## 🔍 Troubleshooting

### Common Issues

**Semantic view not found:**
- Ensure dbt models have been built: `dbt run`
- Check your database/schema permissions
- Verify the semantic view exists: `SHOW SEMANTIC VIEWS`

**Query errors:**
- Ensure underlying Silver models exist and have data
- Check column names match between semantic view and actual tables
- Verify data types are compatible

**Performance issues:**
- Review metric complexity
- Consider adding filters to reduce data volume
- Check underlying table performance

### Validation Commands
```sql
-- Check if semantic view exists
SHOW SEMANTIC VIEWS LIKE 'zefix_business_intelligence';

-- Validate semantic view structure
DESCRIBE SEMANTIC VIEW zefix_business_intelligence;

-- Test basic functionality
SELECT total_companies FROM zefix_business_intelligence;
```

## 📖 Additional Resources

- [Snowflake Semantic Views Documentation](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Semantic Model Specification](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/semantic-model-spec)
- [dbt Post-Hooks Documentation](https://docs.getdbt.com/reference/resource-configs/post-hook)

---

## 🎉 Ready to Use!

Your ZEFIX data is now ready for natural language querying with Cortex Analyst! Simply run your dbt models and start asking business questions about Swiss company data.

**Next Steps:**
1. Run `dbt run` to deploy everything
2. Open Snowflake Snowsight
3. Navigate to Cortex Analyst
4. Reference `zefix_business_intelligence`
5. Start asking questions about your data!

The semantic view bridges the gap between raw ZEFIX data and business insights, making Swiss commercial register analysis accessible to everyone in your organization. 