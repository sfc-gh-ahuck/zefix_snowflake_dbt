# Snowflake Semantic Views Generation

This project uses the [dbt_semantic_view_converter](https://github.com/sfc-gh-ahuck/dbt_semantic_view_converter.git) package to automatically generate Snowflake semantic views from dbt semantic model definitions.

## 📋 How It Works

The package bridges dbt's semantic layer with Snowflake's native semantic views by:

1. **Reading semantic model definitions** from `semantic_models.yml`
2. **Converting them to Snowflake SQL** using the custom `semantic_view` materialization
3. **Creating native Snowflake semantic views** that can be queried directly

## 🚀 Current Setup

### Semantic Models Defined
- **`companies`**: Swiss company master data from ZEFIX
- **`publications`**: SHAB publication activity and business events

### Semantic View Models
- **`companies_semantic_view.sql`**: Generates semantic view for companies data
- **`publications_semantic_view.sql`**: Generates semantic view for publications data

## 🔧 Configuration

The package is configured in `dbt_project.yml`:

```yaml
vars:
  dbt_semantic_view_converter:
    semantic_views_database: "{{ target.database }}"
    semantic_views_schema: "semantic_layer"
    copy_grants: true

models:
  zefix:
    semantic:
      +materialized: semantic_view
      +tags: ["semantic", "cortex"]
```

## 📊 Generated Snowflake Objects

When you run the semantic view models, they will create Snowflake semantic views like:

### Companies Semantic View
```sql
CREATE OR REPLACE SEMANTIC VIEW {database}.semantic_layer.companies_semantic_view
  COMMENT = 'Core semantic model for Swiss company data from ZEFIX registry'
  TABLES (
    companies AS {{ ref('silver_companies') }}
      PRIMARY KEY (company_uid)
  )
  DIMENSIONS (
    companies.legal_form_name AS legal_form,
    companies.company_status AS company_status,
    companies.is_active AS is_active,
    companies.legal_seat AS legal_seat,
    companies.first_observed_shab_date AS registration_date
  )
  METRICS (
    companies.total_companies AS COUNT(DISTINCT company_uid),
    companies.company_count AS COUNT(DISTINCT company_uid)
  )
  COPY GRANTS;
```

### Publications Semantic View
```sql
CREATE OR REPLACE SEMANTIC VIEW {database}.semantic_layer.publications_semantic_view
  COMMENT = 'Semantic model for SHAB publication activity and business events'
  TABLES (
    publications AS {{ ref('silver_shab_publications') }}
      PRIMARY KEY (publication_id),
    companies AS {{ ref('silver_companies') }}
      FOREIGN KEY (company_uid) REFERENCES companies (company_uid)
  )
  DIMENSIONS (
    publications.shab_date AS publication_date,
    publications.mutation_type AS mutation_type,
    publications.activity_type AS activity_type
  )
  METRICS (
    publications.total_publications AS COUNT(DISTINCT publication_id),
    publications.publication_count AS COUNT(DISTINCT publication_id)
  )
  COPY GRANTS;
```

## 🏃‍♀️ Usage Commands

### Installation
```bash
# Install package dependencies
dbt deps

# Run semantic view models
dbt run --models tag:semantic

# Or run specific semantic views
dbt run --models companies_semantic_view publications_semantic_view
```

### Querying in Snowflake
Once created, you can query the semantic views directly in Snowflake:

```sql
-- Query companies semantic view
SELECT 
  legal_form,
  COUNT(*) as company_count
FROM semantic_layer.companies_semantic_view
WHERE is_active = TRUE
GROUP BY legal_form;

-- Query publications semantic view  
SELECT 
  activity_type,
  COUNT(*) as publication_count
FROM semantic_layer.publications_semantic_view
WHERE publication_date >= '2024-01-01'
GROUP BY activity_type;
```

## 🔗 Integration Benefits

### With Cortex Analyst
- Semantic views work seamlessly with Snowflake Cortex Analyst
- Natural language queries against structured business definitions
- Consistent metrics across all analytics tools

### With BI Tools
- Direct connection from Tableau, Looker, Power BI
- Pre-defined business logic embedded in the database
- No need to redefine metrics in each tool

### With dbt Ecosystem
- Version controlled semantic definitions
- Automated testing and documentation
- Integration with dbt's dependency management

## 📝 Adding New Semantic Views

To add a new semantic view:

1. **Define semantic model** in `semantic_models.yml`
2. **Create model file** using the `semantic_view` materialization:
   ```sql
   {{ config(materialized='semantic_view', schema='semantic_layer') }}
   SELECT 1 as placeholder
   ```
3. **Run the model**: `dbt run --models your_new_semantic_view`

## 🔍 Troubleshooting

### Common Issues
- **Package not found**: Ensure `dbt deps` completed successfully
- **Materialization error**: Check that semantic model name matches the file name
- **SQL errors**: Verify underlying models exist and are accessible

### Validation
```bash
# Check generated objects in Snowflake
SHOW SEMANTIC VIEWS IN SCHEMA semantic_layer;

# Describe a specific semantic view
DESCRIBE SEMANTIC VIEW semantic_layer.companies_semantic_view;
```

This setup provides a powerful bridge between dbt's semantic modeling capabilities and Snowflake's native semantic view functionality! 🚀 