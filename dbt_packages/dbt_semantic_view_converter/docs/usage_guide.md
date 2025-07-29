# dbt Semantic View Converter - Usage Guide

## Installation

1. Add to your `packages.yml`:
```yaml
packages:
  - git: "https://github.com/sfc-gh-ahuck/dbt_semantic_view_converter.git"
    revision: main
```

2. Install dependencies:
```bash
dbt deps
```

## Basic Usage

### Step 1: Define Semantic Models

In your `models/schema.yml`, define semantic models following dbt's standard format:

```yaml
version: 2

semantic_models:
  - name: my_semantic_model
    description: "Description of your semantic model"
    model: ref('my_base_table')
    
    entities:
      - name: id
        type: primary
      - name: user_id
        type: foreign
        
    dimensions:
      - name: created_at
        type: time
        type_params:
          time_granularity: day
      - name: status
        type: categorical
        
    measures:
      - name: amount
        agg: sum
      - name: count
        expr: 1
        agg: sum
```

### Step 2: Create Semantic View Model

Create a model file that uses the `semantic_view` materialization:

```sql
-- models/semantic_views/my_semantic_view.sql
{{ config(
    materialized='semantic_view',
    schema='semantic_layer'
) }}

-- The semantic model configuration is read from schema.yml
-- This placeholder query is required but not used
SELECT 1 as placeholder
```

### Step 3: Run dbt

```bash
dbt run --models my_semantic_view
```

## Configuration Options

### Package Variables

Configure in your `dbt_project.yml`:

```yaml
vars:
  dbt_semantic_view_converter:
    # Target database for semantic views
    semantic_views_database: "{{ target.database }}"
    
    # Target schema for semantic views  
    semantic_views_schema: "semantic_layer"
    
    # Whether to copy grants from source tables
    copy_grants: true
```

### Model-Level Configuration

```sql
{{ config(
    materialized='semantic_view',
    
    # Custom schema (overrides package default)
    schema='my_semantic_layer',
    
    # Custom database (overrides package default)  
    database='analytics',
    
    # Standard dbt configs work too
    tags=['semantic', 'daily'],
    meta={'owner': 'data-team'}
) }}
```

## Advanced Examples

### Complex Semantic Model

```yaml
semantic_models:
  - name: sales_analysis
    description: "Comprehensive sales data semantic model"
    model: ref('fact_sales')
    defaults:
      agg_time_dimension: sale_date
    
    entities:
      - name: sale_id
        type: primary
        description: "Unique sale identifier"
      - name: customer_id
        type: foreign
        description: "Customer who made the purchase"
      - name: product_id
        type: foreign
        description: "Product that was sold"
      - name: store_id
        type: foreign
        expr: location_id
        description: "Store location"
    
    dimensions:
      - name: sale_date
        type: time
        expr: date_trunc('day', created_at)
        type_params:
          time_granularity: day
        description: "Date of sale"
      
      - name: sale_month
        type: time
        expr: date_trunc('month', created_at)
        type_params:
          time_granularity: month
        description: "Month of sale"
      
      - name: payment_method
        type: categorical
        description: "How customer paid"
      
      - name: is_large_sale
        type: categorical
        expr: case when total_amount > 100 then 'Large' else 'Small' end
        description: "Sale size classification"
    
    measures:
      - name: total_amount
        description: "Sale amount before tax"
        agg: sum
      
      - name: tax_amount
        description: "Tax collected"
        agg: sum
      
      - name: sale_count
        description: "Number of sales"
        expr: 1
        agg: sum
      
      - name: avg_sale_amount
        description: "Average sale amount"
        expr: total_amount
        agg: avg
      
      - name: unique_customers
        description: "Count of unique customers"
        expr: customer_id
        agg: count_distinct
```

### Multiple Semantic Views

You can create multiple semantic views in the same dbt project:

```sql
-- models/semantic_views/sales_semantic_view.sql
{{ config(materialized='semantic_view') }}
SELECT 1 as placeholder

-- models/semantic_views/customers_semantic_view.sql  
{{ config(materialized='semantic_view') }}
SELECT 1 as placeholder

-- models/semantic_views/products_semantic_view.sql
{{ config(materialized='semantic_view') }}
SELECT 1 as placeholder
```

Then run them all:
```bash
dbt run --models semantic_views
```

## Generated SQL

The materialization generates Snowflake `CREATE SEMANTIC VIEW` statements like:

```sql
CREATE OR REPLACE SEMANTIC VIEW analytics.semantic_layer.sales_analysis
  COMMENT = 'Comprehensive sales data semantic model'
  TABLES (
    sales_analysis AS fact_sales
      PRIMARY KEY (sale_id)
  )
  RELATIONSHIPS (
    to_customer_id AS
      semantic_model (customer_id) REFERENCES customer,
    to_product_id AS
      semantic_model (product_id) REFERENCES product,
    to_store_id AS
      semantic_model (location_id) REFERENCES store
  )
  FACTS (
    sales_analysis.total_amount AS total_amount
      COMMENT 'Sale amount before tax',
    sales_analysis.tax_amount AS tax_amount
      COMMENT 'Tax collected'
  )
  DIMENSIONS (
    sales_analysis.sale_date AS date_trunc('day', created_at)
      COMMENT 'Date of sale',
    sales_analysis.payment_method AS payment_method
      COMMENT 'How customer paid',
    sales_analysis.is_large_sale AS case when total_amount > 100 then 'Large' else 'Small' end
      COMMENT 'Sale size classification'
  )
  METRICS (
    sales_analysis.total_total_amount AS SUM(total_amount)
      COMMENT 'Sale amount before tax',
    sales_analysis.total_count AS COUNT(*)
      COMMENT 'Number of sales',
    sales_analysis.avg_avg_sale_amount AS AVG(total_amount)
      COMMENT 'Average sale amount',
    sales_analysis.unique_customers AS COUNT(DISTINCT customer_id)
      COMMENT 'Count of unique customers'
  )
  COPY GRANTS;
```

## Integration with dbt Workflows

### dbt Docs

Semantic views appear in dbt docs with their descriptions and lineage:

```bash
dbt docs generate
dbt docs serve
```

### dbt Tests

You can add tests to your semantic view models:

```yaml
models:
  - name: orders_semantic_view
    description: "Orders semantic view"
    tests:
      - dbt_utils.expression_is_true:
          expression: "1 = 1"  # Custom test logic
```

### dbt Freshness and Dependencies

Semantic views integrate with dbt's dependency graph:

```bash
# Run upstream models first, then semantic views
dbt run --models +orders_semantic_view

# Run only if upstream has changed
dbt run --models orders_semantic_view --state ./target
```

## Troubleshooting

### Common Issues

1. **"No semantic model configuration found"**
   - Ensure your semantic model name matches your model file name
   - Check that semantic model is defined in schema.yml
   - Verify the schema.yml is in the correct location

2. **"Object does not exist" errors**
   - Ensure referenced tables exist in Snowflake
   - Check that `ref()` functions point to valid models
   - Verify database/schema permissions

3. **Relationship reference issues**
   - Foreign entity relationships are auto-inferred
   - May need manual adjustment for complex relationship names

### Debug Mode

Enable debug logging:
```bash
dbt run --models my_semantic_view --debug
```

### Dry Run

Check generated SQL without executing:
```bash
dbt compile --models my_semantic_view
# Check target/compiled/ directory for generated SQL
```

### Inspect Generated Objects

Check your semantic views in Snowflake:
```sql
-- List all semantic views
SHOW SEMANTIC VIEWS;

-- Describe a specific semantic view
DESCRIBE SEMANTIC VIEW my_semantic_view;

-- Query semantic view metadata
SHOW SEMANTIC DIMENSIONS FOR VIEW my_semantic_view;
SHOW SEMANTIC METRICS FOR VIEW my_semantic_view;
``` 