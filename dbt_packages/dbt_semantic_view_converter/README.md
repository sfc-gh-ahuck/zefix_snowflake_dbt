# dbt Semantic View Converter

A dbt package that provides a custom materialization for creating Snowflake semantic views directly from dbt semantic model configurations.

## Overview

This dbt package bridges the gap between dbt's Semantic Layer and Snowflake's native semantic views, allowing you to:
- Transform dbt semantic model definitions into Snowflake semantic views using native dbt workflows
- Maintain consistent business logic across your semantic layer
- Leverage dbt's dependency management, testing, and documentation features
- Use familiar dbt materialization patterns for semantic views

## 🚀 Quick Start

### Installation

Add this package to your dbt project's `packages.yml`:

```yaml
packages:
  - git: "https://github.com/sfc-gh-ahuck/dbt_semantic_view_converter.git"
    revision: main
```

Then run:
```bash
dbt deps
```

**Important:** After installing, make sure to run `dbt deps` and then `dbt parse` to ensure all macros are properly loaded.

### Usage

#### 1. Define Semantic Models

Create semantic models in your `schema.yml`:

```yaml
semantic_models:
  - name: orders
    description: "Order fact table"
    model: ref('dim_orders')
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: order_status
        type: categorical
    measures:
      - name: order_total
        agg: sum
      - name: order_count
        expr: 1
        agg: sum
```

#### 2. Create Semantic View Models

Create a model file using the `semantic_view` materialization:

```sql
-- models/semantic_views/orders_semantic_view.sql
{{ config(
    materialized='semantic_view',
    schema='semantic_layer'
) }}

SELECT 1 as placeholder
```

#### 3. Run dbt

```bash
dbt run --models orders_semantic_view
```

This creates a Snowflake semantic view based on your semantic model definition!

## ⚙️ Configuration

### Package Variables

Configure the package in your `dbt_project.yml`:

```yaml
vars:
  dbt_semantic_view_converter:
    semantic_views_database: "{{ target.database }}"
    semantic_views_schema: "semantic_layer"
    copy_grants: true
```

### Model-Level Configuration

```sql
{{ config(
    materialized='semantic_view',
    schema='my_semantic_layer',
    database='analytics',
    tags=['semantic', 'daily']
) }}
```

## 📖 Example Output

The package generates Snowflake `CREATE SEMANTIC VIEW` statements like:

```sql
CREATE OR REPLACE SEMANTIC VIEW analytics.semantic_layer.orders
  COMMENT = 'Order fact table'
  TABLES (
    orders AS dim_orders
      PRIMARY KEY (order_id)
  )
  RELATIONSHIPS (
    to_customer_id AS
      semantic_model (customer_id) REFERENCES customer
  )
  FACTS (
    orders.order_total AS order_total
  )
  DIMENSIONS (
    orders.order_date AS DATE_TRUNC('DAY', order_date),
    orders.order_status AS order_status
  )
  METRICS (
    orders.total_order_total AS SUM(order_total),
    orders.total_count AS COUNT(*)
  )
  COPY GRANTS;
```

## 🧪 Testing

Test the package with the included example models:

```bash
# Run example semantic views
dbt run --models semantic_views

# Check generated objects in Snowflake
SHOW SEMANTIC VIEWS;
```

## 🔧 Advanced Usage

### Multiple Semantic Models

Define multiple semantic models and create corresponding view files:

```yaml
# schema.yml
semantic_models:
  - name: orders_semantic_view
    # ... configuration
  - name: customers_semantic_view  
    # ... configuration
```

```sql
-- models/semantic_views/orders_semantic_view.sql
{{ config(materialized='semantic_view') }}
SELECT 1 as placeholder

-- models/semantic_views/customers_semantic_view.sql
{{ config(materialized='semantic_view') }}
SELECT 1 as placeholder
```

### Custom Schema and Database

```sql
{{ config(
    materialized='semantic_view',
    schema='custom_semantic_layer',
    database='custom_analytics_db'
) }}
```

## 🔍 Troubleshooting

### "'get_semantic_model_config' is undefined"

If you encounter this error when importing the package:

1. **Ensure dbt deps is run:** Make sure you've run `dbt deps` after adding the package
2. **Parse the project:** Run `dbt parse` to load all macros
3. **Check package installation:** Verify the package appears in `dbt_packages/` directory
4. **Verify profile:** Ensure your dbt profile is properly configured

### "Invalid enum value: `avg` in enum AggregationType"

dbt's semantic layer doesn't support `avg` aggregation. Use these instead:
- `sum`, `count`, `count_distinct`, `max`, `min`, `median`, `sum_boolean`

### "No semantic model configuration found"

This error occurs when:
- The semantic model name doesn't match your model file name
- The semantic model isn't defined in `schema.yml`
- The `schema.yml` file isn't in the same directory as your model

### Time Spine Requirements

If you see "The semantic layer requires a time spine model", add this to your project:

```sql
-- models/time_spine.sql
{{ config(materialized='table') }}
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2025-01-01' as date)"
) }}
```

## 📚 Documentation

For detailed usage instructions, see [docs/usage_guide.md](docs/usage_guide.md).

---

**Transform your dbt semantic models into Snowflake semantic views with native dbt workflows! 🚀** 