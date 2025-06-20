# Using Post-Hooks for Data Quality Monitoring

## ✅ **Correct Usage**

To automatically apply data quality monitoring when models are built, use post-hooks with the model's identifier passed as a string:

```sql
{{
  config(
    materialized='table',
    post_hook=[
      "{{ setup_data_quality_monitoring(this.identifier, {
          'null_checks': [
            {'column': 'id', 'max_nulls': 0}
          ],
          'freshness_check': {'column': 'updated_at', 'max_age_hours': 24},
          'row_count': {'min_rows': 100}
        }) 
      }}"
    ]
  )
}}

SELECT 
  1 as id,
  'Example record' as description,
  CURRENT_TIMESTAMP() as updated_at
```

## ❌ **Avoid - Causes Cycles**

Do NOT use `this.name` in post-hooks as it can create circular dependencies:

```sql
-- DON'T DO THIS - CAUSES CYCLES
{{ setup_data_quality_monitoring(this.name, {...}) }}
```

## 💡 **Alternative Approaches**

### 1. **Separate Monitoring Setup**
Apply monitoring separately after models are built:

```bash
# Build models first
dbt run --models gold_company_overview

# Then apply monitoring
dbt run --models setup_data_monitoring
```

### 2. **Model-Specific Hooks**
Add to specific models that need monitoring:

```sql
{{
  config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (company_uid) EXPECTATION no_nulls (VALUE = 0)"
  )
}}
```

### 3. **Conditional Application**
Use conditional logic to avoid self-reference:

```sql
{{
  config(
    materialized='table',
    post_hook=[
      "{% if this.identifier != 'problematic_model' %}{{ setup_data_quality_monitoring(this.identifier, {...}) }}{% endif %}"
    ]
  )
}}
```

## 🛠️ **Best Practices**

1. **Use `this.identifier`** instead of `this.name` in post-hooks
2. **Test thoroughly** in development environment first
3. **Apply monitoring manually** for complex scenarios
4. **Use the utility model** `setup_data_monitoring` for bulk application
5. **Monitor dbt logs** for any circular dependency warnings 