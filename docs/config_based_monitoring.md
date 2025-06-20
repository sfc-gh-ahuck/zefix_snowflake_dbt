# Config-Based Data Quality Monitoring

This document explains how to use the new **config-based approach** for data quality monitoring, which allows you to define monitoring rules directly in your model's config block.

## 🎯 **Benefits**

- **✅ No Circular Dependencies** - Avoids the ref() issues from previous approaches
- **🔧 Easy Configuration** - Define monitoring rules alongside other model configs
- **📦 Self-Contained** - Everything for a model is in one place
- **🚀 Automatic Application** - Monitoring applied automatically when model runs
- **🔄 Version Controlled** - Monitoring config is part of your model code

## 📋 **Basic Usage**

### **1. Simple Example**

```sql
{{
  config(
    materialized='table',
    data_quality_config={
      'null_checks': [
        {'column': 'id', 'max_nulls': 0}
      ],
      'freshness_check': {'column': 'updated_at', 'max_age_hours': 24}
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}

SELECT 
  1 as id,
  'Example record' as description,
  CURRENT_TIMESTAMP() as updated_at
```

### **2. Comprehensive Example**

```sql
{{
  config(
    materialized='incremental',
    unique_key='company_uid',
    data_quality_config={
      'null_checks': [
        {'column': 'company_uid', 'max_nulls': 0},
        {'column': 'company_name', 'max_nulls': 5}
      ],
      'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 24},
      'row_count': {'min_rows': 1000, 'max_rows': 1000000},
      'custom_checks': [
        {
          'dmf': 'SNOWFLAKE.CORE.DUPLICATE_COUNT',
          'column': 'company_uid',
          'expectation': 'VALUE = 0'
        }
      ]
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}

-- Your model SQL here...
```

## 🔧 **Configuration Options**

### **NULL Checks**
Monitor columns for excessive NULL values:

```sql
'null_checks': [
  {'column': 'required_field', 'max_nulls': 0},      # No NULLs allowed
  {'column': 'optional_field', 'max_nulls': 100},    # Up to 100 NULLs OK
  {'column': 'sparse_field', 'max_nulls': 10000}     # Up to 10K NULLs OK
]
```

### **Freshness Check**
Ensure data is updated within expected timeframe:

```sql
'freshness_check': {
  'column': 'last_updated_at',    # Timestamp column to check
  'max_age_hours': 24             # Data must be <24 hours old
}
```

### **Row Count Check**
Validate table has expected number of rows:

```sql
'row_count': {
  'min_rows': 1000,        # At least 1K rows
  'max_rows': 1000000      # At most 1M rows
}
```

### **Custom DMF Checks**
Use any Snowflake Data Metric Function:

```sql
'custom_checks': [
  {
    'dmf': 'SNOWFLAKE.CORE.DUPLICATE_COUNT',
    'column': 'unique_field',
    'expectation': 'VALUE = 0'
  },
  {
    'dmf': 'SNOWFLAKE.CORE.UNIQUE_COUNT',
    'column': 'category_field',
    'expectation': 'VALUE BETWEEN 5 AND 50'
  }
]
```

## 🏗️ **Implementation in Gold Models**

All gold models now use this approach:

### **gold_company_overview.sql**
```sql
{{
  config(
    materialized='incremental',
    unique_key='company_uid',
    data_quality_config={
      'null_checks': [
        {'column': 'company_uid', 'max_nulls': 0},
        {'column': 'company_name', 'max_nulls': 5},
        {'column': 'legal_form_id', 'max_nulls': 100}
      ],
      'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 25},
      'row_count': {'min_rows': 1000, 'max_rows': 50000000},
      'custom_checks': [
        {
          'dmf': 'SNOWFLAKE.CORE.DUPLICATE_COUNT', 
          'column': 'company_uid', 
          'expectation': 'VALUE = 0'
        }
      ]
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}
```

### **gold_company_activity.sql**
```sql
data_quality_config={
  'null_checks': [
    {'column': 'company_uid', 'max_nulls': 0},
    {'column': 'shab_id', 'max_nulls': 0},
    {'column': 'shab_date', 'max_nulls': 0},
    {'column': 'activity_type', 'max_nulls': 1000}
  ],
  'freshness_check': {'column': '_loaded_at', 'max_age_hours': 25},
  'row_count': {'min_rows': 5000, 'max_rows': 100000000}
}
```

### **gold_canton_statistics.sql**
```sql
data_quality_config={
  'null_checks': [
    {'column': 'canton', 'max_nulls': 0},
    {'column': 'total_companies', 'max_nulls': 0},
    {'column': 'active_companies', 'max_nulls': 0}
  ],
  'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 25},
  'row_count': {'min_rows': 20, 'max_rows': 30},
  'custom_checks': [
    {
      'dmf': 'SNOWFLAKE.CORE.DUPLICATE_COUNT', 
      'column': 'canton', 
      'expectation': 'VALUE = 0'
    }
  ]
}
```

## 🚀 **How to Use**

### **1. Run Models with Monitoring**
```bash
# Build models - monitoring applied automatically via post-hooks
dbt run --models gold_company_overview

# Or run all gold models
dbt run --models gold
```

### **2. Test Expectations Manually**
```sql
-- Test specific model
{{ test_data_quality_expectations('gold_company_overview') }}

-- Check violations
SELECT * FROM ({{ get_data_quality_violations(7) }});
```

### **3. Monitor via Snowflake**
```sql
-- Check all expectation statuses
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS;

-- Check raw results
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS_RAW
WHERE resource_attributes:snow.data_metric.record_type::STRING = 'EXPECTATION_VIOLATION_STATUS';
```

## 💡 **Best Practices**

### **1. Start Conservative**
Begin with loose thresholds and tighten based on actual data:

```sql
# Start with higher limits
{'column': 'optional_field', 'max_nulls': 1000}

# Tighten after observing patterns
{'column': 'optional_field', 'max_nulls': 100}
```

### **2. Document Your Expectations**
Add comments explaining business rules:

```sql
data_quality_config={
  'null_checks': [
    # Swiss company UIDs are mandatory by law
    {'column': 'company_uid', 'max_nulls': 0},
    # Company names can occasionally be missing during data sync
    {'column': 'company_name', 'max_nulls': 5}
  ]
}
```

### **3. Model-Specific Configuration**
Tailor monitoring to each model's characteristics:

```sql
# High-volume transactional data
'row_count': {'min_rows': 100000, 'max_rows': 10000000}

# Reference data (cantons, legal forms)  
'row_count': {'min_rows': 20, 'max_rows': 100}
```

### **4. Business Hours Considerations**
Adjust freshness checks for business hours:

```sql
# Weekday updates
'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 25}

# Weekend tolerance  
'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 72}
```

## 🔧 **Advanced Features**

### **1. Conditional Configuration**
Apply different rules based on environment:

```sql
{{
  config(
    data_quality_config={
      'row_count': {
        'min_rows': 1000 if target.name == 'prod' else 100,
        'max_rows': 10000000 if target.name == 'prod' else 1000000
      }
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}
```

### **2. Model Introspection**
Get monitoring config for any model:

```sql
{% set config = get_model_data_quality_config('gold_company_overview') %}
{{ log("Monitoring config: " ~ config, info=true) }}
```

## 🛠️ **Troubleshooting**

### **Common Issues**

1. **Post-hook not running**
   - Ensure `post_hook="{{ apply_data_quality_from_config() }}"` is included
   - Check dbt logs for hook execution

2. **DMF not found**
   - Verify Snowflake Enterprise Edition with DMF feature enabled
   - Check warehouse permissions

3. **Syntax errors in config**
   - Validate JSON syntax in data_quality_config
   - Use proper quoting for string values

### **Debugging Commands**

```sql
-- List all DMFs for a table
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_EXPECTATIONS(
  REF_ENTITY_NAME => 'GOLD_COMPANY_OVERVIEW',
  REF_ENTITY_DOMAIN => 'table'
));

-- Check recent monitoring results
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS_RAW
WHERE resource_attributes:snow.data_metric.ref_entity_name::STRING = 'GOLD_COMPANY_OVERVIEW'
ORDER BY timestamp DESC
LIMIT 10;
```

## 🎯 **Migration from Old Approach**

If you were using the previous `setup_data_monitoring` utility model:

### **Before (Centralized)**
```sql
-- In apply_gold_monitoring.sql
{{ setup_data_quality_monitoring('gold_company_overview', {...}) }}
```

### **After (Config-Based)**  
```sql
-- In gold_company_overview.sql config block
data_quality_config={...},
post_hook="{{ apply_data_quality_from_config() }}"
```

### **Benefits of Migration**
- ✅ No more circular dependency issues
- ✅ Monitoring config co-located with model
- ✅ Easier to maintain and understand
- ✅ Automatic application on every model run 