# Data Quality Monitoring with Snowflake DMFs

This project implements comprehensive data quality monitoring using Snowflake's Data Metric Functions (DMFs) and Expectations feature as documented in the [Snowflake documentation](https://docs.snowflake.com/en/LIMITEDACCESS/data-quality/expectations).

## Overview

The data quality monitoring system provides:
- **Automated monitoring** of critical data quality metrics
- **Expectation-based alerting** when data quality thresholds are breached
- **Centralized configuration** through reusable dbt macros
- **Comprehensive violation tracking** and reporting

## Architecture

### Core Components

1. **`data_quality_monitoring.sql`** - Main macro file with reusable functions
2. **`apply_gold_monitoring.sql`** - Model-specific monitoring configurations
3. **`setup_data_monitoring.sql`** - Utility model to apply monitoring

### Monitoring Configuration

Each gold layer table has tailored monitoring:

#### `gold_company_overview`
- **NULL checks**: `company_uid`, `company_name`, `legal_form_id`
- **Freshness**: Data must be <25 hours old
- **Row count**: Between 1,000 and 50,000,000 records
- **Uniqueness**: No duplicate `company_uid` values

#### `gold_company_activity`
- **NULL checks**: `company_uid`, `shab_id`, `shab_date`, `activity_type`
- **Freshness**: Data must be <25 hours old
- **Row count**: Between 5,000 and 100,000,000 records

#### `gold_canton_statistics`
- **NULL checks**: `canton`, `total_companies`, `active_companies`
- **Freshness**: Data must be <25 hours old
- **Row count**: Between 20 and 30 records (Swiss cantons)
- **Uniqueness**: No duplicate `canton` values

## Usage

### Setting Up Monitoring

1. **Apply monitoring to all gold tables:**
   ```bash
   dbt run --models setup_data_monitoring
   ```

2. **Apply monitoring to a specific model:**
   ```sql
   {{ setup_data_quality_monitoring('gold_company_overview', {
       'null_checks': [
         {'column': 'company_uid', 'max_nulls': 0}
       ],
       'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 24}
   }) }}
   ```

### Testing Expectations

Test current expectations without waiting for scheduled execution:

```sql
SELECT * FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(
    REF_ENTITY_NAME => 'GOLD_COMPANY_OVERVIEW'));
```

Or use the macro:
```sql
{{ test_data_quality_expectations('gold_company_overview') }}
```

### Monitoring Violations

#### View Recent Violations
```sql
SELECT 
  timestamp,
  table_name,
  expectation_name,
  expectation_expression,
  dmf_name
FROM ({{ get_data_quality_violations(7) }})
ORDER BY timestamp DESC;
```

#### Monitor via Snowflake Views
```sql
-- Check expectation status across all tables
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS;

-- Raw violation data
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS_RAW
WHERE resource_attributes:snow.data_metric.record_type::STRING = 'EXPECTATION_VIOLATION_STATUS'
  AND value = TRUE;
```

### Removing Monitoring

Remove all monitoring from gold layer:
```sql
{{ remove_gold_layer_monitoring() }}
```

Remove from specific model:
```sql
{{ remove_data_quality_monitoring('gold_company_overview') }}
```

## Available Data Metric Functions

### System DMFs Used

- **`SNOWFLAKE.CORE.NULL_COUNT`** - Count NULL values in columns
- **`SNOWFLAKE.CORE.FRESHNESS`** - Time since last update (seconds)
- **`SNOWFLAKE.CORE.ROW_COUNT`** - Total rows in table
- **`SNOWFLAKE.CORE.DUPLICATE_COUNT`** - Count duplicate values

### Custom DMF Support

The system supports custom DMFs:
```sql
{{ setup_data_quality_monitoring('my_table', {
    'custom_checks': [
      {
        'dmf': 'MY_CUSTOM_SCHEMA.MY_DMF',
        'column': 'my_column',
        'expectation': 'VALUE > 100'
      }
    ]
}) }}
```

## Configuration Parameters

### Monitoring Config Structure

```sql
{
  'null_checks': [
    {'column': 'column_name', 'max_nulls': 0}
  ],
  'freshness_check': {
    'column': 'timestamp_column', 
    'max_age_hours': 24
  },
  'row_count': {
    'min_rows': 1000, 
    'max_rows': 1000000
  },
  'custom_checks': [
    {
      'dmf': 'SCHEMA.FUNCTION_NAME',
      'column': 'column_name',
      'expectation': 'VALUE > 0'
    }
  ]
}
```

### Expectation Expressions

Expectations use Boolean expressions with the `VALUE` keyword:

- **Range checks**: `VALUE BETWEEN 10 AND 100`
- **Threshold checks**: `VALUE < 50`
- **Exact matches**: `VALUE = 0`
- **Complex conditions**: `VALUE > 0 AND VALUE < 1000`

## Alerting and Notifications

### Integration with Snowflake Alerts

Create alerts based on violations:

```sql
CREATE ALERT company_data_quality_alert
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS_RAW
    WHERE resource_attributes:snow.data_metric.record_type::STRING = 'EXPECTATION_VIOLATION_STATUS'
      AND value = TRUE
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL '5 MINUTE'
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'data-team@company.com',
    'Data Quality Alert',
    'Data quality expectations violated in Swiss company data'
  );
```

### Dashboard Integration

Use violation data in Sigma dashboards:
- Real-time data quality status
- Historical violation trends
- Model-specific quality metrics

## Best Practices

### 1. Threshold Setting
- Start with conservative thresholds
- Monitor actual values before tightening
- Account for business cycles and seasonality

### 2. Monitoring Frequency
- Critical tables: Every 5-15 minutes
- Standard tables: Hourly
- Historical tables: Daily

### 3. Expectation Management
- Document all expectations and their business rationale
- Review and adjust thresholds quarterly
- Test expectations after major schema changes

### 4. Incident Response
- Define clear escalation procedures
- Automate notifications for critical violations
- Maintain runbooks for common data quality issues

## Implementation Notes

### Prerequisites
- Snowflake Enterprise Edition
- Data Quality and DMF features enabled
- Appropriate warehouse compute resources

### Performance Considerations
- DMF execution consumes compute credits
- Large tables may require dedicated warehouses
- Consider partitioning for very large datasets

### Security
- Monitor access to violation data
- Implement row-level security if needed
- Audit expectation changes

## Troubleshooting

### Common Issues

1. **DMF Not Found**: Ensure feature is enabled and warehouse has access
2. **Timeout Errors**: Increase warehouse size for large tables
3. **Permission Errors**: Verify table ownership and USAGE rights

### Debugging Commands

```sql
-- List all expectations for a table
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_EXPECTATIONS(
  REF_ENTITY_NAME => 'GOLD_COMPANY_OVERVIEW',
  REF_ENTITY_DOMAIN => 'table'
));

-- Check DMF execution history
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS_RAW
WHERE resource_attributes:snow.data_metric.ref_entity_name::STRING = 'GOLD_COMPANY_OVERVIEW'
ORDER BY timestamp DESC;
``` 