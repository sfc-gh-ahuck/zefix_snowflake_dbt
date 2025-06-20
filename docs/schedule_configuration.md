# Configurable Data Quality Monitoring Schedules

The data quality monitoring system now supports configurable schedules directly in your model configuration. This allows you to set different monitoring frequencies based on your data characteristics and business requirements.

## 📅 **Schedule Configuration**

### **Basic Usage**

```sql
{{
  config(
    materialized='table',
    data_quality_config={
      'schedule': '15 MINUTE',  # Monitor every 15 minutes
      'null_checks': [
        {'column': 'id', 'max_nulls': 0}
      ]
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}
```

### **Schedule Options**

#### **1. Time-Based Intervals**
```sql
'schedule': '5 MINUTE'    # Every 5 minutes
'schedule': '30 MINUTE'   # Every 30 minutes  
'schedule': '1 HOUR'      # Every hour
'schedule': '4 HOUR'      # Every 4 hours
'schedule': '1 DAY'       # Daily
```

#### **2. CRON Expressions**
```sql
# Daily at 8 AM UTC
'schedule': 'USING CRON 0 8 * * * UTC'

# Weekdays only at 8 AM UTC  
'schedule': 'USING CRON 0 8 * * MON,TUE,WED,THU,FRI UTC'

# Three times daily (6 AM, 12 PM, 6 PM UTC)
'schedule': 'USING CRON 0 6,12,18 * * * UTC'

# Every 15 minutes during business hours (9 AM - 5 PM UTC)
'schedule': 'USING CRON */15 9-17 * * MON-FRI UTC'
```

#### **3. Trigger-Based**
```sql
# Monitor when data changes (DML operations)
'schedule': 'TRIGGER_ON_CHANGES'
```

#### **4. Default Schedule**
If no schedule is specified, defaults to `'6 HOUR'`

## 🏗️ **Real-World Examples**

### **High-Frequency Transactional Data**
```sql
{{
  config(
    data_quality_config={
      'schedule': '5 MINUTE',  # Frequent monitoring
      'null_checks': [
        {'column': 'transaction_id', 'max_nulls': 0}
      ],
      'freshness_check': {'column': 'created_at', 'max_age_hours': 1}
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}
```

### **Reference Data (Slower Change)**
```sql
{{
  config(
    data_quality_config={
      'schedule': 'USING CRON 0 8 * * * UTC',  # Daily at 8 AM
      'null_checks': [
        {'column': 'canton', 'max_nulls': 0}
      ],
      'row_count': {'min_rows': 20, 'max_rows': 30}
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}
```

### **Event-Driven Monitoring**
```sql
{{
  config(
    data_quality_config={
      'schedule': 'TRIGGER_ON_CHANGES',  # Monitor on data changes
      'null_checks': [
        {'column': 'user_id', 'max_nulls': 0}
      ]
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}
```

## 📊 **Current Gold Model Configurations**

### **gold_company_overview**
- **Schedule**: `1 HOUR` (hourly monitoring)
- **Rationale**: Consistent hourly monitoring for company data changes

### **gold_company_activity** 
- **Schedule**: `1 HOUR` (hourly monitoring)
- **Rationale**: Activity data is high-volume, hourly checks are sufficient

### **gold_canton_statistics**
- **Schedule**: `1 HOUR` (hourly monitoring)
- **Rationale**: Consistent monitoring schedule across all gold models

## ⚙️ **Schedule Selection Guidelines**

### **High-Frequency Monitoring (1-15 minutes)**
- **Use for**: Real-time dashboards, financial data, critical operational metrics
- **Consider**: Higher compute costs, more alert noise
- **Example**: `'5 MINUTE'`

### **Moderate Monitoring (15 minutes - 2 hours)**
- **Use for**: Business reports, user analytics, standard operations
- **Consider**: Good balance of freshness vs. cost
- **Example**: `'30 MINUTE'`

### **Scheduled Monitoring (Daily/Business Hours)**
- **Use for**: Reference data, batch processing results, summary reports
- **Consider**: Lower costs, predictable execution
- **Example**: `'USING CRON 0 8 * * * UTC'`

### **Event-Driven Monitoring**
- **Use for**: Critical data that must be validated immediately on change
- **Consider**: Unpredictable timing, potential for high frequency
- **Example**: `'TRIGGER_ON_CHANGES'`

## 🛠️ **Implementation Details**

### **How It Works**
1. **Schedule Set First**: `ALTER TABLE table_name SET DATA_METRIC_SCHEDULE = 'schedule'`
2. **DMFs Added**: All data metric functions use the table's schedule
3. **Single Schedule**: One schedule per table applies to all DMFs on that table

### **Schedule Inheritance**
All data quality checks (NULL checks, freshness, row count, custom) on a table follow the same schedule. You cannot have different schedules for different DMFs on the same table.

### **Schedule Updates**
When you change the schedule in your model config and re-run the model, the table's schedule will be updated for all existing and new DMFs.

## 💰 **Cost Considerations**

### **Compute Costs**
- **Higher frequency** = More compute usage
- **CRON schedules** = Predictable costs
- **TRIGGER_ON_CHANGES** = Variable costs based on data activity

### **Cost Optimization Tips**
1. **Start conservative**: Begin with lower frequency, increase if needed
2. **Use business hours**: Schedule during operational periods only
3. **Group similar tables**: Use similar schedules for related models
4. **Monitor usage**: Track DMF execution costs in Snowflake

## 🔧 **Advanced Configuration**

### **Environment-Specific Schedules**
```sql
{{
  config(
    data_quality_config={
      'schedule': '5 MINUTE' if target.name == 'prod' else '1 HOUR',
      'null_checks': [...]
    }
  )
}}
```

### **Time Zone Considerations**
Always specify UTC in CRON expressions for consistency:
```sql
'schedule': 'USING CRON 0 9 * * MON-FRI UTC'  # 9 AM UTC on weekdays
```

### **Business Hours Example**
```sql
# Monitor every 30 minutes during European business hours
'schedule': 'USING CRON */30 7-19 * * MON-FRI UTC'
```

This configurable schedule system gives you fine-grained control over when your data quality checks run, helping you balance monitoring needs with compute costs. 