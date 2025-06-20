{% macro apply_gold_layer_monitoring() %}
  {# 
    Macro to apply data quality monitoring to all Gold layer models
    This macro defines the monitoring configuration for each gold model
  #}
  
  {# Gold Company Overview Monitoring #}
  {% set company_overview_config = {
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
  } %}
  
  {# Gold Company Activity Monitoring #}
  {% set company_activity_config = {
    'null_checks': [
      {'column': 'company_uid', 'max_nulls': 0},
      {'column': 'shab_id', 'max_nulls': 0},
      {'column': 'shab_date', 'max_nulls': 0},
      {'column': 'activity_type', 'max_nulls': 1000}
    ],
    'freshness_check': {'column': '_loaded_at', 'max_age_hours': 25},
    'row_count': {'min_rows': 5000, 'max_rows': 100000000}
  } %}
  
  {# Gold Canton Statistics Monitoring #}
  {% set canton_statistics_config = {
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
  } %}
  
  {# Apply monitoring to each gold model #}
  {{ setup_data_quality_monitoring('gold_company_overview', company_overview_config) }}
  {{ setup_data_quality_monitoring('gold_company_activity', company_activity_config) }}
  {{ setup_data_quality_monitoring('gold_canton_statistics', canton_statistics_config) }}
  
{% endmacro %}

{% macro remove_gold_layer_monitoring() %}
  {# 
    Macro to remove data quality monitoring from all Gold layer models
  #}
  
  {{ remove_data_quality_monitoring('gold_company_overview') }}
  {{ remove_data_quality_monitoring('gold_company_activity') }}
  {{ remove_data_quality_monitoring('gold_canton_statistics') }}
  
{% endmacro %}

{% macro test_all_gold_expectations() %}
  {# 
    Macro to test data quality expectations for all Gold layer models
  #}
  
  {{ test_data_quality_expectations('gold_company_overview') }}
  {{ test_data_quality_expectations('gold_company_activity') }}
  {{ test_data_quality_expectations('gold_canton_statistics') }}
  
{% endmacro %} 