{% macro apply_data_quality_from_config() %}
  {# 
    Macro to apply data quality monitoring based on model config
    This is designed to be used in post-hooks to automatically apply monitoring
    based on the data_quality_config defined in the model's config block
    
    Usage in model:
    {{
      config(
        materialized='table',
        data_quality_config={
          'null_checks': [
            {'column': 'company_uid', 'max_nulls': 0}
          ],
          'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 24}
        },
        post_hook="{{ apply_data_quality_from_config() }}"
      )
    }}
  #}
  
  {% if config.get('data_quality_config') %}
    {% set monitoring_config = config.get('data_quality_config') %}
    {{ setup_data_quality_monitoring_direct(this, monitoring_config) }}
  {% endif %}
  
{% endmacro %}

{% macro setup_data_quality_monitoring_direct(table_ref, monitoring_config) %}
  {# 
    Direct version that accepts a table reference to avoid circular dependencies
    This is the core function that applies all monitoring rules
  #}
  
  {% set sql_statements = [] %}
  
  {# Set data metric schedule parameter on the table (configurable) #}
  {% set schedule = monitoring_config.get('schedule', '360 MINUTE') %}
  {% set set_schedule_sql %}
    ALTER TABLE {{ table_ref }} SET DATA_METRIC_SCHEDULE = '{{ schedule }}'
  {% endset %}
  {% do sql_statements.append(set_schedule_sql) %}
  
  {# Get existing DMFs to determine if we should ADD or MODIFY #}
  {% set get_existing_dmfs_query %}
    SELECT 
      metric_name,
      ref_arguments
    FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
      REF_ENTITY_NAME => '{{ table_ref.identifier.upper() }}',
      REF_ENTITY_DOMAIN => 'table'
    ))
  {% endset %}
  
  {% set existing_dmfs = {} %}
  {% if execute %}
    {% set dmf_results = run_query(get_existing_dmfs_query) %}
    {% for row in dmf_results %}
      {% set metric_name = row[0].split('.')[-1] %}
      {% set ref_args = row[1] %}
      {% if ref_args and ref_args|length > 0 %}
        {% set column_name = ref_args[0] if ref_args[0] != 'TABLE()' else '' %}
      {% else %}
        {% set column_name = '' %}
      {% endif %}
      {% set key = metric_name.upper() ~ '_' ~ column_name.upper() %}
      {% do existing_dmfs.update({key: true}) %}
    {% endfor %}
  {% endif %}
  
  {# NULL Count Checks - Use ADD or MODIFY based on existence #}
  {% if monitoring_config.get('null_checks') %}
    {% for null_check in monitoring_config.null_checks %}
      {% set expectation_name = 'null_check_' ~ null_check.column %}
      {% set dmf_key = 'NULL_COUNT_' ~ null_check.column.upper() %}
      {% if dmf_key in existing_dmfs %}
        {% set sql_stmt %}
          ALTER TABLE {{ table_ref }}
            MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ({{ null_check.column }})
            ADD EXPECTATION {{ expectation_name }} (VALUE <= {{ null_check.max_nulls }})
        {% endset %}
      {% else %}
        {% set sql_stmt %}
          ALTER TABLE {{ table_ref }}
            ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ({{ null_check.column }})
            EXPECTATION {{ expectation_name }} (VALUE <= {{ null_check.max_nulls }})
        {% endset %}
      {% endif %}
      {% do sql_statements.append(sql_stmt) %}
    {% endfor %}
  {% endif %}
  
  {# Freshness Check #}
  {% if monitoring_config.get('freshness_check') %}
    {% set freshness_config = monitoring_config.freshness_check %}
    {% set max_age_seconds = freshness_config.max_age_hours * 3600 %}
    {% set dmf_key = 'FRESHNESS_' ~ freshness_config.column.upper() %}
    {% if dmf_key in existing_dmfs %}
      {% set sql_stmt %}
        ALTER TABLE {{ table_ref }}
          MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({{ freshness_config.column }})
          ADD EXPECTATION freshness_check (VALUE <= {{ max_age_seconds }})
      {% endset %}
    {% else %}
      {% set sql_stmt %}
        ALTER TABLE {{ table_ref }}
          ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({{ freshness_config.column }})
          EXPECTATION freshness_check (VALUE <= {{ max_age_seconds }})
      {% endset %}
    {% endif %}
    {% do sql_statements.append(sql_stmt) %}
  {% endif %}
  
  {# Row Count Check #}
  {% if monitoring_config.get('row_count') %}
    {% set row_config = monitoring_config.row_count %}
    {% set expectation_expr = [] %}
    {% if row_config.get('min_rows') %}
      {% do expectation_expr.append('VALUE >= ' ~ row_config.min_rows) %}
    {% endif %}
    {% if row_config.get('max_rows') %}
      {% do expectation_expr.append('VALUE <= ' ~ row_config.max_rows) %}
    {% endif %}
    {% if expectation_expr %}
      {% set dmf_key = 'ROW_COUNT_' %}
      {% if dmf_key in existing_dmfs %}
        {% set sql_stmt %}
          ALTER TABLE {{ table_ref }}
            MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON (TABLE{{ table_ref }}())
            ADD EXPECTATION row_count_check ({{ expectation_expr | join(' AND ') }})
        {% endset %}
      {% else %}
        {% set sql_stmt %}
          ALTER TABLE {{ table_ref }}
            ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON (TABLE{{ table_ref }}())
            EXPECTATION row_count_check ({{ expectation_expr | join(' AND ') }})
        {% endset %}
      {% endif %}
      {% do sql_statements.append(sql_stmt) %}
    {% endif %}
  {% endif %}
  
  {# Custom DMF Checks #}
  {% if monitoring_config.get('custom_checks') %}
    {% for custom_check in monitoring_config.custom_checks %}
      {% set expectation_name = 'custom_' ~ loop.index %}
      {% set column_clause = '(' ~ custom_check.column ~ ')' if custom_check.get('column') else '(TABLE' ~ table_ref ~ '())' %}
      {% set dmf_name = custom_check.dmf.split('.')[-1] %}
      {% set dmf_key = dmf_name.upper() ~ '_' ~ (custom_check.column.upper() if custom_check.get('column') else '') %}
      {% if dmf_key in existing_dmfs %}
        {% set sql_stmt %}
          ALTER TABLE {{ table_ref }}
            MODIFY DATA METRIC FUNCTION {{ custom_check.dmf }} ON {{ column_clause }}
            ADD EXPECTATION {{ expectation_name }} ({{ custom_check.expectation }})
        {% endset %}
      {% else %}
        {% set sql_stmt %}
          ALTER TABLE {{ table_ref }}
            ADD DATA METRIC FUNCTION {{ custom_check.dmf }} ON {{ column_clause }}
            EXPECTATION {{ expectation_name }} ({{ custom_check.expectation }})
        {% endset %}
      {% endif %}
      {% do sql_statements.append(sql_stmt) %}
    {% endfor %}
  {% endif %}
  
  {# Execute all SQL statements #}
  {% for sql_stmt in sql_statements %}
    {% if execute %}
      {% do run_query(sql_stmt) %}
    {% endif %}
  {% endfor %}
  
{% endmacro %}

{% macro remove_data_quality_monitoring(model_name) %}
  {# 
    Macro to remove all data quality monitoring from a model
    
    Parameters:
    - model_name: Name of the model to remove monitoring from
  #}
  
  {# Query to get all existing DMF associations for the table #}
  {% set get_dmfs_query %}
    SELECT DISTINCT metric_name, ref_entity_name
    FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_EXPECTATIONS(
      REF_ENTITY_NAME => '{{ model_name.upper() }}',
      REF_ENTITY_DOMAIN => 'table'
    ))
  {% endset %}
  
  {% if execute %}
    {% set dmf_results = run_query(get_dmfs_query) %}
    {% for dmf_row in dmf_results %}
      {% set remove_sql %}
        ALTER TABLE {{ ref(model_name) }}
          DROP DATA METRIC FUNCTION {{ dmf_row[0] }}
      {% endset %}
      {% do run_query(remove_sql) %}
    {% endfor %}
    
    {# Unset the data metric schedule parameter #}
    {% set unset_schedule_sql %}
      ALTER TABLE {{ ref(model_name) }} UNSET DATA_METRIC_SCHEDULE
    {% endset %}
    {% do run_query(unset_schedule_sql) %}
  {% endif %}
  
{% endmacro %}

{% macro test_data_quality_expectations(model_name) %}
  {# 
    Macro to test current data quality expectations for a model
    
    Parameters:
    - model_name: Name of the model to test
    
    Returns: Results of expectation evaluation
  #}
  
  {% set test_query %}
    SELECT *
    FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(
        REF_ENTITY_NAME => '{{ model_name.upper() }}'))
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(test_query) %}
    {{ return(results) }}
  {% endif %}
  
{% endmacro %}

{% macro get_data_quality_violations(days_back=7) %}
  {# 
    Macro to retrieve data quality violations from the last N days
    
    Parameters:
    - days_back: Number of days to look back for violations (default: 7)
  #}
  
  {% set violations_query %}
    SELECT 
      timestamp,
      resource_attributes:snow.data_metric.ref_entity_name::STRING AS table_name,
      resource_attributes:snow.data_metric.expectation_name::STRING AS expectation_name,
      resource_attributes:snow.data_metric.expectation_expression::STRING AS expectation_expression,
      value AS is_violated,
      resource_attributes:snow.data_metric.dmf_name::STRING AS dmf_name
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS_RAW
    WHERE resource_attributes:snow.data_metric.record_type::STRING = 'EXPECTATION_VIOLATION_STATUS'
      AND value = TRUE  -- Only violations
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL '{{ days_back }} days'
    ORDER BY timestamp DESC
  {% endset %}
  
  {{ return(violations_query) }}
  
{% endmacro %}

{% macro get_model_data_quality_config(model_name) %}
  {# 
    Helper macro to get the data quality config for a specific model
    This can be used to inspect what monitoring is configured for a model
  #}
  
  {% set model_config = graph.nodes.get('model.' ~ project_name ~ '.' ~ model_name, {}).get('config', {}) %}
  {{ return(model_config.get('data_quality_config', {})) }}
  
{% endmacro %} 