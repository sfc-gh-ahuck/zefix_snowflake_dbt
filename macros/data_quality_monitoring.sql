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
  
  {# NULL Count Checks - Try MODIFY first, then ADD if it fails #}
  {% if monitoring_config.get('null_checks') %}
    {% for null_check in monitoring_config.null_checks %}
      {% set expectation_name = 'null_check_' ~ null_check.column %}
      {# Try to modify existing DMF first #}
      {% set modify_sql_stmt %}
        ALTER TABLE {{ table_ref }}
          MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ({{ null_check.column }})
          SET EXPECTATION {{ expectation_name }} (VALUE <= {{ null_check.max_nulls }})
      {% endset %}
      {# Fallback to add if modify fails #}
      {% set add_sql_stmt %}
        ALTER TABLE {{ table_ref }}
          ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ({{ null_check.column }})
          EXPECTATION {{ expectation_name }} (VALUE <= {{ null_check.max_nulls }})
      {% endset %}
      {% do sql_statements.append({'modify': modify_sql_stmt, 'add': add_sql_stmt}) %}
    {% endfor %}
  {% endif %}
  
  {# Freshness Check - Try MODIFY first, then ADD if it fails #}
  {% if monitoring_config.get('freshness_check') %}
    {% set freshness_config = monitoring_config.freshness_check %}
    {% set max_age_seconds = freshness_config.max_age_hours * 3600 %}
    {# Try to modify existing DMF first #}
    {% set modify_freshness_sql %}
      ALTER TABLE {{ table_ref }}
        MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({{ freshness_config.column }})
        SET EXPECTATION freshness_check (VALUE <= {{ max_age_seconds }})
    {% endset %}
    {# Fallback to add if modify fails #}
    {% set add_freshness_sql %}
      ALTER TABLE {{ table_ref }}
        ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({{ freshness_config.column }})
        EXPECTATION freshness_check (VALUE <= {{ max_age_seconds }})
    {% endset %}
    {% do sql_statements.append({'modify': modify_freshness_sql, 'add': add_freshness_sql}) %}
  {% endif %}
  
  {# Row Count Check - Try MODIFY first, then ADD if it fails #}
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
      {# Try to modify existing DMF first #}
      {% set modify_row_count_sql %}
        ALTER TABLE {{ table_ref }}
          MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON (TABLE{{ table_ref }}())
          SET EXPECTATION row_count_check ({{ expectation_expr | join(' AND ') }})
      {% endset %}
      {# Fallback to add if modify fails #}
      {% set add_row_count_sql %}
        ALTER TABLE {{ table_ref }}
          ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON (TABLE{{ table_ref }}())
          EXPECTATION row_count_check ({{ expectation_expr | join(' AND ') }})
      {% endset %}
      {% do sql_statements.append({'modify': modify_row_count_sql, 'add': add_row_count_sql}) %}
    {% endif %}
  {% endif %}
  
  {# Custom DMF Checks - Try MODIFY first, then ADD if it fails #}
  {% if monitoring_config.get('custom_checks') %}
    {% for custom_check in monitoring_config.custom_checks %}
      {% set expectation_name = 'custom_' ~ loop.index %}
      {% set column_clause = '(' ~ custom_check.column ~ ')' if custom_check.get('column') else '(TABLE' ~ table_ref ~ '())' %}
      {# Try to modify existing DMF first #}
      {% set modify_custom_sql %}
        ALTER TABLE {{ table_ref }}
          MODIFY DATA METRIC FUNCTION {{ custom_check.dmf }} ON {{ column_clause }}
          SET EXPECTATION {{ expectation_name }} ({{ custom_check.expectation }})
      {% endset %}
      {# Fallback to add if modify fails #}
      {% set add_custom_sql %}
        ALTER TABLE {{ table_ref }}
          ADD DATA METRIC FUNCTION {{ custom_check.dmf }} ON {{ column_clause }}
          EXPECTATION {{ expectation_name }} ({{ custom_check.expectation }})
      {% endset %}
      {% do sql_statements.append({'modify': modify_custom_sql, 'add': add_custom_sql}) %}
    {% endfor %}
  {% endif %}
  
  {# Execute all SQL statements with modify-first, add-fallback logic #}
  {% for sql_stmt in sql_statements %}
    {% if execute %}
      {% if sql_stmt is mapping and sql_stmt.get('modify') and sql_stmt.get('add') %}
        {# Try MODIFY first, fallback to ADD if it fails #}
        {% set modify_with_fallback %}
          BEGIN
            {{ sql_stmt.modify }};
          EXCEPTION
            WHEN STATEMENT_ERROR THEN
              -- If MODIFY fails (DMF doesn't exist), try ADD
              {{ sql_stmt.add }};
          END;
        {% endset %}
        {% do run_query(modify_with_fallback) %}
      {% else %}
        {# Execute regular statements (like schedule setting) #}
        {% do run_query(sql_stmt) %}
      {% endif %}
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