{% macro generate_metrics_section(table_alias, measures) %}
  {#- Generate the METRICS section -#}
  
  {%- set metrics = [] -%}
  {%- set aggregation_mapping = {
    'sum': 'SUM',
    'avg': 'AVG',
    'average': 'AVG',
    'count': 'COUNT',
    'count_distinct': 'COUNT',
    'min': 'MIN',
    'max': 'MAX',
    'median': 'MEDIAN',
    'percentile': 'PERCENTILE_CONT',
    'sum_boolean': 'SUM'
  } -%}
  
  {%- for measure in measures -%}
    {%- set name = measure.name -%}
    {%- set agg = measure.agg or 'sum' -%}
    {%- set expr = measure.expr or name -%}
    {%- set description = measure.description or '' -%}
    
    {#- Map dbt aggregation to Snowflake -#}
    {%- set snowflake_agg = aggregation_mapping.get(agg.lower(), agg.upper()) -%}
    
    {#- Handle special cases -#}
    {%- set expr_str = expr | string -%}
    {%- if agg.lower() == 'count_distinct' -%}
      {%- set metric_expr = 'COUNT(DISTINCT ' ~ expr ~ ')' -%}
    {%- elif expr_str == '1' and agg.lower() in ['sum', 'count'] -%}
      {#- Handle count metrics like dbt's "expr: 1, agg: sum" -#}
      {%- set metric_expr = 'COUNT(*)' -%}
    {%- else -%}
      {%- set metric_expr = snowflake_agg ~ '(' ~ expr ~ ')' -%}
    {%- endif -%}
    
    {#- Generate cleaner metric names -#}
    {%- set metric_name = dbt_semantic_view_converter.generate_metric_name(name, agg) -%}
    
    {%- set metric_definition -%}
{{ table_alias }}.{{ metric_name }} AS {{ metric_expr }}
{%- if description %}
  COMMENT = '{{ description }}'
{%- endif %}
    {%- endset -%}
    
    {%- do metrics.append(metric_definition) -%}
  {%- endfor -%}
  
  {%- if metrics -%}
    {{ return(metrics | join(',\n')) }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}

{% endmacro %} 