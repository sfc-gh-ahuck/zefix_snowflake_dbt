{% macro generate_dimensions_section(table_alias, dimensions) %}
  {#- Generate the DIMENSIONS section -#}
  
  {%- set dims = [] -%}
  {%- set time_granularity_mapping = {
    'day': 'DAY',
    'week': 'WEEK',
    'month': 'MONTH',
    'quarter': 'QUARTER',
    'year': 'YEAR',
    'hour': 'HOUR',
    'minute': 'MINUTE'
  } -%}
  
  {%- for dimension in dimensions -%}
    {%- set name = dimension.name -%}
    {%- set expr = dimension.expr or name -%}
    {%- set description = dimension.description or '' -%}
    {%- set dim_type = dimension.type or 'categorical' -%}
    
    {#- Handle time dimensions with granularity -#}
    {%- if dim_type == 'time' -%}
      {%- set type_params = dimension.type_params or {} -%}
      {%- set granularity = type_params.time_granularity or 'day' -%}
      {%- if expr == name and granularity -%}
        {#- If no custom expression, add DATE_TRUNC for time dimensions -#}
        {%- set mapped_granularity = time_granularity_mapping.get(granularity, granularity.upper()) -%}
        {%- set expr = "DATE_TRUNC('" ~ mapped_granularity ~ "', " ~ name ~ ")" -%}
      {%- endif -%}
    {%- endif -%}
    
    {#- Format long expressions nicely -#}
    {%- set expr_str = expr | string -%}
    {%- if expr_str | length > 60 -%}
      {%- set expr_lines = dbt_semantic_view_converter.wrap_long_expression(expr_str) -%}
      {%- set dim_definition -%}
{{ table_alias }}.{{ name }} AS (
{{ expr_lines }}
)
{%- if description %}
  COMMENT = '{{ description }}'
{%- endif %}
      {%- endset -%}
    {%- else -%}
      {%- set dim_definition -%}
{{ table_alias }}.{{ name }} AS {{ expr }}
{%- if description %}
  COMMENT = '{{ description }}'
{%- endif %}
      {%- endset -%}
    {%- endif -%}
    
    {%- do dims.append(dim_definition) -%}
  {%- endfor -%}
  
  {%- if dims -%}
    {{ return(dims | join(',\n')) }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}

{% endmacro %} 