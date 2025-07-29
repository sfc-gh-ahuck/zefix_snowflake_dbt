{% macro wrap_long_expression(expr) %}
  {#- Wrap long expressions for better readability -#}
  
  {%- set expr_lower = expr.lower() -%}
  
  {#- Format case statements nicely -#}
  {%- if 'case ' in expr_lower -%}
    {%- set formatted_expr = expr -%}
    {%- set formatted_expr = formatted_expr | regex_replace('\\s+when\\s+', '\\n      WHEN ', ignorecase=true) -%}
    {%- set formatted_expr = formatted_expr | regex_replace('\\s+then\\s+', ' THEN ', ignorecase=true) -%}
    {%- set formatted_expr = formatted_expr | regex_replace('\\s+else\\s+', '\\n      ELSE ', ignorecase=true) -%}
    {%- set formatted_expr = formatted_expr | regex_replace('\\s+end\\s*', '\\n      END', ignorecase=true) -%}
    {{ return('    ' ~ formatted_expr) }}
  {%- else -%}
    {#- Simple line wrapping for other long expressions -#}
    {{ return('    ' ~ expr) }}
  {%- endif -%}

{% endmacro %}


{% macro generate_metric_name(measure_name, agg) %}
  {#- Generate a clean metric name based on measure name and aggregation -#}
  
  {%- set agg_lower = agg.lower() -%}
  
  {#- Common patterns for cleaner names -#}
  {%- if agg_lower == 'sum' -%}
    {%- if measure_name.endswith('_count') or measure_name == 'count' -%}
      {{ return('total_count') }}
    {%- elif measure_name.endswith('_total') or measure_name.endswith('_amount') or measure_name.endswith('_value') -%}
      {{ return(measure_name) }}  {#- Already descriptive -#}
    {%- else -%}
      {{ return('total_' ~ measure_name) }}
    {%- endif -%}
  {%- elif agg_lower == 'avg' or agg_lower == 'average' -%}
    {{ return('avg_' ~ measure_name) }}
  {%- elif agg_lower == 'count' -%}
    {%- if not measure_name.endswith('_count') -%}
      {{ return(measure_name ~ '_count') }}
    {%- else -%}
      {{ return(measure_name) }}
    {%- endif -%}
  {%- elif agg_lower == 'count_distinct' -%}
    {%- if not measure_name.startswith('unique_') -%}
      {{ return('unique_' ~ measure_name) }}
    {%- else -%}
      {{ return(measure_name) }}
    {%- endif -%}
  {%- elif agg_lower in ['min', 'max'] -%}
    {{ return(agg_lower ~ '_' ~ measure_name) }}
  {%- else -%}
    {{ return(agg_lower ~ '_' ~ measure_name) }}
  {%- endif -%}

{% endmacro %} 