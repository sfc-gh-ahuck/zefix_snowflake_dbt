{% macro generate_facts_section(table_alias, measures) %}
  {#- Generate the FACTS section for row-level data -#}
  
  {%- set facts = [] -%}
  
  {%- for measure in measures -%}
    {%- set name = measure.name -%}
    {%- set expr = measure.expr or name -%}
    {%- set description = measure.description or '' -%}
    {%- set agg = measure.agg or 'sum' -%}
    
    {#- Only include as facts if they represent row-level data -#}
    {#- Skip measures that are clearly aggregations for the metrics section -#}
    {%- set expr_str = expr | string -%}
    {%- if expr_str != '1' and agg.lower() not in ['count', 'count_distinct'] and not expr_str.upper().startswith('COUNT') -%}
      {%- set fact_definition -%}
{{ table_alias }}.{{ name }} AS {{ expr }}
{%- if description %}
  COMMENT = '{{ description }}'
{%- endif %}
      {%- endset -%}
      
      {%- do facts.append(fact_definition) -%}
    {%- endif -%}
  {%- endfor -%}
  
  {%- if facts -%}
    {{ return(facts | join(',\n')) }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}

{% endmacro %} 