{% macro extract_table_name_from_ref(model_ref) %}
  {#- Extract table name from dbt model reference like ref('table_name') -#}
  
  {%- if not model_ref -%}
    {{ return('unknown_table') }}
  {%- endif -%}
  
  {%- set model_ref_str = model_ref | string -%}
  
  {#- Handle ref() function -#}
  {%- if model_ref_str.startswith('ref(') -%}
    {%- set ref_pattern = "ref\\(['\"]([^'\"]+)['\"]" -%}
    {%- set match = modules.re.search(ref_pattern, model_ref_str) -%}
    {%- if match -%}
      {{ return(match.group(1)) }}
    {%- endif -%}
  {%- endif -%}
  
  {#- Return as-is if no ref() function, just clean quotes -#}
  {{ return(model_ref_str.strip("'\"")) }}

{% endmacro %} 