{% macro generate_tables_section(semantic_model_name, table_name, entities) %}
  {#- Generate the TABLES section of the semantic view -#}
  
  {%- set primary_key = none -%}
  
  {#- Find primary entity -#}
  {%- for entity in entities -%}
    {%- if entity.type == 'primary' -%}
      {%- set primary_key = entity.expr or entity.name -%}
      {%- break -%}
    {%- endif -%}
  {%- endfor -%}
  
  {%- if not primary_key -%}
    {#- If no primary key found, use first entity or default -#}
    {%- if entities -%}
      {%- set first_entity = entities[0] -%}
      {%- set primary_key = first_entity.expr or first_entity.name or 'id' -%}
    {%- else -%}
      {%- set primary_key = 'id' -%}
    {%- endif -%}
  {%- endif -%}
  
  {%- set table_alias = semantic_model_name -%}

{%- set result -%}
{{ table_alias }} AS {{ table_name }}
  PRIMARY KEY ({{ primary_key }})
{%- endset -%}

{{ return(result) }}

{% endmacro %} 