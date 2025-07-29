{% macro generate_relationships_section(entities) %}
  {#- Generate the RELATIONSHIPS section -#}
  
  {%- set relationships = [] -%}
  
  {%- for entity in entities -%}
    {%- if entity.type == 'foreign' -%}
      {%- set entity_name = entity.name -%}
      {%- set expr = entity.expr or entity_name -%}
      
      {#- Create a relationship name and infer target table -#}
      {%- set relationship_name = 'to_' ~ entity_name -%}
      {%- set target_table = entity_name.replace('_id', '').replace('id', '') or 'unknown' -%}
      
      {%- set relationship_def -%}
{{ relationship_name }} AS
  semantic_model ({{ expr }}) REFERENCES {{ target_table }}
      {%- endset -%}
      
      {%- do relationships.append(relationship_def) -%}
    {%- endif -%}
  {%- endfor -%}
  
  {%- if relationships -%}
    {{ return(relationships | join(',\n')) }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}

{% endmacro %} 