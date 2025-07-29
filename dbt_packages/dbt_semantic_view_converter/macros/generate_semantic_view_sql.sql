{% macro generate_semantic_view_sql(semantic_model_config, target_relation) %}
  {#- Generate the complete CREATE SEMANTIC VIEW SQL from semantic model config -#}
  
  {%- set model_name = semantic_model_config.name -%}
  {%- set description = semantic_model_config.description or '' -%}
  {%- set model_ref = semantic_model_config.model -%}
  {%- set entities = semantic_model_config.entities or [] -%}
  {%- set dimensions = semantic_model_config.dimensions or [] -%}
  {%- set measures = semantic_model_config.measures or [] -%}
  
  {#- Extract table name from model reference -#}
  {%- set table_name = dbt_semantic_view_converter.extract_table_name_from_ref(model_ref) -%}
  
  {#- Generate each section -#}
  {%- set tables_sql = dbt_semantic_view_converter.generate_tables_section(model_name, table_name, entities) -%}
  {%- set relationships_sql = dbt_semantic_view_converter.generate_relationships_section(entities) -%}
  {%- set facts_sql = dbt_semantic_view_converter.generate_facts_section(model_name, measures) -%}
  {%- set dimensions_sql = dbt_semantic_view_converter.generate_dimensions_section(model_name, dimensions) -%}
  {%- set metrics_sql = dbt_semantic_view_converter.generate_metrics_section(model_name, measures) -%}
  
  {%- set copy_grants_flag = var('dbt_semantic_view_converter:copy_grants', true) -%}

{%- set sql -%}
CREATE OR REPLACE SEMANTIC VIEW {{ target_relation }}
  TABLES (
{{ tables_sql | indent(4, true) }}
  )
{%- if relationships_sql %}
  RELATIONSHIPS (
{{ relationships_sql | indent(4, true) }}
  )
{%- endif %}
{%- if facts_sql %}
  FACTS (
{{ facts_sql | indent(4, true) }}
  )
{%- endif %}
{%- if dimensions_sql %}
  DIMENSIONS (
{{ dimensions_sql | indent(4, true) }}
  )
{%- endif %}
{%- if metrics_sql %}
  METRICS (
{{ metrics_sql | indent(4, true) }}
  )
{%- endif %}
{%- if description %}
  COMMENT = '{{ description | replace("'", "''") }}'
{%- endif %}
{%- endset -%}

{{ return(sql) }}

{% endmacro %} 