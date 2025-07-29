{% macro get_semantic_model_config(model_name) %}
  {#- Get the semantic model configuration for a given model name -#}
  
  {%- set semantic_model_config = none -%}
  
  {%- if graph.nodes -%}
    {%- for node_id, node in graph.nodes.items() -%}
      {%- if node.resource_type == 'semantic_model' -%}
        {{ log("Found node in nodes: " ~ node.name) }}
        {{ log("Found node for model_name: " ~ model_name) }}
        {%- if node.name == model_name -%}
          {%- set semantic_model_config = node -%}
          {%- break -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  
  {%- if not semantic_model_config -%}
    {%- if graph.get('semantic_models') -%}
      {%- for semantic_model_id, semantic_model in graph.semantic_models.items() -%}
        {{ log("Found node in sem: " ~ semantic_model.name) }}
        {{ log("Found node for model_name: " ~ model_name) }}
        {%- if semantic_model.name == model_name -%}
          {{ log("YaaaaY! " ~ semantic_model) }}
          {%- set semantic_model_config = semantic_model -%}
          {{ return(semantic_model_config) }}
          {%- break -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  {{ log("semantic_model_config internal: " ~ semantic_model_config) }}
  {{ return(semantic_model_config) }}

{% endmacro %} 