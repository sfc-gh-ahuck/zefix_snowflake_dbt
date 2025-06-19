{% materialization semantic_view, default %}
  
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(
      database=target.database,
      schema=target.schema,
      identifier=identifier,
      type='view'
  ) -%}
  
  {%- set existing_relation = load_relation(target_relation) -%}
  
  {{ run_hooks(pre_hooks) }}
  
  {%- set build_sql -%}
    {{ sql }}
  {%- endset -%}
  
  {% if existing_relation %}
    {{ log("Replacing existing semantic view: " ~ target_relation, info=true) }}
  {% else %}
    {{ log("Creating new semantic view: " ~ target_relation, info=true) }}
  {% endif %}
  
  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}
  
  {{ run_hooks(post_hooks) }}
  
  {% do persist_docs(target_relation, model) %}
  
  {{ log("✅ Semantic view " ~ target_relation ~ " created successfully!", info=true) }}
  
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %} 