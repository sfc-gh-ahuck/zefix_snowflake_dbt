{% materialization semantic_view, adapter='snowflake' %}

  {%- set full_refresh_mode = (should_full_refresh() or flags.FULL_REFRESH) -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(this) -%}

  {{ log("Creating semantic view: " ~ target_relation) }}

  {%- set semantic_model_name = model.name -%}
  {%- set semantic_model_config = dbt_semantic_view_converter.get_semantic_model_config(semantic_model_name) -%}
  
  {% if not semantic_model_config %}
    {{ exceptions.raise_compiler_error("No semantic model configuration found for model '" ~ semantic_model_name ~ "'. Please define it in your schema.yml file.") }}
  {% endif %}

  {%- set create_semantic_view_sql = dbt_semantic_view_converter.generate_semantic_view_sql(semantic_model_config, target_relation) -%}

  -- Setup
  {{ run_hooks(pre_hooks, inside_transaction=false) }}
  {{ run_hooks(pre_hooks, inside_transaction=true) }}

  {% call statement('main') -%}
    {{ create_semantic_view_sql }}
  {%- endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=true) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=false) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %} 