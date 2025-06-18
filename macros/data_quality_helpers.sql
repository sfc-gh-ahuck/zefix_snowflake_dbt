{% macro get_duplicate_stats(model_name, partition_key) %}
  -- Macro to generate duplicate statistics for any model
  SELECT 
    '{{ model_name }}' AS model_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT {{ partition_key }}) AS unique_keys,
    COUNT(*) - COUNT(DISTINCT {{ partition_key }}) AS duplicate_records,
    ROUND((COUNT(*) - COUNT(DISTINCT {{ partition_key }})) * 100.0 / COUNT(*), 2) AS duplicate_percentage
  FROM {{ ref(model_name) }}
{% endmacro %}

{% macro log_deduplication_summary() %}
  -- Log summary of deduplication across all layers
  {{ log("=== DEDUPLICATION SUMMARY ===", info=true) }}
  {{ log("Bronze: Append-only (preserves source duplicates)", info=true) }}
  {{ log("Silver: Primary deduplication with business logic", info=true) }}
  {{ log("Gold: Incremental with merge strategy", info=true) }}
{% endmacro %} 