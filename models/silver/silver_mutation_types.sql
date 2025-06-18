{{
  config(
    materialized='incremental',
    unique_key=['company_uid', 'shab_id', 'mutation_type_id'],
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['_loaded_at', '_content_hash']
  )
}}

-- Silver layer: Mutation types from SHAB publications
SELECT 
    -- Foreign keys
    pub.company_uid,
    pub.shab_id,
    
    -- Mutation type details
    mut.value:id::number AS mutation_type_id,
    mut.value:key::string AS mutation_type_key,
    
    -- Metadata from parent publication
    pub.shab_date,
    pub.registry_office_canton,
    pub._loaded_at,
    pub._content_hash

FROM {{ ref('silver_shab_publications') }} AS pub,
LATERAL FLATTEN(input => pub.mutation_types_json) AS mut

WHERE pub.mutation_types_json IS NOT NULL 

{% if is_incremental() %}
  -- Incremental logic: only process records with shabDate >= max shabDate in target table - 1 day (for overlap)
  AND pub.shab_date >= (
    SELECT DATEADD('day', -1, MAX(shab_date)) 
    FROM {{ this }}
  )
{% endif %} 