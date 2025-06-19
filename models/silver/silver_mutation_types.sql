{{
  config(
    materialized='incremental',
    unique_key=['company_uid', 'shab_id', 'mutation_type_id'],
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['_loaded_at', '_content_hash']
  )
}}

-- Silver layer: Extract and normalize mutation types from SHAB publications
WITH publications_data AS (
  SELECT *
  FROM {{ ref('silver_shab_publications') }}
  WHERE mutation_types_json IS NOT NULL
    AND company_uid IS NOT NULL
    AND shab_id IS NOT NULL

  {% if is_incremental() %}
    -- Incremental logic: only process recently loaded data
    -- Deduplication will handle chronological ordering per publication
    AND _loaded_at > (
      SELECT MAX(_loaded_at) 
      FROM {{ this }}
    )
  {% endif %}
),

flattened_mutations AS (
  SELECT 
    -- Foreign keys
    pub.company_uid,
    pub.shab_id,
    
    -- Mutation type details (extracted from JSON)
    mut.value:id::string AS mutation_type_id_raw,
    mut.value:key::string AS mutation_type_key_raw,
    
    -- Metadata from parent publication
    pub.shab_date,
    pub.registry_office_canton,
    pub._loaded_at,
    pub._content_hash
    
  FROM publications_data AS pub,
  LATERAL FLATTEN(input => pub.mutation_types_json) AS mut
  
  WHERE mut.value:id IS NOT NULL
),

deduplicated_mutations AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY company_uid, shab_id, mutation_type_id_raw
      ORDER BY 
        shab_date DESC NULLS LAST,
        _loaded_at DESC,
        _content_hash DESC
    ) AS row_num
  FROM flattened_mutations
)

SELECT 
    -- Foreign keys
    company_uid,
    shab_id,
    
    -- Mutation type details (cleaned and typed)
    TRY_TO_NUMBER(mutation_type_id_raw) AS mutation_type_id,
    TRIM(mutation_type_key_raw) AS mutation_type_key,
    
    -- Metadata from parent publication
    shab_date,
    registry_office_canton,
    _loaded_at,
    _content_hash

FROM deduplicated_mutations
WHERE row_num = 1 