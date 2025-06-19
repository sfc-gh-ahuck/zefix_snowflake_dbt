{{
  config(
    materialized='incremental',
    unique_key=['company_uid', 'shab_id'],
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['_loaded_at', '_content_hash']
  )
}}

-- Silver layer: Extract and normalize SHAB publications from Bronze companies data
WITH bronze_data AS (
  SELECT 
    uid AS company_uid,
    shab_publications_json,
    _loaded_at,
    _content_hash
  FROM {{ ref('silver_zefix_companies_raw') }}
  WHERE shab_publications_json IS NOT NULL
    AND uid IS NOT NULL

  {% if is_incremental() %}
    -- Incremental logic: only process recently loaded data
    -- Individual publication deduplication will handle chronological ordering
    AND _loaded_at > (
      SELECT MAX(_loaded_at) 
      FROM {{ this }}
    )
  {% endif %}
),

flattened_publications AS (
  SELECT 
    -- Metadata fields
    bronze._loaded_at,
    bronze._content_hash,
    
    -- Company reference
    bronze.company_uid,
    
    -- SHAB publication details (extracted from JSON)
    pub.value:shabId::string AS shab_id_raw,
    pub.value:shabNr::string AS shab_number_raw,
    pub.value:shabDate::string AS shab_date_raw,
    pub.value:shabPage::string AS shab_page_raw,
    pub.value:shabMutationStatus::string AS shab_mutation_status_raw,
    
    -- Registry office information
    pub.value:registryOfficeId::string AS registry_office_id_raw,
    pub.value:registryOfficeCanton::string AS registry_office_canton_raw,
    pub.value:registryOfficeJournalDate::string AS registry_office_journal_date_raw,
    pub.value:registryOfficeJournalId::string AS registry_office_journal_id_raw,
    
    -- Publication content
    pub.value:message::string AS publication_message_raw,
    
    -- Mutation types (JSON array for downstream processing)
    pub.value:mutationTypes AS mutation_types_json
    
  FROM bronze_data AS bronze,
  LATERAL FLATTEN(input => bronze.shab_publications_json) AS pub
  
  WHERE pub.value:shabId IS NOT NULL
    AND pub.value:shabDate IS NOT NULL
),

deduplicated_publications AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY company_uid, shab_id_raw
      ORDER BY 
        TRY_TO_DATE(shab_date_raw, 'YYYY-MM-DD') DESC NULLS LAST,
        _loaded_at DESC,
        _content_hash DESC
    ) AS row_num
  FROM flattened_publications
)

SELECT 
    -- Foreign key to company
    company_uid,
    
    -- SHAB publication details (cleaned and typed)
    TRY_TO_NUMBER(shab_id_raw) AS shab_id,
    TRY_TO_NUMBER(shab_number_raw) AS shab_number,
    TRY_TO_DATE(shab_date_raw, 'YYYY-MM-DD') AS shab_date,
    TRY_TO_NUMBER(shab_page_raw) AS shab_page,
    TRY_TO_NUMBER(shab_mutation_status_raw) AS shab_mutation_status,
    
    -- Registry office information (cleaned and typed)
    TRY_TO_NUMBER(registry_office_id_raw) AS registry_office_id,
    UPPER(TRIM(registry_office_canton_raw)) AS registry_office_canton,
    TRY_TO_DATE(registry_office_journal_date_raw, 'YYYY-MM-DD') AS registry_office_journal_date,
    TRY_TO_NUMBER(registry_office_journal_id_raw) AS registry_office_journal_id,
    
    -- Publication message (cleaned)
    TRIM(publication_message_raw) AS publication_message,
    
    -- Mutation types (JSON array preserved for downstream processing)
    mutation_types_json,
    
    -- Metadata
    _loaded_at,
    _content_hash

FROM deduplicated_publications
WHERE row_num = 1 