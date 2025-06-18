{{
  config(
    materialized='incremental',
    unique_key=['company_uid', 'shab_id'],
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['_loaded_at', '_content_hash']
  )
}}

-- Silver layer: SHAB (Swiss Official Gazette of Commerce) publications
SELECT 
    -- Foreign key to company
    uid AS company_uid,
    
    -- SHAB publication details
    pub.value:shabId::number AS shab_id,
    pub.value:shabNr::number AS shab_number,
    TRY_TO_DATE(pub.value:shabDate::string, 'YYYY-MM-DD') AS shab_date,
    pub.value:shabPage::number AS shab_page,
    pub.value:shabMutationStatus::number AS shab_mutation_status,
    
    -- Registry office information
    pub.value:registryOfficeId::number AS registry_office_id,
    pub.value:registryOfficeCanton::string AS registry_office_canton,
    TRY_TO_DATE(pub.value:registryOfficeJournalDate::string, 'YYYY-MM-DD') AS registry_office_journal_date,
    pub.value:registryOfficeJournalId::number AS registry_office_journal_id,
    
    -- Publication message
    pub.value:message::string AS publication_message,
    
    -- Mutation types (array)
    pub.value:mutationTypes AS mutation_types_json,
    
    -- Metadata
    _loaded_at,
    _content_hash

FROM {{ ref('bronze_zefix_companies') }} AS base,
LATERAL FLATTEN(input => base.shab_publications_json) AS pub

WHERE shab_publications_json IS NOT NULL 

{% if is_incremental() %}
  -- Incremental logic: only process records with shabDate >= max shabDate in target table - 1 day (for overlap)
  AND TRY_TO_DATE(pub.value:shabDate::string, 'YYYY-MM-DD') >= (
    SELECT DATEADD('day', -1, MAX(shab_date)) 
    FROM {{ this }}
  )
{% endif %} 