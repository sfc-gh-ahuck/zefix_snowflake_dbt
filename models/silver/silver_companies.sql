{{
  config(
    materialized='incremental',
    unique_key='company_uid',
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['_loaded_at', '_content_hash']
  )
}}

-- Silver layer: Cleaned and structured company data with comprehensive deduplication
-- Primary deduplication point since Bronze uses append-only strategy
WITH bronze_data AS (
  SELECT *
  FROM {{ ref('bronze_zefix_companies') }}
  WHERE company_name IS NOT NULL 
    AND shab_date IS NOT NULL

  {% if is_incremental() %}
    -- Incremental logic: only process records with shabDate >= max shabDate in target table - 1 day (for overlap)
    AND TRY_TO_DATE(shab_date, 'YYYY-MM-DD') >= (
      SELECT DATEADD('day', -1, MAX(shab_date)) 
      FROM {{ this }}
    )
  {% endif %}
),

-- Comprehensive deduplication with business logic prioritization
deduplicated_bronze AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY uid 
      ORDER BY 
        -- Prioritize most recent SHAB date (latest business activity)
        TRY_TO_DATE(shab_date, 'YYYY-MM-DD') DESC NULLS LAST,
        -- Then most recent load time (freshest data)
        _loaded_at DESC,
        -- Then content hash for deterministic tie-breaking
        _content_hash DESC
    ) AS row_num,
    
    -- Additional metrics for data quality insights
    COUNT(*) OVER (PARTITION BY uid) AS duplicate_count,
    MIN(TRY_TO_DATE(shab_date, 'YYYY-MM-DD')) OVER (PARTITION BY uid) AS first_shab_date,
    MAX(TRY_TO_DATE(shab_date, 'YYYY-MM-DD')) OVER (PARTITION BY uid) AS latest_shab_date
    
  FROM bronze_data
)

SELECT 
    -- Primary key
    uid AS company_uid,
    
    -- Company identifiers
    uid_formatted AS company_uid_formatted,
    chid AS company_chid,
    chid_formatted AS company_chid_formatted,
    ehraid AS company_ehraid,
    
    -- Basic company information (cleaned)
    TRIM(company_name) AS company_name,
    legal_form_id,
    TRIM(legal_seat) AS legal_seat,
    legal_seat_id,
    register_office_id,
    UPPER(company_status) AS company_status,
    TRIM(company_purpose) AS company_purpose,
    
    -- Address information (cleaned)
    TRIM(address_organisation) AS address_organisation,
    TRIM(address_care_of) AS address_care_of,
    TRIM(address_street) AS address_street,
    TRIM(address_house_number) AS address_house_number,
    TRIM(address_po_box) AS address_po_box,
    TRIM(address_zip_code) AS address_zip_code,
    TRIM(address_town) AS address_town,
    UPPER(TRIM(address_country)) AS address_country,
    TRIM(address_addon) AS address_addon,
    
    -- Date fields (converted to proper dates)
    TRY_TO_DATE(shab_date, 'YYYY-MM-DD') AS shab_date,
    TRY_TO_DATE(delete_date, 'YYYY-MM-DD') AS delete_date,
    
    -- URLs and external references
    cantonal_excerpt_web_url,
    rab_id,
    
    -- Status flags
    CASE 
        WHEN UPPER(company_status) = 'EXISTIEREND' THEN TRUE
        WHEN UPPER(company_status) = 'GELOESCHT' THEN FALSE
        ELSE NULL
    END AS is_active,
    
    CASE 
        WHEN delete_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_deleted,
    
    -- Data quality metrics (for monitoring and debugging)
    duplicate_count AS source_duplicate_count,
    first_shab_date AS first_observed_shab_date,
    latest_shab_date AS latest_observed_shab_date,
    CASE 
        WHEN duplicate_count > 1 THEN TRUE 
        ELSE FALSE 
    END AS had_duplicates_in_source,
    
    -- Metadata
    _loaded_at,
    _content_hash,
    
    -- JSON fields for further processing
    shab_publications_json,
    old_names_json,
    audit_firms_json,
    audit_firm_for_json,
    branch_offices_json,
    main_offices_json,
    further_main_offices_json,
    has_taken_over_json,
    was_taken_over_by_json,
    translation_json

FROM deduplicated_bronze
WHERE row_num = 1 