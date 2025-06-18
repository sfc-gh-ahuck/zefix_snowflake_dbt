{{
  config(
    materialized='table'
  )
}}

-- Silver layer: Cleaned and structured company data
SELECT 
    -- Primary key
    uid AS company_uid,
    
    -- Company identifiers
    uid_formatted AS company_uid_formatted,
    chid AS company_chid,
    chid_formatted AS company_chid_formatted,
    ehraid AS company_ehraid,
    
    -- Basic company information
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

FROM {{ ref('bronze_zefix_companies') }}
WHERE company_name IS NOT NULL 