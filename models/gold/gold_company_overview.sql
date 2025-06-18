{{
  config(
    materialized='incremental',
    unique_key='company_uid',
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['last_updated_at']
  )
}}

-- Gold layer: Company overview with business metrics
SELECT 
    -- Company identification
    c.company_uid,
    c.company_uid_formatted,
    c.company_chid,
    c.company_chid_formatted,
    c.company_name,
    c.company_status,
    c.is_active,
    c.is_deleted,
    
    -- Legal information
    c.legal_form_id,
    CASE 
        WHEN c.legal_form_id = 1 THEN 'Einzelunternehmen'
        WHEN c.legal_form_id = 2 THEN 'Kollektivgesellschaft'
        WHEN c.legal_form_id = 3 THEN 'Aktiengesellschaft'
        WHEN c.legal_form_id = 4 THEN 'Kommanditgesellschaft'
        WHEN c.legal_form_id = 5 THEN 'Gesellschaft mit beschränkter Haftung'
        WHEN c.legal_form_id = 6 THEN 'Genossenschaft'
        WHEN c.legal_form_id = 7 THEN 'Verein'
        WHEN c.legal_form_id = 8 THEN 'Stiftung'
        ELSE 'Other'
    END AS legal_form_name,
    c.legal_seat,
    
    -- Address
    CONCAT_WS(' ', c.address_street, c.address_house_number) AS full_address,
    c.address_zip_code,
    c.address_town,
    c.address_country,
    
    -- Purpose
    c.company_purpose,
    LENGTH(c.company_purpose) AS purpose_length,
    
    -- Dates
    c.shab_date AS last_shab_date,
    c.delete_date,
    
    -- SHAB activity metrics
    COALESCE(shab_stats.total_publications, 0) AS total_shab_publications,
    COALESCE(shab_stats.last_publication_date, c.shab_date) AS last_publication_date,
    COALESCE(shab_stats.first_publication_date, c.shab_date) AS first_publication_date,
    DATEDIFF('day', COALESCE(shab_stats.first_publication_date, c.shab_date), CURRENT_DATE()) AS days_since_first_publication,
    DATEDIFF('day', COALESCE(shab_stats.last_publication_date, c.shab_date), CURRENT_DATE()) AS days_since_last_publication,
    
    -- Canton information
    shab_stats.primary_canton,
    shab_stats.unique_cantons_count,
    
    -- External links
    c.cantonal_excerpt_web_url,
    
    -- Metadata
    c._loaded_at AS last_updated_at

FROM {{ ref('silver_companies') }} AS c
LEFT JOIN (
    SELECT 
        company_uid,
        COUNT(*) AS total_publications,
        MAX(shab_date) AS last_publication_date,
        MIN(shab_date) AS first_publication_date,
        MODE(registry_office_canton) AS primary_canton,
        COUNT(DISTINCT registry_office_canton) AS unique_cantons_count
    FROM {{ ref('silver_shab_publications') }}
    
    {% if is_incremental() %}
    -- Filter publications for incremental load
    WHERE shab_date >= (
      SELECT DATEADD('day', -1, MAX(last_shab_date)) 
      FROM {{ this }}
    )
    {% endif %}
    
    GROUP BY company_uid
) AS shab_stats ON c.company_uid = shab_stats.company_uid

{% if is_incremental() %}
-- Incremental logic: only process companies with shabDate >= max shabDate in target table - 1 day (for overlap)
WHERE c.shab_date >= (
  SELECT DATEADD('day', -1, MAX(last_shab_date)) 
  FROM {{ this }}
)
{% endif %} 