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
    COALESCE(lf.legal_form_name_de, 'Other') AS legal_form_name,
    lf.legal_form_name_en,
    lf.abbreviation,
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
LEFT JOIN {{ ref('legal_forms') }} AS lf
    ON c.legal_form_id = lf.legal_form_id
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
    -- Filter publications for incremental load based on loaded_at
    WHERE _loaded_at > (
      SELECT MAX(last_updated_at) 
      FROM {{ this }}
    )
    {% endif %}
    
    GROUP BY company_uid
) AS shab_stats ON c.company_uid = shab_stats.company_uid

{% if is_incremental() %}
-- Incremental logic: only process companies with _loaded_at >= max last_updated_at in target table
WHERE c._loaded_at > (
  SELECT MAX(last_updated_at) 
  FROM {{ this }}
)
{% endif %} 