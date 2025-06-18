{{
  config(
    materialized='incremental',
    unique_key='canton',
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['last_updated_at']
  )
}}

-- Gold layer: Canton-level statistics
SELECT 
    -- Canton identification
    COALESCE(p.registry_office_canton, 'Unknown') AS canton,
    
    -- Company counts
    COUNT(DISTINCT c.company_uid) AS total_companies,
    COUNT(DISTINCT CASE WHEN c.is_active = TRUE THEN c.company_uid END) AS active_companies,
    COUNT(DISTINCT CASE WHEN c.is_deleted = TRUE THEN c.company_uid END) AS deleted_companies,
    
    -- Legal form distribution
    COUNT(DISTINCT CASE WHEN c.legal_form_id = 3 THEN c.company_uid END) AS aktiengesellschaft_count,
    COUNT(DISTINCT CASE WHEN c.legal_form_id = 5 THEN c.company_uid END) AS gmbh_count,
    COUNT(DISTINCT CASE WHEN c.legal_form_id = 1 THEN c.company_uid END) AS einzelunternehmen_count,
    COUNT(DISTINCT CASE WHEN c.legal_form_id = 7 THEN c.company_uid END) AS verein_count,
    COUNT(DISTINCT CASE WHEN c.legal_form_id = 8 THEN c.company_uid END) AS stiftung_count,
    
    -- Activity metrics
    COUNT(p.shab_id) AS total_publications,
    COUNT(DISTINCT DATE_TRUNC('month', p.shab_date)) AS active_months,
    MAX(p.shab_date) AS last_activity_date,
    MIN(p.shab_date) AS first_activity_date,
    
    -- Recent activity
    COUNT(CASE WHEN p.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN 1 END) AS publications_last_30_days,
    COUNT(CASE WHEN p.shab_date >= CURRENT_DATE() - INTERVAL '90 days' THEN 1 END) AS publications_last_90_days,
    COUNT(CASE WHEN p.shab_date >= CURRENT_DATE() - INTERVAL '365 days' THEN 1 END) AS publications_last_year,
    
    -- Calculated metrics
    ROUND(COUNT(p.shab_id)::FLOAT / NULLIF(COUNT(DISTINCT c.company_uid), 0), 2) AS avg_publications_per_company,
    ROUND(COUNT(DISTINCT CASE WHEN c.is_active = TRUE THEN c.company_uid END)::FLOAT / NULLIF(COUNT(DISTINCT c.company_uid), 0) * 100, 2) AS active_company_percentage,
    
    -- Metadata
    MAX(c._loaded_at) AS last_updated_at

FROM {{ ref('silver_companies') }} AS c
LEFT JOIN {{ ref('silver_shab_publications') }} AS p
    ON c.company_uid = p.company_uid

{% if is_incremental() %}
-- Incremental logic: include companies that have had recent publications or updates
WHERE (
  p.shab_date >= (
    SELECT DATEADD('day', -1, MAX(last_activity_date)) 
    FROM {{ this }}
  )
  OR c.shab_date >= (
    SELECT DATEADD('day', -1, MAX(last_activity_date)) 
    FROM {{ this }}
  )
)
{% endif %}

GROUP BY COALESCE(p.registry_office_canton, 'Unknown')
ORDER BY total_companies DESC 