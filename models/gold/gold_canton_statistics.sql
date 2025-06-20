{{
  config(
    materialized='incremental',
    unique_key='canton',
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['last_updated_at'],
    data_quality_config={
      'schedule': '60 MINUTE',
      'null_checks': [
        {'column': 'canton', 'max_nulls': 0},
        {'column': 'total_companies', 'max_nulls': 0},
        {'column': 'active_companies', 'max_nulls': 0}
      ],
      'freshness_check': {'column': 'last_updated_at', 'max_age_hours': 25},
      'row_count': {'min_rows': 20, 'max_rows': 30},
      'custom_checks': [
        {
          'dmf': 'SNOWFLAKE.CORE.DUPLICATE_COUNT', 
          'column': 'canton', 
          'expectation': 'VALUE = 0'
        }
      ]
    },
    post_hook="{{ apply_data_quality_from_config() }}"
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
    MAX(GREATEST(c._loaded_at, p._loaded_at)) AS last_updated_at

FROM {{ ref('silver_companies') }} AS c
LEFT JOIN {{ ref('silver_shab_publications') }} AS p
    ON c.company_uid = p.company_uid

{% if is_incremental() %}
-- Incremental logic: include records that have been recently loaded
WHERE (
  p._loaded_at > (
    SELECT MAX(last_updated_at) 
    FROM {{ this }}
  )
  OR c._loaded_at > (
    SELECT MAX(last_updated_at) 
    FROM {{ this }}
  )
)
{% endif %}

GROUP BY COALESCE(p.registry_office_canton, 'Unknown')
ORDER BY total_companies DESC 