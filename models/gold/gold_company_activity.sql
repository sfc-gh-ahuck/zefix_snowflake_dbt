{{
  config(
    materialized='incremental',
    unique_key=['company_uid', 'shab_id'],
    on_schema_change='fail',
    incremental_strategy='merge',
    merge_exclude_columns=['_loaded_at'],
    data_quality_config={
      'schedule': '1 HOUR',
      'null_checks': [
        {'column': 'company_uid', 'max_nulls': 0},
        {'column': 'shab_id', 'max_nulls': 0},
        {'column': 'shab_date', 'max_nulls': 0},
        {'column': 'activity_type', 'max_nulls': 1000}
      ],
      'freshness_check': {'column': '_loaded_at', 'max_age_hours': 25},
      'row_count': {'min_rows': 5000, 'max_rows': 100000000}
    },
    post_hook="{{ apply_data_quality_from_config() }}"
  )
}}

-- Gold layer: Company activity analysis
SELECT 
    -- Company identification
    c.company_uid,
    c.company_name,
    c.company_status,
    c.legal_seat,
    
    -- Publication details
    p.shab_id,
    p.shab_date,
    p.registry_office_canton,
    p.publication_message,
    
    -- Mutation analysis
    COALESCE(mut_agg.mutation_count, 0) AS mutation_count,
    mut_agg.mutation_types,
    
    -- Activity classification
    CASE 
        WHEN p.publication_message ILIKE '%gründung%' OR p.publication_message ILIKE '%constitution%' THEN 'Formation'
        WHEN p.publication_message ILIKE '%auflösung%' OR p.publication_message ILIKE '%dissolution%' THEN 'Dissolution'
        WHEN p.publication_message ILIKE '%kapital%' OR p.publication_message ILIKE '%capital%' THEN 'Capital Change'
        WHEN p.publication_message ILIKE '%adresse%' OR p.publication_message ILIKE '%address%' THEN 'Address Change'
        WHEN p.publication_message ILIKE '%verwaltung%' OR p.publication_message ILIKE '%administration%' THEN 'Management Change'
        WHEN p.publication_message ILIKE '%zweck%' OR p.publication_message ILIKE '%purpose%' THEN 'Purpose Change'
        WHEN p.publication_message ILIKE '%fusion%' OR p.publication_message ILIKE '%merger%' THEN 'Merger'
        ELSE 'Other'
    END AS activity_type,
    
    -- Time dimensions
    EXTRACT(YEAR FROM p.shab_date) AS publication_year,
    EXTRACT(MONTH FROM p.shab_date) AS publication_month,
    EXTRACT(QUARTER FROM p.shab_date) AS publication_quarter,
    DATE_TRUNC('month', p.shab_date) AS publication_month_start,
    
    -- Recency flags
    CASE 
        WHEN p.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN 'Last 30 days'
        WHEN p.shab_date >= CURRENT_DATE() - INTERVAL '90 days' THEN 'Last 90 days'
        WHEN p.shab_date >= CURRENT_DATE() - INTERVAL '365 days' THEN 'Last year'
        ELSE 'Older'
    END AS recency_bucket,
    
    -- Metadata
    p._loaded_at

FROM {{ ref('silver_companies') }} AS c
INNER JOIN {{ ref('silver_shab_publications') }} AS p
    ON c.company_uid = p.company_uid
LEFT JOIN (
    SELECT 
        company_uid,
        shab_id,
        COUNT(*) AS mutation_count,
        ARRAY_AGG(DISTINCT mutation_type_key) AS mutation_types
    FROM {{ ref('silver_mutation_types') }}
    
    {% if is_incremental() %}
    -- Filter mutations for incremental load based on loaded_at
    WHERE _loaded_at > (
      SELECT MAX(_loaded_at) 
      FROM {{ this }}
    )
    {% endif %}
    
    GROUP BY company_uid, shab_id
) AS mut_agg ON p.company_uid = mut_agg.company_uid AND p.shab_id = mut_agg.shab_id

WHERE c.is_active = TRUE 

{% if is_incremental() %}
  -- Incremental logic: only process recently loaded publications
  AND p._loaded_at > (
    SELECT MAX(_loaded_at) 
    FROM {{ this }}
  )
{% endif %} 