-- Test for company UID uniqueness across all layers
WITH uid_counts AS (
  SELECT 
    company_uid,
    COUNT(*) as uid_count
  FROM {{ ref('silver_companies') }}
  GROUP BY company_uid
  HAVING COUNT(*) > 1
)

SELECT *
FROM uid_counts 