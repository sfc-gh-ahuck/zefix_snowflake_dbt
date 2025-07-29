{{ config(materialized='table') }}

-- Time spine model for MetricFlow
-- Based on dbt Labs documentation: https://docs.getdbt.com/docs/build/metricflow-time-spine#example-time-spine-tables

WITH spine AS (
  SELECT 
    DATEADD(
      'day',
      ROW_NUMBER() OVER (ORDER BY NULL) - 1,
      CAST('2020-01-01' AS DATE)
    ) AS date_day
  FROM TABLE(GENERATOR(ROWCOUNT => 4018)) -- 11 years of daily dates
)

SELECT 
  CAST(date_day AS DATE) AS date_day
FROM spine
WHERE date_day <= CAST('2030-12-31' AS DATE)
ORDER BY date_day 