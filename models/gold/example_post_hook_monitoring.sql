{{
  config(
    materialized='table',
    post_hook=[
      "{{ setup_data_quality_monitoring(this.name, {
          'null_checks': [
            {'column': 'id', 'max_nulls': 0}
          ],
          'freshness_check': {'column': 'updated_at', 'max_age_hours': 24},
          'row_count': {'min_rows': 100}
        }) 
      }}"
    ]
  )
}}

-- Example model showing how to automatically apply data quality monitoring via post-hooks
-- This approach ensures monitoring is applied every time the model is rebuilt

SELECT 
  1 as id,
  'Example record' as description,
  CURRENT_TIMESTAMP() as updated_at 