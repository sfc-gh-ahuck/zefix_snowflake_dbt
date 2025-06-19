{{
  config(
    materialized='view',
    post_hook=[
      "{{ create_zefix_semantic_view() }}"
    ]
  )
}}

-- This model serves as a trigger to deploy the ZEFIX semantic view
-- The semantic view is created via the post-hook macro after this model runs
-- This ensures the semantic view is always up-to-date with the latest dbt models

SELECT 
  'ZEFIX Semantic View Deployer' as deployment_status,
  CURRENT_TIMESTAMP() as deployed_at,
  '{{ target.schema }}' as target_schema,
  '{{ target.database }}' as target_database

-- The actual semantic view creation happens in the post-hook macro 