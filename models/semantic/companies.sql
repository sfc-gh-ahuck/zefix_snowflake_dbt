{{
  config(
    materialized='semantic_view'
  )
}}

-- This model will generate a Snowflake semantic view based on the 'companies' semantic model
-- The semantic_view materialization will read the semantic model definition from semantic_models.yml
-- and create the corresponding CREATE SEMANTIC VIEW SQL statement

SELECT 1 as placeholder 