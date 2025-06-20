{{ config(materialized='ephemeral') }}

-- This is a utility model to set up data quality monitoring
-- Run this model to apply monitoring to all gold layer tables

-- Apply data quality monitoring to all gold models
{{ apply_gold_layer_monitoring() }}

-- Return a simple result to indicate completion
SELECT 'Data quality monitoring applied successfully' AS status 