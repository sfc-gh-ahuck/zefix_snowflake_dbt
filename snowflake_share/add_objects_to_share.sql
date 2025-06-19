-- =====================================================
-- ZEFIX Data Platform - Add Objects to Share
-- =====================================================
-- This script creates shared views/tables and adds them
-- to the ZEFIX_DATA_PLATFORM_SHARE
-- =====================================================

-- Set context
USE ROLE ZEFIX_SHARE_ADMIN;
USE DATABASE ZEFIX_SHARED_DB;
USE SCHEMA SHARED_DATA;

-- Variables (adjust these to match your source database/schema)
-- You may need to modify these based on your actual dbt target configuration
SET SOURCE_DATABASE = 'ZEFIX_DATA_PLATFORM';  -- Replace with your actual database name
SET SOURCE_SCHEMA = 'PUBLIC';                  -- Replace with your actual schema name

-- =====================================================
-- Share Semantic Views Directly
-- =====================================================
-- Note: Semantic views are shared directly from their source location
-- using GRANT REFERENCES ON SEMANTIC VIEW syntax per Snowflake docs:
-- https://docs.snowflake.com/en/sql-reference/sql/grant-privilege-share

-- Grant USAGE on source database and schema for semantic views
GRANT USAGE ON DATABASE IDENTIFIER($SOURCE_DATABASE) TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT USAGE ON SCHEMA IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA) TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- Share semantic views directly using REFERENCES privilege
GRANT REFERENCES ON SEMANTIC VIEW IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_company_overview') TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_company_types_by_canton') TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_business_changes') TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_geographic_analysis') TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_publication_activity') TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- =====================================================
-- Create Shared Gold Layer Tables/Views
-- =====================================================

-- Create shared gold view: Company Overview
CREATE OR REPLACE VIEW gold_company_overview AS
SELECT * FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.gold_company_overview');

-- Create shared gold view: Company Activity
CREATE OR REPLACE VIEW gold_company_activity AS
SELECT * FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.gold_company_activity');

-- Create shared gold view: Canton Statistics
CREATE OR REPLACE VIEW gold_canton_statistics AS
SELECT * FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.gold_canton_statistics');

-- =====================================================
-- Add Objects to Share
-- =====================================================

-- Semantic views are already shared directly above using REFERENCES privilege
-- No additional grants needed for semantic views

-- Add gold layer views to share
GRANT SELECT ON VIEW gold_company_overview TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT SELECT ON VIEW gold_company_activity TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT SELECT ON VIEW gold_canton_statistics TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- =====================================================
-- Create Sample Data Views (Optional)
-- =====================================================

-- Create sample data views referencing the source semantic views
-- Note: These are local views that reference the source semantic views for preview purposes
CREATE OR REPLACE VIEW sample_company_data AS
SELECT * FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_company_overview') LIMIT 100;

CREATE OR REPLACE VIEW sample_geographic_data AS
SELECT * FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_geographic_analysis') LIMIT 50;

CREATE OR REPLACE VIEW sample_activity_data AS
SELECT * FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_publication_activity') LIMIT 100;

-- Add sample views to share
GRANT SELECT ON VIEW sample_company_data TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT SELECT ON VIEW sample_geographic_data TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT SELECT ON VIEW sample_activity_data TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- =====================================================
-- Create Data Quality Summary
-- =====================================================

CREATE OR REPLACE VIEW data_quality_summary AS
SELECT 
    'sem_company_overview' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT company_uid) as unique_companies,
    MAX(last_updated) as last_refresh
FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_company_overview')
UNION ALL
SELECT 
    'sem_company_types_by_canton' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT canton) as unique_cantons,
    MAX(last_updated) as last_refresh
FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_company_types_by_canton')
UNION ALL
SELECT 
    'sem_business_changes' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT company_uid) as unique_companies,
    MAX(last_updated) as last_refresh
FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_business_changes')
UNION ALL
SELECT 
    'gold_company_overview' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT company_uid) as unique_companies,
    MAX(last_updated) as last_refresh
FROM gold_company_overview
UNION ALL
SELECT 
    'gold_company_activity' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT company_uid) as unique_companies,
    MAX(last_updated) as last_refresh
FROM gold_company_activity;

-- Add data quality summary to share
GRANT SELECT ON VIEW data_quality_summary TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- =====================================================
-- Verification and Status
-- =====================================================

-- Show all objects in the share
SELECT 'Objects successfully added to share!' as STATUS;

-- Display share contents
SHOW OBJECTS IN SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- Test queries to verify data access
SELECT 'Testing semantic views...' as TEST_STATUS;
SELECT COUNT(*) as total_companies FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_company_overview');
SELECT COUNT(*) as total_cantons FROM IDENTIFIER($SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.sem_company_types_by_canton');

SELECT 'Testing gold views...' as TEST_STATUS;
SELECT COUNT(*) as company_records FROM gold_company_overview;
SELECT COUNT(*) as activity_records FROM gold_company_activity;

-- Show share summary
SELECT 
    'ZEFIX_DATA_PLATFORM_SHARE' as share_name,
    COUNT(*) as total_objects,
    CURRENT_TIMESTAMP() as setup_completed
FROM (
    SELECT 'sem_company_overview' as object_name
    UNION ALL SELECT 'sem_company_types_by_canton'
    UNION ALL SELECT 'sem_business_changes'
    UNION ALL SELECT 'sem_geographic_analysis'
    UNION ALL SELECT 'sem_publication_activity'
    UNION ALL SELECT 'gold_company_overview'
    UNION ALL SELECT 'gold_company_activity'
    UNION ALL SELECT 'gold_canton_statistics'
    UNION ALL SELECT 'sample_company_data'
    UNION ALL SELECT 'sample_geographic_data'
    UNION ALL SELECT 'sample_activity_data'
    UNION ALL SELECT 'data_quality_summary'
    UNION ALL SELECT 'share_documentation'
); 