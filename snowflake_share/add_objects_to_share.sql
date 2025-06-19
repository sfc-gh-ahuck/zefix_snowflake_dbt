-- =====================================================
-- ZEFIX Data Platform - Add Objects to Share
-- =====================================================
-- This script creates shared views/tables and adds them
-- to the ZEFIX_DATA_PLATFORM_SHARE
-- =====================================================

-- Set context - using ACCOUNTADMIN for all operations
USE ROLE ACCOUNTADMIN;
USE DATABASE ZEFIX;
USE SCHEMA PROD;

-- Load configuration variables from share_config.sql
-- Make sure to run @share_config.sql first to set these variables
-- Or set them directly here to match your environment:
SET SOURCE_DATABASE = 'ZEFIX';  -- Your actual dbt target database  
SET SOURCE_SCHEMA = 'PROD';     -- Your actual dbt target schema

-- =====================================================
-- Share Semantic Views and Gold Models Directly
-- =====================================================
-- Note: All objects are now shared directly from ZEFIX.PROD database
-- Semantic views use REFERENCES privilege, other objects use SELECT privilege

-- Share semantic views directly using REFERENCES privilege
GRANT REFERENCES ON SEMANTIC VIEW sem_company_overview TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW sem_company_types_by_canton TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW sem_business_changes TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW sem_geographic_analysis TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT REFERENCES ON SEMANTIC VIEW sem_publication_activity TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- =====================================================
-- Share Gold Layer Models Directly
-- =====================================================

-- Share gold models directly using SELECT privilege (they're already in ZEFIX.PROD)
GRANT SELECT ON VIEW gold_company_overview TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT SELECT ON VIEW gold_company_activity TO SHARE ZEFIX_DATA_PLATFORM_SHARE;
GRANT SELECT ON VIEW gold_canton_statistics TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- =====================================================
-- Create Sample Data Views for Organization Listing
-- =====================================================

-- Create sample data views for consumer preview (optional)
CREATE OR REPLACE VIEW sample_company_data AS
SELECT * FROM sem_company_overview LIMIT 100;

CREATE OR REPLACE VIEW sample_geographic_data AS
SELECT * FROM sem_geographic_analysis LIMIT 50;

CREATE OR REPLACE VIEW sample_activity_data AS
SELECT * FROM sem_publication_activity LIMIT 100;

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
FROM sem_company_overview
UNION ALL
SELECT 
    'sem_company_types_by_canton' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT canton) as unique_cantons,
    MAX(last_updated) as last_refresh
FROM sem_company_types_by_canton
UNION ALL
SELECT 
    'sem_business_changes' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT company_uid) as unique_companies,
    MAX(last_updated) as last_refresh
FROM sem_business_changes
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
SELECT COUNT(*) as total_companies FROM sem_company_overview;
SELECT COUNT(*) as total_cantons FROM sem_company_types_by_canton;

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