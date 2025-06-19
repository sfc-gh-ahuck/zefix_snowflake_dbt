-- =====================================================
-- ZEFIX Data Platform - Complete Deployment with Configuration
-- =====================================================
-- This script loads configuration and deploys the complete 
-- ZEFIX data sharing infrastructure with proper variable setup
-- =====================================================

-- Step 1: Load configuration
SELECT '🔧 Loading Configuration...' as STATUS;
@share_config.sql;

-- Verify configuration loaded correctly
SELECT 'Configuration loaded for database: ' || $SOURCE_DATABASE_NAME || '.' || $SOURCE_SCHEMA_NAME as CONFIG_STATUS;

-- Step 2: Create share infrastructure
SELECT '📋 Creating Share Infrastructure...' as STATUS;
@create_zefix_share.sql;

-- Step 3: Add objects to share with proper database context
SELECT '📊 Adding Objects to Share...' as STATUS;

-- Set context and execute object addition - using ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;
USE DATABASE ZEFIX;
USE SCHEMA PROD;

-- Verify source database exists and objects are accessible
SELECT 'Verifying source objects...' as VERIFICATION_STATUS;

-- Test access to source semantic views
SELECT COUNT(*) as sem_company_overview_count 
FROM ZEFIX.PROD.sem_company_overview LIMIT 1;

SELECT COUNT(*) as sem_geographic_analysis_count 
FROM ZEFIX.PROD.sem_geographic_analysis LIMIT 1;

-- If verification passes, add objects to share
@add_objects_to_share.sql;

-- Step 4: Create organization listing
SELECT '🏪 Creating Organization Marketplace Listing...' as STATUS;
@create_marketplace_listing.sql;

-- Final verification
SELECT '✅ Deployment Complete!' as FINAL_STATUS,
       'ZEFIX_DATA_PLATFORM_SHARE' as SHARE_NAME,
       'ZEFIX_SWISS_COMPANY_INTELLIGENCE' as LISTING_NAME,
       CURRENT_TIMESTAMP() as COMPLETED_AT;

-- Show final share contents
SHOW OBJECTS IN SHARE ZEFIX_DATA_PLATFORM_SHARE; 