-- =====================================================
-- ZEFIX Data Platform - Complete Share & Marketplace Deployment
-- =====================================================
-- Master script to deploy the complete ZEFIX data sharing
-- infrastructure including share creation, object addition,
-- and marketplace listing setup
-- =====================================================

-- Check prerequisites
SELECT '🚀 Starting ZEFIX Data Platform Share & Marketplace Deployment' as DEPLOYMENT_STATUS,
       CURRENT_TIMESTAMP() as START_TIME;

-- Verify current role and context
SELECT CURRENT_ROLE() as CURRENT_ROLE,
       CURRENT_DATABASE() as CURRENT_DATABASE,
       CURRENT_SCHEMA() as CURRENT_SCHEMA;

-- =====================================================
-- STEP 1: Create Share Infrastructure
-- =====================================================

SELECT '📋 STEP 1: Creating Share Infrastructure...' as STEP_STATUS;

-- Execute share creation script
@create_zefix_share.sql;

SELECT '✅ Share infrastructure created successfully!' as STEP_RESULT;

-- =====================================================
-- STEP 2: Add Objects to Share
-- =====================================================

SELECT '📊 STEP 2: Adding Objects to Share...' as STEP_STATUS;

-- Execute object addition script
@add_objects_to_share.sql;

SELECT '✅ Objects added to share successfully!' as STEP_RESULT;

-- =====================================================
-- STEP 3: Create Marketplace Listing
-- =====================================================

SELECT '🏪 STEP 3: Creating Organization Marketplace Listing...' as STEP_STATUS;

-- Execute marketplace listing creation
@create_marketplace_listing.sql;

SELECT '✅ Organization marketplace listing created successfully!' as STEP_RESULT;

-- =====================================================
-- DEPLOYMENT VERIFICATION
-- =====================================================

SELECT '🔍 Verifying Deployment...' as VERIFICATION_STATUS;

-- Check share exists and has objects
SHOW SHARES LIKE 'ZEFIX_DATA_PLATFORM_SHARE';
SHOW OBJECTS IN SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- Check organization listing exists
SHOW ORGANIZATION LISTINGS LIKE 'ZEFIX_SWISS_COMPANY_INTELLIGENCE';

-- Verify data access through shared objects
USE DATABASE ZEFIX_SHARED_DB;
USE SCHEMA SHARED_DATA;

-- Test semantic views
SELECT 'Testing sem_company_overview...' as TEST_NAME,
       COUNT(*) as RECORD_COUNT
FROM sem_company_overview
LIMIT 1;

SELECT 'Testing sem_geographic_analysis...' as TEST_NAME,
       COUNT(*) as RECORD_COUNT  
FROM sem_geographic_analysis
LIMIT 1;

-- Test gold views
SELECT 'Testing gold_company_overview...' as TEST_NAME,
       COUNT(*) as RECORD_COUNT
FROM gold_company_overview
LIMIT 1;

-- Test documentation and sample data
SELECT 'Testing share documentation...' as TEST_NAME,
       COUNT(*) as RECORD_COUNT
FROM share_documentation;

SELECT 'Testing data quality summary...' as TEST_NAME,
       COUNT(*) as RECORD_COUNT
FROM data_quality_summary;

-- =====================================================
-- DEPLOYMENT SUMMARY
-- =====================================================

SELECT '📈 DEPLOYMENT SUMMARY' as SECTION_TITLE;

-- Share summary
SELECT 
    'ZEFIX_DATA_PLATFORM_SHARE' as SHARE_NAME,
    'ZEFIX_SHARED_DB.SHARED_DATA' as SCHEMA_LOCATION,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'SHARED_DATA') as TOTAL_VIEWS,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SHARED_DATA') as TOTAL_TABLES;

-- Objects in share
SELECT 
    'Shared Objects' as CATEGORY,
    object_name,
    object_type
FROM (
    SELECT 'sem_company_overview' as object_name, 'SEMANTIC_VIEW' as object_type
    UNION ALL SELECT 'sem_company_types_by_canton', 'SEMANTIC_VIEW'
    UNION ALL SELECT 'sem_business_changes', 'SEMANTIC_VIEW'
    UNION ALL SELECT 'sem_geographic_analysis', 'SEMANTIC_VIEW'
    UNION ALL SELECT 'sem_publication_activity', 'SEMANTIC_VIEW'
    UNION ALL SELECT 'gold_company_overview', 'GOLD_VIEW'
    UNION ALL SELECT 'gold_company_activity', 'GOLD_VIEW'
    UNION ALL SELECT 'gold_canton_statistics', 'GOLD_VIEW'
    UNION ALL SELECT 'sample_company_data', 'SAMPLE_VIEW'
    UNION ALL SELECT 'sample_geographic_data', 'SAMPLE_VIEW'
    UNION ALL SELECT 'sample_activity_data', 'SAMPLE_VIEW'
    UNION ALL SELECT 'data_quality_summary', 'UTILITY_VIEW'
    UNION ALL SELECT 'share_documentation', 'DOCUMENTATION'
)
ORDER BY object_type, object_name;

-- =====================================================
-- NEXT STEPS & INSTRUCTIONS
-- =====================================================

SELECT '
🎉 ZEFIX DATA PLATFORM SHARE & ORGANIZATION MARKETPLACE DEPLOYMENT COMPLETE!

✅ WHAT WAS CREATED:
• Share: ZEFIX_DATA_PLATFORM_SHARE
• Database: ZEFIX_SHARED_DB
• Schema: SHARED_DATA
• Organization Marketplace Listing: ZEFIX_SWISS_COMPANY_INTELLIGENCE
• 5 Semantic Views (Cortex Analyst Ready) - Shared Directly
• 3 Gold Layer Views
• 3 Sample Data Views
• Documentation & Data Quality Views

🔗 ACCESS INFORMATION:
• Share Name: ZEFIX_DATA_PLATFORM_SHARE
• Organization Listing Name: ZEFIX_SWISS_COMPANY_INTELLIGENCE
• Database: ZEFIX_SHARED_DB
• Schema: SHARED_DATA

📊 INCLUDED DATASETS:
• Swiss Company Intelligence
• Geographic Business Analysis
• Business Change Tracking
• Publication Activity Monitoring
• Canton-level Statistics

🚀 NEXT STEPS:
1. Review organization listing in Snowflake UI
2. Test subscription process with organization accounts
3. Share listing with organization members
4. Set up monitoring and alerts
5. Plan regular data refresh schedule

💡 ORGANIZATION BENEFITS:
• Available across all organization accounts
• Natural language queries with Cortex Analyst
• Pre-built business intelligence views
• Comprehensive Swiss market data
• Real-time updates and data quality monitoring
• Complete documentation and examples
• Centralized governance and compliance

🛠️ MAINTENANCE:
• Monitor usage across organization accounts
• Update marketplace description as needed
• Refresh sample data regularly
• Respond to organization member feedback
• Maintain data quality standards

For technical support or questions, contact your data team.
' as DEPLOYMENT_COMPLETE;

-- Final timestamp
SELECT 'Deployment completed at:' as FINAL_STATUS,
       CURRENT_TIMESTAMP() as COMPLETION_TIME; 