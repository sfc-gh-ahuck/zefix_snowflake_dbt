-- =====================================================
-- ZEFIX Data Platform - Snowflake Share Creation
-- =====================================================
-- This script creates the necessary infrastructure for 
-- sharing ZEFIX data through Snowflake's data sharing
-- =====================================================

-- Set context
USE ROLE ACCOUNTADMIN;

-- Create a dedicated database for sharing
CREATE DATABASE IF NOT EXISTS ZEFIX_SHARED_DB
COMMENT = 'Database containing ZEFIX data for sharing via Snowflake Data Sharing';

-- Create schema for shared objects
CREATE SCHEMA IF NOT EXISTS ZEFIX_SHARED_DB.SHARED_DATA
COMMENT = 'Schema containing ZEFIX semantic views and gold models for data sharing';

-- Create the share
CREATE SHARE ZEFIX_DATA_PLATFORM_SHARE
COMMENT = 'ZEFIX Swiss Company Intelligence - Semantic Views & Business Analytics
Premium Swiss company data with AI-ready semantic views for natural language 
business intelligence. Includes comprehensive company information, geographic 
analysis, business changes, and publication activity from the Swiss Commercial Register.';

-- Grant usage on database to share (for gold models and utility views)
GRANT USAGE ON DATABASE ZEFIX_SHARED_DB TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- Grant usage on schema to share (for gold models and utility views)
GRANT USAGE ON SCHEMA ZEFIX_SHARED_DB.SHARED_DATA TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- Note: Semantic views will be shared directly from their source database
-- This requires additional USAGE grants on the source database/schema

-- Create role for managing the share
CREATE ROLE IF NOT EXISTS ZEFIX_SHARE_ADMIN
COMMENT = 'Role for managing ZEFIX data share';

-- Grant necessary privileges to share admin role
GRANT USAGE ON DATABASE ZEFIX_SHARED_DB TO ROLE ZEFIX_SHARE_ADMIN;
GRANT USAGE ON SCHEMA ZEFIX_SHARED_DB.SHARED_DATA TO ROLE ZEFIX_SHARE_ADMIN;
GRANT CREATE VIEW ON SCHEMA ZEFIX_SHARED_DB.SHARED_DATA TO ROLE ZEFIX_SHARE_ADMIN;
GRANT CREATE TABLE ON SCHEMA ZEFIX_SHARED_DB.SHARED_DATA TO ROLE ZEFIX_SHARE_ADMIN;

-- Grant share privileges
GRANT OWNERSHIP ON SHARE ZEFIX_DATA_PLATFORM_SHARE TO ROLE ZEFIX_SHARE_ADMIN;

-- Grant role to SYSADMIN (adjust as needed for your org)
GRANT ROLE ZEFIX_SHARE_ADMIN TO ROLE SYSADMIN;

-- Switch to share admin role
USE ROLE ZEFIX_SHARE_ADMIN;
USE DATABASE ZEFIX_SHARED_DB;
USE SCHEMA SHARED_DATA;

-- Create documentation table for the share
CREATE OR REPLACE TABLE SHARE_DOCUMENTATION (
    object_name VARCHAR(255),
    object_type VARCHAR(50),
    description TEXT,
    business_value TEXT,
    usage_examples TEXT,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Documentation and metadata for objects in the ZEFIX data share';

-- Insert documentation for shared objects
INSERT INTO SHARE_DOCUMENTATION VALUES
('sem_company_overview', 'SEMANTIC_VIEW', 
 'Comprehensive company statistics and metrics optimized for Cortex Analyst',
 'Enables natural language queries about Swiss company landscape, activity levels, and key business metrics',
 'Ask: "How many companies are currently active?" or "What percentage of companies are stock companies?"',
 CURRENT_TIMESTAMP()),

('sem_company_types_by_canton', 'SEMANTIC_VIEW',
 'Geographic distribution of company legal forms across Swiss cantons',
 'Supports regional market analysis and geographic expansion planning',
 'Ask: "Which canton has the most AGs?" or "Show me legal form distribution by region"',
 CURRENT_TIMESTAMP()),

('sem_business_changes', 'SEMANTIC_VIEW',
 'Business mutations and change tracking with intelligent categorization',
 'Monitors business activity trends, management changes, and corporate events',
 'Ask: "How many management changes happened this year?" or "Show me recent capital changes"',
 CURRENT_TIMESTAMP()),

('sem_geographic_analysis', 'SEMANTIC_VIEW',
 'Canton-level business activity analysis with spatial intelligence',
 'Enables geographic market research and regional business development',
 'Ask: "Which regions have the highest business activity?" or "Compare company density by canton"',
 CURRENT_TIMESTAMP()),

('sem_publication_activity', 'SEMANTIC_VIEW',
 'SHAB publication trends and statistics with temporal analysis',
 'Tracks regulatory filing activity and business communication patterns',
 'Ask: "What are the publication trends this year?" or "How many companies published recently?"',
 CURRENT_TIMESTAMP()),

('gold_company_overview', 'TABLE',
 'Core company information with complete business profiles',
 'Primary dataset for company research, due diligence, and market analysis',
 'Use for: Company lookups, legal form analysis, geographic distribution studies',
 CURRENT_TIMESTAMP()),

('gold_company_activity', 'TABLE',
 'Business activity aggregations with time-series data',
 'Quantifies business dynamics, formation rates, and activity patterns',
 'Use for: Trend analysis, activity forecasting, market timing insights',
 CURRENT_TIMESTAMP()),

('gold_canton_statistics', 'TABLE',
 'Geographic business statistics aggregated by Swiss cantons',
 'Regional business intelligence for market expansion and location planning',
 'Use for: Regional analysis, market sizing, location strategy',
 CURRENT_TIMESTAMP());

-- Grant select on documentation to share
GRANT SELECT ON TABLE SHARE_DOCUMENTATION TO SHARE ZEFIX_DATA_PLATFORM_SHARE;

-- Display confirmation
SELECT 'ZEFIX Share Infrastructure Created Successfully!' as STATUS,
       'ZEFIX_DATA_PLATFORM_SHARE' as SHARE_NAME,
       'ZEFIX_SHARED_DB.SHARED_DATA' as SCHEMA_NAME,
       CURRENT_TIMESTAMP() as CREATED_AT;

-- Show share details
SHOW SHARES LIKE 'ZEFIX_DATA_PLATFORM_SHARE'; 