-- =====================================================
-- ZEFIX Data Platform - Share Configuration
-- =====================================================
-- Configuration settings and variables for the ZEFIX
-- data sharing infrastructure. Modify these values to
-- match your environment before running the deployment.
-- =====================================================

-- =====================================================
-- ENVIRONMENT CONFIGURATION
-- =====================================================

-- Source database and schema (where your dbt models are deployed)
-- Update these to match your actual dbt target configuration
SET SOURCE_DATABASE_NAME = 'ZEFIX';  -- Your dbt target database
SET SOURCE_SCHEMA_NAME = 'PROD';                  -- Your dbt target schema

-- Share infrastructure names
SET SHARE_DATABASE_NAME = 'ZEFIX_SHARED_DB';
SET SHARE_SCHEMA_NAME = 'SHARED_DATA';
SET SHARE_NAME = 'ZEFIX_DATA_PLATFORM_SHARE';

-- Organization marketplace listing configuration
SET LISTING_NAME = 'ZEFIX_SWISS_COMPANY_INTELLIGENCE';
SET LISTING_CATEGORIES = ('Business & Finance', 'Government', 'Analytics');

-- =====================================================
-- ROLE AND PERMISSION CONFIGURATION
-- =====================================================

-- Roles for managing the share (adjust based on your org structure)
SET SHARE_ADMIN_ROLE = 'ZEFIX_SHARE_ADMIN';
SET SHARE_PROVIDER_ROLE = 'ACCOUNTADMIN';  -- Role that can create shares
SET SHARE_CONSUMER_ROLE = 'SYSADMIN';      -- Role that gets access to manage

-- =====================================================
-- DATA GOVERNANCE SETTINGS
-- =====================================================

-- Data classification and compliance
SET DATA_CLASSIFICATION = 'PUBLIC';        -- PUBLIC, INTERNAL, CONFIDENTIAL
SET DATA_SOURCE = 'Swiss Commercial Register (ZEFIX)';
SET COMPLIANCE_FRAMEWORK = 'Swiss Data Protection Act';
SET DATA_RETENTION_DAYS = 2555;           -- ~7 years default retention

-- Update frequency and refresh settings
SET REFRESH_FREQUENCY = 'DAILY';          -- DAILY, WEEKLY, MONTHLY
SET AUTO_REFRESH_ENABLED = TRUE;
SET DATA_QUALITY_CHECKS = TRUE;

-- =====================================================
-- ORGANIZATION MARKETPLACE CONFIGURATION
-- =====================================================

-- Organization listing settings (configured in YAML manifest)
SET ORGANIZATION_PROFILE = 'INTERNAL';   -- Internal organization listing
SET ORGANIZATION_ACCESS = '*';           -- All organization accounts
SET AUTO_FULFILLMENT_SCHEDULE = '24 HOUR';  -- Refresh schedule
SET LISTING_PUBLISHED = TRUE;            -- Auto-publish on creation

-- Notification settings
SET NOTIFICATION_EMAIL = 'data-team@yourcompany.com';  -- Update with your email
SET ENABLE_NOTIFICATIONS = FALSE;        -- Set to TRUE to enable email notifications

-- Regional availability (uncomment regions you want to support)
SET SUPPORTED_REGIONS = (
    'AWS_EU_CENTRAL_1'     -- Primary region for Swiss data
    -- ,'AWS_US_EAST_1'    -- North America
    -- ,'AZURE_WEST_EUROPE' -- Azure Europe
    -- ,'GCP_EUROPE_WEST4'  -- Google Cloud Europe
);

-- =====================================================
-- SEMANTIC VIEWS CONFIGURATION
-- =====================================================

-- Semantic views to include in the share
SET SEMANTIC_VIEWS = (
    'sem_company_overview',
    'sem_company_types_by_canton', 
    'sem_business_changes',
    'sem_geographic_analysis',
    'sem_publication_activity'
);

-- Gold layer models to include
SET GOLD_MODELS = (
    'gold_company_overview',
    'gold_company_activity', 
    'gold_canton_statistics'
);

-- Sample data limits
SET SAMPLE_COMPANY_LIMIT = 100;
SET SAMPLE_GEOGRAPHIC_LIMIT = 50;
SET SAMPLE_ACTIVITY_LIMIT = 100;

-- =====================================================
-- BUSINESS METADATA
-- =====================================================

-- Business context and descriptions
SET BUSINESS_PURPOSE = 'Swiss Company Intelligence and Market Analysis';
SET PRIMARY_USE_CASES = (
    'Market Research & Competitive Analysis',
    'Business Development & Lead Generation',
    'Economic Research & Trend Analysis', 
    'Compliance & Due Diligence',
    'Geographic Market Expansion Planning'
);

-- Key differentiators
SET KEY_FEATURES = (
    'Cortex Analyst Ready Semantic Views',
    'Complete Swiss Company Registry Data',
    'Geographic Intelligence by Canton',
    'Real-time Business Change Tracking',
    'Natural Language Query Support'
);

-- Target audience
SET TARGET_CONSUMERS = (
    'Business Analysts',
    'Market Researchers', 
    'Investment Professionals',
    'Compliance Teams',
    'Economic Researchers'
);

-- =====================================================
-- PRICING AND MONETIZATION (Optional)
-- =====================================================

-- Pricing model configuration (uncomment if implementing paid sharing)
/*
SET PRICING_MODEL = 'SUBSCRIPTION';       -- SUBSCRIPTION, USAGE_BASED, FREE
SET BASE_PRICE = 500.00;                  -- Monthly subscription price in USD
SET CURRENCY = 'USD';
SET BILLING_FREQUENCY = 'MONTHLY';        -- MONTHLY, QUARTERLY, ANNUAL
SET FREE_TRIAL_DAYS = 30;                 -- Free trial period
SET USAGE_BASED_PRICING = FALSE;          -- Enable usage-based pricing
*/

-- =====================================================
-- QUALITY AND SLA CONFIGURATION  
-- =====================================================

-- Data quality thresholds
SET MIN_DATA_FRESHNESS_HOURS = 24;        -- Maximum data age in hours
SET MIN_COMPLETENESS_PERCENT = 95.0;      -- Minimum data completeness
SET MAX_ERROR_RATE_PERCENT = 1.0;         -- Maximum acceptable error rate

-- Service level agreement
SET AVAILABILITY_SLA = 99.9;              -- Uptime SLA percentage
SET SUPPORT_HOURS = '24x7';               -- Support availability
SET RESPONSE_TIME_HOURS = 4;              -- Maximum response time

-- =====================================================
-- DOCUMENTATION URLS
-- =====================================================

-- External documentation and support links
SET DOCUMENTATION_URL = 'https://github.com/your-org/zefix-dbt-platform';
SET SUPPORT_URL = 'mailto:data-team@yourcompany.com';
SET SAMPLE_QUERIES_URL = 'https://github.com/your-org/zefix-dbt-platform/blob/main/examples/';

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

-- Validation queries to verify configuration
SELECT 'Configuration Validation' as CHECK_TYPE,
       $SOURCE_DATABASE_NAME as SOURCE_DB,
       $SOURCE_SCHEMA_NAME as SOURCE_SCHEMA,
       $SHARE_NAME as SHARE_NAME,
       $LISTING_NAME as LISTING_NAME;

-- Verify source objects exist (uncomment to run validation)
/*
SELECT 'Source Object Validation' as CHECK_TYPE,
       table_name,
       table_type
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = $SOURCE_SCHEMA_NAME
  AND table_name IN (
    'sem_company_overview',
    'sem_company_types_by_canton',
    'sem_business_changes', 
    'sem_geographic_analysis',
    'sem_publication_activity',
    'gold_company_overview',
    'gold_company_activity',
    'gold_canton_statistics'
  )
ORDER BY table_name;
*/

-- =====================================================
-- CONFIGURATION SUMMARY
-- =====================================================

SELECT '
📋 ZEFIX SHARE CONFIGURATION SUMMARY

🎯 SHARE DETAILS:
• Share Name: ' || $SHARE_NAME || '
• Database: ' || $SHARE_DATABASE_NAME || '
• Schema: ' || $SHARE_SCHEMA_NAME || '
• Source: ' || $SOURCE_DATABASE_NAME || '.' || $SOURCE_SCHEMA_NAME || '

🏪 MARKETPLACE LISTING:
• Listing Name: ' || $LISTING_NAME || '
• Category: ' || $LISTING_CATEGORY || '
• Public: ' || $LISTING_PUBLIC || '
• Auto-fulfillment: ' || $AUTO_FULFILLMENT || '

📊 DATA CONFIGURATION:
• Classification: ' || $DATA_CLASSIFICATION || '
• Refresh: ' || $REFRESH_FREQUENCY || '
• Retention: ' || $DATA_RETENTION_DAYS || ' days
• Quality Checks: ' || $DATA_QUALITY_CHECKS || '

⚠️  IMPORTANT: Review and update all configuration values
before running the deployment scripts!
' as CONFIGURATION_SUMMARY; 