-- =====================================================
-- ZEFIX Data Platform - Marketplace Listing Creation
-- =====================================================
-- This script creates a marketplace listing for the
-- ZEFIX Data Platform share
-- =====================================================

-- Set context - Must use ORGADMIN role for organization listings
USE ROLE ACCOUNTADMIN;

-- =====================================================
-- Create Organization Marketplace Listing
-- =====================================================

CREATE ORGANIZATION LISTING ZEFIX_SWISS_COMPANY_INTELLIGENCE
SHARE ZEFIX_DATA_PLATFORM_SHARE AS
$$
title: "ZEFIX Swiss Company Intelligence - Semantic Views & Business Analytics"
subtitle: "AI-Ready Swiss Company Data with Cortex Analyst Optimization"
description: |
  Transform your business intelligence with comprehensive Swiss company data from the official ZEFIX Commercial Register. 
  This organization listing provides semantic views optimized for Snowflake Cortex Analyst, enabling natural language 
  queries and advanced analytics across your organization.

  KEY FEATURES:
  • Cortex Analyst Ready: Semantic views optimized for natural language queries
  • Complete Company Data: All Swiss registered companies with detailed metadata
  • Geographic Intelligence: Canton and municipality-level business insights
  • Business Analytics: Pre-built metrics for formations, dissolutions, and changes
  • Real-time Updates: Daily refreshed data from official ZEFIX sources
  • Time Series Analysis: Historical trends and activity patterns

  INCLUDED DATASETS:
  
  SEMANTIC VIEWS (Cortex Analyst Ready):
  • sem_company_overview - Comprehensive company statistics and key metrics
  • sem_company_types_by_canton - Geographic distribution of legal forms
  • sem_business_changes - Business mutations and change tracking
  • sem_geographic_analysis - Canton-level business activity analysis
  • sem_publication_activity - SHAB publication trends and statistics

  GOLD LAYER MODELS:
  • gold_company_overview - Core company information and profiles
  • gold_company_activity - Business activity aggregations
  • gold_canton_statistics - Geographic business statistics

  Perfect for: Business analysts, market researchers, investment professionals, compliance teams, 
  and anyone needing comprehensive Swiss market intelligence.

organization_profile: "INTERNAL"
organization_targets:
  discovery:
    - account: "*"  # Available to all accounts in organization
  access:
    - account: "*"  # All organization accounts can access
      roles:
        - "PUBLIC"
        - "SYSADMIN"
        - "ANALYST_ROLE"

listing_terms:
  type: "STANDARD"

auto_fulfillment:
  refresh_schedule: "24 HOUR"
  refresh_type: "SUB_DATABASE"

business_needs:
  - name: "Market Research & Competitive Analysis"
    description: "Comprehensive Swiss company data for market analysis, competitive intelligence, and business development initiatives."
  - name: "Economic Research & Trend Analysis" 
    description: "Historical and real-time business formation trends, geographic distribution analysis, and economic indicators."
  - name: "Compliance & Due Diligence"
    description: "Official company registry data for compliance verification, due diligence processes, and regulatory reporting."
  - name: "Business Intelligence & Analytics"
    description: "AI-ready semantic views for natural language queries, business metrics, and advanced analytics workflows."

categories:
  - "Business & Finance"
  - "Government"
  - "Analytics"

data_attributes:
  refresh_rate: "DAILY"
  geography:
    granularity:
      - "COUNTRY"
      - "STATE_PROVINCE"
      - "CITY"
    geo_option: "COUNTRIES"
    coverage:
      continents:
        EUROPE:
          - "SWITZERLAND"
  time:
    granularity: "DAILY"
    time_frame: "SINCE"
    start_date: "01-01-1995"  # Historical data availability

data_dictionary:
  - database: "ZEFIX_SHARED_DB"
    objects:
      - name: "sem_company_overview"
        schema: "SHARED_DATA"
        domain: "SEMANTIC_VIEW"
        description: "Comprehensive company statistics and metrics optimized for Cortex Analyst natural language queries"
      - name: "sem_company_types_by_canton"
        schema: "SHARED_DATA" 
        domain: "SEMANTIC_VIEW"
        description: "Geographic distribution of company legal forms across Swiss cantons"
      - name: "sem_business_changes"
        schema: "SHARED_DATA"
        domain: "SEMANTIC_VIEW"
        description: "Business mutations and change tracking with intelligent categorization"
      - name: "sem_geographic_analysis"
        schema: "SHARED_DATA"
        domain: "SEMANTIC_VIEW"
        description: "Canton-level business activity analysis with spatial intelligence"
      - name: "sem_publication_activity"
        schema: "SHARED_DATA"
        domain: "SEMANTIC_VIEW"
        description: "SHAB publication trends and statistics with temporal analysis"
      - name: "gold_company_overview"
        schema: "SHARED_DATA"
        domain: "VIEW"
        description: "Core company information with complete business profiles"
      - name: "gold_company_activity"
        schema: "SHARED_DATA"
        domain: "VIEW"
        description: "Business activity aggregations with time-series data"
      - name: "gold_canton_statistics"
        schema: "SHARED_DATA"
        domain: "VIEW"
        description: "Geographic business statistics aggregated by Swiss cantons"

data_preview:
  has_pii: FALSE

usage_examples:
  - title: "Count Active Companies"
    description: "Get the total number of currently active companies in Switzerland"
    query: |
      SELECT COUNT(*) as active_companies 
      FROM sem_company_overview 
      WHERE company_status = 'ACTIVE';
  
  - title: "Company Formations by Canton"
    description: "Analyze new company formations by Swiss canton for market expansion planning"
    query: |
      SELECT canton, COUNT(*) as new_formations
      FROM sem_company_types_by_canton 
      WHERE registration_year = YEAR(CURRENT_DATE())
      GROUP BY canton 
      ORDER BY new_formations DESC;
  
  - title: "Business Changes Trend Analysis"
    description: "Track business mutation trends over time for market intelligence"
    query: |
      SELECT 
        DATE_TRUNC('month', change_date) as month,
        change_type,
        COUNT(*) as change_count
      FROM sem_business_changes 
      WHERE change_date >= DATEADD('year', -1, CURRENT_DATE())
      GROUP BY month, change_type
      ORDER BY month DESC;

  - title: "Geographic Business Distribution"
    description: "Analyze business density and activity patterns across Swiss regions"
    query: |
      SELECT 
        canton,
        total_companies,
        companies_per_1000_residents,
        business_density_rank
      FROM sem_geographic_analysis
      ORDER BY business_density_rank;

  - title: "Natural Language Query with Cortex Analyst"
    description: "Example of natural language querying capabilities with semantic views"
    query: |
      -- Ask Cortex Analyst: "How many companies were formed this year compared to last year?"
      -- The semantic view will automatically interpret and execute the appropriate logic
      SELECT * FROM sem_company_overview LIMIT 5;

resources:
  documentation: "https://github.com/your-org/zefix-dbt-platform"

support_contact: "data-team@yourcompany.com"
approver_contact: "data-governance@yourcompany.com"

locations:
  access_regions:
    - name: "PUBLIC.AWS_EU_CENTRAL_1"
    - name: "PUBLIC.AWS_US_EAST_1"
$$
PUBLISH = TRUE;

-- =====================================================
-- Post-Creation Configuration
-- =====================================================
-- Note: Most configuration is now handled in the YAML manifest above.
-- The listing is automatically published with PUBLISH = TRUE.

-- Optional: Additional regions can be added if needed
-- (Regions are defined in the YAML manifest under locations.access_regions)
/*
ALTER ORGANIZATION LISTING ZEFIX_SWISS_COMPANY_INTELLIGENCE ADD REGION 'AZURE_WEST_EUROPE';
ALTER ORGANIZATION LISTING ZEFIX_SWISS_COMPANY_INTELLIGENCE ADD REGION 'GCP_EUROPE_WEST4';
*/

-- =====================================================
-- Optional: Create Listing Notifications
-- =====================================================

-- Create notification integration for listing events (optional)
-- Note: Support and approver contacts are defined in the YAML manifest
/*
CREATE NOTIFICATION INTEGRATION zefix_listing_notifications
  TYPE = EMAIL
  ENABLED = TRUE
  EMAIL_LIST = ('data-team@yourcompany.com', 'data-governance@yourcompany.com');

-- Notification settings can be configured if needed
*/

-- =====================================================
-- Verification and Status
-- =====================================================

-- Show the created organization listing
SHOW ORGANIZATION LISTINGS LIKE 'ZEFIX_SWISS_COMPANY_INTELLIGENCE';

-- Display organization listing details
DESCRIBE ORGANIZATION LISTING ZEFIX_SWISS_COMPANY_INTELLIGENCE;

-- Show organization listing regions
SHOW REGIONS FOR ORGANIZATION LISTING ZEFIX_SWISS_COMPANY_INTELLIGENCE;

-- Confirmation message
SELECT 
    'ZEFIX Organization Marketplace Listing Created Successfully!' as STATUS,
    'ZEFIX_SWISS_COMPANY_INTELLIGENCE' as LISTING_NAME,
    'Ready for organization accounts to discover and subscribe' as AVAILABILITY,
    CURRENT_TIMESTAMP() as CREATED_AT;

-- =====================================================
-- Additional Management Queries
-- =====================================================

-- Query to check listing status
/*
SELECT 
    listing_name,
    title,
    category,
    discoverable,
    public,
    auto_fulfillment,
    usage_tracking,
    created_on,
    owner
FROM SNOWFLAKE.ORGANIZATION_USAGE.LISTINGS 
WHERE listing_name = 'ZEFIX_SWISS_COMPANY_INTELLIGENCE';
*/

-- Query to monitor listing consumption
/*
SELECT 
    listing_name,
    consumer_account_name,
    consumer_account_locator,
    subscription_name,
    subscription_status,
    created_on
FROM SNOWFLAKE.ORGANIZATION_USAGE.LISTING_CONSUMPTION_DAILY
WHERE listing_name = 'ZEFIX_SWISS_COMPANY_INTELLIGENCE'
ORDER BY created_on DESC;
*/

-- =====================================================
-- Next Steps Instructions
-- =====================================================

SELECT '
🎉 ZEFIX Organization Marketplace Listing Setup Complete!

✅ YAML MANIFEST FEATURES IMPLEMENTED:
• Complete organization listing with YAML manifest
• Organization-wide discovery and access configuration
• Comprehensive data dictionary with semantic views
• Business needs and use case definitions
• Sample SQL queries and usage examples
• Geographic and temporal data attributes
• Auto-fulfillment and refresh scheduling

NEXT STEPS:
1. Review organization listing in Snowflake Marketplace UI
2. Test subscription process with organization accounts
3. Monitor usage across organization accounts
4. Update YAML manifest as needed for new features
5. Set up automated data refresh processes

ORGANIZATION MEMBER ONBOARDING:
• Share listing with organization accounts
• Provide sample queries and use cases from manifest
• Leverage built-in usage examples for training
• Monitor subscription requests across the organization

MAINTENANCE TASKS:
• Regular data quality checks
• Monitor usage patterns across organization
• Update YAML manifest for new datasets
• Refresh sample data periodically
• Review and respond to organization member feedback

YAML MANIFEST BENEFITS:
• Structured metadata and documentation
• Built-in data dictionary and usage examples
• Organization-specific targeting and access control
• Automatic categorization and discovery
• Comprehensive business need definitions
' as COMPLETION_NOTES; 