# 🔗 ZEFIX Data Platform - Snowflake Share & Organization Marketplace

This folder contains the configuration and scripts to create a Snowflake share for the ZEFIX Swiss company data and publish it to Snowflake's organization marketplace.

## 📋 Overview

The ZEFIX Data Platform share includes:
- **Semantic Views**: Ready-to-query business intelligence views optimized for Cortex Analyst
- **Gold Layer Models**: Curated business-level data models
- **Documentation**: Complete data dictionary and usage examples

## 🎯 What's Included in the Share

### Semantic Views (Cortex Analyst Ready)
- `sem_company_overview` - Comprehensive company statistics and metrics
- `sem_company_types_by_canton` - Geographic distribution of legal forms
- `sem_business_changes` - Business mutations and change tracking
- `sem_geographic_analysis` - Canton-level business activity analysis
- `sem_publication_activity` - SHAB publication trends and statistics

**Note**: Semantic views are shared directly from the source database using `GRANT REFERENCES ON SEMANTIC VIEW` syntax as per [Snowflake documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege-share). This preserves their Cortex Analyst optimization and metadata.

### Gold Layer Models
- `gold_company_overview` - Core company information
- `gold_company_activity` - Business activity aggregations
- `gold_canton_statistics` - Geographic business statistics

## 🚀 Setup Instructions

### 1. Create the Share
```sql
-- Run the share creation script
@create_zefix_share.sql
```

### 2. Add Objects to Share
```sql
-- Add all semantic views and gold models to the share
@add_objects_to_share.sql
```

### 3. Create Organization Marketplace Listing
```sql
-- Create the organization marketplace listing
@create_marketplace_listing.sql
```

## 📊 Share Benefits

### For Organization Members
- **Organization-wide Access**: Available to all accounts within your organization
- **Natural Language Queries**: Use Cortex Analyst with semantic views
- **Business-Ready Data**: Pre-aggregated and cleaned datasets
- **Swiss Market Intelligence**: Comprehensive company and business activity data
- **Real-time Updates**: Automatically refreshed with latest ZEFIX data

### For Data Providers
- **Monetization**: Potential revenue from data sharing
- **Reduced Support**: Self-service data access
- **Compliance**: Controlled data access with usage tracking
- **Scale**: Serve multiple consumers efficiently

## 🔧 Technical Details

### Share Configuration
- **Share Name**: `ZEFIX_DATA_PLATFORM_SHARE`
- **Database**: `ZEFIX_SHARED_DB`
- **Schema**: `PUBLIC`
- **Access Type**: Organization Marketplace
- **Update Frequency**: Daily

### Data Governance
- **Data Classification**: Business data (non-PII)
- **Access Controls**: Role-based access through Snowflake RBAC
- **Usage Monitoring**: Built-in Snowflake usage tracking
- **Documentation**: Complete data lineage and business glossary

## 📈 Organization Marketplace Listing Details

### Title
"ZEFIX Swiss Company Intelligence - Semantic Views & Business Analytics"

### Description
Premium Swiss company data with AI-ready semantic views for natural language business intelligence. Includes comprehensive company information, geographic analysis, business changes, and publication activity from the Swiss Commercial Register (ZEFIX).

### Key Features
- 🧠 **Cortex Analyst Ready**: Semantic views optimized for natural language queries
- 🏢 **Complete Company Data**: All Swiss registered companies with detailed metadata
- 📍 **Geographic Intelligence**: Canton and municipality-level business insights
- 📊 **Business Analytics**: Pre-built metrics for company formations, dissolutions, and changes
- 🔄 **Real-time Updates**: Daily refreshed data from official ZEFIX sources

### Use Cases
- Market research and competitive analysis
- Business development and lead generation
- Economic research and trend analysis
- Compliance and due diligence
- Geographic market expansion planning

## 🛡️ Security & Compliance

### Data Protection
- All data is sourced from public Swiss Commercial Register
- No personal identifying information (PII) included
- Compliant with Swiss data protection regulations
- Regular security audits and monitoring

### Access Controls
- Snowflake native role-based access control
- Consumer account verification required
- Usage tracking and audit logs
- Configurable access permissions

## 📞 Support

For questions about the ZEFIX Data Platform share:
- Technical Documentation: See `/docs` folder
- Data Questions: Contact data team
- Access Issues: Contact Snowflake administrator

---

*This share is maintained as part of the ZEFIX Data Platform dbt project. All data models are version-controlled and automatically deployed.* 