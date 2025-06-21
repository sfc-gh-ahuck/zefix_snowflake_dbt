# ZEFIX Snowflake Share & Marketplace

Share Swiss company data within your Snowflake organization.

## 📋 What's Shared

### Semantic Views (Cortex Analyst Ready)
- Company overview and statistics
- Geographic distribution analysis
- Business changes and mutations
- Publication activity trends

### Gold Layer Models
- `gold_company_overview` - Core company data
- `gold_company_activity` - Business activity
- `gold_canton_statistics` - Geographic stats

## 🚀 Quick Setup

```sql
-- 1. Create share
@create_zefix_share.sql

-- 2. Add objects to share
@add_objects_to_share.sql

-- 3. Create marketplace listing
@create_marketplace_listing.sql
```

## 📊 Benefits

**For Consumers:**
- Natural language queries with Cortex Analyst
- Pre-built business intelligence views
- Swiss market insights
- Daily data updates

**For Providers:**
- Reduced support overhead
- Controlled data access
- Usage tracking
- Potential monetization

## 🎯 Use Cases

- Market research and competitive analysis
- Business development and lead generation
- Economic research and trend analysis
- Compliance and due diligence
- Geographic expansion planning

## 🔧 Technical Details

- **Share Name**: `ZEFIX_DATA_PLATFORM_SHARE`
- **Access**: Organization marketplace
- **Updates**: Daily refresh
- **Security**: Snowflake RBAC controls

## 📈 Marketplace Listing

**Title**: "ZEFIX Swiss Company Intelligence"

**Features**:
- AI-ready semantic views
- Complete Swiss company data
- Geographic business insights
- Real-time updates from official sources

Built and maintained through the ZEFIX dbt project. 