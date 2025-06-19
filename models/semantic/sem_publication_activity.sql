{{
  config(
    materialized='semantic_view'
  )
}}

-- Publication Activity Semantic View
-- Focused on SHAB publications, business activity tracking, and company events
-- Enables queries like: "How many publications were there this month?" or "Show formation activity by canton"

CREATE OR REPLACE SEMANTIC VIEW {{ target.database }}.{{ target.schema }}.sem_publication_activity
TABLES (
  companies AS {{ ref('silver_companies') }}
    PRIMARY KEY (company_uid)
    WITH SYNONYMS ('business_entities', 'firms', 'organizations')
    COMMENT = 'Core business entities registered in ZEFIX',
  
  publications AS {{ ref('silver_shab_publications') }}
    PRIMARY KEY (shab_id)
    UNIQUE (company_uid, shab_date)
    WITH SYNONYMS ('shab_records', 'gazette_entries', 'business_announcements', 'official_notices')
    COMMENT = 'Official business publication records from Swiss Official Gazette'
)
RELATIONSHIPS (
  publications_to_companies AS
    publications (company_uid)
    REFERENCES companies (company_uid)
)
FACTS (
  publications.shab_id AS publications.shab_id
    WITH SYNONYMS ('publication_id', 'gazette_id', 'announcement_id', 'notice_id')
    COMMENT = 'Unique publication identifier',
  
  publications.days_since_publication AS DATEDIFF('day', publications.shab_date, CURRENT_DATE())
    WITH SYNONYMS ('publication_age_days', 'days_ago', 'notice_age')
    COMMENT = 'Number of days since this publication'
)
DIMENSIONS (
  -- Company dimensions
  companies.company_uid AS companies.company_uid
    WITH SYNONYMS ('uid', 'business_id', 'entity_id')
    COMMENT = 'Unique company identifier',
  
  companies.company_name AS companies.company_name
    WITH SYNONYMS ('business_name', 'firm_name', 'entity_name')
    COMMENT = 'Official company name',
  
  companies.is_active AS companies.is_active
    WITH SYNONYMS ('active_status', 'operational')
    COMMENT = 'Whether the company is currently active',
  
  -- Publication dimensions
  publications.company_uid AS publications.company_uid
    WITH SYNONYMS ('business_id', 'entity_reference')
    COMMENT = 'Company identifier (foreign key)',
  
  publications.registry_office_canton AS publications.registry_office_canton
    WITH SYNONYMS ('canton', 'state', 'region', 'province')
    COMMENT = 'Swiss canton handling the publication',
  
  publications.publication_year AS EXTRACT(YEAR FROM publications.shab_date)
    WITH SYNONYMS ('announcement_year', 'gazette_year', 'notice_year')
    COMMENT = 'Year of SHAB publication',
  
  publications.publication_quarter AS EXTRACT(QUARTER FROM publications.shab_date)
    WITH SYNONYMS ('quarter', 'period', 'q')
    COMMENT = 'Quarter of SHAB publication',
  
  publications.publication_month AS EXTRACT(MONTH FROM publications.shab_date)
    WITH SYNONYMS ('month', 'announcement_month')
    COMMENT = 'Month of SHAB publication',
  
  publications.shab_date AS publications.shab_date
    WITH SYNONYMS ('publication_date', 'announcement_date', 'gazette_date', 'notice_date')
    COMMENT = 'Date of SHAB publication',
  
  publications.activity_type AS CASE 
    WHEN publications.publication_message ILIKE '%gründung%' OR publications.publication_message ILIKE '%constitution%' THEN 'Formation'
    WHEN publications.publication_message ILIKE '%auflösung%' OR publications.publication_message ILIKE '%dissolution%' THEN 'Dissolution'
    WHEN publications.publication_message ILIKE '%kapital%' OR publications.publication_message ILIKE '%capital%' THEN 'Capital Change'
    WHEN publications.publication_message ILIKE '%adresse%' OR publications.publication_message ILIKE '%address%' THEN 'Address Change'
    WHEN publications.publication_message ILIKE '%verwaltung%' OR publications.publication_message ILIKE '%administration%' THEN 'Management Change'
    WHEN publications.publication_message ILIKE '%zweck%' OR publications.publication_message ILIKE '%purpose%' THEN 'Purpose Change'
    WHEN publications.publication_message ILIKE '%fusion%' OR publications.publication_message ILIKE '%merger%' THEN 'Merger'
    ELSE 'Other'
  END
    WITH SYNONYMS ('business_activity', 'change_type', 'event_category', 'activity_category')
    COMMENT = 'Type of business activity based on publication content',
  
  publications.recency_bucket AS CASE 
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN 'Last 30 days'
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '90 days' THEN 'Last 90 days'
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '365 days' THEN 'Last year'
    ELSE 'Older'
  END
    WITH SYNONYMS ('recency', 'time_category', 'age_group', 'freshness')
    COMMENT = 'How recent the publication activity was'
)
METRICS (
  -- Publication Volume Metrics
  publications.total_publications AS COUNT(publications.shab_id)
    WITH SYNONYMS ('publication_count', 'announcement_count', 'gazette_entries', 'total_notices')
    COMMENT = 'Total number of SHAB publications',
  
  publications.recent_publications AS COUNT(CASE WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN publications.shab_id END)
    WITH SYNONYMS ('monthly_publications', 'recent_activity', 'last_month_notices')
    COMMENT = 'Publications in the last 30 days',
  
  publications.this_year_publications AS COUNT(CASE WHEN EXTRACT(YEAR FROM publications.shab_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN publications.shab_id END)
    WITH SYNONYMS ('annual_publications', 'yearly_activity', 'current_year_notices')
    COMMENT = 'Publications in the current year',
  
  publications.this_quarter_publications AS COUNT(CASE WHEN EXTRACT(QUARTER FROM publications.shab_date) = EXTRACT(QUARTER FROM CURRENT_DATE()) AND EXTRACT(YEAR FROM publications.shab_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN publications.shab_id END)
    WITH SYNONYMS ('quarterly_publications', 'current_quarter_activity')
    COMMENT = 'Publications in the current quarter',
  
  publications.average_publications_per_company AS ROUND(
    COUNT(publications.shab_id)::FLOAT 
    / NULLIF(COUNT(DISTINCT publications.company_uid), 0), 
    2
  )
    WITH SYNONYMS ('publication_frequency', 'activity_ratio', 'average_activity')
    COMMENT = 'Average number of publications per company',
  
  -- Business Event Metrics
  publications.company_formations AS COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END)
    WITH SYNONYMS ('new_businesses', 'incorporations', 'startups', 'new_companies')
    COMMENT = 'Number of company formations',
  
  publications.company_dissolutions AS COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END)
    WITH SYNONYMS ('business_closures', 'liquidations', 'terminations', 'company_closures')
    COMMENT = 'Number of company dissolutions',
  
  publications.net_company_formation AS (COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END) -
   COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END))
    WITH SYNONYMS ('business_growth', 'net_incorporations', 'net_business_creation')
    COMMENT = 'Net company formation (formations minus dissolutions)',
  
  publications.capital_changes AS COUNT(CASE WHEN publications.activity_type = 'Capital Change' THEN publications.shab_id END)
    WITH SYNONYMS ('capital_modifications', 'capital_events', 'financial_changes')
    COMMENT = 'Number of capital-related changes',
  
  publications.address_changes AS COUNT(CASE WHEN publications.activity_type = 'Address Change' THEN publications.shab_id END)
    WITH SYNONYMS ('location_changes', 'address_modifications', 'relocation_events')
    COMMENT = 'Number of address changes',
  
  publications.management_changes AS COUNT(CASE WHEN publications.activity_type = 'Management Change' THEN publications.shab_id END)
    WITH SYNONYMS ('leadership_changes', 'administration_changes', 'governance_changes')
    COMMENT = 'Number of management/administration changes',
  
  -- Geographic Metrics
  publications.unique_cantons AS COUNT(DISTINCT publications.registry_office_canton)
    WITH SYNONYMS ('canton_count', 'regional_coverage', 'geographic_spread')
    COMMENT = 'Number of different Swiss cantons with publication activity'
)
COMMENT = 'Publication activity semantic view focusing on SHAB publications, business events, and temporal patterns. Ideal for tracking business formation, dissolution, and change activity over time.' 