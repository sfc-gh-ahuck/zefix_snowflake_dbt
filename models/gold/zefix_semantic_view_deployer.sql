{{
  config(
    materialized='semantic_view'
  )
}}

-- ZEFIX Semantic View - Business Intelligence Layer
-- This model uses the custom semantic_view materialization to create a Snowflake semantic view
-- The semantic view enables natural language queries via Cortex Analyst

CREATE OR REPLACE SEMANTIC VIEW {{ target.database }}.{{ target.schema }}.zefix_business_intelligence
TABLES (
  -- Companies logical table - core business entity
  companies AS {{ ref('silver_companies') }}
    PRIMARY KEY (company_uid)
    WITH SYNONYMS ('business_entities', 'firms', 'organizations')
    COMMENT = 'Core business entities registered in ZEFIX',
  
  -- Publications logical table - represents business activity
  publications AS {{ ref('silver_shab_publications') }}
    PRIMARY KEY (shab_id)
    UNIQUE (company_uid, shab_date)
    WITH SYNONYMS ('shab_records', 'gazette_entries', 'business_announcements')
    COMMENT = 'Official business publication records from Swiss Official Gazette',
  
  -- Mutations logical table - represents specific business changes
  mutations AS {{ ref('silver_mutation_types') }}
    PRIMARY KEY (mutation_type_id)
    UNIQUE (company_uid, shab_date, mutation_type_key)
    WITH SYNONYMS ('business_changes', 'company_modifications', 'corporate_events')
    COMMENT = 'Specific types of business changes and modifications'
)
RELATIONSHIPS (
  -- Link publications to companies
  publications_to_companies AS
    publications (company_uid)
    REFERENCES companies (company_uid),
  
  -- Link mutations to companies
  mutations_to_companies AS
    mutations (company_uid)
    REFERENCES companies (company_uid),
  
  -- Link mutations to publications
  mutations_to_publications AS
    mutations (company_uid, shab_date)
    REFERENCES publications (company_uid, shab_date)
)
DIMENSIONS (
  -- Company dimensions
  companies.company_uid AS companies.company_uid
    WITH SYNONYMS ('uid', 'business_id', 'entity_id')
    COMMENT = 'Unique company identifier',
  
  companies.company_name AS companies.company_name
    WITH SYNONYMS ('business_name', 'firm_name', 'entity_name')
    COMMENT = 'Official company name',
  
  companies.legal_form_name AS companies.legal_form_name
    WITH SYNONYMS ('entity_type', 'business_form', 'corporate_structure')
    COMMENT = 'Type of legal entity (AG, GmbH, etc.)',
  
  companies.company_status AS companies.company_status
    WITH SYNONYMS ('status', 'state', 'condition')
    COMMENT = 'Current status of the company (Active/Deleted)',
  
  companies.legal_seat AS companies.legal_seat
    WITH SYNONYMS ('domicile', 'headquarters', 'seat')
    COMMENT = 'Legal domicile of the company',
  
  companies.address_town AS companies.address_town
    WITH SYNONYMS ('city', 'location', 'municipality')
    COMMENT = 'Town/city where company is located',
  
  companies.is_active AS companies.is_active
    WITH SYNONYMS ('active_status', 'operational')
    COMMENT = 'Whether the company is currently active',
  
  EXTRACT(YEAR FROM companies.first_observed_shab_date) AS companies.registration_year
    WITH SYNONYMS ('founding_year', 'incorporation_year', 'establishment_year')
    COMMENT = 'Year when company first appeared in register',
  
  companies.shab_date AS companies.shab_date
    WITH SYNONYMS ('last_publication_date', 'recent_activity_date')
    COMMENT = 'Most recent SHAB publication date',
  
  -- Publication dimensions
  publications.company_uid AS publications.company_uid
    WITH SYNONYMS ('business_id', 'entity_reference')
    COMMENT = 'Company identifier (foreign key)',
  
  publications.registry_office_canton AS publications.registry_office_canton
    WITH SYNONYMS ('canton', 'state', 'region')
    COMMENT = 'Swiss canton handling the publication',
  
  EXTRACT(YEAR FROM publications.shab_date) AS publications.publication_year
    WITH SYNONYMS ('announcement_year', 'gazette_year')
    COMMENT = 'Year of SHAB publication',
  
  EXTRACT(QUARTER FROM publications.shab_date) AS publications.publication_quarter
    WITH SYNONYMS ('quarter', 'period')
    COMMENT = 'Quarter of SHAB publication',
  
  publications.shab_date AS publications.shab_date
    WITH SYNONYMS ('publication_date', 'announcement_date', 'gazette_date')
    COMMENT = 'Date of SHAB publication',
  
  CASE 
    WHEN publications.publication_message ILIKE '%gründung%' OR publications.publication_message ILIKE '%constitution%' THEN 'Formation'
    WHEN publications.publication_message ILIKE '%auflösung%' OR publications.publication_message ILIKE '%dissolution%' THEN 'Dissolution'
    WHEN publications.publication_message ILIKE '%kapital%' OR publications.publication_message ILIKE '%capital%' THEN 'Capital Change'
    WHEN publications.publication_message ILIKE '%adresse%' OR publications.publication_message ILIKE '%address%' THEN 'Address Change'
    WHEN publications.publication_message ILIKE '%verwaltung%' OR publications.publication_message ILIKE '%administration%' THEN 'Management Change'
    WHEN publications.publication_message ILIKE '%zweck%' OR publications.publication_message ILIKE '%purpose%' THEN 'Purpose Change'
    WHEN publications.publication_message ILIKE '%fusion%' OR publications.publication_message ILIKE '%merger%' THEN 'Merger'
    ELSE 'Other'
  END AS publications.activity_type
    WITH SYNONYMS ('business_activity', 'change_type', 'event_category')
    COMMENT = 'Type of business activity based on publication content',
  
  CASE 
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN 'Last 30 days'
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '90 days' THEN 'Last 90 days'
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '365 days' THEN 'Last year'
    ELSE 'Older'
  END AS publications.recency_bucket
    WITH SYNONYMS ('recency', 'time_category', 'age_group')
    COMMENT = 'How recent the publication activity was',
  
  -- Mutation dimensions
  mutations.company_uid AS mutations.company_uid
    WITH SYNONYMS ('business_id', 'entity_reference')
    COMMENT = 'Company identifier (foreign key)',
  
  mutations.mutation_type_key AS mutations.mutation_type_key
    WITH SYNONYMS ('change_type', 'modification_type', 'event_type')
    COMMENT = 'Type of business change/mutation',
  
  mutations.registry_office_canton AS mutations.registry_office_canton
    WITH SYNONYMS ('canton', 'state', 'region')
    COMMENT = 'Swiss canton handling the change',
  
  EXTRACT(YEAR FROM mutations.shab_date) AS mutations.change_year
    WITH SYNONYMS ('modification_year', 'event_year')
    COMMENT = 'Year when the business change occurred',
  
  mutations.shab_date AS mutations.shab_date
    WITH SYNONYMS ('change_date', 'modification_date', 'event_date')
    COMMENT = 'Date of the business change publication'
)
FACTS (
  -- Company facts
  DATEDIFF('day', companies.first_observed_shab_date, CURRENT_DATE()) AS companies.days_since_registration
    WITH SYNONYMS ('company_age_days', 'registration_age')
    COMMENT = 'Number of days since company first registration',
  
  LENGTH(companies.company_purpose) AS companies.purpose_length
    WITH SYNONYMS ('purpose_complexity', 'description_length')
    COMMENT = 'Length of company purpose description (complexity indicator)',
  
  -- Publication facts
  publications.shab_id AS publications.shab_id
    WITH SYNONYMS ('publication_id', 'gazette_id', 'announcement_id')
    COMMENT = 'Unique publication identifier',
  
  DATEDIFF('day', publications.shab_date, CURRENT_DATE()) AS publications.days_since_publication
    WITH SYNONYMS ('publication_age_days', 'days_ago')
    COMMENT = 'Number of days since this publication',
  
  -- Mutation facts
  mutations.mutation_type_id AS mutations.mutation_type_id
    WITH SYNONYMS ('change_id', 'modification_id')
    COMMENT = 'Numeric identifier for the mutation type'
)
METRICS (
  -- Company Overview Metrics
  COUNT(DISTINCT companies.company_uid) AS total_companies
    WITH SYNONYMS ('company_count', 'business_count', 'entity_count')
    COMMENT = 'Total number of companies in the system',
  
  COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END) AS active_companies
    WITH SYNONYMS ('operational_companies', 'current_businesses')
    COMMENT = 'Number of currently active companies',
  
  COUNT(DISTINCT CASE WHEN companies.is_active = FALSE THEN companies.company_uid END) AS deleted_companies
    WITH SYNONYMS ('dissolved_companies', 'inactive_businesses')
    COMMENT = 'Number of deleted or dissolved companies',
  
  ROUND(
    COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END) * 100.0 
    / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 
    2
  ) AS active_company_percentage
    WITH SYNONYMS ('activity_rate', 'operational_rate')
    COMMENT = 'Percentage of companies that are currently active',
  
  -- Publication Activity Metrics
  COUNT(publications.shab_id) AS total_publications
    WITH SYNONYMS ('publication_count', 'announcement_count', 'gazette_entries')
    COMMENT = 'Total number of SHAB publications',
  
  COUNT(CASE WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN publications.shab_id END) AS recent_publications
    WITH SYNONYMS ('monthly_publications', 'recent_activity')
    COMMENT = 'Publications in the last 30 days',
  
  COUNT(CASE WHEN EXTRACT(YEAR FROM publications.shab_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN publications.shab_id END) AS this_year_publications
    WITH SYNONYMS ('annual_publications', 'yearly_activity')
    COMMENT = 'Publications in the current year',
  
  ROUND(
    COUNT(publications.shab_id)::FLOAT 
    / NULLIF(COUNT(DISTINCT publications.company_uid), 0), 
    2
  ) AS average_publications_per_company
    WITH SYNONYMS ('publication_frequency', 'activity_ratio')
    COMMENT = 'Average number of publications per company',
  
  -- Formation and Dissolution Activity
  COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END) AS company_formations
    WITH SYNONYMS ('new_businesses', 'incorporations', 'startups')
    COMMENT = 'Number of company formations',
  
  COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END) AS company_dissolutions
    WITH SYNONYMS ('business_closures', 'liquidations', 'terminations')
    COMMENT = 'Number of company dissolutions',
  
  (COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END) -
   COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END)) AS net_company_formation
    WITH SYNONYMS ('business_growth', 'net_incorporations')
    COMMENT = 'Net company formation (formations minus dissolutions)',
  
  -- Geographic Distribution
  COUNT(DISTINCT publications.registry_office_canton) AS unique_cantons
    WITH SYNONYMS ('canton_count', 'regional_coverage')
    COMMENT = 'Number of different Swiss cantons with registered companies',      
  
  -- Time-based Metrics
  MAX(companies.days_since_registration) AS oldest_company_age_days
    WITH SYNONYMS ('maximum_age', 'longest_registered')
    COMMENT = 'Age in days of the oldest company in the system',
  
  COUNT(DISTINCT CASE WHEN companies.registration_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN companies.company_uid END) AS companies_registered_this_year
    WITH SYNONYMS ('new_registrations', 'current_year_formations')
    COMMENT = 'Number of companies registered in the current year'
)
COMMENT = 'Semantic view for ZEFIX Swiss company data - enables Cortex Analyst natural language queries about company registrations, business activities, legal forms, and geographic distribution.' 