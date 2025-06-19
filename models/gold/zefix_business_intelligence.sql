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
FACTS (
  -- Company facts
  companies.days_since_registration AS DATEDIFF('day', companies.first_observed_shab_date, CURRENT_DATE())
    WITH SYNONYMS ('company_age_days', 'registration_age')
    COMMENT = 'Number of days since company first registration',
  
  companies.purpose_length AS LENGTH(companies.company_purpose)
    WITH SYNONYMS ('purpose_complexity', 'description_length')
    COMMENT = 'Length of company purpose description (complexity indicator)',
  
  -- Publication facts
  publications.shab_id AS publications.shab_id
    WITH SYNONYMS ('publication_id', 'gazette_id', 'announcement_id')
    COMMENT = 'Unique publication identifier',
  
  publications.days_since_publication AS DATEDIFF('day', publications.shab_date, CURRENT_DATE())
    WITH SYNONYMS ('publication_age_days', 'days_ago')
    COMMENT = 'Number of days since this publication',
  
  -- Mutation facts
  mutations.mutation_type_id AS mutations.mutation_type_id
    WITH SYNONYMS ('change_id', 'modification_id')
    COMMENT = 'Numeric identifier for the mutation type'
)
DIMENSIONS (
  -- Company dimensions
  companies.company_uid AS companies.company_uid
    WITH SYNONYMS ('uid', 'business_id', 'entity_id')
    COMMENT = 'Unique company identifier',
  
  companies.company_name AS companies.company_name
    WITH SYNONYMS ('business_name', 'firm_name', 'entity_name')
    COMMENT = 'Official company name',
  
  companies.legal_form_id AS companies.legal_form_id
    WITH SYNONYMS ('entity_type_id', 'business_form_id', 'legal_entity_id')
    COMMENT = 'Numeric legal form identifier (1-8)',
  
  companies.legal_form_name AS CASE 
    WHEN companies.legal_form_id = 1 THEN 'Einzelunternehmen'
    WHEN companies.legal_form_id = 2 THEN 'Kollektivgesellschaft'
    WHEN companies.legal_form_id = 3 THEN 'Aktiengesellschaft'
    WHEN companies.legal_form_id = 4 THEN 'Kommanditgesellschaft'
    WHEN companies.legal_form_id = 5 THEN 'Gesellschaft mit beschränkter Haftung'
    WHEN companies.legal_form_id = 6 THEN 'Genossenschaft'
    WHEN companies.legal_form_id = 7 THEN 'Verein'
    WHEN companies.legal_form_id = 8 THEN 'Stiftung'
    ELSE 'Other'
  END
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
  
  companies.registration_year AS EXTRACT(YEAR FROM companies.first_observed_shab_date)
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
  
  publications.publication_year AS EXTRACT(YEAR FROM publications.shab_date)
    WITH SYNONYMS ('announcement_year', 'gazette_year')
    COMMENT = 'Year of SHAB publication',
  
  publications.publication_quarter AS EXTRACT(QUARTER FROM publications.shab_date)
    WITH SYNONYMS ('quarter', 'period')
    COMMENT = 'Quarter of SHAB publication',
  
  publications.shab_date AS publications.shab_date
    WITH SYNONYMS ('publication_date', 'announcement_date', 'gazette_date')
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
    WITH SYNONYMS ('business_activity', 'change_type', 'event_category')
    COMMENT = 'Type of business activity based on publication content',
  
  publications.recency_bucket AS CASE 
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN 'Last 30 days'
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '90 days' THEN 'Last 90 days'
    WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '365 days' THEN 'Last year'
    ELSE 'Older'
  END
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
  
  mutations.change_year AS EXTRACT(YEAR FROM mutations.shab_date)
    WITH SYNONYMS ('modification_year', 'event_year')
    COMMENT = 'Year when the business change occurred',
  
  mutations.shab_date AS mutations.shab_date
    WITH SYNONYMS ('change_date', 'modification_date', 'event_date')
    COMMENT = 'Date of the business change publication'
)
METRICS (
  -- Company Overview Metrics
  companies.total_companies AS COUNT(DISTINCT companies.company_uid)
    WITH SYNONYMS ('company_count', 'business_count', 'entity_count')
    COMMENT = 'Total number of companies in the system',
  
  companies.active_companies AS COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END)
    WITH SYNONYMS ('operational_companies', 'current_businesses')
    COMMENT = 'Number of currently active companies',
  
  companies.deleted_companies AS COUNT(DISTINCT CASE WHEN companies.is_active = FALSE THEN companies.company_uid END)
    WITH SYNONYMS ('dissolved_companies', 'inactive_businesses')
    COMMENT = 'Number of deleted or dissolved companies',
  
  companies.active_company_percentage AS ROUND(
    COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END) * 100.0 
    / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 
    2
  )
    WITH SYNONYMS ('activity_rate', 'operational_rate')
    COMMENT = 'Percentage of companies that are currently active',
  
  companies.oldest_company_age_days AS MAX(companies.days_since_registration)
    WITH SYNONYMS ('maximum_age', 'longest_registered')
    COMMENT = 'Age in days of the oldest company in the system',
  
  companies.companies_registered_this_year AS COUNT(DISTINCT CASE WHEN companies.registration_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN companies.company_uid END)
    WITH SYNONYMS ('new_registrations', 'current_year_formations')
    COMMENT = 'Number of companies registered in the current year',
  
  -- Publication Activity Metrics
  publications.total_publications AS COUNT(publications.shab_id)
    WITH SYNONYMS ('publication_count', 'announcement_count', 'gazette_entries')
    COMMENT = 'Total number of SHAB publications',
  
  publications.recent_publications AS COUNT(CASE WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN publications.shab_id END)
    WITH SYNONYMS ('monthly_publications', 'recent_activity')
    COMMENT = 'Publications in the last 30 days',
  
  publications.this_year_publications AS COUNT(CASE WHEN EXTRACT(YEAR FROM publications.shab_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN publications.shab_id END)
    WITH SYNONYMS ('annual_publications', 'yearly_activity')
    COMMENT = 'Publications in the current year',
  
  publications.average_publications_per_company AS ROUND(
    COUNT(publications.shab_id)::FLOAT 
    / NULLIF(COUNT(DISTINCT publications.company_uid), 0), 
    2
  )
    WITH SYNONYMS ('publication_frequency', 'activity_ratio')
    COMMENT = 'Average number of publications per company',
  
  publications.company_formations AS COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END)
    WITH SYNONYMS ('new_businesses', 'incorporations', 'startups')
    COMMENT = 'Number of company formations',
  
  publications.company_dissolutions AS COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END)
    WITH SYNONYMS ('business_closures', 'liquidations', 'terminations')
    COMMENT = 'Number of company dissolutions',
  
  publications.net_company_formation AS (COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END) -
   COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END))
    WITH SYNONYMS ('business_growth', 'net_incorporations')
    COMMENT = 'Net company formation (formations minus dissolutions)',
  
  publications.unique_cantons AS COUNT(DISTINCT publications.registry_office_canton)
    WITH SYNONYMS ('canton_count', 'regional_coverage')
    COMMENT = 'Number of different Swiss cantons with registered companies',
  
  -- Mutation Metrics
  mutations.total_mutations AS COUNT(mutations.mutation_type_id)
    WITH SYNONYMS ('change_count', 'modification_count')
    COMMENT = 'Total number of business changes/mutations'
)
COMMENT = 'Semantic view for ZEFIX Swiss company data - enables Cortex Analyst natural language queries about company registrations, business activities, legal forms, and geographic distribution.' 