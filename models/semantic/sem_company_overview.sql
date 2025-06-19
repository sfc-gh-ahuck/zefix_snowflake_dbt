{{
  config(
    materialized='semantic_view'
  )
}}

-- Company Overview Semantic View
-- Focused on basic company information, legal forms, and status
-- Enables queries like: "How many active companies are there?" or "Show me all AG companies"

CREATE OR REPLACE SEMANTIC VIEW {{ target.database }}.{{ target.schema }}.sem_company_overview
TABLES (
  companies AS {{ ref('silver_companies') }}
    PRIMARY KEY (company_uid)
    WITH SYNONYMS ('business_entities', 'firms', 'organizations', 'enterprises')
    COMMENT = 'Core business entities registered in ZEFIX'
)
FACTS (
  companies.days_since_registration AS DATEDIFF('day', companies.first_observed_shab_date, CURRENT_DATE())
    WITH SYNONYMS ('company_age_days', 'registration_age', 'business_age')
    COMMENT = 'Number of days since company first registration',
  
  companies.purpose_length AS LENGTH(companies.company_purpose)
    WITH SYNONYMS ('purpose_complexity', 'description_length', 'purpose_detail')
    COMMENT = 'Length of company purpose description (complexity indicator)'
)
DIMENSIONS (
  companies.company_uid AS companies.company_uid
    WITH SYNONYMS ('uid', 'business_id', 'entity_id', 'company_id')
    COMMENT = 'Unique company identifier',
  
  companies.company_name AS companies.company_name
    WITH SYNONYMS ('business_name', 'firm_name', 'entity_name', 'organization_name')
    COMMENT = 'Official company name',
  
  companies.legal_form_id AS companies.legal_form_id
    WITH SYNONYMS ('entity_type_id', 'business_form_id', 'legal_entity_id')
    COMMENT = 'Numeric legal form identifier (1-8)',
  
  companies.legal_form_name AS companies.legal_form_name
    WITH SYNONYMS ('entity_type', 'business_form', 'corporate_structure', 'legal_entity_type')
    COMMENT = 'Type of legal entity (AG, GmbH, etc.)',
  
  companies.legal_form_name_en AS companies.legal_form_name_en
    WITH SYNONYMS ('entity_type_english', 'business_form_english', 'legal_entity_type_en')
    COMMENT = 'English translation of legal entity type',
  
  companies.abbreviation AS companies.abbreviation
    WITH SYNONYMS ('legal_form_abbreviation', 'entity_abbreviation', 'form_abbrev')
    COMMENT = 'Abbreviation for legal form (e.g., AG, GmbH)',
  
  companies.company_status AS companies.company_status
    WITH SYNONYMS ('status', 'state', 'condition', 'business_status')
    COMMENT = 'Current status of the company (EXISTIEREND/GELOESCHT)',
  
  companies.is_active AS companies.is_active
    WITH SYNONYMS ('active_status', 'operational', 'active_flag')
    COMMENT = 'Whether the company is currently active',
  
  companies.legal_seat AS companies.legal_seat
    WITH SYNONYMS ('domicile', 'headquarters', 'seat', 'legal_domicile')
    COMMENT = 'Legal domicile of the company',
  
  companies.address_town AS companies.address_town
    WITH SYNONYMS ('city', 'location', 'municipality', 'town')
    COMMENT = 'Town/city where company is located',
  
  companies.company_purpose AS companies.company_purpose
    WITH SYNONYMS ('business_purpose', 'purpose', 'activity_description', 'business_activity', 'company_description')
    COMMENT = 'Official business purpose and activity description of the company',
  
  companies.registration_year AS EXTRACT(YEAR FROM companies.first_observed_shab_date)
    WITH SYNONYMS ('founding_year', 'incorporation_year', 'establishment_year')
    COMMENT = 'Year when company first appeared in register'
)
METRICS (
  companies.total_companies AS COUNT(DISTINCT companies.company_uid)
    WITH SYNONYMS ('company_count', 'business_count', 'entity_count', 'total_entities')
    COMMENT = 'Total number of companies in the system',
  
  companies.active_companies AS COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END)
    WITH SYNONYMS ('operational_companies', 'current_businesses', 'active_entities')
    COMMENT = 'Number of currently active companies',
  
  companies.deleted_companies AS COUNT(DISTINCT CASE WHEN companies.is_active = FALSE THEN companies.company_uid END)
    WITH SYNONYMS ('dissolved_companies', 'inactive_businesses', 'closed_companies')
    COMMENT = 'Number of deleted or dissolved companies',
  
  companies.active_company_percentage AS ROUND(
    COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END) * 100.0 
    / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 
    2
  )
    WITH SYNONYMS ('activity_rate', 'operational_rate', 'active_percentage')
    COMMENT = 'Percentage of companies that are currently active',
  
  companies.ag_companies AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 3 THEN companies.company_uid END)
    WITH SYNONYMS ('aktiengesellschaft_count', 'stock_companies', 'ag_count')
    COMMENT = 'Number of stock companies (Aktiengesellschaft)',
  
  companies.gmbh_companies AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 5 THEN companies.company_uid END)
    WITH SYNONYMS ('limited_liability_companies', 'gmbh_count', 'llc_count')
    COMMENT = 'Number of limited liability companies (GmbH)',
  
  companies.verein_companies AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 7 THEN companies.company_uid END)
    WITH SYNONYMS ('associations', 'verein_count', 'association_count')
    COMMENT = 'Number of associations (Verein)',
  
  companies.oldest_company_age_days AS MAX(companies.days_since_registration)
    WITH SYNONYMS ('maximum_age', 'longest_registered', 'oldest_business')
    COMMENT = 'Age in days of the oldest company in the system',
  
  companies.companies_registered_this_year AS COUNT(DISTINCT CASE WHEN companies.registration_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN companies.company_uid END)
    WITH SYNONYMS ('new_registrations', 'current_year_formations', 'this_year_companies')
    COMMENT = 'Number of companies registered in the current year'
)
COMMENT = 'Company overview semantic view focusing on basic company information, legal forms, and operational status. Ideal for general business intelligence and company demographics analysis.' 