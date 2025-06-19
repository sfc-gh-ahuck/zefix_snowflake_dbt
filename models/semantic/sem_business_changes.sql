{{
  config(
    materialized='semantic_view'
  )
}}

-- Business Changes Semantic View
-- Focused on specific business mutations, changes, and corporate events
-- Enables queries like: "Show me all management changes this year" or "How many capital increases were there?"

CREATE OR REPLACE SEMANTIC VIEW {{ target.database }}.{{ target.schema }}.sem_business_changes
TABLES (
  companies AS {{ ref('silver_companies') }}
    PRIMARY KEY (company_uid)
    WITH SYNONYMS ('business_entities', 'firms', 'organizations')
    COMMENT = 'Core business entities registered in ZEFIX',
  
  mutations AS {{ ref('silver_mutation_types') }}
    PRIMARY KEY (mutation_type_id)
    UNIQUE (company_uid, shab_date, mutation_type_key)
    WITH SYNONYMS ('business_changes', 'company_modifications', 'corporate_events', 'mutations')
    COMMENT = 'Specific types of business changes and modifications',
  
  publications AS {{ ref('silver_shab_publications') }}
    PRIMARY KEY (shab_id)
    WITH SYNONYMS ('shab_records', 'gazette_entries', 'official_notices')
    COMMENT = 'Official business publication records from Swiss Official Gazette'
)
RELATIONSHIPS (
  mutations_to_companies AS
    mutations (company_uid)
    REFERENCES companies (company_uid),
  
  mutations_to_publications AS
    mutations (company_uid, shab_id)
    REFERENCES publications (company_uid, shab_id)
)
FACTS (
  mutations.mutation_type_id AS mutations.mutation_type_id
    WITH SYNONYMS ('change_id', 'modification_id', 'event_id')
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
  
  companies.is_active AS companies.is_active
    WITH SYNONYMS ('active_status', 'operational')
    COMMENT = 'Whether the company is currently active',
  
  -- Mutation dimensions
  mutations.company_uid AS mutations.company_uid
    WITH SYNONYMS ('business_id', 'entity_reference')
    COMMENT = 'Company identifier (foreign key)',
  
  mutations.mutation_type_key AS mutations.mutation_type_key
    WITH SYNONYMS ('change_type', 'modification_type', 'event_type', 'mutation_key')
    COMMENT = 'Type of business change/mutation',
  
  mutations.registry_office_canton AS mutations.registry_office_canton
    WITH SYNONYMS ('canton', 'state', 'region', 'province')
    COMMENT = 'Swiss canton handling the change',
  
  mutations.change_year AS EXTRACT(YEAR FROM mutations.shab_date)
    WITH SYNONYMS ('modification_year', 'event_year', 'mutation_year')
    COMMENT = 'Year when the business change occurred',
  
  mutations.change_quarter AS EXTRACT(QUARTER FROM mutations.shab_date)
    WITH SYNONYMS ('modification_quarter', 'event_quarter')
    COMMENT = 'Quarter when the business change occurred',
  
  mutations.change_month AS EXTRACT(MONTH FROM mutations.shab_date)
    WITH SYNONYMS ('modification_month', 'event_month')
    COMMENT = 'Month when the business change occurred',
  
  mutations.shab_date AS mutations.shab_date
    WITH SYNONYMS ('change_date', 'modification_date', 'event_date', 'mutation_date')
    COMMENT = 'Date of the business change publication',
  
  mutations.change_category AS CASE 
    WHEN mutations.mutation_type_key ILIKE '%organ%' OR mutations.mutation_type_key ILIKE '%verwaltung%' THEN 'Management'
    WHEN mutations.mutation_type_key ILIKE '%kapital%' OR mutations.mutation_type_key ILIKE '%capital%' THEN 'Capital'
    WHEN mutations.mutation_type_key ILIKE '%adresse%' OR mutations.mutation_type_key ILIKE '%address%' THEN 'Address'
    WHEN mutations.mutation_type_key ILIKE '%zweck%' OR mutations.mutation_type_key ILIKE '%purpose%' THEN 'Purpose'
    WHEN mutations.mutation_type_key ILIKE '%name%' OR mutations.mutation_type_key ILIKE '%firma%' THEN 'Name'
    WHEN mutations.mutation_type_key ILIKE '%auflösung%' OR mutations.mutation_type_key ILIKE '%dissolution%' THEN 'Dissolution'
    WHEN mutations.mutation_type_key ILIKE '%gründung%' OR mutations.mutation_type_key ILIKE '%formation%' THEN 'Formation'
    ELSE 'Other'
  END
    WITH SYNONYMS ('mutation_category', 'change_category', 'event_classification')
    COMMENT = 'Categorized type of business change',
  
  mutations.recency_bucket AS CASE 
    WHEN mutations.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN 'Last 30 days'
    WHEN mutations.shab_date >= CURRENT_DATE() - INTERVAL '90 days' THEN 'Last 90 days'
    WHEN mutations.shab_date >= CURRENT_DATE() - INTERVAL '365 days' THEN 'Last year'
    ELSE 'Older'
  END
    WITH SYNONYMS ('recency', 'time_category', 'age_group')
    COMMENT = 'How recent the mutation was'
)
METRICS (
  -- General Mutation Metrics
  mutations.total_mutations AS COUNT(mutations.mutation_type_id)
    WITH SYNONYMS ('change_count', 'modification_count', 'total_changes', 'mutation_count')
    COMMENT = 'Total number of business changes/mutations',
  
  mutations.recent_mutations AS COUNT(CASE WHEN mutations.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN mutations.mutation_type_id END)
    WITH SYNONYMS ('recent_changes', 'monthly_mutations', 'last_month_changes')
    COMMENT = 'Mutations in the last 30 days',
  
  mutations.this_year_mutations AS COUNT(CASE WHEN EXTRACT(YEAR FROM mutations.shab_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN mutations.mutation_type_id END)
    WITH SYNONYMS ('annual_mutations', 'yearly_changes', 'current_year_mutations')
    COMMENT = 'Mutations in the current year',
  
  mutations.unique_companies_with_changes AS COUNT(DISTINCT mutations.company_uid)
    WITH SYNONYMS ('companies_with_mutations', 'active_changing_companies')
    COMMENT = 'Number of unique companies that had mutations',
  
  mutations.average_mutations_per_company AS ROUND(
    COUNT(mutations.mutation_type_id)::FLOAT 
    / NULLIF(COUNT(DISTINCT mutations.company_uid), 0), 
    2
  )
    WITH SYNONYMS ('mutation_frequency', 'change_ratio', 'average_changes')
    COMMENT = 'Average number of mutations per company',
  
  -- Category-specific Metrics
  mutations.management_changes AS COUNT(CASE WHEN mutations.change_category = 'Management' THEN mutations.mutation_type_id END)
    WITH SYNONYMS ('leadership_changes', 'administration_changes', 'governance_mutations')
    COMMENT = 'Number of management/administration changes',
  
  mutations.capital_changes AS COUNT(CASE WHEN mutations.change_category = 'Capital' THEN mutations.mutation_type_id END)
    WITH SYNONYMS ('capital_modifications', 'financial_changes', 'capital_mutations')
    COMMENT = 'Number of capital-related changes',
  
  mutations.address_changes AS COUNT(CASE WHEN mutations.change_category = 'Address' THEN mutations.mutation_type_id END)
    WITH SYNONYMS ('location_changes', 'address_modifications', 'relocation_mutations')
    COMMENT = 'Number of address changes',
  
  mutations.purpose_changes AS COUNT(CASE WHEN mutations.change_category = 'Purpose' THEN mutations.mutation_type_id END)
    WITH SYNONYMS ('business_purpose_changes', 'activity_modifications', 'purpose_mutations')
    COMMENT = 'Number of business purpose changes',
  
  mutations.name_changes AS COUNT(CASE WHEN mutations.change_category = 'Name' THEN mutations.mutation_type_id END)
    WITH SYNONYMS ('company_name_changes', 'rebranding_events', 'name_mutations')
    COMMENT = 'Number of company name changes',
  
  -- Geographic Metrics
  mutations.unique_cantons AS COUNT(DISTINCT mutations.registry_office_canton)
    WITH SYNONYMS ('canton_count', 'regional_coverage', 'geographic_spread')
    COMMENT = 'Number of different Swiss cantons with mutation activity',
  
  mutations.mutations_per_canton AS ROUND(
    COUNT(mutations.mutation_type_id)::FLOAT 
    / NULLIF(COUNT(DISTINCT mutations.registry_office_canton), 0), 
    2
  )
    WITH SYNONYMS ('average_mutations_per_canton', 'regional_mutation_density')
    COMMENT = 'Average number of mutations per canton'
)
COMMENT = 'Business changes semantic view focusing on specific mutations, corporate events, and change patterns. Ideal for tracking detailed business modifications and governance changes.' 