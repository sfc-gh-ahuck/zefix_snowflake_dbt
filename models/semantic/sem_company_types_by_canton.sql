{{
  config(
    materialized='semantic_view'
  )
}}

-- Company Types by Canton Semantic View
-- Focused on legal form distribution across Swiss cantons and business structure analysis
-- Enables queries like: "Which canton has the most stock companies?" or "Show AG distribution by region"

CREATE OR REPLACE SEMANTIC VIEW {{ target.database }}.{{ target.schema }}.sem_company_types_by_canton
TABLES (
  companies AS {{ ref('silver_companies') }}
    PRIMARY KEY (company_uid)
    WITH SYNONYMS ('business_entities', 'firms', 'organizations')
    COMMENT = 'Core business entities registered in ZEFIX',
  
  legal_forms AS {{ ref('silver_legal_forms') }}
    PRIMARY KEY (legal_form_id)
    WITH SYNONYMS ('entity_types', 'business_forms', 'corporate_structures')
    COMMENT = 'Reference data for Swiss legal forms and business structures',
  
  publications AS {{ ref('silver_shab_publications') }}
    PRIMARY KEY (shab_id)
    WITH SYNONYMS ('shab_records', 'gazette_entries', 'official_notices')
    COMMENT = 'Official business publication records from Swiss Official Gazette'
)
RELATIONSHIPS (
  companies_to_legal_forms AS
    companies (legal_form_id)
    REFERENCES legal_forms (legal_form_id),
  
  publications_to_companies AS
    publications (company_uid)
    REFERENCES companies (company_uid)
)
FACTS (
  companies.days_since_registration AS DATEDIFF('day', companies.first_observed_shab_date, CURRENT_DATE())
    WITH SYNONYMS ('company_age_days', 'registration_age', 'business_age')
    COMMENT = 'Number of days since company first registration'
)
DIMENSIONS (
  -- Company Legal Form Dimensions
  companies.company_uid AS companies.company_uid
    WITH SYNONYMS ('uid', 'business_id', 'entity_id')
    COMMENT = 'Unique company identifier',
  
  companies.company_name AS companies.company_name
    WITH SYNONYMS ('business_name', 'firm_name', 'entity_name')
    COMMENT = 'Official company name',
  
  companies.legal_form_id AS companies.legal_form_id
    WITH SYNONYMS ('entity_type_id', 'business_form_id', 'legal_entity_id')
    COMMENT = 'Numeric legal form identifier (1-8)',
  
  companies.legal_form_name AS companies.legal_form_name
    WITH SYNONYMS ('entity_type', 'business_form', 'corporate_structure', 'legal_entity_type')
    COMMENT = 'German name of legal entity type',
  
  companies.legal_form_name_en AS companies.legal_form_name_en
    WITH SYNONYMS ('entity_type_english', 'business_form_english', 'legal_entity_type_en')
    COMMENT = 'English translation of legal entity type',
  
  companies.abbreviation AS companies.abbreviation
    WITH SYNONYMS ('legal_form_abbreviation', 'entity_abbreviation', 'form_abbrev')
    COMMENT = 'Abbreviation for legal form (e.g., AG, GmbH)',
  
  companies.is_active AS companies.is_active
    WITH SYNONYMS ('active_status', 'operational')
    COMMENT = 'Whether the company is currently active',
  
  companies.legal_seat AS companies.legal_seat
    WITH SYNONYMS ('domicile', 'headquarters', 'seat', 'legal_domicile')
    COMMENT = 'Legal domicile of the company',
  
  companies.address_town AS companies.address_town
    WITH SYNONYMS ('city', 'location', 'municipality', 'town')
    COMMENT = 'Town/city where company is located',
  
  -- Legal Form Reference Dimensions
  legal_forms.legal_form_id AS legal_forms.legal_form_id
    WITH SYNONYMS ('form_id', 'type_id', 'structure_id')
    COMMENT = 'Legal form identifier',
  
  legal_forms.legal_form_name_de AS legal_forms.legal_form_name_de
    WITH SYNONYMS ('german_name', 'deutsche_bezeichnung', 'legal_name_de')
    COMMENT = 'German name of legal form',
  
  legal_forms.legal_form_name_en AS legal_forms.legal_form_name_en
    WITH SYNONYMS ('english_name', 'legal_name_en', 'english_translation')
    COMMENT = 'English name of legal form',
  
  legal_forms.abbreviation AS legal_forms.abbreviation
    WITH SYNONYMS ('short_form', 'abbrev', 'code')
    COMMENT = 'Standard abbreviation for legal form',
  
  -- Canton and Geographic Dimensions
  publications.registry_office_canton AS publications.registry_office_canton
    WITH SYNONYMS ('canton', 'state', 'region', 'province', 'kanton')
    COMMENT = 'Swiss canton handling company registration',
  
  publications.language_region AS CASE 
    WHEN publications.registry_office_canton IN ('ZH', 'BE', 'LU', 'UR', 'SZ', 'OW', 'NW', 'GL', 'ZG', 'SO', 'BS', 'BL', 'SH', 'AR', 'AI', 'SG', 'GR', 'AG', 'TG') THEN 'German'
    WHEN publications.registry_office_canton IN ('VD', 'VS', 'NE', 'GE', 'JU') THEN 'French'
    WHEN publications.registry_office_canton = 'TI' THEN 'Italian'
    ELSE 'Other'
  END
    WITH SYNONYMS ('linguistic_region', 'language_area', 'cultural_region')
    COMMENT = 'Swiss linguistic region based on canton',
  
  publications.economic_region AS CASE 
    WHEN publications.registry_office_canton IN ('ZH', 'ZG', 'BS', 'GE') THEN 'Major Economic Centers'
    WHEN publications.registry_office_canton IN ('VD', 'BE', 'AG', 'SG', 'TI') THEN 'Regional Centers'
    WHEN publications.registry_office_canton IN ('LU', 'SO', 'BL', 'SH', 'TG', 'GR', 'NE', 'VS', 'JU') THEN 'Smaller Cantons'
    WHEN publications.registry_office_canton IN ('UR', 'SZ', 'OW', 'NW', 'GL', 'AR', 'AI') THEN 'Alpine Cantons'
    ELSE 'Other'
  END
    WITH SYNONYMS ('business_region', 'economic_zone', 'commercial_area')
    COMMENT = 'Economic classification of Swiss regions',
  
  -- Legal Form Categories
  companies.business_category AS CASE 
    WHEN companies.legal_form_id IN (3, 4) THEN 'Corporations'
    WHEN companies.legal_form_id = 5 THEN 'Limited Liability'
    WHEN companies.legal_form_id IN (1, 2) THEN 'Partnerships'
    WHEN companies.legal_form_id IN (7, 8) THEN 'Non-Profit'
    WHEN companies.legal_form_id = 6 THEN 'Cooperatives'
    ELSE 'Other'
  END
    WITH SYNONYMS ('entity_category', 'business_type', 'legal_category')
    COMMENT = 'High-level categorization of business legal forms',
  
  companies.liability_type AS CASE 
    WHEN companies.legal_form_id IN (3, 5) THEN 'Limited Liability'
    WHEN companies.legal_form_id IN (1, 2, 4) THEN 'Unlimited/Mixed Liability'
    WHEN companies.legal_form_id IN (6, 7, 8) THEN 'Association/Non-Profit'
    ELSE 'Other'
  END
    WITH SYNONYMS ('liability_structure', 'risk_profile', 'liability_classification')
    COMMENT = 'Liability structure classification',
  
  -- Time Dimensions
  companies.registration_year AS EXTRACT(YEAR FROM companies.first_observed_shab_date)
    WITH SYNONYMS ('founding_year', 'incorporation_year', 'establishment_year')
    COMMENT = 'Year when company first appeared in register'
)
METRICS (
  -- Overall Company Metrics
  companies.total_companies AS COUNT(DISTINCT companies.company_uid)
    WITH SYNONYMS ('company_count', 'business_count', 'entity_count')
    COMMENT = 'Total number of companies',
  
  companies.active_companies AS COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END)
    WITH SYNONYMS ('operational_companies', 'current_businesses', 'active_entities')
    COMMENT = 'Number of currently active companies',
  
  companies.unique_cantons AS COUNT(DISTINCT publications.registry_office_canton)
    WITH SYNONYMS ('canton_count', 'regional_coverage', 'geographic_spread')
    COMMENT = 'Number of different Swiss cantons with companies',
  
  -- Legal Form Distribution Metrics
  companies.aktiengesellschaft_count AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 3 THEN companies.company_uid END)
    WITH SYNONYMS ('ag_count', 'stock_companies', 'corporations', 'aktiengesellschaften')
    COMMENT = 'Number of stock companies (Aktiengesellschaft)',
  
  companies.gmbh_count AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 5 THEN companies.company_uid END)
    WITH SYNONYMS ('limited_liability_companies', 'llc_count', 'gesellschaften_mbh')
    COMMENT = 'Number of limited liability companies (GmbH)',
  
  companies.einzelunternehmen_count AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 1 THEN companies.company_uid END)
    WITH SYNONYMS ('sole_proprietorships', 'individual_businesses', 'einzelfirmen')
    COMMENT = 'Number of sole proprietorships (Einzelunternehmen)',
  
  companies.verein_count AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 7 THEN companies.company_uid END)
    WITH SYNONYMS ('associations', 'non_profit_associations', 'vereinigungen')
    COMMENT = 'Number of associations (Verein)',
  
  companies.stiftung_count AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 8 THEN companies.company_uid END)
    WITH SYNONYMS ('foundations', 'charitable_foundations', 'stiftungen')
    COMMENT = 'Number of foundations (Stiftung)',
  
  companies.genossenschaft_count AS COUNT(DISTINCT CASE WHEN companies.legal_form_id = 6 THEN companies.company_uid END)
    WITH SYNONYMS ('cooperatives', 'cooperative_societies', 'genossenschaften')
    COMMENT = 'Number of cooperatives (Genossenschaft)',
  
  -- Percentage Metrics
  companies.ag_percentage AS ROUND(
    COUNT(DISTINCT CASE WHEN companies.legal_form_id = 3 THEN companies.company_uid END) * 100.0
    / NULLIF(COUNT(DISTINCT companies.company_uid), 0),
    2
  )
    WITH SYNONYMS ('stock_company_percentage', 'ag_share', 'corporation_rate')
    COMMENT = 'Percentage of companies that are stock companies',
  
  companies.gmbh_percentage AS ROUND(
    COUNT(DISTINCT CASE WHEN companies.legal_form_id = 5 THEN companies.company_uid END) * 100.0
    / NULLIF(COUNT(DISTINCT companies.company_uid), 0),
    2
  )
    WITH SYNONYMS ('limited_liability_percentage', 'gmbh_share', 'llc_rate')
    COMMENT = 'Percentage of companies that are limited liability companies',
  
  companies.limited_liability_percentage AS ROUND(
    COUNT(DISTINCT CASE WHEN companies.legal_form_id IN (3, 5) THEN companies.company_uid END) * 100.0
    / NULLIF(COUNT(DISTINCT companies.company_uid), 0),
    2
  )
    WITH SYNONYMS ('limited_companies_percentage', 'liability_protection_rate')
    COMMENT = 'Percentage of companies with limited liability structure',
  
  -- Category-based Metrics
  companies.corporations_count AS COUNT(DISTINCT CASE WHEN companies.business_category = 'Corporations' THEN companies.company_uid END)
    WITH SYNONYMS ('corporate_entities', 'corporation_count', 'kapitalgesellschaften')
    COMMENT = 'Number of corporations (AG, KG)',
  
  companies.non_profit_count AS COUNT(DISTINCT CASE WHEN companies.business_category = 'Non-Profit' THEN companies.company_uid END)
    WITH SYNONYMS ('non_profit_organizations', 'charitable_entities', 'gemeinnuetzige_organisationen')
    COMMENT = 'Number of non-profit organizations (Verein, Stiftung)',
  
  companies.partnerships_count AS COUNT(DISTINCT CASE WHEN companies.business_category = 'Partnerships' THEN companies.company_uid END)
    WITH SYNONYMS ('partnership_entities', 'personengesellschaften')
    COMMENT = 'Number of partnerships (Einzelunternehmen, Kollektivgesellschaft)',
  
  -- Geographic Concentration Metrics
  companies.companies_per_canton AS ROUND(
    COUNT(DISTINCT companies.company_uid)::FLOAT 
    / NULLIF(COUNT(DISTINCT publications.registry_office_canton), 0), 
    2
  )
    WITH SYNONYMS ('business_density_per_canton', 'regional_density', 'cantonal_concentration')
    COMMENT = 'Average number of companies per canton',
  
  companies.top_canton_concentration AS ROUND(
    COUNT(DISTINCT CASE WHEN publications.registry_office_canton IN ('ZH', 'GE', 'VD', 'BE') THEN companies.company_uid END) * 100.0
    / NULLIF(COUNT(DISTINCT companies.company_uid), 0),
    2
  )
    WITH SYNONYMS ('major_canton_share', 'geographic_concentration', 'top_regions_percentage')
    COMMENT = 'Percentage of companies concentrated in top 4 cantons',
  
  -- Business Structure Diversity
  companies.legal_form_diversity AS COUNT(DISTINCT companies.legal_form_id)
    WITH SYNONYMS ('entity_type_variety', 'business_form_diversity', 'structural_diversity')
    COMMENT = 'Number of different legal forms present',
  
  companies.herfindahl_index AS ROUND(
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 1 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2) +
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 2 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2) +
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 3 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2) +
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 4 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2) +
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 5 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2) +
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 6 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2) +
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 7 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2) +
    POWER(COUNT(DISTINCT CASE WHEN companies.legal_form_id = 8 THEN companies.company_uid END) * 100.0 / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 2),
    2
  )
    WITH SYNONYMS ('market_concentration_index', 'business_structure_concentration')
    COMMENT = 'Herfindahl-Hirschman Index measuring concentration of legal forms (0-10000)',
  
  -- Language Region Analysis
  companies.german_region_companies AS COUNT(DISTINCT CASE WHEN publications.language_region = 'German' THEN companies.company_uid END)
    WITH SYNONYMS ('deutschschweiz_companies', 'german_speaking_businesses')
    COMMENT = 'Number of companies in German-speaking regions',
  
  companies.french_region_companies AS COUNT(DISTINCT CASE WHEN publications.language_region = 'French' THEN companies.company_uid END)
    WITH SYNONYMS ('suisse_romande_companies', 'french_speaking_businesses')
    COMMENT = 'Number of companies in French-speaking regions',
  
  companies.italian_region_companies AS COUNT(DISTINCT CASE WHEN publications.language_region = 'Italian' THEN companies.company_uid END)
    WITH SYNONYMS ('ticino_companies', 'italian_speaking_businesses')
    COMMENT = 'Number of companies in Italian-speaking regions'
)
COMMENT = 'Company types by canton semantic view focusing on legal form distribution across Swiss regions. Ideal for analyzing business structure patterns, regional preferences for corporate forms, and geographic concentration of different entity types.' 