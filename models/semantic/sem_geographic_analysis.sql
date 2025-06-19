{{
  config(
    materialized='semantic_view'
  )
}}

-- Geographic Analysis Semantic View
-- Focused on location-based analysis, cantonal distribution, and regional patterns
-- Enables queries like: "Which canton has the most companies?" or "Show business activity by region"

CREATE OR REPLACE SEMANTIC VIEW {{ target.database }}.{{ target.schema }}.sem_geographic_analysis
TABLES (
  companies AS {{ ref('silver_companies') }}
    PRIMARY KEY (company_uid)
    WITH SYNONYMS ('business_entities', 'firms', 'organizations')
    COMMENT = 'Core business entities registered in ZEFIX',
  
  publications AS {{ ref('silver_shab_publications') }}
    PRIMARY KEY (shab_id)
    UNIQUE (company_uid, shab_date)
    WITH SYNONYMS ('shab_records', 'gazette_entries', 'official_notices')
    COMMENT = 'Official business publication records from Swiss Official Gazette'
)
RELATIONSHIPS (
  publications_to_companies AS
    publications (company_uid)
    REFERENCES companies (company_uid)
)
FACTS (
  companies.days_since_registration AS DATEDIFF('day', companies.first_observed_shab_date, CURRENT_DATE())
    WITH SYNONYMS ('company_age_days', 'registration_age')
    COMMENT = 'Number of days since company first registration',
  
  publications.shab_id AS publications.shab_id
    WITH SYNONYMS ('publication_id', 'gazette_id', 'announcement_id')
    COMMENT = 'Unique publication identifier'
)
DIMENSIONS (
  -- Company Geographic Dimensions
  companies.company_uid AS companies.company_uid
    WITH SYNONYMS ('uid', 'business_id', 'entity_id')
    COMMENT = 'Unique company identifier',
  
  companies.company_name AS companies.company_name
    WITH SYNONYMS ('business_name', 'firm_name', 'entity_name')
    COMMENT = 'Official company name',
  
  companies.legal_seat AS companies.legal_seat
    WITH SYNONYMS ('domicile', 'headquarters', 'seat', 'legal_domicile')
    COMMENT = 'Legal domicile of the company',
  
  companies.address_town AS companies.address_town
    WITH SYNONYMS ('city', 'location', 'municipality', 'town', 'commune')
    COMMENT = 'Town/city where company is located',
  
  companies.address_zip_code AS companies.address_zip_code
    WITH SYNONYMS ('postal_code', 'zip', 'plz')
    COMMENT = 'Swiss postal code',
  
  companies.address_country AS companies.address_country
    WITH SYNONYMS ('country', 'nation', 'country_code')
    COMMENT = 'Country code (typically CH for Switzerland)',
  
  companies.is_active AS companies.is_active
    WITH SYNONYMS ('active_status', 'operational')
    COMMENT = 'Whether the company is currently active',
  
  -- Publication Geographic Dimensions
  publications.registry_office_canton AS publications.registry_office_canton
    WITH SYNONYMS ('canton', 'state', 'region', 'province', 'kanton')
    COMMENT = 'Swiss canton handling the publication',
  
  publications.publication_year AS EXTRACT(YEAR FROM publications.shab_date)
    WITH SYNONYMS ('announcement_year', 'gazette_year')
    COMMENT = 'Year of SHAB publication',
  
  publications.shab_date AS publications.shab_date
    WITH SYNONYMS ('publication_date', 'announcement_date', 'gazette_date')
    COMMENT = 'Date of SHAB publication',
  
  -- Regional Groupings
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
    COMMENT = 'Economic classification of Swiss regions'
)
METRICS (
  -- Company Distribution Metrics
  companies.total_companies AS COUNT(DISTINCT companies.company_uid)
    WITH SYNONYMS ('company_count', 'business_count', 'entity_count')
    COMMENT = 'Total number of companies',
  
  companies.active_companies AS COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END)
    WITH SYNONYMS ('operational_companies', 'current_businesses')
    COMMENT = 'Number of currently active companies',
  
  companies.unique_towns AS COUNT(DISTINCT companies.address_town)
    WITH SYNONYMS ('city_count', 'municipality_count', 'location_count')
    COMMENT = 'Number of unique towns/cities with companies',
  
  companies.unique_zip_codes AS COUNT(DISTINCT companies.address_zip_code)
    WITH SYNONYMS ('postal_code_count', 'zip_count', 'plz_count')
    COMMENT = 'Number of unique postal codes with companies',
  
  companies.companies_per_town AS ROUND(
    COUNT(DISTINCT companies.company_uid)::FLOAT 
    / NULLIF(COUNT(DISTINCT companies.address_town), 0), 
    2
  )
    WITH SYNONYMS ('business_density_per_town', 'companies_per_municipality')
    COMMENT = 'Average number of companies per town/city',
  
  -- Canton-level Metrics
  publications.unique_cantons AS COUNT(DISTINCT publications.registry_office_canton)
    WITH SYNONYMS ('canton_count', 'regional_coverage', 'state_count')
    COMMENT = 'Number of different Swiss cantons with activity',
  
  publications.total_publications AS COUNT(publications.shab_id)
    WITH SYNONYMS ('publication_count', 'announcement_count', 'gazette_entries')
    COMMENT = 'Total number of SHAB publications',
  
  publications.publications_per_canton AS ROUND(
    COUNT(publications.shab_id)::FLOAT 
    / NULLIF(COUNT(DISTINCT publications.registry_office_canton), 0), 
    2
  )
    WITH SYNONYMS ('activity_per_canton', 'regional_activity_density')
    COMMENT = 'Average number of publications per canton',
  
  -- Language Region Metrics
  publications.german_region_activity AS COUNT(CASE WHEN publications.language_region = 'German' THEN publications.shab_id END)
    WITH SYNONYMS ('german_speaking_activity', 'deutschschweiz_activity')
    COMMENT = 'Business activity in German-speaking regions',
  
  publications.french_region_activity AS COUNT(CASE WHEN publications.language_region = 'French' THEN publications.shab_id END)
    WITH SYNONYMS ('french_speaking_activity', 'suisse_romande_activity')
    COMMENT = 'Business activity in French-speaking regions',
  
  publications.italian_region_activity AS COUNT(CASE WHEN publications.language_region = 'Italian' THEN publications.shab_id END)
    WITH SYNONYMS ('italian_speaking_activity', 'ticino_activity')
    COMMENT = 'Business activity in Italian-speaking regions',
  
  -- Economic Region Metrics
  publications.major_centers_activity AS COUNT(CASE WHEN publications.economic_region = 'Major Economic Centers' THEN publications.shab_id END)
    WITH SYNONYMS ('financial_centers_activity', 'major_cities_activity')
    COMMENT = 'Business activity in major economic centers',
  
  publications.regional_centers_activity AS COUNT(CASE WHEN publications.economic_region = 'Regional Centers' THEN publications.shab_id END)
    WITH SYNONYMS ('medium_cities_activity', 'regional_hubs_activity')
    COMMENT = 'Business activity in regional centers',
  
  publications.alpine_cantons_activity AS COUNT(CASE WHEN publications.economic_region = 'Alpine Cantons' THEN publications.shab_id END)
    WITH SYNONYMS ('mountain_regions_activity', 'rural_activity')
    COMMENT = 'Business activity in alpine/mountain cantons',
  
  -- Concentration Metrics
  publications.activity_concentration_ratio AS ROUND(
    COUNT(CASE WHEN publications.registry_office_canton IN ('ZH', 'GE', 'VD', 'BE') THEN publications.shab_id END) * 100.0
    / NULLIF(COUNT(publications.shab_id), 0),
    2
  )
    WITH SYNONYMS ('top_cantons_concentration', 'regional_concentration')
    COMMENT = 'Percentage of activity concentrated in top 4 cantons'
)
COMMENT = 'Geographic analysis semantic view focusing on regional distribution, cantonal patterns, and location-based business intelligence. Ideal for understanding regional business dynamics and geographic concentration patterns.' 