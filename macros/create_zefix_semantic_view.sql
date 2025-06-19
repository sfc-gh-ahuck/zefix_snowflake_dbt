{% macro create_zefix_semantic_view() %}

  {%- set semantic_view_sql -%}
    CREATE OR REPLACE SEMANTIC VIEW {{ target.database }}.{{ target.schema }}.zefix_business_intelligence AS
    LOGICAL_TABLES:
      
      -- Companies logical table - core business entity
      companies:
        TABLE: {{ ref('silver_companies') }}
        DIMENSIONS:
          - company_uid:
              DESCRIPTION: "Unique company identifier"
              DATA_TYPE: STRING
          - company_name:
              DESCRIPTION: "Official company name"
              DATA_TYPE: STRING
          - legal_form_name:
              DESCRIPTION: "Type of legal entity (AG, GmbH, etc.)"
              DATA_TYPE: STRING
              EXPR: |
                CASE 
                  WHEN legal_form_id = 1 THEN 'Einzelunternehmen'
                  WHEN legal_form_id = 2 THEN 'Kollektivgesellschaft'
                  WHEN legal_form_id = 3 THEN 'Aktiengesellschaft'
                  WHEN legal_form_id = 4 THEN 'Kommanditgesellschaft'
                  WHEN legal_form_id = 5 THEN 'Gesellschaft mit beschränkter Haftung'
                  WHEN legal_form_id = 6 THEN 'Genossenschaft'
                  WHEN legal_form_id = 7 THEN 'Verein'
                  WHEN legal_form_id = 8 THEN 'Stiftung'
                  ELSE 'Other'
                END
          - company_status:
              DESCRIPTION: "Current status of the company (Active/Deleted)"
              DATA_TYPE: STRING
          - legal_seat:
              DESCRIPTION: "Legal domicile of the company"
              DATA_TYPE: STRING
          - address_town:
              DESCRIPTION: "Town/city where company is located"
              DATA_TYPE: STRING
          - is_active:
              DESCRIPTION: "Whether the company is currently active"
              DATA_TYPE: BOOLEAN
          - registration_year:
              DESCRIPTION: "Year when company first appeared in register"
              DATA_TYPE: NUMBER
              EXPR: EXTRACT(YEAR FROM first_observed_shab_date)
          - shab_date:
              DESCRIPTION: "Most recent SHAB publication date"
              DATA_TYPE: DATE
        
        FACTS:
          - days_since_registration:
              DESCRIPTION: "Number of days since company first registration"
              DATA_TYPE: NUMBER
              EXPR: DATEDIFF('day', first_observed_shab_date, CURRENT_DATE())
          - purpose_length:
              DESCRIPTION: "Length of company purpose description (complexity indicator)"
              DATA_TYPE: NUMBER
              EXPR: LENGTH(company_purpose)

      -- Publications logical table - represents business activity
      publications:
        TABLE: {{ ref('silver_shab_publications') }}
        DIMENSIONS:
          - company_uid:
              DESCRIPTION: "Company identifier (foreign key)"
              DATA_TYPE: STRING
          - registry_office_canton:
              DESCRIPTION: "Swiss canton handling the publication"
              DATA_TYPE: STRING
          - publication_year:
              DESCRIPTION: "Year of SHAB publication"
              DATA_TYPE: NUMBER
              EXPR: EXTRACT(YEAR FROM shab_date)
          - publication_quarter:
              DESCRIPTION: "Quarter of SHAB publication"
              DATA_TYPE: NUMBER
              EXPR: EXTRACT(QUARTER FROM shab_date)
          - shab_date:
              DESCRIPTION: "Date of SHAB publication"
              DATA_TYPE: DATE
          - activity_type:
              DESCRIPTION: "Type of business activity based on publication content"
              DATA_TYPE: STRING
              EXPR: |
                CASE 
                  WHEN publication_message ILIKE '%gründung%' OR publication_message ILIKE '%constitution%' THEN 'Formation'
                  WHEN publication_message ILIKE '%auflösung%' OR publication_message ILIKE '%dissolution%' THEN 'Dissolution'
                  WHEN publication_message ILIKE '%kapital%' OR publication_message ILIKE '%capital%' THEN 'Capital Change'
                  WHEN publication_message ILIKE '%adresse%' OR publication_message ILIKE '%address%' THEN 'Address Change'
                  WHEN publication_message ILIKE '%verwaltung%' OR publication_message ILIKE '%administration%' THEN 'Management Change'
                  WHEN publication_message ILIKE '%zweck%' OR publication_message ILIKE '%purpose%' THEN 'Purpose Change'
                  WHEN publication_message ILIKE '%fusion%' OR publication_message ILIKE '%merger%' THEN 'Merger'
                  ELSE 'Other'
                END
          - recency_bucket:
              DESCRIPTION: "How recent the publication activity was"
              DATA_TYPE: STRING
              EXPR: |
                CASE 
                  WHEN shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN 'Last 30 days'
                  WHEN shab_date >= CURRENT_DATE() - INTERVAL '90 days' THEN 'Last 90 days'
                  WHEN shab_date >= CURRENT_DATE() - INTERVAL '365 days' THEN 'Last year'
                  ELSE 'Older'
                END
        
        FACTS:
          - shab_id:
              DESCRIPTION: "Unique publication identifier"
              DATA_TYPE: NUMBER
          - days_since_publication:
              DESCRIPTION: "Number of days since this publication"
              DATA_TYPE: NUMBER
              EXPR: DATEDIFF('day', shab_date, CURRENT_DATE())

      -- Mutations logical table - represents specific business changes
      mutations:
        TABLE: {{ ref('silver_mutation_types') }}
        DIMENSIONS:
          - company_uid:
              DESCRIPTION: "Company identifier (foreign key)"
              DATA_TYPE: STRING
          - mutation_type_key:
              DESCRIPTION: "Type of business change/mutation"
              DATA_TYPE: STRING
          - registry_office_canton:
              DESCRIPTION: "Swiss canton handling the change"
              DATA_TYPE: STRING
          - change_year:
              DESCRIPTION: "Year when the business change occurred"
              DATA_TYPE: NUMBER
              EXPR: EXTRACT(YEAR FROM shab_date)
          - shab_date:
              DESCRIPTION: "Date of the business change publication"
              DATA_TYPE: DATE

        FACTS:
          - mutation_type_id:
              DESCRIPTION: "Numeric identifier for the mutation type"
              DATA_TYPE: NUMBER

    -- Define relationships between logical tables
    RELATIONSHIPS:
      - FROM: publications
        TO: companies
        JOIN_TYPE: LEFT
        ON: publications.company_uid = companies.company_uid
        
      - FROM: mutations
        TO: companies
        JOIN_TYPE: LEFT
        ON: mutations.company_uid = companies.company_uid
        
      - FROM: mutations
        TO: publications
        JOIN_TYPE: LEFT
        ON: mutations.company_uid = publications.company_uid 
            AND mutations.shab_date = publications.shab_date

    -- Define business metrics for analysis
    METRICS:
      
      # Company Overview Metrics
      - total_companies:
          DESCRIPTION: "Total number of companies in the system"
          EXPR: COUNT(DISTINCT companies.company_uid)
          
      - active_companies:
          DESCRIPTION: "Number of currently active companies"
          EXPR: COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END)
          
      - deleted_companies:
          DESCRIPTION: "Number of deleted or dissolved companies"
          EXPR: COUNT(DISTINCT CASE WHEN companies.is_active = FALSE THEN companies.company_uid END)
          
      - active_company_percentage:
          DESCRIPTION: "Percentage of companies that are currently active"
          EXPR: |
            ROUND(
              COUNT(DISTINCT CASE WHEN companies.is_active = TRUE THEN companies.company_uid END) * 100.0 
              / NULLIF(COUNT(DISTINCT companies.company_uid), 0), 
              2
            )
      
      # Publication Activity Metrics
      - total_publications:
          DESCRIPTION: "Total number of SHAB publications"
          EXPR: COUNT(publications.shab_id)
          
      - recent_publications:
          DESCRIPTION: "Publications in the last 30 days"
          EXPR: COUNT(CASE WHEN publications.shab_date >= CURRENT_DATE() - INTERVAL '30 days' THEN publications.shab_id END)
          
      - this_year_publications:
          DESCRIPTION: "Publications in the current year"
          EXPR: COUNT(CASE WHEN publications.publication_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN publications.shab_id END)
          
      - average_publications_per_company:
          DESCRIPTION: "Average number of publications per company"
          EXPR: |
            ROUND(
              COUNT(publications.shab_id)::FLOAT 
              / NULLIF(COUNT(DISTINCT publications.company_uid), 0), 
              2
            )
      
      # Formation and Dissolution Activity
      - company_formations:
          DESCRIPTION: "Number of company formations"
          EXPR: COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END)
          
      - company_dissolutions:
          DESCRIPTION: "Number of company dissolutions"
          EXPR: COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END)
          
      - net_company_formation:
          DESCRIPTION: "Net company formation (formations minus dissolutions)"
          EXPR: |
            COUNT(CASE WHEN publications.activity_type = 'Formation' THEN publications.shab_id END) -
            COUNT(CASE WHEN publications.activity_type = 'Dissolution' THEN publications.shab_id END)
      
      # Legal Form Distribution
      - aktiengesellschaft_count:
          DESCRIPTION: "Number of stock companies (AG)"
          EXPR: COUNT(DISTINCT CASE WHEN companies.legal_form_name = 'Aktiengesellschaft' THEN companies.company_uid END)
          
      - gmbh_count:
          DESCRIPTION: "Number of limited liability companies (GmbH)"
          EXPR: COUNT(DISTINCT CASE WHEN companies.legal_form_name = 'Gesellschaft mit beschränkter Haftung' THEN companies.company_uid END)
          
      - verein_count:
          DESCRIPTION: "Number of associations (Verein)"
          EXPR: COUNT(DISTINCT CASE WHEN companies.legal_form_name = 'Verein' THEN companies.company_uid END)
          
      - stiftung_count:
          DESCRIPTION: "Number of foundations (Stiftung)"
          EXPR: COUNT(DISTINCT CASE WHEN companies.legal_form_name = 'Stiftung' THEN companies.company_uid END)
          
      - genossenschaft_count:
          DESCRIPTION: "Number of cooperatives (Genossenschaft)"
          EXPR: COUNT(DISTINCT CASE WHEN companies.legal_form_name = 'Genossenschaft' THEN companies.company_uid END)
      
      # Business Change Activity
      - total_mutations:
          DESCRIPTION: "Total number of business changes/mutations"
          EXPR: COUNT(mutations.mutation_type_id)
          
      - management_changes:
          DESCRIPTION: "Number of management/administration changes"
          EXPR: COUNT(CASE WHEN mutations.mutation_type_key ILIKE '%verwaltung%' OR mutations.mutation_type_key ILIKE '%administration%' THEN mutations.mutation_type_id END)
          
      - capital_changes:
          DESCRIPTION: "Number of capital-related changes"
          EXPR: COUNT(CASE WHEN publications.activity_type = 'Capital Change' THEN publications.shab_id END)
          
      - address_changes:
          DESCRIPTION: "Number of address changes"
          EXPR: COUNT(CASE WHEN publications.activity_type = 'Address Change' THEN publications.shab_id END)
      
      # Geographic Distribution
      - unique_cantons:
          DESCRIPTION: "Number of different Swiss cantons with registered companies"
          EXPR: COUNT(DISTINCT publications.registry_office_canton)
          
      - companies_per_canton:
          DESCRIPTION: "Average number of companies per canton"
          EXPR: |
            ROUND(
              COUNT(DISTINCT companies.company_uid)::FLOAT 
              / NULLIF(COUNT(DISTINCT publications.registry_office_canton), 0), 
              2
            )
      
      # Time-based Metrics
      - oldest_company_age_days:
          DESCRIPTION: "Age in days of the oldest company in the system"
          EXPR: MAX(companies.days_since_registration)
          
      - companies_registered_this_year:
          DESCRIPTION: "Number of companies registered in the current year"
          EXPR: COUNT(DISTINCT CASE WHEN companies.registration_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN companies.company_uid END)

    COMMENT = 'Semantic view for ZEFIX Swiss company data - enables Cortex Analyst natural language queries about company registrations, business activities, legal forms, and geographic distribution. Auto-deployed via dbt post-hook.'
  {%- endset -%}

  {{ log("Creating ZEFIX Semantic View: zefix_business_intelligence", info=true) }}
  
  {% if execute %}
    {% do run_query(semantic_view_sql) %}
    {{ log("✅ ZEFIX Semantic View created successfully!", info=true) }}
  {% endif %}

{% endmacro %} 