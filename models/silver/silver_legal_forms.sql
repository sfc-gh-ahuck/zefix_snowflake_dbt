{{
  config(
    materialized='table'
  )
}}

-- Legal forms reference data as constants
-- Replaces the legal_forms seed with a model containing Swiss legal form mappings
SELECT 
    legal_form_id,
    legal_form_name_de,
    legal_form_name_en,
    abbreviation,
    description
FROM VALUES
    (1, 'Einzelunternehmen', 'Sole Proprietorship', NULL, 'Individual business owned and operated by one person'),
    (2, 'Kollektivgesellschaft', 'General Partnership', NULL, 'Partnership where all partners have unlimited liability'),
    (3, 'Aktiengesellschaft', 'Stock Company', 'AG', 'Corporation with share capital divided into shares'),
    (4, 'Kommanditgesellschaft', 'Limited Partnership', NULL, 'Partnership with both general and limited partners'),
    (5, 'Gesellschaft mit beschränkter Haftung', 'Limited Liability Company', 'GmbH', 'Company with limited liability for its members'),
    (6, 'Genossenschaft', 'Cooperative', NULL, 'Association of persons or entities for mutual benefit'),
    (7, 'Verein', 'Association', NULL, 'Non-profit organization for specific purposes'),
    (8, 'Stiftung', 'Foundation', NULL, 'Legal entity established for charitable or public purposes')
AS legal_forms_data(legal_form_id, legal_form_name_de, legal_form_name_en, abbreviation, description) 