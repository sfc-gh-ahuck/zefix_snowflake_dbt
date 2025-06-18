{{
  config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
  )
}}

-- Bronze layer: Extract JSON fields from raw variant data
SELECT 
    -- Metadata fields
    CURRENT_TIMESTAMP() AS _loaded_at,
    MD5(content::string) AS _content_hash,
    
    -- Core company identifiers
    content:uid::string AS uid,
    content:uidFormatted::string AS uid_formatted,
    content:chid::string AS chid,
    content:chidFormatted::string AS chid_formatted,
    content:ehraid::number AS ehraid,
    
    -- Basic company information
    content:name::string AS company_name,
    content:legalFormId::number AS legal_form_id,
    content:legalSeat::string AS legal_seat,
    content:legalSeatId::number AS legal_seat_id,
    content:registerOfficeId::number AS register_office_id,
    content:status::string AS company_status,
    content:purpose::string AS company_purpose,
    
    -- Address information (nested object)
    content:address AS address_json,
    content:address:organisation::string AS address_organisation,
    content:address:careOf::string AS address_care_of,
    content:address:street::string AS address_street,
    content:address:houseNumber::string AS address_house_number,
    content:address:poBox::string AS address_po_box,
    content:address:swissZipCode::string AS address_zip_code,
    content:address:town::string AS address_town,
    content:address:country::string AS address_country,
    content:address:addon::string AS address_addon,
    
    -- SHAB (Swiss Official Gazette of Commerce) information
    content:shabDate::string AS shab_date,
    content:shabPub AS shab_publications_json,
    
    -- Additional fields
    content:cantonalExcerptWeb::string AS cantonal_excerpt_web_url,
    content:deleteDate::string AS delete_date,
    content:rabId::string AS rab_id,
    content:translation AS translation_json,
    
    -- Related entities (arrays/objects)
    content:oldNames AS old_names_json,
    content:auditFirms AS audit_firms_json,
    content:auditFirmFor AS audit_firm_for_json,
    content:branchOffices AS branch_offices_json,
    content:mainOffices AS main_offices_json,
    content:furtherMainOffices AS further_main_offices_json,
    content:hasTakenOver AS has_taken_over_json,
    content:wasTakenOverBy AS was_taken_over_by_json,
    
    -- Full content for reference
    content AS full_content_json

FROM {{ source('zefix_raw', 'raw') }}
WHERE content IS NOT NULL 