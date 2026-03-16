-- Dimension model for reporting owners
-- Consolidates reporting owner information from SEC data

WITH reporting_owners AS (
    SELECT * FROM {{ ref('stg_sec_reportingowner') }}
),

-- Add derived columns for better analytics
enhanced AS (
    SELECT 
        RPTOWNERCIK as reporting_owner_cik,
        RPTOWNERNAME as reporting_owner_name,
        RPTOWNER_RELATIONSHIP,
        RPTOWNER_TITLE,
        RPTOWNER_TXT,
        RPTOWNER_STREET1,
        RPTOWNER_STREET2,
        RPTOWNER_CITY,
        RPTOWNER_STATE,
        RPTOWNER_ZIPCODE,
        RPTOWNER_STATE_DESC,
        FILE_NUMBER,
        -- Create role type from relationship field
        CASE 
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%' AND LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Director & Officer'
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%' THEN 'Director'
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Officer'
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%10%%' THEN '10% Owner'
            ELSE 'Other'
        END as role_type,
        -- Create a flag for any insider role
        CASE 
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%' OR LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' OR LOWER(RPTOWNER_RELATIONSHIP) LIKE '%10%%' 
            THEN 1 ELSE 0 
        END as is_insider
    FROM reporting_owners
)

SELECT * FROM enhanced
