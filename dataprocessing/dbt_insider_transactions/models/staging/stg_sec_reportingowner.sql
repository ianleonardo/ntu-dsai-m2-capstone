-- Staging model for SEC reporting owner data
-- Excludes REMARKS, AFF10B5ONE, and columns ending with _FN as requested

WITH source AS (
    SELECT * 
    FROM {{ source('insider_transactions', 'sec_reportingowner') }}
),

renamed AS (
    SELECT
        ACCESSION_NUMBER,
        RPTOWNERCIK,
        RPTOWNERNAME,
        RPTOWNER_RELATIONSHIP,
        RPTOWNER_TITLE,
        RPTOWNER_TXT,
        RPTOWNER_STREET1,
        RPTOWNER_STREET2,
        RPTOWNER_CITY,
        RPTOWNER_STATE,
        RPTOWNER_ZIPCODE,
        RPTOWNER_STATE_DESC,
        FILE_NUMBER
    FROM source
    WHERE 1=1
    -- Exclude columns ending with _FN and _sdc metadata columns
)

SELECT * FROM renamed
