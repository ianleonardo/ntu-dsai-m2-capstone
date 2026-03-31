-- Reporting-owner bridge: one row per raw SEC reporting-owner row (accession × owner line).
-- Keeps SEC column names (RPTOWNER*) for downstream SQL and the FastAPI cluster breakdown.

WITH source AS (
    SELECT *
    FROM {{ source('insider_transactions', 'sec_reportingowner') }}
),

base AS (
    SELECT
        ACCESSION_NUMBER,
        RPTOWNERCIK,
        RPTOWNERNAME,
        -- Raw loads may leave relationship NULL/blank (e.g. Form 4 XML with no flags set, legacy rows).
        COALESCE(
            NULLIF(TRIM(RPTOWNER_RELATIONSHIP), ''),
            'Unknown'
        ) AS RPTOWNER_RELATIONSHIP,
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
    WHERE 1 = 1
)

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
    FILE_NUMBER,
    CASE
        WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%' AND LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Director & Officer'
        WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%' THEN 'Director'
        WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Officer'
        WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%10%%' THEN '10% Owner'
        ELSE 'Other'
    END AS role_type,
    CASE
        WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%' OR LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' OR LOWER(RPTOWNER_RELATIONSHIP) LIKE '%10%%'
            THEN 1
        ELSE 0
    END AS is_insider
FROM base
