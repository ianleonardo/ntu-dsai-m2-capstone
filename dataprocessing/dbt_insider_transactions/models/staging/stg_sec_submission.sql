-- Staging model for SEC submission data
-- Excludes REMARKS, AFF10B5ONE, and columns ending with _FN as requested

WITH source AS (
    SELECT * 
    FROM {{ source('insider_transactions', 'sec_submission') }}
),

renamed AS (
    SELECT
        ACCESSION_NUMBER,
        SAFE_CAST(FILING_DATE AS DATE) as FILING_DATE,
        SAFE_CAST(PERIOD_OF_REPORT AS DATE) as PERIOD_OF_REPORT,
        SAFE_CAST(DATE_OF_ORIG_SUB AS DATE) as DATE_OF_ORIGINAL_SUBMISSION,
        NO_SECURITIES_OWNED,
        NOT_SUBJECT_SEC16,
        FORM3_HOLDINGS_REPORTED,
        FORM4_TRANS_REPORTED,
        DOCUMENT_TYPE,
        ISSUERCIK,
        ISSUERNAME,
        ISSUERTRADINGSYMBOL
    FROM source
    WHERE 1=1
    -- Exclude REMARKS, AFF10B5ONE, and columns ending with _FN
    -- Also exclude _sdc metadata columns
)

SELECT * FROM renamed
