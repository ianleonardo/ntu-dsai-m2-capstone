-- Staging model for SEC submission data
-- Excludes REMARKS, AFF10B5ONE, and columns ending with _FN as requested
--
-- Date columns: BigQuery may store SEC TSV dates as STRING, INT64, or FLOAT64.
-- See macro `parse_sec_date` for normalization (PARSE_DATE requires STRING).

WITH source AS (
    SELECT *
    FROM {{ source('insider_transactions', 'sec_submission') }}
),

renamed AS (
    SELECT
        ACCESSION_NUMBER,
        {{ parse_sec_date('FILING_DATE') }} AS FILING_DATE,
        {{ parse_sec_date('PERIOD_OF_REPORT') }} AS PERIOD_OF_REPORT,
        {{ parse_sec_date('DATE_OF_ORIG_SUB') }} AS DATE_OF_ORIGINAL_SUBMISSION,
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
