-- Staging model for SEC submission data
-- Excludes REMARKS, AFF10B5ONE, and columns ending with _FN as requested
--
-- Date columns: BigQuery may store SEC TSV dates as STRING, INT64, or FLOAT64.
-- See macro `parse_sec_date` for normalization (PARSE_DATE requires STRING).

WITH source AS (
    SELECT *
    FROM {{ source('insider_transactions', 'sec_submission') }}
),

-- Raw BigQuery tables may contain duplicate ACCESSION_NUMBER (e.g. re-running ingestion
-- appends the same filings). Keep one row per filing; prefer the latest load `year` when present.
deduped AS (
    SELECT * EXCEPT (rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ACCESSION_NUMBER
                ORDER BY COALESCE(SAFE_CAST(year AS INT64), -1) DESC
            ) AS rn
        FROM source
    )
    WHERE rn = 1
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
    FROM deduped
    WHERE 1=1
    -- Exclude REMARKS, AFF10B5ONE, and columns ending with _FN
    -- Also exclude _sdc metadata columns
)

SELECT * FROM renamed
