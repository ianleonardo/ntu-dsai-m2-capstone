-- Filing-level dimension: one row per ACCESSION_NUMBER after deduping raw SEC submission loads.

WITH source AS (
    SELECT *
    FROM {{ source('insider_transactions', 'sec_submission') }}
),

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
    WHERE 1 = 1
)

SELECT * FROM renamed
