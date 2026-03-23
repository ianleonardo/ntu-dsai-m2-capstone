{{ config(
    materialized='table',
    partition_by={
        "field": "TRANS_DATE",
        "data_type": "date",
        "granularity": "year",
    },
    cluster_by=["TRANS_DATE", "symbol_norm"],
) }}


WITH f AS (
    SELECT * FROM {{ ref('fct_insider_transactions') }}
),

s AS (
    SELECT * FROM {{ ref('dim_sp500_company') }}
)

SELECT
    f.*,
    s.symbol_norm AS symbol_norm,
    TRIM(s.gics_sector) AS issuer_gics_sector
FROM f
INNER JOIN s
    ON UPPER(TRIM(COALESCE(f.ISSUERTRADINGSYMBOL, ''))) = s.symbol_norm
    OR (
        s.cik_int IS NOT NULL
        AND SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(f.ISSUERCIK AS STRING)), r'[^0-9]', '') AS INT64) IS NOT NULL
        AND s.cik_int = SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(f.ISSUERCIK AS STRING)), r'[^0-9]', '') AS INT64)
    )
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY f.ACCESSION_NUMBER
    ORDER BY
        CASE WHEN UPPER(TRIM(COALESCE(f.ISSUERTRADINGSYMBOL, ''))) = s.symbol_norm THEN 0 ELSE 1 END,
        s.symbol_norm
) = 1
