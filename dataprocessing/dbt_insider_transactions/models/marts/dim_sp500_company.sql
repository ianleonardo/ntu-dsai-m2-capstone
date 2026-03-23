-- S&P 500 constituent dimension (grain: symbol_norm).

WITH raw AS (
    SELECT * FROM {{ source('insider_transactions', 'sp500_companies') }}
),

parsed AS (
    SELECT
        UPPER(TRIM(symbol)) AS symbol_norm,
        SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(cik AS STRING)), r'[^0-9]', '') AS INT64) AS cik_int,
        TRIM(security) AS sp500_security_name,
        TRIM(gics_sector) AS gics_sector,
        TRIM(gics_sub_industry) AS gics_sub_industry
    FROM raw
    WHERE TRIM(COALESCE(symbol, '')) != ''
)

SELECT DISTINCT
    symbol_norm,
    cik_int,
    sp500_security_name,
    gics_sector,
    gics_sub_industry
FROM parsed
WHERE symbol_norm != ''
