-- Staging model for SEC non-derivative holding data
-- Excludes REMARKS, AFF10B5ONE, and columns ending with _FN as requested

WITH source AS (
    SELECT * 
    FROM {{ source('insider_transactions', 'sec_nonderiv_holding') }}
),

renamed AS (
    SELECT
        ACCESSION_NUMBER,
        NONDERIV_HOLDING_SK,
        SECURITY_TITLE,
        SHRS_OWND_FOLWNG_TRANS as SHARES_OWNED_FOLLOWING_TRANSACTION,
        VALU_OWND_FOLWNG_TRANS as VALUE_OWNED_FOLLOWING_TRANSACTION,
        DIRECT_INDIRECT_OWNERSHIP,
        NATURE_OF_OWNERSHIP
    FROM source
    WHERE 1=1
    -- Exclude columns ending with _FN and _sdc metadata columns
)

SELECT * FROM renamed
