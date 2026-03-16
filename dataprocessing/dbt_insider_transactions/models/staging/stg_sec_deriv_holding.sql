-- Staging model for SEC derivative holding data
-- Excludes REMARKS, AFF10B5ONE, and columns ending with _FN as requested

WITH source AS (
    SELECT * 
    FROM {{ source('insider_transactions', 'sec_deriv_holding') }}
),

renamed AS (
    SELECT
        ACCESSION_NUMBER,
        DERIV_HOLDING_SK,
        SECURITY_TITLE,
        CONV_EXERCISE_PRICE as EXERCISE_PRICE,
        SAFE_CAST(EXERCISE_DATE AS DATE) as EXERCISE_DATE,
        SAFE_CAST(EXPIRATION_DATE AS DATE) as EXPIRATION_DATE,
        UNDLYNG_SEC_TITLE as UNDERLYING_SECURITY_TITLE,
        UNDLYNG_SEC_SHARES as UNDERLYING_SECURITY_SHARES,
        UNDLYNG_SEC_VALUE as UNDERLYING_SECURITY_VALUE,
        SHRS_OWND_FOLWNG_TRANS as SHARES_OWNED_FOLLOWING_TRANSACTION,
        VALU_OWND_FOLWNG_TRANS as VALUE_OWNED_FOLLOWING_TRANSACTION,
        DIRECT_INDIRECT_OWNERSHIP,
        NATURE_OF_OWNERSHIP,
        TRANS_FORM_TYPE as FORM_TYPE
    FROM source
    WHERE 1=1
    -- Exclude columns ending with _FN and _sdc metadata columns
)

SELECT * FROM renamed
