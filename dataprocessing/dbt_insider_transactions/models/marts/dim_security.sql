-- Dimension model for securities/instruments
-- Consolidates security information from both derivative and non-derivative data

WITH non_deriv_securities AS (
    SELECT DISTINCT 
        SECURITY_TITLE,
        'Non-Derivative' as security_type,
        CAST(NULL AS STRING) as EXERCISE_PRICE,
        CAST(NULL AS DATE) as EXPIRATION_DATE,
        CAST(NULL AS STRING) as UNDERLYING_SECURITY_TITLE,
        CAST(NULL AS STRING) as UNDERLYING_SECURITY_SHARES,
        CAST(NULL AS STRING) as UNDERLYING_SECURITY_VALUE
    FROM {{ ref('stg_sec_nonderiv_trans') }}
    WHERE SECURITY_TITLE IS NOT NULL
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        SECURITY_TITLE,
        'Non-Derivative' as security_type,
        CAST(NULL AS STRING) as EXERCISE_PRICE,
        CAST(NULL AS DATE) as EXPIRATION_DATE,
        CAST(NULL AS STRING) as UNDERLYING_SECURITY_TITLE,
        CAST(NULL AS STRING) as UNDERLYING_SECURITY_SHARES,
        CAST(NULL AS STRING) as UNDERLYING_SECURITY_VALUE
    FROM {{ ref('stg_sec_nonderiv_holding') }}
    WHERE SECURITY_TITLE IS NOT NULL
),

deriv_securities AS (
    SELECT DISTINCT 
        SECURITY_TITLE,
        'Derivative' as security_type,
        CAST(EXERCISE_PRICE AS STRING) as EXERCISE_PRICE,
        EXPIRATION_DATE,
        UNDERLYING_SECURITY_TITLE,
        CAST(UNDERLYING_SECURITY_SHARES AS STRING) as UNDERLYING_SECURITY_SHARES,
        CAST(UNDERLYING_SECURITY_VALUE AS STRING) as UNDERLYING_SECURITY_VALUE
    FROM {{ ref('stg_sec_deriv_trans') }}
    WHERE SECURITY_TITLE IS NOT NULL
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        SECURITY_TITLE,
        'Derivative' as security_type,
        CAST(EXERCISE_PRICE AS STRING) as EXERCISE_PRICE,
        EXPIRATION_DATE,
        UNDERLYING_SECURITY_TITLE,
        CAST(UNDERLYING_SECURITY_SHARES AS STRING) as UNDERLYING_SECURITY_SHARES,
        CAST(UNDERLYING_SECURITY_VALUE AS STRING) as UNDERLYING_SECURITY_VALUE
    FROM {{ ref('stg_sec_deriv_holding') }}
    WHERE SECURITY_TITLE IS NOT NULL
),

all_securities AS (
    SELECT * FROM non_deriv_securities
    UNION ALL
    SELECT * FROM deriv_securities
),

-- Add surrogate key and clean up
enhanced AS (
    SELECT 
        SECURITY_TITLE,
        security_type,
        EXERCISE_PRICE,
        EXPIRATION_DATE,
        UNDERLYING_SECURITY_TITLE,
        UNDERLYING_SECURITY_SHARES,
        UNDERLYING_SECURITY_VALUE,
        -- Generate a consistent hash key for security
        {{ dbt_utils.generate_surrogate_key(['SECURITY_TITLE', 'security_type']) }} as security_key
    FROM all_securities
    WHERE SECURITY_TITLE IS NOT NULL
)

SELECT * FROM enhanced
