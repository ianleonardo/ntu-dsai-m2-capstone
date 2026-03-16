-- Dimension model for transaction types
-- Provides reference data for different transaction coding

WITH transaction_codes AS (
    SELECT DISTINCT 
        TRANSACTION_CODING_CODE,
        TRANSACTION_ACQUIRED_DISPOSED_CODE,
        FORM_TYPE
    FROM {{ ref('stg_sec_nonderiv_trans') }}
    WHERE TRANSACTION_CODING_CODE IS NOT NULL
    
    UNION ALL
    
    SELECT DISTINCT 
        TRANSACTION_CODING_CODE,
        TRANSACTION_ACQUIRED_DISPOSED_CODE,
        FORM_TYPE
    FROM {{ ref('stg_sec_deriv_trans') }}
    WHERE TRANSACTION_CODING_CODE IS NOT NULL
),

-- Add descriptive attributes
enhanced AS (
    SELECT 
        TRANSACTION_CODING_CODE,
        TRANSACTION_ACQUIRED_DISPOSED_CODE,
        FORM_TYPE,
        -- Add descriptive labels based on common SEC codes
        CASE 
            WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'A' THEN 'Acquired'
            WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'D' THEN 'Disposed'
            ELSE 'Unknown'
        END as transaction_direction,
        -- Combine codes for unique transaction type
        CONCAT(TRANSACTION_CODING_CODE, '_', TRANSACTION_ACQUIRED_DISPOSED_CODE) as transaction_type_key,
        -- Form type description
        CASE 
            WHEN FORM_TYPE = '3' THEN 'Initial Statement'
            WHEN FORM_TYPE = '4' THEN 'Changes in Ownership'
            WHEN FORM_TYPE = '5' then 'Annual Statement'
            ELSE 'Other'
        END as form_type_description
    FROM transaction_codes
    WHERE TRANSACTION_CODING_CODE IS NOT NULL
)

SELECT * FROM enhanced
