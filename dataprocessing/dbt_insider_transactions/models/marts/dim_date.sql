-- Date dimension for temporal analysis of insider transactions
-- Generates comprehensive date attributes for analytics

WITH dates AS (
    SELECT DISTINCT 
        TRANSACTION_DATE as date_field
    FROM {{ ref('stg_sec_nonderiv_trans') }}
    WHERE TRANSACTION_DATE IS NOT NULL
    
    UNION ALL
    
    SELECT DISTINCT 
        TRANSACTION_DATE as date_field
    FROM {{ ref('stg_sec_deriv_trans') }}
    WHERE TRANSACTION_DATE IS NOT NULL
    
    UNION ALL
    
    SELECT DISTINCT 
        FILING_DATE as date_field
    FROM {{ ref('stg_sec_submission') }}
    WHERE FILING_DATE IS NOT NULL
    
    UNION ALL
    
    SELECT DISTINCT 
        PERIOD_OF_REPORT as date_field
    FROM {{ ref('stg_sec_submission') }}
    WHERE PERIOD_OF_REPORT IS NOT NULL
),

-- Add comprehensive date attributes
enhanced AS (
    SELECT 
        date_field,
        EXTRACT(YEAR FROM date_field) as year,
        EXTRACT(QUARTER FROM date_field) as quarter,
        EXTRACT(MONTH FROM date_field) as month,
        EXTRACT(DAY FROM date_field) as day,
        EXTRACT(DAYOFWEEK FROM date_field) as day_of_week,
        EXTRACT(DAYOFYEAR FROM date_field) as day_of_year,
        EXTRACT(WEEK FROM date_field) as week_of_year,
        -- Date formatting
        FORMAT_DATE('%Y-%m-%d', date_field) as date_key,
        FORMAT_DATE('%B', date_field) as month_name,
        FORMAT_DATE('%A', date_field) as day_name,
        -- Flags
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_field) IN (1, 7) THEN 1 
            ELSE 0 
        END as is_weekend,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_field) BETWEEN 2 AND 6 THEN 1 
            ELSE 0 
        END as is_weekday,
        -- Quarter formatting
        CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_field) AS STRING), ' ', CAST(EXTRACT(YEAR FROM date_field) AS STRING)) as quarter_name
    FROM dates
    WHERE date_field IS NOT NULL
)

SELECT * FROM enhanced
