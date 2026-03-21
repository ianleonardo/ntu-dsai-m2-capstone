-- Fact table for insider transactions
-- Central fact table with ACCESSION_NUMBER as primary key
-- Links to all dimension tables following star schema principles

{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'FILING_DATE',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by = ['ISSUERTRADINGSYMBOL', 'ISSUERCIK']
  )
}}

WITH submission AS (
    SELECT * FROM {{ ref('stg_sec_submission') }}
),

-- Aggregate transaction data for each submission
non_deriv_trans_agg AS (
    SELECT 
        ACCESSION_NUMBER,
        COUNT(*) as non_deriv_transaction_count,
        SUM(
            CASE 
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'A' 
                THEN SAFE_CAST(TRANSACTION_SHARES AS FLOAT64)
                ELSE 0 
            END
        ) as non_deriv_shares_acquired,
        SUM(
            CASE 
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'D' 
                THEN SAFE_CAST(TRANSACTION_SHARES AS FLOAT64)
                ELSE 0 
            END
        ) as non_deriv_shares_disposed,
        SUM(
            CASE 
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'A' 
                THEN SAFE_CAST(VALUE_OWNED_FOLLOWING_TRANSACTION AS FLOAT64)
                ELSE 0 
            END
        ) as non_deriv_value_acquired,
        SUM(
            CASE 
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'D' 
                THEN SAFE_CAST(VALUE_OWNED_FOLLOWING_TRANSACTION AS FLOAT64)
                ELSE 0 
            END
        ) as non_deriv_value_disposed
    FROM {{ ref('stg_sec_nonderiv_trans') }}
    GROUP BY ACCESSION_NUMBER
),

deriv_trans_agg AS (
    SELECT 
        ACCESSION_NUMBER,
        COUNT(*) as deriv_transaction_count,
        SUM(
            CASE 
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'A' 
                THEN SAFE_CAST(TRANSACTION_SHARES AS FLOAT64)
                ELSE 0 
            END
        ) as deriv_shares_acquired,
        SUM(
            CASE 
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'D' 
                THEN SAFE_CAST(TRANSACTION_SHARES AS FLOAT64)
                ELSE 0 
            END
        ) as deriv_shares_disposed
    FROM {{ ref('stg_sec_deriv_trans') }}
    GROUP BY ACCESSION_NUMBER
),

-- Aggregate holding data
non_deriv_holdings_agg AS (
    SELECT 
        ACCESSION_NUMBER,
        COUNT(*) as non_deriv_holding_count,
        SUM(SAFE_CAST(SHARES_OWNED_FOLLOWING_TRANSACTION AS FLOAT64)) as total_non_deriv_shares_owned
    FROM {{ ref('stg_sec_nonderiv_holding') }}
    GROUP BY ACCESSION_NUMBER
),

deriv_holdings_agg AS (
    SELECT 
        ACCESSION_NUMBER,
        COUNT(*) as deriv_holding_count
    FROM {{ ref('stg_sec_deriv_holding') }}
    GROUP BY ACCESSION_NUMBER
),

-- Get reporting owner information
reporting_owners AS (
    SELECT 
        ACCESSION_NUMBER,
        COUNT(DISTINCT RPTOWNERCIK) as reporting_owner_count,
        STRING_AGG(DISTINCT RPTOWNERCIK, ', ') as reporting_owner_ciks
    FROM {{ ref('stg_sec_reportingowner') }}
    GROUP BY ACCESSION_NUMBER
),

-- Combine all data into fact table
fact_table AS (
    SELECT 
        -- Primary key
        s.ACCESSION_NUMBER,
        
        -- Date dimensions
        d.date_key as filing_date_key,
        d_period.date_key as period_report_date_key,
        
        -- Submission attributes
        s.FILING_DATE,
        s.PERIOD_OF_REPORT,
        s.DATE_OF_ORIGINAL_SUBMISSION,
        s.NO_SECURITIES_OWNED,
        s.ISSUERCIK,
        s.ISSUERNAME,
        s.ISSUERTRADINGSYMBOL,
        
        -- Transaction aggregates
        COALESCE(nt.non_deriv_transaction_count, 0) as non_deriv_transaction_count,
        COALESCE(dt.deriv_transaction_count, 0) as deriv_transaction_count,
        COALESCE(nt.non_deriv_transaction_count, 0) + COALESCE(dt.deriv_transaction_count, 0) as total_transaction_count,
        
        -- Share aggregates
        COALESCE(nt.non_deriv_shares_acquired, 0) as non_deriv_shares_acquired,
        COALESCE(nt.non_deriv_shares_disposed, 0) as non_deriv_shares_disposed,
        COALESCE(dt.deriv_shares_acquired, 0) as deriv_shares_acquired,
        COALESCE(dt.deriv_shares_disposed, 0) as deriv_shares_disposed,
        COALESCE(nt.non_deriv_shares_acquired, 0) + COALESCE(dt.deriv_shares_acquired, 0) as total_shares_acquired,
        COALESCE(nt.non_deriv_shares_disposed, 0) + COALESCE(dt.deriv_shares_disposed, 0) as total_shares_disposed,
        
        -- Value aggregates
        COALESCE(nt.non_deriv_value_acquired, 0) as non_deriv_value_acquired,
        COALESCE(nt.non_deriv_value_disposed, 0) as non_deriv_value_disposed,
        COALESCE(nt.non_deriv_value_acquired, 0) + COALESCE(nt.non_deriv_value_disposed, 0) as total_non_deriv_value,
        
        -- Holding aggregates
        COALESCE(nh.non_deriv_holding_count, 0) as non_deriv_holding_count,
        COALESCE(dh.deriv_holding_count, 0) as deriv_holding_count,
        COALESCE(nh.total_non_deriv_shares_owned, 0) as total_non_deriv_shares_owned,
        
        -- Reporting owner aggregates
        COALESCE(ro.reporting_owner_count, 0) as reporting_owner_count,
        ro.reporting_owner_ciks
        
    FROM submission s
    LEFT JOIN {{ ref('dim_date') }} d ON d.date_field = s.FILING_DATE
    LEFT JOIN {{ ref('dim_date') }} d_period ON d_period.date_field = s.PERIOD_OF_REPORT
    LEFT JOIN non_deriv_trans_agg nt ON s.ACCESSION_NUMBER = nt.ACCESSION_NUMBER
    LEFT JOIN deriv_trans_agg dt ON s.ACCESSION_NUMBER = dt.ACCESSION_NUMBER
    LEFT JOIN non_deriv_holdings_agg nh ON s.ACCESSION_NUMBER = nh.ACCESSION_NUMBER
    LEFT JOIN deriv_holdings_agg dh ON s.ACCESSION_NUMBER = dh.ACCESSION_NUMBER
    LEFT JOIN reporting_owners ro ON s.ACCESSION_NUMBER = ro.ACCESSION_NUMBER
)

SELECT * FROM fact_table
