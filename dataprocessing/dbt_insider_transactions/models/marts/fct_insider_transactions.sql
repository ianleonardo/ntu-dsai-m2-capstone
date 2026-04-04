-- Fact model for insider transactions (materialized as a view).
-- Non-derivative transactions only (no derivative SEC tables).
-- ACCESSION_NUMBER is the grain. Share/value aggregates sum A/D across all non-derivative lines;
-- non_deriv_transaction_count / total_transaction_count count only P/S (Purchase/Sale) lines.

WITH submission AS (
    SELECT * FROM {{ ref('dim_sec_submission') }}
),

non_deriv_trans_agg AS (
    SELECT
        ACCESSION_NUMBER,
        COUNTIF(UPPER(TRIM(CAST(TRANSACTION_CODING_CODE AS STRING))) IN ('P', 'S')) AS non_deriv_transaction_count,
        SUM(
            CASE
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'A'
                THEN SAFE_CAST(TRANSACTION_SHARES AS FLOAT64)
                ELSE 0
            END
        ) AS non_deriv_shares_acquired,
        SUM(
            CASE
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'D'
                THEN SAFE_CAST(TRANSACTION_SHARES AS FLOAT64)
                ELSE 0
            END
        ) AS non_deriv_shares_disposed,
        SUM(
            CASE
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'A'
                THEN SAFE_CAST(VALUE_OWNED_FOLLOWING_TRANSACTION AS FLOAT64)
                ELSE 0
            END
        ) AS non_deriv_value_acquired,
        SUM(
            CASE
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'D'
                THEN SAFE_CAST(VALUE_OWNED_FOLLOWING_TRANSACTION AS FLOAT64)
                ELSE 0
            END
        ) AS non_deriv_value_disposed,
        SUM(
            CASE
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'A' THEN COALESCE(
                    SAFE_MULTIPLY(
                        SAFE_CAST(TRANSACTION_SHARES AS FLOAT64),
                        SAFE_CAST(TRANSACTION_PRICE_PER_SHARE AS FLOAT64)
                    ),
                    0
                )
                ELSE 0
            END
        ) AS est_acquire_value,
        SUM(
            CASE
                WHEN TRANSACTION_ACQUIRED_DISPOSED_CODE = 'D' THEN COALESCE(
                    SAFE_MULTIPLY(
                        SAFE_CAST(TRANSACTION_SHARES AS FLOAT64),
                        SAFE_CAST(TRANSACTION_PRICE_PER_SHARE AS FLOAT64)
                    ),
                    0
                )
                ELSE 0
            END
        ) AS est_dispose_value,
        -- Post-transaction shares on each line (SHRS_OWND_FOLWNG_TRANS); MAX ≈ largest reported balance in filing.
        MAX(SAFE_CAST(SHARES_OWNED_FOLLOWING_TRANSACTION AS FLOAT64)) AS total_non_deriv_shares_owned
    FROM {{ ref('fct_sec_nonderiv_line') }}
    GROUP BY ACCESSION_NUMBER
),

-- Line-level date: many filings leave TRANS_DATE empty but set DEEMED_EXECUTION_DATE.
-- Pipeline is non-derivative only (no deriv_trans); fall back to period / filing in fact_table.
trans_dates AS (
    SELECT
        ACCESSION_NUMBER,
        COALESCE(TRANSACTION_DATE, DEEMED_EXECUTION_DATE) AS line_txn_date
    FROM {{ ref('fct_sec_nonderiv_line') }}
    WHERE COALESCE(TRANSACTION_DATE, DEEMED_EXECUTION_DATE) IS NOT NULL
),

trans_date_agg AS (
    SELECT ACCESSION_NUMBER, MIN(line_txn_date) AS trans_date
    FROM trans_dates
    GROUP BY ACCESSION_NUMBER
),

transaction_type_from_code_rows AS (
    SELECT
        ACCESSION_NUMBER,
        {{ transaction_code_type_label('TRANSACTION_CODING_CODE') }} AS transaction_type_from_code
    FROM {{ ref('fct_sec_nonderiv_line') }}
),

transaction_type_from_code_agg AS (
    SELECT
        ACCESSION_NUMBER,
        STRING_AGG(
            DISTINCT transaction_type_from_code,
            ', '
            ORDER BY transaction_type_from_code
        ) AS transaction_type_from_code
    FROM transaction_type_from_code_rows
    WHERE transaction_type_from_code IS NOT NULL
    GROUP BY ACCESSION_NUMBER
),

reporting_owner_enriched AS (
    SELECT
        ACCESSION_NUMBER,
        RPTOWNERCIK,
        RPTOWNERNAME,
        TRIM(RPTOWNER_TITLE) AS owner_title,
        CASE
            -- Title-based detection takes priority for C-suite roles
            WHEN REGEXP_CONTAINS(UPPER(TRIM(RPTOWNER_TITLE)), r'\bCEO\b|CHIEF EXECUTIVE') THEN 'CEO'
            WHEN REGEXP_CONTAINS(UPPER(TRIM(RPTOWNER_TITLE)), r'\bCFO\b|CHIEF FINANCIAL') THEN 'CFO'
            WHEN REGEXP_CONTAINS(UPPER(TRIM(RPTOWNER_TITLE)), r'\bCOB\b|CHAIRMAN OF THE BOARD|CHAIR OF THE BOARD') THEN 'COB (Chairman)'
            WHEN REGEXP_CONTAINS(UPPER(TRIM(RPTOWNER_TITLE)), r'\bCXO\b|CHIEF \w+ OFFICER') THEN 'Officer (C-Suite)'
            -- Relationship-based fallback
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%'
                AND LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Director & Officer'
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%director%' THEN 'Director'
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Officer'
            WHEN LOWER(RPTOWNER_RELATIONSHIP) LIKE '%10%%' THEN '10% Owner'
            ELSE 'Other'
        END AS role_type
    FROM {{ ref('dim_sec_reporting_owner') }}
),

reporting_owners AS (
    SELECT
        ACCESSION_NUMBER,
        COUNT(DISTINCT RPTOWNERCIK) AS reporting_owner_count,
        STRING_AGG(DISTINCT RPTOWNERNAME, ' | ' ORDER BY RPTOWNERNAME) AS reporting_owner_names,
        STRING_AGG(DISTINCT role_type, ', ' ORDER BY role_type) AS reporting_owner_role_types,
        STRING_AGG(
            DISTINCT NULLIF(owner_title, ''),
            ' | '
            ORDER BY NULLIF(owner_title, '')
        ) AS reporting_owner_titles
    FROM reporting_owner_enriched
    GROUP BY ACCESSION_NUMBER
),

fact_table AS (
    SELECT
        s.ACCESSION_NUMBER,

        CAST(FORMAT_DATE('%Y%m%d', s.FILING_DATE) AS INT64) AS filing_date_key,
        CAST(FORMAT_DATE('%Y%m%d', s.PERIOD_OF_REPORT) AS INT64) AS period_report_date_key,

        s.FILING_DATE,
        s.PERIOD_OF_REPORT,
        COALESCE(td.trans_date, s.PERIOD_OF_REPORT, s.FILING_DATE) AS TRANS_DATE,
        s.DATE_OF_ORIGINAL_SUBMISSION,
        s.NO_SECURITIES_OWNED,
        s.ISSUERCIK,
        s.ISSUERNAME,
        s.ISSUERTRADINGSYMBOL,

        COALESCE(nt.non_deriv_transaction_count, 0) AS non_deriv_transaction_count,
        COALESCE(nt.non_deriv_transaction_count, 0) AS total_transaction_count,

        COALESCE(nt.non_deriv_shares_acquired, 0) AS non_deriv_shares_acquired,
        COALESCE(nt.non_deriv_shares_disposed, 0) AS non_deriv_shares_disposed,
        COALESCE(nt.non_deriv_shares_acquired, 0) AS total_shares_acquired,
        COALESCE(nt.non_deriv_shares_disposed, 0) AS total_shares_disposed,

        COALESCE(nt.non_deriv_value_acquired, 0) AS non_deriv_value_acquired,
        COALESCE(nt.non_deriv_value_disposed, 0) AS non_deriv_value_disposed,
        COALESCE(nt.non_deriv_value_acquired, 0) + COALESCE(nt.non_deriv_value_disposed, 0) AS total_non_deriv_value,

        COALESCE(nt.est_acquire_value, 0) AS est_acquire_value,
        COALESCE(nt.est_dispose_value, 0) AS est_dispose_value,

        COALESCE(nt.total_non_deriv_shares_owned, 0) AS total_non_deriv_shares_owned,

        COALESCE(ro.reporting_owner_count, 0) AS reporting_owner_count,
        ro.reporting_owner_names,
        ro.reporting_owner_role_types,
        ro.reporting_owner_titles,
        tt.transaction_type_from_code

    FROM submission s
    LEFT JOIN trans_date_agg td ON s.ACCESSION_NUMBER = td.ACCESSION_NUMBER
    LEFT JOIN transaction_type_from_code_agg tt ON s.ACCESSION_NUMBER = tt.ACCESSION_NUMBER
    LEFT JOIN non_deriv_trans_agg nt ON s.ACCESSION_NUMBER = nt.ACCESSION_NUMBER
    LEFT JOIN reporting_owners ro ON s.ACCESSION_NUMBER = ro.ACCESSION_NUMBER
)

SELECT * FROM fact_table
