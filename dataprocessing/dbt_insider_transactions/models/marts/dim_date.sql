-- Dimension model for dates used by insider transactions fact table.
-- Provides date_key and components derived from SEC submission dates.

WITH dates AS (
    SELECT DISTINCT CAST(FILING_DATE AS DATE) AS date_field
    FROM {{ ref('stg_sec_submission') }}
    WHERE FILING_DATE IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT CAST(PERIOD_OF_REPORT AS DATE) AS date_field
    FROM {{ ref('stg_sec_submission') }}
    WHERE PERIOD_OF_REPORT IS NOT NULL
),

enhanced AS (
    SELECT
        -- yyyymmdd integer key
        CAST(FORMAT_DATE('%Y%m%d', date_field) AS INT64) AS date_key,
        date_field,
        EXTRACT(YEAR FROM date_field) AS year,
        EXTRACT(QUARTER FROM date_field) AS quarter,
        EXTRACT(MONTH FROM date_field) AS month
    FROM dates
)

SELECT * FROM enhanced
