{#-
  Normalize SEC date columns from BigQuery `sec_submission`.

  SEC SUBMISSION.tsv commonly uses DD-MON-YYYY (e.g. 31-MAR-2023). Loads may also
  store YYYYMMDD strings, INT64/FLOAT64, or strings with trailing ".0". PARSE_DATE
  requires STRING for text formats; numeric types need FORMAT before parse.

  Do not cast bare numeric values to TIMESTAMP (BQ may treat them as epoch seconds).
-#}
{% macro parse_sec_date(column) -%}
(
  {%- set c = column -%}
  COALESCE(
    -- Column already DATE in BigQuery
    SAFE_CAST({{ c }} AS DATE),
    -- ISO / flexible string cast
    SAFE_CAST(
      NULLIF(
        TRIM(REGEXP_REPLACE(CAST({{ c }} AS STRING), r'\.0+$', '')),
        ''
      ) AS DATE
    ),
    SAFE.PARSE_DATE(
      '%Y-%m-%d',
      NULLIF(TRIM(REGEXP_REPLACE(CAST({{ c }} AS STRING), r'\.0+$', '')), '')
    ),
    -- SEC Form DRS / SUBMISSION.tsv uses DD-MON-YYYY (e.g. 31-MAR-2023)
    SAFE.PARSE_DATE(
      '%d-%b-%Y',
      NULLIF(TRIM(REGEXP_REPLACE(CAST({{ c }} AS STRING), r'\.0+$', '')), '')
    ),
    SAFE.PARSE_DATE(
      '%e-%b-%Y',
      NULLIF(TRIM(REGEXP_REPLACE(CAST({{ c }} AS STRING), r'\.0+$', '')), '')
    ),
    SAFE.PARSE_DATE(
      '%Y%m%d',
      NULLIF(TRIM(REGEXP_REPLACE(CAST({{ c }} AS STRING), r'\.0+$', '')), '')
    ),
    SAFE.PARSE_DATE(
      '%m/%d/%Y',
      NULLIF(TRIM(REGEXP_REPLACE(CAST({{ c }} AS STRING), r'\.0+$', '')), '')
    ),
    SAFE.PARSE_DATE(
      '%d/%m/%Y',
      NULLIF(TRIM(REGEXP_REPLACE(CAST({{ c }} AS STRING), r'\.0+$', '')), '')
    ),
    -- Pure 8-digit YYYYMMDD string
    IF(
      REGEXP_CONTAINS(TRIM(CAST({{ c }} AS STRING)), r'^[0-9]{8}$'),
      SAFE.PARSE_DATE('%Y%m%d', TRIM(CAST({{ c }} AS STRING))),
      CAST(NULL AS DATE)
    ),
    -- Numeric YYYYMMDD (INT64 / FLOAT with no fractional part; MOD() does not accept FLOAT64 in BQ)
    IF(
      SAFE_CAST({{ c }} AS FLOAT64) IS NOT NULL
      AND ABS(SAFE_CAST({{ c }} AS FLOAT64)) BETWEEN 19000101 AND 21001231
      AND CAST(ABS(SAFE_CAST({{ c }} AS FLOAT64)) AS INT64) = ABS(SAFE_CAST({{ c }} AS FLOAT64)),
      SAFE.PARSE_DATE(
        '%Y%m%d',
        FORMAT(
          '%08d',
          CAST(ABS(SAFE_CAST({{ c }} AS FLOAT64)) AS INT64)
        )
      ),
      CAST(NULL AS DATE)
    ),
    -- Last resort: digits only (handles hidden chars); guard year to avoid MDY false positives
    IF(
      LENGTH(
        REGEXP_EXTRACT(
          REGEXP_REPLACE(TRIM(CAST({{ c }} AS STRING)), r'[^0-9]', ''),
          r'^([0-9]{8})'
        )
      ) = 8
      AND SAFE_CAST(
        SUBSTR(
          REGEXP_EXTRACT(
            REGEXP_REPLACE(TRIM(CAST({{ c }} AS STRING)), r'[^0-9]', ''),
            r'^([0-9]{8})'
          ),
          1,
          4
        ) AS INT64
      ) BETWEEN 1990 AND 2035,
      SAFE.PARSE_DATE(
        '%Y%m%d',
        REGEXP_EXTRACT(
          REGEXP_REPLACE(TRIM(CAST({{ c }} AS STRING)), r'[^0-9]', ''),
          r'^([0-9]{8})'
        )
      ),
      CAST(NULL AS DATE)
    )
  )
)
{%- endmacro %}
