-- filtered dimension mapping to only insiders who transact in S&P 500 companies.
-- grain: accession_number × rptownercik (same as dim_sec_reporting_owner, but heavily subsetted)

WITH sp500_accessions AS (
    SELECT DISTINCT ACCESSION_NUMBER
    FROM {{ ref('sp500_insider_transactions') }}
    WHERE ACCESSION_NUMBER IS NOT NULL
)

SELECT d.*
FROM {{ ref('dim_sec_reporting_owner') }} AS d
INNER JOIN sp500_accessions AS s
    ON d.ACCESSION_NUMBER = s.ACCESSION_NUMBER
