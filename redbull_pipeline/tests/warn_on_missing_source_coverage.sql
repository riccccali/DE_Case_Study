{{ config(severity='warn') }}

WITH expected_coverage AS (
    SELECT 'outlet' AS source_name, 'USA' AS market
    UNION ALL
    SELECT 'outlet', 'GBR'
    UNION ALL
    SELECT 'outlet', 'DEU'
    UNION ALL
    SELECT 'portfolio', 'USA'
    UNION ALL
    SELECT 'portfolio', 'GBR'
    UNION ALL
    SELECT 'portfolio', 'DEU'
    UNION ALL
    SELECT 'matching', 'USA'
    UNION ALL
    SELECT 'matching', 'GBR'
    UNION ALL
    SELECT 'matching', 'DEU'
),

actual_coverage AS (
    SELECT
        'outlet' AS source_name,
        UPPER(TRIM(market)) AS market
    FROM {{ source('bronze_layer', 'OUTLET_PARSED') }}
    GROUP BY UPPER(TRIM(market))

    UNION ALL

    SELECT
        'portfolio' AS source_name,
        UPPER(TRIM(market)) AS market
    FROM {{ source('bronze_layer', 'PORTFOLIO_PARSED') }}
    GROUP BY UPPER(TRIM(market))

    UNION ALL

    SELECT
        'matching' AS source_name,
        UPPER(TRIM(market)) AS market
    FROM {{ source('bronze_layer', 'MATCHING_PARSED') }}
    GROUP BY UPPER(TRIM(market))
)

SELECT
    e.source_name,
    e.market
FROM expected_coverage e
LEFT JOIN actual_coverage a
    ON e.source_name = a.source_name
   AND e.market = a.market
WHERE a.market IS NULL
