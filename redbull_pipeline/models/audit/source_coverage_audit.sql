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
        UPPER(TRIM(market)) AS market,
        COUNT(*) AS row_count,
        COUNT(DISTINCT source_file) AS source_file_count
    FROM {{ source('bronze_layer', 'OUTLET_PARSED') }}
    GROUP BY UPPER(TRIM(market))

    UNION ALL

    SELECT
        'portfolio' AS source_name,
        UPPER(TRIM(market)) AS market,
        COUNT(*) AS row_count,
        COUNT(DISTINCT source_file) AS source_file_count
    FROM {{ source('bronze_layer', 'PORTFOLIO_PARSED') }}
    GROUP BY UPPER(TRIM(market))

    UNION ALL

    SELECT
        'matching' AS source_name,
        UPPER(TRIM(market)) AS market,
        COUNT(*) AS row_count,
        COUNT(DISTINCT source_file) AS source_file_count
    FROM {{ source('bronze_layer', 'MATCHING_PARSED') }}
    GROUP BY UPPER(TRIM(market))
)

SELECT
    e.source_name,
    e.market,
    COALESCE(a.row_count, 0) AS row_count,
    COALESCE(a.source_file_count, 0) AS source_file_count,
    CASE
        WHEN a.market IS NULL THEN 'missing_expected_market'
        ELSE 'present'
    END AS coverage_status
FROM expected_coverage e
LEFT JOIN actual_coverage a
    ON e.source_name = a.source_name
   AND e.market = a.market
ORDER BY e.source_name, e.market
