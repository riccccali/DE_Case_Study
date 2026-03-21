{{ config(materialized='view') }}

WITH market_codes AS (
    SELECT 'USA' AS market
    UNION ALL
    SELECT 'GBR' AS market
    UNION ALL
    SELECT 'DEU' AS market
),

enriched AS (
    SELECT
        market,
        CASE
            WHEN market = 'USA' THEN 'United States'
            WHEN market = 'GBR' THEN 'United Kingdom'
            WHEN market = 'DEU' THEN 'Germany'
        END AS market_name
    FROM market_codes
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['market']) }} AS market_key,
    market,
    market_name
FROM enriched
