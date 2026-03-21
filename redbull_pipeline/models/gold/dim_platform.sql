{{ config(materialized='view') }}

WITH platform_source AS (
    SELECT
        market,
        id_platform,
        platform_name,
        load_timestamp
    FROM {{ ref('int_outlets_valid') }}
    WHERE id_platform IS NOT NULL
),

deduplicated AS (
    SELECT
        market,
        id_platform,
        platform_name,
        ROW_NUMBER() OVER (
            PARTITION BY market, id_platform
            ORDER BY load_timestamp DESC, platform_name
        ) AS row_num
    FROM platform_source
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['market', 'id_platform']) }} AS platform_key,
    {{ dbt_utils.generate_surrogate_key(['market']) }} AS market_key,
    market,
    id_platform,
    platform_name
FROM deduplicated
WHERE row_num = 1
