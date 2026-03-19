{{ config(materialized='view') }}

WITH staged AS (
    SELECT *
    FROM {{ ref('stg_matching') }}
),

classified AS (
    SELECT
        *,
        CASE
            WHEN raw_record IS NULL THEN 'MISSING_RAW_RECORD'
            WHEN created_at IS NULL THEN 'MISSING_CREATED_AT'
            ELSE NULL
        END AS error_code,
        CASE
            WHEN raw_record IS NULL THEN 'Lineage join back to bronze raw failed.'
            WHEN created_at IS NULL THEN 'Created_at could not be parsed from the source row.'
            ELSE NULL
        END AS error_reason
    FROM staged
)

SELECT
    market,
    source_file,
    file_row_number,
    error_code,
    error_reason,
    raw_record,
    load_timestamp,
    id_outlet,
    id_platform,
    platform_name,
    id_ext_link,
    place_id,
    similarity_score_name,
    similarity_score_address,
    merged_chain_name,
    is_chain,
    serves_drinks,
    serves_red_bull,
    sugar_free_available,
    organics_available,
    editions_available,
    num_restaurants,
    ed,
    ed_comp,
    sd_coke,
    sd,
    leading_id_ext_link,
    created_at
FROM classified
WHERE error_code IS NOT NULL
