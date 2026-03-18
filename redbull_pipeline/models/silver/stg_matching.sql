{{ config(materialized='view') }}

WITH extraction AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        -- Initial extraction: pulling strings, trimming quotes, and nullifying empty ""
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 1), '"'), '') AS raw_id_outlet,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 2), '"'), '') AS raw_id_platform,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 3), '"'), '') AS raw_platform_name,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 4), '"'), '') AS raw_id_ext_link,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 5), '"'), '') AS raw_place_id,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 6), '"'), '') AS raw_similarity_score_name,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 7), '"'), '') AS raw_similarity_score_address,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 8), '"'), '') AS raw_merged_chain_name,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 9), '"'), '') AS raw_is_chain,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 10), '"'), '') AS raw_num_restaurants,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 11), '"'), '') AS raw_serves_drinks,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 12), '"'), '') AS raw_serves_red_bull,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 13), '"'), '') AS raw_sugar_free_available,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 14), '"'), '') AS raw_organics_available,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 15), '"'), '') AS raw_editions_available,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 16), '"'), '') AS raw_ed,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 17), '"'), '') AS raw_ed_comp,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 18), '"'), '') AS raw_sd_coke,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 19), '"'), '') AS raw_sd,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 20), '"'), '') AS raw_leading_id_ext_link,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 21), '"'), '') AS raw_created_at,
        LOAD_TIMESTAMP
    FROM {{ source('bronze_layer', 'MATCHING_RAW') }}
),

refined AS (
    SELECT
        MARKET,
        SOURCE_FILE,
        -- IDs & Platform
        TRY_TO_NUMBER(raw_id_outlet) AS id_outlet,
        TRY_TO_NUMBER(raw_id_platform) AS id_platform,
        LOWER(REGEXP_REPLACE(raw_platform_name, '[^a-zA-Z0-9]', '')) AS platform_name,
        TRY_TO_NUMBER(raw_id_ext_link) AS id_ext_link,
        raw_place_id AS place_id,
        TRY_TO_DOUBLE(raw_similarity_score_name) AS similarity_score_name,
        TRY_TO_DOUBLE(raw_similarity_score_address) AS similarity_score_address,
        raw_merged_chain_name AS merged_chain_name,
        -- Booleans
        CASE WHEN raw_is_chain = '1' THEN TRUE ELSE FALSE END AS is_chain,
        CASE WHEN raw_serves_drinks = '1' THEN TRUE ELSE FALSE END AS serves_drinks,
        CASE WHEN raw_serves_red_bull = '1' THEN TRUE ELSE FALSE END AS serves_red_bull,
        CASE WHEN raw_sugar_free_available = '1' THEN TRUE ELSE FALSE END AS sugar_free_available,
        CASE WHEN raw_organics_available = '1' THEN TRUE ELSE FALSE END AS organics_available,
        CASE WHEN raw_editions_available = '1' THEN TRUE ELSE FALSE END AS editions_available,
        -- Integer Metrics & Flags
        TRY_TO_NUMBER(raw_num_restaurants) AS num_restaurants,
        TRY_TO_NUMBER(raw_ed) AS ed,
        TRY_TO_NUMBER(raw_ed_comp) AS ed_comp,
        TRY_TO_NUMBER(raw_sd_coke) AS sd_coke,
        TRY_TO_NUMBER(raw_sd) AS sd,
        TRY_TO_NUMBER(raw_leading_id_ext_link) AS leading_id_ext_link,
        -- Timestamps
        TRY_TO_TIMESTAMP(raw_created_at) AS created_at,
        LOAD_TIMESTAMP
    FROM extraction
)

SELECT * FROM refined