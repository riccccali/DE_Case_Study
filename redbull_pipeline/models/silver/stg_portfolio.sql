{{ config(materialized='view') }}

WITH extraction AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        -- Tab-separated extraction
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 1), '"'), '') AS raw_id_beverage,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 2), '"'), '') AS raw_id_ext_link,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 3), '"'), '') AS raw_id_outlet,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 4), '"'), '') AS raw_item_position,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 5), '"'), '') AS raw_item_category,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 6), '"'), '') AS raw_id_drink,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 7), '"'), '') AS raw_item_manufacturer,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 8), '"'), '') AS raw_item_brand,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 9), '"'), '') AS raw_item_subbrand,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 10), '"'), '') AS raw_item_volume,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 11), '"'), '') AS raw_item_price,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 12), '"'), '') AS raw_id_category,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 13), '"'), '') AS raw_item_drink_category_1,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 14), '"'), '') AS raw_item_drink_category_2,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 15), '"'), '') AS raw_item_image_url,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 16), '"'), '') AS raw_item_image_hash,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 17), '"'), '') AS raw_item_name,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 18), '"'), '') AS raw_item_desc,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 19), '"'), '') AS raw_addon_prompt,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 20), '"'), '') AS raw_addon_prompt_text,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 21), '"'), '') AS raw_packaging_size,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 22), '"'), '') AS raw_banner_available,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 23), '"'), '') AS raw_banner_img_link,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 24), '"'), '') AS raw_banner_img_hash,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 25), '"'), '') AS raw_created_at,
        LOAD_TIMESTAMP
    FROM {{ source('bronze_layer', 'PORTFOLIO_RAW') }}
),

refined AS (
    SELECT
        MARKET,
        SOURCE_FILE,
        -- IDs (Kept as VARCHAR for 19-digit precision)
        raw_id_beverage AS id_beverage,
        raw_id_ext_link AS id_ext_link,
        raw_id_outlet AS id_outlet,
        raw_id_drink AS id_drink,
        raw_id_category AS id_category,

        -- Product Attributes
        raw_item_name AS item_name,
        raw_item_brand AS item_brand,
        raw_item_subbrand AS item_subbrand,
        raw_item_manufacturer AS item_manufacturer,
        raw_item_category AS menu_category,
        raw_item_drink_category_1 AS drink_category_primary,
        raw_item_drink_category_2 AS drink_category_secondary,
        raw_item_desc AS item_description,
        
        -- Measurements & Pricing
        TRY_TO_DOUBLE(raw_item_volume) AS item_volume_ml,
        TRY_TO_DOUBLE(raw_item_price) AS item_price,
        raw_item_position AS item_position_code,
        raw_packaging_size AS packaging_size,

        -- Booleans & Flags
        CASE WHEN LOWER(raw_banner_available) = 'true' THEN TRUE ELSE FALSE END AS is_banner_available,
        CASE WHEN raw_addon_prompt = '1' THEN TRUE ELSE FALSE END AS has_addon_prompt,
        raw_addon_prompt_text AS addon_text,

        -- Metadata & Assets
        raw_item_image_url AS item_image_url,
        TRY_TO_TIMESTAMP(raw_created_at) AS created_at,
        LOAD_TIMESTAMP
    FROM extraction
)

SELECT * FROM refined