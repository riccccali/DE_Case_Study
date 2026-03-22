{{ config(materialized='view') }}

WITH staged AS (
    SELECT *
    FROM {{ ref('stg_portfolio') }}
),

classified AS (
    SELECT
        *,
        CASE
            WHEN item_price IS NULL THEN 'MISSING_ITEM_PRICE'
            ELSE NULL
        END AS error_code,
        CASE
            WHEN item_price IS NULL THEN 'Item price could not be parsed from the source row.'
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
    id_beverage,
    id_ext_link,
    id_outlet,
    id_drink,
    id_category,
    item_name,
    item_brand,
    item_subbrand,
    item_manufacturer,
    menu_category,
    drink_category_primary,
    drink_category_secondary,
    item_description,
    item_volume_ml,
    item_price,
    item_position_code,
    packaging_size,
    is_banner_available,
    has_addon_prompt,
    addon_text,
    item_image_url,
    created_at
FROM classified
WHERE error_code IS NOT NULL
