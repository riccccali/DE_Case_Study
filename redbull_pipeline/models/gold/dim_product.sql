{{ config(materialized='view') }}

WITH product_source AS (
    SELECT
        market,
        id_beverage,
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
        packaging_size,
        item_image_url,
        has_addon_prompt,
        addon_text,
        created_at,
        load_timestamp
    FROM {{ ref('int_portfolio_valid') }}
    WHERE id_beverage IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY market, id_beverage
            ORDER BY created_at DESC NULLS LAST, load_timestamp DESC
        ) AS row_num
    FROM product_source
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['market', 'id_beverage']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['market']) }} AS market_key,
    market,
    id_beverage,
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
    packaging_size,
    item_image_url,
    has_addon_prompt,
    addon_text,
    created_at
FROM deduplicated
WHERE row_num = 1
