{{ config(materialized='view') }}

WITH listings AS (
    SELECT
        market,
        source_file,
        file_row_number,
        id_outlet,
        id_beverage,
        id_ext_link,
        item_price,
        item_volume_ml,
        item_position_code,
        menu_category,
        is_banner_available,
        has_addon_prompt,
        created_at,
        load_timestamp
    FROM {{ ref('int_portfolio_valid') }}
    WHERE id_outlet IS NOT NULL
      AND id_beverage IS NOT NULL
),

enriched AS (
    SELECT
        l.market,
        l.source_file,
        l.file_row_number,
        l.id_outlet,
        l.id_beverage,
        l.id_ext_link,
        l.item_price,
        l.item_volume_ml,
        l.item_position_code,
        l.menu_category,
        l.is_banner_available,
        l.has_addon_prompt,
        l.created_at,
        l.load_timestamp,
        o.platform_key,
        o.outlet_key,
        p.product_key,
        {{ dbt_utils.generate_surrogate_key(['l.market']) }} AS market_key,
        o.serves_red_bull,
        o.serves_drinks,
        o.is_chain,
        o.average_rating AS outlet_average_rating,
        o.num_ratings AS outlet_num_ratings,
        o.delivery_fee,
        o.min_order_amount
    FROM listings l
    LEFT JOIN {{ ref('dim_outlet') }} o
        ON l.market = o.market
       AND l.id_ext_link = o.id_ext_link
    LEFT JOIN {{ ref('dim_product') }} p
        ON l.market = p.market
       AND l.id_beverage = p.id_beverage
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['market', 'source_file', 'file_row_number']) }} AS listing_key,
    market_key,
    platform_key,
    outlet_key,
    product_key,
    market,
    source_file,
    file_row_number,
    id_outlet,
    id_beverage,
    id_ext_link,
    item_price,
    item_volume_ml,
    item_position_code,
    menu_category,
    is_banner_available,
    has_addon_prompt,
    serves_red_bull,
    serves_drinks,
    is_chain,
    outlet_average_rating,
    outlet_num_ratings,
    delivery_fee,
    min_order_amount,
    created_at,
    load_timestamp
FROM enriched
WHERE outlet_key IS NOT NULL
