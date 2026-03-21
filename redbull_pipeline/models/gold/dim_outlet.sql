{{ config(materialized='view') }}

WITH outlet_source AS (
    SELECT
        market,
        id_outlet,
        id_ext_link,
        id_platform,
        platform_name,
        name,
        business_name,
        primary_category,
        segment_type,
        latitude,
        longitude,
        city,
        street_address,
        postal_code,
        address_state_locality,
        address_country,
        num_ratings,
        average_rating,
        delivery_fee,
        min_order_amount,
        local_currency,
        is_banner_available,
        is_ghost_kitchen,
        created_at,
        link,
        website,
        load_timestamp
    FROM {{ ref('int_outlets_valid') }}
    WHERE id_outlet IS NOT NULL
),

matching_agg AS (
    SELECT
        market,
        id_outlet,
        MAX(serves_drinks) AS serves_drinks,
        MAX(serves_red_bull) AS serves_red_bull,
        MAX(sugar_free_available) AS sugar_free_available,
        MAX(organics_available) AS organics_available,
        MAX(editions_available) AS editions_available,
        MAX(is_chain) AS is_chain,
        MAX(num_restaurants) AS num_restaurants,
        AVG(similarity_score_name) AS avg_similarity_score_name,
        AVG(similarity_score_address) AS avg_similarity_score_address
    FROM {{ ref('int_matching_valid') }}
    WHERE id_outlet IS NOT NULL
    GROUP BY market, id_outlet
),

final AS (
    SELECT
        d.market,
        d.id_outlet,
        d.id_ext_link,
        d.id_platform,
        d.platform_name,
        d.name AS outlet_name,
        d.business_name,
        d.primary_category,
        d.segment_type,
        d.latitude,
        d.longitude,
        d.city,
        d.street_address,
        d.postal_code,
        d.address_state_locality,
        d.address_country,
        d.num_ratings,
        d.average_rating,
        d.delivery_fee,
        d.min_order_amount,
        d.local_currency,
        d.is_banner_available,
        d.is_ghost_kitchen,
        d.created_at,
        d.link,
        d.website,
        COALESCE(m.serves_drinks, FALSE) AS serves_drinks,
        COALESCE(m.serves_red_bull, FALSE) AS serves_red_bull,
        COALESCE(m.sugar_free_available, FALSE) AS sugar_free_available,
        COALESCE(m.organics_available, FALSE) AS organics_available,
        COALESCE(m.editions_available, FALSE) AS editions_available,
        COALESCE(m.is_chain, FALSE) AS is_chain,
        m.num_restaurants,
        m.avg_similarity_score_name,
        m.avg_similarity_score_address
    FROM outlet_source d
    LEFT JOIN matching_agg m
        ON d.market = m.market
       AND d.id_outlet = m.id_outlet
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['market', 'id_ext_link']) }} AS outlet_key,
    {{ dbt_utils.generate_surrogate_key(['market']) }} AS market_key,
    {{ dbt_utils.generate_surrogate_key(['market', 'id_platform']) }} AS platform_key,
    *
FROM final
