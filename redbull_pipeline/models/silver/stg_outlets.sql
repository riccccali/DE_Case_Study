{{ config(materialized='view') }}

WITH extraction AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 1), '"'), '') AS raw_id_ext_link,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 2), '"'), '') AS raw_id_outlet,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 3), '"'), '') AS raw_id_platform,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 4), '"'), '') AS raw_platform_name,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 5), '"'), '') AS raw_link,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 6), '"'), '') AS raw_name,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 7), '"'), '') AS raw_address_bulk,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 8), '"'), '') AS raw_business,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 9), '"'), '') AS raw_business_url,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 10), '"'), '') AS raw_delivery,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 11), '"'), '') AS raw_category,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 12), '"'), '') AS raw_description,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 13), '"'), '') AS raw_telephone,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 14), '"'), '') AS raw_latitude,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 15), '"'), '') AS raw_longitude,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 16), '"'), '') AS raw_num_ratings,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 17), '"'), '') AS raw_average_rating,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 18), '"'), '') AS raw_average_cost,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 19), '"'), '') AS raw_city,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 20), '"'), '') AS raw_icon_url,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 21), '"'), '') AS raw_local_icon_name,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 22), '"'), '') AS raw_min_order_amount,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 23), '"'), '') AS raw_banner_available,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 24), '"'), '') AS raw_banner_img_link,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 25), '"'), '') AS raw_banner_img_hash,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 26), '"'), '') AS raw_market_field,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 27), '"'), '') AS raw_street_address,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 28), '"'), '') AS raw_postal_code,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 29), '"'), '') AS raw_address_locality,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 30), '"'), '') AS raw_address_country,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 31), '"'), '') AS raw_telephone_platform,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 32), '"'), '') AS raw_cuisine,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 33), '"'), '') AS raw_website,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 34), '"'), '') AS raw_ghost_kitchen,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 35), '"'), '') AS raw_local_currency,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 36), '"'), '') AS raw_created_at,
        NULLIF(TRIM(SPLIT_PART(RAW_DATA, '\t', 37), '"'), '') AS raw_segment_type,
        LOAD_TIMESTAMP
    FROM {{ source('bronze_layer', 'OUTLET_RAW') }}
),

refined AS (
    SELECT
        MARKET,
        SOURCE_FILE,
        -- IDs (Keeping as VARCHAR due to 19-digit length to prevent precision loss)
        raw_id_ext_link AS id_ext_link,
        raw_id_outlet AS id_outlet,
        raw_id_platform AS id_platform,
        
        -- Descriptive
        raw_platform_name AS platform_name,
        raw_name AS name,
        raw_business AS business_name,
        raw_category AS primary_category,
        raw_segment_type AS segment_type,
        
        -- Location
        TRY_TO_DOUBLE(raw_latitude) AS latitude,
        TRY_TO_DOUBLE(raw_longitude) AS longitude,
        raw_city AS city,
        raw_street_address AS street_address,
        raw_postal_code AS postal_code,
        raw_address_locality AS address_state_locality,
        raw_address_country AS address_country,

        -- Ratings & Delivery
        TRY_TO_NUMBER(raw_num_ratings) AS num_ratings,
        TRY_TO_DOUBLE(raw_average_rating) AS average_rating,
        TRY_TO_DOUBLE(raw_delivery) AS delivery_fee,
        TRY_TO_DOUBLE(raw_min_order_amount) AS min_order_amount,
        raw_local_currency AS local_currency,

        -- Booleans
        CASE WHEN LOWER(raw_banner_available) = 'true' THEN TRUE ELSE FALSE END AS is_banner_available,
        CASE WHEN raw_ghost_kitchen = '1' THEN TRUE ELSE FALSE END AS is_ghost_kitchen,

        -- Dates & Links
        TRY_TO_TIMESTAMP(raw_created_at) AS created_at,
        raw_link AS link,
        raw_website AS website,
        LOAD_TIMESTAMP
    FROM extraction
)

SELECT * FROM refined