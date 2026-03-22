{{ config(materialized='view') }}

WITH staged AS (
    SELECT *
    FROM {{ ref('stg_outlets') }}
),

classified AS (
    SELECT
        *,
        CASE
            WHEN created_at IS NULL THEN 'MISSING_CREATED_AT'
            WHEN segment_type NOT IN (
                'restaurant',
                'grocery/convenience store/supermarket/liquor store'
            ) THEN 'INVALID_SEGMENT_TYPE'
            ELSE NULL
        END AS error_code,
        CASE
            WHEN created_at IS NULL THEN 'Created_at could not be parsed from the source row.'
            WHEN segment_type NOT IN (
                'restaurant',
                'grocery/convenience store/supermarket/liquor store'
            ) THEN 'Segment type is outside the currently accepted business domain.'
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
    id_ext_link,
    id_outlet,
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
    website
FROM classified
WHERE error_code IS NOT NULL
