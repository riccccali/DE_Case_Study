{{ config(materialized='view') }}

WITH parsed_source AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        ID_EXT_LINK,
        ID_OUTLET,
        ID_PLATFORM,
        PLATFORM_NAME,
        LINK,
        NAME,
        ADDRESS_BULK,
        BUSINESS,
        BUSINESS_URL,
        DELIVERY,
        CATEGORY,
        DESCRIPTION,
        TELEPHONE,
        LATITUDE,
        LONGITUDE,
        NUM_RATINGS,
        AVERAGE_RATING,
        AVERAGE_COST,
        CITY,
        ICON_URL,
        LOCAL_ICON_NAME,
        MIN_ORDER_AMOUNT,
        BANNER_AVAILABLE,
        BANNER_IMG_LINK,
        BANNER_IMG_HASH,
        MARKET_FIELD,
        STREET_ADDRESS,
        POSTAL_CODE,
        ADDRESS_LOCALITY,
        ADDRESS_COUNTRY,
        TELEPHONE_PLATFORM,
        CUISINE,
        WEBSITE,
        GHOST_KITCHEN,
        LOCAL_CURRENCY,
        CREATED_AT,
        SEGMENT_TYPE,
        LOAD_TIMESTAMP
    FROM {{ source('bronze_layer', 'OUTLET_PARSED') }}
),

raw_source AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        RAW_DATA AS raw_record
    FROM {{ source('bronze_layer', 'OUTLET_RAW') }}
),

refined AS (
    SELECT
        p.MARKET,
        p.SOURCE_FILE,
        p.FILE_ROW_NUMBER,
        p.ID_EXT_LINK AS id_ext_link,
        p.ID_OUTLET AS id_outlet,
        p.ID_PLATFORM AS id_platform,
        p.PLATFORM_NAME AS platform_name,
        p.NAME AS name,
        p.BUSINESS AS business_name,
        p.CATEGORY AS primary_category,
        p.SEGMENT_TYPE AS segment_type,
        TRY_TO_DOUBLE(p.LATITUDE) AS latitude,
        TRY_TO_DOUBLE(p.LONGITUDE) AS longitude,
        p.CITY AS city,
        p.STREET_ADDRESS AS street_address,
        p.POSTAL_CODE AS postal_code,
        p.ADDRESS_LOCALITY AS address_state_locality,
        p.ADDRESS_COUNTRY AS address_country,
        TRY_TO_NUMBER(p.NUM_RATINGS) AS num_ratings,
        TRY_TO_DOUBLE(p.AVERAGE_RATING) AS average_rating,
        TRY_TO_DOUBLE(p.DELIVERY) AS delivery_fee,
        TRY_TO_DOUBLE(p.MIN_ORDER_AMOUNT) AS min_order_amount,
        p.LOCAL_CURRENCY AS local_currency,
        CASE WHEN LOWER(p.BANNER_AVAILABLE) = 'true' THEN TRUE ELSE FALSE END AS is_banner_available,
        CASE WHEN p.GHOST_KITCHEN = '1' THEN TRUE ELSE FALSE END AS is_ghost_kitchen,
        TRY_TO_TIMESTAMP(p.CREATED_AT) AS created_at,
        p.LINK AS link,
        p.WEBSITE AS website,
        r.raw_record,
        p.LOAD_TIMESTAMP
    FROM parsed_source p
    LEFT JOIN raw_source r
        ON p.MARKET = r.MARKET
       AND p.SOURCE_FILE = r.SOURCE_FILE
       AND p.FILE_ROW_NUMBER = r.FILE_ROW_NUMBER
)

SELECT * FROM refined
