{{ config(materialized='view') }}

WITH parsed_source AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        ID_BEVERAGE,
        ID_EXT_LINK,
        ID_OUTLET,
        ITEM_POSITION,
        ITEM_CATEGORY,
        ID_DRINK,
        ITEM_MANUFACTURER,
        ITEM_BRAND,
        ITEM_SUBBRAND,
        ITEM_VOLUME,
        ITEM_PRICE,
        ID_CATEGORY,
        ITEM_DRINK_CATEGORY_1,
        ITEM_DRINK_CATEGORY_2,
        ITEM_IMAGE_URL,
        ITEM_IMAGE_HASH,
        ITEM_NAME,
        ITEM_DESC,
        ADDON_PROMPT,
        ADDON_PROMPT_TEXT,
        PACKAGING_SIZE,
        BANNER_AVAILABLE,
        BANNER_IMG_LINK,
        BANNER_IMG_HASH,
        CREATED_AT,
        LOAD_TIMESTAMP
    FROM {{ source('bronze_layer', 'PORTFOLIO_PARSED') }}
),

raw_source AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        RAW_DATA AS raw_record
    FROM {{ source('bronze_layer', 'PORTFOLIO_RAW') }}
),

refined AS (
    SELECT
        p.MARKET,
        p.SOURCE_FILE,
        p.FILE_ROW_NUMBER,
        p.ID_BEVERAGE AS id_beverage,
        p.ID_EXT_LINK AS id_ext_link,
        p.ID_OUTLET AS id_outlet,
        p.ID_DRINK AS id_drink,
        p.ID_CATEGORY AS id_category,
        p.ITEM_NAME AS item_name,
        p.ITEM_BRAND AS item_brand,
        p.ITEM_SUBBRAND AS item_subbrand,
        p.ITEM_MANUFACTURER AS item_manufacturer,
        p.ITEM_CATEGORY AS menu_category,
        p.ITEM_DRINK_CATEGORY_1 AS drink_category_primary,
        p.ITEM_DRINK_CATEGORY_2 AS drink_category_secondary,
        p.ITEM_DESC AS item_description,
        TRY_TO_DOUBLE(p.ITEM_VOLUME) AS item_volume_ml,
        TRY_TO_DOUBLE(p.ITEM_PRICE) AS item_price,
        p.ITEM_POSITION AS item_position_code,
        p.PACKAGING_SIZE AS packaging_size,
        CASE WHEN LOWER(p.BANNER_AVAILABLE) = 'true' THEN TRUE ELSE FALSE END AS is_banner_available,
        CASE WHEN p.ADDON_PROMPT = '1' THEN TRUE ELSE FALSE END AS has_addon_prompt,
        p.ADDON_PROMPT_TEXT AS addon_text,
        p.ITEM_IMAGE_URL AS item_image_url,
        TRY_TO_TIMESTAMP(p.CREATED_AT) AS created_at,
        r.raw_record,
        p.LOAD_TIMESTAMP
    FROM parsed_source p
    LEFT JOIN raw_source r
        ON p.MARKET = r.MARKET
       AND p.SOURCE_FILE = r.SOURCE_FILE
       AND p.FILE_ROW_NUMBER = r.FILE_ROW_NUMBER
)

SELECT * FROM refined
