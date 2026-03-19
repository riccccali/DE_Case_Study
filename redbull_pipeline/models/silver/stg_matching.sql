{{ config(materialized='view') }}

WITH parsed_source AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        ID_OUTLET,
        ID_PLATFORM,
        PLATFORM_NAME,
        ID_EXT_LINK,
        PLACE_ID,
        SIMILARITY_SCORE_NAME,
        SIMILARITY_SCORE_ADDRESS,
        MERGED_CHAIN_NAME,
        IS_CHAIN,
        NUM_RESTAURANTS,
        SERVES_DRINKS,
        SERVES_RED_BULL,
        SUGAR_FREE_AVAILABLE,
        ORGANICS_AVAILABLE,
        EDITIONS_AVAILABLE,
        ED,
        ED_COMP,
        SD_COKE,
        SD,
        LEADING_ID_EXT_LINK,
        CREATED_AT,
        LOAD_TIMESTAMP
    FROM {{ source('bronze_layer', 'MATCHING_PARSED') }}
),

raw_source AS (
    SELECT
        UPPER(TRIM(MARKET)) AS MARKET,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        RAW_DATA AS raw_record
    FROM {{ source('bronze_layer', 'MATCHING_RAW') }}
),

refined AS (
    SELECT
        p.MARKET,
        p.SOURCE_FILE,
        p.FILE_ROW_NUMBER,
        TRY_TO_NUMBER(p.ID_OUTLET) AS id_outlet,
        TRY_TO_NUMBER(p.ID_PLATFORM) AS id_platform,
        LOWER(REGEXP_REPLACE(p.PLATFORM_NAME, '[^a-zA-Z0-9]', '')) AS platform_name,
        TRY_TO_NUMBER(p.ID_EXT_LINK) AS id_ext_link,
        p.PLACE_ID AS place_id,
        TRY_TO_DOUBLE(p.SIMILARITY_SCORE_NAME) AS similarity_score_name,
        TRY_TO_DOUBLE(p.SIMILARITY_SCORE_ADDRESS) AS similarity_score_address,
        p.MERGED_CHAIN_NAME AS merged_chain_name,
        CASE WHEN p.IS_CHAIN = '1' THEN TRUE ELSE FALSE END AS is_chain,
        CASE WHEN p.SERVES_DRINKS = '1' THEN TRUE ELSE FALSE END AS serves_drinks,
        CASE WHEN p.SERVES_RED_BULL = '1' THEN TRUE ELSE FALSE END AS serves_red_bull,
        CASE WHEN p.SUGAR_FREE_AVAILABLE = '1' THEN TRUE ELSE FALSE END AS sugar_free_available,
        CASE WHEN p.ORGANICS_AVAILABLE = '1' THEN TRUE ELSE FALSE END AS organics_available,
        CASE WHEN p.EDITIONS_AVAILABLE = '1' THEN TRUE ELSE FALSE END AS editions_available,
        TRY_TO_NUMBER(p.NUM_RESTAURANTS) AS num_restaurants,
        TRY_TO_NUMBER(p.ED) AS ed,
        TRY_TO_NUMBER(p.ED_COMP) AS ed_comp,
        TRY_TO_NUMBER(p.SD_COKE) AS sd_coke,
        TRY_TO_NUMBER(p.SD) AS sd,
        TRY_TO_NUMBER(p.LEADING_ID_EXT_LINK) AS leading_id_ext_link,
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
