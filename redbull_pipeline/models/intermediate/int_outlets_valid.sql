{{ config(materialized='view') }}

SELECT s.*
FROM {{ ref('stg_outlets') }} s
LEFT JOIN {{ ref('int_outlets_rejected') }} r
    ON s.market = r.market
   AND s.source_file = r.source_file
   AND s.file_row_number = r.file_row_number
WHERE r.file_row_number IS NULL
