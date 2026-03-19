{{ config(materialized='view') }}

SELECT s.*
FROM {{ ref('stg_portfolio') }} s
LEFT JOIN {{ ref('int_portfolio_rejected') }} r
    ON s.market = r.market
   AND s.source_file = r.source_file
   AND s.file_row_number = r.file_row_number
WHERE r.file_row_number IS NULL
