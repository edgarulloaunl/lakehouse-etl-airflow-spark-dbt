SELECT DISTINCT
    product_category
FROM {{ ref('stg_transactions') }}