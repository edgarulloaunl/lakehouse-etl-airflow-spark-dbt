SELECT
    transaction_id,
    user_id,
    product_category,
    amount,
    currency,
    status
FROM {{ ref('stg_transactions') }}