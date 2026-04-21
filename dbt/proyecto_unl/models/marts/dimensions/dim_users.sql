SELECT DISTINCT
    user_id
FROM {{ ref('stg_transactions') }}