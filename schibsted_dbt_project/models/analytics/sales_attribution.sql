WITH touchpoints AS (
    SELECT 
        s.sales_transaction_id,
        s.product_id,
        s.sales_transaction_date,
        s.revenue_per_sales,
        a.advertisement_id,
        a.advertisment_impression,
        a.user_clicks_on_ad,
        a.advertisement_timestamp,
        r.sales_representative_id,
        r.sales_representative_name
    FROM {{ ref('final_sales') }} s
    LEFT JOIN {{ ref('final_dsp') }} a 
        ON s.product_id = a.product_id
        AND s.sales_transaction_date::TIMESTAMP >= a.advertisement_timestamp 
        AND s.sales_transaction_date::TIMESTAMP <= a.advertisement_timestamp + INTERVAL '7 days'
    LEFT JOIN {{ ref('final_sales_representative') }} r
        ON s.sales_representative_id = r.sales_representative_id
)
SELECT 
    sales_transaction_id,
    product_id,
    sales_transaction_date,
    revenue_per_sales,
    advertisement_id,
    advertisment_impression,
    user_clicks_on_ad,
    sales_representative_id,
    sales_representative_name,
    COALESCE(user_clicks_on_ad/ NULLIF(advertisment_impression, 0), 0) AS click_through_rate,
    RANK() OVER (PARTITION BY product_id ORDER BY advertisement_timestamp ASC, sales_transaction_date ASC) AS first_touch,
    RANK() OVER (PARTITION BY product_id ORDER BY advertisement_timestamp DESC, sales_transaction_date DESC) AS last_touch
FROM touchpoints
Where  1 = 1
ORDER by 
    product_id,
    sales_transaction_date 