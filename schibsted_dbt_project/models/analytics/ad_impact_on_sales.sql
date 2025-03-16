
{{
    config(
        materialized='table'
    )
}}

WITH sales AS (
    SELECT 
        s.sales_transaction_id,
        s.product_id,
        s.sales_representative_id,
        s.sales_transaction_date,
        s.revenue_per_sales,
        r.sales_representative_team,
        r.sales_representative_region,
        r.sales_representative_name
    FROM {{ ref('final_sales') }} s
    LEFT JOIN {{ ref('final_sales_representative') }} r 
        ON s.sales_representative_id = r.sales_representative_id
),

ad_performance AS (
    SELECT 
        a.product_id,
        DATE_TRUNC('day', a.advertisement_timestamp) AS ad_date,
        SUM(a.advertisment_impression) AS total_impressions,
        SUM(a.user_clicks_on_ad) AS total_clicks
    FROM {{ ref('final_dsp') }} a
    GROUP BY product_id, ad_date
),

ad_sales_analysis AS (
    SELECT 
        s.product_id,
        s.sales_transaction_date,
        s.sales_representative_id,
        s.revenue_per_sales,
        a.total_impressions,
        a.total_clicks,
        COALESCE(a.total_clicks / NULLIF(a.total_impressions, 0), 0) AS click_through_rate,
        SUM(s.revenue_per_sales) OVER (PARTITION BY s.product_id, s.sales_transaction_date) AS total_revenue,
        SUM(a.total_impressions) OVER (PARTITION BY s.product_id, s.sales_transaction_date) AS impressions_on_sales_day
    FROM sales s
    LEFT JOIN ad_performance a 
        ON s.product_id = a.product_id 
        AND s.sales_transaction_date = a.ad_date
)

SELECT * FROM ad_sales_analysis
WHERE 1 = 1
AND total_impressions > 0  -- Only analyze where ads were shown
ORDER BY sales_transaction_date DESC
