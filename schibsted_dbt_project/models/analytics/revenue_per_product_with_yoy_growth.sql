
{{
    config(
        materialized='table'
    )
}}

With yearly_revenue AS (
    SELECT 
        product_id,
        sales_year,
        SUM(revenue_per_sales) AS total_revenue_per_product
    FROM {{ ref('final_sales') }}
    GROUP BY product_id, sales_year
),

yoy_growth AS (
    SELECT 
        product_id,
        sales_year,
        total_revenue_per_product,
        LAG(total_revenue_per_product) OVER (PARTITION BY product_id ORDER BY sales_year) AS prev_year_revenue,
        CASE 
            WHEN LAG(total_revenue_per_product) OVER (PARTITION BY product_id ORDER BY sales_year) IS NOT NULL
            THEN ROUND(
                ((total_revenue_per_product - prev_year_revenue) / 
                        prev_year_revenue) * 100, 2)
            ELSE NULL
        END AS yoy_growth_percentage
    FROM yearly_revenue
)

Select 
    product_id,
    sales_year,
    total_revenue_per_product,
    prev_year_revenue,
    yoy_growth_percentage
From yoy_growth
