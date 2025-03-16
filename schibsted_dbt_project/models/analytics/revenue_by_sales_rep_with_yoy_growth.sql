
{{
    config(
        materialized='table'
    )
}}

With yearly_revenue AS (
    SELECT 
        sales_representative_id,
        sales_year,
        SUM(revenue_per_sales) AS total_revenue_by_sales_rep
    FROM {{ ref('final_sales') }}
    GROUP BY sales_representative_id, sales_year
),

yoy_growth AS (
    SELECT 
        sales_representative_id,
        sales_year,
        total_revenue_by_sales_rep,
        LAG(total_revenue_by_sales_rep) OVER (PARTITION BY sales_representative_id ORDER BY sales_year) AS prev_year_revenue_by_sales_rep,
        CASE 
            WHEN LAG(total_revenue_by_sales_rep) OVER (PARTITION BY sales_representative_id ORDER BY sales_year) IS NOT NULL
            THEN ROUND(
                ((total_revenue_by_sales_rep - prev_year_revenue_by_sales_rep) / 
                        prev_year_revenue_by_sales_rep) * 100, 2)
            ELSE NULL
        END AS yoy_growth_percentage
    FROM yearly_revenue
)

Select 
    sales_representative_id,
    sales_year,
    total_revenue_by_sales_rep,
    prev_year_revenue_by_sales_rep,
    yoy_growth_percentage
From yoy_growth
