{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key= ['sales_representative_id', 'sales_rep_start_date']
) }}

WITH new_valid_sales_reps AS (
    SELECT 
        sales_representative_id,
        sales_representative_name,
        sales_representative_team,
        sales_representative_region,
        sales_rep_start_date,
        sales_rep_end_date, 
        ingestion_timestamp 
    FROM {{ ref('staging_sales_representative') }} 
    WHERE 1 = 1
    AND sales_rep_start_date IS NOT NULL
    {% if is_incremental() %}
        AND ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

 overlapping_records AS (
    SELECT 
        sales_representative_id,
        sales_representative_name,
        sales_representative_team,
        sales_representative_region,
        sales_rep_start_date,
        sales_rep_end_date, 
        LEAD(sales_rep_start_date) OVER (PARTITION BY sales_representative_id ORDER BY sales_rep_start_date) AS sales_rep_next_start_date,
        ingestion_timestamp 
    FROM new_valid_sales_reps
)

SELECT 
    sales_representative_id,
    sales_representative_name,
    sales_representative_team,
    sales_representative_region,
    sales_rep_start_date,
    sales_rep_end_date,
    CASE 
        WHEN sales_rep_next_start_date IS NOT NULL AND (sales_rep_end_date IS NULL OR sales_rep_end_date > sales_rep_start_date) 
        THEN TRUE 
        ELSE FALSE 
    END AS is_team_overlapping,
    ingestion_timestamp
FROM overlapping_records
WHERE 1 = 1

QUALIFY 
    ROW_NUMBER() OVER 
    (PARTITION BY sales_representative_id, sales_rep_start_date ORDER BY ingestion_timestamp DESC) = 1
