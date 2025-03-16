{{ config(
    materialized='incremental',
    incremental_strategy='append'
    )
}}

WITH new_sales_transaction AS (
    SELECT 
        transaction_id AS sales_transaction_id,
        product_id,
        rep_id AS sales_representative_id,
        TRY_CAST(date AS DATE) AS sales_transaction_date,  
        TRY_CAST(revenue AS FLOAT) AS revenue_per_sales,  
        filename AS source_filename,
        ingestion_timestamp,
        part_created 
    FROM {{ ref('raw_sales') }} -- Reference raw data
    Where 1 = 1
    {% if is_incremental() %}
        AND ingestion_timestamp > (select max(ingestion_timestamp) from {{ this}} )
    {% endif %}
)

SELECT 
    sales_transaction_id,
    product_id,
    sales_representative_id,
    sales_transaction_date,
    revenue_per_sales,
    source_filename,
    ingestion_timestamp,
    part_created
FROM new_sales_transaction
Where 1 = 1

QUALIFY 
    ROW_NUMBER() OVER 
    (PARTITION BY 
        sales_transaction_id 
    ORDER BY 
        ingestion_timestamp DESC) = 1
