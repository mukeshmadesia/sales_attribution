{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='sales_transaction_id'
    )
}}

WITH new_valid_sales_transaction AS (
    SELECT 
        sales_transaction_id,
        product_id,
        sales_representative_id,
        sales_transaction_date,  
        revenue_per_sales, 
        source_filename,
        ingestion_timestamp,
        part_created 
    FROM {{ ref('staging_sales') }} 
    Where 1 = 1
    AND sales_transaction_id is not NULL
    AND sales_transaction_date is not NULL
    AND revenue_per_sales is not NULL
    {% if is_incremental() %}
        AND ingestion_timestamp > ( select max(ingestion_timestamp) from {{ this}} )
    {% endif %}

)

SELECT 
    sales_transaction_id,
    product_id,
    sales_representative_id,
    sales_transaction_date,
    year(sales_transaction_date) AS sales_year,
    month(sales_transaction_date) AS sales_month,
    revenue_per_sales,
    source_filename,
    ingestion_timestamp,
    part_created
FROM new_valid_sales_transaction
Where 1 = 1


QUALIFY 
    ROW_NUMBER() OVER 
    (PARTITION BY 
        sales_transaction_id 
    ORDER BY 
        ingestion_timestamp DESC) = 1
