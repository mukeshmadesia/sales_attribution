{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH new_sales_reps AS (
    SELECT 
        rep_id AS sales_representative_id,
        rep_name AS sales_representative_name,
        team AS sales_representative_team,
        COALESCE(NULLIF(region, ''), 'Unknown') AS sales_representative_region,
        TRY_CAST(start_date AS DATE) AS sales_rep_start_date,
        TRY_CAST(end_date AS DATE) AS sales_rep_end_date,  
        filename AS source_filename,
        ingestion_timestamp,
        part_created 
    FROM {{ ref('raw_sales_representative') }} -- Reference raw data
    Where 1 = 1
    {% if is_incremental() %}
        AND ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT 
    sales_representative_id,
    sales_representative_name,
    sales_representative_team,
    sales_representative_region,
    sales_rep_start_date,
    sales_rep_end_date,
    source_filename,
    ingestion_timestamp,
    part_created
FROM new_sales_reps
Where 1 = 1

QUALIFY 
    ROW_NUMBER() OVER 
    (PARTITION BY sales_representative_id, sales_rep_start_date ORDER BY ingestion_timestamp DESC) = 1
