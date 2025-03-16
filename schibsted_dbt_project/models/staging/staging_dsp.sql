{{ config(
    materialized='incremental',
    incremental_strategy='append'
    )
}}

WITH new_ads_data AS (
    SELECT 
        ad_id AS advertisement_id,
        lower(brand) AS brand,
        product_id,
        TRY_CAST(timestamp AS TIMESTAMP)  AS advertisement_timestamp,
        TRY_CAST(impressions AS INTEGER)  AS advertisment_impression,  
        TRY_CAST(clicks AS INTEGER)       AS user_clicks_on_ad,  
        filename AS source_filename,
        ingestion_timestamp,
        part_created 
    FROM {{ ref('raw_dsp') }} -- Reference raw data
    Where 1 = 1
    {% if is_incremental() %}
        AND ingestion_timestamp > (select max(ingestion_timestamp) from {{ this}} )
    {% endif %}
)

SELECT 
    advertisement_id,
    brand,
    product_id,
    advertisement_timestamp,
    advertisment_impression,
    user_clicks_on_ad,  
    source_filename,
    ingestion_timestamp,
    part_created
FROM new_ads_data
Where 1 = 1

QUALIFY 
    ROW_NUMBER() OVER 
    (PARTITION BY 
        advertisement_id 
    ORDER BY 
        ingestion_timestamp DESC) = 1
