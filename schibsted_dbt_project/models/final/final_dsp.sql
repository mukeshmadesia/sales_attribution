{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='advertisement_id'
    )
}}

WITH new_valid_ads_data AS (
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
    FROM {{ ref('staging_dsp') }} 
    Where 1 = 1
    AND advertisement_id is not NULL
    AND advertisement_timestamp is not NULL
    AND advertisment_impression is not NULL
    AND user_clicks_on_ad is not NULL
    {% if is_incremental() %}
        AND ingestion_timestamp > ( select max(ingestion_timestamp) from {{ this}} )
    {% endif %}

)

SELECT 
    advertisement_id,
    brand,
    product_id,
    advertisement_timestamp,
    advertisment_impression,
    user_clicks_on_ad,  
    COALESCE(user_clicks_on_ad/NULLIF(advertisment_impression, 0),0) AS click_through_rate,
    source_filename,
    ingestion_timestamp,
    part_created
FROM new_valid_ads_data
Where 1 = 1


QUALIFY 
    ROW_NUMBER() OVER 
    (PARTITION BY 
        advertisement_id 
    ORDER BY 
        ingestion_timestamp DESC) = 1
