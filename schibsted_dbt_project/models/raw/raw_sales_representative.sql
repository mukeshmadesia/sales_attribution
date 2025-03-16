{{ config(materialized='incremental') }}

SELECT *,
CURRENT_TIMESTAMP AS ingestion_timestamp
FROM read_parquet('{{ var("crm_data_path", "../landing_place/crm_data_long/*/data.parquet") }}',
    filename = true
    )

Where 1 = 1
{% if is_incremental() and var("crm_data_path", '') == '' %}
-- Append only new records
AND part_created > (SELECT max(part_created) FROM {{ this }})
{% endif %}