{{ config(materialized='incremental') }}

SELECT *,
CURRENT_TIMESTAMP AS ingestion_timestamp
FROM read_parquet('{{ var("dsp_data_path", "../landing_place/dsp_data_long/*/data.parquet") }}',
    filename = true
    )

Where 1 = 1
{% if is_incremental() and var("dsp_data_path", '') =='' %}
-- Append only new records
AND part_created > (SELECT max(part_created) FROM {{ this }})
{% endif %}