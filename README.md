

# Project Sales Attribution

## Project setup

1. Clone Repo
2. Install Poetry - Package manger
3. Install dependencies -  python, duckdb, and dbt-duckdb adapter
4. profiles.yml - contains database details, update if required
5. Test setup with `dbt debug`command
6. Source data to be placed in `source_data` path 

## Project Structure

            SALES_ATTRIBUTION/
            │
            ├── schibsted.duckdb                # datawarehouse (duckdb)
            ├── schibsted_dbt_project/          #   dbt project
            ├── batch_extraction_sales_attribution/           
            │   └── batch_data_loader..py         # Script to load data in data lake
            │   └── config.yml                     # contains schema and source file path
            ├── source_files/                   # source CSV files
            ├── pyproject.toml
     


## Extraction - Load into Data lake

Batch script will read csv and will create dummy data lake as per below structure 

    landing_place/ 
        ├── dsp_data_long/
        │   ├── part-created=YYYY-MM-DD/
        │   │   ├── data.parquet
        ├── crm_data_long/
        │   ├── part-created=YYYY-MM-DD/
        │   │   ├── data.parquet
        ├── sales_data_long/
        │   ├── part-created=YYYY-MM-DD/
        │   │   ├── data.parquet

Run 
`poetry run batch_extraction_sales_attribution/batch_data_loader.py`


## DBT Pipeline 

### Seed 
Static data -  brand and product mapping csv file

### Models

Each folder corresponds to each schema
### Raw  
- This stage is to maintain all data 
- Add audit colums for each row
    - Part_created
    - source_filename
    - ingestion_timestamp
- Any null/invalid value will be handled at further stage
- To handle the error scenario gracefully, we will later check the data type and values

### Staging
- This is an incremental model with Append strategy to keep the historical state of data for future reference
    - E.g. If there is any change in represenatative or revenue amount for given Sales transaction, in staging table we will have both the datapoints to refer where as at later final stage we wil have only latest datapoint
- Data type - Conversion using (TRY_CAST) so that invalid values are converted to NULL
- Column name changes

### Final
- This is incremental model with "delete+insert" strategy to update the existing records with latest data, this allows to handle any update in historical records
- E.g. If there is any change in represenatative or revenue amount for given Sales transaction, then Final will contain the latest data where as at staging table we will have both the datapoints to refer 
- Validation for Not NULL and Invalid data types

### Analytics 

- This is table Model used for reporting using final tables.
- Optimized by clustering as per Reporting needs
E.g. Model ‘revenue_by_sales_rep_with_yoy_growth’ can be clustered using sales_rep_id and Model ‘revenue_per_product_with_yoy_growth’ will be clustered using product_id

### Macro
* get_custom_schema - Added a Macro - to overide the schema naming convention by DBT - otherwise DBT add default_schema('main') as prefix 


## command 

To run python load script 

```
poetry run batch_extraction_sales_attribution/batch_data_loader.py
```

To execute dbt models

```
cd schibsted_dbt_project
```

```console 
dbt debug  # To connect and check connection
dbt seed  # To create table from seed file
dbt build  # To run and test models
dbt run --select raw_sales # To run specific model
```

For historical load of specific data file
```
dbt run --select raw_sales+ --vars '{"sales_data_path": "../landing_place/sales_data_long/part_created=2025-03-14/data.parquet"}'
```