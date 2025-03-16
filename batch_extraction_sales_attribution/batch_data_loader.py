#!/usr/bin/env python3


'''
Actual prod scenario would be to read from source path or database and write in to cloud bucket (S3).
The script is show how data lake structure would look like and how historical updated would be handled in further DBT pipelines.

This script read csv and write into required path(folder structure) in parquet format, since parquet format is more optimized to processed the large data volume.
daily file will be stored in corresponding part_created to maintain the historical data and scalability in furthure processing.
script can be modified to run with previous date to overwrite existing part_created, or new updated records can be written as new part_created.

Script checks there is no missing columns but strict data type check of date column is not done to allow system to clean the data in further process to use other data for required analytical purposes.

Further DBT Pipeline can handle both scenario:
    1. Updates received in new delta file will be handled and old records will be updated and deduplicated as required
    2. DBT Model can be executed in adhoc mode with specific file path

'''

import polars as pl
import yaml
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# yaml file contains list of input source and correspoing schema 
with open("batch_extraction_sales_attribution/config.yml", "r") as file:
    config = yaml.safe_load(file)

input_files = config["input_files"]
output_base_path = config["output_base_path"]
expected_schemas = config["expected_schema"]

# current date will be used as part_created to partition the data lake on basis of date
part_created = datetime.today().strftime("%Y-%m-%d")

# For the task, directly csv is used as source but crm data has to be fetched from sql database 
# In actual database connection would be required to query data from crm,
# Delta (updated/inserted) records can be fetched depending upon the columns like created_at and updated_at in database. 
# DBT Pipeline will handle the full load from database also and will deduplicate at final stage
for dataset_name, input_file in input_files.items():
    try:
        # Schema is provided, strict schema validation of date is not being done here but records which can not be converted to date format will be 
        # removed from final model while DBT pipeline
        if dataset_name not in expected_schemas:
            logger.warning(f"⚠️ No schema defined for {dataset_name}. Skipping...")
            continue

        schema_details = expected_schemas[dataset_name]
        expected_columns = schema_details["columns"]

        # Convert YAML schema to Polars dtypes
        polars_schema = {col: getattr(pl, dtype) for col, dtype in expected_columns.items()}

        df = pl.read_csv(input_file, dtypes=polars_schema)

        # If any columns are missing from file, will skip the fle and error will be raised
        # which needs to be followed with source data team or the reason and re-run the batch before execution SLA of DBT pipeline
        missing_columns = [col for col in expected_columns.keys() if col not in df.columns]
        if missing_columns:
            logger.error(f"❌ Missing columns in {input_file}: {missing_columns}")
            continue  # Skip this file

        # Generate output path
        output_dir = os.path.join(output_base_path, dataset_name, f"part_created={part_created}")
        output_path = os.path.join(output_dir, "data.parquet")

        # Ensure directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Write Parquet
        df.write_parquet(output_path)
        logger.info(f"✅ Processed {input_file} → {output_path}") ## "\u2705 Success!" Unicode for tick

        # Re-verify schema of the parquet file written
        df_check = pl.read_parquet(output_path)
        if df.schema == df_check.schema:
            logger.info(f"✅ Schema verified for {output_path}")
        else:
            logger.warning(f"⚠️ Schema mismatch in {output_path}!")

    except Exception as e:
        logger.error(f"❌ Error processing {input_file}: {e}") ## "\u274C Error!" Unicode for X
