/*
Create stage for loading parquet files

General purpose that can be used various datasets
*/

create stage if not exists {DATABASE}.dagster.processed 
storage_integration = s3_int
url = '{STAGE_URL}'
file_format = default_parquet
;