/*
Copy staged files into transactions_raw table
*/

copy into {DATABASE}.{SCHEMA}.events_raw 
from @{DATABASE}.dagster.processed
pattern = '.path/to/your/datalake/files*[.]parquet'
file_format = (format_name = 'RAW.DAGSTER.DEFAULT_PARQUET')
on_error = skip_file
match_by_column_name = case_insensitive
;
