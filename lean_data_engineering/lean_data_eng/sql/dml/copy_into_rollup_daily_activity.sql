/*
Copy staged files into rollup table
*/

copy into {DATABASE}.{SCHEMA}.rollup_daily_activity 
from @{DATABASE}.dagster.processed
pattern = '.*path/to/your/datalake/files.*[.]parquet'
file_format = (format_name = {DATABASE}.{SCHEMA}.default_parquet)
on_error = skip_file
match_by_column_name = case_insensitive
;