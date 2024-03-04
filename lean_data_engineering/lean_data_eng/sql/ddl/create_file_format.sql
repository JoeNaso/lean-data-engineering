/*
Create default parquet file format
*/

create file format if not exists {DATABASE}.{SCHEMA}.default_parquet 
type = PARQUET
compression = SNAPPY
;