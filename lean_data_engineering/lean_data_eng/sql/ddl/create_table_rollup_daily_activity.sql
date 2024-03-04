/*
Parameterized query creating rollup table into Snowflake. 
Timestamp columns are currently varchar due to DuckDB/ parquet typing weirdness
Schema value filled by python string literal
*/

create table if not exists {DATABASE}.{SCHEMA}.rollup_daily_activity (
    occurred_at_date_utc    varchar,
    occurred_at_hour_utc    varchar,
    a_unique_identifier     varchar,
    another_id              varchar,
    event_type              varchar,
    event_count             number,
    event_origin            varchar,
    _pipeline_received_at   varchar,
    _surrogate_key          varchar
);