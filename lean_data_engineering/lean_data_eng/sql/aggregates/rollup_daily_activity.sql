/*
Parameterized query for hourly aggregation. 
Executed by DuckDB
so type conversion can still happen in DBT

:origin:        origin of data (source)
:target_date:   date partition 
:target_hour:   datetime with hour partition

STAGE_URL value handled via python
*/

copy (
    with daily_rollup as (
            select
                occurred_at_date_utc, 
                occurred_at_hour_utc,
                try_cast(a_unique_identifier as varchar) as a_unique_identifier,
                try_cast(another_id as varchar) as another_id,
                event_type,
                event_origin,
                count(*) AS event_count
            from read_parquet('/tmp/json-to-parquet/*/*/*/*.parquet', hive_partitioning=1)
            where occurred_at_date_utc = $target_date
                and occurred_at_hour_utc = $target_hour
                and event_origin = $origin
            group by all 
    )
    select
        occurred_at_date_utc,
        occurred_at_hour_utc::varchar as occurred_at_hour_utc,
        a_unique_identifier,
        another_id,
        event_type,
        event_count,
        event_origin,
        timezone('UTC', now())::varchar as _pipeline_received_at,
        concat_ws('|', $origin, event_type, occurred_at_date_utc, occurred_at_hour_utc, a_unique_identifier) as _surrogate_key,
    from daily_rollup
) 
TO '{STAGE_URL}' (FORMAT PARQUET)
;