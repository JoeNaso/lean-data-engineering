/*
Process gzipped JSON files and convert them to local partitioned parquet
Parameters:
    :target_files:  List of filenames in s3
    :origin:        Name of event origin, for downstream processing
*/

COPY(
    select 
        *,
        date_trunc('day', timezone('UTC', occurred_at::timestamptz))::varchar  as occurred_at_date_utc,
        date_trunc('hour', timezone('UTC', occurred_at::timestamptz))::varchar  as occurred_at_hour_utc,
        $origin AS event_origin,
    from read_json(
        $target_files,
        format='newline_delimited',
        union_by_name=True, 
        columns={
            a_unique_identifier: 'INTEGER', 
            another_id: 'INTEGER', 
            event_type: 'VARCHAR',
            occurred_at: 'TIMESTAMPTZ', 
            source: 'VARCHAR',
            raw: 'JSON'
    })
)
TO '/tmp/json-to-parquet/' 
(
    FORMAT PARQUET, 
    PARTITION_BY (occurred_at_date_utc, occurred_at_hour_utc, event_origin),
    OVERWRITE_OR_IGNORE 1
);
