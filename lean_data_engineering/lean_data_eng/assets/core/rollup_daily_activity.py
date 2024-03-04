import datetime
import os
import time
import re

from typing import Tuple

from lean_data_eng.assets.duck import DuckDB
from lean_data_eng import constants
from lean_data_eng.resources import SnowflakeResource

from dagster import (
    asset,
    AssetExecutionContext,
    BackfillPolicy,
    MaterializeResult,
    MetadataValue,
    HourlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from dagster_aws.s3 import S3Resource


COPY_INTO_SNOWFLAKE = os.path.join(
    os.sep, constants.SQL_DIR, "dml", "copy_into_rollup_daily_activity.sql"
)
COPY_JSON_TO_PARQUET_LOCAL = os.path.join(
    os.sep, constants.SQL_DIR, "dml", "copy_json_to_parquet_local.sql"
)
CREATE_FILE_FORMAT = os.path.join(
    os.sep, constants.SQL_DIR, "ddl", "create_file_format.sql"
)
CREATE_ROLLUP_STAGE = os.path.join(
    os.sep, constants.SQL_DIR, "ddl", "create_stage_s3_datalake.sql"
)
CREATE_ROLLUP_TABLE = os.path.join(
    os.sep, constants.SQL_DIR, "ddl", "create_table_rollup_daily_activity.sql"
)
ROLLUP_DAILY_ACTIVITY = os.path.join(
    os.sep, constants.SQL_AGGREGATES_DIR, "rollup_daily_activity.sql"
)

# were using Hourly here, even though our rollups are daily. This is intentional
rollup_partition = MultiPartitionsDefinition(
    {
        "target_date": HourlyPartitionsDefinition(
            start_date="2024-01-01-01:00", timezone="Etc/UTC"
        ),
        "data_origin": StaticPartitionsDefinition(constants.RAW_ACTIVITY_ORIGINS),
    }
)

# Half a day per single run
multi_partition_backfill = BackfillPolicy(max_partitions_per_run=12)


def get_export_path(data_origin, target_date) -> str:
    """
    Get full s3 export path
    ex:
        s3://datalake-bucket/some-partitiion/target-date/data_origin.parquet
    """
    path = f"aggregates/rollups/{target_date}/{data_origin}.parquet"
    return f"{constants.S3_SNOWFLAKE_STAGE}{path}"


def get_target_datetimes(datetime_partition: str) -> Tuple[datetime.datetime, str, str]:
    """
    Given partition datetime value, return the corresponding date and hour formatted values
    Partitions are configured with an offset, but we always process the previous "complete" hour

    ie 2023-12-01-11:15 --> 2023-12-01-10:00
    """
    target = datetime.datetime.strptime(datetime_partition, "%Y-%m-%d-%H:%M")
    adjusted_datetime = target.replace(minute=0) - datetime.timedelta(hours=1)
    target_hour = (adjusted_datetime).strftime("%Y-%m-%d %H:%M:%S")

    return adjusted_datetime, adjusted_datetime.date().isoformat(), target_hour


def filter_s3_files(
    origin: str,
    adjusted_datetime: datetime,
    s3_raw_activity: S3Resource,
):
    """
    Filter s3 bucket files based on provided origin and datetime
    
    This function doesn't do much as-is, but this is where you'd determine the files based on partition
    """
    response = s3_raw_activity.get_client().list_objects_v2(
        Bucket=constants.RAW_ACTIVITY_BUCKET,
        Prefix=f"some-partition/{origin}/{adjusted_datetime.date().isoformat()}/",
    )
    filepaths = [file["Key"] for file in response["Contents"]]
    # Do some work to figure out the files you should process based on the partition
    target_files = map(lambda x: f"s3://{constants.RAW_ACTIVITY_BUCKET}/{x}", filepaths)
    return list(target_files)


@asset(partitions_def=rollup_partition, backfill_policy=multi_partition_backfill)
def hive_partitioned_events(
    context: AssetExecutionContext, s3_raw_activity: S3Resource
) -> MaterializeResult:
    """
    Query s3 data and dump local hive partitioned parquet
    """
    partitions = context.partition_key.keys_by_dimension
    context.log.info(partitions)
    origin = partitions.get("data_origin")
    datetime_partition = partitions.get("target_datetime")
    adjusted_datetime, _, target_hour = get_target_datetimes(datetime_partition)

    context.log.info("Fetching target files...")
    target_files = filter_s3_files(
        origin, adjusted_datetime, s3_raw_activity
    )
    # Return early if there is no data
    if not len(target_files):
        return MaterializeResult(
            metadata={
                "total_target_files": 0,
                "origin": MetadataValue.text(origin),
                "adjusted_datetime": MetadataValue.text(adjusted_datetime.isoformat()),
                "target_hour": None,
                "query_duration_seconds": None,
            }
        )

    params = {
        "target_files": target_files,
        "origin": origin,
    }
    duck = DuckDB.build()
    with open(COPY_JSON_TO_PARQUET_LOCAL, "r") as qry:
        query_start = time.perf_counter()
        cmd = qry.read()
        context.log.info(f"Query:\n{cmd}")
        context.log.info(f"Params:\n{params}")
        _ = duck.execute(cmd, params=params, fetch_all=False)
        query_end = time.perf_counter()

    return MaterializeResult(
        metadata={
            "total_target_files": len(target_files),
            "origin": MetadataValue.text(origin),
            "adjusted_datetime": MetadataValue.text(adjusted_datetime.isoformat()),
            "target_hour": MetadataValue.text(target_hour),
            "query_duration_seconds": MetadataValue.float(
                round(query_end - query_start, 0)
            ),
        }
    )


@asset(
    partitions_def=rollup_partition,
    non_argument_deps={"hive_partitioned_events"},
    backfill_policy=multi_partition_backfill,
)
def staged_rollup(context: AssetExecutionContext) -> MaterializeResult:
    """
    Take local hive parititioned parquet files and aggregate in duckdb
    Dump the aggregated files to s3, which is a Snowflake External Stage
    """
    partitions = context.partition_key.keys_by_dimension
    context.log.info(partitions)
    data_origin = partitions.get("data_origin")
    target_date = partitions.get("target_date")
    params = {
        "target_date": target_date,
        "origin": data_origin,
    }
    duck = DuckDB.build()
    stage_url = get_export_path(**params)
    with open(ROLLUP_DAILY_ACTIVITY, "r") as qry:
        query_start = time.perf_counter()
        cmd = qry.read().format(STAGE_URL=stage_url)
        context.log.info(f"Query:\n{cmd}")
        context.log.info(f"Params:\n{params}")
        _ = duck.execute(cmd, params=params, fetch_all=False)
        query_end = time.perf_counter()

    return MaterializeResult(
        metadata={
            "stage_url": MetadataValue.text(stage_url),
            "origin": MetadataValue.text(data_origin),
            "target_date": MetadataValue.text(target_date),
            "query_duration_seconds": MetadataValue.float(
                round(query_end - query_start, 0)
            ),
        },
    )


@asset(
    partitions_def=rollup_partition,
    non_argument_deps={"staged_rollup"},
    backfill_policy=multi_partition_backfill,
)
def rollup_daily(context, snowflake: SnowflakeResource) -> MaterializeResult:
    """
    Load the aggregates into Snowflake via external stage
    """
    context.log.info(context.partition_key.keys_by_dimension)
    datetime_partition = context.partition_key.keys_by_dimension.get("target_datetime")
    _, adjusted_date, target_hour = get_target_datetimes(datetime_partition)
    snowflake = context.resources.snowflake

    snowflake.run_command(
        CREATE_FILE_FORMAT, DATABASE=snowflake.database, SCHEMA=snowflake.schema_name
    )
    snowflake.run_command(
        CREATE_ROLLUP_STAGE,
        DATABASE=snowflake.database,
        STAGE_URL=f"{constants.S3_SNOWFLAKE_STAGE}",
    )
    snowflake.run_command(
        CREATE_ROLLUP_TABLE, DATABASE=snowflake.database, SCHEMA=snowflake.schema_name
    )

    snowflake.copy_into(
        COPY_INTO_SNOWFLAKE,
        DATABASE=snowflake.database,
        SCHEMA=snowflake.schema_name,
        TARGET_DATE=adjusted_date,
        TARGET_HOUR=target_hour,
    )
    return MaterializeResult(
        metadata={
            "datetime_partition": MetadataValue.text(datetime_partition),
            "adjusted_date": MetadataValue.text(adjusted_date),
            "target_hour": MetadataValue.text(target_hour),
        }
    )
