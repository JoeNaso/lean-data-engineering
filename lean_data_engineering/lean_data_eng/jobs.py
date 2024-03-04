"""Jobs and their associated schedules"""

from lean_data_eng.assets import core

from dagster import (
    build_schedule_from_partitioned_job,
    define_asset_job,
)

core_rollup_job = define_asset_job(
    "core_rollup_job",
    description="Postgres/ DuckDB rollup written to Snowflake",
    selection=[
        core.rollup_daily_activity.hive_partitioned_events,
        core.rollup_daily_activity.staged_rollup,
        core.rollup_daily_activity.rollup_daily,
    ],
    partitions_def=core.rollup_daily_activity.rollup_partition,
)
core_rollup_schedule = build_schedule_from_partitioned_job(
    core_rollup_job,
)

ALL_SCHEDULES = [
    core_rollup_schedule,
]
