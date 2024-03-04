from lean_data_eng import resources
from lean_data_eng.assets import ALL_ASSETS

from lean_data_eng.jobs import (
    ALL_SCHEDULES,
    core_rollup_job,
)

from dagster import Definitions


defs = Definitions(
    assets=ALL_ASSETS,
    schedules=ALL_SCHEDULES,
    resources=resources.resource_def,
    jobs=[core_rollup_job],
)
