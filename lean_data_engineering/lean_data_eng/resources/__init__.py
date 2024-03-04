import os
import logging

from dagster import EnvVar, FilesystemIOManager
from dagster_aws.s3 import S3Resource
from dagster_snowflake_pandas import snowflake_pandas_io_manager

from lean_data_eng.resources.databases import (
    DatalakeResource,
    SnowflakeResource,
)

from lean_data_eng import constants

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


snowflake_credentials = {
    "account": EnvVar("SNOWFLAKE_ACCOUNT"),
    "user": EnvVar("SNOWFLAKE_USER"),
    "password": EnvVar("SNOWFLAKE_PASSWORD"),
    "database": EnvVar("SNOWFLAKE_DATABASE"),
    "warehouse": EnvVar("SNOWFLAKE_WAREHOUSE"),
    "role": EnvVar("SNOWFLAKE_ROLE"),
    "schema_name": os.environ.get("SNOWFLAKE_SCHEMA", "DAGSTER"),
}

datalake_credentials = {
    "bucket": constants.S3_DATALAKE_BUCKET,
    "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
}

resources = {
    "local": {
        "snowflake": SnowflakeResource(**snowflake_credentials),
        "snowflake_io_manager": snowflake_pandas_io_manager.configured(
            {
                "account": {"env": "SNOWFLAKE_ACCOUNT"},
                "user": {"env": "SNOWFLAKE_USER"},
                "password": {"env": "SNOWFLAKE_PASSWORD"},
                "database": {"env": "SNOWFLAKE_DATABASE"},
                "warehouse": {"env": "SNOWFLAKE_WAREHOUSE"},
                "role": {"env": "SNOWFLAKE_ROLE"},
            }
        ),
        "s3_raw_activity": S3Resource(
            region_name="us-east-1",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        ),
        "filesystem_manager": FilesystemIOManager(base_dir="data")
    },
    "production": {
        "snowflake": SnowflakeResource(**snowflake_credentials),
        "snowflake_io_manager": snowflake_pandas_io_manager.configured(
            {
                "account": {"env": "SNOWFLAKE_ACCOUNT"},
                "user": {"env": "SNOWFLAKE_USER"},
                "password": {"env": "SNOWFLAKE_PASSWORD"},
                "database": {"env": "SNOWFLAKE_DATABASE"},
                "warehouse": {"env": "SNOWFLAKE_WAREHOUSE"},
                "role": {"env": "SNOWFLAKE_ROLE"},
            }
        ),
        "s3_raw_activity": S3Resource(
            region_name="us-east-1",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        ),
        "filesystem_manager": FilesystemIOManager(base_dir="data")
    },
}
deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")

resource_def = resources[deployment_name]
