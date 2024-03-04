import os

SQL_DIR = os.path.join(os.sep, os.path.dirname(os.path.abspath(__file__)), "sql")
SQL_AGGREGATES_DIR = os.path.join(os.sep, SQL_DIR, "aggregates")
RAW_ACTIVITY_ORIGINS = (
    "prod-app",
    "something-else",
    "a-third-place",
    "why-not-a-fourth"
)
S3_DATALAKE_BUCKET = "s3://your-orgs-datalake/" 
S3_SNOWFLAKE_STAGE = "{BUCKET}{ENV}/dagster-processed/".format(
    BUCKET=S3_DATALAKE_BUCKET,
    ENV=os.environ.get("DAGSTER_DEPLOYMENT", "local")
)

SCRIPTS_DIR = os.path.join(os.sep, os.path.dirname(os.path.abspath(__file__)), "scripts")