from dagster import ConfigurableResource, get_dagster_logger
import snowflake.connector


LOGGER = get_dagster_logger()


class SnowflakeResource(ConfigurableResource):
    account: str
    user: str
    password: str
    database: str
    warehouse: str
    role: str
    schema_name: str

    def credentials(self):
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "warehouse": self.warehouse,
            "role": self.role,
            "schema": self.schema_name,
        }

    def get_connection_string(self) -> str:
        return f"snowflake://{self.user}:{self.password}@{self.account}/{self.database}?warehouse={self.warehouse}&role={self.role}&schema={self.schema_name}"

    def get_connection(self):
        return snowflake.connector.connect(**self.credentials())

    def stage_file(self, filepath: str) -> None:
        """We dont actually use this, but here's another example of what you could do"""
        cmd = f"PUT file://{filepath} @staging.dagster.rollups/{filepath} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
        with snowflake.get_connection() as conn:
            LOGGER.info(cmd)
            res = conn.cursor().execute(cmd)
            LOGGER.info(res.fetchall())
        return
    
    def run_command(self, query_filepath: str, **kwargs) -> None:
        """Given a filepath, run the SQL command in that file"""
        text = open(query_filepath)
        with self.get_connection() as conn:
            cmd = text.read().format(**kwargs) if kwargs else text.read()
            LOGGER.info(cmd)
            conn.cursor().execute(cmd)
        conn.close()

    def copy_into(self, filepath: str, **kwargs) -> None:
        LOGGER.info(f"Copy from stage to Snowflake\nFilepath: {filepath}")
        self.run_command(filepath, **kwargs)


class DatalakeResource(ConfigurableResource):
    """s3 Datalake"""
    bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str