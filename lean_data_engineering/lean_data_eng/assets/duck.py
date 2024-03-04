import os
import pathlib
from typing import TypeVar, Optional

import duckdb

DUCK_DB_TEMP_DIR = "/tmp/duck-db-spillover/"
DuckDBInstance = TypeVar("DuckDBInstance", bound="DuckDB")


class DuckDB:
    def __init__(self, **kwargs) -> None:
        self._conn = None

    @classmethod
    def build(cls, *args, **kwargs) -> DuckDBInstance:
        """
        Build a DuckDB instance with specific defaults and settings
        Setup a local temp directory
        Explicitly set memory to ~60% of Dagster Cloud RAM
        """
        if not os.path.exists(DUCK_DB_TEMP_DIR):
            pathlib.Path(DUCK_DB_TEMP_DIR).mkdir(parents=True, exist_ok=True)

        instance = cls(*args, **kwargs)
        instance.conn.query(
            f"""
            install httpfs;
            load httpfs;
            install json; 
            load json;
            set s3_region='{os.environ.get("AWS_REGION", "us-east-1")}';
            set s3_access_key_id='{os.environ.get("AWS_ACCESS_KEY_ID")}';
            set s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY')}';
            
            set enable_progress_bar=false;
            set temp_directory='{DUCK_DB_TEMP_DIR}';
            set memory_limit='10GB';
            """
        )
        return instance

    @property
    def conn(self):
        if self._conn is not None:
            return self._conn
        self._conn = duckdb.connect(":memory:")
        return self._conn

    def execute(
        self, qry: str, params: dict = {}, fetch_all: bool = True
    ) -> Optional[duckdb.duckdb.DuckDBPyRelation]:
        """
        Execute a query with provided named params
        Params expects a dictionary with keys corresponding to the parameters of the query
        qry:    select * from $important_data
        params: {"important_data": "my_table"}
        """
        out = self.conn.execute(qry, parameters=params)
        return out.fetchall() if fetch_all else out
