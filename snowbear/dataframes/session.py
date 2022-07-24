from __future__ import annotations

from contextlib import contextmanager

from pandas import DataFrame
from snowflake.connector.options import pandas
from sqlalchemy.engine import Connection

from snowbear import temporary_dataframe_table, read_sql_query
from snowbear.dataframes.sql_dataframe import SqlDataFrame


class Dataset(SqlDataFrame):
    def get_alias_name(self):
        return self._name

    def __init__(self, name: str, schema: str = None, session: Session = None):
        super().__init__(session)
        self._name = name
        self._schema = schema

class Session:
    def __init__(self, connection: Connection):
        self.connection = connection

    def dataset(self, name: str, schema: str = None) -> Dataset:
        return Dataset(name=name, schema=schema, session=self)

    @contextmanager
    def temp_dataset(self, dataframe: DataFrame) -> Dataset:
        with temporary_dataframe_table(dataframe, self.connection) as table_name:
            return Dataset(name=table_name, session=self)

    def query(self, sql: str) -> pandas.DataFrame:
        return read_sql_query(sql, self.connection)

