from __future__ import annotations

from contextlib import contextmanager
from typing import List

from pandas import DataFrame
from snowflake.connector.options import pandas
from sqlalchemy.engine import Connection

from snowbear import temporary_dataframe_table, read_sql_query, to_sql
from snowbear.dataframes.sql_dataframe import Dataset, SqlDataFrame
from snowbear.dataframes.transformations.set_transformation import SetTransformation


class Session:
    def __init__(self, connection: Connection):
        self.connection = connection

    def dataset(self, name: str, schema: str = None) -> Dataset:
        return Dataset(name=name, schema=schema, session=self)

    def union(self, dataframes: List[SqlDataFrame]) -> SqlDataFrame:
        transformation = SetTransformation(dataframes, "UNION")
        return SqlDataFrame(transformation=transformation, session=self)

    def union_all(self, dataframes: List[SqlDataFrame]) -> SqlDataFrame:
        transformation = SetTransformation(dataframes, "UNION ALL")
        return SqlDataFrame(transformation=transformation, session=self)

    @contextmanager
    def create_temp_dataset(self, dataframe: DataFrame) -> Dataset:
        with temporary_dataframe_table(dataframe, self.connection) as table_name:
            return Dataset(name=table_name, session=self)

    def create_dataset(self, dataframe: DataFrame, name: str, schema: str = None) -> Dataset:
        dataset = Dataset(name=name, schema=schema, session=self)
        to_sql(dataframe, dataset.get_alias_name(), self.connection, if_exists="append", index=False)
        return dataset

    def query(self, sql: str) -> pandas.DataFrame:
        return read_sql_query(sql, self.connection)
