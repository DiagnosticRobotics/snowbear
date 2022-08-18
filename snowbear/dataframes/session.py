from __future__ import annotations

from contextlib import contextmanager
from typing import List

import pandas
from snowflake.connector.options import pandas
from sqlalchemy.engine import Connection

from snowbear import temporary_dataframe_table, read_sql_query, to_sql
from snowbear.dataframes.sql_dataframe import Dataset, DataFrame
from snowbear.dataframes.transformations.raw_sql_transformation import RawSqlTransformation


class Session:
    def __init__(self, connection: Connection):
        self.connection = connection

    def dataset(self, name: str, schema: str = None) -> Dataset:
        return Dataset(name=name, schema=schema, session=self)

    def sql(self, query: str) -> DataFrame:
        return DataFrame(self, RawSqlTransformation(query))

    @staticmethod
    def union(datasets: List[DataFrame]) -> DataFrame:
        it = iter(datasets)
        accumulate = next(it)
        for element in it:
            accumulate = accumulate.union(element)
        return accumulate

    @contextmanager
    def create_temp_dataset(self, dataframe: pandas.DataFrame) -> Dataset:
        with temporary_dataframe_table(dataframe, self.connection) as table_name:
            return Dataset(name=table_name, session=self)

    def create_dataset(self, dataframe: pandas.DataFrame, name: str, schema: str = None) -> Dataset:
        dataset = Dataset(name=name, schema=schema, session=self)
        to_sql(dataframe, dataset.get_alias_name, self.connection, if_exists="append", index=False)
        return dataset

    def query(self, sql: str) -> pandas.DataFrame:
        return read_sql_query(sql, self.connection)

    def get_columns(self, sql: str) -> List[str]:
        return read_sql_query(sql, self.connection).columns

