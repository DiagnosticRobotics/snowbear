from __future__ import annotations

from contextlib import contextmanager
from typing import List

import pandas
from sqlalchemy.engine import Connection

from snowbear import read_sql_query, temporary_dataframe_table, to_sql
from snowbear.dataframes.sql_dataframe import DataFrame, Dataset
from snowbear.dataframes.transformations.raw_sql_transformation import (
    RawSqlTransformation,
)
from snowbear.dataframes.transformations.set_transformation import SetTransformation


class Session:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.dialect = "snowflake"
        self.QUOTE_CHAR = None
        self.ALIAS_QUOTE_CHAR = '"'
        self.QUERY_ALIAS_QUOTE_CHAR = ""

    def get_kwargs_defaults(self) -> None:
        kwargs = {}
        kwargs.setdefault("quote_char", self.QUOTE_CHAR)
        kwargs.setdefault("dialect", self.dialect)
        return kwargs

    def dataset(self, name: str, schema: str = None) -> Dataset:
        return Dataset(name=name, schema=schema, session=self)

    def sql(self, query: str) -> DataFrame:
        return DataFrame(self, RawSqlTransformation(query))

    def union(self, dataframes: List[DataFrame]) -> DataFrame:
        transformation = SetTransformation(dataframes, "UNION")
        return DataFrame(transformation=transformation, session=self)

    def union_all(self, dataframes: List[DataFrame]) -> DataFrame:
        transformation = SetTransformation(dataframes, "UNION ALL")
        return DataFrame(transformation=transformation, session=self)

    @contextmanager
    def create_temp_dataset(self, dataframe: pandas.DataFrame) -> Dataset:
        with temporary_dataframe_table(dataframe, self.connection) as table_name:
            return Dataset(name=table_name, session=self)

    def create_dataset(
        self, dataframe: pandas.DataFrame, name: str, schema: str = None
    ) -> Dataset:
        dataset = Dataset(name=name, schema=schema, session=self)
        to_sql(
            dataframe,
            dataset.get_alias_name,
            self.connection,
            if_exists="append",
            index=False,
        )
        return dataset

    def query(self, sql: str) -> pandas.DataFrame:
        return read_sql_query(sql, self.connection)
