import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest
from pypika import Table, Field
from sqlalchemy import create_engine

import snowbear as sb

fallback_url = "sqlite:///C:\\sqlitedbs\\database.db"
snowflake_url = os.environ["SNFLK_TEST_URL"]
database_urls = [fallback_url, snowflake_url]
database_names = ["sqlite", "snowflake"]


def get_dataset(type):
    if type == "snowflake":
        return sb.SnowflakeDatasetQuery()
    else:
        return sb.SQLLiteDatasetQuery()


def test_to_df_chunks_snowfalke():
    connection = create_engine(snowflake_url)
    items = 300_303
    collected_items = 0
    for chunk in sb.read_sql_query(f'select seq4() as n from table(generator(rowcount => {items}));',
                                   connection,
                                   chunksize=10_000):
        collected_items += len(chunk)
        assert len(chunk) <= 10_000
    assert collected_items==items


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_to_df(database):
    connection = create_engine(database)
    try:
        df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                          columns=['a', 'b', 'c'])
        sb.to_sql(df, "test_table", con=connection, index=False)
        test_table = Table("test_table")
        df2 = sb.SnowflakeDatasetQuery().from_("test_table").select(test_table.star).to_df(connection)
        pd.testing.assert_frame_equal(df, df2, check_dtype=False)
    finally:
        connection.execute("DROP TABLE test_table")


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_head(database):
    connection = create_engine(database)
    try:
        df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                          columns=['a', 'b', 'c'])
        sb.to_sql(df, "test_table", con=connection, index=False)
        test_table = Table("test_table")
        df2 = sb.SnowflakeDatasetQuery().from_("test_table").select(test_table.star).head(connection, 2)
        pd.testing.assert_frame_equal(df.head(2), df2, check_dtype=False)
    finally:
        connection.execute("DROP TABLE test_table")
