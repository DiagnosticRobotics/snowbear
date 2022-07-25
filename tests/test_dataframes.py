import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from snowbear import to_sql
from snowbear.dataframes import functions

from snowbear.dataframes import Session

fallback_url = "sqlite:///C:\\sqlitedbs\\database.db"
snowflake_url = os.environ.get("SNFLK_TEST_URL")
database_urls = [fallback_url, snowflake_url]
database_names = ["sqlite", "snowflake"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_aggregate_query(database):
    try:
        connection = create_engine(database)
        session = Session(connection)

        df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                          columns=['a', 'b', 'c'])
        to_sql(df, "test_table", con=connection, index=False)

        test_table = session.dataset("test_table")

        aggs = test_table \
            .select(a=test_table.a, b=test_table.b, n=test_table.c) \
            .groupby(lambda x: x.n) \
            .aggregate(a_sum=lambda x: functions.Sum(x.a),
                       b_sum=lambda x: functions.Sum(x.b))

        result_df = aggs.to_df()
        assert result_df['b_sum'][1] == 13.7
    finally:
        connection.execute("DROP TABLE test_table")


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_simple_query(database):
    try:
        connection = create_engine(database)
        session = Session(connection)

        df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                          columns=['a', 'b', 'c'])
        to_sql(df, "test_table", con=connection, index=False)

        test_table = session.dataset("test_table")

        pd.testing.assert_frame_equal(df, test_table.to_df(), check_dtype=False)

    finally:
        connection.execute("DROP TABLE test_table")



@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_chunked_query(database):
    try:
        connection = create_engine(database)
        session = Session(connection)

        df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                          columns=['a', 'b', 'c'])
        to_sql(df, "test_table", con=connection, index=False)

        test_table = session.dataset("test_table")

        for chunk in test_table.to_df(chunksize=1000):
            assert len(chunk) == 3

    finally:
        connection.execute("DROP TABLE test_table")