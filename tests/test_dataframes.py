import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from snowbear import to_sql
from snowbear.dataframes import functions

from snowbear.dataframes import Session, col
from snowbear.dataframes.encoders import OneHotEncoder

fallback_url = "sqlite://"
database_urls = [fallback_url]
database_names = ["sqlite"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_aggregate_query(database):
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

    result_df = aggs.to_pandas()
    assert result_df['b_sum'][1] == 13.7


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_simple_query(database):
    connection = create_engine(database)
    session = Session(connection)

    df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                      columns=['a', 'b', 'c'])
    to_sql(df, "test_table", con=connection, index=False)

    test_table = session.dataset("test_table")

    pd.testing.assert_frame_equal(df, test_table.to_pandas(), check_dtype=False)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_to_table(database):
    connection = create_engine(database)
    session = Session(connection)

    df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                      columns=['a', 'b', 'c'])

    test_table = session.create_dataset(df, "test_table")
    table2 = test_table.to_table("test_table_2")
    pd.testing.assert_frame_equal(df, table2.to_pandas(), check_dtype=False)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_inser_into_table(database):
    connection = create_engine(database)
    session = Session(connection)

    df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                      columns=['a', 'b', 'c'])

    test_table = session.create_dataset(df, "test_table")
    table2 = test_table.to_table("test_table_2")
    test_table.insert_into_table("test_table_2")

    assert len(table2.to_pandas()) == len(df) * 2


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_chunked_query(database):
    connection = create_engine(database)
    session = Session(connection)

    df = pd.DataFrame(np.array([[1, 2.3, 'A'], [4, 5.7, 'B'], [7, 8.0, 'B']]),
                      columns=['a', 'b', 'c'])
    to_sql(df, "test_table", con=connection, index=False)

    test_table = session.dataset("test_table")

    for chunk in test_table.to_pandas_batches(chunksize=1000):
        assert len(chunk) == 3


def test_sql_builder():
    session = Session(None)
    d1 = session.dataset("test1")
    d2 = session.dataset("test2")
    d3 = session.dataset("test3")
    res = d1 \
        .left_join(d2).on(d1.id == d2.id).groupby(d1.column).aggregate() \
        .left_join(d3).on(d1.id == d3.id) \
        .where(d1.code > 33).where(d2.name.isin(["k"])).select(d1=d1.name, d2=d2.id).groupby(d1.name).aggregate(
        x=lambda x: x.cnt)
    print(res.to_sql())


def test_union():
    session = Session(None)
    d1 = session.dataset("test1")
    d2 = session.dataset("test2")
    d3 = session.dataset("test3")
    d4 = d1.where(d1.code > 33).where(d2.name.isin(["k"]))
    res = session.union([d1,d2,d3,d4])
    print(res.to_sql())