import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from snowbear import to_sql
from snowbear.dataframes import SqliteSession, functions
from snowbear.dataframes.encoders import OneHotEncoder
from snowbear.dataframes.enums import Order

fallback_url = "sqlite://"
database_urls = [fallback_url]
database_names = ["sqlite"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_to_table(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    df = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )

    test_table = session.create_dataset(df, "test_table")
    table2 = test_table.to_table("test_table_2")
    pd.testing.assert_frame_equal(df, table2.to_pandas(), check_dtype=False)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_inser_into_table(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    df = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )

    test_table = session.create_dataset(df, "test_table")
    table2 = test_table.to_table("test_table_2")
    test_table.insert_into_table("test_table_2")

    assert len(table2.to_pandas()) == len(df) * 2


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_chunked_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    df = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )
    to_sql(df, "test_table", con=connection, index=False)

    test_table = session.dataset("test_table")

    for chunk in test_table.to_pandas_batches(chunksize=1000):
        assert len(chunk) == 3


def test_union():
    session = SqliteSession(None)
    d1 = session.dataset("test1")
    d2 = session.dataset("test2")
    d3 = session.dataset("test3")
    d4 = d1.where(d1.code > 33).where(d2.name.isin(["k"]))
    res = session.union([d1, d2, d3, d4])
    print(res.to_sql())


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_ohe(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    df = pd.DataFrame(
        np.array(
            [[1, "A", 0], [4, "B", 0.3], [7, "B", 0.1], [7, "A", 0.1], [7, "B", 0.1]]
        ),
        columns=["id", "category", "val"],
    )
    to_sql(df, "test_table", con=connection, index=False)

    test_table = session.dataset("test_table")
    ohe = OneHotEncoder(input_col="category")
    df = ohe.fit_transform(test_table)
    df = df.groupby(df.id).aggregate(
        category_a=functions.Sum(df.category_a), category_b=functions.Sum(df.category_b)
    )
    print(df.to_pandas())


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_dedup(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    df = pd.DataFrame(
        np.array(
            [[1, "A", 0], [4, "B", 0.3], [7, "B", 0.1], [7, "A", 0.1], [7, "B", 0.1]]
        ),
        columns=["id", "category", "val"],
    )
    to_sql(df, "test_table", con=connection, index=False)

    test_table = session.dataset("test_table")
    test_table = test_table.remove_duplicates(
        test_table.category, orderby=test_table.val, direction=Order.asc
    )
    print(test_table.to_sql())
