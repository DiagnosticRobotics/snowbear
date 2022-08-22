import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from snowbear.dataframes import Session, SqliteSession, col, functions

fallback_url = "sqlite://"
database_urls = [fallback_url]
database_names = ["sqlite"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_simple_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    df = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )

    with session.create_temp_dataset(df) as test_table:
        pd.testing.assert_frame_equal(df, test_table.to_pandas(), check_dtype=False)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_filter_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    source = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )
    expected = pd.DataFrame(
        np.array([[4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )
    with session.create_temp_dataset(source) as test_table:
        df = test_table.where(test_table.b > 5)
        pd.testing.assert_frame_equal(expected, df.to_pandas(), check_dtype=False)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_limit_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    source = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )
    expected = pd.DataFrame(np.array([[1, 2.3, "A"]]), columns=["a", "b", "c"])
    with session.create_temp_dataset(source) as test_table:
        df = test_table.limit(1)
        pd.testing.assert_frame_equal(expected, df.to_pandas(), check_dtype=False)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_partials_selectors_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    source = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )
    expected = pd.DataFrame(
        np.array([["A", 1, "jack"], ["B", 4, "jack"], ["B", 7, "jack"]]),
        columns=["c", "a_tag", "name"],
    )
    with session.create_temp_dataset(source) as test_table:
        df = test_table.rename("a", "a_tag").drop_column("b").with_column(name="jack")
        pd.testing.assert_frame_equal(expected, df.to_pandas(), check_dtype=False)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_groupby_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    source = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )
    expected = pd.DataFrame(np.array([["A", 2.3], ["B", 13.7]]), columns=["c", "sum"])
    expected["sum"] = expected["sum"].astype("float")
    with session.create_temp_dataset(source) as test_table:
        df = test_table.groupby(col("c")).aggregate(sum=functions.Sum(test_table.b))
        pd.testing.assert_frame_equal(
            expected, df.to_pandas(), check_dtype=False, check_exact=False, atol=0.001
        )