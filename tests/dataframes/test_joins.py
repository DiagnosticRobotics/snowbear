import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from snowbear.dataframes import Session, col, functions, SqliteSession

fallback_url = "sqlite://"
database_urls = [fallback_url]
database_names = ["sqlite"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_left_join_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    left = pd.DataFrame(
        np.array([['1', 'shani'], ['2', 'lior'], ['3', 'itay'], ['4', 'meori']]), columns=["id", "name"]
    )
    right = pd.DataFrame(
        np.array([['1', 'developer'], ['2', 'developer'], ['3', 'developer']]), columns=["id", "role"]
    )

    expected = pd.DataFrame(
        np.array([['1', 'shani','developer'], ['2', 'lior','developer'], ['3', 'itay','developer'], ['4', 'meori',None]]), columns=["id", "name","role"]
    )

    with session.create_temp_dataset(left) as left_table,session.create_temp_dataset(right) as right_table:
        joined = left_table.left_join(right_table).on(left_table.id==right_table.id)
        pd.testing.assert_frame_equal(
            expected, joined.to_pandas(), check_dtype=False, check_exact=False, atol=0.001
        )

@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_inner_join_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    left = pd.DataFrame(
        np.array([['1', 'shani'], ['2', 'lior'], ['3', 'itay'], ['4', 'meori']]), columns=["id", "name"]
    )
    right = pd.DataFrame(
        np.array([['1', 'developer'], ['2', 'developer'], ['3', 'developer']]), columns=["id", "role"]
    )

    expected = pd.DataFrame(
        np.array([['1', 'shani','developer'], ['2', 'lior','developer'], ['3', 'itay','developer']]), columns=["id", "name","role"]
    )

    with session.create_temp_dataset(left) as left_table,session.create_temp_dataset(right) as right_table:
        joined = left_table.inner_join(right_table).on(left_table.id==right_table.id)
        pd.testing.assert_frame_equal(
            expected, joined.to_pandas(), check_dtype=False, check_exact=False, atol=0.001
        )