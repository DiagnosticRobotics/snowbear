import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from snowbear import to_sql
from snowbear.dataframes import Session, col, functions, SqliteSession
from snowbear.dataframes.functions import Cast
from snowbear.dataframes.terms import ValueWrapper

fallback_url = "sqlite://"
database_urls = [fallback_url]
database_names = ["sqlite"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_simple_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    source = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )

    with session.create_temp_dataset(source) as test_table:
        df = session.sql(f"SELECT * FROM {test_table.get_table_name}")
        pd.testing.assert_frame_equal(
            source, df.to_pandas(), check_dtype=False, check_exact=False, atol=0.001
        )


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_chained_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    source = pd.DataFrame(
        np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]), columns=["a", "b", "c"]
    )

    with session.create_temp_dataset(source) as test_table:
        df = test_table.sql("SELECT * FROM {{{source}}}")
        pd.testing.assert_frame_equal(
            source, df.to_pandas(), check_dtype=False, check_exact=False, atol=0.001
        )
