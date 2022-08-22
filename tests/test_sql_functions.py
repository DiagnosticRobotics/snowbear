import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

import snowbear as sb

fallback_url = "sqlite:///C:\\sqlitedbs\\database.db"
snowflake_url = os.environ["SNFLK_TEST_URL"]
database_urls = [fallback_url, snowflake_url]
database_names = ["sqlite", "snowflake"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_to_sql_new_dataset(database):
    connection = create_engine(database)
    try:
        df = pd.DataFrame(
            np.array([[1, 2.3, "A"], [4, 5.7, "B"], [7, 8.0, "B"]]),
            columns=["a", "b", "c"],
        )
        sb.to_sql(df, "test_table", con=connection, index=False)
        df2 = sb.read_sql_query("select * from test_table", con=connection)
        pd.testing.assert_frame_equal(df, df2, check_dtype=False)
    finally:
        connection.execute("DROP TABLE test_table")


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_temporary_dataframe_table_dataframe_available_for_query(database):
    engine = create_engine(database)
    df = pd.DataFrame(
        np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]), columns=["a", "b", "c"]
    )
    with engine.connect() as connection, sb.temporary_dataframe_table(
        df, connection
    ) as x:
        df2 = sb.read_sql_query(f"select * from {x}", con=connection)
        pd.testing.assert_frame_equal(df, df2, check_dtype=False)
    with pytest.raises(Exception):
        sb.read_sql_query(f"select * from {x}", con=engine)


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_temporary_list_table_available_for_query(database):
    engine = create_engine(database)
    with engine.connect() as connection, sb.temporary_ids_table(
        [1, 2, 3, 4], connection
    ) as x:
        df2 = sb.read_sql_query(f"select * from {x}", con=connection)
        assert list(df2["ids"]) == [1, 2, 3, 4]
    with pytest.raises(Exception):
        sb.read_sql_query(f"select * from {x}", con=engine)
