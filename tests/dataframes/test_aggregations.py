import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from snowbear.dataframes import SqliteSession
from snowbear.dataframes.functions import Count, IsNull

fallback_url = "sqlite://"
database_urls = [fallback_url]
database_names = ["sqlite"]


@pytest.mark.parametrize("database", database_urls, ids=database_names)
def test_groupby_agg_query(database):
    connection = create_engine(database)
    session = SqliteSession(connection)

    left = pd.DataFrame(
        np.array([["1", "shani"], ["2", "lior"], ["3", "itay"], ["4", "meori"]]),
        columns=["id", "name"],
    )
    right = pd.DataFrame(
        np.array([["1", "developer"], ["2", "developer"], ["3", "developer"]]),
        columns=["id", "role"],
    )

    expected = pd.DataFrame(np.array([["developer", 3]]), columns=["role", "sum"])
    expected["sum"] = expected["sum"].astype(int)

    with session.create_temp_dataset(left) as left_table, session.create_temp_dataset(
        right
    ) as right_table:
        joined = (
            left_table.left_join(right_table)
            .on(left_table.id == right_table.id)
            .groupby(right_table.role)
            .aggregate(sum=Count(left_table.id))
            .where(lambda x: x.role.notnull())
        )
        print(joined.to_pandas())
        pd.testing.assert_frame_equal(
            expected,
            joined.to_pandas(),
            check_dtype=False,
            check_exact=False,
            atol=0.001,
        )
