import uuid
from contextlib import contextmanager
from typing import Iterable, Union

import pandas as pd
import sqlalchemy
from pandas import DataFrame
from pandas.core.generic import bool_t
from pandas.io.sql import get_schema
from snowflake.connector.options import pandas
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy.engine import Engine, Connection

DEFAULT_UPLOAD_CHUNK_SIZE = 200_000


def pd_writer(
    table: pandas.io.sql.SQLTable,
    conn: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
    keys: Iterable,
    data_iter: Iterable,
    quote_identifiers: bool = False,
) -> None:
    sf_connection = conn.connection.connection
    df = pandas.DataFrame(data_iter, columns=keys)
    write_pandas(
        chunk_size=DEFAULT_UPLOAD_CHUNK_SIZE,
        conn=sf_connection,
        df=df,
        # Note: Our sqlalchemy connector creates tables case insensitively
        table_name=table.name.upper(),
        schema=table.schema,
        quote_identifiers=quote_identifiers,
    )


def _get_dialect(con):
    if isinstance(con, Engine):
        return con.dialect.name
    elif isinstance(con, Connection):
        return con.dialect
    else:
        raise "Cannot detect dialect from object"


def _get_batches(cursor, chunksize):
    """
    snowflake fetch_pandas_batches return batch sizes of it's own choice,
    to conform to the chunksize parameter we split and merge the batches
    into chunksize
    """
    current_batch = None
    for batch in cursor.fetch_pandas_batches():
        if current_batch is None:
            current_batch = batch
        else:
            current_batch = pd.concat([current_batch, batch])
        while len(current_batch) > chunksize:
            batch_subset = current_batch.iloc[0:chunksize].copy()
            current_batch = current_batch.iloc[chunksize:]
            batch_subset.rename(columns=str.lower, inplace=True)
            yield batch_subset
    current_batch.rename(columns=str.lower, inplace=True)
    yield current_batch


def read_sql_query(
    sql: str, con: Engine, index_col=None, coerce_float=True, chunksize=None
) -> pd.DataFrame:
    if isinstance(con.dialect, SnowflakeDialect):
        with con.connect() as connection:
            cursor = connection.connection.cursor()
            cursor.execute(sql)
            if chunksize:
                return _get_batches(cursor, chunksize)
            else:
                df = cursor.fetch_pandas_all()
                df.rename(columns=str.lower, inplace=True)
                return df
    else:
        return pd.read_sql_query(
            sql=sql,
            con=con,
            index_col=index_col,
            coerce_float=coerce_float,
            chunksize=chunksize,
        )


def to_sql(
    df,
    name: str,
    con,
    schema=None,
    if_exists: str = "fail",
    index: bool_t = True,
    index_label=None,
    chunksize=None,
    dtype=None,
    method=None,
) -> None:
    if _get_dialect(con) == "snowflake":
        return df.to_sql(
            name,
            con=con,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=pd_writer,
        )
    else:
        return df.to_sql(
            name,
            con=con,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=method,
        )


def temporary_ids_table(ids: Iterable, connection: Connection, column="ids") -> str:
    dataframe = pd.DataFrame({column: pd.Series(ids)})
    return temporary_dataframe_table(dataframe, connection)


@contextmanager
def temporary_dataframe_table(dataframe: DataFrame, connection: Connection) -> str:
    temp_table_name = f"tmp_{uuid.uuid4().hex}".lower()
    dataframe.reset_index(drop=True, inplace=True)
    table_frame = dataframe
    create_statement = get_schema(table_frame, name=temp_table_name, con=connection)
    create_statement = create_statement.replace(
        "CREATE TABLE", "CREATE TEMPORARY TABLE"
    )

    connection.execute(create_statement)
    to_sql(table_frame, temp_table_name, connection, if_exists="append", index=False)
    yield temp_table_name
    connection.execute(f"DROP TABLE {temp_table_name}")
