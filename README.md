# Snowbear 🐻‍❄️
snowbear is a utility library that helps integrating pandas with snowflake efficiently. all snowbear functions implement semaless fallback to pandas to aloow you to test you code on sqlite, postgres or any database of your choosing using the same code you'll use for snowflake.

![Python Versions Supported](https://img.shields.io/badge/python-3.7+-blue.svg)

## Installation
snowbear is published to the Python Package Index (PyPI) under the name snowbear. To install it, run:

``` shell
pip install snowbear
```

## Quick Start

### read_sql_query
read_sql_query is a drop in replacement to pandas read_sql_query, it is implemented to fetch quickly large datasets using snowflake's fetch_pandas_all

```python
import snowbear as sb
df = sb.read_sql_query("SELECT * FROM test_table", con=engine)
```

### to_sql
to_sql is a drop in replacement to pandas to_sql, it is implemented to upload large datasets using snowflake's pd_writer and staging mechanism

```python
import snowbear as sb
sb.to_sql(df, "test_table", con=connection)
```

### with temporary_dataframe_table
temporary_dataframe_table is an helper method that helps you join snowflake data over a local dataframe by creating a temporary table on snowflake

```python
import snowbear as sb
with engine.connect() as connection, \
    sb.temporary_dataframe_table(df, connection) as temp_table:
    result_df = sb.read_sql_query(f"SELECT * FROM test_table JOIN {temp_table} USING (id)", engine=connection)
```

### with temporary_ids_table
temporary_ids_table is an helper method that helps you join snowflake data over a local id list by creating a temporary table on snowflake

```python
import snowbear as sb
with engine.connect() as connection, \
    sb.temporary_ids_table([1,2,3,4,5], connection, column = 'ids') as temp_table:
    result_df = sb.read_sql_query(f"SELECT * FROM test_table JOIN {temp_table} USING (id)", engine=connection)
```

### DatasetQuery
DatasetQuery is wrapper over pypika, adding to_df() and preview() function

```python
import snowbear as sb
result_df = sb.SnowflakeDatasetQuery().from_("test_table").select("*").to_df(connection)
top_5 = sb. SnowflakeDatasetQuery().from_("test_table").select("*").head(connection)
```

## How to contribute

Have any feedback? Wish to implement an extenstion or new capability? 
Every contribution to _snowbear_ is greatly appreciated.

## Acknowledgements
terms and expression capabilities in sql datasets were copied and modified from Pypika in accordance to Apache License, Version 2.0