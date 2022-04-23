from typing import Any, Union
from pypika.dialects import SnowflakeQuery, SQLLiteQuery, SnowflakeCreateQueryBuilder, SnowflakeDropQueryBuilder, \
    SnowflakeQueryBuilder, SQLLiteQueryBuilder
from pypika.queries import QueryBuilder, Query, Table
from sqlalchemy.engine import Engine, Connection

from snowbear import read_sql_query


class DatasetQueryBuilder(QueryBuilder):
    def to_df(self, con: [Engine, Connection]):
        query = self.get_sql()
        return read_sql_query(query, con=con)

    def to_table(self, table: Union[str, Table], con: Union[Engine, Connection], append: bool = False):
        if append:
            insert_query = SQLLiteQuery.into(table).from_(self).select("*")
            print(insert_query.__str__())
            con.execute(insert_query.get_sql())
        else:
            create_query = SQLLiteQuery.create_table(table).as_select(self)
            print(create_query.__str__())
            con.execute(create_query.get_sql())

    def head(self, con: Union[Engine, Connection], limit=5):
        query = self.limit(limit).get_sql()
        return read_sql_query(query, con=con)


class DatasetCreateQueryBuilder(QueryBuilder):

    def execute(self, con: Union[Engine, Connection]):
        con.execute(self.get_sql())


class SnowflakeDatasetQuery(SnowflakeQuery):
    class SnowflakeDatasetQueryBuilder(SnowflakeQueryBuilder, DatasetQueryBuilder):
        pass

    class SnowflakeDatasetCreateQueryBuilder(SnowflakeCreateQueryBuilder, DatasetQueryBuilder):
        pass

    @classmethod
    def _builder(cls, **kwargs: Any) -> "QueryBuilder":
        return SnowflakeDatasetQuery.SnowflakeDatasetQueryBuilder(**kwargs)

    @classmethod
    def create_table(cls, table: Union[str, Table]) -> "SnowflakeCreateQueryBuilder":
        return SnowflakeDatasetQuery.SnowflakeDatasetCreateQueryBuilder().create_table(table)

    @classmethod
    def drop_table(cls, table: Union[str, Table]) -> "SnowflakeDropQueryBuilder":
        return SnowflakeDropQueryBuilder().drop_table(table)


class SQLLiteDatasetQuery(SQLLiteQuery):
    class SQLLiteDatasetQueryBuilder(SQLLiteQueryBuilder, DatasetQueryBuilder,DatasetCreateQueryBuilder):
        pass

    @classmethod
    def _builder(cls, **kwargs: Any) -> "QueryBuilder":
        return SQLLiteDatasetQuery.SQLLiteDatasetQueryBuilder(**kwargs)
