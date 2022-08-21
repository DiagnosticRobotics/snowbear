from __future__ import annotations

import random
import string
import typing
from textwrap import indent
from typing import Callable, Generator, List, Union

import pandas

if typing.TYPE_CHECKING:
    from snowbear.dataframes import Session

from snowbear.sql import read_sql_query
from snowbear.dataframes import analytics
from snowbear.dataframes.enums import Order
from snowbear.dataframes.terms import Field, Term
from snowbear.dataframes.transformations.dataframe_transformation import (
    DataframeTransformation,
)
from snowbear.dataframes.transformations.raw_sql_transformation import (
    RawSqlTransformation,
)
from snowbear.dataframes.transformations.set_transformation import SetTransformation
from snowbear.dataframes.transformations.transformations import SQLTransformation


def get_or_create_transformation(source: DataFrame) -> DataframeTransformation:
    source_transformation = source.get_transformation()
    if (
        isinstance(source_transformation, DataframeTransformation)
        and not source_transformation.is_sealed()
    ):
        return source_transformation.copy()
    else:
        return DataframeTransformation(source)


class JoinExpression:
    def __init__(self, selectable: DataFrame, other: DataFrame, join_type: str):
        self._selectable = selectable
        self._join_type = join_type
        self._source = selectable
        self._other = other

    def on(self, *args: Term) -> DataFrame:
        transformation = get_or_create_transformation(self._source)
        transformation.add_join(self._other, self._join_type, "ON", args)
        return DataFrame(
            transformation=transformation, session=self._selectable.session
        )

    def using(self, field: Field) -> DataFrame:
        transformation = get_or_create_transformation(self._source)
        transformation.add_join(self._other, self._join_type, "USING", [field])

        return DataFrame(
            transformation=transformation, session=self._selectable.session
        )


def parse_from_context(k, v, selectable):
    if isinstance(v, Callable):
        term = v(selectable)
    else:
        term = v
    return term.as_(k)


def parse_from_context_tuple(k, v, selectable):
    if isinstance(v, Callable):
        term = v(selectable)
    else:
        term = v
    return k, term


def parse_array_from_context(v, selectable):
    if isinstance(v, Callable):
        term = v(selectable)
    else:
        term = v
    return term


class GroupbyExpression:
    def __init__(self, selectable: DataFrame, by: List[Field]):
        self._selectable = selectable
        self._by = by

    def aggregate(
        self, **kwargs: Union[Term, Callable[[DataFrame], Term]]
    ) -> DataFrame:
        items = [
            parse_from_context(k, v, self._selectable) for (k, v) in kwargs.items()
        ]
        transformation = get_or_create_transformation(self._selectable)
        transformation.add_groupby(by=self._by, aggs=items)
        return DataFrame(
            transformation=transformation, session=self._selectable.session
        )


def dedup_by_key(deps):
    seen = set()
    return [(a, b) for a, b in deps if not (a in seen or seen.add(a))]


class DataFrame:
    def __init__(self, session: "Session", transformation: SQLTransformation = None):
        letters = string.ascii_uppercase
        self.session = session
        self._alias = "".join(random.choice(letters) for i in range(10))
        self._transformation = transformation

    def __repr__(self):
        return self.__class__.__name__ + "(" + self.get_alias_name + ")"

    @property
    def get_table_name(self) -> str:
        return self.get_alias_name

    @property
    def get_alias_name(self) -> str:
        return self._alias

    def join(self, other: DataFrame) -> JoinExpression:
        """Performs a left join with the current DataFrame and another DataFrame

        Args:
            other (DataFrame): The DataFrame to join with
        """
        return JoinExpression(self, other, "JOIN")

    def left_join(self, other: DataFrame) -> JoinExpression:
        """Performs a left join with the current DataFrame and another DataFrame

        Args:
            other (DataFrame): The DataFrame to join with
        """
        return JoinExpression(self, other, "LEFT JOIN")

    def right_join(self, other: DataFrame) -> JoinExpression:
        """Performs a right join with the current DataFrame and another DataFrame

        Args:
            other (DataFrame): The DataFrame to join with
        """
        return JoinExpression(self, other, "RIGHT JOIN")

    def inner_join(self, other: DataFrame) -> JoinExpression:
        """Performs an inner join with the current DataFrame and another DataFrame

        Args:
            other (DataFrame): The DataFrame to join with
        """
        return JoinExpression(self, other, "INNER JOIN")

    def qualify(self, qualify: Term) -> DataFrame:
        """Performs a qualify filter on the current DataFrame

        Args:
            qualify (Term): The term to run qualify with
        """
        transformation = get_or_create_transformation(self)
        transformation.add_qualify(qualify)
        return DataFrame(transformation=transformation, session=self.session)

    def remove_duplicates(
        self, keys: List[Field], orderby: Field, direction: Order = Order.asc
    ) -> DataFrame:
        """Removes duplicate rows from the current DataFrame

        Example:
            >>> df.remove_duplicates([df.user_id], orderby=df.update_date, direction=Order.desc)

        Args:
            keys (List[Field]): Keys to detect duplicates by
            orderby (Field): The field to order by
            direction (Order): The direction to order"""
        return self.qualify(
            analytics.RowNumber().over(keys).orderby(orderby, order=direction) == 1
        )

    def limit(self, limit: int) -> DataFrame:
        """Performs a limit filter on the current DataFrame

        Example:
            df.limit(30)

        Args:
            limit (int): The maximum number of rows to return
        """
        transformation = get_or_create_transformation(self)
        transformation.add_limit(limit)
        return DataFrame(transformation=transformation, session=self.session)

    def select(
        self,
        **kwargs: Union[
            int, float, str, bool, Term, Field, Callable[[DataFrame], Term]
        ],
    ) -> DataFrame:
        """Perform a select transformations on the current DataFrame with the specified Column expressions as output

        Example:
            >>> df.select(df.user_id, df.update_date, df.first_name+df.last_name)
            >>> df.select(df.user_id, df["update_date"], df.first_name+df.last_name)
            >>> df.select(col("user_id"), col("update_date"), col("first_name")+col("last_name"))
        Args:
            **kwargs: select terms to
        """
        transformation = get_or_create_transformation(self)
        transformation.add_select(
            [parse_from_context(k, v, self) for (k, v) in kwargs.items()]
        )
        return DataFrame(transformation=transformation, session=self.session)

    def drop_column(self, *args: List[str]) -> DataFrame:
        """
        Returns a new DataFrame that excludes the columns with the specified names
        from the output.
        Args:
            *args: The names of the columns to drop.
        Example:
            >>> df.drop_column('temp_column','temp_column_2')
        """
        transformation = get_or_create_transformation(self)
        columns = [
            self[column].as_(column) for column in self.columns() if column not in args
        ]
        transformation.add_select(columns)

        return DataFrame(transformation=transformation, session=self.session)

    def with_column(
        self,
        **kwargs: Union[
            int, float, str, bool, Term, Field, Callable[[DataFrame], Term]
        ],
    ) -> DataFrame:
        """
        Returns a DataFrame with additional columns.
        Args:
            **kwargs: Columns to be included in the output.
        Example:
            >>> dataframe.with_column(max_age=functions.Max(dataframe.age))
        """
        transformation = get_or_create_transformation(self)
        columns = [self[column].as_(column) for column in self.columns()]
        columns = columns + [
            parse_from_context(k, v, self) for (k, v) in kwargs.items()
        ]
        transformation.add_select(columns)

        return DataFrame(transformation=transformation, session=self.session)

    def rename(self, column: Union[str, Field], new_name: str) -> DataFrame:
        """
        Renames a column.
        Args:
            column: Column to be renamed.
            new_name: New name for the column.
        Example:
            >>> df.rename("phone","phone_number")
        """
        transformation = get_or_create_transformation(self)
        if isinstance(column, str):
            column_to_rename = self[column]
        else:
            column_to_rename = column
        columns = [
            self[column].as_(column)
            for column in self.columns()
            if column != column_to_rename.name
        ]
        columns = columns + [column_to_rename.as_(new_name)]
        transformation.add_select(columns)

        return DataFrame(transformation=transformation, session=self.session)

    def columns(self) -> List[str]:
        """
        Returns all column names of a dataframe.
        Note:
            This operation will perform a LIMIT 0 query to infer the columns of the query.
        Returns:
            List of columns.
        """
        meta_dataframe = self.limit(0).to_pandas()
        return list(meta_dataframe.columns.values)

    def where(self, *args: Union[Term, Callable[[DataFrame], Term]]) -> DataFrame:
        """
        Filters rows based on the specified conditional expression

        Example:
            >>> df.where(df.phone_number == "555-0559-2993")
            >>> df.where(df.age > 18)
        Args:
            *args: Expressions to filter by
        """
        transformation = get_or_create_transformation(self)
        transformation.add_filter(
            filters=[parse_array_from_context(v, self) for v in args]
        )
        return DataFrame(transformation=transformation, session=self.session)

    def groupby(
        self, *args: Union[Field, Callable[[DataFrame], Field]]
    ) -> GroupbyExpression:
        """
        Groups rows by the specified columns.
        Example:
            >>> df.groupby(df.name).aggregate(functions.Average(df.age))
        Args:
            *args: Fields to group by
        """
        return GroupbyExpression(
            self, [parse_array_from_context(v, self) for v in args]
        )

    def sql(
        self, sql: str, **kwargs: Union[Term, Callable[[DataFrame], Term]]
    ) -> DataFrame:
        """Performs a custom SQL query over a dataframe."""
        transformation = RawSqlTransformation(
            sql, self, dict([parse_from_context_tuple(k, v, self) for k, v in kwargs])
        )
        return DataFrame(transformation=transformation, session=self.session)

    def union(self, other: DataFrame) -> DataFrame:
        """
        Concatenates two DataFrames.
        Example:
            >>> df.union(df.name)
        Args:
            other: dataframe to be union
        """
        transformation = SetTransformation([self, other], "UNION")
        return DataFrame(transformation=transformation, session=self.session)

    def union_all(self, other: DataFrame) -> DataFrame:
        transformation = SetTransformation([self, other], "UNION ALL")
        return DataFrame(transformation=transformation, session=self.session)

    def intersect(self, other: DataFrame) -> DataFrame:
        """
        Intersects two DataFrames.
        Example:
            >>> df.intersect(df.name)
        Args:
            other: dataframe to be intersect
        """
        transformation = SetTransformation([self, other], "INTERSECT")
        return DataFrame(transformation=transformation, session=self.session)

    def except_of(self, other: DataFrame) -> DataFrame:
        transformation = SetTransformation([self, other], "EXCEPT")
        return DataFrame(transformation=transformation, session=self.session)

    def minus(self, other: DataFrame) -> DataFrame:
        transformation = SetTransformation([self, other], "MINUS")
        return DataFrame(transformation=transformation, session=self.session)

    def order_by(self, *args: Field, direction: Order = Order.asc) -> DataFrame:
        """
        Sorts a dataframe by the given fields.
        Example:
            >>> df.order_by(df.age)
            >>> df.order_by(df.age, direction=Order.desc)
        Args:
            *args: fields to be sorted by
            direction: direction to sort by

        """
        transformation = get_or_create_transformation(self)
        transformation.add_orderby(
            [([parse_array_from_context(v, self) for v in args], direction)]
        )
        return DataFrame(transformation=transformation, session=self.session)

    def __getattr__(self, name: str) -> Field:
        return Field(name=name, table=self)

    def __getitem__(self, name: str) -> Field:
        return Field(name=name, table=self)

    def to_pandas(self) -> pandas.DataFrame:
        sql = self.to_sql()
        return self.session.query(sql)

    def to_pandas_batches(self, chunksize: int) -> Generator[pandas.DataFrame]:
        sql = self.to_sql()
        return read_sql_query(sql, con=self.session.connection, chunksize=chunksize)

    def to_table(self, name: str, schema: str = None) -> "Dataset":
        dataset = Dataset(name=name, schema=schema, session=self.session)
        sql = self.to_sql()
        create_sql = f"CREATE TABLE {dataset.get_alias_name} AS  {sql} "
        self.session.connection.execute(create_sql)
        return dataset

    def insert_into_table(self, name: str, schema: str = None) -> "Dataset":
        dataset = Dataset(name=name, schema=schema, session=self.session)
        sql = self.to_sql()
        create_sql = f"INSERT INTO {dataset.get_alias_name} {sql} "
        self.session.connection.execute(create_sql)
        return dataset

    def alias(self, alias: str) -> DataFrame:
        self._alias = alias
        return self

    def to_sql(self) -> str:
        """
        Executes the query DataFrame and returns the result as a Pandas DataFrame.
        """
        deps = self._transformation.get_dependencies()
        deps = dedup_by_key(deps)
        tab = "\t"
        cte = ",\n\n".join(
            [f"{dep[0]} AS (\n{indent(dep[1].get_sql(), tab)}\n)" for dep in deps]
        )
        if len(deps) == 0:
            return self._transformation.get_sql()

        return f"WITH\n\n{cte}\n\n--final\n{self._transformation.get_sql()}"

    def get_transformation(self):
        return self._transformation


class Dataset(DataFrame):
    def to_sql(self) -> str:
        return f"SELECT * FROM {self.get_alias_name}"

    @property
    def get_alias_name(self):
        table_sql = self._name

        if self._schema is not None:
            table_sql = "{schema}.{table}".format(schema=self._schema, table=table_sql)
        return table_sql

    def __init__(self, name: str, schema: str = None, session: "Session" = None):
        super().__init__(session)
        self._name = name
        self._schema = schema
