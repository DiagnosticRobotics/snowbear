from __future__ import annotations

import random
import string
from textwrap import indent
from typing import Callable, Union

from snowbear import read_sql_query
from snowbear.dataframes.terms import Term, Field
from snowbear.dataframes.transformations.transformations import SQLTransformation
from snowbear.dataframes.transformations.join_using_transformation import JoinUsingTransformation
from snowbear.dataframes.transformations.set_transformation import SetTransformation
from snowbear.dataframes.transformations.groupby_transformation import GroupbyTransformation
from snowbear.dataframes.transformations.join_transformation import JoinTransformation
from snowbear.dataframes.transformations.select_transformation import SelectTransformation
from snowbear.dataframes.transformations.orderby_transformation import OrderbyTransformation
from snowbear.dataframes.transformations.where_transformation import WhereTransformation
from snowbear.dataframes.transformations.raw_sql_transformation import RawSqlTransformation


class JoinExpression:
    def __init__(self, selectable: SqlDataFrame, other: SqlDataFrame, join_type: str):
        self._selectable = selectable
        self._join_type = join_type
        self._source = selectable
        self._other = other

    def on(self, *args: Term) -> SqlDataFrame:
        transformation = JoinTransformation(
            self._source, self._other, self._join_type, args
        )
        return SqlDataFrame(transformation=transformation, session=self._selectable.session)

    def using(self, field: Field) -> SqlDataFrame:
        transformation = JoinUsingTransformation(
            self._source, self._other, self._join_type, field
        )
        return SqlDataFrame(transformation=transformation, session=self._selectable.session)


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
    def __init__(self, selectable: SqlDataFrame, by: list[Field]):
        self._selectable = selectable
        self._by = by

    def aggregate(
            self, **kwargs: Union[Term, Callable[[SqlDataFrame], Term]]
    ) -> SqlDataFrame:
        items = [
            parse_from_context(k, v, self._selectable) for (k, v) in kwargs.items()
        ]
        transformation = GroupbyTransformation(
            self._selectable, by=self._by, aggs=items
        )
        return SqlDataFrame(transformation=transformation,session=self._selectable.session)


class SqlDataFrame:
    def __init__(self, session: "Session", transformation: SQLTransformation = None):
        letters = string.ascii_uppercase
        self.session = session
        self._alias = "".join(random.choice(letters) for i in range(10))
        self._transformation = transformation

    def get_table_name(self) -> str:
        return self.get_alias_name()

    def get_alias_name(self):
        return self._alias

    def join(self, other: SqlDataFrame) -> JoinExpression:
        return JoinExpression(self, other, "JOIN")

    def left_join(self, other: SqlDataFrame) -> JoinExpression:
        return JoinExpression(self, other, "LEFT JOIN")

    def right_join(self, other: SqlDataFrame) -> JoinExpression:
        return JoinExpression(self, other, "RIGHT JOIN")

    def inner_join(self, other: SqlDataFrame) -> JoinExpression:
        return JoinExpression(self, other, "INNER JOIN")

    def select(
            self,
            **kwargs: Union[
                int, float, str, bool, Term, Field, Callable[[SqlDataFrame], Term]
            ],
    ) -> SqlDataFrame:
        transformation = SelectTransformation(
            self, [parse_from_context(k, v, self) for (k, v) in kwargs.items()]
        )
        return SqlDataFrame(transformation=transformation, session=self.session)

    def where(self, *args: Union[Term, Callable[[SqlDataFrame], Term]]) -> SqlDataFrame:
        transformation = WhereTransformation(
            self, [parse_array_from_context(v, self) for v in args]
        )
        return SqlDataFrame(transformation=transformation, session=self.session)

    def groupby(
            self, *args: Union[Field, Callable[[SqlDataFrame], Field]]
    ) -> GroupbyExpression:
        return GroupbyExpression(
            self, [parse_array_from_context(v, self) for v in args]
        )

    def sql(
            self, sql: str, **kwargs: Union[Term, Callable[[SqlDataFrame], Term]]
    ) -> SqlDataFrame:
        transformation = RawSqlTransformation(
            sql, self, dict([parse_from_context_tuple(k, v, self) for k, v in kwargs])
        )
        return SqlDataFrame(transformation=transformation, session=self.session)

    def union(self, other: SqlDataFrame) -> SqlDataFrame:
        transformation = SetTransformation(self, other, "UNION")
        return SqlDataFrame(transformation=transformation)

    def union_all(self, other: SqlDataFrame) -> SqlDataFrame:
        transformation = SetTransformation(self, other, "UNION ALL")
        return SqlDataFrame(transformation=transformation, session=self.session)

    def intersect(self, other: SqlDataFrame) -> SqlDataFrame:
        transformation = SetTransformation(self, other, "INTERSECT")
        return SqlDataFrame(transformation=transformation, session=self.session)

    def except_of(self, other: SqlDataFrame) -> SqlDataFrame:
        transformation = SetTransformation(self, other, "EXCEPT")
        return SqlDataFrame(transformation=transformation, session=self.session)

    def minus(self, other: SqlDataFrame) -> SqlDataFrame:
        transformation = SetTransformation(self, other, "MINUS")
        return SqlDataFrame(transformation=transformation, session=self.session)

    def order_by(self, *args: Field, direction="ASC") -> SqlDataFrame:
        transformation = OrderbyTransformation(
            self, [parse_array_from_context(v, self) for v in args], direction=direction
        )
        return SqlDataFrame(transformation=transformation, session=self.session)

    def __getattr__(self, name: str) -> Field:
        return Field(name=name, table=self)

    def __getitem__(self, name: str) -> Field:
        return Field(name=name, table=self)

    def to_df(self, chunksize: int = None) -> SqlDataFrame:
        sql = self.to_sql()
        return read_sql_query(sql, con=self.session.connection, chunksize=chunksize)

    def alias(self, alias: str) -> SqlDataFrame:
        self._alias = alias
        return self

    def to_sql(self) -> str:
        deps = self._transformation.get_dependencies()
        tab = "\t"
        cte = ",\n\n".join(
            [f"{dep[0]} AS (\n{indent(dep[1].get_sql(), tab)}\n)" for dep in deps]
        )
        if len(deps)==0:
            return self._transformation.get_sql()

        return f"WITH\n\n{cte}\n\n--final\n{self._transformation.get_sql()}"

    def get_transformation(self):
        return self._transformation
