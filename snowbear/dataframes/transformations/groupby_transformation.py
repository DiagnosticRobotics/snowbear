from textwrap import indent

from snowbear.dataframes.terms import Field, Term
from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations, TAB


class GroupbyTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        return dep_list

    def __init__(self, source, by: list[Field], aggs: list[Term]) -> None:
        self._source = source
        self._by = by
        self._aggs = aggs

    def get_sql(self):
        select_terms = [x.get_sql(with_alias=True) for x in self._by] + [x.get_sql(with_alias=True) for x in self._aggs]
        terms = indent(',\n'.join(select_terms), TAB)
        by_terms = indent(',\n'.join([x.get_sql() for x in self._by]), TAB)
        return f"SELECT\n{terms}\nFROM {self._source.get_alias_name()}\nGROUP BY\n{by_terms}"