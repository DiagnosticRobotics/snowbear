from textwrap import indent
from typing import List

from snowbear.dataframes.terms import Term
from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations, TAB


class SelectTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        return dep_list

    def __init__(self, source, selectors: List[Term]) -> None:
        self._source = source
        self._selectors = selectors

    def get_sql(self):
        terms = [x.get_sql(with_alias=True) for x in self._selectors]
        selectors = indent(',\n'.join(terms), TAB)
        return f"SELECT\n{selectors}\nFROM {self._source.get_alias_name()}"