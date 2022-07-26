from typing import Sequence

from snowbear.dataframes.terms import Term
from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations


class JoinTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        dep_list.extend(extend_transformations(self._other))
        return dep_list

    def __init__(
            self,
            source: "Selectable",
            other: "Selectable",
            join_type: str,
            on: Sequence[Term],
    ) -> None:
        self._source = source
        self._on = on
        self._other = other
        self._join_type = join_type

    def get_sql(self):
        terms = [x.get_sql() for x in self._on]
        on_term = 'AND\n'.join(terms)
        return f"SELECT *\nFROM {self._source.get_alias_name()}\n{self._join_type} {self._other.get_alias_name()}\nON {on_term}"