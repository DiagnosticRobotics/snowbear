from snowbear.dataframes.terms import Term
from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations


class OrderbyTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        return dep_list

    def __init__(self, source, sorters: list[Term], direction: str) -> None:
        self._source = source
        self._sorters = sorters
        self._direction = direction

    def get_sql(self):
        terms = [x.get_sql() + f" {self._direction}" for x in self._sorters]
        return f"SELECT * \nFROM {self._source.get_alias_name()} \nORDER BY {', '.join(terms)}"