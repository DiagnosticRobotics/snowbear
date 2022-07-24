from snowbear.dataframes.terms import Term
from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations


class WhereTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        return dep_list

    def __init__(self, source, filters: list[Term]) -> None:
        self._source = source
        self._filters = filters

    def get_sql(self):
        terms = [x.get_sql() for x in self._filters]
        return f"SELECT * \nFROM {self._source.get_alias_name()} \nWHERE {' AND '.join(terms)}"