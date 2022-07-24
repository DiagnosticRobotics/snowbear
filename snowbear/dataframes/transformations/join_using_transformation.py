from snowbear.dataframes.terms import Field
from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations


class JoinUsingTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        dep_list.extend(extend_transformations(self._other))
        return dep_list

    def __init__(
            self, source: "Selectable", other: "Selectable", join_type: str, using: "Field"
    ) -> None:
        self._source = source
        self._using = using
        self._other = other
        self._join_type = join_type

    def get_sql(self):
        terms = self._using.get_sql()
        return f"SELECT *\nFROM {self._source.get_alias_name()}\n{self._join_type} {self._other.get_alias_name()}\nUSING ({terms})"