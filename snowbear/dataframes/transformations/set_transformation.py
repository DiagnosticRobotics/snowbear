from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations


class SetTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        dep_list.extend(extend_transformations(self._other))
        return dep_list

    def __init__(
            self, source: "Selectable", other: "Selectable", set_type: str,
    ) -> None:
        self._source = source

        self._other = other
        self._set_type = set_type

    def get_sql(self):
        return f"SELECT *\nFROM {self._source.get_alias_name}\n{self._set_type}\n{self._other.get_alias_name}"