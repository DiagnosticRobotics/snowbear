from typing import List

from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations


class MergeTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        for source in self._source:
            dep_list.extend(extend_transformations(source))
        return dep_list

    def __init__(
            self, source: List["SqlDataFrame"], fields: List["Field"]
    ) -> None:
        self._source = source
        self._fields = fields

    def get_sql(self):
        sql = f"SELECT {}\n"+\
        f"FROM {_source.get_alias_name()}\n"+\
        '\n'.join(_others.get_alias_name())+'\n'

        .join([source.get_alias_name() for source in self._source])
        return f"\n{self._set_type}\n".join([source.get_alias_name() for source in self._source])
