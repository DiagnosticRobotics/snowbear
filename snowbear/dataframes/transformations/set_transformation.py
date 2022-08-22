import typing
from typing import List

from snowbear.dataframes.transformations.transformations import (
    SQLTransformation, extend_transformations)

if typing.TYPE_CHECKING:
    from snowbear.dataframes import DataFrame


class SetTransformation(SQLTransformation):
    def get_dependencies(self):
        dep_list = []
        for source in self._source:
            dep_list.extend(extend_transformations(source))
        return dep_list

    def __init__(
        self,
        source: List["DataFrame"],
        set_type: str,
    ) -> None:
        self._source = source
        self._set_type = set_type

    def get_sql(self):
        return f"\n{self._set_type}\n".join(
            [source.get_alias_name for source in self._source]
        )
