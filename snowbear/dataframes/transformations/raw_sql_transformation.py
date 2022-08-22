import typing
from abc import abstractmethod
from typing import Dict

from snowbear.dataframes.transformations.transformations import \
    extend_transformations

if typing.TYPE_CHECKING:
    from snowbear.dataframes import DataFrame


class RawSqlTransformation:
    def __init__(self, query, _sources: Dict[str, "DataFrame"] = None) -> None:
        self._sources = _sources
        if self._sources is None:
            self._sources = {}

        self._query = query

    def get_sql(self):
        query = self._query
        for key, source in self._sources.items():
            query = query.replace("{{{" + key + "}}}", source.get_alias_name)
        return query

    @abstractmethod
    def get_dependencies(self):
        dep_list = []
        for source in self._sources.values():
            dep_list.extend(extend_transformations(source))
        return dep_list
