from abc import abstractmethod
from typing import Dict

from snowbear.dataframes.transformations.transformations import extend_transformations


class RawSqlTransformation:
    def __init__(self, sql, source, additional_sources: Dict[str, "Selectable"]) -> None:
        self._source = source
        self._additional_sources = additional_sources
        self._sql = sql

    def get_sql(self):
        sql= self._sql.replace(f'{{{{source}}}}', self._source.get_alias_name())
        for key,source in self._additional_sources.items():
            sql = sql.replace(f'{{{{{key}}}}}', source.get_alias_name())
        return sql

    @abstractmethod
    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        for source in self._additional_sources:
            dep_list.extend(extend_transformations(source))
        return dep_list