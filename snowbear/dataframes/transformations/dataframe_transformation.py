from dataclasses import dataclass
from textwrap import indent
from typing import List, Tuple

from snowbear.dataframes.terms import Term, Field
from snowbear.dataframes.transformations.transformations import SQLTransformation, extend_transformations, TAB


@dataclass
class JoinDefiniton:
    source: "SQLDataframe"
    join_type: str
    join_terms: List[Term]
    join_terms_type: str


NEWLINE = '\n'


class DataframeTransformation(SQLTransformation):

    def get_dependencies(self):
        dep_list = []
        dep_list.extend(extend_transformations(self._source))
        return dep_list

    def __init__(self, source, selectors=None, joins=None, filters=None, groupby: List[Field] = None,
                 aggs: List[Term] = None,
                 orderby: List[Tuple[Term, str]] = None, deps=None) -> None:
        self._source = source

        self._groupby = groupby if groupby else []
        self._aggs = aggs if aggs else []
        self._selectors = selectors if selectors else []
        self._filters = filters if filters else []
        self._orderby = orderby if orderby else []
        self._joins = joins if joins else []
        self._deps = deps if deps else []

    def copy(self):
        return DataframeTransformation(self._source, selectors=self._selectors.copy(), joins=self._joins.copy(),
                                       filters=self._filters.copy(),
                                       groupby=self._groupby.copy(), aggs=self._aggs.copy(), deps=self._deps.copy())

    def get_groupby_term(self) -> List[str]:
        if len(self._groupby) > 0:
            return ',\n'.join([x.get_sql() for x in self._groupby])
        else:
            return None

    def is_sealed(self):
        return len(self._groupby) > 0 or len(self._selectors) > 0

    def create_join_term(self, join: JoinDefiniton) -> str:
        if join.join_terms_type == "ON":
            terms = [x.get_sql() for x in join.join_terms]
            on_term = 'AND\n'.join(terms)
            return f"{join.join_type} {join.source.get_alias_name()} ON\n{indent(on_term, TAB)}"
        if join.join_terms_type == "ON":
            terms = [x.get_sql() for x in join.join_terms]
            on_term = ','.join(terms)
            return f"{join.join_type} {join.source.get_alias_name()} USING\n({indent(on_term, TAB)})"

        raise "join type must be ON or USING"

    def get_join_terms(self) -> List[str]:
        return [self.create_join_term(join) for join in self._joins]

        pass

    def get_select_term(self) -> str:
        if self._groupby:
            terms = [x.get_sql(with_alias=True) for x in self._groupby] + [x.get_sql(with_alias=True) for x in
                                                                           self._aggs]
        else:
            terms = [x.get_sql(with_alias=True) for x in self._selectors]
        if len(terms) > 0:
            return ',\n'.join(terms)
        else:
            return '*'

    def get_where_term(self) -> str:
        if len(self._filters) > 0:
            terms = [x.get_sql() for x in self._filters]
            return '\nAND '.join(terms)
        else:
            return None

    def get_orderby_term(self) -> str:
        if len(self._orderby) > 0:
            terms = [x[0].get_sql() + f" {x[1].upper()}" for x in self._orderby]
            return '\n, '.join(terms)
        else:
            return None

    def get_sql(self):

        select_section = f"SELECT\n{indent(self.get_select_term(), TAB)}"
        from_section = f"FROM {self._source.get_alias_name()}"
        join_section = f"{NEWLINE.join(self.get_join_terms())}" if len(self.get_join_terms()) > 0 else ""
        where_section = f"WHERE\n{indent(self.get_where_term(), TAB)}" if self.get_where_term() else ""
        group_section = f"GROUP BY\n{indent(self.get_groupby_term(), TAB)}" if self.get_groupby_term() else ""
        order_section = f"ORDER BY\n{indent(self.get_orderby_term(), TAB)}" if self.get_orderby_term() else ""
        sql_segments = filter(lambda s: len(s) > 0,
                              [select_section, from_section, join_section, where_section, group_section, order_section])
        return '\n'.join(sql_segments)

    def add_join(self, other: "SQLDataframe", join_type: str, term_types: str, terms: List[Term]):
        self._joins.append(
            JoinDefiniton(source=other, join_type=join_type, join_terms_type=term_types, join_terms=terms))
        self._deps.append(other)

    def add_groupby(self, by: List[Field], aggs: List[Term]):
        self._groupby.extend(by)
        self._aggs.extend(aggs)

    def add_filter(self, filters: List[Term]):
        self._filters.extend(filters)

    def add_select(self, selectors: List[Term]):
        self._selectors.extend(selectors)

    def add_orderby(self, orderby: List[Tuple[List[Term], str]]):
        self._orderby.extend(orderby)
