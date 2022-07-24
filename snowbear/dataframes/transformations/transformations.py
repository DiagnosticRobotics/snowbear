from abc import abstractmethod

TAB = '\t'


class SQLTransformation:
    def get_sql(self):
        pass

    @abstractmethod
    def get_dependencies(self):
        pass


def extend_transformations(source):
    dep_list = []
    if source.get_transformation():
        dep_list.extend(source.get_transformation().get_dependencies())
        dep_list.append((source.get_table_name(), source.get_transformation()))
    return dep_list


