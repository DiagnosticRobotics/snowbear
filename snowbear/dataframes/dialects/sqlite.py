from snowbear.dataframes import Session


class SqliteSession(Session):
    def __init__(self, connection: "Connection"):
        super().__init__(connection, "sqlite")
        self.dialect = "sqlite"
        self.QUOTE_CHAR = None
        self.ALIAS_QUOTE_CHAR = '"'
        self.QUERY_ALIAS_QUOTE_CHAR = ""
