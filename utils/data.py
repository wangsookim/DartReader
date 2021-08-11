from dataclasses import dataclass

import jaydebeapi
import pandas as pd


@dataclass
class SQLiteConnector:
    database: str = '../easystock.sqlite'

    def __post_init__(self):
        self.conn = jaydebeapi.connect("org.sqlite.JDBC",
                                       f"""jdbc:sqlite:{self.database}""",
                                       None,
                                       "sqlite-jdbc-3.33.0.jar")

    def __del__(self):
        self.conn.close()

    def select(self, query: str) -> pd.core.frame.DataFrame:
        """SQL Select 문

        :param str query: SQL query

        :returns pandas.core.frame.DataFrame: SQL query 결과
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.Dataframe(data)
        cursor.close()

        return df