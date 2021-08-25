import sqlite3
from typing import List
from dataclasses import dataclass

import jaydebeapi
import psycopg2
import pandas as pd


@dataclass
class DBConnector:
    host: str
    port: int
    user: str
    password: str
    db_name: str = 'postgres'

    def __post_init__(self):
        self.conn = psycopg2.connect(host=self.host,
                                     database=self.db_name,
                                     user=self.user,
                                     password=self.password,
                                     port=self.port)

    def __del__(self):
        self.conn.close()

    def insert(self, sql: str, data: List[tuple]) -> None:

        try:
            cursor = self.conn.cursor()
            cursor.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print('insert ', e)

    def select(self, query: str) -> pd.DataFrame:

        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        cursor.close()

        return df

@dataclass
class SQLiteConnector:
    database: str = '../easystock.sqlite'

    def __post_init__(self):
        self.conn = sqlite3.connect(self.database)
        #jaydebeapi.connect("org.sqlite.JDBC", f"""jdbc:sqlite:{self.database}""", None, "sqlite-jdbc-3.33.0.jar")

    def __del__(self):
        self.conn.close()

    def create(self, query: str) -> None:
        """SQL CREATE 문

        :param str query: create를 위한 query
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()

    def insert(self, query:str, data:List[tuple]) -> None:
        """SQL INSERT 문

        :param str query: insert를 위한 query
        :param List[tuple] data: insert할 데이터
        """

        cursor = self.conn.cursor()
        cursor.executemany(query, data)
        self.conn.commit()

    def select(self, query: str) -> pd.core.frame.DataFrame:
        """SQL SELECT 문

        :param str query: SQL query

        :returns pandas.core.frame.DataFrame: SQL select query 결과
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        cursor.close()

        return df
