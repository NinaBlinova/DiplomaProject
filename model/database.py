from sqlalchemy import create_engine, text
import pandas as pd


class DatabaseEngine:
    def __init__(self, server="localhost", database="Taxpayer_Database_DiplomaProject",
                 driver="ODBC Driver 17 for SQL Server"):
        self.server = server
        self.database = database
        self.driver = driver
        self.engine = None

    def get_engine(self):
        """Создать или вернуть существующий SQLAlchemy engine"""
        if self.engine is None:
            connection_string = (
                f"mssql+pyodbc://@{self.server}/{self.database}?"
                f"trusted_connection=yes&"
                f"driver={self.driver.replace(' ', '+')}"
            )
            try:
                self.engine = create_engine(connection_string)
            except Exception as e:
                print(f"Ошибка при создании engine: {e}")
                return None
        return self.engine

    def execute_query(self, query, params=None):
        """Выполнить SQL-запрос и вернуть DataFrame"""
        engine = self.get_engine()
        if engine is None:
            return pd.DataFrame()

        try:
            with engine.connect() as conn:
                if params is not None:
                    # Если params - список, преобразуем в кортеж для pyodbc
                    if isinstance(params, list):
                        params = tuple(params)
                return pd.read_sql(query, conn, params=params)
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return pd.DataFrame()

    def dispose_engine(self):
        """Закрыть engine"""
        if self.engine:
            self.engine.dispose()
            self.engine = None