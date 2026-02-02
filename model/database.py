import pyodbc


class Database:
    def __init__(self):
        self.connection = None

    def get_connection(self):
        """Получить соединение с базой данных"""
        if self.connection is None:
            try:
                self.connection = pyodbc.connect(
                    "DRIVER={ODBC Driver 17 for SQL Server};"
                    "Server=localhost;"
                    "Database=Taxpayer_Database_DiplomaProject;"
                    "Trusted_Connection=yes;"
                    "MARS_Connection=yes;"
                )
            except pyodbc.Error as e:
                print(f"Ошибка подключения к базе данных: {e}")
                return None
        return self.connection

    def close_connection(self):
        """Закрыть соединение"""
        if self.connection:
            self.connection.close()
            self.connection = None


# Создаем глобальный экземпляр
db = Database()
