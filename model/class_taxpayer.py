from sqlalchemy import text
import pandas as pd


class TaxpayerRepository:
    def __init__(self, db_engine):
        """
        :param db_engine: экземпляр DatabaseEngine
        """
        self.db_engine = db_engine

    def get_all_taxpayers(self):
        """
        Получить всех налогоплательщиков
        :return: DataFrame
        """
        query = """
            SELECT 
                TaxpayerId,
                FullName,
                PassportNumber,
                INN,
                TaxpayerType,
                activity_type,
                registration_district,
                has_employees,
                employees_count
            FROM Taxpayer
        """
        return self.db_engine.execute_query(query)

    def get_taxpayer_by_inn(self, inn: str):
        """
        Получить налогоплательщика по ИНН
        :param inn: ИНН (12 символов)
        :return: DataFrame
        """
        engine = self.db_engine.get_engine()
        if engine is None:
            return None

        query = text("""
            SELECT 
                TaxpayerId,
                FullName,
                PassportNumber,
                INN,
                TaxpayerType,
                activity_type,
                registration_district,
                has_employees,
                employees_count
            FROM Taxpayers
            WHERE INN = :inn
        """)

        try:
            with engine.connect() as connection:
                df = pd.read_sql(query, connection, params={"inn": inn})
                print(df)
                return df
        except Exception as e:
            print(f"Ошибка при получении налогоплательщика по ИНН: {e}")
            return None
