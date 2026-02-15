import pandas as pd


class TaxDataRepository:

    def __init__(self, db_engine):
        self.db_engine = db_engine

    def get_years(self):
        query = "SELECT DISTINCT Year FROM MonthlyTaxData ORDER BY Year"
        return pd.read_sql(query, self.db_engine)

    def get_taxpayer(self, inn: str):
        query = "SELECT * FROM Taxpayer WHERE INN = :inn"
        return pd.read_sql(query, self.db_engine, params={"inn": inn})

    def get_monthly_by_inn(self, inn: str):
        query = """
        SELECT m.Year, m.Month, m.Income, m.Transactions, m.Tax
        FROM MonthlyTaxData m
        JOIN Taxpayer t ON t.TaxpayerId = m.TaxpayerId
        WHERE t.INN = :inn
        ORDER BY m.Year, m.Month
        """
        return pd.read_sql(query, self.db_engine, params={"inn": inn})

    def get_monthly_all(self):
        query = """
        SELECT Year, Month, Income, Transactions, Tax
        FROM MonthlyTaxData
        ORDER BY Year, Month
        """
        return pd.read_sql(query, self.db_engine)
