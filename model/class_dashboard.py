import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import joblib


# ищет налогоплательщика по ИНН
# возвращает помесячные данные за каждый год
# считает годовые итоги (sum) / считает медианные значения
# считает %-разницу между годами
# учитывает модель прогнозирования
# готов отдавать данные для 3 графиков: доход, транзакции, налог

class DashboardData:
    def __init__(self, db_engine):
        """
                :param db_engine: экземпляр DatabaseEngine
                """
        self.db_engine = db_engine

        # Загружаем ML модели
        self.income_model = joblib.load("linear_income_model.pkl")
        self.transactions_model = joblib.load("linear_transactions_model.pkl")
        self.tax_model = joblib.load("linear_tax_model.pkl")

    # find taxpayer by INN
    def get_taxpayer_by_inn(self, inn: str) -> pd.DataFrame:
        query = """
        SELECT *
        FROM Taxpayer
        WHERE INN = :inn
        """
        return pd.read_sql(query, self.db_engine, params={"inn": inn})

    def get_global_year_range(self) -> dict:
        """
        Получает минимальный и максимальный год по всем данным в базе

        :return: словарь с min_year и max_year
        """
        query = """
        SELECT 
            MIN(Year) as min_year,
            MAX(Year) as max_year
        FROM MonthlyTaxData
        """

        result = pd.read_sql(query, self.db_engine)

        if result.empty or result["min_year"].isna().all():
            return {
                "min_year": None,
                "max_year": None,
                "error": "No data found in database"
            }

        return {
            "min_year": int(result["min_year"].iloc[0]),
            "max_year": int(result["max_year"].iloc[0])
        }

    def get_monthly_data(self, inn: str) -> pd.DataFrame:
        query = """
        SELECT 
            m.Year,
            m.Month,
            m.Income,
            m.Transactions,
            m.Tax
        FROM MonthlyTaxData m
        JOIN Taxpayer t ON t.TaxpayerId = m.TaxpayerId
        WHERE t.INN = :inn
        ORDER BY m.Year, m.Month
        """
        return pd.read_sql(query, self.db_engine, params={"inn": inn})

    def get_yearly_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        yearly = df.groupby("Year").agg(
            TotalIncome=("Income", "sum"),
            TotalTransactions=("Transactions", "sum"),
            TotalTax=("Tax", "sum")
        ).reset_index()

        return yearly

    def get_yearly_median(self, df: pd.DataFrame) -> pd.DataFrame:
        yearly = df.groupby("Year").agg(
            MedianIncome=("Income", "median"),
            MedianTransactions=("Transactions", "median"),
            MedianTax=("Tax", "median")
        ).reset_index()

        return yearly

    def calculate_yearly_growth(self, yearly_df: pd.DataFrame) -> pd.DataFrame:
        yearly_df = yearly_df.sort_values("Year").copy()

        yearly_df["IncomeGrowth_%"] = yearly_df["TotalIncome"].pct_change() * 100
        yearly_df["TransactionsGrowth_%"] = yearly_df["TotalTransactions"].pct_change() * 100
        yearly_df["TaxGrowth_%"] = yearly_df["TotalTax"].pct_change() * 100

        return yearly_df

    def predict_next_year(self, taxpayer_row: pd.Series, last_year: int) -> pd.DataFrame:

        next_year = last_year + 1

        def get_season(month):
            if month in [12, 1, 2]:
                return 'winter'
            elif month in [3, 4, 5]:
                return 'spring'
            elif month in [6, 7, 8]:
                return 'summer'
            return 'autumn'

        future_rows = []

        for month in range(1, 13):
            future_rows.append({
                "Year": next_year,
                "Month": month,
                "season": get_season(month),
                "TaxType": taxpayer_row["TaxpayerType"],
                "TaxpayerType": taxpayer_row["TaxpayerType"],
                "activity_type": taxpayer_row["activity_type"],
                "registration_district": taxpayer_row["registration_district"],
                "has_employees": taxpayer_row["has_employees"],
                "employees_count": taxpayer_row["employees_count"]
            })

        future_df = pd.DataFrame(future_rows)

        features = [
            'Year', 'Month', 'season',
            'TaxType', 'TaxpayerType',
            'activity_type', 'registration_district',
            'has_employees', 'employees_count'
        ]

        X = future_df[features]

        future_df["Income"] = self.income_model.predict(X)
        future_df["Transactions"] = self.transactions_model.predict(X)
        future_df["Tax"] = self.tax_model.predict(X)

        # защита от отрицательных значений
        future_df[["Income", "Transactions", "Tax"]] = \
            future_df[["Income", "Transactions", "Tax"]].clip(lower=0)

        return future_df

    def build_dashboard_data(self, inn: str, include_prediction=True):

        taxpayer = self.get_taxpayer_by_inn(inn)
        if taxpayer.empty:
            return {"error": "Taxpayer not found"}

        monthly_df = self.get_monthly_data(inn)

        # годовые итоги
        yearly_totals = self.get_yearly_totals(monthly_df)

        # медианы
        yearly_median = self.get_yearly_median(monthly_df)

        # процент роста
        yearly_growth = self.calculate_yearly_growth(yearly_totals)

        # прогноз
        if include_prediction:
            last_year = monthly_df["Year"].max()
            prediction_df = self.predict_next_year(taxpayer.iloc[0], last_year)

            monthly_df = pd.concat([monthly_df, prediction_df])

        return {
            "monthly": monthly_df,
            "yearly_totals": yearly_totals,
            "yearly_median": yearly_median,
            "yearly_growth": yearly_growth
        }

    def close(self):
        self.db_engine.dispose()
