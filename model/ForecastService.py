import joblib
import pandas as pd


class ForecastService:

    def __init__(self):
        self.income_model = joblib.load("linear_income_model.pkl")
        self.transactions_model = joblib.load("linear_transactions_model.pkl")
        self.tax_model = joblib.load("linear_tax_model.pkl")

    def predict_year(self, taxpayer_row, year: int):

        def get_season(month):
            if month in [12, 1, 2]:
                return 'winter'
            elif month in [3, 4, 5]:
                return 'spring'
            elif month in [6, 7, 8]:
                return 'summer'
            return 'autumn'

        rows = []

        for month in range(1, 13):
            rows.append({
                "Year": year,
                "Month": month,
                "season": get_season(month),
                "TaxType": taxpayer_row["TaxpayerType"],
                "TaxpayerType": taxpayer_row["TaxpayerType"],
                "activity_type": taxpayer_row["activity_type"],
                "registration_district": taxpayer_row["registration_district"],
                "has_employees": taxpayer_row["has_employees"],
                "employees_count": taxpayer_row["employees_count"]
            })

        df = pd.DataFrame(rows)

        features = [
            'Year', 'Month', 'season',
            'TaxType', 'TaxpayerType',
            'activity_type', 'registration_district',
            'has_employees', 'employees_count'
        ]

        X = df[features]

        df["Income"] = self.income_model.predict(X)
        df["Transactions"] = self.transactions_model.predict(X)
        df["Tax"] = self.tax_model.predict(X)

        df[["Income", "Transactions", "Tax"]] = \
            df[["Income", "Transactions", "Tax"]].clip(lower=0)

        return df[["Year", "Month", "Income", "Transactions", "Tax"]]
