# services/forecast_service.py
import joblib
import os

import numpy as np
import pandas as pd
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ForecastService:

    def __init__(self, model_name, model_version, models_path=None):
        if models_path is None:
            models_path = r"C:\Users\blino\DiplomaProject\model\regression"
        self.models_path = models_path
        self.model_name = model_name
        self.model_version = model_version
        self.load_models()

    def prediction_exists(self, db_engine, year):
        """Check if prediction already exists in Predict table"""
        query = """
            SELECT COUNT(*) 
            FROM Predict
            WHERE Year = ?
              AND ModelName = ?
              AND ModelVersion = ?
        """
        result = db_engine.execute_query(query, [year, self.model_name, self.model_version])
        print(result)
        if result.empty:
            return False
        return int(result.iloc[0, 0]) > 0

    def load_models(self):
        """Loading ML models dynamically"""
        try:
            base_path = os.path.join(
                self.models_path,
                self.model_name
            )

            self.income_model = joblib.load(
                os.path.join(base_path, "linear_income_model.pkl")
            )
            self.transactions_model = joblib.load(
                os.path.join(base_path, "linear_transactions_model.pkl")
            )
            self.tax_model = joblib.load(
                os.path.join(base_path, "linear_tax_model.pkl")
            )
            logger.info(
                f"Loaded {self.model_name} version {self.model_version}"
            )
        except Exception as e:
            logger.error(f"Error loading models: {e}")
            raise

    def get_season(self, month):
        """to determine seasons by month"""
        if month in [12, 1, 2]:
            return 'winter'
        elif month in [3, 4, 5]:
            return 'spring'
        elif month in [6, 7, 8]:
            return 'summer'
        return 'autumn'

    def prepare_features(self, df):
        """prepare of signs"""
        features = [
            'Year', 'Month', 'season',
            'TaxType', 'TaxpayerType',
            'activity_type', 'registration_district',
            'has_employees', 'employees_count'
        ]
        return df[features]

    def predict_for_taxpayers(self, taxpayers_df, target_year):
        logger.info(f"Starting prediction for {len(taxpayers_df)} taxpayers for year {target_year}")
        future_rows = []
        for _, taxpayer in taxpayers_df.iterrows():
            for month in range(1, 13):
                future_rows.append({
                    'TaxpayerId': taxpayer['TaxpayerId'],
                    'FullName': taxpayer.get('FullName', ''),
                    'INN': taxpayer.get('INN', ''),
                    'Year': target_year,
                    'Month': month,
                    'season': self.get_season(month),
                    'TaxType': taxpayer.get('TaxType', taxpayer.get('TaxpayerType')),
                    'TaxpayerType': taxpayer['TaxpayerType'],
                    'activity_type': taxpayer['activity_type'],
                    'registration_district': taxpayer['registration_district'],
                    'has_employees': taxpayer['has_employees'],
                    'employees_count': taxpayer['employees_count']
                })

        future_df = pd.DataFrame(future_rows)
        X_future = self.prepare_features(future_df)
        # Forecast
        future_df['PredictedIncome'] = self.income_model.predict(X_future)
        future_df['PredictedTransactions'] = self.transactions_model.predict(X_future)
        future_df['PredictedTax'] = self.tax_model.predict(X_future)
        cols_to_clip = ['PredictedIncome', 'PredictedTransactions', 'PredictedTax']
        future_df[cols_to_clip] = future_df[cols_to_clip].clip(lower=0)
        future_df['PredictedIncome'] = future_df['PredictedIncome'].round(2)
        future_df['PredictedTransactions'] = future_df['PredictedTransactions'].round(0).astype(int)
        future_df['PredictedTax'] = future_df['PredictedTax'].round(2)
        yearly_summary = future_df.groupby(
            ['TaxpayerId', 'FullName', 'INN']
        ).agg(
            TotalPredictedIncome=('PredictedIncome', 'sum'),
            TotalPredictedTransactions=('PredictedTransactions', 'sum'),
            TotalPredictedTax=('PredictedTax', 'sum')
        ).reset_index()
        yearly_summary['Year'] = target_year
        logger.info(f"Prediction completed. Generated {len(future_df)} monthly records")
        return future_df, yearly_summary

    def save_predictions_to_db(self, engine, monthly_df):
        monthly_save_df = monthly_df.rename(columns={
            'PredictedIncome': 'Income',
            'PredictedTransactions': 'Transactions',
            'PredictedTax': 'Tax'
        })

        monthly_save_df["ModelName"] = self.model_name
        monthly_save_df["ModelVersion"] = self.model_version

        predict_columns = [
            'TaxpayerId', 'FullName', 'INN', 'Year', 'Month',
            'Income', 'Transactions', 'Tax',
            'TaxType', 'TaxpayerType', 'activity_type',
            'registration_district', 'has_employees', 'employees_count',
            'ModelName', 'ModelVersion'
        ]

        monthly_save_df = monthly_save_df[predict_columns]

        for col in predict_columns:
            if col not in monthly_save_df:
                monthly_save_df[col] = None

        forecast_year = int(monthly_save_df['Year'].iloc[0])


        monthly_save_df['Income'] = monthly_save_df['Income'].fillna(0).round(2).astype(float)
        monthly_save_df['Tax'] = monthly_save_df['Tax'].fillna(0).round(2).astype(float)
        monthly_save_df['Transactions'] = monthly_save_df['Transactions'].fillna(0).round(0).astype(int)
        monthly_save_df['employees_count'] = (
            monthly_save_df['employees_count']
            .replace({np.nan: None})
        )

        print("MAX VALUES:")
        print("Income max:", monthly_save_df['Income'].max())
        print("Tax max:", monthly_save_df['Tax'].max())
        print("Transactions max:", monthly_save_df['Transactions'].max())
        print("employees_count max:", monthly_save_df['employees_count'].max())
        print("employees_count min:", monthly_save_df['employees_count'].min())


        print(monthly_save_df.head(5).to_string())
        pd.DataFrame({monthly_save_df.to_csv(f"predictions.csv", index=False)})

        if self.prediction_exists(engine, forecast_year):
            logger.info(
                f"Forecast already exists for Year={forecast_year}, Model={self.model_name} {self.model_version}. Skipping save."
            )
            return

        try:
            conn = engine.get_engine().raw_connection()
            cursor = conn.cursor()
            cursor.fast_executemany = True

            batch_size = 10000
            for start in range(0, len(monthly_save_df), batch_size):
                end = start + batch_size
                batch_data = [tuple(row[col] for col in predict_columns)
                              for _, row in monthly_save_df.iloc[start:end].iterrows()]
                cursor.executemany("""
                    INSERT INTO Predict
                    (TaxpayerId, FullName, INN, Year, Month,
                     Income, Transactions, Tax,
                     TaxType, TaxpayerType, activity_type, registration_district,
                     has_employees, employees_count,
                     ModelName, ModelVersion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_data)

            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error saving predictions to DB: {e}")
            raise
