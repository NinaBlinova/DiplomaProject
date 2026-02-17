# services/forecast_service.py
import joblib
import pandas as pd
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ForecastService:

    def __init__(self, models_path=None):
        if models_path is None:
            models_path = r"C:\Users\blino\DiplomaProject\model\regression\Linear regression\\"

        self.models_path = models_path
        self.model_version = "linear_regression_v1.0"
        self.load_models()

    def load_models(self):
        """Loading models ML"""
        try:
            self.income_model = joblib.load(f"{self.models_path}linear_income_model.pkl")
            self.transactions_model = joblib.load(f"{self.models_path}linear_transactions_model.pkl")
            self.tax_model = joblib.load(f"{self.models_path}linear_tax_model.pkl")
            logger.info("Models loaded successfully")
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
        """
        Forecast for all taxpayers

        Parameters:
        -----------
        taxpayers_df : DataFrame
            Necessary data from taxpayers
        target_year : int
            Year to predict for

        Returns:
        --------
        tuple: (monthly_forecast_df, yearly_summary_df)
        """

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

    def save_predictions_to_db(self, engine, monthly_df, yearly_df):
        """
            Save monthly (and optionally yearly) forecasts into the Predict table.
            Deletes only data for the forecast year (not full table).

            Parameters:
            -----------
            engine : SQLAlchemy engine
                Connection to the database.
            monthly_df : pd.DataFrame
                Monthly forecast with columns:
                ['TaxpayerId', 'FullName', 'INN', 'Year', 'Month', 'PredictedIncome',
                 'PredictedTransactions', 'PredictedTax', 'TaxType', 'TaxpayerType',
                 'activity_type', 'registration_district', 'has_employees', 'employees_count']
            yearly_df : pd.DataFrame, optional
                Yearly summary data (can be ignored if only monthly data is needed)
        """
        monthly_save_df = monthly_df.rename(columns={
            'PredictedIncome': 'Income',
            'PredictedTransactions': 'Transactions',
            'PredictedTax': 'Tax'
        })

        predict_columns = [
            'TaxpayerId', 'FullName', 'INN', 'Year', 'Month',
            'Income', 'Transactions', 'Tax',
            'TaxType', 'TaxpayerType', 'activity_type',
            'registration_district', 'has_employees', 'employees_count'
        ]

        missing_cols = set(predict_columns) - set(monthly_save_df.columns)
        for col in missing_cols:
            monthly_save_df[col] = None

        forecast_year = int(monthly_save_df['Year'].iloc[0])

        logger.info(f"Deleting old predictions for Year = {forecast_year}...")

        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM Predict WHERE Year = :year"),
                {"year": forecast_year}
            )

            logger.info("Old data deleted successfully.")

        monthly_save_df[predict_columns].to_sql(
            'Predict',
            con=engine,
            if_exists='append',
            index=False)

        logger.info(f"Saved {len(monthly_save_df)} monthly predictions to Predict table.")

        if yearly_df is not None:
            logger.info("Yearly summary not saved in this function, only monthly data is saved.")
