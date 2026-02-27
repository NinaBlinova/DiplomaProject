import json
import pandas as pd
from flask import Blueprint, jsonify
from model.ForecastService import ForecastService, logger
from model.TaxDataRepository import TaxDataRepository
from model.database import DatabaseEngine

models_bp = Blueprint('models', __name__, url_prefix='/api/models')

db_engine = DatabaseEngine()
repository = TaxDataRepository(db_engine)

forecaster = ForecastService(
    model_name="LightGBM",
    model_version="v1.0"
)


def ensure_prediction_up_to_date():
    df_real_years = repository.get_years()
    if df_real_years.empty:
        logger.info("No historical data found.")
        return pd.DataFrame()

    last_real_year = int(df_real_years["Year"].max())
    engine = repository.db_engine

    next_year = last_real_year + 1
    df_pred = repository.get_predict_data(
        model_name=forecaster.model_name,
        model_version=forecaster.model_version
    )
    if df_pred.empty or not forecaster.prediction_exists(engine, next_year):
        logger.info(f"No prediction for {next_year}, generating...")
        df_pred = create_prediction(last_real_year)
    else:
        logger.info(f"Prediction already exists for year {next_year}")
    return df_pred


def create_prediction(last_real_year):
    taxpayers_df = repository.get_taxpayers()
    next_year = last_real_year + 1
    engine = repository.db_engine
    print(next_year)
    if forecaster.prediction_exists(engine, next_year):
        logger.info(f"Forecast already exists for year {next_year}, skipping prediction.")
        return repository.get_predict_data(
            model_name=forecaster.model_name,
            model_version=forecaster.model_version
        )
    forecast_df, yearly_summary = forecaster.predict_for_taxpayers(
        taxpayers_df, next_year
    )
    print(forecast_df)
    forecaster.save_predictions_to_db(engine, forecast_df)
    return forecast_df


def convert_numpy_types(obj):
    if isinstance(obj, (pd.Series, pd.DataFrame)):
        return obj.to_dict()
    import numpy as np
    if isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    elif isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    return obj


def df_to_json(df):
    if df is None or df.empty:
        return []
    return json.loads(
        df.to_json(orient='records', date_format='iso', default_handler=convert_numpy_types)
    )

def initialize_predictions():
    try:
        df = ensure_prediction_up_to_date()
        print(f"Prediction check complete. Rows: {len(df)}")
    except Exception as e:
        logger.error("Error during prediction initialization", exc_info=True)


def get_current_model_info():
    return forecaster.model_name, forecaster.model_version