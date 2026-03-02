import json
import pandas as pd
from flask import Blueprint, jsonify, current_app, request
from model.ForecastService import ForecastService, logger
from model.ModelsRepository import ModelsRepository
from model.TaxDataRepository import TaxDataRepository
from model.database import DatabaseEngine

models_bp = Blueprint('models', __name__, url_prefix='/api/models')

db_engine = DatabaseEngine()
repository = TaxDataRepository(db_engine)
repository_model = ModelsRepository(db_engine)


def get_forecaster():
    model_name = current_app.config["ACTIVE_MODEL_NAME"]
    model_version = current_app.config["ACTIVE_MODEL_VERSION"]

    return ForecastService(
        model_name=model_name,
        model_version=model_version
    )


def ensure_prediction_up_to_date():
    df_real_years = repository.get_years()
    forecaster = get_forecaster()
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


def handle_df_response(df, transform=None):
    if df is None or df.empty:
        return jsonify({'success': False, 'error': 'No data found'}), 404
    if transform:
        df = transform(df)
    return jsonify({'success': True, 'data': df_to_json(df)})


def create_prediction(last_real_year):
    taxpayers_df = repository.get_taxpayers()
    forecaster = get_forecaster()
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
    forecaster = get_forecaster()
    return forecaster.model_name, forecaster.model_version


@models_bp.route('/', methods=['GET'])
def index():
    try:
        models = repository_model.get_models()
        return handle_df_response(models)
    except Exception as e:
        logger.error(f"Error during prediction initialization {e}", exc_info=True)


@models_bp.route('/get_models', methods=['GET'])
def get_models_api():
    try:
        available_models = repository_model.get_available_models()
        return handle_df_response(available_models)
    except Exception as e:
        logger.error(f"Error getting available models {e}", exc_info=True)


@models_bp.route('/set_active', methods=['POST'])
def set_active_model():
    try:
        data = request.json
        model_name = data.get("ModelName")
        model_version = data.get("ModelVersion")
        if not model_name or not model_version:
            return jsonify({"success": False, "error": "Invalid data"}), 400
        current_app.config["ACTIVE_MODEL_NAME"] = model_name
        current_app.config["ACTIVE_MODEL_VERSION"] = model_version
        current_app.forecaster = ForecastService(
            model_name=model_name,
            model_version=model_version
        )
        return jsonify({
            "success": True,
            "active_model": {
                "ModelName": model_name,
                "ModelVersion": model_version
            }
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@models_bp.route('/info', methods=['GET'])
def get_model_info_api():
    try:
        model_name = request.args.get("ModelName")
        model_version = request.args.get("ModelVersion")
        if not model_name or not model_version:
            return jsonify({"success": False, "error": "Missing parameters"}), 400
        model_info = repository_model.get_model_info(model_name, model_version)
        return handle_df_response(model_info)

    except Exception as e:
        logger.error(f"Error getting model info {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@models_bp.route('/active', methods=['GET'])
def get_active_model():
    return jsonify({
        "success": True,
        "active_model": {
            "ModelName": current_app.config["ACTIVE_MODEL_NAME"],
            "ModelVersion": current_app.config["ACTIVE_MODEL_VERSION"]
        }
    })