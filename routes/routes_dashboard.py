import json
import time

import numpy as np
import pandas as pd
from flask import Blueprint, jsonify
from model.AggregationService import AggregationService
from model.ForecastService import ForecastService, logger
from model.TaxDataRepository import TaxDataRepository
from model.database import DatabaseEngine

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

db_engine = DatabaseEngine()
repository = TaxDataRepository(db_engine)
aggregator = AggregationService()
forecaster = ForecastService()


# support function
def convert_numpy_types(obj):
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
        df.to_json(
            orient='records',
            date_format='iso',
            default_handler=convert_numpy_types
        )
    )


def ensure_prediction_up_to_date():
    df_real_years = repository.get_years()
    if df_real_years.empty:
        return pd.DataFrame()
    last_real_year = int(df_real_years["Year"].max())
    df_pred = repository.get_predict_data()
    if df_pred.empty:
        return create_prediction(last_real_year)
    last_pred_year = int(df_pred["Year"].max())
    if last_pred_year > last_real_year:
        return df_pred
    return create_prediction(last_real_year)


def create_prediction(last_real_year):
    taxpayers_df = repository.get_taxpayers()
    next_year = last_real_year + 1
    forecast_df, yearly_summary_df = forecaster.predict_for_taxpayers(
        taxpayers_df, next_year
    )
    engine = repository.db_engine.get_engine()
    forecaster.save_predictions_to_db(engine, forecast_df, yearly_summary_df)
    return forecast_df


def handle_df_response(df, transform=None):
    if df is None or df.empty:
        return jsonify({'success': False, 'error': 'No data found'}), 404
    if transform:
        df = transform(df)
    return jsonify({'success': True, 'data': df_to_json(df)})


def initialize_predictions():
    try:
        print("🔄 Checking predictions on startup...")
        df = ensure_prediction_up_to_date()
        print(f"✅ Prediction check complete. Rows: {len(df)}")
    except Exception as e:
        print("❌ Error during prediction initialization:", e)


# TAXPAYER INFO
@dashboard_bp.route('/taxpayer/<inn>', methods=['GET'])
def get_taxpayer(inn):
    try:
        taxpayer = repository.get_taxpayer(inn)
        return handle_df_response(taxpayer)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# MONTHLY DATA
@dashboard_bp.route('/monthly/<inn>', methods=['GET'])
def get_monthly(inn):
    try:
        df = repository.get_monthly_by_inn(inn)
        return handle_df_response(df)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/yearly/totals/<inn>', methods=['GET'])
def get_yearly_totals(inn):
    try:
        df = repository.get_monthly_by_inn(inn)
        return handle_df_response(df, lambda x: aggregator.aggregate_yearly(x, "sum"))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# YEARLY MEDIAN
@dashboard_bp.route('/yearly/median/<inn>', methods=['GET'])
def get_yearly_median_inn(inn):
    try:
        df = repository.get_monthly_by_inn(inn)
        return handle_df_response(df, lambda x: aggregator.aggregate_yearly(x, "median"))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/monthly/median', defaults={'tax_type': None}, methods=['GET'], strict_slashes=False)
@dashboard_bp.route('/monthly/median/<tax_type>', methods=['GET'], strict_slashes=False)
def get_monthly_median_all(tax_type):
    try:
        df = repository.get_monthly_data(
            source="real",
            tax_type=tax_type,
            aggregate=False
        )
        return handle_df_response(df, lambda x: aggregator.aggregate_monthly(x, "median"))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/monthly/general', defaults={'tax_type': None}, methods=['GET'])
@dashboard_bp.route('/monthly/general/<tax_type>', methods=['GET'])
def get_monthly_general_all(tax_type):
    try:
        df = repository.get_monthly_data(
            source="real",
            tax_type=tax_type,
            aggregate=True
        )
        return handle_df_response(df)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# YEARLY GROWTH
@dashboard_bp.route('/yearly/growth/<inn>', methods=['GET'])
def get_yearly_growth_inn(inn):
    try:
        df = repository.get_monthly_by_inn(inn)
        return handle_df_response(
            df,
            lambda x: aggregator.calculate_growth(
                aggregator.aggregate_yearly(x, "sum")
            )
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/yearly/growth/general', defaults={'tax_type': None}, methods=['GET'])
@dashboard_bp.route('/yearly/growth/general/<tax_type>', methods=['GET'])
def get_yearly_growth_general(tax_type):
    try:
        df_real = repository.get_monthly_data(
            source="real",
            tax_type=tax_type,
            aggregate=False
        )

        df_pred = repository.get_monthly_data(
            source="predict",
            tax_type=tax_type,
            aggregate=False
        )
        if df_real.empty and df_pred.empty:
            return jsonify({'success': False, 'error': 'No data found'}), 404

        yearly_sum_real = aggregator.aggregate_yearly(df_real, "sum")
        yearly_sum_pred = aggregator.aggregate_yearly(df_pred, "sum")

        combined = pd.concat([yearly_sum_real, yearly_sum_pred], ignore_index=True)
        combined = combined.sort_values("Year")

        growth = aggregator.calculate_growth(combined)
        return jsonify({'success': True, 'data': df_to_json(growth)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/yearly/growth/median', defaults={'tax_type': None}, methods=['GET'])
@dashboard_bp.route('/yearly/growth/median/<tax_type>', methods=['GET'])
def get_yearly_growth_median(tax_type):
    try:
        df_real = repository.get_monthly_data(
            source="real",
            tax_type=tax_type,
            aggregate=False
        )

        df_pred = repository.get_monthly_data(
            source="predict",
            tax_type=tax_type,
            aggregate=False
        )

        if df_real.empty and df_pred.empty:
            return jsonify({'success': False, 'error': 'No data found'}), 404

        yearly_sum_real = aggregator.aggregate_yearly(df_real, "median")
        yearly_sum_pred = aggregator.aggregate_yearly(df_pred, "median")

        combined = pd.concat([yearly_sum_real, yearly_sum_pred], ignore_index=True)
        combined = combined.sort_values("Year")

        growth = aggregator.calculate_growth(combined)

        return jsonify({'success': True, 'data': df_to_json(growth)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# PREDICTION
@dashboard_bp.route('/predict_inn/<inn>', methods=['GET'])
def get_prediction_inn(inn):
    try:
        taxpayer = repository.get_taxpayer(inn)
        if taxpayer.empty:
            return jsonify({'success': False, 'error': 'Taxpayer not found'}), 404
        monthly_df = repository.get_monthly_by_inn(inn)
        if monthly_df.empty:
            return jsonify({'success': False, 'error': 'No historical data'}), 404
        last_year = int(monthly_df["Year"].max())
        prediction_df, _ = forecaster.predict_for_taxpayers(taxpayer, last_year + 1)
        return jsonify({
            'success': True,
            'prediction_year': last_year + 1,
            'data': df_to_json(prediction_df)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/predict_generale/result', methods=['GET'])
def get_prediction_general_result():
    try:
        df = repository.get_predict_data()

        if df.empty:
            return jsonify({'success': False, 'error': 'No prediction data'}), 404

        general_df = df.groupby('Month').agg(
            TotalIncome=('Income', 'sum'),
            TotalTransactions=('Transactions', 'sum'),
            TotalTax=('Tax', 'sum')
        ).reset_index()

        median_df = df.groupby('Month').agg(
            MedianIncome=('Income', 'median'),
            MedianTransactions=('Transactions', 'median'),
            MedianTax=('Tax', 'median')
        ).reset_index()

        return jsonify({
            "success": True,
            "general": general_df.to_dict(orient='records'),
            "median": median_df.to_dict(orient='records')
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/taxpayers', defaults={'tax_type': None}, methods=['GET'])
@dashboard_bp.route('/taxpayers/<tax_type>', methods=['GET'])
def get_taxpayers_api(tax_type):
    try:
        count = repository.get_taxpayers_count(tax_type)

        return jsonify({
            "status": "success",
            "tax_type": tax_type,
            "count": count
        }), 200

    except Exception as e:
        print("ERROR:", e)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# GLOBAL YEAR RANGE
@dashboard_bp.route('/global-year-range', methods=['GET'])
def get_global_year_range():
    try:
        df = repository.get_years()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data found'}), 404
        years = df['Year'].tolist()
        return jsonify({
            'success': True,
            'data': {
                'min_year': min(years),
                'max_year': max(years),
                'years': years
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# CLOSE DB
@dashboard_bp.route('/close', methods=['POST'])
def close_connection():
    try:
        db_engine.dispose_engine()
        return jsonify({'success': True, 'message': 'Database connection closed'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
