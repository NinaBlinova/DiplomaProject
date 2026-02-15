import json
import numpy as np
import pandas as pd
from flask import Blueprint, jsonify, request

from model.AggregationService import AggregationService
from model.DashboardService import DashboardService
from model.ForecastService import ForecastService
from model.TaxDataRepository import TaxDataRepository
from model.database import DatabaseEngine

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

db_engine = DatabaseEngine()
repository = TaxDataRepository(db_engine)
aggregator = AggregationService()
forecaster = ForecastService()
dashboard_service = DashboardService(repository, aggregator, forecaster)


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


# TAXPAYER INFO
@dashboard_bp.route('/taxpayer/<inn>', methods=['GET'])
def get_taxpayer(inn):
    try:
        taxpayer = repository.get_taxpayer(inn)

        if taxpayer.empty:
            return jsonify({'success': False, 'error': 'Taxpayer not found'}), 404

        return jsonify({'success': True, 'data': df_to_json(taxpayer)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# MONTHLY DATA
@dashboard_bp.route('/monthly/<inn>', methods=['GET'])
def get_monthly(inn):
    try:
        df = repository.get_monthly_by_inn(inn)

        if df.empty:
            return jsonify({'success': False, 'error': 'No data found'}), 404

        return jsonify({'success': True, 'data': df_to_json(df)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# YEARLY TOTALS
@dashboard_bp.route('/yearly/totals/<inn>', methods=['GET'])
def get_yearly_totals(inn):
    try:
        df = repository.get_monthly_by_inn(inn)

        if df.empty:
            return jsonify({'success': False, 'error': 'No data found'}), 404

        yearly = aggregator.aggregate_yearly(df, "sum")

        return jsonify({'success': True, 'data': df_to_json(yearly)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# YEARLY MEDIAN
@dashboard_bp.route('/yearly/median/<inn>', methods=['GET'])
def get_yearly_median(inn):
    try:
        df = repository.get_monthly_by_inn(inn)

        if df.empty:
            return jsonify({'success': False, 'error': 'No data found'}), 404

        yearly = aggregator.aggregate_yearly(df, "median")

        return jsonify({'success': True, 'data': df_to_json(yearly)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# YEARLY GROWTH
@dashboard_bp.route('/yearly/growth/<inn>', methods=['GET'])
def get_yearly_growth(inn):
    try:
        df = repository.get_monthly_by_inn(inn)

        if df.empty:
            return jsonify({'success': False, 'error': 'No data found'}), 404

        yearly_sum = aggregator.aggregate_yearly(df, "sum")
        growth = aggregator.calculate_growth(yearly_sum)

        return jsonify({'success': True, 'data': df_to_json(growth)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# PREDICTION
@dashboard_bp.route('/predict/<inn>', methods=['GET'])
def get_prediction(inn):
    try:
        taxpayer = repository.get_taxpayer(inn)

        if taxpayer.empty:
            return jsonify({'success': False, 'error': 'Taxpayer not found'}), 404

        monthly_df = repository.get_monthly_by_inn(inn)

        if monthly_df.empty:
            return jsonify({'success': False, 'error': 'No historical data'}), 404

        last_year = int(monthly_df["Year"].max())
        prediction_df = forecaster.predict_year(taxpayer.iloc[0], last_year + 1)

        return jsonify({
            'success': True,
            'prediction_year': last_year + 1,
            'data': df_to_json(prediction_df)
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# FULL DASHBOARD
@dashboard_bp.route('/full/<inn>', methods=['GET'])
def get_full_dashboard(inn):
    try:
        include_prediction = request.args.get('include_prediction', 'true').lower() == 'true'
        dashboard_data = dashboard_service.build_taxpayer_dashboard(
            inn,
            predict_years=1 if include_prediction else 0
        )
        if "error" in dashboard_data:
            return jsonify({'success': False, 'error': dashboard_data["error"]}), 404
        result = {
            'monthly': df_to_json(dashboard_data['monthly']),
            'yearly_totals': df_to_json(dashboard_data['yearly_sum']),
            'yearly_median': df_to_json(dashboard_data['yearly_median']),
            'yearly_growth': df_to_json(dashboard_data['growth'])
        }

        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# MODELS STATUS
@dashboard_bp.route('/models/status', methods=['GET'])
def get_models_status():
    try:
        models_status = {
            'income_model': forecaster.income_model is not None,
            'transactions_model': forecaster.transactions_model is not None,
            'tax_model': forecaster.tax_model is not None
        }
        return jsonify({'success': True, 'data': models_status})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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
