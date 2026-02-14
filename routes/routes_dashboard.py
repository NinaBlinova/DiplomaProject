import pandas as pd
from flask import Blueprint, jsonify, request
import json
import numpy as np

from model.class_dashboard import DashboardData
from model.database import DatabaseEngine

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

db_engine = DatabaseEngine()
repo = DashboardData(db_engine)


# Вспомогательная функция для конвертации numpy типов в Python типы
def convert_numpy_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    else:
        return obj


# Вспомогательная функция для форматирования DataFrame в JSON
def df_to_json(df):
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient='records', date_format='iso', default_handler=convert_numpy_types))


# Маршрут для поиска налогоплательщика по ИНН
@dashboard_bp.route('/taxpayer/<inn>', methods=['GET'])
def get_taxpayer(inn):
    """
    Получить информацию о налогоплательщике по ИНН
    """
    try:
        taxpayer = repo.get_taxpayer_by_inn(inn)
        if taxpayer.empty:
            return jsonify({
                'success': False,
                'error': 'Taxpayer not found'
            }), 404

        result = df_to_json(taxpayer)
        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для получения помесячных данных
@dashboard_bp.route('/monthly/<inn>', methods=['GET'])
def get_monthly_data(inn):
    """
    Получить помесячные данные налогоплательщика
    """
    try:
        monthly_df = repo.get_monthly_data(inn)
        if monthly_df.empty:
            return jsonify({
                'success': False,
                'error': 'No monthly data found for this taxpayer'
            }), 404

        result = df_to_json(monthly_df)
        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для получения годовых итогов
@dashboard_bp.route('/yearly/totals/<inn>', methods=['GET'])
def get_yearly_totals(inn):
    """
    Получить годовые итоги (суммы) налогоплательщика
    """
    try:
        monthly_df = repo.get_monthly_data(inn)
        if monthly_df.empty:
            return jsonify({
                'success': False,
                'error': 'No monthly data found for this taxpayer'
            }), 404

        yearly_totals = repo.get_yearly_totals(monthly_df)
        result = df_to_json(yearly_totals)

        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для получения годовых медиан
@dashboard_bp.route('/yearly/median/<inn>', methods=['GET'])
def get_yearly_median(inn):
    """
    Получить годовые медианные значения налогоплательщика
    """
    try:
        monthly_df = repo.get_monthly_data(inn)
        if monthly_df.empty:
            return jsonify({
                'success': False,
                'error': 'No monthly data found for this taxpayer'
            }), 404

        yearly_median = repo.get_yearly_median(monthly_df)
        result = df_to_json(yearly_median)

        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для получения годового роста в процентах
@dashboard_bp.route('/yearly/growth/<inn>', methods=['GET'])
def get_yearly_growth(inn):
    """
    Получить процентный рост показателей по годам
    """
    try:
        monthly_df = repo.get_monthly_data(inn)
        if monthly_df.empty:
            return jsonify({
                'success': False,
                'error': 'No monthly data found for this taxpayer'
            }), 404

        yearly_totals = repo.get_yearly_totals(monthly_df)
        yearly_growth = repo.calculate_yearly_growth(yearly_totals)
        result = df_to_json(yearly_growth)

        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для получения прогноза на следующий год
@dashboard_bp.route('/predict/<inn>', methods=['GET'])
def get_prediction(inn):
    """
    Получить прогнозные данные на следующий год
    """
    try:
        taxpayer = repo.get_taxpayer_by_inn(inn)
        if taxpayer.empty:
            return jsonify({
                'success': False,
                'error': 'Taxpayer not found'
            }), 404

        monthly_df = repo.get_monthly_data(inn)
        if monthly_df.empty:
            return jsonify({
                'success': False,
                'error': 'No monthly data found for this taxpayer'
            }), 404

        last_year = monthly_df["Year"].max()
        prediction_df = repo.predict_next_year(taxpayer.iloc[0], last_year)
        result = df_to_json(prediction_df)

        return jsonify({
            'success': True,
            'data': result,
            'prediction_year': last_year + 1
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Главный маршрут для получения всех данных для дашборда
@dashboard_bp.route('/full/<inn>', methods=['GET'])
def get_full_dashboard(inn):
    """
    Получить полные данные для дашборда:
    - помесячные данные
    - годовые итоги
    - годовые медианы
    - годовой рост
    - прогноз
    """
    try:
        include_prediction = request.args.get('include_prediction', 'true').lower() == 'true'

        dashboard_data = repo.build_dashboard_data(inn, include_prediction)

        if "error" in dashboard_data:
            return jsonify({
                'success': False,
                'error': dashboard_data["error"]
            }), 404

        # Конвертируем все DataFrame в JSON
        result = {
            'monthly': df_to_json(dashboard_data['monthly']),
            'yearly_totals': df_to_json(dashboard_data['yearly_totals']),
            'yearly_median': df_to_json(dashboard_data['yearly_median']),
            'yearly_growth': df_to_json(dashboard_data['yearly_growth'])
        }

        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для получения данных для конкретного графика
@dashboard_bp.route('/chart/<chart_type>/<inn>', methods=['GET'])
def get_chart_data(chart_type, inn):
    """
    Получить данные для конкретного типа графика:
    - income: данные о доходах
    - transactions: данные о транзакциях
    - tax: данные о налогах
    """
    try:
        include_prediction = request.args.get('include_prediction', 'true').lower() == 'true'

        dashboard_data = repo.build_dashboard_data(inn, include_prediction)

        if "error" in dashboard_data:
            return jsonify({
                'success': False,
                'error': dashboard_data["error"]
            }), 404

        monthly_df = dashboard_data['monthly']

        if chart_type == 'income':
            chart_df = monthly_df[['Year', 'Month', 'Income']]
        elif chart_type == 'transactions':
            chart_df = monthly_df[['Year', 'Month', 'Transactions']]
        elif chart_type == 'tax':
            chart_df = monthly_df[['Year', 'Month', 'Tax']]
        else:
            return jsonify({
                'success': False,
                'error': f'Invalid chart type: {chart_type}. Choose from: income, transactions, tax'
            }), 400

        result = df_to_json(chart_df)

        return jsonify({
            'success': True,
            'chart_type': chart_type,
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для проверки доступности моделей
@dashboard_bp.route('/models/status', methods=['GET'])
def get_models_status():
    """
    Проверить статус загруженных ML моделей
    """
    try:
        models_status = {
            'income_model': hasattr(repo, 'income_model') and repo.income_model is not None,
            'transactions_model': hasattr(repo, 'transactions_model') and repo.transactions_model is not None,
            'tax_model': hasattr(repo, 'tax_model') and repo.tax_model is not None
        }

        return jsonify({
            'success': True,
            'data': models_status
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для получения глобального диапазона лет (по всей базе)
@dashboard_bp.route('/global-year-range', methods=['GET'])
def get_global_year_range():
    """
    Получить минимальный и максимальный год по всем данным в базе
    """
    try:
        year_range = repo.get_global_year_range()

        if year_range["min_year"] is None:
            return jsonify({
                'success': False,
                'error': 'No data found in database'
            }), 404

        return jsonify({
            'success': True,
            'data': year_range
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Маршрут для закрытия соединения с БД (обычно вызывается при завершении работы приложения)
@dashboard_bp.route('/close', methods=['POST'])
def close_connection():
    """
    Закрыть соединение с базой данных
    """
    try:
        repo.close()
        return jsonify({
            'success': True,
            'message': 'Database connection closed'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
