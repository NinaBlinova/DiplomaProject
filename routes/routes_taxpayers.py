import pandas as pd
from flask import Blueprint, jsonify, request
import json
import numpy as np

from model.class_taxpayer import TaxpayerRepository
from model.database import DatabaseEngine

routes_taxpayer = Blueprint('routes_taxpayer', __name__)

db_engine = DatabaseEngine()
repo = TaxpayerRepository(db_engine)


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        if isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        if isinstance(obj, (np.bool_)):
            return bool(obj)
        if pd.isna(obj):
            return None
        return super(NpEncoder, self).default(obj)


def map_taxpayer_to_user(row):
    """
    Преобразование строки БД в формат,
    который ожидает фронтенд (User)
    """
    # Конвертируем numpy типы в стандартные Python типы
    taxpayer_id = int(row["TaxpayerId"]) if isinstance(row["TaxpayerId"], (np.integer, np.int64)) else row["TaxpayerId"]
    has_employees = bool(row["has_employees"]) if isinstance(row["has_employees"], (np.bool_, bool)) else row[
        "has_employees"]
    employees_count = int(row["employees_count"]) if row["employees_count"] and not pd.isna(
        row["employees_count"]) else None

    return {
        "id": taxpayer_id,
        "name": str(row["FullName"]),
        "INN": str(row["INN"]),
        "registration_district": str(row["registration_district"]),
        "has_employees": has_employees,
        "employees_count": employees_count,
        "avatar": {
            "src": f"https://api.dicebear.com/7.x/initials/svg?seed={str(row['FullName'])}"
        }
    }


@routes_taxpayer.route('/api/taxpayers', methods=['GET'])
def get_taxpayers():
    """
    Получить налогоплательщиков с пагинацией и фильтрацией
    """
    try:
        # Параметры запроса
        page = int(request.args.get('page', 1))
        page_size = min(int(request.args.get('pageSize', 10)), 100)
        inn_filter = request.args.get('inn', '')
        district_filter = request.args.get('district', '')
        sort_by = request.args.get('sortBy', 'TaxpayerId')
        sort_order = request.args.get('sortOrder', 'asc').upper()
        print("PAGE:", page)
        print("PAGE SIZE:", page_size)

        result = repo.get_taxpayers_paginated(
            page=page,
            page_size=page_size,
            inn_filter=inn_filter if inn_filter else None,
            district_filter=district_filter if district_filter else None,
            sort_by=sort_by,
            sort_order=sort_order
        )

        taxpayers = []
        if not result['data'].empty:
            for _, row in result['data'].iterrows():
                taxpayers.append(map_taxpayer_to_user(row))

        response_data = {
            'data': taxpayers,
            'total': int(result['total']),
            'page': int(result['page']),
            'pageSize': int(result['page_size']),
            'totalPages': int(result['total_pages'])
        }

        # Используем собственный JSON encoder для обработки numpy типов
        return jsonify(response_data), 200

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Database error: {str(e)}"}), 500


@routes_taxpayer.route('/api/taxpayers/<string:inn>', methods=['GET'])
def get_taxpayer_by_inn(inn):
    """
    Получить налогоплательщика по ИНН
    """
    df = repo.get_taxpayer_by_inn(inn)

    if df.empty:
        return jsonify({"error": "Taxpayer not found"}), 404

    taxpayer = map_taxpayer_to_user(df.iloc[0])
    return jsonify(taxpayer), 200