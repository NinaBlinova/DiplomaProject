from flask import Blueprint, jsonify

from model.class_taxpayer import TaxpayerRepository
from model.database import DatabaseEngine

routes_taxpayer = Blueprint('routes_taxpayer', __name__)

db_engine = DatabaseEngine()
repo = TaxpayerRepository(db_engine)


def map_taxpayer_to_user(row):
    """
    Преобразование строки БД в формат,
    который ожидает фронтенд (User)
    """
    print(row)
    return {
        "id": row["TaxpayerId"],
        "name": row["FullName"],
        "INN": row["INN"],
        "registration_district": row["registration_district"],
        "has_employees": row["has_employees"],
        "avatar": {
            "src": f"https://api.dicebear.com/7.x/initials/svg?seed={row['FullName']}"
        }
    }


@routes_taxpayer.route('/api/taxpayers', methods=['GET'])
def get_taxpayers():
    """
    Получить всех налогоплательщиков
    """
    df = repo.get_all_taxpayers()
    print(df)

    if df is None:
        return jsonify({"error": "Database error"}), 500

    taxpayers = [map_taxpayer_to_user(row) for _, row in df.iterrows()]
    return jsonify(taxpayers), 200


@routes_taxpayer.route('/api/taxpayers/<string:inn>', methods=['GET'])
def get_taxpayer_by_inn(inn):
    """
    Получить налогоплательщика по ИНН
    """
    df = repo.get_taxpayer_by_inn(inn)

    if df is None:
        return jsonify({"error": "Database error"}), 500

    if df.empty:
        return jsonify({"error": "Taxpayer not found"}), 404

    taxpayer = map_taxpayer_to_user(df.iloc[0])
    return jsonify(taxpayer), 200
