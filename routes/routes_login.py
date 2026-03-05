from flask import Blueprint, request, jsonify

from model.data.login.AuthService import AuthService
from model.database import DatabaseEngine

login_bp = Blueprint("login_bp", __name__, url_prefix="/api/auth")
db_engine = DatabaseEngine()
service = AuthService(db_engine)

# @login_bp.route("/register", methods=["POST"])
# def register():
#     data = request.get_json()
#     username = data.get("username")
#     email = data.get("email")
#     password = data.get("password")
#     full_name = data.get("full_name")
#     if not all([username, email, password, full_name]):
#         return jsonify({"success": False, "message": "Missing required fields"}), 400
#     success, message = service.register(username, email, password, full_name)
#     if not success:
#         return jsonify({"success": False, "message": message}), 400
#     return jsonify({"success": True, "message": message}), 201


@login_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        return jsonify({"success": False, "message": "Missing credentials"}), 400
    success, result = service.login(username, password)
    if not success:
        return jsonify({"success": False, "message": result}), 401
    result.pop("PasswordHash", None)
    result.pop("Avatar", None)
    return jsonify({
        "success": True,
        "user": result
    }), 200
