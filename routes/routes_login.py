from flask import Blueprint, request, jsonify

from model.data.login.AuthService import AuthService
from model.database import DatabaseEngine

login_bp = Blueprint("login_bp", __name__, url_prefix="/api/auth")
db_engine = DatabaseEngine()
service = AuthService(db_engine)


@login_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        return jsonify({"success": False, "message": "Missing credentials"}), 400
    success, result = service.login(username, password)
    if not success:
        return jsonify({"success": False, "message": result}), 200
    result.pop("PasswordHash", None)
    result.pop("Avatar", None)
    return jsonify({
        "success": True,
        "user": result
    }), 200

@login_bp.route("/logout", methods=["POST"])
def logout():
    data = request.get_json(silent=True) or {}
    username = data.get("username")
    user_id = data.get("user_id")
    if username and user_id:
        service.logger.log_action(
            user_id=int(user_id),
            username=username,
            action="Logout",
            additional_info="User logged out"
        )
    print('User logged out')
    return jsonify({"success": True, "message": "Logged out"}), 200