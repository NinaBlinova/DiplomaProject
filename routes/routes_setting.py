from flask import Blueprint, request, jsonify, send_file
from io import BytesIO

from model.data.setting.UserSettingsService import UserSettingsService
from model.database import DatabaseEngine

setting_bp = Blueprint("setting_bp", __name__, url_prefix="/api/settings")
db_engine = DatabaseEngine()
service_bp = UserSettingsService(db_engine)


# update full profile
@setting_bp.route("/profile", methods=["PUT"])
def update_profile():
    data = request.json

    user_id = data.get("user_id")
    full_name = data.get("full_name")
    email = data.get("email")
    username = data.get("username")
    bio = data.get("bio")

    success, message = service_bp.update_profile(
        user_id, full_name, email, username, bio
    )

    print("DATA FROM FRONTEND:", data)

    status = 200 if success else 400
    return jsonify({"success": success, "message": message}), status


# change avatar
@setting_bp.route("/avatar", methods=["PATCH"])
def change_avatar():
    user_id = request.form.get("user_id")
    avatar_file = request.files.get("avatar")
    if not avatar_file:
        return jsonify({"success": False, "message": "No file"}), 400
    avatar_bytes = avatar_file.read()
    success, message = service_bp.update_users_info(
        user_id, "Avatar", avatar_bytes
    )
    status = 200 if success else 400
    return jsonify({"success": success, "message": message}), status


# change password
@setting_bp.route("/password", methods=["PATCH"])
def change_password():
    data = request.json
    user_id = data.get("user_id")
    old_password = data.get("old_password")
    new_password = data.get("new_password")
    success, message = service_bp.change_password(
        user_id, old_password, new_password
    )
    status = 200 if success else 400
    return jsonify({"success": success, "message": message}), status

@setting_bp.route("/avatar/<int:user_id>", methods=["GET"])
def get_avatar(user_id):
    success, avatar_bytes, message = service_bp.get_avatar(user_id)

    if not success:
        return jsonify({"success": False, "message": message}), 404

    if isinstance(avatar_bytes, memoryview):
        avatar_bytes = avatar_bytes.tobytes()

    return send_file(
        BytesIO(avatar_bytes),
        mimetype="image/jpeg",
        as_attachment=False
    )