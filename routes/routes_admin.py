from flask import Blueprint, jsonify, request

from model.data.UserRepository import UserRepository
from model.data.setting.AdminSettingsService import AdminSettingsService
from model.data.setting.UserSettingsService import UserSettingsService
from model.database import DatabaseEngine

admin_bp = Blueprint("admin_bp", __name__, url_prefix="/api/admin")
db_engine = DatabaseEngine()
repository_users = UserRepository(db_engine)
admin_service = AdminSettingsService(db_engine)
edit_service = UserSettingsService(db_engine)


@admin_bp.route("/members", methods=["GET"])
def get_members():
    try:
        users_df = repository_users.get_users(sort_by="Username", sort_order="ASC")
        members = []
        for _, row in users_df.iterrows():
            members.append({
                "Id": row["UserId"],
                "FullName": row["FullName"],
                "Username": row["Username"],
                "Email": row["Email"],
                "user_role": row["user_role"],
                "IsActive": row["IsActive"],
                "CreatedAt": row["CreatedAt"].isoformat(),
            })
        print(f'members: {members}')
        return jsonify(members), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/deactivate_user", methods=["PUT"])
def deactivate_users():
    try:
        data = request.get_json()
        print(f'DEBUG deactivate_user payload: {data}')
        admin_id = data.get("admin_id")
        target_user_id = data.get("target_user_id")
        if not all([admin_id, target_user_id]):
            return jsonify({"success": False, "message": "Missing admin_id or target_user_id"}), 400
        success, msg = admin_service.manage_account(admin_id=admin_id, target_user_id=target_user_id, is_active=False)
        status_code = 200 if success else 400
        return jsonify({"success": success, "message": msg}), status_code
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@admin_bp.route("/activate_user", methods=["PUT"])
def activate_users():
    try:
        data = request.get_json()
        admin_id = data.get("admin_id")
        target_user_id = data.get("target_user_id")
        if not all([admin_id, target_user_id]):
            return jsonify({"success": False, "message": "Missing admin_id or target_user_id"}), 400
        success, msg = admin_service.manage_account(admin_id=admin_id, target_user_id=target_user_id, is_active=True)
        status_code = 200 if success else 400
        return jsonify({"success": success, "message": msg}), status_code
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@admin_bp.route("/register", methods=["POST"])
def register():
    try:
        data = request.get_json()
        admin_id = data.get("admin_id")
        username = data.get("username")
        email = data.get("email")
        password = data.get("password")
        full_name = data.get("full_name")
        if not all([admin_id, username, email, password, full_name]):
            return jsonify({"success": False, "message": "Missing required fields"}), 400
        # admin_id: int, username: str, email: str, password: str, full_name: str
        success, message = admin_service.register(
            admin_id=int(admin_id),
            username=username,
            email=email,
            password=password,
            full_name=full_name
        )
        status_code = 201 if success else 400
        return jsonify({"success": success, "message": message}), status_code
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@admin_bp.route("/user_logs/<int:user_id>", methods=["GET"])
def get_user_logs(user_id):
    try:
        limit = request.args.get("limit", default=100, type=int)
        logs_df = repository_users.get_user_log(user_id=user_id, limit=limit)
        logs = []
        for _, row in logs_df.iterrows():
            logs.append({
                "LogId": row["LogId"],
                "UserId": row["UserId"],
                "Username": row["Username"],
                "Action": row["Action"],
                "AdditionalInfo": row["AdditionalInfo"],
                "ActionDate": row["ActionDate"].isoformat(),
            })
        return jsonify({"success": True, "logs": logs}), 200
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@admin_bp.route("/edit_user", methods=["PUT"])
def edit_user():
    try:
        data = request.get_json()
        admin_id = data.get("admin_id")
        target_user_id = data.get("user_id")
        updates = data.get("updates")  # dict {"FullName": "...", "Email": "...", "Bio": "..."}
        if not all([admin_id, target_user_id, updates]):
            return jsonify({"success": False, "message": "Missing required fields"}), 400
        results = {}
        for column, value in updates.items():
            success, msg = edit_service.update_users_info(target_user_id, column, value)
            results[column] = {"success": success, "message": msg}
        overall_success = all(r["success"] for r in results.values())
        status_code = 200 if overall_success else 400
        return jsonify({"success": overall_success, "results": results}), status_code
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
