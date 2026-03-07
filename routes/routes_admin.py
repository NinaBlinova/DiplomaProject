import base64

from flask import Blueprint, jsonify

from model.data.UserRepository import UserRepository
from model.database import DatabaseEngine

admin_bp = Blueprint("admin_bp", __name__, url_prefix="/api/admin")
db_engine = DatabaseEngine()
repository_users = UserRepository(db_engine)


@admin_bp.route("/members", methods=["GET"])
def get_members():
    try:
        users_df = repository_users.get_users_with_logs(sort_by="Username", sort_order="ASC")
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