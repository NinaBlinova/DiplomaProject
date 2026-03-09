import datetime
from werkzeug.security import generate_password_hash

from model.data.LoggerService import LoggerService


class AdminSettingsService:
    def __init__(self, db_engine):
        self.db_engine = db_engine
        self.logger = LoggerService(db_engine)

    def manage_account(self, admin_id: int, target_user_id: int, is_active: bool):
        admin_query = "SELECT Username FROM Users WHERE Id = ?"
        admin = self.db_engine.execute_query(admin_query, [admin_id])
        if admin.empty:
            return False, "Admin not found"
        admin_username = admin.iloc[0]["Username"]
        user_query = "SELECT Id, Username FROM Users WHERE Id = ?"
        user = self.db_engine.execute_query(user_query, [target_user_id])
        if user.empty:
            return False, "User not found"
        username = user.iloc[0]["Username"]
        user_id = user.iloc[0]["Id"]
        update_query = """
        UPDATE Users
        SET IsActive = :is_active
        WHERE Id = :user_id
        """
        params = {
            "user_id": int(user_id),
            "is_active": 1 if is_active else 0
        }
        try:
            self.db_engine.execute_non_query(update_query, params)
            self.logger.log_action(
                user_id=admin_id,
                username=admin_username,
                action="Admin Deactivated User",
                additional_info=f"Admin {admin_username} (ID {admin_id}) set IsActive={params['is_active']} for user {username} (ID {user_id})"
            )
            message = "Account activated" if is_active else "Account deactivated"
            return True, message
        except Exception as e:
            return False, str(e)

    def register(self, admin_id: int, username: str, email: str, password: str, full_name: str):
        admin_query = "SELECT Username FROM Users WHERE Id = ?"
        admin = self.db_engine.execute_query(admin_query, [admin_id])
        if admin.empty:
            return False, "Admin not found"
        admin_username = admin.iloc[0]["Username"]
        check_query = "SELECT Id FROM Users WHERE Username = ? OR Email = ?"
        existing_user = self.db_engine.execute_query(check_query, [username, email])

        if not existing_user.empty:
            return False, "User with this username or email already exists"

        password_hash = generate_password_hash(password)
        insert_query = """
            INSERT INTO Users (
                Username,
                Email,
                PasswordHash,
                FullName,
                CreatedAt,
                IsActive,
            )
            VALUES (
                :username,
                :email,
                :password_hash,
                :full_name,
                :created_at,
                :is_active,
            )
        """
        params = {
            "username": username,
            "email": email,
            "password_hash": password_hash,
            "full_name": full_name,
            "created_at": datetime.datetime.utcnow(),
            "is_active": 1,
        }
        try:
            self.db_engine.execute_non_query(insert_query, params)
            self.logger.log_action(
                user_id=admin_id,
                username=admin_username,
                action="Admin Created User",
                additional_info=f"Admin {admin_username} (ID {admin_id}) created user {username}, ФИО: {full_name}"
            )
            return True, "User created successfully"
        except Exception as e:
            return False, str(e)

    def show_history(self):
        pass
