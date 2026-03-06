from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text
import datetime


class UserSettingsService:

    def __init__(self, db_engine):
        self.db = db_engine

    def update_profile(self, user_id, full_name, email, username, bio):
        query = """
        UPDATE Users
        SET FullName = :full_name,
            Email = :email,
            Username = :username,
            Bio = :bio,
            CreatedAt = :createdAt
        WHERE Id = :user_id
        """
        try:
            params = {
                'full_name': full_name,
                'email': email,
                'username': username,
                'bio': bio,
                'user_id': user_id,
                'createdAt': datetime.datetime.now(),
            }
            self.db.execute_non_query(query, params)
            return True, "Profile updated"
        except Exception as e:
            return False, str(e)

    def change_password(self, user_id, old_password, new_password):
        query = "SELECT PasswordHash FROM Users WHERE Id = ?"
        user = self.db.execute_query(query, [user_id])
        if user.empty:
            return False, "User not found"
        stored_hash = user.iloc[0]["PasswordHash"]
        if not check_password_hash(stored_hash, old_password):
            return False, "Old password incorrect"
        update_query = """
        UPDATE Users
        SET PasswordHash = :password
        WHERE Id = :user_id
        """
        params = {
            "password": generate_password_hash(new_password),
            "user_id": user_id
        }
        self.db.execute_non_query(update_query, params)
        return True, "Password updated"

    def update_users_info(self, user_id, column_name, value):
        query = f"UPDATE Users SET {column_name} = :value WHERE Id = :user_id"
        success = self.db.execute_non_query(query, {
            "value": value,
            "user_id": user_id
        })
        if success:
            return True, "Value updated"
        else:
            return False, "Database error"

    def get_avatar(self, user_id):
        query = """
        SELECT Avatar
        FROM Users
        WHERE Id = ?
        """
        try:
            result = self.db.execute_query(query, [user_id])
            if result.empty:
                return False, None, "User not found"
            avatar = result.iloc[0]["Avatar"]
            if avatar is None:
                return False, None, "Avatar not found"
            return True, avatar, "Avatar loaded"
        except Exception as e:
            return False, None, str(e)

    # def delete_account(self, user_id):
    #     query = "DELETE FROM Users WHERE Id = ?"
    #     try:
    #         self.db.execute_query(query, [user_id])
    #         return True, "Account deleted"
    #     except Exception as e:
    #         return False, str(e)
