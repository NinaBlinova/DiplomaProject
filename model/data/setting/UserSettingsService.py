from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text
import datetime


class UserSettingsService:

    def __init__(self, db_engine):
        self.db = db_engine

    def update_profile(self, user_id, full_name, email,
                       username, bio):
        query = """
        UPDATE Users
        SET FullName = ?,
            Email = ?,
            Username = ?,
            Bio = ?
        WHERE Id = ?
        """
        try:
            self.db.execute_query(query,
                                  [full_name, email, username, bio, user_id])
            return True, "Profile updated"
        except Exception as e:
            return False, str(e)

    def update_avatar(self, user_id, avatar_bytes):
        query = """
        UPDATE Users
        SET Avatar = ?
        WHERE Id = ?
        """
        try:
            self.db.execute_query(query, [avatar_bytes, user_id])
            return True, "Avatar updated"
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
        new_hash = generate_password_hash(new_password)
        update_query = """
        UPDATE Users
        SET PasswordHash = ?
        WHERE Id = ?
        """
        self.db.execute_query(update_query, [new_hash, user_id])
        return True, "Password updated"

    def delete_account(self, user_id):
        query = "DELETE FROM Users WHERE Id = ?"
        try:
            self.db.execute_query(query, [user_id])
            return True, "Account deleted"
        except Exception as e:
            return False, str(e)
