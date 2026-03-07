from werkzeug.security import generate_password_hash, check_password_hash
import datetime

from model.data.LoggerService import LoggerService


class UserSettingsService:

    def __init__(self, db_engine):
        self.db = db_engine
        self.logger = LoggerService(db_engine)

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
            self.logger.log_action(
                user_id=user_id,
                username=username,
                action="Update Profile",
                additional_info=f"FullName={full_name}, Email={email}, Bio={bio}"
            )
            return True, "Profile updated"
        except Exception as e:
            return False, str(e)

    def change_password(self, user_id, old_password, new_password):
        query = "SELECT PasswordHash FROM Users WHERE Id = ?"
        user = self.db.execute_query(query, [user_id])
        if user.empty:
            return False, "User not found"
        stored_hash = user.iloc[0]["PasswordHash"]
        username = user.iloc[0]["Username"]
        if not check_password_hash(stored_hash, old_password):
            return False, "Old password incorrect"
        update_query = """
        UPDATE Users
        SET PasswordHash = :password
        CreatedAt = :createdAt
        WHERE Id = :user_id
        """
        params = {
            "password": generate_password_hash(new_password),
            "user_id": user_id,
            'createdAt': datetime.datetime.now()
        }
        self.db.execute_non_query(update_query, params)
        self.logger.log_action(
            user_id=user_id,
            username=username,
            action="Change Password"
        )
        return True, "Password updated"

    def update_users_info(self, user_id, column_name, value):
        result = self.db.execute_query("SELECT Username FROM Users WHERE Id = ?", [user_id])
        username = result.iloc[0]["Username"] if not result.empty else ""
        query = f"UPDATE Users SET {column_name} = :value WHERE Id = :user_id"
        success = self.db.execute_non_query(query, {
            "value": value,
            "user_id": user_id
        })
        if success:
            self.logger.log_action(
                user_id=user_id,
                username=username,
                action=f"Update {column_name}",
                additional_info=f"New value: {value}"
            )
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
