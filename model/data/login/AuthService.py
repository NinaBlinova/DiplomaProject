import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text


class AuthService:

    def __init__(self, db_engine):
        self.db = db_engine

    # def register(self, username, email, password, full_name):
    #     password_hash = generate_password_hash(password)
    #     query = """
    #         INSERT INTO Users (Username, Email, PasswordHash, FullName, CreatedAt)
    #         VALUES (:username, :email, :password_hash, :full_name, :created_at)
    #         """
    #     params = {
    #         "username": username,
    #         "email": email,
    #         "password_hash": password_hash,
    #         "full_name": full_name,
    #         "created_at": datetime.datetime.utcnow()
    #     }
    #
    #     success = self.db.execute_non_query(query, params)
    #     if success:
    #         return True, "User created successfully"
    #     else:
    #         return False, "Insert failed"

    def login(self, username, password):
        try:
            query = "SELECT * FROM Users WHERE Username = ?"
            user = self.db.execute_query(query, [username])
            if user.empty:
                print(f"[Login] User not found: {username}")
                return False, "User not found"
            user_data = user.iloc[0]
            password_hash = user_data.get("PasswordHash")
            if not password_hash:
                print(f"[Login] PasswordHash missing for user: {username}")
                return False, "Password hash missing"
            if not check_password_hash(password_hash, password):
                print(f"[Login] Invalid password for user: {username}")
                return False, "Invalid password"
            return True, user_data.to_dict()
        except Exception as e:
            print(f"[AuthService.login] Error: {e}")
            return False, str(e)
