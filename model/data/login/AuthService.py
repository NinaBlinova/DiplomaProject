import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text


class AuthService:

    def __init__(self, db_engine):
        self.db = db_engine

    def register(self, username, email, password, full_name):
        password_hash = generate_password_hash(password)
        query = """
            INSERT INTO Users (Username, Email, PasswordHash, FullName, CreatedAt)
            VALUES (:username, :email, :password_hash, :full_name, :created_at)
            """
        params = {
            "username": username,
            "email": email,
            "password_hash": password_hash,
            "full_name": full_name,
            "created_at": datetime.datetime.utcnow()
        }

        success = self.db.execute_non_query(query, params)
        if success:
            return True, "User created successfully"
        else:
            return False, "Insert failed"

    def login(self, username, password):
        query = "SELECT * FROM Users WHERE Username = ?"
        user = self.db.execute_query(query, [username])
        if user.empty:
            return False, "User not found"
        user_data = user.iloc[0]
        if not check_password_hash(user_data["PasswordHash"], password):
            return False, "Invalid password"
        return True, user_data.to_dict()
