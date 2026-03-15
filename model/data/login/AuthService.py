from werkzeug.security import check_password_hash

from model.data.LoggerService import LoggerService


class AuthService:

    def __init__(self, db_engine):
        self.db = db_engine
        self.logger = LoggerService(db_engine)

    def login(self, username, password):
        try:
            query = "SELECT * FROM Users WHERE Username = ?"
            user = self.db.execute_query(query, [username])
            print(f'User {user} logged in as {user["Username"]}')
            if user.empty:
                self.logger.log_action(
                    user_id=None,
                    username=username,
                    action="Ошибка входа",
                    additional_info="Пользователь не найден"
                )
                print(f"[Login] User not found: {username}")
                return False, "Пользователь не найден"
            user_data = user.iloc[0]
            user_id = int(user_data["Id"])
            print(f'use_data {user_data}')
            if user_data["IsActive"] == 0:
                self.logger.log_action(
                    user_id=user_id,
                    username=username,
                    action="Ошибка входа",
                    additional_info="Аккаунт не активен"
                )
                print(f"[Login] User inactive: {username}")
                return False, "Аккаунт не активен"
            password_hash = user_data.get("PasswordHash")
            if not password_hash:
                self.logger.log_action(
                    user_id=user_id,
                    username=username,
                    action="Ошибка входа",
                    additional_info="Хэш пароля отсутствует"
                )
                print(f"[Login] PasswordHash missing for user: {username}")
                return False, "Хэш пароля отсутствует"
            if not check_password_hash(password_hash, password):
                self.logger.log_action(
                    user_id=user_id,
                    username=username,
                    action="Ошибка входа",
                    additional_info="Неверный пароль"
                )
                print(f"[Login] Invalid password for user: {username}")
                return False, "Неверный пароль"
            self.logger.log_action(
                user_id=user_id,
                username=username,
                action="Вход успешный"
            )
            return True, user_data.to_dict()
        except Exception as e:
            print(f"[AuthService.login] Error: {e}")
            return False, str(e)
