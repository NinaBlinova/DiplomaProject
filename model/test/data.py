from werkzeug.security import generate_password_hash

from model.database import DatabaseEngine
from model.data.login.AuthService import AuthService

# Подключение к БД
db_engine = DatabaseEngine()
auth_service = AuthService(db_engine)

# # Список новых пользователей
# new_users = [
#     {
#         "username": "vladimirblinov",
#         "email": "vladimirblinov@example.com",
#         "password": "vova123",
#         "full_name": "Vladimir Blinov"
#     },
#     {
#         "username": "ninablinova",
#         "email": "ninablinova@example.com",
#         "password": "nina123",
#         "full_name": "Nina Blinova"
#     },
#     {
#         "username": "sergeiblinov",
#         "email": "sergeiblinov@example.com",
#         "password": "sergei123",
#         "full_name": "Sergei Blinov"
#     }
# ]
#
# # Вставка пользователей через register()
# for user in new_users:
#     success, message = auth_service.register(
#         username=user["username"],
#         email=user["email"],
#         password=user["password"],
#         full_name=user["full_name"]
#     )
#     print(user["username"], ":", message)

user_id = 17
new_password = "pv_blino123"  # временный пароль

# Хеширование пароля (scrypt)
hashed_password = generate_password_hash(new_password, method="scrypt")

# Обновление пароля через execute_non_query
query = """
UPDATE users
SET PasswordHash = :password
WHERE Id = :user_id
"""

success = db_engine.execute_non_query(query, {"password": hashed_password, "user_id": user_id})

if success:
    print(f"Пароль для пользователя с Id={user_id} успешно сброшен на: {new_password}")
else:
    print("Не удалось сбросить пароль")