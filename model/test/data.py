from model.database import DatabaseEngine
from model.data.login.AuthService import AuthService

# Подключение к БД
db_engine = DatabaseEngine()
auth_service = AuthService(db_engine)

# Список новых пользователей
new_users = [
    {
        "username": "vladimirblinov",
        "email": "vladimirblinov@example.com",
        "password": "vova123",
        "full_name": "Vladimir Blinov"
    },
    {
        "username": "ninablinova",
        "email": "ninablinova@example.com",
        "password": "nina123",
        "full_name": "Nina Blinova"
    },
    {
        "username": "sergeiblinov",
        "email": "sergeiblinov@example.com",
        "password": "sergei123",
        "full_name": "Sergei Blinov"
    }
]

# Вставка пользователей через register()
for user in new_users:
    success, message = auth_service.register(
        username=user["username"],
        email=user["email"],
        password=user["password"],
        full_name=user["full_name"]
    )
    print(user["username"], ":", message)
