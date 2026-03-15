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
        SET IsActive = :is_active,
        CreatedAt = :created_at
        WHERE Id = :user_id
        """
        params = {
            "user_id": int(user_id),
            "is_active": 1 if is_active else 0,
            "created_at": datetime.datetime.now()
        }
        try:
            self.db_engine.execute_non_query(update_query, params)
            action = "Учетная запись активирована" if is_active else "Учетная запись деактивирована"
            self.logger.log_action(
                user_id=admin_id,
                username=admin_username,
                action=action,
                additional_info=f"Администратор {admin_username} (ID {admin_id}) установил IsActive={params['is_active']} для пользователя {username} (ID {user_id})"
            )
            message = "Учетная запись активирована" if is_active else "Учетная запись деактивирована"
            return True, message
        except Exception as e:
            return False, str(e)

    def register_employee(
            self,
            admin_id: int,
            username: str,
            email: str,
            password: str,
            full_name: str,
            passport_series: str = None,
            passport_number: str = None,
            passport_issued_by: str = None,
            passport_issue_date=None,
            snils: str = None,
            inn: str = None,
            oms_policy: str = None,
            birth_date=None,
            gender: str = None,
            address_reg: str = None,
            phone: str = None
    ):
        admin_query = "SELECT Username FROM Users WHERE Id = ?"
        admin = self.db_engine.execute_query(admin_query, [admin_id])

        if admin.empty:
            return False, "Admin not found"

        admin_username = admin.iloc[0]["Username"]

        check_query = "SELECT Id FROM Users WHERE Username = ? OR Email = ?"
        existing_user = self.db_engine.execute_query(check_query, [username, email])

        if not existing_user.empty:
            return False, "Пользователь с таким именем пользователя или адресом электронной почты уже существует."

        password_hash = generate_password_hash(password)

        insert_query = """
        INSERT INTO Users (
            Username,
            Email,
            PasswordHash,
            FullName,
            PassportSeries,
            PassportNumber,
            PassportIssuedBy,
            PassportIssueDate,
            SNILS,
            INN,
            OMSPolicyNumber,
            BirthDate,
            Gender,
            Address_Reg,
            Phone,
            CreatedAt,
            IsActive
        )
        VALUES (
            :username,
            :email,
            :password_hash,
            :full_name,
            :passport_series,
            :passport_number,
            :passport_issued_by,
            :passport_issue_date,
            :snils,
            :inn,
            :oms_policy,
            :birth_date,
            :gender,
            :address_reg,
            :phone,
            :created_at,
            :is_active
        )
        """

        params = {
            "username": username,
            "email": email,
            "password_hash": password_hash,
            "full_name": full_name,
            "passport_series": passport_series,
            "passport_number": passport_number,
            "passport_issued_by": passport_issued_by,
            "passport_issue_date": passport_issue_date,
            "snils": snils,
            "inn": inn,
            "oms_policy": oms_policy,
            "birth_date": birth_date,
            "gender": gender,
            "address_reg": address_reg,
            "phone": phone,
            "created_at": datetime.datetime.now(),
            "is_active": 1
        }

        try:
            self.db_engine.execute_non_query(insert_query, params)

            self.logger.log_action(
                user_id=admin_id,
                username=admin_username,
                action="Администратор создал сотрудника",
                additional_info=f"Администратор {admin_username} создал сотрудника {username}, ФИО: {full_name}"
            )

            return True, "Сотрудник успешно создан"

        except Exception as e:
            return False, str(e)

    def update_employee(
            self,
            admin_id: int,
            user_id: int,
            email: str = None,
            full_name: str = None,
            passport_series: str = None,
            passport_number: str = None,
            passport_issued_by: str = None,
            passport_issue_date=None,
            snils: str = None,
            inn: str = None,
            oms_policy: str = None,
            birth_date=None,
            gender: str = None,
            address_reg: str = None,
            phone: str = None
    ):

        admin_query = "SELECT Username FROM Users WHERE Id = ?"
        admin = self.db_engine.execute_query(admin_query, [admin_id])

        if admin.empty:
            return False, "Администратор не найден"

        admin_username = admin.iloc[0]["Username"]
        fields_map = {
            "Email": email,
            "FullName": full_name,
            "PassportSeries": passport_series,
            "PassportNumber": passport_number,
            "PassportIssuedBy": passport_issued_by,
            "PassportIssueDate": passport_issue_date,
            "SNILS": snils,
            "INN": inn,
            "OMSPolicyNumber": oms_policy,
            "BirthDate": birth_date,
            "Gender": gender,
            "Address_Reg": address_reg,
            "Phone": phone,
        }

        update_parts = []
        params = {"user_id": user_id}
        for column, value in fields_map.items():
            if value is not None:
                param_name = column.lower()
                update_parts.append(f"{column} = :{param_name}")
                params[param_name] = value
        if not update_parts:
            return False, "Нет полей для обновления."
        update_parts.append("createdAt = :createdat")
        params["createdat"] = datetime.datetime.now()
        update_query = f"""
        UPDATE Users
        SET {", ".join(update_parts)}
        WHERE Id = :user_id
        """
        try:
            self.db_engine.execute_non_query(update_query, params)

            self.logger.log_action(
                user_id=admin_id,
                username=admin_username,
                action="Администратор обновил информацию о сотруднике.",
                additional_info=f"Администратор {admin_username} обновил сотрудника ID {user_id}"
            )
            return True, "Информация о сотруднике успешно обновлена."
        except Exception as e:
            return False, str(e)
